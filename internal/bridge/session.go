/*
 * OpenFriend — Minecraft Java Edition Friends List bridge.
 * Copyright (c) 2026 ZSHARE (https://zpw.jp). Licensed under the MIT License.
 */
package bridge

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

type Role int

const (
	RoleAcceptor Role = iota
	RoleInitiator
)

type Session struct {
	SessionID string
	Role      Role

	pc     *webrtc.PeerConnection
	dc     *webrtc.DataChannel
	logger *slog.Logger

	mu        sync.Mutex
	queuedICE []webrtc.ICECandidateInit
	remoteSet bool

	onLocalICE func(*webrtc.ICECandidate)
	onData     func([]byte)
	onClose    func()

	dcOpenCh   chan struct{}
	dcOpenOnce sync.Once

	// DataChannel 수신 큐 - pion goroutine 블로킹 방지
	recvCh   chan *recvBuf
	recvOnce sync.Once

	// DataChannel 흐름 제어 - backpressure
	pauseMu    sync.Mutex
	sendPaused bool
	resumeCh   chan struct{}
}

func NewSession(api *webrtc.API, cfg webrtc.Configuration, role Role, sessionID string, _ [16]byte,
	onLocalICE func(*webrtc.ICECandidate),
	onData func([]byte),
	onClose func(),
	logger *slog.Logger) (*Session, error) {
	if logger == nil {
		logger = slog.Default()
	}
	pc, err := api.NewPeerConnection(cfg)
	if err != nil {
		return nil, err
	}
	s := &Session{
		SessionID:  sessionID,
		Role:       role,
		pc:         pc,
		logger:     logger,
		onLocalICE: onLocalICE,
		onData:     onData,
		onClose:    onClose,
		dcOpenCh:   make(chan struct{}),
		recvCh:     make(chan *recvBuf, 16384),
		resumeCh:   make(chan struct{}),
	}
	go s.recvLoop()
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil || s.onLocalICE == nil {
			return
		}
		s.onLocalICE(c)
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		s.logger.Info("PeerConnection state", "state", state)
		switch state {
		case webrtc.PeerConnectionStateFailed:
			// ICE 재연결 불가 → 세션 종료
			if s.onClose != nil {
				s.onClose()
			}
		case webrtc.PeerConnectionStateDisconnected:
			// 일시적 연결 끊김 — pion이 자동 ICE 재연결 시도.
			// 재연결 안 되면 Failed로 전환됨.
			s.logger.Warn("[rtc] PeerConnection disconnected, waiting for reconnect...")
		case webrtc.PeerConnectionStateConnected:
			s.logger.Info("[rtc] PeerConnection connected")
		}
	})

	if role == RoleAcceptor {
		pc.OnDataChannel(s.attachDataChannel)
	}
	return s, nil
}

const (
	// dcBufferLow - 이 이하로 내려가면 Send 재개
	dcBufferLow = 512 * 1024 // 512KB

	// dcBufferHigh - 이 이상이면 Send 대기.
	// TCP의 소켓 버퍼와 동일한 역할 — 차면 블로킹, 빠지면 재개.
	// 타임아웃 없음: TCP도 버퍼가 차면 Write가 블로킹될 뿐 연결을 끊지 않음.
	dcBufferHigh = 4 * 1024 * 1024 // 4MB
)

// recvBuf - 수신 버퍼 하나
type recvBuf struct {
	data [65536]byte
	len  int
}

// recvBufPool - 미리 할당된 고정 버퍼 풀
type recvBufPoolT struct {
	ch chan *recvBuf
}

// globalRecvPool - 모든 Session이 공유하는 수신 버퍼 풀
// 100명 동시 세션 × burst를 커버하도록 32768로 확대
var globalRecvPool = newRecvBufPool(32768)

func newRecvBufPool(size int) *recvBufPoolT {
	ch := make(chan *recvBuf, size)
	for i := 0; i < size; i++ {
		ch <- &recvBuf{}
	}
	return &recvBufPoolT{ch: ch}
}

func (p *recvBufPoolT) get() *recvBuf {
	select {
	case b := <-p.ch:
		return b
	default:
		return &recvBuf{}
	}
}

func (p *recvBufPoolT) put(b *recvBuf) {
	select {
	case p.ch <- b:
	default:
	}
}

func (s *Session) attachDataChannel(dc *webrtc.DataChannel) {
	s.logger.Info("[rtc] DataChannel attached", "label", dc.Label())
	s.mu.Lock()
	s.dc = dc
	s.mu.Unlock()

	dc.SetBufferedAmountLowThreshold(dcBufferLow)
	dc.OnBufferedAmountLow(func() {
		s.pauseMu.Lock()
		if s.sendPaused {
			s.sendPaused = false
			ch := s.resumeCh
			s.resumeCh = make(chan struct{})
			close(ch)
		}
		s.pauseMu.Unlock()
	})

	dc.OnOpen(func() {
		s.dcOpenOnce.Do(func() { close(s.dcOpenCh) })
	})
	dc.OnClose(func() {
		// DataChannel 닫힘 → 대기 중인 Send() 깨워서 에러 반환하게 함
		s.pauseMu.Lock()
		if s.sendPaused {
			s.sendPaused = false
			ch := s.resumeCh
			s.resumeCh = make(chan struct{})
			close(ch)
		}
		s.pauseMu.Unlock()
		if s.onClose != nil {
			s.onClose()
		}
	})
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		b := globalRecvPool.get()
		b.len = copy(b.data[:], msg.Data)
		s.recvCh <- b
	})
	if dc.ReadyState() == webrtc.DataChannelStateOpen {
		s.dcOpenOnce.Do(func() { close(s.dcOpenCh) })
	}
}

func (s *Session) HandleOffer(offerSDP string) (string, error) {
	err := s.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	})
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.remoteSet = true
	queued := s.queuedICE
	s.queuedICE = nil
	s.mu.Unlock()
	for _, c := range queued {
		if err := s.pc.AddICECandidate(c); err != nil {
			s.logger.Warn("Failed to add queued ICE", "err", err)
		}
	}

	answer, err := s.pc.CreateAnswer(nil)
	if err != nil {
		return "", err
	}
	if err := s.pc.SetLocalDescription(answer); err != nil {
		return "", err
	}
	return answer.SDP, nil
}

func (s *Session) CreateOffer() (string, error) {
	if s.Role != RoleInitiator {
		return "", errors.New("CreateOffer only valid for initiator")
	}
	ordered := true
	dc, err := s.pc.CreateDataChannel("minecraft", &webrtc.DataChannelInit{
		Ordered: &ordered,
	})
	if err != nil {
		return "", err
	}
	s.attachDataChannel(dc)

	offer, err := s.pc.CreateOffer(nil)
	if err != nil {
		return "", err
	}
	if err := s.pc.SetLocalDescription(offer); err != nil {
		return "", err
	}
	return offer.SDP, nil
}

func (s *Session) HandleAnswer(answerSDP string) error {
	if s.Role != RoleInitiator {
		return errors.New("HandleAnswer only valid for initiator")
	}
	err := s.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answerSDP,
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.remoteSet = true
	queued := s.queuedICE
	s.queuedICE = nil
	s.mu.Unlock()
	for _, c := range queued {
		if err := s.pc.AddICECandidate(c); err != nil {
			s.logger.Warn("Failed to add queued ICE", "err", err)
		}
	}
	return nil
}

func (s *Session) AddRemoteICE(init webrtc.ICECandidateInit) {
	s.mu.Lock()
	if !s.remoteSet {
		s.queuedICE = append(s.queuedICE, init)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	if err := s.pc.AddICECandidate(init); err != nil {
		s.logger.Warn("Failed to add ICE", "err", err)
	}
}

func (s *Session) WaitDataChannelOpen(ctx context.Context, timeout time.Duration) (*webrtc.DataChannel, error) {
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-s.dcOpenCh:
		s.mu.Lock()
		dc := s.dc
		s.mu.Unlock()
		if dc == nil {
			return nil, errors.New("data channel was reset")
		}
		return dc, nil
	case <-tctx.Done():
		return nil, tctx.Err()
	}
}

// recvLoop - recvCh에서 꺼내서 onData 호출.
func (s *Session) recvLoop() {
	for b := range s.recvCh {
		if s.onData != nil {
			s.onData(b.data[:b.len])
		}
		globalRecvPool.put(b)
	}
}

// Send - DataChannel로 데이터 전송.
// SCTP 버퍼가 dcBufferHigh를 초과하면 OnBufferedAmountLow 콜백까지 블로킹.
// TCP Write와 동일하게 버퍼가 빠질 때까지 대기 — 타임아웃으로 세션을 끊지 않음.
// DataChannel이 실제로 닫혔을 때만 에러 반환.
func (s *Session) Send(data []byte) error {
	s.mu.Lock()
	dc := s.dc
	s.mu.Unlock()
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return errors.New("data channel not open")
	}

	for dc.BufferedAmount() > dcBufferHigh {
		s.pauseMu.Lock()
		s.sendPaused = true
		resumeCh := s.resumeCh
		s.pauseMu.Unlock()

		// 버퍼가 빠질 때까지 대기 — TCP Write 블로킹과 동일한 동작.
		// OnClose에서도 resumeCh를 닫으므로 DataChannel 종료 시 여기서 탈출.
		<-resumeCh

		if dc.ReadyState() != webrtc.DataChannelStateOpen {
			return errors.New("data channel closed while waiting")
		}
	}

	return dc.Send(data)
}

func (s *Session) Close() {
	s.mu.Lock()
	dc := s.dc
	pc := s.pc
	s.dc = nil
	s.pc = nil
	s.mu.Unlock()
	if dc != nil {
		_ = dc.Close()
	}
	if pc != nil {
		_ = pc.Close()
	}
	s.recvOnce.Do(func() { close(s.recvCh) })
}