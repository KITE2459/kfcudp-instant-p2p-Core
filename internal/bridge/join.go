package bridge

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"jp.zpw.openfriend/internal/signaling"
)

type JoinManager struct {
	sig    *signaling.Client
	logger *slog.Logger

	// API 샤드 - sid 해시로 세션을 분산 (host와 동일한 구조)
	apiShards [apiShardCount]*webrtc.API

	listener net.Listener
	stopCh   chan struct{}
	doneCh   chan struct{}

	mu      sync.Mutex
	current *joinSession
}

// pickAPI - sid의 첫 바이트로 샤드 선택
func (j *JoinManager) pickAPI(sid string) *webrtc.API {
	if sid == "" {
		return j.apiShards[0]
	}
	return j.apiShards[sid[0]%apiShardCount]
}

func NewJoinManager(sig *signaling.Client, logger *slog.Logger) *JoinManager {
	if logger == nil {
		logger = slog.Default()
	}
	se := webrtc.SettingEngine{}
	se.SetNetworkTypes([]webrtc.NetworkType{
		webrtc.NetworkTypeUDP4,
		webrtc.NetworkTypeUDP6,
	})

	jm := &JoinManager{
		sig:    sig,
		logger: logger,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	for i := range jm.apiShards {
		jm.apiShards[i] = webrtc.NewAPI(webrtc.WithSettingEngine(se))
	}
	return jm
}

func (j *JoinManager) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	j.listener = ln
	j.logger.Info("[join] listener up", "addr", ln.Addr().String())
	go j.acceptLoop()
	return nil
}

func (j *JoinManager) acceptLoop() {
	defer close(j.doneCh)
	for {
		conn, err := j.listener.Accept()
		if err != nil {
			select {
			case <-j.stopCh:
				return
			default:
			}
			j.logger.Warn("[join] accept failed", "err", err)
			return
		}
		go j.handleIncoming(conn)
	}
}

func (j *JoinManager) handleIncoming(local net.Conn) {
	j.mu.Lock()
	if j.current != nil {
		j.mu.Unlock()
		j.logger.Warn("[join] rejecting - session already in progress")
		_ = local.Close()
		return
	}
	sess := &joinSession{
		jm:       j,
		local:    local,
		acceptCh: make(chan struct{}),
		rejectCh: make(chan struct{}),
	}
	j.current = sess
	j.mu.Unlock()

	defer func() {
		j.mu.Lock()
		if j.current == sess {
			j.current = nil
		}
		j.mu.Unlock()
	}()

	j.logger.Info("[join] MC connected; sending JOIN", "addr", local.RemoteAddr())

	// 시그널링에 JOIN 전송
	if err := j.sig.Send(map[string]any{
		"type":      "JOIN",
		"roomId":    j.sig.RoomID(),
		"sessionId": j.sig.SessionID(),
	}); err != nil {
		j.logger.Warn("[join] send JOIN failed", "err", err)
		_ = local.Close()
		return
	}

	// JOIN_ACCEPTED 대기
	select {
	case <-sess.acceptCh:
		j.logger.Info("[join] host accepted")
	case <-sess.rejectCh:
		j.logger.Warn("[join] host rejected")
		_ = local.Close()
		return
	case <-time.After(60 * time.Second):
		j.logger.Warn("[join] timeout waiting for host")
		_ = local.Close()
		return
	}

	// WebRTC 협상 시작 (OFFER 전송만 하고 리턴)
	if err := sess.startInitiator(); err != nil {
		j.logger.Warn("[join] initiator failed", "err", err)
		sess.close()
		return
	}

	// DataChannel 열릴 때까지 대기 (ANSWER/ICE는 OnMessage에서 처리)
	dc, err := sess.rtc.WaitDataChannelOpen(context.Background(), 15*time.Second)
	if err != nil {
		j.logger.Warn("[join] DataChannel did not open", "err", err)
		sess.close()
		return
	}
	_ = dc
	j.logger.Info("[join] DataChannel open; bridging")
	sess.startTCPBridge()
}

// OnMessage - 시그널링 서버로부터 받은 메시지 처리
func (j *JoinManager) OnMessage(msg map[string]any) {
	typ, _ := msg["type"].(string)

	j.mu.Lock()
	sess := j.current
	j.mu.Unlock()

	switch typ {
	case "JOIN_ACCEPTED":
		j.logger.Info("[join] JOIN_ACCEPTED")
		if sess != nil {
			select {
			case <-sess.acceptCh:
			default:
				close(sess.acceptCh)
			}
		}
	case "JOIN_REJECTED":
		j.logger.Warn("[join] JOIN_REJECTED")
		if sess != nil {
			select {
			case <-sess.rejectCh:
			default:
				close(sess.rejectCh)
			}
		}
	case "ANSWER":
		// sess.rtc가 준비됐으면 바로 처리, 아니면 pending
		if sess != nil {
			sess.mu.Lock()
			rtc := sess.rtc
			sess.mu.Unlock()
			if rtc != nil {
				if err := rtc.HandleAnswer(signaling.GetSDP(msg)); err != nil {
					j.logger.Warn("[join] HandleAnswer failed", "err", err)
				}
			} else {
				sess.mu.Lock()
				sess.pendingAnswer = signaling.GetSDP(msg)
				sess.mu.Unlock()
			}
		}
	case "ICE_CANDIDATE":
		ic, ok := signaling.ParseIceCandidate(msg)
		if !ok {
			return
		}
		if sess != nil {
			sess.mu.Lock()
			rtc := sess.rtc
			if rtc == nil {
				sess.pendingICE = append(sess.pendingICE, ic)
				sess.mu.Unlock()
				return
			}
			sess.mu.Unlock()
			mid := ic.SdpMid
			idx := uint16(ic.SdpMLineIndex)
			rtc.AddRemoteICE(webrtc.ICECandidateInit{
				Candidate:     ic.Candidate,
				SDPMid:        &mid,
				SDPMLineIndex: &idx,
			})
		}
	case "HOST_DISCONNECTED":
		j.logger.Warn("[join] host disconnected")
		if sess != nil {
			sess.close()
		}
	}
}

func (j *JoinManager) Close() {
	close(j.stopCh)
	if j.listener != nil {
		_ = j.listener.Close()
	}
	select {
	case <-j.doneCh:
	default:
	}
	j.mu.Lock()
	sess := j.current
	j.current = nil
	j.mu.Unlock()
	if sess != nil {
		sess.close()
	}
}

// joinReadBuf - join TCP 읽기용 고정 버퍼
type joinReadBuf struct {
	data [65536]byte
}

var joinReadPool = func() chan *joinReadBuf {
	ch := make(chan *joinReadBuf, 512)
	for i := 0; i < 512; i++ {
		ch <- &joinReadBuf{}
	}
	return ch
}()

func getJoinReadBuf() *joinReadBuf {
	select {
	case b := <-joinReadPool:
		return b
	default:
		return &joinReadBuf{}
	}
}

func putJoinReadBuf(b *joinReadBuf) {
	select {
	case joinReadPool <- b:
	default:
	}
}

type joinSession struct {
	jm    *JoinManager
	local net.Conn
	rtc   *Session

	acceptCh chan struct{}
	rejectCh chan struct{}
	upCh     chan *chunkBuf // 클라→서버 송신 채널 (upstream)
	downCh   chan *chunkBuf // 서버→클라 수신 채널 (downstream), recvLoop 블로킹 방지

	once sync.Once
	mu   sync.Mutex

	pendingAnswer string
	pendingICE    []signaling.IceCandidatePayload
}

func (s *joinSession) startInitiator() error {
	cfg := webrtc.Configuration{
		ICEServers:         turnServers,
		// ICETransportPolicy: webrtc.ICETransportPolicyRelay,
	}
	sid := s.jm.sig.SessionID()

	rtc, err := NewSession(s.jm.pickAPI(sid), cfg, RoleInitiator, sid, [16]byte{},
		func(c *webrtc.ICECandidate) {
			init := c.ToJSON()
			mid := "0"
			if init.SDPMid != nil {
				mid = *init.SDPMid
			}
			idx := 0
			if init.SDPMLineIndex != nil {
				idx = int(*init.SDPMLineIndex)
			}
			_ = s.jm.sig.Send(signaling.IceCandidate(sid, init.Candidate, mid, idx))
		},
		func(data []byte) {
			// DataChannel 수신 → downCh 비동기 전달
			// recvLoop goroutine을 블로킹하지 않도록 채널로 분리
			// local.Write 블로킹이 pion 내부에 전파되는 것을 방지
			cb := globalChunkPool.get()
			cb.len = copy(cb.data[:], data)
			s.mu.Lock()
			downCh := s.downCh
			s.mu.Unlock()
			if downCh != nil {
				select {
				case downCh <- cb:
				default:
					// downCh 포화 = local TCP 쪽이 소비를 못 하는 상황
					// TCP 스트림이므로 드롭 불가 — 블로킹으로 전환
					downCh <- cb
				}
			} else {
				globalChunkPool.put(cb)
			}
		},
		func() {
			s.close()
		},
		s.jm.logger,
	)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.rtc = rtc
	pendingAns := s.pendingAnswer
	pendingICE := s.pendingICE
	s.pendingAnswer = ""
	s.pendingICE = nil
	s.mu.Unlock()

	offerSDP, err := rtc.CreateOffer()
	if err != nil {
		return err
	}
	if err := s.jm.sig.Send(signaling.Offer(sid, offerSDP)); err != nil {
		return err
	}
	s.jm.logger.Info("[join] OFFER sent", "sid", sid)

	// OFFER 전송 전에 도착한 pending ANSWER/ICE 처리
	if pendingAns != "" {
		s.jm.logger.Info("[join] applying pending ANSWER")
		if err := rtc.HandleAnswer(pendingAns); err != nil {
			return err
		}
		for _, ic := range pendingICE {
			mid := ic.SdpMid
			idx := uint16(ic.SdpMLineIndex)
			rtc.AddRemoteICE(webrtc.ICECandidateInit{
				Candidate:     ic.Candidate,
				SDPMid:        &mid,
				SDPMLineIndex: &idx,
			})
		}
	}

	return nil
}

func (s *joinSession) startTCPBridge() {
	// 수신 채널 초기화 (서버→클라: DataChannel → local TCP)
	// onData 콜백이 recvLoop goroutine 블로킹 없이 여기로 전달
	s.mu.Lock()
	s.downCh = make(chan *chunkBuf, 16384)
	downCh := s.downCh
	s.mu.Unlock()

	// 수신 goroutine - downCh에서 청크를 꺼내 local TCP에 배치 쓰기.
	// net.Buffers(writev) 로 최대 coalesceBatchMax 개를 단일 syscall로 기록.
	// local.Write 블로킹이 recvLoop/onData 경로에 전파되지 않음.
	go func() {
		batch := make([]*chunkBuf, 0, coalesceBatchMax)
		for first := range downCh {
			batch = append(batch[:0], first)
			total := first.len

			// 논블로킹으로 추가 청크 흡수
			for len(batch) < coalesceBatchMax {
				select {
				case next, ok := <-downCh:
					if !ok {
						goto flush
					}
					if total+next.len > coalesceMaxBytes {
						// 크기 초과 → 지금 batch 먼저 flush 후 next를 새 batch 시작
						s.mu.Lock()
						local := s.local
						s.mu.Unlock()
						if local == nil {
							for _, c := range batch {
								globalChunkPool.put(c)
							}
							globalChunkPool.put(next)
							return
						}
						if err := batchWrite(local, batch, tcpWriteTimeout); err != nil {
							for _, c := range batch {
								globalChunkPool.put(c)
							}
							globalChunkPool.put(next)
							if !errors.Is(err, net.ErrClosed) {
								s.jm.logger.Debug("[join] local write failed", "err", err)
							}
							s.close()
							return
						}
						for _, c := range batch {
							globalChunkPool.put(c)
						}
						batch = append(batch[:0], next)
						total = next.len
					} else {
						batch = append(batch, next)
						total += next.len
					}
				default:
					goto flush
				}
			}

		flush:
			s.mu.Lock()
			local := s.local
			s.mu.Unlock()
			if local == nil {
				for _, c := range batch {
					globalChunkPool.put(c)
				}
				return
			}
			if err := batchWrite(local, batch, tcpWriteTimeout); err != nil {
				for _, c := range batch {
					globalChunkPool.put(c)
				}
				if !errors.Is(err, net.ErrClosed) {
					s.jm.logger.Debug("[join] local write failed", "err", err)
				}
				s.close()
				return
			}
			for _, c := range batch {
				globalChunkPool.put(c)
			}
		}
	}()

	// 송신 채널 초기화 (클라→서버: local TCP → DataChannel)
	s.upCh = make(chan *chunkBuf, 16384)

	// 송신 goroutine - upCh에서 청크를 꺼내 DataChannel.Send() 호출.
	// Coalescing: 연속으로 쌓인 청크를 합쳐 DataChannel.Send() 횟수를 줄임.
	// rtc.Send() 블로킹이 TCP readLoop에 영향 없음.
	go func() {
		cb := getCoalesceBuf()
		defer putCoalesceBuf(cb)

		for first := range s.upCh {
			rtc := s.rtc
			if rtc == nil {
				globalChunkPool.put(first)
				continue
			}

			total := first.len
			copy(cb.data[:], first.data[:first.len])
			globalChunkPool.put(first)

			// 논블로킹으로 추가 청크 흡수
			for i := 1; i < coalesceBatchMax; i++ {
				select {
				case next, ok := <-s.upCh:
					if !ok {
						goto upSend
					}
					if total+next.len > coalesceMaxBytes {
						// 크기 초과 → 지금 것 전송 후 next로 새 round
						if err := rtc.Send(cb.data[:total]); err != nil {
							globalChunkPool.put(next)
							s.jm.logger.Warn("[join] send failed", "err", err)
							s.close()
							return
						}
						total = next.len
						copy(cb.data[:], next.data[:next.len])
						globalChunkPool.put(next)
						i = coalesceBatchMax
						continue
					}
					copy(cb.data[total:], next.data[:next.len])
					total += next.len
					globalChunkPool.put(next)
				default:
					i = coalesceBatchMax
				}
			}

		upSend:
			if err := rtc.Send(cb.data[:total]); err != nil {
				s.jm.logger.Warn("[join] send failed", "err", err)
				s.close()
				return
			}
		}
	}()

	// 읽기 루프 - 읽기만 담당
	rbuf := getJoinReadBuf()
	defer putJoinReadBuf(rbuf)
	buf := rbuf.data[:]
	for {
		n, err := s.local.Read(buf)
		if n > 0 {
			cb := globalChunkPool.get()
			cb.len = copy(cb.data[:], buf[:n])
			s.upCh <- cb
		}
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				s.jm.logger.Debug("[join] TCP ended", "err", err)
			}
			s.close()
			return
		}
	}
}

func (s *joinSession) close() {
	s.once.Do(func() {
		s.mu.Lock()
		local := s.local
		upCh := s.upCh
		downCh := s.downCh
		s.local = nil
		s.upCh = nil
		s.downCh = nil
		s.mu.Unlock()
		if downCh != nil {
			close(downCh)
		}
		if upCh != nil {
			close(upCh)
		}
		if local != nil {
			_ = local.Close()
		}
		if s.rtc != nil {
			s.rtc.Close()
		}
	})
}