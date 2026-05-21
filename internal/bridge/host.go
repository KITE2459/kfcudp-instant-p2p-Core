package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"jp.zpw.openfriend/internal/signaling"
)

const (
	handshakeTimeout   = 10 * time.Second
	targetProbeTimeout = 1 * time.Second
)

var turnServers = []webrtc.ICEServer{
	{
		URLs:       []string{"turn:193.122.114.163:3478"},
		Username:   "minecraft",
		Credential: "minecraft",
	},
	{
		URLs: []string{"stun:193.122.114.163:3478"},
	},
}

// apiShardCount - webrtc.API 샤드 수.
// pion 내부 UDP 멀티플렉서(디스패처 goroutine)가 API 인스턴스당 독립적으로 동작.
// 100세션을 단일 API에 몰면 디스패처 1개가 전부 처리 → 병목.
// 샤드 수만큼 병렬 처리되며, CPU 코어 수와 맞추는 게 이상적.
// 8로 고정: 코어 수가 더 많아도 pion 내부 락 경합으로 그 이상 효과가 선형이 아님.
const apiShardCount = 8

type HostManager struct {
	sig      *signaling.Client
	target   string
	useProxy bool
	logger   *slog.Logger

	// API 샤드 - sid 해시로 세션을 분산해 pion 디스패처 병렬화
	apiShards [apiShardCount]*webrtc.API

	mu         sync.Mutex
	sessions   map[string]*hostSession
	pendingIPs map[string]string // sid → clientIp
}

// pickAPI - sid 의 첫 바이트를 샤드 인덱스로 사용.
// sid는 UUID 계열이므로 첫 바이트가 충분히 균등하게 분포함.
func (p *HostManager) pickAPI(sid string) *webrtc.API {
	if sid == "" {
		return p.apiShards[0]
	}
	return p.apiShards[sid[0]%apiShardCount]
}

func NewHostManager(sig *signaling.Client, target string, useProxy bool, logger *slog.Logger) *HostManager {
	if logger == nil {
		logger = slog.Default()
	}

	// 공통 SettingEngine (NetworkType 설정만, 샤드 간 공유해도 무방)
	se := webrtc.SettingEngine{}
	se.SetNetworkTypes([]webrtc.NetworkType{
		webrtc.NetworkTypeUDP4,
		webrtc.NetworkTypeUDP6,
	})

	hm := &HostManager{
		sig:        sig,
		target:     target,
		useProxy:   useProxy,
		logger:     logger,
		sessions:   map[string]*hostSession{},
		pendingIPs: map[string]string{},
	}
	// 각 샤드마다 독립적인 webrtc.API 생성 → 독립적인 UDP 멀티플렉서
	for i := range hm.apiShards {
		hm.apiShards[i] = webrtc.NewAPI(webrtc.WithSettingEngine(se))
	}
	return hm
}

func (p *HostManager) OnMessage(msg map[string]any) {
	typ, _ := msg["type"].(string)
	sid := signaling.GetSessionID(msg)

	switch typ {
	case "HOST_ACK":
		p.logger.Info("[host] registered on signaling server")
	case "JOIN_REQUEST":
		clientIp, _ := msg["clientIp"].(string)
		p.logger.Info("[host] JOIN_REQUEST", "sid", sid, "ip", clientIp)
		// IP를 나중에 OFFER 처리 시 사용하기 위해 저장
		p.mu.Lock()
		p.pendingIPs[sid] = clientIp
		p.mu.Unlock()
		go func() {
			if ProbeTCP(p.target, targetProbeTimeout) {
				p.logger.Info("[host] target reachable; accepting", "sid", sid)
				_ = p.sig.Send(signaling.JoinAccepted(sid))
			} else {
				p.logger.Warn("[host] target unreachable; rejecting", "target", p.target, "sid", sid)
				_ = p.sig.Send(signaling.JoinRejected(sid))
			}
		}()
	case "OFFER":
		p.handleOffer(sid, signaling.GetSDP(msg))
	case "ICE_CANDIDATE":
		p.handleICE(sid, msg)
	}
}

func (p *HostManager) handleOffer(sid, sdp string) {
	if sdp == "" {
		p.logger.Warn("[host] OFFER without sdp")
		return
	}
	p.logger.Info("[host] OFFER received", "sid", sid)

	p.mu.Lock()
	clientIp := p.pendingIPs[sid]
	delete(p.pendingIPs, sid)
	p.mu.Unlock()

	go p.startSession(sid, sdp, clientIp)
}

func (p *HostManager) startSession(sid, offerSDP, clientIp string) {
	cfg := webrtc.Configuration{
		ICEServers:         turnServers,
		// ICETransportPolicy: webrtc.ICETransportPolicyRelay,
	}

	ps := &hostSession{
		sid:      sid,
		pm:       p,
		target:   p.target,
		clientIp: clientIp,
		useProxy: p.useProxy,
		feedCh:   make(chan []byte, 16384),
	}
	// feedCh 소비 goroutine - DataChannel→TCP 방향.
	// Coalescing: 연속된 청크를 net.Buffers(writev)로 묶어 단일 syscall로 기록.
	// tcp가 초기화되기 전(첫 패킷)에는 기존 onPeerData 경로로 폴백.
	go func() {
		cb := getCoalesceBuf()
		defer putCoalesceBuf(cb)

		for first := range ps.feedCh {
			ps.mu.Lock()
			tcp := ps.tcp
			ps.mu.Unlock()

			// TCP 연결이 아직 없으면(첫 패킷) 기존 경로로 처리
			if tcp == nil {
				ps.onPeerData(first)
				continue
			}

			total := len(first)
			if total > coalesceMaxBytes {
				// 단일 청크가 상한 초과 시 그냥 직접 기록
				_ = tcp.Feed(first)
				continue
			}
			copy(cb.data[:], first)

			// 논블로킹으로 추가 데이터 흡수
			for i := 1; i < coalesceBatchMax; i++ {
				select {
				case next, ok := <-ps.feedCh:
					if !ok {
						goto feedSend
					}
					if total+len(next) > coalesceMaxBytes {
						// 크기 초과 → 지금 것 먼저 쓰고 next는 다음 round로
						_ = tcp.Feed(cb.data[:total])
						total = len(next)
						copy(cb.data[:], next)
						i = coalesceBatchMax
						continue
					}
					copy(cb.data[total:], next)
					total += len(next)
				default:
					i = coalesceBatchMax
				}
			}

		feedSend:
			_ = tcp.Feed(cb.data[:total])
		}
	}()
	p.mu.Lock()
	if prev, ok := p.sessions[sid]; ok {
		prev.close()
	}
	p.sessions[sid] = ps
	p.mu.Unlock()

	rtc, err := NewSession(p.pickAPI(sid), cfg, RoleAcceptor, sid, [16]byte{},
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
			_ = p.sig.Send(signaling.IceCandidate(sid, init.Candidate, mid, idx))
		},
		func(data []byte) {
			// pion 내부 goroutine 블로킹 방지
			// 채널로 전달해서 순서 보장하면서 비동기 처리
			// MC는 TCP 스트림 - 드롭하면 안 됨, 블로킹으로 처리
			ps.feedCh <- data
		},
		func() {
			p.logger.Info("[host] DataChannel closed", "sid", sid)
			ps.close()
			p.removeSession(sid, ps)
		},
		p.logger,
	)
	if err != nil {
		p.logger.Warn("[host] New WebRTC session failed", "err", err)
		return
	}
	ps.rtc = rtc

	answerSDP, err := rtc.HandleOffer(offerSDP)
	if err != nil {
		p.logger.Warn("[host] HandleOffer failed", "err", err)
		ps.close()
		return
	}
	if err := p.sig.Send(signaling.Answer(sid, answerSDP)); err != nil {
		p.logger.Warn("[host] Send ANSWER failed", "err", err)
		ps.close()
		return
	}
	p.logger.Info("[host] ANSWER sent", "sid", sid)

	go func() {
		dc, err := rtc.WaitDataChannelOpen(context.Background(), handshakeTimeout)
		if err != nil {
			p.logger.Warn("[host] handshake timeout", "sid", sid, "err", err)
			ps.close()
			p.removeSession(sid, ps)
			return
		}
		_ = dc
		p.logger.Info("[host] DataChannel open; waiting for first data", "sid", sid, "clientIp", clientIp)
	}()
}

func (p *HostManager) handleICE(sid string, msg map[string]any) {
	ic, ok := signaling.ParseIceCandidate(msg)
	if !ok {
		return
	}
	p.mu.Lock()
	ps, ok := p.sessions[sid]
	p.mu.Unlock()
	if !ok || ps.rtc == nil {
		return
	}
	mid := ic.SdpMid
	idx := uint16(ic.SdpMLineIndex)
	ps.rtc.AddRemoteICE(webrtc.ICECandidateInit{
		Candidate:     ic.Candidate,
		SDPMid:        &mid,
		SDPMLineIndex: &idx,
	})
}

func (p *HostManager) removeSession(sid string, ps *hostSession) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.sessions[sid]; ok && cur == ps {
		delete(p.sessions, sid)
	}
}

func (p *HostManager) Close() {
	p.mu.Lock()
	all := make([]*hostSession, 0, len(p.sessions))
	for _, ps := range p.sessions {
		all = append(all, ps)
	}
	p.sessions = map[string]*hostSession{}
	p.mu.Unlock()
	for _, ps := range all {
		ps.close()
	}
}

type hostSession struct {
	sid      string
	pm       *HostManager
	target   string
	clientIp string
	useProxy bool

	mu       sync.Mutex
	rtc      *Session
	tcp      *TCPBridge
	closed   bool
	tcpOnce  sync.Once

	// 서버→클라 송신 채널 (readLoop 블로킹 방지)
	downCh   chan *chunkBuf
	// 클라→서버 Feed 채널 (pion goroutine 블로킹 방지, 순서 보장)
	feedCh   chan []byte
}

func (ps *hostSession) onPeerData(data []byte) {
	ps.mu.Lock()
	tcp := ps.tcp
	closed := ps.closed
	ps.mu.Unlock()

	if closed {
		return
	}

	if tcp == nil {
		ps.tcpOnce.Do(func() {
			ps.pm.logger.Info("[host] first data received; dialing target",
				"target", ps.target, "clientIp", ps.clientIp)
			// 송신 채널 초기화 및 goroutine 시작
				ps.downCh = make(chan *chunkBuf, 16384)
				go func() {
					cb := getCoalesceBuf()
					defer putCoalesceBuf(cb)

					for first := range ps.downCh {
						ps.mu.Lock()
						rtc := ps.rtc
						ps.mu.Unlock()
						if rtc == nil {
							globalChunkPool.put(first)
							continue
						}

						total := first.len
						copy(cb.data[:], first.data[:first.len])
						globalChunkPool.put(first)

						// 논블로킹으로 추가 청크 흡수 → DataChannel.Send() 횟수 감소
						for i := 1; i < coalesceBatchMax; i++ {
							select {
							case next, ok := <-ps.downCh:
								if !ok {
									goto hostSend
								}
								if total+next.len > coalesceMaxBytes {
									_ = rtc.Send(cb.data[:total])
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

					hostSend:
						_ = rtc.Send(cb.data[:total])
					}
				}()

				t, err := DialTCP(ps.target,
				func(downstream []byte) {
					cb := globalChunkPool.get()
					cb.len = copy(cb.data[:], downstream)
					ps.downCh <- cb
				},
				func() {
					ps.close()
					ps.pm.removeSession(ps.sid, ps)
				},
				ps.pm.logger,
			)
			if err != nil {
				ps.pm.logger.Warn("[host] Failed to dial target", "target", ps.target, "err", err)
				ps.close()
				return
			}

			// PROXY protocol v1 헤더 전송 (useProxy가 true일 때만)
			if ps.useProxy && ps.clientIp != "" {
				proxyHeader := buildProxyHeader(ps.clientIp, ps.target)
				if proxyHeader != "" {
					if err := t.Feed([]byte(proxyHeader)); err != nil {
						ps.pm.logger.Warn("[host] Failed to send PROXY header", "err", err)
						ps.close()
						t.Close()
						return
					}
					ps.pm.logger.Info("[host] PROXY header sent", "clientIp", ps.clientIp)
				}
			}

			ps.mu.Lock()
			ps.tcp = t
			tcp = t
			ps.mu.Unlock()

			if err := t.Feed(data); err != nil {
				ps.pm.logger.Warn("[host] TCP write failed on first data", "err", err)
				ps.close()
			}
		})
		return
	}

	if err := tcp.Feed(data); err != nil {
		ps.pm.logger.Warn("[host] TCP write failed", "err", err)
		ps.close()
	}
}

// buildProxyHeader - PROXY protocol v1 헤더 생성
// IPv4-mapped IPv6 (::ffff:1.2.3.4) 도 IPv4로 정규화
func buildProxyHeader(clientIp, target string) string {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return ""
	}

	// IPv4-mapped IPv6 주소 정규화
	ip := net.ParseIP(clientIp)
	if ip == nil {
		return ""
	}
	proto := "TCP4"
	if ip4 := ip.To4(); ip4 != nil {
		clientIp = ip4.String()
		proto = "TCP4"
	} else {
		clientIp = ip.String()
		proto = "TCP6"
	}

	// 클라 포트는 알 수 없으므로 0으로 설정
	return fmt.Sprintf("PROXY %s %s %s 0 %s\r\n", proto, clientIp, host, port)
}

func (ps *hostSession) close() {
	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return
	}
	ps.closed = true
	tcp := ps.tcp
	rtc := ps.rtc
	downCh := ps.downCh
	feedCh := ps.feedCh
	ps.tcp = nil
	ps.rtc = nil
	ps.downCh = nil
	ps.feedCh = nil
	ps.mu.Unlock()
	if downCh != nil {
		close(downCh)
	}
	if feedCh != nil {
		close(feedCh)
	}
	if tcp != nil {
		tcp.Close()
	}
	if rtc != nil {
		rtc.Close()
	}
}

func ParseTarget(s string) (string, error) {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return "", err
	}
	if _, err := strconv.Atoi(port); err != nil {
		return "", err
	}
	return net.JoinHostPort(host, port), nil
}