package kcpbridge

import (
	"net"

	kcp "github.com/xtaci/kcp-go/v5"
)

// ────────────────────────────────────────────────────────
// 크기 상수
// ────────────────────────────────────────────────────────
const (
	copyBufSize    = 1024 * 1024      // 1MB
	udpSockBufSize = 32 * 1024 * 1024 // 32MB
	tcpSockBufSize = 512 * 1024       // 512KB
	maxShards      = 4
	maxSessions    = 200 // 동시 최대 세션 (maxTCPConnsPerHost와 동일)
	fwdBufSize     = 2048
	ctrlBufSize    = 5
)

// ────────────────────────────────────────────────────────
// 풀 크기 상수
// 이론적 최대 동시 사용량 기준으로 계산 → 소진 불가
// ────────────────────────────────────────────────────────
const (
	// copyBuf: maxSessions × 4방향(host 2 + reverse 2) = 800 → 1024
	copyBufPoolSize = 1024

	// fwdBuf: shards × burst
	fwdBufPoolSize = 4096

	// pktBuf: 새 유저 첫 패킷, maxSessions 이하
	pktBufPoolSize = 512

	// ctrlBuf: maxSessions × 2(send+recv) = 400 → 1024
	ctrlBufPoolSize = 1024

	// doneChan: maxSessions = 200 → 512
	doneChanPoolSize = 512
)

// ────────────────────────────────────────────────────────
// 아레나 — 패키지 초기화 시 1회 할당
// _copyBufArena: 1GB(1024×1MB)를 바이너리 BSS에 박으면 UPX가 거부하므로
//               런타임에 1회 할당. GC 대상이 아닌 전역 변수로 참조 유지.
// 나머지 소형 아레나: 바이너리 BSS에 정적 선언.
// ────────────────────────────────────────────────────────

// copyBuf: 런타임 1회 할당 (UPX mem_size 제한 회피)
var _copyBufArena [][]byte // init()에서 할당

type fwdBuf struct{ data [fwdBufSize]byte }

var _fwdBufArena [fwdBufPoolSize]fwdBuf

type pktBuf struct {
	data [fwdBufSize]byte
	n    int
}

var _pktBufArena [pktBufPoolSize]pktBuf

type ctrlBuf struct{ data [ctrlBufSize]byte }

var _ctrlBufArena [ctrlBufPoolSize]ctrlBuf

var _doneChanArena [doneChanPoolSize]chan struct{}

// ────────────────────────────────────────────────────────
// 채널 풀 변수
// ────────────────────────────────────────────────────────

var copyBufPool  chan []byte
var fwdBufPool   chan *fwdBuf
var pktBufPool   chan *pktBuf
var ctrlBufPool  chan *ctrlBuf
var doneChanPool chan chan struct{}

// connSemaphore — 정적 선언 (newConnSemaphore() make 제거)
// host.go의 maxTCPConnsPerHost(=maxSessions)개를 시작 시 채움
var _connSemCh [maxSessions]struct{}
var _globalConnSem connSemaphore

type connSemaphore struct{ ch chan struct{} }

func (s *connSemaphore) acquire() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

func (s *connSemaphore) release() { s.ch <- struct{}{} }

func init() {
	// copyBuf 풀 — 런타임 1회 할당 (전역 참조로 GC 대상 아님)
	_copyBufArena = make([][]byte, copyBufPoolSize)
	for i := range _copyBufArena {
		_copyBufArena[i] = make([]byte, copyBufSize)
	}
	copyBufPool = make(chan []byte, copyBufPoolSize)
	for i := range _copyBufArena {
		copyBufPool <- _copyBufArena[i]
	}

	// fwdBuf 풀
	fwdBufPool = make(chan *fwdBuf, fwdBufPoolSize)
	for i := range _fwdBufArena {
		fwdBufPool <- &_fwdBufArena[i]
	}

	// pktBuf 풀
	pktBufPool = make(chan *pktBuf, pktBufPoolSize)
	for i := range _pktBufArena {
		pktBufPool <- &_pktBufArena[i]
	}

	// ctrlBuf 풀
	ctrlBufPool = make(chan *ctrlBuf, ctrlBufPoolSize)
	for i := range _ctrlBufArena {
		ctrlBufPool <- &_ctrlBufArena[i]
	}

	// done 채널 풀
	doneChanPool = make(chan chan struct{}, doneChanPoolSize)
	for i := range _doneChanArena {
		_doneChanArena[i] = make(chan struct{}, 2)
		doneChanPool <- _doneChanArena[i]
	}

	// connSemaphore 정적 초기화
	_globalConnSem.ch = make(chan struct{}, maxSessions)
	for range _connSemCh {
		_globalConnSem.ch <- struct{}{}
	}

}

// newConnSemaphore — 정적 인스턴스 반환, make 없음
func newConnSemaphore(_ int) *connSemaphore {
	return &_globalConnSem
}

// ────────────────────────────────────────────────────────
// 풀 접근 함수 — 비블로킹, 폴백 없음
// 풀 크기를 이론적 최대치로 잡았으므로 정상 운영에서 소진 불가
// ────────────────────────────────────────────────────────

func getCopyBuf() []byte {
	select {
	case b := <-copyBufPool:
		return b
	default:
		// 절대 도달 불가 (copyBufPoolSize = maxSessions × 4 × 여유)
		// 만약 도달한다면 설계 오류 — 빈 슬라이스 반환으로 CopyBuffer가 자체 할당
		return make([]byte, copyBufSize)
	}
}

func putCopyBuf(b []byte) {
	if cap(b) != copyBufSize {
		return
	}
	select {
	case copyBufPool <- b:
	default:
	}
}

func getFwdBuf() *fwdBuf {
	select {
	case b := <-fwdBufPool:
		return b
	default:
		return &fwdBuf{}
	}
}

func putFwdBuf(b *fwdBuf) {
	select {
	case fwdBufPool <- b:
	default:
	}
}

func getPktBuf() *pktBuf {
	select {
	case b := <-pktBufPool:
		return b
	default:
		return &pktBuf{}
	}
}

func putPktBuf(b *pktBuf) {
	select {
	case pktBufPool <- b:
	default:
	}
}

func getCtrlBuf() *ctrlBuf {
	select {
	case b := <-ctrlBufPool:
		return b
	default:
		return &ctrlBuf{}
	}
}

func putCtrlBuf(b *ctrlBuf) {
	select {
	case ctrlBufPool <- b:
	default:
	}
}

func getDoneChan() chan struct{} {
	select {
	case ch := <-doneChanPool:
		return ch
	default:
		return make(chan struct{}, 2)
	}
}

func putDoneChan(ch chan struct{}) {
	for len(ch) > 0 {
		<-ch
	}
	select {
	case doneChanPool <- ch:
	default:
	}
}

// ────────────────────────────────────────────────────────
// KCP / TCP 옵션
// ────────────────────────────────────────────────────────

// applyKCPOptions - 클라이언트 facing KCP 설정.
// RTT 10ms, 1MB/s × 100명 기준.
// interval=2ms: RTT/4 이하.
// resend=1: ACK 1번 누락 즉시 재전송.
// nc=1: 혼잡제어 OFF — 고정 처리량 환경.
// WindowSize=2048: 100명 in-flight 800패킷 × 여유.
func applyKCPOptions(conn *kcp.UDPSession) {
	conn.SetStreamMode(true)
	conn.SetWriteDelay(false)
	conn.SetNoDelay(1, 2, 1, 0)
	conn.SetMtu(1450)
	conn.SetWindowSize(20480, 20480)
	conn.SetACKNoDelay(true)
	_ = conn.SetReadBuffer(udpSockBufSize)
	_ = conn.SetWriteBuffer(udpSockBufSize)
}

func setTCPOptions(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetReadBuffer(tcpSockBufSize)
		_ = tc.SetWriteBuffer(tcpSockBufSize)
	}
}

// ────────────────────────────────────────────────────────
// 브리지 함수
// ────────────────────────────────────────────────────────

// applyBackendKCPOptions - 오라클↔백엔드 facing KCP 설정.
// RTT 5ms, 100명 합산 단일 세션 처리.
// interval=1ms: RTT/4 이하.
// WindowSize=2048: 100명 합산 370패킷 × 여유 4배.
func applyBackendKCPOptions(conn *kcp.UDPSession) {
	conn.SetStreamMode(true)
	conn.SetWriteDelay(false)
	conn.SetNoDelay(1, 2, 2, 1)
	conn.SetMtu(1450)
	conn.SetWindowSize(81920, 81920)
	conn.SetACKNoDelay(true)
	_ = conn.SetReadBuffer(udpSockBufSize)
	_ = conn.SetWriteBuffer(udpSockBufSize)
}

// joinConnGeneric, joinKCPtoKCP 는 pipe.go에 정의됨.