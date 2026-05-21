package quicbridge

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	// copyBufSize - io.CopyBuffer 버퍼 크기.
	// QUIC은 내부적으로 스트림 단위로 분할하므로 크게 잡아도 무방.
	// 512KB → 1MB: 청크 전송이 많은 레이싱 서버에서 Read 횟수 절반으로 감소.
	copyBufSize = 1024 * 1024 // 1MB

	// tcpSockBufSize - 로컬 MC TCP 소켓 OS 버퍼.
	// 256KB → 512KB: 16명 burst 흡수.
	tcpSockBufSize = 512 * 1024 // 512KB

	// quicStreamWindow / quicConnWindow - QUIC 수신 윈도우.
	// 스트림 윈도우가 작으면 sender가 ACK 대기로 멈춤 (== 고무줄).
	// 레이싱 게임 + 청크 전송 고려해 스트림 64MB, 커넥션 512MB로 확대.
	quicStreamWindow = 64 * 1024 * 1024  // 64MB (기존 32MB)
	quicConnWindow   = 512 * 1024 * 1024 // 512MB (기존 256MB)
)

var quicBackendServerConfig = &quic.Config{
	MaxIncomingStreams: 4096,
	KeepAlivePeriod:   5 * time.Second,
	MaxIdleTimeout:    60 * time.Second,

	InitialStreamReceiveWindow:     quicStreamWindow,
	MaxStreamReceiveWindow:         quicStreamWindow,
	InitialConnectionReceiveWindow: quicConnWindow,
	MaxConnectionReceiveWindow:     quicConnWindow,

	// 경로 MTU 탐색 활성화: 패킷 단편화를 피해 단일 패킷에 더 많은 데이터를 실음.
	DisablePathMTUDiscovery: false,
}

var quicClientConfig = &quic.Config{
	KeepAlivePeriod: 5 * time.Second,
	MaxIdleTimeout:  60 * time.Second,

	InitialStreamReceiveWindow:     quicStreamWindow,
	MaxStreamReceiveWindow:         quicStreamWindow,
	InitialConnectionReceiveWindow: quicConnWindow,
	MaxConnectionReceiveWindow:     quicConnWindow,

	DisablePathMTUDiscovery: false,
}

type quicStreamConn struct {
	*quic.Stream
	conn *quic.Conn
}

func wrapQuicStream(s *quic.Stream, c *quic.Conn) net.Conn {
	return &quicStreamConn{Stream: s, conn: c}
}

func (c *quicStreamConn) LocalAddr() net.Addr {
	if c.conn != nil {
		return c.conn.LocalAddr()
	}
	return &net.TCPAddr{}
}

func (c *quicStreamConn) RemoteAddr() net.Addr {
	if c.conn != nil {
		return c.conn.RemoteAddr()
	}
	return &net.TCPAddr{}
}

func (c *quicStreamConn) Close() error {
	c.Stream.CancelRead(0)
	return c.Stream.Close()
}

func (c *quicStreamConn) SetDeadline(t time.Time) error      { return c.Stream.SetDeadline(t) }
func (c *quicStreamConn) SetReadDeadline(t time.Time) error  { return c.Stream.SetReadDeadline(t) }
func (c *quicStreamConn) SetWriteDeadline(t time.Time) error { return c.Stream.SetWriteDeadline(t) }

// copyBufPool - io.CopyBuffer 용 버퍼 풀.
// goroutine마다 신규 할당하면 GC 부담 — 풀에서 재사용.
var copyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, copyBufSize)
		return &b
	},
}

func getCopyBuf() []byte {
	return *copyBufPool.Get().(*[]byte)
}

func putCopyBuf(b []byte) {
	copyBufPool.Put(&b)
}

// joinConns - 양방향 복사 (a ↔ b).
func joinConns(a, b net.Conn) {
	joinConnGeneric(a, b)
}

// joinConnGeneric - QUIC 스트림 ↔ TCP 양방향 복사.
// io.CopyBuffer 기반으로 오버헤드 최소화.
// 한쪽이 끊기면 반대쪽도 즉시 Close해서 goroutine 고착 방지.
func joinConnGeneric(quicConn net.Conn, tcpConn net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	// QUIC → TCP
	go func() {
		defer wg.Done()
		buf := getCopyBuf()
		defer putCopyBuf(buf)
		_, _ = io.CopyBuffer(tcpConn, quicConn, buf)
		// 읽기 끝 → 반대쪽 Write 종료
		if tc, ok := tcpConn.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		} else {
			tcpConn.Close()
		}
		quicConn.Close()
	}()

	// TCP → QUIC
	go func() {
		defer wg.Done()
		buf := getCopyBuf()
		defer putCopyBuf(buf)
		_, _ = io.CopyBuffer(quicConn, tcpConn, buf)
		quicConn.Close()
		if tc, ok := tcpConn.(*net.TCPConn); ok {
			_ = tc.CloseRead()
		}
	}()

	wg.Wait()
	quicConn.Close()
	tcpConn.Close()
}