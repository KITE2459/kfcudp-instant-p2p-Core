package kcpbridge

// pipe.go - KCP ↔ TCP 브리지.

import (
	"io"
	"net"
	"sync/atomic"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

// idleTimeout - KCP 무통신 타임아웃.
// MC 클라이언트 종료 시 UDP(KCP)는 FIN 없음 → 무통신으로 감지.
// 1초는 너무 짧아 정상 연결도 끊길 수 있음 → 10초.
const idleTimeout = 1 * time.Second

// lastSeen 아레나 — 세션마다 힙 할당 제거.
const lastSeenPoolSize = maxSessions * 2

type lastSeen struct {
	t atomic.Int64 // UnixNano
}

var _lastSeenArena [lastSeenPoolSize]lastSeen
var lastSeenPool chan *lastSeen

func init() {
	lastSeenPool = make(chan *lastSeen, lastSeenPoolSize)
	for i := range _lastSeenArena {
		lastSeenPool <- &_lastSeenArena[i]
	}
}

func getLastSeen() *lastSeen {
	select {
	case ls := <-lastSeenPool:
		ls.t.Store(time.Now().UnixNano())
		return ls
	default:
		ls := &lastSeen{}
		ls.t.Store(time.Now().UnixNano())
		return ls
	}
}

func putLastSeen(ls *lastSeen) {
	select {
	case lastSeenPool <- ls:
	default:
	}
}

func (ls *lastSeen) touch() {
	ls.t.Store(time.Now().UnixNano())
}

func (ls *lastSeen) idle() bool {
	return time.Since(time.Unix(0, ls.t.Load())) > idleTimeout
}

// joinConnGeneric - KCP ↔ TCP 양방향 복사.
// KCP 방향 idleTimeout 감지 → 고스트 방지.
func joinConnGeneric(kcpConn net.Conn, tcpConn net.Conn) {
	done := getDoneChan()
	kcpSeen := getLastSeen()
	stop := getDoneChan()

	// KCP → TCP
	go func() {
		buf := getCopyBuf()
		defer putCopyBuf(buf)
		for {
			n, err := kcpConn.Read(buf)
			if n > 0 {
				kcpSeen.touch()
				if _, werr := tcpConn.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		if tc, ok := tcpConn.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		} else {
			tcpConn.Close()
		}
		kcpConn.Close()
		done <- struct{}{}
	}()

	// TCP → KCP
	go func() {
		buf := getCopyBuf()
		_, _ = io.CopyBuffer(kcpConn, tcpConn, buf)
		putCopyBuf(buf)
		tcpConn.Close()
		kcpConn.Close()
		done <- struct{}{}
	}()

	// watchdog - KCP 방향 무통신 감지
	// idleTimeout/2 마다 체크 — 100명 환경에서 ticker 부하 최소화
	go func() {
		ticker := time.NewTicker(idleTimeout / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if kcpSeen.idle() {
					kcpConn.Close()
					tcpConn.Close()
					return
				}
			case <-stop:
				return
			}
		}
	}()

	<-done
	kcpConn.Close()
	tcpConn.Close()
	<-done
	stop <- struct{}{}
	putDoneChan(stop)
	putLastSeen(kcpSeen)
	putDoneChan(done)
}

// joinKCPtoKCP - 사용되지 않음 (udp 포워딩 방식으로 대체).
// 하위 호환을 위해 유지.
func joinKCPtoKCP(a, b *kcp.UDPSession) {
	done := getDoneChan()
	seen := getLastSeen()
	stop := getDoneChan()

	go func() {
		buf := getCopyBuf()
		defer putCopyBuf(buf)
		for {
			n, err := a.Read(buf)
			if n > 0 {
				seen.touch()
				if _, werr := b.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		a.Close()
		b.Close()
		done <- struct{}{}
	}()

	go func() {
		buf := getCopyBuf()
		_, _ = io.CopyBuffer(a, b, buf)
		putCopyBuf(buf)
		a.Close()
		b.Close()
		done <- struct{}{}
	}()

	go func() {
		ticker := time.NewTicker(idleTimeout / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if seen.idle() {
					a.Close()
					b.Close()
					return
				}
			case <-stop:
				return
			}
		}
	}()

	<-done
	a.Close()
	b.Close()
	<-done
	stop <- struct{}{}
	putDoneChan(stop)
	putLastSeen(seen)
	putDoneChan(done)
}