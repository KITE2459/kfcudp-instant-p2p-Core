package kcpbridge

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

func resolveSRV(addr string) string {
	if strings.Contains(addr, ":") {
		return addr
	}
	_, srvs, err := net.LookupSRV("minecraft", "tcp", addr)
	if err == nil && len(srvs) > 0 {
		host := strings.TrimSuffix(srvs[0].Target, ".")
		return fmt.Sprintf("%s:%d", host, srvs[0].Port)
	}
	return addr + ":25565"
}

// sharedKCPConn - QUIC의 sharedQUICConn에 대응하는 KCP 재연결 관리자.
//
// QUIC은 연결 1개 + 스트림 N개 멀티플렉싱.
// KCP는 연결 = 세션 1:1이므로 멀티플렉싱 불가.
// 대신 연결 상태를 추적하고 끊기면 자동 재연결.
// 각 MC 클라이언트는 독립적인 KCP 연결을 맺음.
//
// QUIC 대비 차이:
//   - openStream() 대신 newConn() — 매번 새 KCP 연결
//   - 공유 연결 없음 — 핸드셰이크 비용 있음 (단, TLS 없어서 QUIC보다 빠름)
type sharedKCPConn struct {
	serverAddr  string
	logger      *slog.Logger
	activeConns atomic.Int64

	// 마지막 성공한 연결 상태 — 재연결 백오프 리셋용
	mu          sync.Mutex
	lastSuccess time.Time
}

func newSharedKCPConn(serverAddr string, logger *slog.Logger) *sharedKCPConn {
	return &sharedKCPConn{
		serverAddr: serverAddr,
		logger:     logger,
	}
}

// newConn - KCP 연결을 새로 수립.
// 실패 시 지수 백오프로 재시도 (50ms 시작, 최대 10초, 총 30초).
// QUIC openStream()의 재연결 로직과 동일한 구조.
func (s *sharedKCPConn) newConn(ctx context.Context) (*kcp.UDPSession, error) {
	const totalTimeout = 30 * time.Second
	deadline := time.Now().Add(totalTimeout)
	backoff := 50 * time.Millisecond

	for {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("KCP connect timeout after %s", totalTimeout)
		}

		conn, err := kcp.DialWithOptions(s.serverAddr, nil, 0, 0)
		if err == nil {
			applyKCPOptions(conn)
			s.mu.Lock()
			s.lastSuccess = time.Now()
			s.mu.Unlock()
			return conn, nil
		}

		s.logger.Warn("[kcp-join] dial failed", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

func Join(ctx context.Context, listenAddr, serverAddr string, logger *slog.Logger) error {
	serverAddr = resolveSRV(serverAddr)
	logger.Info("[kcp-join] resolved server", "addr", serverAddr)

	shared := newSharedKCPConn(serverAddr, logger)

	// 초기 연결 확인 — 서버 미응답 시 조기 실패
	testConn, err := shared.newConn(ctx)
	if err != nil {
		return fmt.Errorf("initial KCP connect: %w", err)
	}
	testConn.Close()

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("local TCP listen: %w", err)
	}
	defer ln.Close()

	logger.Info("[kcp-join] listening", "listen", listenAddr, "server", serverAddr)
	fmt.Println("WEBRTC_READY")
	fmt.Printf("KCP Join: listen=%s server=%s\n", listenAddr, serverAddr)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		local, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("[kcp-join] accept failed", "err", err)
			return err
		}
		go handleLocal(ctx, local, shared, logger)
	}
}

// handleLocal - MC 클라이언트 1명을 처리.
// QUIC의 handleLocalShared와 동일한 역할.
// KCP 연결을 새로 맺어 MC 클라이언트와 브리지.
func handleLocal(ctx context.Context, local net.Conn, shared *sharedKCPConn, logger *slog.Logger) {
	defer local.Close()
	setTCPOptions(local)

	kcpConn, err := shared.newConn(ctx)
	if err != nil {
		logger.Warn("[kcp-join] failed to get KCP conn", "err", err)
		return
	}

	count := shared.activeConns.Add(1)
	logger.Info("[kcp-join] KCP connected",
		"server", shared.serverAddr,
		"client", local.RemoteAddr(),
		"active", count)

	joinConnGeneric(kcpConn, local)

	shared.activeConns.Add(-1)
	logger.Debug("[kcp-join] connection closed",
		"client", local.RemoteAddr(),
		"active", shared.activeConns.Load())
}