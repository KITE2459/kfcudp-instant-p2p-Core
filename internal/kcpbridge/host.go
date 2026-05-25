package kcpbridge

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"sync/atomic"

	kcp "github.com/xtaci/kcp-go/v5"
)

// maxTCPConnsPerHost - MC 서버에 동시에 맺을 최대 TCP 연결 수.
// common.go의 maxSessions와 동일값 — connSemaphore 정적 초기화에 사용.
const maxTCPConnsPerHost = maxSessions


type hostState struct {
	target       string
	logger       *slog.Logger
	sem          *connSemaphore
	activeConns  atomic.Int64
}

// Host - SO_REUSEPORT 멀티샤드 KCP 리스너.
// QUIC의 단일 리스너 구조와 달리 샤드별로 독립 goroutine + CPU 코어 사용.
// 각 샤드가 AcceptKCP 루프를 독립 실행 → 단일 goroutine 병목 제거.
func Host(ctx context.Context, listenAddr, target string, logger *slog.Logger) error {
	shards := runtime.NumCPU()
	if shards > maxShards {
		shards = maxShards
	}
	if shards < 1 {
		shards = 1
	}

	hs := &hostState{
		target: target,
		logger: logger,
		sem:    newConnSemaphore(maxTCPConnsPerHost),
	}

	logger.Info("[kcp-host] listening",
		"addr", listenAddr, "target", target,
		"shards", shards, "maxConns", maxTCPConnsPerHost)
	fmt.Printf("KCP Host: listen=%s target=%s shards=%d\n", listenAddr, target, shards)

	errCh := make(chan error, shards)
	for i := 0; i < shards; i++ {
		idx := i
		go func() { errCh <- runShard(ctx, idx, listenAddr, hs, logger) }()
	}

	for i := 0; i < shards; i++ {
		if err := <-errCh; err != nil && ctx.Err() == nil {
			return err
		}
	}
	return nil
}

func runShard(ctx context.Context, idx int, listenAddr string, hs *hostState, logger *slog.Logger) error {
	pconn, err := newReusePortConn(listenAddr)
	if err != nil {
		return fmt.Errorf("shard %d listen: %w", idx, err)
	}
	if uc, ok := pconn.(*net.UDPConn); ok {
		_ = uc.SetReadBuffer(udpSockBufSize)
		_ = uc.SetWriteBuffer(udpSockBufSize)
	}

	listener, err := kcp.ServeConn(nil, 0, 0, pconn)
	if err != nil {
		pconn.Close()
		return fmt.Errorf("shard %d ServeConn: %w", idx, err)
	}
	defer listener.Close()

	logger.Info("[kcp-host] shard ready", "shard", idx, "addr", listenAddr)

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.AcceptKCP()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("[kcp-host] accept failed", "shard", idx, "err", err)
			continue
		}
		applyKCPOptions(conn)
		go hs.handleConn(conn)
	}
}

func (hs *hostState) handleConn(kcpConn *kcp.UDPSession) {
	// 세마포어 획득 실패 = 연결 한도 초과 → 즉시 거부
	if !hs.sem.acquire() {
		hs.logger.Warn("[kcp-host] conn limit reached, rejecting",
			"remote", kcpConn.RemoteAddr(), "limit", maxTCPConnsPerHost)
		kcpConn.Close()
		return
	}
	defer hs.sem.release()

	count := hs.activeConns.Add(1)
	defer hs.activeConns.Add(-1)

	remote := kcpConn.RemoteAddr().String()
	hs.logger.Info("[kcp-host] connection accepted", "remote", remote, "active", count)

	tcp, err := net.Dial("tcp", hs.target)
	if err != nil {
		hs.logger.Warn("[kcp-host] dial target failed", "target", hs.target, "err", err)
		kcpConn.Close()
		return
	}
	setTCPOptions(tcp)

	joinConnGeneric(kcpConn, tcp)
	hs.logger.Debug("[kcp-host] connection closed",
		"remote", remote, "active", hs.activeConns.Load())
}