/*
 * OpenFriend — Minecraft Java Edition Friends List bridge.
 * Copyright (c) 2026 ZSHARE (https://zpw.jp). Licensed under the MIT License.
 */
package quicbridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"

	"github.com/quic-go/quic-go"
)

// maxTCPConnsPerHost - MC 서버에 동시에 맺을 최대 TCP 연결 수.
// 플레이어 1명 = 스트림 1개 = MC 서버 TCP 연결 1개이므로
// 수용할 최대 플레이어 수와 동일하게 설정.
// OS의 소켓 파일 디스크립터 한도(ulimit -n)를 고려해 넉넉히 잡음.
const maxTCPConnsPerHost = 200

// connSemaphore - MC 서버 TCP 연결 수를 maxTCPConnsPerHost 이하로 제한.
// 초과 시 대기가 아닌 즉시 거부 → 클라이언트에게 명시적 에러 반환.
type connSemaphore struct {
	ch chan struct{}
}

func newConnSemaphore(max int) *connSemaphore {
	ch := make(chan struct{}, max)
	for i := 0; i < max; i++ {
		ch <- struct{}{}
	}
	return &connSemaphore{ch: ch}
}

func (s *connSemaphore) acquire() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

func (s *connSemaphore) release() {
	s.ch <- struct{}{}
}

// hostState - Host 전역 상태.
// QUIC 리스너 하나가 모든 클라이언트 연결을 처리하며
// 세마포어로 MC 서버 TCP 연결 수를 제어.
type hostState struct {
	target string
	logger *slog.Logger
	sem    *connSemaphore

	// 활성 스트림 수 (로그/모니터링용)
	activeStreams atomic.Int64
}

func Host(ctx context.Context, listenAddr, target string, logger *slog.Logger) error {
	tlsCfg, err := ServerTLSConfig()
	if err != nil {
		return fmt.Errorf("TLS config: %w", err)
	}

	ln, err := quic.ListenAddr(listenAddr, tlsCfg, quicBackendServerConfig)
	if err != nil {
		return fmt.Errorf("QUIC listen: %w", err)
	}
	defer ln.Close()

	hs := &hostState{
		target: target,
		logger: logger,
		sem:    newConnSemaphore(maxTCPConnsPerHost),
	}

	logger.Info("[quic-host] listening", "addr", listenAddr, "target", target,
		"maxConns", maxTCPConnsPerHost)
	fmt.Printf("QUIC Host: listen=%s target=%s\n", listenAddr, target)

	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("[quic-host] accept failed", "err", err)
			continue
		}
		go hs.handleConn(ctx, conn)
	}
}

// handleConn - QUIC 연결 1개를 담당.
// 단일 QUIC 연결 위에서 여러 스트림을 멀티플렉싱하며,
// 스트림 하나 = 플레이어 MC 세션 하나.
func (hs *hostState) handleConn(ctx context.Context, conn *quic.Conn) {
	remote := conn.RemoteAddr().String()
	hs.logger.Info("[quic-host] client connected", "remote", remote)
	defer conn.CloseWithError(0, "bye")

	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				hs.logger.Debug("[quic-host] stream accept ended", "remote", remote, "err", err)
			}
			return
		}
		go hs.bridgeStream(stream, conn, remote)
	}
}

// bridgeStream - QUIC 스트림 ↔ MC 서버 TCP 연결 브리지.
// 세마포어로 동시 연결 수를 제한하고, 종료 시 반드시 반환.
func (hs *hostState) bridgeStream(stream *quic.Stream, conn *quic.Conn, remote string) {
	// 세마포어 획득 실패 = 연결 한도 초과 → 즉시 거부
	if !hs.sem.acquire() {
		hs.logger.Warn("[quic-host] TCP conn limit reached, rejecting stream",
			"remote", remote, "limit", maxTCPConnsPerHost)
		stream.CancelRead(0)
		_ = stream.Close()
		return
	}
	defer hs.sem.release()

	count := hs.activeStreams.Add(1)
	defer hs.activeStreams.Add(-1)
	hs.logger.Info("[quic-host] stream accepted", "remote", remote, "active", count)

	quicConn := wrapQuicStream(stream, conn)

	tcp, err := net.Dial("tcp", hs.target)
	if err != nil {
		hs.logger.Warn("[quic-host] dial target failed", "target", hs.target, "err", err)
		quicConn.Close()
		return
	}
	if tc, ok := tcp.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetWriteBuffer(tcpSockBufSize)
		_ = tc.SetReadBuffer(tcpSockBufSize)
	}

	joinConnGeneric(quicConn, tcp)
	hs.logger.Debug("[quic-host] stream closed", "remote", remote,
		"active", hs.activeStreams.Load())
}