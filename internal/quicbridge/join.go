/*
 * OpenFriend — Minecraft Java Edition Friends List bridge.
 * Copyright (c) 2026 ZSHARE (https://zpw.jp). Licensed under the MIT License.
 */
package quicbridge

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
)

func resolveSRV(addr string) string {
	if strings.Contains(addr, ":") {
		return addr
	}
	_, srvs, err := net.LookupSRV("minecraft", "tcp", addr)
	if err == nil && len(srvs) > 0 {
		host := strings.TrimSuffix(srvs[0].Target, ".")
		port := srvs[0].Port
		return fmt.Sprintf("%s:%d", host, port)
	}
	return addr + ":25565"
}

// sharedQUICConn - 여러 로컬 TCP 연결이 공유하는 단일 QUIC 연결.
// QUIC 연결 1개를 유지하고 스트림만 새로 열어 핸드셰이크 비용을 없앰.
// 연결이 끊기면 openStream 호출 시점에 재연결 — 백그라운드 감시 goroutine 없음.
type sharedQUICConn struct {
	serverAddr string
	logger     *slog.Logger

	mu           sync.Mutex
	conn         *quic.Conn
	activeStreams atomic.Int64
}

func newSharedQUICConn(serverAddr string, logger *slog.Logger) *sharedQUICConn {
	return &sharedQUICConn{
		serverAddr: serverAddr,
		logger:     logger,
	}
}

// dial - QUIC 연결을 수립하고 내부에 저장.
func (s *sharedQUICConn) dial(ctx context.Context) error {
	conn, err := quic.DialAddr(ctx, s.serverAddr, ClientTLSConfig(), quicClientConfig)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	s.logger.Info("[quic-join] QUIC connection established", "server", s.serverAddr)
	return nil
}

// openStream - 공유 QUIC 연결에서 스트림을 엽니다.
// 연결이 없거나 끊겼으면 재연결 후 스트림을 엽니다.
// 재연결 실패 시 지수 백오프로 재시도 (최대 10초 간격, 총 30초).
func (s *sharedQUICConn) openStream(ctx context.Context) (*quic.Stream, *quic.Conn, error) {
	const totalTimeout = 30 * time.Second
	deadline := time.Now().Add(totalTimeout)
	backoff := 500 * time.Millisecond

	for {
		if time.Now().After(deadline) {
			return nil, nil, fmt.Errorf("QUIC connect timeout after %s", totalTimeout)
		}

		s.mu.Lock()
		conn := s.conn
		s.mu.Unlock()

		// 연결이 살아있으면 스트림 시도
		if conn != nil {
			stream, err := conn.OpenStreamSync(ctx)
			if err == nil {
				return stream, conn, nil
			}
			// 스트림 실패 = 연결 만료 → 재연결
			s.logger.Debug("[quic-join] stream open failed, reconnecting", "err", err)
			s.mu.Lock()
			if s.conn == conn {
				s.conn = nil
			}
			s.mu.Unlock()
		}

		// 재연결 시도
		s.logger.Info("[quic-join] reconnecting", "server", s.serverAddr)
		if err := s.dial(ctx); err != nil {
			s.logger.Warn("[quic-join] reconnect failed", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < 10*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 500 * time.Millisecond
	}
}

func Join(ctx context.Context, listenAddr, serverAddr string, logger *slog.Logger) error {
	serverAddr = resolveSRV(serverAddr)
	logger.Info("[quic-join] resolved server address", "addr", serverAddr)

	shared := newSharedQUICConn(serverAddr, logger)
	if err := shared.dial(ctx); err != nil {
		return fmt.Errorf("initial QUIC connect: %w", err)
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("local listen: %w", err)
	}
	defer ln.Close()

	logger.Info("[quic-join] listening", "listen", listenAddr, "server", serverAddr)
	fmt.Println("WEBRTC_READY")
	fmt.Printf("QUIC Join: listen=%s server=%s\n", listenAddr, serverAddr)

	for {
		local, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("[quic-join] accept failed", "err", err)
			return err
		}
		go handleLocalShared(ctx, local, shared, logger)
	}
}

// handleLocalShared - 로컬 MC 클라이언트 1명을 처리.
// 공유 QUIC 연결에서 스트림만 열어 사용 — DTLS 핸드셰이크 없음.
func handleLocalShared(ctx context.Context, local net.Conn, shared *sharedQUICConn, logger *slog.Logger) {
	defer local.Close()

	if tc, ok := local.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetWriteBuffer(tcpSockBufSize)
		_ = tc.SetReadBuffer(tcpSockBufSize)
	}

	stream, conn, err := shared.openStream(ctx)
	if err != nil {
		logger.Warn("[quic-join] open stream failed", "err", err)
		return
	}

	count := shared.activeStreams.Add(1)
	logger.Info("[quic-join] stream opened", "remote", local.RemoteAddr(), "active", count)

	quicConn := wrapQuicStream(stream, conn)
	joinConnGeneric(quicConn, local)

	shared.activeStreams.Add(-1)
	logger.Debug("[quic-join] stream closed", "remote", local.RemoteAddr(),
		"active", shared.activeStreams.Load())
}