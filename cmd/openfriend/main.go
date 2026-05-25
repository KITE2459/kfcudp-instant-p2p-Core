package main

import (
	cryptorand "crypto/rand"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"

	"jp.zpw.openfriend/internal/bridge"
	"jp.zpw.openfriend/internal/kcpbridge"
	"jp.zpw.openfriend/internal/signaling"
)

var version = "dev"

func main() {
	debug.SetGCPercent(-1)
	runtime.GOMAXPROCS(runtime.NumCPU())

	var (
		protocol        string
		signalingURL    string
		roomID          string
		target          string
		listenAddr      string
		serverAddr      string
		userAddr        string
		dataAddr        string
		joinMode        bool
		reverseHostMode bool
		reverseJoinMode bool
		sessionID       string
		verbose         bool
		showVersion     bool
		noProxy         bool
	)

	flag.StringVar(&protocol, "protocol", "webrtc", "통신 프로토콜: webrtc | kcp")
	flag.StringVar(&signalingURL, "signaling", "ws://193.122.114.163:8765", "signaling server URL (webrtc)")
	flag.StringVar(&roomID, "room", "", "room ID (webrtc)")
	flag.StringVar(&target, "target", "127.0.0.1:25565", "MC 서버 주소 (host/reverse-join)")
	flag.StringVar(&listenAddr, "listen", "127.0.0.1:25566", "로컬 수신 주소 (join)")
	flag.StringVar(&serverAddr, "server", "", "서버 주소 (kcp join/reverse-join)")
	flag.StringVar(&userAddr, "user-addr", "0.0.0.0:25565", "사용자 접속 주소 (reverse-host)")
	flag.StringVar(&dataAddr, "data-addr", "", "오라클 데이터 채널 주소 (reverse-join 전용, 예: 193.122.114.163:60819)")
	flag.BoolVar(&joinMode, "join", false, "join mode")
	flag.BoolVar(&reverseHostMode, "reverse-host", false, "KCP 리버스 프록시 호스트 (공인 IP 서버)")
	flag.BoolVar(&reverseJoinMode, "reverse-join", false, "KCP 리버스 프록시 조인 (NAT 뒤 백엔드)")
	flag.StringVar(&sessionID, "session", "", "세션 ID (webrtc join)")
	flag.BoolVar(&verbose, "verbose", false, "debug logging")
	flag.BoolVar(&noProxy, "no-proxy", false, "PROXY protocol 헤더 비활성화 (webrtc host)")
	flag.BoolVar(&showVersion, "version", false, "버전 출력")
	flag.Parse()

	if showVersion {
		fmt.Println("openfriend", version)
		return
	}

	lvl := slog.LevelInfo
	if verbose {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(logger)

	go func() { _ = http.ListenAndServe("127.0.0.1:6060", nil) }()

	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		fmt.Println("Shutting down...")
		cancel()
	}()

	switch {
	case reverseHostMode:
		// reverse-host: 공인 IP 서버
		// --reverse-host --listen 0.0.0.0:7000 --user-addr 0.0.0.0:25565
		runReverseHost(ctx, listenAddr, userAddr, logger)
	case reverseJoinMode:
		// reverse-join: NAT 뒤 백엔드
		// --reverse-join --server 공인IP:65535 --data-addr 공인IP:60819 --target 127.0.0.1:25565
		runReverseJoin(ctx, serverAddr, dataAddr, userAddr, target, logger)
	default:
		switch protocol {
		case "webrtc":
			runWebRTC(ctx, signalingURL, roomID, sessionID, target, listenAddr, !noProxy, joinMode, logger)
		case "kcp":
			runKCP(ctx, listenAddr, serverAddr, target, joinMode, logger)
		default:
			fmt.Fprintf(os.Stderr, "error: 알 수 없는 protocol %q (webrtc|kcp)\n", protocol)
			os.Exit(2)
		}
	}
}

// ── WebRTC ──────────────────────────────────────────────

func runWebRTC(ctx context.Context, signalingURL, roomID, sessionID, target, listenAddr string, useProxy, joinMode bool, logger *slog.Logger) {
	if roomID == "" {
		fmt.Fprintln(os.Stderr, "error: --room 이 필요합니다 (webrtc)")
		os.Exit(2)
	}
	stop := ctx.Done()
	if joinMode {
		runWebRTCJoin(signalingURL, roomID, sessionID, listenAddr, stop, logger)
	} else {
		runWebRTCHost(signalingURL, roomID, target, useProxy, stop, logger)
	}
}

func runWebRTCHost(signalingURL, roomID, target string, useProxy bool, stop <-chan struct{}, logger *slog.Logger) {
	if _, err := bridge.ParseTarget(target); err != nil {
		fmt.Fprintf(os.Stderr, "invalid --target %q: %v\n", target, err)
		os.Exit(2)
	}
	var pm *bridge.HostManager
	sig := signaling.NewHostClient(signalingURL, roomID, func(msg map[string]any) {
		if pm != nil {
			pm.OnMessage(msg)
		}
	}, logger)
	pm = bridge.NewHostManager(sig, target, useProxy, logger)
	sig.Connect()
	fmt.Printf("WebRTC Host: room=%s target=%s\n", roomID, target)
	<-stop
	pm.Close()
	sig.Close()
}

func runWebRTCJoin(signalingURL, roomID, sessionID, listenAddr string, stop <-chan struct{}, logger *slog.Logger) {
	if sessionID == "" {
		sessionID = newSessionID()
	}
	var jm *bridge.JoinManager
	sig := signaling.NewJoinClient(signalingURL, roomID, sessionID, func(msg map[string]any) {
		if jm != nil {
			jm.OnMessage(msg)
		}
	}, func() {
		if err := jm.Listen(listenAddr); err != nil {
			fmt.Fprintf(os.Stderr, "listen failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("WEBRTC_READY")
	}, logger)
	jm = bridge.NewJoinManager(sig, logger)
	fmt.Printf("WebRTC Join: room=%s listen=%s session=%s\n", roomID, listenAddr, sessionID)
	sig.Connect()
	<-stop
	jm.Close()
	sig.Close()
}

// ── KCP ─────────────────────────────────────────────────

func runKCP(ctx context.Context, listenAddr, serverAddr, target string, joinMode bool, logger *slog.Logger) {
	var err error
	if joinMode {
		if serverAddr == "" {
			fmt.Fprintln(os.Stderr, "error: --server 가 필요합니다 (kcp join)")
			os.Exit(2)
		}
		err = kcpbridge.Join(ctx, listenAddr, serverAddr, logger)
	} else {
		err = kcpbridge.Host(ctx, listenAddr, target, logger)
	}
	if err != nil && ctx.Err() == nil {
		logger.Error("KCP fatal", "err", err)
		os.Exit(1)
	}
}

// ── KCP Reverse Proxy ────────────────────────────────────

func runReverseHost(ctx context.Context, listenAddr, userAddr string, logger *slog.Logger) {
	// listenAddr: reverse-join이 연결하는 주소 (제어 채널)
	// userAddr:   MC 사용자가 접속하는 주소
	if listenAddr == "" {
		fmt.Fprintln(os.Stderr, "error: --listen 이 필요합니다 (reverse-host)")
		os.Exit(2)
	}
	if err := kcpbridge.ReverseHost(ctx, listenAddr, userAddr, logger); err != nil && ctx.Err() == nil {
		logger.Error("ReverseHost fatal", "err", err)
		os.Exit(1)
	}
}

func runReverseJoin(ctx context.Context, hostAddr, dataAddr, userAddr, target string, logger *slog.Logger) {
	if hostAddr == "" {
		fmt.Fprintln(os.Stderr, "error: --server 가 필요합니다 (reverse-join)")
		os.Exit(2)
	}
	// --user-addr: 오라클 데이터 채널 주소 (예: 193.122.114.163:60819)
	// --server의 host + --user-addr의 port로 자동 조합 가능
	resolvedUserAddr := userAddr
	if !containsHost(userAddr) {
		serverHost, _, err := net.SplitHostPort(hostAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: --server 파싱 실패: %v\n", err)
			os.Exit(2)
		}
		_, port, _ := net.SplitHostPort(userAddr)
		resolvedUserAddr = net.JoinHostPort(serverHost, port)
		logger.Info("[reverse-join] user-addr resolved", "addr", resolvedUserAddr)
	}
	if err := kcpbridge.ReverseJoin(ctx, hostAddr, resolvedUserAddr, target, logger); err != nil && ctx.Err() == nil {
		logger.Error("ReverseJoin fatal", "err", err)
		os.Exit(1)
	}
}

// containsHost - addr에 0.0.0.0이나 ::가 아닌 실제 호스트가 있는지 확인
func containsHost(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return true // 도메인명
	}
	return !ip.IsUnspecified()
}

// ────────────────────────────────────────────────────────

func newSessionID() string {
	b := make([]byte, 8)
	_, _ = cryptorand.Read(b)
	return fmt.Sprintf("%x", b)
}