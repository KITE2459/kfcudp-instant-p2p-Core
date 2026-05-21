package main

import (
	cryptorand "crypto/rand"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"

	"jp.zpw.openfriend/internal/bridge"
	"jp.zpw.openfriend/internal/signaling"
)

var version = "dev"

func main() {
	// GC 튜닝: 힙이 4배 될 때까지 GC를 미룸 (기본 100%)
	// WebRTC 패킷 처리 중 GC pause로 인한 핑 스파이크 감소
	debug.SetGCPercent(400)

	// 16명 동시 WebRTC 세션의 goroutine 스케줄링을 위해
	// GOMAXPROCS를 명시적으로 CPU 수에 맞게 설정
	runtime.GOMAXPROCS(runtime.NumCPU())

	var (
		signalingURL string
		roomID       string
		target       string
		listenAddr   string
		joinMode     bool
		sessionID    string
		verbose      bool
		showVersion  bool
	)

	flag.StringVar(&signalingURL, "signaling", "ws://193.122.114.163:8765", "WebSocket signaling server URL")
	flag.StringVar(&roomID, "room", "", "room ID")
	flag.StringVar(&target, "target", "127.0.0.1:25565", "host mode: 브릿지할 Minecraft 서버 주소")
	flag.StringVar(&listenAddr, "listen", "127.0.0.1:25566", "join mode: 로컬 주소")
	flag.BoolVar(&joinMode, "join", false, "join mode로 실행")
	flag.StringVar(&sessionID, "session", "", "join mode: 세션 ID")
	flag.BoolVar(&verbose, "verbose", false, "debug logging")
	var noProxy bool
	flag.BoolVar(&noProxy, "no-proxy", false, "PROXY protocol 헤더 전송 안 함")
	flag.BoolVar(&showVersion, "version", false, "버전 출력")
	flag.Parse()

	if showVersion {
		fmt.Println("openfriend-bridge", version)
		return
	}

	if roomID == "" {
		fmt.Fprintln(os.Stderr, "error: --room 이 필요합니다")
		os.Exit(2)
	}

	lvl := slog.LevelInfo
	if verbose {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(logger)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	if joinMode {
		runJoin(signalingURL, roomID, sessionID, listenAddr, stop, logger)
	} else {
		runHost(signalingURL, roomID, target, !noProxy, stop, logger)
	}
}

func runHost(signalingURL, roomID, target string, useProxy bool, stop chan os.Signal, logger *slog.Logger) {
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
	fmt.Printf("Host mode: room=%s target=%s\n", roomID, target)

	<-stop
	fmt.Println("Shutting down...")
	pm.Close()
	sig.Close()
}

func runJoin(signalingURL, roomID, sessionID, listenAddr string, stop chan os.Signal, logger *slog.Logger) {
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

	fmt.Printf("Join mode: room=%s listen=%s session=%s\n", roomID, listenAddr, sessionID)

	sig.Connect()

	<-stop
	fmt.Println("Shutting down...")
	jm.Close()
	sig.Close()
}

func newSessionID() string {
	b := make([]byte, 8)
	_, _ = cryptorand.Read(b)
	return fmt.Sprintf("%x", b)
}