package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"jp.zpw.openfriend/internal/quicbridge"
)

func main() {
	var (
		listenAddr string
		serverAddr string
		target     string
		joinMode   bool
		verbose    bool
	)

	flag.StringVar(&listenAddr, "listen", "0.0.0.0:25565", "listen address")
	flag.StringVar(&serverAddr, "server", "", "join: 서버 주소")
	flag.StringVar(&target, "target", "127.0.0.1:25565", "host: MC서버 주소")
	flag.BoolVar(&joinMode, "join", false, "join mode (클라이언트)")
	flag.BoolVar(&verbose, "verbose", false, "debug logging")
	flag.Parse()

	lvl := slog.LevelInfo
	if verbose {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))

	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		fmt.Println("Shutting down...")
		cancel()
	}()

	// pprof 서버 (프로파일링용)
	go func() {
		_ = http.ListenAndServe("127.0.0.1:6060", nil)
	}()

	var err error
	switch {
	case joinMode:
		if serverAddr == "" {
			fmt.Fprintln(os.Stderr, "error: --server 가 필요합니다")
			os.Exit(2)
		}
		err = quicbridge.Join(ctx, listenAddr, serverAddr, logger)
	default:
		// host mode: QUIC 수신 → TCP 변환
		err = quicbridge.Host(ctx, listenAddr, target, logger)
	}

	if err != nil && ctx.Err() == nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}