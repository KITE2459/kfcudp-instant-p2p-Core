package kcpbridge

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

const (
	msgHello   byte = 0x01
	msgNewWork byte = 0x03 // 오라클→백엔드: [0x03][sessionID 4B][convID 4B] = 9바이트
	msgWorkACK byte = 0x04 // 백엔드→오라클 ctrl: [0x04][sessionID 4B][port 2B] = 7바이트

	workConnTimeout         = 15 * time.Second
	reverseReconnectBackoff = 2 * time.Second

	workChanPoolSize = 512
	pongDeadPoolSize = 64

	fwdPktBufSize = 2048
)

// ────────────────────────────────────────────────────────
// 풀
// ────────────────────────────────────────────────────────

var _workChanArena [workChanPoolSize]chan *net.UDPAddr
var workChanPool chan chan *net.UDPAddr
var pongDeadPool chan chan struct{}

func init() {
	workChanPool = make(chan chan *net.UDPAddr, workChanPoolSize)
	for i := range _workChanArena {
		_workChanArena[i] = make(chan *net.UDPAddr, 1)
		workChanPool <- _workChanArena[i]
	}
	pongDeadPool = make(chan chan struct{}, pongDeadPoolSize)
	for i := 0; i < pongDeadPoolSize; i++ {
		pongDeadPool <- make(chan struct{})
	}
}

func getWorkAddrChan() chan *net.UDPAddr {
	select {
	case ch := <-workChanPool:
		return ch
	default:
		return make(chan *net.UDPAddr, 1)
	}
}

func putWorkAddrChan(ch chan *net.UDPAddr) {
	select {
	case <-ch:
	default:
	}
	select {
	case workChanPool <- ch:
	default:
	}
}

func getPongDeadChan() chan struct{} {
	select {
	case ch := <-pongDeadPool:
		return ch
	default:
		return make(chan struct{})
	}
}

func replenishPongDead() {
	select {
	case pongDeadPool <- make(chan struct{}):
	default:
	}
}

// ────────────────────────────────────────────────────────
// ctrlWriter
// ────────────────────────────────────────────────────────
type ctrlWriter struct {
	mu   sync.Mutex
	conn *kcp.UDPSession
}

// send5 - [type 1B][payload 4B]
func (cw *ctrlWriter) send5(typ byte, payload uint32) error {
	b := getCtrlBuf()
	b.data[0] = typ
	binary.BigEndian.PutUint32(b.data[1:], payload)
	cw.mu.Lock()
	_, err := cw.conn.Write(b.data[:])
	cw.mu.Unlock()
	putCtrlBuf(b)
	return err
}

// sendNewWork - [0x03][sessionID 4B][convID 4B] = 9바이트
func (cw *ctrlWriter) sendNewWork(sessionID, convID uint32) error {
	var buf [9]byte
	buf[0] = msgNewWork
	binary.BigEndian.PutUint32(buf[1:5], sessionID)
	binary.BigEndian.PutUint32(buf[5:9], convID)
	cw.mu.Lock()
	_, err := cw.conn.Write(buf[:])
	cw.mu.Unlock()
	return err
}

// ────────────────────────────────────────────────────────
// workAddrMap
// ────────────────────────────────────────────────────────
type workAddrMap struct {
	mu sync.Mutex
	m  map[uint32]chan *net.UDPAddr
}

func newWorkAddrMap() *workAddrMap {
	return &workAddrMap{m: make(map[uint32]chan *net.UDPAddr)}
}

func (w *workAddrMap) register(id uint32) chan *net.UDPAddr {
	ch := getWorkAddrChan()
	w.mu.Lock()
	w.m[id] = ch
	w.mu.Unlock()
	return ch
}

func (w *workAddrMap) deliver(id uint32, addr *net.UDPAddr) bool {
	w.mu.Lock()
	ch, ok := w.m[id]
	if ok {
		delete(w.m, id)
	}
	w.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- addr:
		return true
	default:
		return false
	}
}

func (w *workAddrMap) unregister(id uint32) {
	w.mu.Lock()
	ch, ok := w.m[id]
	if ok {
		delete(w.m, id)
	}
	w.mu.Unlock()
	if ok {
		putWorkAddrChan(ch)
	}
}

// ────────────────────────────────────────────────────────
// fwdPkt - 포워딩 패킷
type fwdPkt struct {
	buf *fwdBuf
	len int
	dst *net.UDPAddr
}

// udpForwarder
// ────────────────────────────────────────────────────────
type udpForwarder struct {
	conn    *net.UDPConn   // 첫 번째 소켓 (매핑 등록/MAPPING_READY 전송용)
	conns   []*net.UDPConn // SO_REUSEPORT 멀티소켓
	logger  *slog.Logger
	workMap *workAddrMap

	mu           sync.RWMutex
	clientToBack map[string]*net.UDPAddr
	backIPToClient map[string]*net.UDPAddr // 백엔드 IP → 클라이언트
	lastActivity map[string]int64

	newClientCh chan newClientEvt
}

type newClientEvt struct {
	addr   *net.UDPAddr
	buf    *fwdBuf
	len    int
	convID uint32
}

func newUDPForwarder(addr string, logger *slog.Logger, workMap *workAddrMap) (*udpForwarder, error) {
	nShards := runtime.NumCPU()
	if nShards < 2 {
		nShards = 2
	}
	conns := make([]*net.UDPConn, nShards)
	for i := range conns {
		pc, err := newReusePortConn(addr)
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			return nil, err
		}
		conn := pc.(*net.UDPConn)
		_ = conn.SetReadBuffer(udpSockBufSize)
		_ = conn.SetWriteBuffer(udpSockBufSize)
		conns[i] = conn
	}
	logger.Info("[udp-fwd] reuseport shards", "n", nShards, "addr", addr)
	return &udpForwarder{
		conn:           conns[0],
		conns:          conns,
		logger:         logger,
		workMap:        workMap,
		clientToBack:   make(map[string]*net.UDPAddr),
		backIPToClient: make(map[string]*net.UDPAddr),
		lastActivity:   make(map[string]int64),
		newClientCh:    make(chan newClientEvt, 256),
	}, nil
}

func (f *udpForwarder) addMapping(clientAddr, backAddr *net.UDPAddr) {
	f.mu.Lock()
	f.clientToBack[clientAddr.String()] = backAddr
	f.backIPToClient[backAddr.IP.String()] = clientAddr
	f.lastActivity[clientAddr.String()] = time.Now().UnixNano()
	f.mu.Unlock()
	f.logger.Debug("[udp-fwd] mapping added", "client", clientAddr, "backend", backAddr)
}

func (f *udpForwarder) removeMapping(clientAddr, backAddr *net.UDPAddr) {
	f.mu.Lock()
	delete(f.clientToBack, clientAddr.String())
	delete(f.backIPToClient, backAddr.IP.String())
	delete(f.lastActivity, clientAddr.String())
	f.mu.Unlock()
	f.logger.Debug("[udp-fwd] mapping removed", "client", clientAddr)
}

func (f *udpForwarder) touchActivity(clientKey string) {
	f.mu.Lock()
	f.lastActivity[clientKey] = time.Now().UnixNano()
	f.mu.Unlock()
}

func (f *udpForwarder) isIdle(clientKey string) bool {
	f.mu.RLock()
	t := f.lastActivity[clientKey]
	f.mu.RUnlock()
	return t == 0 || time.Since(time.Unix(0, t)) > idleTimeout
}

func (f *udpForwarder) run(ctx context.Context) {
	// 각 SO_REUSEPORT 소켓마다 독립 goroutine
	// OS RSS로 패킷 분산 → CPU 코어 수만큼 수신 처리량 확장
	for _, conn := range f.conns {
		go f.runShard(ctx, conn)
	}
}

func (f *udpForwarder) runShard(ctx context.Context, conn *net.UDPConn) {
	buf := make([]byte, fwdPktBufSize)
	go func() { <-ctx.Done(); conn.Close() }()

	// 송신 goroutine 분리 — WriteToUDP 블로킹이 수신 루프에 영향 안 줌
	sendCh := make(chan fwdPkt, 4096)
	go func() {
		for pkt := range sendCh {
			_, _ = conn.WriteToUDP(pkt.buf.data[:pkt.len], pkt.dst)
			putFwdBuf(pkt.buf)
		}
	}()
	defer close(sendCh)

	for {
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if n == 0 {
			continue
		}

		fromStr := from.String()

		f.mu.RLock()
		dst := f.clientToBack[fromStr]
		src := f.backIPToClient[from.IP.String()]
		f.mu.RUnlock()

		if dst != nil {
			// 클라이언트 → 백엔드
			f.touchActivity(fromStr)
			b := getFwdBuf()
			copy(b.data[:n], buf[:n])
			select {
			case sendCh <- fwdPkt{buf: b, len: n, dst: dst}:
			default:
				putFwdBuf(b)
			}
			continue
		}

		if src != nil {
			// 백엔드 → 클라이언트
			b := getFwdBuf()
			copy(b.data[:n], buf[:n])
			select {
			case sendCh <- fwdPkt{buf: b, len: n, dst: src}:
			default:
				putFwdBuf(b)
			}
			continue
		}

		// 더미 패킷 무시
		if n == 1 && buf[0] == 0xFF {
			continue
		}

		// 새 클라이언트: KCP 패킷이면 conv ID 추출 (첫 4바이트, LittleEndian)
		var convID uint32
		if n >= 4 {
			convID = binary.LittleEndian.Uint32(buf[:4])
		}

		// pending 중인지 확인 (중복 처리 방지)
		f.mu.RLock()
		_, pending := f.lastActivity[fromStr]
		f.mu.RUnlock()
		if pending {
			continue
		}

		// pending 등록
		f.mu.Lock()
		f.lastActivity[fromStr] = time.Now().UnixNano()
		f.mu.Unlock()

		b := getFwdBuf()
		copy(b.data[:n], buf[:n])
		addrCopy := *from
		select {
		case f.newClientCh <- newClientEvt{addr: &addrCopy, buf: b, len: n, convID: convID}:
		default:
			putFwdBuf(b)
			f.mu.Lock()
			delete(f.lastActivity, fromStr)
			f.mu.Unlock()
		}
	}
}

func (f *udpForwarder) close() {
	for _, conn := range f.conns {
		conn.Close()
	}
}

// ────────────────────────────────────────────────────────
// ReverseHost
// ────────────────────────────────────────────────────────

type reverseHostState struct {
	logger      *slog.Logger
	fwd         *udpForwarder
	workMap     *workAddrMap
	activeConns atomic.Int64
	sessionSeq  atomic.Uint32

	cwMu sync.RWMutex
	cw   *ctrlWriter
}

func (hs *reverseHostState) getCW() *ctrlWriter {
	hs.cwMu.RLock()
	defer hs.cwMu.RUnlock()
	return hs.cw
}

func (hs *reverseHostState) setCW(cw *ctrlWriter) {
	hs.cwMu.Lock()
	hs.cw = cw
	hs.cwMu.Unlock()
}

func ReverseHost(ctx context.Context, ctrlAddr, userAddr string, logger *slog.Logger) error {
	ctrlListener, err := kcp.ListenWithOptions(ctrlAddr, nil, 0, 0)
	if err != nil {
		return fmt.Errorf("ctrl listen: %w", err)
	}
	defer ctrlListener.Close()
	_ = ctrlListener.SetReadBuffer(udpSockBufSize)
	_ = ctrlListener.SetWriteBuffer(udpSockBufSize)

	workMap := newWorkAddrMap()
	fwd, err := newUDPForwarder(userAddr, logger, workMap)
	if err != nil {
		return fmt.Errorf("udp forwarder: %w", err)
	}
	defer fwd.close()

	hs := &reverseHostState{
		logger:  logger,
		fwd:     fwd,
		workMap: workMap,
	}

	logger.Info("[reverse-host] listening", "ctrl", ctrlAddr, "user", userAddr)
	fmt.Printf("KCP ReverseHost: ctrl=%s user=%s\n", ctrlAddr, userAddr)

	go fwd.run(ctx)
	go hs.watchNewClients(ctx)
	go func() { <-ctx.Done(); ctrlListener.Close() }()

	for {
		conn, err := ctrlListener.AcceptKCP()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		applyBackendKCPOptions(conn)
		go hs.handleCtrl(ctx, conn)
	}
}

func (hs *reverseHostState) watchNewClients(ctx context.Context) {
	for {
		select {
		case ev := <-hs.fwd.newClientCh:
			go hs.handleNewClient(ctx, ev.addr, ev.buf, ev.len, ev.convID)
		case <-ctx.Done():
			return
		}
	}
}

func (hs *reverseHostState) handleNewClient(ctx context.Context, clientAddr *net.UDPAddr, firstBuf *fwdBuf, firstLen int, convID uint32) {
	defer putFwdBuf(firstBuf)
	cw := hs.getCW()
	if cw == nil {
		hs.logger.Warn("[reverse-host] no backend connected")
		return
	}

	count := hs.activeConns.Add(1)
	defer hs.activeConns.Add(-1)

	sessionID := hs.sessionSeq.Add(1)
	hs.logger.Info("[reverse-host] new user",
		"remote", clientAddr, "active", count, "session", sessionID, "conv", convID)

	backCh := hs.workMap.register(sessionID)
	defer hs.workMap.unregister(sessionID)

	// NEW_WORK에 convID 포함 전송
	if err := cw.sendNewWork(sessionID, convID); err != nil {
		hs.logger.Warn("[reverse-host] NEW_WORK failed", "err", err)
		return
	}

	select {
	case backAddr := <-backCh:
		hs.logger.Info("[reverse-host] matched",
			"session", sessionID, "client", clientAddr, "backend", backAddr)

		hs.fwd.addMapping(clientAddr, backAddr)

		// 클라이언트 첫 패킷을 백엔드로 전달
		_, _ = hs.fwd.conn.WriteToUDP(firstBuf.data[:firstLen], backAddr)

		hs.keepSession(ctx, clientAddr, backAddr)

	case <-time.After(workConnTimeout):
		hs.logger.Warn("[reverse-host] work timeout", "session", sessionID)
	case <-ctx.Done():
	}
}

func (hs *reverseHostState) keepSession(ctx context.Context, clientAddr, backAddr *net.UDPAddr) {
	defer hs.fwd.removeMapping(clientAddr, backAddr)

	clientKey := clientAddr.String()
	ticker := time.NewTicker(idleTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if hs.fwd.isIdle(clientKey) {
				hs.logger.Info("[udp-fwd] idle timeout", "client", clientAddr)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (hs *reverseHostState) handleCtrl(ctx context.Context, ctrl *kcp.UDPSession) {
	remote := ctrl.RemoteAddr().String()
	backIP := ctrl.RemoteAddr().(*net.UDPAddr).IP
	hs.logger.Info("[reverse-host] join connected", "remote", remote)
	defer func() {
		ctrl.Close()
		hs.setCW(nil)
	}()

	cw := &ctrlWriter{conn: ctrl}
	hs.setCW(cw)

	pongDead := getPongDeadChan()
	go func() {
		defer close(pongDead)
		defer replenishPongDead()
		// 스택 배열 사용 — 루프마다 힙 할당 없음
		var msgBuf [7]byte
		for {
			if _, err := io.ReadFull(ctrl, msgBuf[:1]); err != nil {
				return
			}
			switch msgBuf[0] {
			case 0x00:
				if _, err := io.ReadFull(ctrl, msgBuf[1:5]); err != nil {
					return
				}
			case msgWorkACK:
				if _, err := io.ReadFull(ctrl, msgBuf[1:7]); err != nil {
					return
				}
				sessionID := binary.BigEndian.Uint32(msgBuf[1:5])
				port := binary.BigEndian.Uint16(msgBuf[5:7])
				backAddr := &net.UDPAddr{IP: backIP, Port: int(port)}
				hs.logger.Debug("[reverse-host] WORK_ACK",
					"session", sessionID, "backend", backAddr)
				if !hs.workMap.deliver(sessionID, backAddr) {
					hs.logger.Warn("[reverse-host] WORK_ACK no waiter",
						"session", sessionID)
				}
			default:
				if _, err := io.ReadFull(ctrl, msgBuf[1:5]); err != nil {
					return
				}
			}
		}
	}()

	select {
	case <-pongDead:
		hs.logger.Info("[reverse-host] join disconnected", "remote", remote)
	case <-ctx.Done():
		ctrl.Close()
	}
}

// ────────────────────────────────────────────────────────
// ReverseJoin
// ────────────────────────────────────────────────────────

type reverseJoinState struct {
	hostAddr    string
	userAddr    string
	userUDPAddr *net.UDPAddr
	target      string
	logger      *slog.Logger
	activeConns atomic.Int64
}

func ReverseJoin(ctx context.Context, hostAddr, userAddr, target string, logger *slog.Logger) error {
	resolvedUserAddr, err := net.ResolveUDPAddr("udp", userAddr)
	if err != nil {
		return fmt.Errorf("resolve userAddr %q: %w", userAddr, err)
	}

	js := &reverseJoinState{
		hostAddr:    hostAddr,
		userAddr:    userAddr,
		userUDPAddr: resolvedUserAddr,
		target:      target,
		logger:      logger,
	}

	js.logger.Info("[reverse-join] starting",
		"host", hostAddr, "user", userAddr, "target", target)
	fmt.Printf("KCP ReverseJoin: host=%s target=%s\n", hostAddr, target)
	fmt.Println("WEBRTC_READY")

	ctrlUDPConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return fmt.Errorf("ctrl UDP socket: %w", err)
	}
	defer ctrlUDPConn.Close()
	_ = ctrlUDPConn.SetReadBuffer(udpSockBufSize)
	_ = ctrlUDPConn.SetWriteBuffer(udpSockBufSize)

	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := js.run(ctx, ctrlUDPConn); err != nil {
			js.logger.Warn("[reverse-join] disconnected", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reverseReconnectBackoff):
		}
	}
}

func (js *reverseJoinState) run(ctx context.Context, ctrlUDPConn *net.UDPConn) error {
	resolvedHostAddr, err := net.ResolveUDPAddr("udp", js.hostAddr)
	if err != nil {
		return fmt.Errorf("resolve hostAddr: %w", err)
	}
	ctrl, err := kcp.NewConn3(1, resolvedHostAddr, nil, 0, 0, ctrlUDPConn)
	if err != nil {
		return fmt.Errorf("ctrl dial: %w", err)
	}
	applyBackendKCPOptions(ctrl)
	defer ctrl.Close()

	go func() {
		<-ctx.Done()
		ctrl.Close()
	}()

	helloMsg := getCtrlBuf()
	helloMsg.data[0] = msgHello
	binary.BigEndian.PutUint32(helloMsg.data[1:], 0)
	_, err = ctrl.Write(helloMsg.data[:])
	putCtrlBuf(helloMsg)
	if err != nil {
		return fmt.Errorf("HELLO: %w", err)
	}
	js.logger.Info("[reverse-join] connected", "host", js.hostAddr)

	cw := &ctrlWriter{conn: ctrl}

	// ping goroutine
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		ping := getCtrlBuf()
		defer putCtrlBuf(ping)
		ping.data[0] = 0x00
		for {
			select {
			case <-ticker.C:
				if _, err := ctrl.Write(ping.data[:]); err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	var buf [9]byte // msgNewWork 최대 크기 — 스택 할당

	for {
		// 타입 바이트 먼저 읽기
		if _, err := io.ReadFull(ctrl, buf[:1]); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("ctrl read: %w", err)
		}

		switch buf[0] {
		case 0x00:
			// ping: 4바이트 소비
			if _, err := io.ReadFull(ctrl, buf[1:5]); err != nil {
				return fmt.Errorf("ping read: %w", err)
			}
		case msgNewWork:
			// [sessionID 4B][convID 4B]
			if _, err := io.ReadFull(ctrl, buf[1:9]); err != nil {
				return fmt.Errorf("NEW_WORK read: %w", err)
			}
			sessionID := binary.BigEndian.Uint32(buf[1:5])
			convID := binary.BigEndian.Uint32(buf[5:9])
			js.logger.Debug("[reverse-join] NEW_WORK",
				"session", sessionID, "conv", convID)
			go js.openWorkConn(ctx, sessionID, convID, cw)
		default:
			// 알 수 없는 타입: 4바이트 소비
			if _, err := io.ReadFull(ctrl, buf[1:5]); err != nil {
				return fmt.Errorf("unknown msg read: %w", err)
			}
		}
	}
}

func (js *reverseJoinState) openWorkConn(ctx context.Context, sessionID, convID uint32, cw *ctrlWriter) {
	// 로컬 포트 확보
	localConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		js.logger.Warn("[reverse-join] local UDP failed", "session", sessionID, "err", err)
		return
	}
	localPort := uint16(localConn.LocalAddr().(*net.UDPAddr).Port)

	// NAT 홀 뚫기: localConn → 오라클:60819
	_, _ = localConn.WriteToUDP([]byte{0xFF}, js.userUDPAddr)

	// WORK_ACK 전송: ctrl로 [0x04][sessionID 4B][port 2B]
	var ack [7]byte
	ack[0] = msgWorkACK
	binary.BigEndian.PutUint32(ack[1:5], sessionID)
	binary.BigEndian.PutUint16(ack[5:7], localPort)
	cw.mu.Lock()
	_, err = cw.conn.Write(ack[:])
	cw.mu.Unlock()
	if err != nil {
		js.logger.Warn("[reverse-join] WORK_ACK failed", "session", sessionID, "err", err)
		localConn.Close()
		return
	}
	js.logger.Debug("[reverse-join] WORK_ACK sent", "session", sessionID, "port", localPort)

	// convID로 kcp.NewConn3 생성
	// 클라이언트와 같은 convID 사용 → 오라클이 포워딩하는 패킷을 자신의 세션으로 인식
	workConn, err := kcp.NewConn3(convID, js.userUDPAddr, nil, 0, 0, localConn)
	if err != nil {
		js.logger.Warn("[reverse-join] work KCP failed", "session", sessionID, "err", err)
		localConn.Close()
		return
	}
	applyKCPOptions(workConn)

	tcp, err := net.Dial("tcp", js.target)
	if err != nil {
		js.logger.Warn("[reverse-join] MC dial failed", "target", js.target, "err", err)
		workConn.Close()
		localConn.Close()
		return
	}
	setTCPOptions(tcp)

	count := js.activeConns.Add(1)
	js.logger.Info("[reverse-join] established",
		"session", sessionID, "active", count, "conv", convID)

	joinConnGeneric(workConn, tcp)

	js.activeConns.Add(-1)
	localConn.Close()
	js.logger.Debug("[reverse-join] closed",
		"session", sessionID, "active", js.activeConns.Load())
}