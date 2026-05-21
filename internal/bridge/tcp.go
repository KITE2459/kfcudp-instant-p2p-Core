/*
 * OpenFriend — Minecraft Java Edition Friends List bridge.
 * Copyright (c) 2026 ZSHARE (https://zpw.jp). Licensed under the MIT License.
 */
package bridge

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// sendQueueSize - DataChannel Send 큐 크기.
// 16명 × 세션당 burst를 흡수할 수 있도록 충분히 확보.
// TCP 스트림이므로 드롭 불가 — 큐는 배압(backpressure)용이며 절대 드롭하지 않음.
const sendQueueSize = 16384

// coalesceBatchMax - 한 번의 DataChannel.Send() 에 합칠 최대 청크 수.
// 청크당 최대 65536 바이트이므로 최악의 경우 coalesceBatchMax * 65536 바이트가
// 하나의 Send() 호출로 묶임. SCTP 메시지 크기 제한(256KB~1MB)을 고려해 4로 설정.
const coalesceBatchMax = 4

// coalesceMaxBytes - 합치기 상한선 (bytes).
// 256KB를 넘으면 SCTP 내부에서 단편화가 발생하므로 그 이하로 유지.
const coalesceMaxBytes = 256 * 1024

// tcpReadBuf - TCP 읽기용 고정 버퍼
type tcpReadBuf struct {
	data [65536]byte
}

// tcpReadBufPool - 미리 할당된 고정 읽기 버퍼 풀
// 16명 세션 × 동시 read 여유를 위해 512개로 확대
var tcpReadPool = func() chan *tcpReadBuf {
	ch := make(chan *tcpReadBuf, 512)
	for i := 0; i < 512; i++ {
		ch <- &tcpReadBuf{}
	}
	return ch
}()

func getTCPReadBuf() *tcpReadBuf {
	select {
	case b := <-tcpReadPool:
		return b
	default:
		return &tcpReadBuf{}
	}
}

func putTCPReadBuf(b *tcpReadBuf) {
	select {
	case tcpReadPool <- b:
	default:
	}
}

// coalesceBuf - coalescing 전용 임시 조합 버퍼 (스택 할당 유도용 구조체)
// sendLoop 에서만 사용하며 goroutine 1개당 1개씩 보유함.
type coalesceBuf struct {
	data [coalesceMaxBytes]byte
}

var coalescePool = func() chan *coalesceBuf {
	ch := make(chan *coalesceBuf, 256)
	for i := 0; i < 256; i++ {
		ch <- &coalesceBuf{}
	}
	return ch
}()

func getCoalesceBuf() *coalesceBuf {
	select {
	case b := <-coalescePool:
		return b
	default:
		return &coalesceBuf{}
	}
}

func putCoalesceBuf(b *coalesceBuf) {
	select {
	case coalescePool <- b:
	default:
	}
}

// chunkBuf - 송신 버퍼 하나
type chunkBuf struct {
	data [65536]byte
	len  int
}

// chunkBufPool - 미리 할당된 고정 송신 버퍼 풀
type chunkBufPool struct {
	ch chan *chunkBuf
}

func newChunkBufPool(size int) *chunkBufPool {
	ch := make(chan *chunkBuf, size)
	for i := 0; i < size; i++ {
		ch <- &chunkBuf{}
	}
	return &chunkBufPool{ch: ch}
}

func (p *chunkBufPool) get() *chunkBuf {
	select {
	case b := <-p.ch:
		return b
	default:
		return &chunkBuf{}
	}
}

func (p *chunkBufPool) put(b *chunkBuf) {
	select {
	case p.ch <- b:
	default:
	}
}

// 전역 송신 버퍼 풀 (TCPBridge 인스턴스 간 공유)
// 100명 × 세션당 동시 청크 수를 고려해 32768로 확대
// (8192 → 32768: 세션당 보장 슬롯 82 → 327개)
var globalChunkPool = newChunkBufPool(32768)

type TCPBridge struct {
	conn   net.Conn
	logger *slog.Logger

	sendCh       chan *chunkBuf
	closeOnce    sync.Once
	onClose      func()
	writeTimeout time.Duration // 0이면 타임아웃 없음
}

// tcpWriteTimeout - 로컬 MC 서버 TCP Write 타임아웃.
// 서버가 패킷을 읽지 않아 OS 소켓 버퍼가 꽉 찼을 때 goroutine이 영구 블로킹되는 것을 방지.
// 레이싱 게임 특성상 서버가 살아있으면 즉시 소비하므로 5초면 충분히 보수적.
const tcpWriteTimeout = 5 * time.Second

func DialTCP(addr string, onDownstream func([]byte), onClose func(), logger *slog.Logger) (*TCPBridge, error) {
	if logger == nil {
		logger = slog.Default()
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		// 16명 burst를 OS 레벨에서 흡수하도록 버퍼 확대
		_ = tcp.SetReadBuffer(512 * 1024)
		_ = tcp.SetWriteBuffer(512 * 1024)
	}
	b := &TCPBridge{
		conn:         conn,
		logger:       logger,
		onClose:      onClose,
		sendCh:       make(chan *chunkBuf, sendQueueSize),
		writeTimeout: tcpWriteTimeout,
	}
	go b.readLoop()
	go b.sendLoop(onDownstream)
	return b, nil
}

// readLoop - TCP 읽기 전담. Send 블로킹과 완전히 분리.
// 서버→클라 방향: TCP 스트림이므로 절대 드롭하지 않음.
// sendCh가 가득 차면 블로킹 — 이 배압이 DataChannel 쪽 속도를 자연스럽게 조절함.
func (b *TCPBridge) readLoop() {
	defer b.Close()
	rbuf := getTCPReadBuf()
	defer putTCPReadBuf(rbuf)
	buf := rbuf.data[:]
	for {
		n, err := b.conn.Read(buf)
		if n > 0 {
			cp := globalChunkPool.get()
			cp.len = copy(cp.data[:], buf[:n])
			// 블로킹 send — TCP 스트림 드롭은 프로토콜 파괴이므로 절대 불가
			b.sendCh <- cp
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				b.logger.Debug("TCP read ended", "err", err)
			}
			return
		}
	}
}

// sendLoop - sendCh에서 청크를 꺼내 DataChannel.Send() 호출 전담.
//
// Coalescing 전략:
//  1. 첫 청크를 블로킹으로 꺼낸다 (큐가 비어 있으면 여기서 대기).
//  2. 논블로킹으로 추가 청크를 최대 coalesceBatchMax-1 개, coalesceMaxBytes 이하까지 더 꺼낸다.
//  3. 합쳐진 데이터를 onDownstream() 1번으로 전달 → DataChannel.Send() 1번.
//
// 효과: 레이싱 게임처럼 작은 패킷이 연속으로 쏟아질 때 syscall / SCTP 메시지 수를
// 최대 coalesceBatchMax 배 줄여 throughput 향상, CPU 사용률 감소.
func (b *TCPBridge) sendLoop(onDownstream func([]byte)) {
	cb := getCoalesceBuf()
	defer putCoalesceBuf(cb)

	for first := range b.sendCh {
		// 합치기 시작
		total := first.len
		copy(cb.data[:], first.data[:first.len])
		globalChunkPool.put(first)

		// 논블로킹으로 추가 청크 흡수
		for i := 1; i < coalesceBatchMax; i++ {
			select {
			case next, ok := <-b.sendCh:
				if !ok {
					// 채널 닫힘 — 지금까지 모은 것 전송 후 종료
					onDownstream(cb.data[:total])
					return
				}
				if total+next.len > coalesceMaxBytes {
					// 크기 초과 → 지금 것 먼저 전송하고 next는 다음 round로
					onDownstream(cb.data[:total])
					total = next.len
					copy(cb.data[:], next.data[:next.len])
					globalChunkPool.put(next)
					// 다음 루프를 바로 시작하기 위해 inner loop 탈출
					i = coalesceBatchMax
					continue
				}
				copy(cb.data[total:], next.data[:next.len])
				total += next.len
				globalChunkPool.put(next)
			default:
				// 큐가 비어있음 — 지금까지 모은 것 즉시 전송
				i = coalesceBatchMax
			}
		}

		onDownstream(cb.data[:total])
	}
}

// batchWrite - net.Buffers(writev)를 이용한 배치 TCP Write.
// 여러 chunkBuf를 단일 syscall로 기록해 서버→클라 방향 처리량 향상.
// timeout > 0이면 Write 전에 SetWriteDeadline을 설정해 무한 블로킹을 방지.
// 호출자가 bufs 슬라이스와 그 안의 chunkBuf들을 소유하며, 반환 후 풀에 돌려줘야 함.
func batchWrite(conn net.Conn, chunks []*chunkBuf, timeout time.Duration) error {
	if len(chunks) == 0 {
		return nil
	}
	if timeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	if len(chunks) == 1 {
		_, err := conn.Write(chunks[0].data[:chunks[0].len])
		return err
	}
	// net.Buffers → writev(2) 로 단일 syscall
	bufs := make(net.Buffers, len(chunks))
	for i, c := range chunks {
		bufs[i] = c.data[:c.len]
	}
	_, err := bufs.WriteTo(conn)
	return err
}

// Feed - DataChannel에서 받은 데이터를 MC 서버 TCP 소켓으로 기록.
// writeTimeout 동안 소켓이 쓸 수 없으면 에러 반환 → 호출자가 세션을 닫음.
func (b *TCPBridge) Feed(data []byte) error {
	if b.writeTimeout > 0 {
		_ = b.conn.SetWriteDeadline(time.Now().Add(b.writeTimeout))
	}
	_, err := b.conn.Write(data)
	return err
}

func (b *TCPBridge) Close() {
	b.closeOnce.Do(func() {
		_ = b.conn.Close()
		close(b.sendCh)
		if b.onClose != nil {
			b.onClose()
		}
	})
}

func ProbeTCP(addr string, timeout time.Duration) bool {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}