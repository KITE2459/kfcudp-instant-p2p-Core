package signaling

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
)

type MessageHandler func(msg map[string]any)

type Client struct {
	serverURL   string
	roomID      string
	role        string
	sessionID   string
	onMessage   MessageHandler
	onConnected func() // 시그널링 연결 완료 시 1회 호출
	logger      *slog.Logger

	mu          sync.Mutex
	ws          *websocket.Conn
	closed      bool
	backoff     time.Duration
	connectedOnce sync.Once

	ctx    context.Context
	cancel context.CancelFunc
}

func NewHostClient(serverURL, roomID string, onMessage MessageHandler, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		serverURL: serverURL,
		roomID:    roomID,
		role:      "host",
		onMessage: onMessage,
		logger:    logger,
		backoff:   initialBackoff,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func NewJoinClient(serverURL, roomID, sessionID string, onMessage MessageHandler, onConnected func(), logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		serverURL:   serverURL,
		roomID:      roomID,
		role:        "client",
		sessionID:   sessionID,
		onMessage:   onMessage,
		onConnected: onConnected,
		logger:      logger,
		backoff:     initialBackoff,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (c *Client) Connect() {
	go c.connectLoop()
}

func (c *Client) connectLoop() {
	for {
		if c.isClosed() {
			return
		}
		if err := c.dial(); err != nil {
			c.logger.Warn("Signaling connect failed", "err", err)
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(c.backoff):
				c.backoff = min(c.backoff*2, maxBackoff)
			}
			continue
		}
		c.backoff = initialBackoff
	}
}

func (c *Client) dial() error {
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()

	ws, _, err := websocket.Dial(ctx, c.serverURL, nil)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.ws = ws
	c.mu.Unlock()

	c.logger.Info("Signaling connected", "url", c.serverURL, "role", c.role)

	if c.role == "host" {
		c.sendRaw(map[string]any{
			"type":   "HOST",
			"roomId": c.roomID,
		})
	} else {
		// 클라이언트: 연결 완료 콜백 1회 호출 (WEBRTC_READY 출력용)
		c.connectedOnce.Do(func() {
			if c.onConnected != nil {
				c.onConnected()
			}
		})
	}

	for {
		_, raw, err := ws.Read(c.ctx)
		if err != nil {
			c.mu.Lock()
			c.ws = nil
			c.mu.Unlock()
			if c.isClosed() {
				return nil
			}
			return err
		}

		var msg map[string]any
		if err := json.Unmarshal(raw, &msg); err != nil {
			c.logger.Warn("Invalid JSON from signaling", "err", err)
			continue
		}
		if c.onMessage != nil {
			c.onMessage(msg)
		}
	}
}

func (c *Client) Send(msg map[string]any) error {
	return c.sendRaw(msg)
}

func (c *Client) sendRaw(msg map[string]any) error {
	c.mu.Lock()
	ws := c.ws
	c.mu.Unlock()

	if ws == nil {
		return nil
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return ws.Write(c.ctx, websocket.MessageText, b)
}

func (c *Client) SessionID() string { return c.sessionID }
func (c *Client) RoomID() string    { return c.roomID }

func (c *Client) Close() {
	c.mu.Lock()
	c.closed = true
	ws := c.ws
	c.ws = nil
	c.mu.Unlock()
	c.cancel()
	if ws != nil {
		_ = ws.Close(websocket.StatusNormalClosure, "bye")
	}
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}