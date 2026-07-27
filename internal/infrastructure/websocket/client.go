package websocket

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"gameserver/internal/domain"

	"github.com/gorilla/websocket"
)

const (
	writeWait = 3 * time.Second
	pongWait = 60 * time.Second
	maxMessageSize = 4096
)

type simpleBufferPool struct {
	pool sync.Pool
}

func (p *simpleBufferPool) Get() interface{} {
	v := p.pool.Get()
	if v == nil {
		return make([]byte, 2048)
	}
	return v
}

func (p *simpleBufferPool) Put(v interface{}) {
	if buf, ok := v.([]byte); ok {
		p.pool.Put(buf[:0])
	} else {
		p.pool.Put(v)
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	WriteBufferPool: &simpleBufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 2048)
			},
		},
	},
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type ClientMessage struct {
	Type    string          `json:"type"`
	GameID  string          `json:"gameId,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type ServerMessage struct {
	Type    string      `json:"type"`
	GameID  string      `json:"gameId,omitempty"`
	Payload interface{} `json:"payload,omitempty"`
}

type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	player *domain.Player
	mu     sync.Mutex
}

func (c *Client) ReadPump() {
	defer func() {
		c.hub.Unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	if err := c.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		log.Printf("SetReadDeadline error: %v", err)
	}
	
	c.conn.SetPingHandler(func(appData string) error {
		if err := c.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
			return err
		}
		return c.conn.WriteMessage(websocket.PongMessage, []byte(appData))
	})
	
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("websocket read error: %v", err)
			}
			break
		}

		var msg ClientMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("failed to parse message: %v", err)
			c.SendError("Invalid message format")
			continue
		}

		c.hub.HandleMessage(c, &msg)
	}
}

func (c *Client) SendJSON(msg *ServerMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("failed to marshal server message: %v", err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		log.Printf("SetWriteDeadline error: %v", err)
		return
	}

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Printf("failed to write websocket message: %v", err)
	}
}

func (c *Client) SendError(reason string) {
	c.SendJSON(&ServerMessage{
		Type: "error",
		Payload: map[string]string{
			"message": reason,
		},
	})
}
