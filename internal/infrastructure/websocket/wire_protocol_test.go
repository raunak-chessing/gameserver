package websocket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gameserver/internal/application"
	"gameserver/internal/domain"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// fakeGameCreator lets this test exercise the real matchmaking wire format
// without needing a live, Postgres-backed GameService.
type fakeGameCreator struct{}

func (fakeGameCreator) CreateNewGame(ctx context.Context, whitePlayerID, blackPlayerID string, timeControl string, gameType string, variant domain.GameVariant) (string, error) {
	return "wire-test-game-id", nil
}

func newWireTestServer(t *testing.T, hub *Hub) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		playerID := r.URL.Query().Get("player")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		c := &Client{
			hub:    hub,
			conn:   conn,
			player: &domain.Player{ID: playerID, Name: playerID},
		}
		hub.Register <- c
		go c.ReadPump()
	})
	return httptest.NewServer(mux)
}

func dialWireTestClient(t *testing.T, server *httptest.Server, playerID string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?player=" + playerID
	header := http.Header{}
	header.Set("Origin", "http://localhost:3000")
	conn, _, err := websocket.DefaultDialer.Dial(url, header)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	return conn
}

// TestWireProtocol_JoinMatchmakingProducesMatchFound drives a real
// join_matchmaking message over a real WebSocket connection through the
// actual server dispatch path (Client.ReadPump -> Hub.HandleMessage ->
// Matchmaker.Join), the same path that silently swallowed mismatched event
// names before this session's fixes. A future rename of the "type" string on
// either side of the wire should fail this test immediately, rather than
// only being caught by manually driving a real frontend against a real
// server.
func TestWireProtocol_JoinMatchmakingProducesMatchFound(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis is not running, skipping wire protocol test")
	}

	matchmaker := application.NewMatchmaker(fakeGameCreator{}, rdb)
	go matchmaker.Start(ctx)

	hub := &Hub{
		clients:      make(map[string]*Client),
		shardedGames: NewShardedGameMap(),
		Register:     make(chan *Client),
		Unregister:   make(chan *Client),
		matchmaker:   matchmaker,
	}
	go hub.Run(ctx)

	server := newWireTestServer(t, hub)
	defer server.Close()

	connA := dialWireTestClient(t, server, "wire-test-player-a")
	defer connA.Close()
	connB := dialWireTestClient(t, server, "wire-test-player-b")
	defer connB.Close()

	// A time control unique to this test so it never shares a matchmaking
	// queue with another package's tests running concurrently against the
	// same Redis instance (go test runs different packages in parallel).
	joinMsg := `{"type":"join_matchmaking","payload":{"timeControl":"10|0-wire-protocol-test","variant":"standard"}}`
	if err := connA.WriteMessage(websocket.TextMessage, []byte(joinMsg)); err != nil {
		t.Fatalf("failed to send join_matchmaking for player A: %v", err)
	}
	if err := connB.WriteMessage(websocket.TextMessage, []byte(joinMsg)); err != nil {
		t.Fatalf("failed to send join_matchmaking for player B: %v", err)
	}

	for _, conn := range []*websocket.Conn{connA, connB} {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("did not receive a response on the wire: %v", err)
		}

		var msg struct {
			Type    string `json:"type"`
			GameID  string `json:"gameId"`
			Payload struct {
				GameID string `json:"gameId"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Fatalf("failed to parse response: %v\nraw: %s", err, raw)
		}

		if msg.Type != "match_found" {
			t.Fatalf("expected a match_found message, got type=%q (raw: %s)", msg.Type, raw)
		}
		if msg.GameID != "wire-test-game-id" && msg.Payload.GameID != "wire-test-game-id" {
			t.Fatalf("expected gameId 'wire-test-game-id' in the response, got: %s", raw)
		}
	}
}
