package websocket

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"gameserver/internal/domain"
)

func TestParseTimeControl(t *testing.T) {
	tests := []struct {
		input        string
		expectedTC   string
		expectedType string
	}{
		{"", "10|0", "RAPID"},
		{"3|2", "3|2", "BLITZ"},
		{"1|0", "1|0", "BULLET"},
		{"10|0", "10|0", "RAPID"},
		{"30|0", "30|0", "RAPID"},
		{"1 day", "1 day", "DAILY"},
		{"invalid", "invalid", "RAPID"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			tc, gt := ParseTimeControl(tt.input)
			if tc != tt.expectedTC || gt != tt.expectedType {
				t.Errorf("ParseTimeControl(%q) = (%q, %q), want (%q, %q)", tt.input, tc, gt, tt.expectedTC, tt.expectedType)
			}
		})
	}
}

func TestGetPlayerRatingForType(t *testing.T) {
	p := &domain.Player{
		RatingBullet: 1200,
		RatingBlitz:  1300,
		RatingRapid:  1400,
		RatingDaily:  1500,
	}

	if getPlayerRatingForType(p, "BULLET") != 1200 {
		t.Error("BULLET rating should be 1200")
	}
	if getPlayerRatingForType(p, "BLITZ") != 1300 {
		t.Error("BLITZ rating should be 1300")
	}
	if getPlayerRatingForType(p, "RAPID") != 1400 {
		t.Error("RAPID rating should be 1400")
	}
	if getPlayerRatingForType(p, "DAILY") != 1500 {
		t.Error("DAILY rating should be 1500")
	}
	if getPlayerRatingForType(p, "UNKNOWN") != 1400 { // Default is RAPID
		t.Error("UNKNOWN rating should fallback to 1400 (RAPID)")
	}
}

func TestShardedGameMap(t *testing.T) {
	m := NewShardedGameMap()

	key1 := "game-1"
	key2 := "game-2"

	shard1 := m.getShard(key1)
	shard2 := m.getShard(key2)

	if shard1 == nil || shard2 == nil {
		t.Error("Shards should not be nil")
	}

	// Test basic put and get
	shard1.mu.Lock()
	shard1.games[key1] = &GameSession{id: key1}
	shard1.mu.Unlock()

	shard1.mu.RLock()
	session, exists := shard1.games[key1]
	shard1.mu.RUnlock()

	if !exists || session.id != key1 {
		t.Error("Failed to retrieve session from shard")
	}
}

func TestHubRegisterAndUnregister(t *testing.T) {
	hub := &Hub{
		clients:    make(map[string]*Client),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		// Leaving matchmaker nil for basic tests without triggering it
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Run(ctx)

	c := &Client{
		player: &domain.Player{ID: "player-1"},
	}

	// Register
	hub.Register <- c
	time.Sleep(10 * time.Millisecond) // Wait for processing

	hub.clientsMu.RLock()
	_, exists := hub.clients["player-1"]
	hub.clientsMu.RUnlock()

	if !exists {
		t.Error("Client was not registered")
	}

	// Unregister
	hub.clientsMu.Lock()
	delete(hub.clients, "player-1")
	hub.clientsMu.Unlock()

	hub.clientsMu.RLock()
	_, exists = hub.clients["player-1"]
	hub.clientsMu.RUnlock()

	if exists {
		t.Error("Client was not unregistered")
	}
}

func TestGameSessionHandleMove(t *testing.T) {
	session := &GameSession{
		id:        "test-game",
		game:      domain.NewChessGame("test-game", "player-1", "10|0", "RAPID", domain.VariantStandard),
		eventChan: make(chan *GameSessionEvent, 10),
		clients:   make(map[string]*Client),
	}

	_ = session.game.Start("player-2")

	// Test invalid payload
	session.handleEvent(context.Background(), &GameSessionEvent{
		Type:     EventMove,
		PlayerID: "player-1",
		Payload:  json.RawMessage(`invalid json`),
	})

	if session.game.MovesCount() != 0 {
		t.Error("Invalid JSON payload should not result in a move")
	}
}
