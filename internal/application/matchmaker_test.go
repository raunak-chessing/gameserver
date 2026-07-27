package application

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type MockGameCreator struct {
	mu      sync.Mutex
	created []struct {
		white string
		black string
	}
}

func (m *MockGameCreator) CreateNewGame(ctx context.Context, whitePlayerID, blackPlayerID string, timeControl string, gameType string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.created = append(m.created, struct {
		white string
		black string
	}{whitePlayerID, blackPlayerID})
	return "mock-game-id", nil
}

type FailGameCreator struct{}

func (f FailGameCreator) CreateNewGame(ctx context.Context, whitePlayerID, blackPlayerID string, timeControl string, gameType string) (string, error) {
	return "", errors.New("creation failed")
}

func TestMatchmakerImmediate(t *testing.T) {
	creator := &MockGameCreator{}

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis is not running, skipping matchmaker test")
	}

	m := NewMatchmaker(creator, rdb)
	defer cancel()

	go m.Start(ctx)

	replyA := m.Join("player-a", 1500, "10|0", "RAPID")
	replyB := m.Join("player-b", 1520, "10|0", "RAPID")

	select {
	case resA := <-replyA:
		if resA.GameID != "mock-game-id" {
			t.Errorf("expected game ID 'mock-game-id', got '%s'", resA.GameID)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("matchmaking timed out for Player A")
	}

	select {
	case resB := <-replyB:
		if resB.GameID != "mock-game-id" {
			t.Errorf("expected game ID 'mock-game-id', got '%s'", resB.GameID)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("matchmaking timed out for Player B")
	}

	creator.mu.Lock()
	defer creator.mu.Unlock()
	if len(creator.created) != 1 {
		t.Errorf("expected 1 game to be created, got %d", len(creator.created))
	}
}
