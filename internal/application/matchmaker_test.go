package application

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"gameserver/internal/domain"

	"github.com/redis/go-redis/v9"
)

type MockGameCreator struct {
	mu      sync.Mutex
	created []struct {
		white string
		black string
	}
}

func (m *MockGameCreator) CreateNewGame(ctx context.Context, whitePlayerID, blackPlayerID string, timeControl string, gameType string, variant domain.GameVariant) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.created = append(m.created, struct {
		white string
		black string
	}{whitePlayerID, blackPlayerID})
	return "mock-game-id", nil
}

type FailGameCreator struct{}

func (f FailGameCreator) CreateNewGame(ctx context.Context, whitePlayerID, blackPlayerID string, timeControl string, gameType string, variant domain.GameVariant) (string, error) {
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

	replyA := m.Join("player-a", 1500, "10|0", "RAPID", domain.VariantStandard)
	replyB := m.Join("player-b", 1520, "10|0", "RAPID", domain.VariantStandard)

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

// TestMatchmakerCrossInstance simulates two gameserver instances sharing one
// Redis: two independent Matchmaker structs, each with their own in-memory
// requests map, both watching the same Redis-backed queue. Before this
// session's fix, a match found by one instance's findMatches could only be
// delivered to players present in that instance's own local map — a player
// who joined via the other instance would silently never receive a reply.
func TestMatchmakerCrossInstance(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis is not running, skipping matchmaker test")
	}

	creator := &MockGameCreator{}

	instance1 := NewMatchmaker(creator, rdb)
	instance2 := NewMatchmaker(creator, rdb)
	go instance1.Start(ctx)
	go instance2.Start(ctx)

	timeControl := "10|0-cross-instance-test"
	replyA := instance1.Join("cross-instance-player-a", 1500, timeControl, "RAPID", domain.VariantStandard)
	replyB := instance2.Join("cross-instance-player-b", 1520, timeControl, "RAPID", domain.VariantStandard)

	var resA, resB *MatchResponse
	timeout := time.After(5 * time.Second)
	for resA == nil || resB == nil {
		select {
		case r, ok := <-replyA:
			if ok {
				resA = r
			}
		case r, ok := <-replyB:
			if ok {
				resB = r
			}
		case <-timeout:
			t.Fatalf("timed out waiting for a cross-instance match (resA=%v resB=%v)", resA, resB)
		}
	}

	if resA.Err != "" {
		t.Fatalf("player A (instance 1) got an error instead of a match: %s", resA.Err)
	}
	if resB.Err != "" {
		t.Fatalf("player B (instance 2) got an error instead of a match: %s", resB.Err)
	}
	if resA.GameID == "" || resA.GameID != resB.GameID {
		t.Fatalf("expected both players to be matched into the same game, got resA.GameID=%q resB.GameID=%q", resA.GameID, resB.GameID)
	}
}

// TestMatchmakerNoDoubleMatch exercises the double-match race fixed this
// session: multiple gameserver instances (represented here as concurrent
// findMatches calls sharing one Redis-backed queue) racing to claim the
// same players. Before the fix, findMatches removed both members of a
// pairing with a single batched ZRem and trusted the pairing unconditionally
// — but a batched ZRem's count doesn't say which member it actually
// removed, so a player already claimed by a concurrent match could still be
// paired again, creating two separate games for the same player.
func TestMatchmakerNoDoubleMatch(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis is not running, skipping matchmaker test")
	}

	creator := &MockGameCreator{}
	m := NewMatchmaker(creator, rdb)

	gameType := "RAPID"
	timeControl := "10|0-no-double-match-test"
	variant := domain.VariantStandard
	key := queueKey(gameType, timeControl, variant)

	// Guard against leftover state from a prior run against the same local
	// Redis so the seeded player count below stays exact.
	rdb.Del(ctx, key)
	defer rdb.Del(ctx, key)

	const numPlayers = 40
	playerIDs := make([]string, numPlayers)
	for i := 0; i < numPlayers; i++ {
		id := fmt.Sprintf("no-double-match-player-%d", i)
		playerIDs[i] = id
		member := id + "|" + time.Now().Format(time.RFC3339Nano)
		if err := rdb.ZAdd(ctx, key, redis.Z{Score: float64(1500 + i), Member: member}).Err(); err != nil {
			t.Fatalf("failed to seed queue: %v", err)
		}
	}
	rdb.SAdd(ctx, activeQueuesSetKey, key)
	rdb.HSet(ctx, key+queueMetaKeySuffix, map[string]interface{}{
		"gameType":    gameType,
		"timeControl": timeControl,
		"variant":     string(variant),
	})

	// Race several concurrent findMatches calls against the same queue, as
	// would happen across real gameserver instances all ticking around the
	// same time and sharing this Redis-backed queue.
	const numRacers = 5
	var wg sync.WaitGroup
	for i := 0; i < numRacers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.findMatches(ctx)
		}()
	}
	wg.Wait()

	// Game creation happens in its own goroutine per pairing; give any
	// still-in-flight ones a moment to finish.
	deadline := time.Now().Add(3 * time.Second)
	for {
		creator.mu.Lock()
		n := len(creator.created)
		creator.mu.Unlock()
		if n >= numPlayers/2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	creator.mu.Lock()
	defer creator.mu.Unlock()

	seen := make(map[string]int)
	for _, g := range creator.created {
		seen[g.white]++
		seen[g.black]++
	}
	for _, id := range playerIDs {
		if seen[id] > 1 {
			t.Errorf("player %s was matched into %d separate games (double-match)", id, seen[id])
		}
		if seen[id] == 0 {
			t.Errorf("player %s was never matched into a game", id)
		}
	}
	if len(creator.created) != numPlayers/2 {
		t.Errorf("expected %d games to be created, got %d", numPlayers/2, len(creator.created))
	}
}

// TestMatchmakerRejoinDoesNotOrphanQueueEntry exercises the rejoin bug fixed
// this session: joining the same queue twice (client retry, reconnect, or a
// duplicate request) before the first join was matched or left used to leave
// the first Redis queue entry orphaned, since playerMembers is keyed by
// playerID alone and a second Join silently lost the only handle needed to
// remove the first entry. That orphaned entry could later be matched into a
// second, invisible game the player's client is never told about, and the
// abandoned first Reply channel would only ever be cleaned up by its own
// timeout — which evicts the player from the queue they just rejoined.
func TestMatchmakerRejoinDoesNotOrphanQueueEntry(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis is not running, skipping matchmaker test")
	}

	creator := &MockGameCreator{}
	m := NewMatchmaker(creator, rdb)
	go m.Start(ctx)

	timeControl := "10|0-rejoin-test"
	gameType := "RAPID"

	// Guard against leftover state from a prior run against the same local
	// Redis (e.g. a manually interrupted run) so this test's own assertions
	// can't be polluted by unrelated stale entries.
	key := queueKey(gameType, timeControl, domain.VariantStandard)
	rdb.Del(ctx, key)
	defer rdb.Del(ctx, key)

	firstReply := m.Join("rejoin-player", 1500, timeControl, gameType, domain.VariantStandard)
	// Give the Start loop a moment to process the first join before firing
	// the second, so this deterministically exercises "already queued,
	// joins again" rather than racing the join channel send.
	time.Sleep(100 * time.Millisecond)
	secondReply := m.Join("rejoin-player", 1500, timeControl, gameType, domain.VariantStandard)

	select {
	case res, ok := <-firstReply:
		if ok {
			t.Errorf("expected the stale first-join reply channel to be closed with no value, got %v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("stale first-join reply channel was never closed after rejoin")
	}

	partnerReply := m.Join("rejoin-partner", 1520, timeControl, gameType, domain.VariantStandard)

	var resSecond, resPartner *MatchResponse
	timeout := time.After(5 * time.Second)
	for resSecond == nil || resPartner == nil {
		select {
		case r, ok := <-secondReply:
			if ok {
				resSecond = r
			}
		case r, ok := <-partnerReply:
			if ok {
				resPartner = r
			}
		case <-timeout:
			t.Fatalf("timed out waiting for match after rejoin (resSecond=%v resPartner=%v)", resSecond, resPartner)
		}
	}

	if resSecond.GameID == "" || resSecond.GameID != resPartner.GameID {
		t.Fatalf("expected rejoin-player and rejoin-partner to match into the same game, got %q and %q", resSecond.GameID, resPartner.GameID)
	}

	remaining, err := rdb.ZRangeByScore(ctx, key, &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
	if err != nil {
		t.Fatalf("failed to inspect queue: %v", err)
	}
	for _, member := range remaining {
		if strings.HasPrefix(member, "rejoin-player|") {
			t.Errorf("stale queue entry for rejoin-player was left behind: %q", member)
		}
	}
}
