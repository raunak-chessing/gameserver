package application

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"gameserver/internal/domain"
	"gameserver/internal/metrics"

	"github.com/redis/go-redis/v9"
)

const (
	matchResultsChannel = "matchmaking:results"
	activeQueuesSetKey  = "matchmaking:active_queues"
	queueMetaKeySuffix  = ":meta"
)

type MatchRequest struct {
	PlayerID    string
	Rating      int
	TimeControl string
	GameType    string
	Variant     domain.GameVariant
	Reply       chan *MatchResponse
}

type MatchResponse struct {
	GameID        string
	WhitePlayerID string
	BlackPlayerID string
	Err           string
}

type QueuePlayer struct {
	PlayerID    string
	Rating      int
	TimeControl string
	GameType    string
	Variant     domain.GameVariant
	JoinedAt    time.Time
	Reply       chan *MatchResponse
}

type GameCreator interface {
	CreateNewGame(ctx context.Context, whitePlayerID, blackPlayerID string, timeControl string, gameType string, variant domain.GameVariant) (string, error)
}

// matchmakingResult is published on matchResultsChannel once a match is
// created, so that whichever gameserver instance actually holds each
// player's WebSocket connection can deliver it — the instance that ran
// findMatches and created the game is not necessarily the instance either
// player is connected to.
type matchmakingResult struct {
	PlayerAID     string `json:"playerAId"`
	PlayerBID     string `json:"playerBId"`
	GameID        string `json:"gameId,omitempty"`
	WhitePlayerID string `json:"whitePlayerId,omitempty"`
	BlackPlayerID string `json:"blackPlayerId,omitempty"`
	Err           string `json:"err,omitempty"`
}

type Matchmaker struct {
	joinChan      chan *MatchRequest
	leaveChan     chan string
	resultChan    chan *matchmakingResult
	redisClient   *redis.Client
	gameCreator   GameCreator
	requests      map[string]*MatchRequest
	playerMembers map[string]string // playerID -> Redis sorted set member string
}

func NewMatchmaker(gameCreator GameCreator, redisClient *redis.Client) *Matchmaker {
	return &Matchmaker{
		joinChan:      make(chan *MatchRequest, 1000),
		leaveChan:     make(chan string, 1000),
		resultChan:    make(chan *matchmakingResult, 1000),
		redisClient:   redisClient,
		gameCreator:   gameCreator,
		requests:      make(map[string]*MatchRequest),
		playerMembers: make(map[string]string),
	}
}

func (m *Matchmaker) Start(ctx context.Context) {
	go m.listenForResults(ctx)

	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case req := <-m.joinChan:
			// A player who is already queued and joins again (client retry,
			// reconnect, or a duplicate request) must have their old queue
			// entry cleaned up first. playerMembers is keyed by playerID
			// alone, so a blind overwrite below would lose the only handle
			// on the old Redis member string, orphaning it in the shared
			// queue forever; it would also leave the old Reply channel
			// with no sender or closer, so its waiting goroutine can only
			// ever exit via its own timeout — which then calls Leave with
			// this playerID and evicts the player from the queue they just
			// (re)joined.
			if oldReq, exists := m.requests[req.PlayerID]; exists {
				oldKey := queueKey(oldReq.GameType, oldReq.TimeControl, oldReq.Variant)
				if oldMember, hasMember := m.playerMembers[req.PlayerID]; hasMember {
					m.redisClient.ZRem(ctx, oldKey, oldMember)
				}
				close(oldReq.Reply)
			}

			m.requests[req.PlayerID] = req
			key := queueKey(req.GameType, req.TimeControl, req.Variant)
			member := req.PlayerID + "|" + time.Now().Format(time.RFC3339Nano)
			m.redisClient.ZAdd(ctx, key, redis.Z{
				Score:  float64(req.Rating),
				Member: member,
			})
			m.playerMembers[req.PlayerID] = member
			m.redisClient.SAdd(ctx, activeQueuesSetKey, key)
			m.redisClient.HSet(ctx, key+queueMetaKeySuffix, map[string]interface{}{
				"gameType":    req.GameType,
				"timeControl": req.TimeControl,
				"variant":     string(normalizeVariant(req.Variant)),
			})
			log.Printf("Player %s (%d ELO) joined Redis queue %s", req.PlayerID, req.Rating, key)

		case playerID := <-m.leaveChan:
			if req, ok := m.requests[playerID]; ok {
				key := queueKey(req.GameType, req.TimeControl, req.Variant)
				if member, hasMember := m.playerMembers[playerID]; hasMember {
					m.redisClient.ZRem(ctx, key, member)
					delete(m.playerMembers, playerID)
				}
				delete(m.requests, playerID)
			}

		case result := <-m.resultChan:
			m.deliverResult(result)

		case <-ticker.C:
			if paused, _ := m.redisClient.Get(ctx, "matchmaking:paused").Result(); paused == "1" {
				continue
			}
			m.findMatches(ctx)

		case <-ctx.Done():
			return
		}
	}
}

func (m *Matchmaker) Join(playerID string, rating int, timeControl string, gameType string, variant domain.GameVariant) chan *MatchResponse {
	reply := make(chan *MatchResponse, 1)
	m.joinChan <- &MatchRequest{
		PlayerID:    playerID,
		Rating:      rating,
		TimeControl: timeControl,
		GameType:    gameType,
		Variant:     variant,
		Reply:       reply,
	}
	return reply
}

func normalizeVariant(variant domain.GameVariant) domain.GameVariant {
	if variant == "" {
		return domain.VariantStandard
	}
	return variant
}

func queueKey(gameType, timeControl string, variant domain.GameVariant) string {
	if variant == "" || variant == domain.VariantStandard {
		return "queue:" + gameType + ":" + timeControl
	}
	return "queue:" + gameType + ":" + timeControl + ":" + string(variant)
}

func (m *Matchmaker) Leave(playerID string) {
	m.leaveChan <- playerID
}

// listenForResults subscribes to the cross-instance match-result channel and
// feeds every message into resultChan, so it's processed on Start's single
// goroutine alongside joins/leaves without needing a mutex on requests.
func (m *Matchmaker) listenForResults(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("recovered from panic in matchmaking result listener: %v\n%s", r, debug.Stack())
		}
	}()

	pubsub := m.redisClient.Subscribe(ctx, matchResultsChannel)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var result matchmakingResult
			if err := json.Unmarshal([]byte(msg.Payload), &result); err != nil {
				log.Printf("failed to parse matchmaking result: %v", err)
				continue
			}
			select {
			case m.resultChan <- &result:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// deliverResult hands a match result to whichever of its two players this
// instance actually has a pending request for. It is a no-op for players
// this instance doesn't own, since the same message reaches every instance.
func (m *Matchmaker) deliverResult(result *matchmakingResult) {
	for _, playerID := range [2]string{result.PlayerAID, result.PlayerBID} {
		req, ok := m.requests[playerID]
		if !ok {
			continue
		}

		if result.Err != "" {
			req.Reply <- &MatchResponse{Err: result.Err}
		} else {
			req.Reply <- &MatchResponse{
				GameID:        result.GameID,
				WhitePlayerID: result.WhitePlayerID,
				BlackPlayerID: result.BlackPlayerID,
			}
		}
		close(req.Reply)

		delete(m.requests, playerID)
		delete(m.playerMembers, playerID)
	}
}

func (m *Matchmaker) publishResult(ctx context.Context, result *matchmakingResult) {
	payload, err := json.Marshal(result)
	if err != nil {
		log.Printf("failed to marshal matchmaking result: %v", err)
		return
	}
	if err := m.redisClient.Publish(ctx, matchResultsChannel, payload).Err(); err != nil {
		log.Printf("failed to publish matchmaking result: %v", err)
	}
}

func (m *Matchmaker) findMatches(ctx context.Context) {
	keys, err := m.redisClient.SMembers(ctx, activeQueuesSetKey).Result()
	if err != nil {
		return
	}

	for _, key := range keys {
		meta, err := m.redisClient.HGetAll(ctx, key+queueMetaKeySuffix).Result()
		if err != nil || meta["gameType"] == "" {
			continue
		}
		gameType := meta["gameType"]
		timeControl := meta["timeControl"]
		variant := domain.GameVariant(meta["variant"])

		// Get all players in this queue, sorted by rating
		// This is O(N) to fetch, but pairing is O(N) linear scan instead of O(N^2)
		// To truly be O(log N) per player, we'd pop one and ZRangeByScore for its closest match.
		// For high concurrency, popping and finding closest is best.

		for {
			// Get lowest rated player
			mem, err := m.redisClient.ZRangeWithScores(ctx, key, 0, 0).Result()
			if err != nil || len(mem) == 0 {
				break
			}

			pA := mem[0]
			memberA := pA.Member.(string)
			pipeIdx := strings.Index(memberA, "|")
			if pipeIdx < 1 {
				m.redisClient.ZRem(ctx, key, pA.Member)
				continue
			}
			idA := memberA[:pipeIdx]
			timeStr := memberA[pipeIdx+1:]
			joinedAt, _ := time.Parse(time.RFC3339Nano, timeStr)

			waitSecs := time.Since(joinedAt).Seconds()
			allowedDiff := 100.0 + (waitSecs * 10.0)

			// Find closest match within allowed diff. Fetch a few candidates
			// (not just the single nearest) because same-score ties mean the
			// nearest entry in this range can be pA itself.
			minScore := pA.Score
			maxScore := pA.Score + allowedDiff

			opt := &redis.ZRangeBy{
				Min:   strconv.FormatFloat(minScore, 'f', -1, 64),
				Max:   strconv.FormatFloat(maxScore, 'f', -1, 64),
				Count: 6,
			}

			matches, err := m.redisClient.ZRangeByScoreWithScores(ctx, key, opt).Result()
			if err != nil || len(matches) == 0 {
				break
			}

			var pB redis.Z
			var idB string
			found := false
			for _, cand := range matches {
				cm, ok := cand.Member.(string)
				if !ok {
					continue
				}
				idxB := strings.Index(cm, "|")
				if idxB < 1 {
					m.redisClient.ZRem(ctx, key, cand.Member)
					continue
				}
				candID := cm[:idxB]
				if candID == idA {
					continue
				}
				pB = cand
				idB = candID
				found = true
				break
			}
			if !found {
				// Every candidate in range was this player themself; wait for
				// the next tick rather than spinning on the same queue entry.
				break
			}

			// Match found — but this queue is shared over Redis with every
			// other gameserver instance, and each instance runs its own
			// findMatches on its own ticker. Between the peeks above and
			// this point, another instance could have already matched pA
			// or pB into a different pairing. A single batched ZRem here
			// wouldn't tell us that: it returns a count, not which members
			// it actually removed, so we could proceed to create a game
			// for a player already claimed elsewhere — the classic
			// double-match. Removing each member individually is atomic
			// per call (Redis commands are single-threaded), so the two
			// results below tell us definitively whether we — and not a
			// concurrent instance — actually won each player.
			removedA, _ := m.redisClient.ZRem(ctx, key, pA.Member).Result()
			removedB, _ := m.redisClient.ZRem(ctx, key, pB.Member).Result()

			if removedA == 0 && removedB == 0 {
				// Both already claimed by a concurrent match; nothing to roll back.
				continue
			}
			if removedA == 0 || removedB == 0 {
				// Only claimed one side of the pair. Put it back with its
				// original score and member string (preserving its true
				// join time for fair wait-time scaling) instead of
				// stranding it out of the queue with no match.
				if removedA == 1 {
					m.redisClient.ZAdd(ctx, key, redis.Z{Score: pA.Score, Member: pA.Member})
				}
				if removedB == 1 {
					m.redisClient.ZAdd(ctx, key, redis.Z{Score: pB.Score, Member: pB.Member})
				}
				continue
			}

			go func(idA, idB, timeControl, gameType string, variant domain.GameVariant) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("recovered from panic in match creation goroutine: %v\n%s", r, debug.Stack())
					}
				}()

				whiteID, blackID := idA, idB
				if rand.Intn(2) == 0 {
					whiteID, blackID = idB, idA
				}

				gameID, err := m.gameCreator.CreateNewGame(ctx, whiteID, blackID, timeControl, gameType, variant)
				if err != nil {
					log.Printf("Failed to create game for matched players %s/%s: %v", idA, idB, err)
					m.publishResult(ctx, &matchmakingResult{
						PlayerAID: idA,
						PlayerBID: idB,
						Err:       "Failed to create game, please rejoin the queue",
					})
					return
				}
				metrics.MatchesCreated.Inc()

				m.publishResult(ctx, &matchmakingResult{
					PlayerAID:     idA,
					PlayerBID:     idB,
					GameID:        gameID,
					WhitePlayerID: whiteID,
					BlackPlayerID: blackID,
				})
			}(idA, idB, timeControl, gameType, variant)
		}
	}
}
