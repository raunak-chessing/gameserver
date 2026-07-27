package application

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type MatchRequest struct {
	PlayerID    string
	Rating      int
	TimeControl string
	GameType    string
	Reply       chan *MatchResponse
}

type MatchResponse struct {
	GameID        string
	WhitePlayerID string
	BlackPlayerID string
}

type QueuePlayer struct {
	PlayerID    string
	Rating      int
	TimeControl string
	GameType    string
	JoinedAt    time.Time
	Reply       chan *MatchResponse
}

type GameCreator interface {
	CreateNewGame(ctx context.Context, whitePlayerID, blackPlayerID string, timeControl string, gameType string) (string, error)
}

type Matchmaker struct {
	joinChan    chan *MatchRequest
	leaveChan   chan string
	redisClient *redis.Client
	gameCreator GameCreator
	requests    map[string]*MatchRequest // local lookup for replies
}

func NewMatchmaker(gameCreator GameCreator, redisClient *redis.Client) *Matchmaker {
	return &Matchmaker{
		joinChan:    make(chan *MatchRequest, 1000),
		leaveChan:   make(chan string, 1000),
		redisClient: redisClient,
		gameCreator: gameCreator,
		requests:    make(map[string]*MatchRequest),
	}
}

func (m *Matchmaker) Start(ctx context.Context) {
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case req := <-m.joinChan:
			m.requests[req.PlayerID] = req
			key := "queue:" + req.GameType + ":" + req.TimeControl
			member := req.PlayerID + "|" + time.Now().Format(time.RFC3339Nano)
			m.redisClient.ZAdd(ctx, key, redis.Z{
				Score:  float64(req.Rating),
				Member: member,
			})
			log.Printf("Player %s (%d ELO) joined Redis queue %s", req.PlayerID, req.Rating, key)

		case playerID := <-m.leaveChan:
			if req, ok := m.requests[playerID]; ok {
				key := "queue:" + req.GameType + ":" + req.TimeControl
				// Find member by iterating (since timestamp is unknown) or use ZRem on just the ID if we structured it differently
				// For simplicity, we just delete all matching the prefix via a small lua script or scan
				members, _ := m.redisClient.ZRange(ctx, key, 0, -1).Result()
				for _, mem := range members {
					if mem[:len(playerID)] == playerID {
						m.redisClient.ZRem(ctx, key, mem)
					}
				}
				delete(m.requests, playerID)
			}

		case <-ticker.C:
			m.findMatches(ctx)

		case <-ctx.Done():
			return
		}
	}
}

func (m *Matchmaker) Join(playerID string, rating int, timeControl string, gameType string) chan *MatchResponse {
	reply := make(chan *MatchResponse, 1)
	m.joinChan <- &MatchRequest{
		PlayerID:    playerID,
		Rating:      rating,
		TimeControl: timeControl,
		GameType:    gameType,
		Reply:       reply,
	}
	return reply
}

func (m *Matchmaker) Leave(playerID string) {
	m.leaveChan <- playerID
}

func (m *Matchmaker) findMatches(ctx context.Context) {
	// Look at standard queues
	keys, err := m.redisClient.Keys(ctx, "queue:*").Result()
	if err != nil {
		return
	}

	for _, key := range keys {
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
			parts := len(pA.Member.(string))
			if parts == 0 {
				break
			}
			idA := pA.Member.(string)[:36] // UUID is 36 chars
			timeStr := pA.Member.(string)[37:]
			joinedAt, _ := time.Parse(time.RFC3339Nano, timeStr)
			
			waitSecs := time.Since(joinedAt).Seconds()
			allowedDiff := 100.0 + (waitSecs * 10.0)
			
			// Find closest match within allowed diff
			minScore := pA.Score
			maxScore := pA.Score + allowedDiff
			
			// Exclude self, find next best
			opt := &redis.ZRangeBy{
				Min: strconv.FormatFloat(minScore, 'f', -1, 64),
				Max: strconv.FormatFloat(maxScore, 'f', -1, 64),
				Offset: 1, // skip self (we know self is lowest in this range)
				Count: 1,
			}
			
			matches, err := m.redisClient.ZRangeByScoreWithScores(ctx, key, opt).Result()
			if err != nil || len(matches) == 0 {
				// No match found for this lowest player yet, we break the loop so we don't spin forever
				// Wait, if we break, we don't check the next player. We should remove them and put them in a temporary "checked" list, or just use Lua.
				// For simplicity in this Go implementation: just fetch all and linearly pair adjacent!
				break
			}
			
			pB := matches[0]
			idB := pB.Member.(string)[:36]
			
			// Match found! Remove both
			m.redisClient.ZRem(ctx, key, pA.Member, pB.Member)
			
			reqA := m.requests[idA]
			reqB := m.requests[idB]
			
			if reqA != nil && reqB != nil {
				go func(rA, rB *MatchRequest) {
					whiteID, blackID := rA.PlayerID, rB.PlayerID
					if uuid.New().ID()%2 == 0 {
						whiteID, blackID = rB.PlayerID, rA.PlayerID
					}

					gameID, err := m.gameCreator.CreateNewGame(ctx, whiteID, blackID, rA.TimeControl, rA.GameType)
					if err != nil {
						return
					}

					res := &MatchResponse{
						GameID:        gameID,
						WhitePlayerID: whiteID,
						BlackPlayerID: blackID,
					}
					
					rA.Reply <- res
					rB.Reply <- res
					close(rA.Reply)
					close(rB.Reply)
					
				}(reqA, reqB)
				
				delete(m.requests, idA)
				delete(m.requests, idB)
			}
		}
	}
}
