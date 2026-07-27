package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"gameserver/internal/application"
	"gameserver/internal/domain"
	"gameserver/internal/infrastructure/cache"
	"gameserver/internal/infrastructure/db"
)

type GameShard struct {
	mu    sync.RWMutex
	games map[string]*GameSession
}

type ShardedGameMap struct {
	shards [256]*GameShard
}

func NewShardedGameMap() *ShardedGameMap {
	m := &ShardedGameMap{}
	for i := 0; i < 256; i++ {
		m.shards[i] = &GameShard{games: make(map[string]*GameSession)}
	}
	return m
}

func (m *ShardedGameMap) getShard(key string) *GameShard {
	// Simple FNV-1a hash
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return m.shards[hash%256]
}

type Hub struct {
	clients   map[string]*Client
	clientsMu sync.RWMutex

	shardedGames *ShardedGameMap

	Register   chan *Client
	Unregister chan *Client

	matchmaker  *application.Matchmaker
	gameService *application.GameService
	redis       *cache.RedisClient
	db          *db.DB
}

func NewHub(
	matchmaker *application.Matchmaker,
	gameService *application.GameService,
	redis *cache.RedisClient,
	database *db.DB,
) *Hub {
	return &Hub{
		clients:      make(map[string]*Client),
		shardedGames: NewShardedGameMap(),
		Register:     make(chan *Client),
		Unregister:   make(chan *Client),
		matchmaker:   matchmaker,
		gameService:  gameService,
		redis:        redis,
		db:           database,
	}
}

func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case client := <-h.Register:
			h.clientsMu.Lock()
			if oldClient, exists := h.clients[client.player.ID]; exists {
				oldClient.SendJSON(&ServerMessage{
					Type: "disconnect",
					Payload: map[string]string{
						"reason": "Logged in from another session",
					},
				})
				oldClient.conn.Close()
			}
			h.clients[client.player.ID] = client
			h.clientsMu.Unlock()
			log.Printf("Player connected: %s (%s)", client.player.Name, client.player.ID)

		case client := <-h.Unregister:
			h.clientsMu.Lock()
			if _, ok := h.clients[client.player.ID]; ok {
				delete(h.clients, client.player.ID)
				log.Printf("Player disconnected: %s (%s)", client.player.Name, client.player.ID)
			}
			h.clientsMu.Unlock()

			h.matchmaker.Leave(client.player.ID)

		case <-ctx.Done():
			return
		}
	}
}

func (h *Hub) HandleMessage(c *Client, msg *ClientMessage) {
	switch msg.Type {
	case "join_matchmaking":
		var payload struct {
			TimeControl string `json:"timeControl"`
		}
		_ = json.Unmarshal(msg.Payload, &payload)

		timeControl, gameType := ParseTimeControl(payload.TimeControl)
		rating := getPlayerRatingForType(c.player, gameType)

		replyChan := h.matchmaker.Join(c.player.ID, rating, timeControl, gameType)
		go func() {
			select {
			case res, ok := <-replyChan:
				if ok && res != nil {
					c.SendJSON(&ServerMessage{
						Type:   "match_found",
						GameID: res.GameID,
						Payload: map[string]string{
							"gameId":        res.GameID,
							"whitePlayerId": res.WhitePlayerID,
							"blackPlayerId": res.BlackPlayerID,
						},
					})
				}
			case <-time.After(5 * time.Minute):
				h.matchmaker.Leave(c.player.ID)
				c.SendError("Matchmaking timeout")
			}
		}()

	case "leave_matchmaking":
		h.matchmaker.Leave(c.player.ID)
		c.SendJSON(&ServerMessage{Type: "left_matchmaking"})

	case "join_game":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.joinGameSession(c, msg.GameID)

	case "make_move":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventMove,
			PlayerID: c.player.ID,
			Payload:  msg.Payload,
		})

	case "resign":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventResign,
			PlayerID: c.player.ID,
			Payload:  msg.Payload,
		})

	default:
		c.SendError(fmt.Sprintf("Unknown action: %s", msg.Type))
	}
}

func (h *Hub) joinGameSession(c *Client, gameID string) {
	shard := h.shardedGames.getShard(gameID)
	shard.mu.Lock()
	session, exists := shard.games[gameID]
	if !exists {
		g, err := h.gameService.GetGame(context.Background(), gameID)
		if err != nil {
			shard.mu.Unlock()
			c.SendError(fmt.Sprintf("Failed to load game: %v", err))
			return
		}

		session = NewGameSession(gameID, g, h.gameService, h.redis, h)
		shard.games[gameID] = session
		go session.Run(context.Background())
	}
	shard.mu.Unlock()

	session.RegisterClient(c)
}

func (h *Hub) forwardToGame(gameID string, event *GameSessionEvent) {
	shard := h.shardedGames.getShard(gameID)
	shard.mu.RLock()
	session, exists := shard.games[gameID]
	shard.mu.RUnlock()

	if exists {
		session.eventChan <- event
	} else {
		log.Printf("Warning: received move for game %s but no active session found locally", gameID)
	}
}

func (h *Hub) removeGameSession(gameID string) {
	shard := h.shardedGames.getShard(gameID)
	shard.mu.Lock()
	delete(shard.games, gameID)
	shard.mu.Unlock()
	log.Printf("Game session %s cleaned up from memory", gameID)
}

type GameEventType string

const (
	EventJoin       GameEventType = "join"
	EventLeave      GameEventType = "leave"
	EventMove       GameEventType = "move"
	EventResign     GameEventType = "resign"
	EventRemoteSync GameEventType = "remote_sync"
)

type GameSessionEvent struct {
	Type     GameEventType
	PlayerID string
	Payload  json.RawMessage
}

type GameSession struct {
	id          string
	game        *domain.ChessGame
	gameService *application.GameService
	redis       *cache.RedisClient
	hub         *Hub
	eventChan   chan *GameSessionEvent
	clients     map[string]*Client
	clientsMu   sync.Mutex
}

func NewGameSession(
	id string,
	game *domain.ChessGame,
	gameService *application.GameService,
	redis *cache.RedisClient,
	hub *Hub,
) *GameSession {
	return &GameSession{
		id:          id,
		game:        game,
		gameService: gameService,
		redis:       redis,
		hub:         hub,
		eventChan:   make(chan *GameSessionEvent, 100),
		clients:     make(map[string]*Client),
	}
}

func (s *GameSession) RegisterClient(c *Client) {
	s.eventChan <- &GameSessionEvent{
		Type:     EventJoin,
		PlayerID: c.player.ID,
	}

	s.clientsMu.Lock()
	s.clients[c.player.ID] = c
	s.clientsMu.Unlock()
}

func (s *GameSession) Run(ctx context.Context) {
	pubsub := s.redis.SubscribeGameEvent(ctx, s.id)
	defer pubsub.Close()

	redisChan := pubsub.Channel()

	go func() {
		for {
			select {
			case msg, ok := <-redisChan:
				if !ok {
					return
				}
				s.eventChan <- &GameSessionEvent{
					Type:    EventRemoteSync,
					Payload: json.RawMessage(msg.Payload),
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	idleTimer := time.NewTimer(5 * time.Minute)
	defer idleTimer.Stop()
	
	abandonTimer := time.NewTimer(60 * time.Second)
	abandonTimer.Stop()

	flagTimer := time.NewTimer(time.Hour * 24)
	flagTimer.Stop()

	resetFlagTimer := func() {
		if s.game.Status != domain.StatusInProgress || s.game.LastMoveTime == nil {
			flagTimer.Stop()
			return
		}
		
		turn := s.game.CurrentTurnPlayerID()
		var remMs int64
		if turn == s.game.WhitePlayerID {
			remMs = s.game.WhiteTimeMs
		} else {
			remMs = s.game.BlackTimeMs
		}
		
		now := time.Now()
		elapsed := now.Sub(*s.game.LastMoveTime).Milliseconds()
		timeLeft := time.Duration(remMs - elapsed) * time.Millisecond
		if timeLeft <= 0 {
			timeLeft = 0
		}
		flagTimer.Reset(timeLeft)
	}

	for {
		select {
		case event := <-s.eventChan:
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(5 * time.Minute)

			s.handleEvent(ctx, event)

			s.clientsMu.Lock()
			count := len(s.clients)
			s.clientsMu.Unlock()
			
			if count == 0 {
				abandonTimer.Reset(60 * time.Second)
			} else {
				if !abandonTimer.Stop() {
					select {
					case <-abandonTimer.C:
					default:
					}
				}
			}

			if s.game.Status == domain.StatusCompleted || s.game.Status == domain.StatusDraw {
				s.updateRatings(ctx)
				time.Sleep(1 * time.Second)
				s.hub.removeGameSession(s.id)
				return
			}
			
			resetFlagTimer()

		case <-flagTimer.C:
			now := time.Now()
			if s.game.CheckFlag(now) {
				s.game.Flag(now)
				_ = s.gameService.SaveGame(ctx, s.game)
				
				var winnerStr *string
				if s.game.Winner != nil {
					str := string(*s.game.Winner)
					winnerStr = &str
				}
				
				broadcastPayload, _ := json.Marshal(map[string]interface{}{
					"type":   "game_over",
					"gameId": s.id,
					"sender": "server",
					"payload": map[string]interface{}{
						"status": s.game.Status,
						"winner": winnerStr,
						"reason": "Timeout",
					},
				})
				_ = s.redis.PublishGameEvent(ctx, s.id, broadcastPayload)
				
				s.updateRatings(ctx)
				time.Sleep(1 * time.Second)
				s.hub.removeGameSession(s.id)
				return
			} else {
				resetFlagTimer()
			}
			
		case <-abandonTimer.C:
			log.Printf("Game session %s abandoned by players", s.id)
			// Automatically draw or give win to remaining player if applicable, for simplicity just abort here
			s.game.Status = domain.StatusAbandoned
			_ = s.gameService.SaveGame(ctx, s.game)
			s.hub.removeGameSession(s.id)
			return

		case <-idleTimer.C:
			s.clientsMu.Lock()
			count := len(s.clients)
			s.clientsMu.Unlock()

			if count == 0 {
				log.Printf("Game session %s idle timeout, shutting down loop", s.id)
				s.hub.removeGameSession(s.id)
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (s *GameSession) handleEvent(ctx context.Context, event *GameSessionEvent) {
	switch event.Type {
	case EventJoin:
		log.Printf("Player %s joined game session %s", event.PlayerID, s.id)
		s.clientsMu.Lock()
		c, exists := s.clients[event.PlayerID]
		s.clientsMu.Unlock()

		if exists {
			var winnerStr *string
			if s.game.Winner != nil {
				str := string(*s.game.Winner)
				winnerStr = &str
			}
			c.SendJSON(&ServerMessage{
				Type:   "game_state",
				GameID: s.id,
				Payload: map[string]interface{}{
					"fen":                 s.game.FEN,
					"pgn":                 s.game.PGN,
					"status":              s.game.Status,
					"winner":              winnerStr,
					"timeControl":         s.game.TimeControl,
					"gameType":            s.game.GameType,
					"whitePlayerId":       s.game.WhitePlayerID,
					"blackPlayerId":       s.game.BlackPlayerID,
					"serverWhiteMs":       s.game.WhiteTimeMs,
					"serverBlackMs":       s.game.BlackTimeMs,
					"serverSyncTimestamp": time.Now().UnixMilli(),
				},
			})
		}

	case EventMove:
		var payload struct {
			Move string `json:"move"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: "Invalid move payload"})
			return
		}

		err := s.game.MakeMove(event.PlayerID, payload.Move)
		if err != nil {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: err.Error()})
			return
		}

		if err := s.gameService.SaveGame(ctx, s.game); err != nil {
			log.Printf("Failed to save game state: %v", err)
		}

		var winnerStr *string
		if s.game.Winner != nil {
			str := string(*s.game.Winner)
			winnerStr = &str
		}
		broadcastPayload, _ := json.Marshal(map[string]interface{}{
			"type":   "move_made",
			"gameId": s.id,
			"sender": event.PlayerID,
			"payload": map[string]interface{}{
				"move":                payload.Move,
				"fen":                 s.game.FEN,
				"pgn":                 s.game.PGN,
				"status":              s.game.Status,
				"winner":              winnerStr,
				"serverWhiteMs":       s.game.WhiteTimeMs,
				"serverBlackMs":       s.game.BlackTimeMs,
				"serverSyncTimestamp": time.Now().UnixMilli(),
			},
		})
		_ = s.redis.PublishGameEvent(ctx, s.id, broadcastPayload)

	case EventResign:
		err := s.game.Resign(event.PlayerID)
		if err != nil {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: err.Error()})
			return
		}

		if err := s.gameService.SaveGame(ctx, s.game); err != nil {
			log.Printf("Failed to save game state: %v", err)
		}

		var winnerStr *string
		if s.game.Winner != nil {
			str := string(*s.game.Winner)
			winnerStr = &str
		}
		broadcastPayload, _ := json.Marshal(map[string]interface{}{
			"type":   "game_over",
			"gameId": s.id,
			"sender": event.PlayerID,
			"payload": map[string]interface{}{
				"status": s.game.Status,
				"winner": winnerStr,
				"reason": fmt.Sprintf("Player %s resigned", event.PlayerID),
			},
		})
		_ = s.redis.PublishGameEvent(ctx, s.id, broadcastPayload)

	case EventRemoteSync:
		var msg struct {
			Type    string          `json:"type"`
			GameID  string          `json:"gameId"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(event.Payload, &msg); err != nil {
			return
		}

		if msg.Type == "move_made" {
			var movePayload struct {
				Move          string            `json:"move"`
				FEN           string            `json:"fen"`
				PGN           string            `json:"pgn"`
				Status        domain.GameStatus `json:"status"`
				Winner        *string           `json:"winner"`
				ServerWhiteMs int64             `json:"serverWhiteMs"`
				ServerBlackMs int64             `json:"serverBlackMs"`
			}
			if err := json.Unmarshal(msg.Payload, &movePayload); err == nil {
				var winner *domain.GameWinner
				if movePayload.Winner != nil {
					w := domain.GameWinner(*movePayload.Winner)
					winner = &w
				}
				
				wTimeMs := movePayload.ServerWhiteMs
				if wTimeMs == 0 {
					wTimeMs = s.game.WhiteTimeMs
				}
				bTimeMs := movePayload.ServerBlackMs
				if bTimeMs == 0 {
					bTimeMs = s.game.BlackTimeMs
				}

				now := time.Now()
				s.game, _ = domain.LoadChessGame(
					s.id,
					s.game.WhitePlayerID,
					s.game.BlackPlayerID,
					movePayload.FEN,
					movePayload.PGN,
					s.game.Status,
					winner,
					s.game.TimeControl,
					s.game.GameType,
					s.game.CreatedAt,
					now,
					wTimeMs,
					bTimeMs,
					s.game.IncrementMs,
					&now,
				)
			}
		}

		s.broadcastLocal(&ServerMessage{
			Type:    msg.Type,
			GameID:  s.id,
			Payload: msg.Payload,
		})
	}
}

func (s *GameSession) sendToPlayer(playerID string, msg *ServerMessage) {
	s.clientsMu.Lock()
	c, exists := s.clients[playerID]
	s.clientsMu.Unlock()

	if exists {
		c.SendJSON(msg)
	}
}

func (s *GameSession) broadcastLocal(msg *ServerMessage) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	for _, client := range s.clients {
		client.SendJSON(msg)
	}
}

// updateRatings processes Glicko-1 updates for both players
func (s *GameSession) updateRatings(ctx context.Context) {
	if s.game.WhitePlayerID == "" || s.game.BlackPlayerID == "" {
		return
	}

	// Under chess.com rules, a game is aborted if ended before both players made their first move.
	// We check if history has less than 2 moves (White move 1 + Black move 1).
	if s.game.MovesCount() < 2 {
		log.Printf("Game %s aborted (moves %d < 2). Ratings are unaffected.", s.id, s.game.MovesCount())
		return
	}

	pA, err := s.hub.db.GetPlayer(ctx, s.game.WhitePlayerID)
	if err != nil {
		log.Printf("Failed to load player A (%s) for rating update: %v", s.game.WhitePlayerID, err)
		return
	}

	pB, err := s.hub.db.GetPlayer(ctx, s.game.BlackPlayerID)
	if err != nil {
		log.Printf("Failed to load player B (%s) for rating update: %v", s.game.BlackPlayerID, err)
		return
	}

	var rA, rdA float64
	var rB, rdB float64
	var lastActiveA, lastActiveB time.Time

	switch s.game.GameType {
	case "BULLET":
		rA = float64(pA.RatingBullet)
		rdA = pA.RDBullet
		lastActiveA = pA.LastActiveBullet
		rB = float64(pB.RatingBullet)
		rdB = pB.RDBullet
		lastActiveB = pB.LastActiveBullet
	case "BLITZ":
		rA = float64(pA.RatingBlitz)
		rdA = pA.RDBlitz
		lastActiveA = pA.LastActiveBlitz
		rB = float64(pB.RatingBlitz)
		rdB = pB.RDBlitz
		lastActiveB = pB.LastActiveBlitz
	case "DAILY":
		rA = float64(pA.RatingDaily)
		rdA = pA.RDDaily
		lastActiveA = pA.LastActiveDaily
		rB = float64(pB.RatingDaily)
		rdB = pB.RDDaily
		lastActiveB = pB.LastActiveDaily
	default: // RAPID
		rA = float64(pA.RatingRapid)
		rdA = pA.RDRapid
		lastActiveA = pA.LastActiveRapid
		rB = float64(pB.RatingRapid)
		rdB = pB.RDRapid
		lastActiveB = pB.LastActiveRapid
	}

	rdA = domain.DecayRD(rdA, lastActiveA)
	rdB = domain.DecayRD(rdB, lastActiveB)

	outcomeA := 0.5
	if s.game.Winner != nil {
		if *s.game.Winner == domain.WinnerWhite {
			outcomeA = 1.0
		} else if *s.game.Winner == domain.WinnerBlack {
			outcomeA = 0.0
		}
	}
	outcomeB := 1.0 - outcomeA

	newRA, newRDA := domain.CalculateNewRatingAndRD(rA, rdA, rB, rdB, outcomeA)
	newRB, newRDB := domain.CalculateNewRatingAndRD(rB, rdB, rA, rdA, outcomeB)

	now := time.Now()
	switch s.game.GameType {
	case "BULLET":
		pA.RatingBullet = int(math.Round(newRA))
		pA.RDBullet = newRDA
		pA.LastActiveBullet = now
		pB.RatingBullet = int(math.Round(newRB))
		pB.RDBullet = newRDB
		pB.LastActiveBullet = now
	case "BLITZ":
		pA.RatingBlitz = int(math.Round(newRA))
		pA.RDBlitz = newRDA
		pA.LastActiveBlitz = now
		pB.RatingBlitz = int(math.Round(newRB))
		pB.RDBlitz = newRDB
		pB.LastActiveBlitz = now
	case "DAILY":
		pA.RatingDaily = int(math.Round(newRA))
		pA.RDDaily = newRDA
		pA.LastActiveDaily = now
		pB.RatingDaily = int(math.Round(newRB))
		pB.RDDaily = newRDB
		pB.LastActiveDaily = now
	default: // RAPID
		pA.RatingRapid = int(math.Round(newRA))
		pA.RDRapid = newRDA
		pA.LastActiveRapid = now
		pB.RatingRapid = int(math.Round(newRB))
		pB.RDRapid = newRDB
		pB.LastActiveRapid = now
	}

	pA.Rating = int(math.Round(newRA))
	pB.Rating = int(math.Round(newRB))

	if err := s.hub.db.UpdatePlayerRatings(ctx, pA, pB, s.game.GameType); err != nil {
		log.Printf("Failed to update player ratings: %v", err)
		return
	}

	log.Printf("[Glicko-1] Game %s (%s) concluded. Player A (%s): %d -> %d, Player B (%s): %d -> %d",
		s.id, s.game.GameType, pA.ID, int(math.Round(rA)), pA.Rating, pB.ID, int(math.Round(rB)), pB.Rating)
}

func ParseTimeControl(tc string) (string, string) {
	if tc == "" {
		return "10|0", "RAPID"
	}

	if strings.Contains(strings.ToLower(tc), "day") {
		return tc, "DAILY"
	}

	parts := strings.FieldsFunc(tc, func(r rune) bool {
		return r == '|' || r == '+'
	})

	if len(parts) == 0 {
		return tc, "RAPID"
	}

	baseMinutes, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return tc, "RAPID"
	}

	incrementSeconds := 0.0
	if len(parts) > 1 {
		inc, err := strconv.ParseFloat(parts[1], 64)
		if err == nil {
			incrementSeconds = inc
		}
	}

	totalSeconds := baseMinutes*60.0 + 40.0*incrementSeconds

	var gameType string
	if totalSeconds < 180.0 {
		gameType = "BULLET"
	} else if totalSeconds < 600.0 {
		gameType = "BLITZ"
	} else {
		gameType = "RAPID"
	}

	return tc, gameType
}

func getPlayerRatingForType(p *domain.Player, gameType string) int {
	switch gameType {
	case "BULLET":
		return p.RatingBullet
	case "BLITZ":
		return p.RatingBlitz
	case "DAILY":
		return p.RatingDaily
	default:
		return p.RatingRapid
	}
}
