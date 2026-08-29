package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"gameserver/internal/application"
	"gameserver/internal/domain"
	"gameserver/internal/infrastructure/cache"
	"gameserver/internal/infrastructure/db"
	"gameserver/internal/metrics"

	"github.com/getsentry/sentry-go"
)

func recoverAndLog(goroutine string) {
	if r := recover(); r != nil {
		log.Printf("recovered from panic in %s: %v\n%s", goroutine, r, debug.Stack())
		sentry.CurrentHub().Recover(r)
		sentry.Flush(2 * time.Second)
	}
}

const (
	gameEventChanBuffer       = 500
	matchmakingWaitTimeout    = 5 * time.Minute
	sessionIdleTimeout        = 5 * time.Minute
	abandonTimeout            = 60 * time.Second
	flagCheckInterval         = 24 * time.Hour
	stalledSessionSendTimeout = 2 * time.Second
	gameEndedEventsChannel    = "gameserver:events"
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

// ActiveGameSessionCount returns the number of in-memory game sessions
// currently held across all shards on this instance.
func (h *Hub) ActiveGameSessionCount() int {
	count := 0
	for _, shard := range h.shardedGames.shards {
		shard.mu.RLock()
		count += len(shard.games)
		shard.mu.RUnlock()
	}
	return count
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
			oldClient, exists := h.clients[client.player.ID]
			h.clients[client.player.ID] = client
			h.clientsMu.Unlock()

			if exists {
				oldClient.SendJSON(&ServerMessage{
					Type: "disconnect",
					Payload: map[string]string{
						"reason": "Logged in from another session",
					},
				})
				oldClient.conn.Close()
			}
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
		if c.player.IsFlaggedForCheating {
			c.SendJSON(&ServerMessage{
				Type:    "error",
				Payload: "Your account has been flagged for fair play violations. You cannot join matchmaking.",
			})
			return
		}

		var payload struct {
			TimeControl string `json:"timeControl"`
			Variant     string `json:"variant"`
		}
		_ = json.Unmarshal(msg.Payload, &payload)

		timeControl, gameType := ParseTimeControl(payload.TimeControl)
		rating := getPlayerRatingForType(c.player, gameType)
		variant := parseGameVariant(payload.Variant)

		replyChan := h.matchmaker.Join(c.player.ID, rating, timeControl, gameType, variant)
		go func() {
			defer recoverAndLog("matchmaking wait goroutine")
			select {
			case res, ok := <-replyChan:
				if ok && res != nil {
					if res.Err != "" {
						c.SendError(res.Err)
					} else {
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
				}
			case <-time.After(matchmakingWaitTimeout):
				h.matchmaker.Leave(c.player.ID)
				c.SendError("Matchmaking timeout")
			}
		}()

	case "leave_matchmaking":
		h.matchmaker.Leave(c.player.ID)
		c.SendJSON(&ServerMessage{Type: "left_matchmaking"})

	case "sendGameMessage":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventChat,
			PlayerID: c.player.ID,
			Payload:  msg.Payload,
		})

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

	case "abort":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventAbort,
			PlayerID: c.player.ID,
		})

	case "requestRematch":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventRequestRematch,
			PlayerID: c.player.ID,
		})

	case "acceptRematch":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventAcceptRematch,
			PlayerID: c.player.ID,
		})

	case "declineRematch":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventDeclineRematch,
			PlayerID: c.player.ID,
		})

	case "requestUndo":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventRequestUndo,
			PlayerID: c.player.ID,
		})

	case "acceptUndo":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventAcceptUndo,
			PlayerID: c.player.ID,
		})

	case "declineUndo":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventDeclineUndo,
			PlayerID: c.player.ID,
		})

	case "offer_draw":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventOfferDraw,
			PlayerID: c.player.ID,
		})

	case "accept_draw":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventAcceptDraw,
			PlayerID: c.player.ID,
		})

	case "decline_draw":
		if msg.GameID == "" {
			c.SendError("Missing gameId")
			return
		}
		h.forwardToGame(msg.GameID, &GameSessionEvent{
			Type:     EventDeclineDraw,
			PlayerID: c.player.ID,
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
		go func() {
			defer recoverAndLog(fmt.Sprintf("game session %s", gameID))
			session.Run(context.Background())
		}()
	}
	shard.mu.Unlock()

	session.RegisterClient(c)
}

func (h *Hub) forwardToGame(gameID string, event *GameSessionEvent) {
	shard := h.shardedGames.getShard(gameID)
	shard.mu.RLock()
	session, exists := shard.games[gameID]
	shard.mu.RUnlock()

	if !exists {
		log.Printf("Warning: received event for game %s but no active session found locally", gameID)
		return
	}

	select {
	case session.eventChan <- event:
	case <-time.After(stalledSessionSendTimeout):
		log.Printf("Warning: dropped event type %v for stalled game session %s", event.Type, gameID)
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
	EventJoin           GameEventType = "join"
	EventLeave          GameEventType = "leave"
	EventMove           GameEventType = "move"
	EventResign         GameEventType = "resign"
	EventAbort          GameEventType = "abort"
	EventOfferDraw      GameEventType = "offer_draw"
	EventAcceptDraw     GameEventType = "accept_draw"
	EventDeclineDraw    GameEventType = "decline_draw"
	EventRequestRematch GameEventType = "request_rematch"
	EventAcceptRematch  GameEventType = "accept_rematch"
	EventDeclineRematch GameEventType = "decline_rematch"
	EventRequestUndo    GameEventType = "request_undo"
	EventAcceptUndo     GameEventType = "accept_undo"
	EventDeclineUndo    GameEventType = "decline_undo"
	EventRemoteSync     GameEventType = "remote_sync"
	EventChat           GameEventType = "chat"
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
		eventChan:   make(chan *GameSessionEvent, gameEventChanBuffer),
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
		defer recoverAndLog(fmt.Sprintf("redis pubsub consumer for game %s", s.id))
		for {
			select {
			case msg, ok := <-redisChan:
				if !ok {
					return
				}
				event := &GameSessionEvent{
					Type:    EventRemoteSync,
					Payload: json.RawMessage(msg.Payload),
				}
				select {
				case s.eventChan <- event:
				case <-time.After(stalledSessionSendTimeout):
					log.Printf("Warning: dropped remote sync event for stalled game session %s", s.id)
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	idleTimer := time.NewTimer(sessionIdleTimeout)
	defer idleTimer.Stop()

	abandonTimer := time.NewTimer(abandonTimeout)
	abandonTimer.Stop()

	flagTimer := time.NewTimer(flagCheckInterval)
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
		timeLeft := time.Duration(remMs-elapsed) * time.Millisecond
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
			idleTimer.Reset(sessionIdleTimeout)

			s.handleEvent(ctx, event)

			s.clientsMu.Lock()
			count := len(s.clients)
			s.clientsMu.Unlock()

			if count == 0 {
				abandonTimer.Reset(abandonTimeout)
			} else {
				if !abandonTimer.Stop() {
					select {
					case <-abandonTimer.C:
					default:
					}
				}
			}

			if s.game.Status == domain.StatusCompleted || s.game.Status == domain.StatusDraw {
				application.UpdateRatings(ctx, s.hub.db, s.game, s.id)
				s.publishGameEnded(ctx)
				time.AfterFunc(1*time.Second, func() {
					s.hub.removeGameSession(s.id)
				})
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
				s.publishOrLog(ctx, broadcastPayload)

				application.UpdateRatings(ctx, s.hub.db, s.game, s.id)
				s.publishGameEnded(ctx)
				time.AfterFunc(1*time.Second, func() {
					s.hub.removeGameSession(s.id)
				})
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
					"variant":             s.game.Variant,
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
		metrics.MovesProcessed.Inc()

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
				"whiteChecks":         s.game.WhiteChecks,
				"blackChecks":         s.game.BlackChecks,
				"serverWhiteMs":       s.game.WhiteTimeMs,
				"serverBlackMs":       s.game.BlackTimeMs,
				"serverSyncTimestamp": time.Now().UnixMilli(),
			},
		})
		s.publishOrLog(ctx, broadcastPayload)

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
		s.publishOrLog(ctx, broadcastPayload)

	case EventAbort:
		if s.game.MovesCount() > 0 {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: "Cannot abort game after moves have been played"})
			return
		}
		err := s.game.Abort()
		if err != nil {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: err.Error()})
			return
		}

		if err := s.gameService.SaveGame(ctx, s.game); err != nil {
			log.Printf("Failed to save game state: %v", err)
		}

		broadcastPayload, _ := json.Marshal(map[string]interface{}{
			"type":   "game_aborted",
			"gameId": s.id,
			"sender": event.PlayerID,
			"payload": map[string]interface{}{
				"status": s.game.Status,
				"reason": fmt.Sprintf("Player %s aborted the game", event.PlayerID),
			},
		})
		s.publishOrLog(ctx, broadcastPayload)

	case EventOfferDraw:
		broadcastPayload, _ := json.Marshal(map[string]interface{}{
			"type":   "draw_offered",
			"gameId": s.id,
			"sender": event.PlayerID,
		})
		s.publishOrLog(ctx, broadcastPayload)

	case EventAcceptDraw:
		if event.PlayerID != s.game.WhitePlayerID && event.PlayerID != s.game.BlackPlayerID {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: "Not a player in this game"})
			return
		}

		err := s.game.Draw()
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
				"reason": "Draw by agreement",
			},
		})
		s.publishOrLog(ctx, broadcastPayload)

	case EventDeclineDraw:
		broadcastPayload, _ := json.Marshal(map[string]interface{}{
			"type":   "draw_declined",
			"gameId": s.id,
			"sender": event.PlayerID,
		})
		s.publishOrLog(ctx, broadcastPayload)

	case EventRequestUndo:
		opponentID, ok := s.opponentOf(event.PlayerID)
		if !ok {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: "Not a player in this game"})
			return
		}
		s.sendToPlayer(opponentID, &ServerMessage{Type: "undoRequested", GameID: s.id})

	case EventDeclineUndo:
		opponentID, ok := s.opponentOf(event.PlayerID)
		if !ok {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: "Not a player in this game"})
			return
		}
		s.sendToPlayer(opponentID, &ServerMessage{Type: "undoDeclined", GameID: s.id})

	case EventAcceptUndo:
		if event.PlayerID != s.game.WhitePlayerID && event.PlayerID != s.game.BlackPlayerID {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: "Not a player in this game"})
			return
		}

		if err := s.game.UndoLastMove(); err != nil {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: err.Error()})
			return
		}

		if err := s.gameService.SaveGame(ctx, s.game); err != nil {
			log.Printf("Failed to save game state: %v", err)
		}

		broadcastPayload, _ := json.Marshal(map[string]interface{}{
			"type":   "opponentUndo",
			"gameId": s.id,
			"sender": event.PlayerID,
			"payload": map[string]interface{}{
				"fen": s.game.FEN,
				"pgn": s.game.PGN,
			},
		})
		s.publishOrLog(ctx, broadcastPayload)

	case EventRequestRematch:
		broadcastPayload, _ := json.Marshal(map[string]interface{}{
			"type":   "rematchRequested",
			"gameId": s.id,
			"sender": event.PlayerID,
		})
		s.publishOrLog(ctx, broadcastPayload)

	case EventDeclineRematch:
		broadcastPayload, _ := json.Marshal(map[string]interface{}{
			"type":   "rematch_declined",
			"gameId": s.id,
			"sender": event.PlayerID,
		})
		s.publishOrLog(ctx, broadcastPayload)

	case EventChat:
		var payload struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: "Invalid chat payload"})
			return
		}

		p, err := s.hub.db.GetPlayer(ctx, event.PlayerID)
		if err != nil {
			return
		}

		broadcastPayload, _ := json.Marshal(map[string]interface{}{
			"type":   "gameMessage",
			"gameId": s.id,
			"payload": map[string]interface{}{
				"sender":    p.Name,
				"message":   payload.Message,
				"timestamp": time.Now().Format(time.RFC3339),
			},
		})
		s.publishOrLog(ctx, broadcastPayload)

	case EventAcceptRematch:
		if event.PlayerID != s.game.WhitePlayerID && event.PlayerID != s.game.BlackPlayerID {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: "Not a player in this game"})
			return
		}
		// Create new game swapping colors
		newGameID, err := s.gameService.CreateNewGame(
			ctx,
			s.game.BlackPlayerID, // new white
			s.game.WhitePlayerID, // new black
			s.game.TimeControl,
			s.game.GameType,
			s.game.Variant,
		)
		if err != nil {
			s.sendToPlayer(event.PlayerID, &ServerMessage{Type: "error", Payload: "Failed to create rematch game"})
			log.Printf("Failed to create rematch game: %v", err)
			return
		}

		broadcastPayload, _ := json.Marshal(map[string]interface{}{
			"type":   "rematchAccepted",
			"gameId": s.id,
			"sender": event.PlayerID,
			"payload": map[string]interface{}{
				"newGameId": newGameID,
			},
		})
		s.publishOrLog(ctx, broadcastPayload)

	case EventRemoteSync:
		var msg struct {
			Type    string          `json:"type"`
			GameID  string          `json:"gameId"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(event.Payload, &msg); err != nil {
			return
		}

		syncOK := true

		if msg.Type == "move_made" {
			var movePayload struct {
				Move          string            `json:"move"`
				FEN           string            `json:"fen"`
				PGN           string            `json:"pgn"`
				Status        domain.GameStatus `json:"status"`
				Winner        *string           `json:"winner"`
				WhiteChecks   int               `json:"whiteChecks"`
				BlackChecks   int               `json:"blackChecks"`
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
				reloaded, loadErr := domain.LoadChessGame(domain.LoadChessGameParams{
					ID:            s.id,
					WhitePlayerID: s.game.WhitePlayerID,
					BlackPlayerID: s.game.BlackPlayerID,
					FEN:           movePayload.FEN,
					PGN:           movePayload.PGN,
					Status:        s.game.Status,
					Winner:        winner,
					TimeControl:   s.game.TimeControl,
					GameType:      s.game.GameType,
					Variant:       s.game.Variant,
					WhiteChecks:   movePayload.WhiteChecks,
					BlackChecks:   movePayload.BlackChecks,
					CreatedAt:     s.game.CreatedAt,
					UpdatedAt:     now,
					WhiteTimeMs:   wTimeMs,
					BlackTimeMs:   bTimeMs,
					IncrementMs:   s.game.IncrementMs,
					LastMoveTime:  &now,
				})
				if loadErr != nil {
					log.Printf("Failed to apply remote sync for game %s: %v", s.id, loadErr)
					syncOK = false
				} else {
					s.game = reloaded
				}
			} else {
				syncOK = false
			}
		}

		if syncOK {
			s.broadcastLocal(&ServerMessage{
				Type:    msg.Type,
				GameID:  s.id,
				Payload: msg.Payload,
			})
		}
	}
}

func (s *GameSession) opponentOf(playerID string) (string, bool) {
	switch playerID {
	case s.game.WhitePlayerID:
		return s.game.BlackPlayerID, true
	case s.game.BlackPlayerID:
		return s.game.WhitePlayerID, true
	default:
		return "", false
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

func (s *GameSession) publishOrLog(ctx context.Context, payload []byte) {
	if err := s.redis.PublishGameEvent(ctx, s.id, payload); err != nil {
		log.Printf("Failed to publish game event for game %s: %v", s.id, err)
	}
}

func (s *GameSession) publishGameEnded(ctx context.Context) {
	var winner *string
	if s.game.Winner != nil {
		w := string(*s.game.Winner)
		winner = &w
	}

	payload, err := json.Marshal(map[string]interface{}{
		"type":   "game_ended",
		"gameId": s.id,
		"winner": winner,
	})
	if err != nil {
		log.Printf("Failed to marshal game_ended event for game %s: %v", s.id, err)
		return
	}

	if err := s.redis.PublishGlobalEvent(ctx, gameEndedEventsChannel, payload); err != nil {
		log.Printf("Failed to publish game_ended event for game %s: %v", s.id, err)
	}
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

func parseGameVariant(v string) domain.GameVariant {
	switch domain.GameVariant(v) {
	case domain.VariantChess960, domain.VariantThreeCheck, domain.VariantKingOfTheHill:
		return domain.GameVariant(v)
	default:
		return domain.VariantStandard
	}
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
