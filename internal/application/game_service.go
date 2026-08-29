package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gameserver/internal/domain"

	"github.com/google/uuid"
)

type GameRepository interface {
	GetGame(ctx context.Context, id string) (*domain.ChessGame, error)
	SaveGame(ctx context.Context, g *domain.ChessGame) error
}

type RedisCache interface {
	SaveFullGameState(ctx context.Context, gameID string, state []byte, expiration time.Duration) error
	LoadFullGameState(ctx context.Context, gameID string) ([]byte, error)
}

type GameService struct {
	repo  GameRepository
	redis RedisCache
}

func NewGameService(repo GameRepository, redis RedisCache) *GameService {
	return &GameService{repo: repo, redis: redis}
}

func (s *GameService) GetGame(ctx context.Context, id string) (*domain.ChessGame, error) {
	// 1. Try Redis first
	stateBytes, err := s.redis.LoadFullGameState(ctx, id)
	if err == nil && len(stateBytes) > 0 {
		var g domain.ChessGame
		if err := json.Unmarshal(stateBytes, &g); err == nil {
			// Must reconstitute the engine state
			loaded, err := domain.LoadChessGame(domain.LoadChessGameParams{
				ID:            g.ID,
				WhitePlayerID: g.WhitePlayerID,
				BlackPlayerID: g.BlackPlayerID,
				FEN:           g.FEN,
				PGN:           g.PGN,
				Status:        g.Status,
				Winner:        g.Winner,
				TimeControl:   g.TimeControl,
				GameType:      g.GameType,
				Variant:       g.Variant,
				WhiteChecks:   g.WhiteChecks,
				BlackChecks:   g.BlackChecks,
				CreatedAt:     g.CreatedAt,
				UpdatedAt:     g.UpdatedAt,
				WhiteTimeMs:   g.WhiteTimeMs,
				BlackTimeMs:   g.BlackTimeMs,
				IncrementMs:   g.IncrementMs,
				LastMoveTime:  g.LastMoveTime,
			})
			if err == nil {
				return loaded, nil
			}
		}
	}

	// 2. Fallback to DB
	return s.repo.GetGame(ctx, id)
}

func (s *GameService) CreateNewGame(ctx context.Context, whitePlayerID, blackPlayerID string, timeControl string, gameType string, variant domain.GameVariant) (string, error) {
	id := uuid.New().String()
	g := domain.NewChessGame(id, whitePlayerID, timeControl, gameType, variant)

	if err := g.Start(blackPlayerID); err != nil {
		return "", fmt.Errorf("failed to start game: %w", err)
	}

	if err := s.repo.SaveGame(ctx, g); err != nil {
		return "", fmt.Errorf("failed to save new game: %w", err)
	}

	// Save to Redis immediately
	if stateBytes, err := json.Marshal(g); err == nil {
		_ = s.redis.SaveFullGameState(ctx, id, stateBytes, 24*time.Hour)
	}

	return id, nil
}

func (s *GameService) SaveGame(ctx context.Context, g *domain.ChessGame) error {
	g.UpdatedAt = time.Now()

	// Save to Redis synchronously
	if stateBytes, err := json.Marshal(g); err == nil {
		_ = s.redis.SaveFullGameState(ctx, g.ID, stateBytes, 24*time.Hour)
	}

	// Still queue it for async persistence to PostgreSQL
	return s.repo.SaveGame(ctx, g)
}
