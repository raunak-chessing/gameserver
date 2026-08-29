package application

import (
	"context"
	"testing"
	"time"

	"gameserver/internal/domain"
)

type fakeRatingStore struct {
	players map[string]*domain.Player
	saved   bool
	savedA  *domain.Player
	savedB  *domain.Player
}

func newFakeRatingStore(players ...*domain.Player) *fakeRatingStore {
	m := make(map[string]*domain.Player)
	for _, p := range players {
		m[p.ID] = p
	}
	return &fakeRatingStore{players: m}
}

func (f *fakeRatingStore) GetPlayer(ctx context.Context, playerID string) (*domain.Player, error) {
	return f.players[playerID], nil
}

func (f *fakeRatingStore) UpdatePlayerRatings(ctx context.Context, pA *domain.Player, pB *domain.Player, gameType string) error {
	f.saved = true
	f.savedA = pA
	f.savedB = pB
	return nil
}

func newTestPlayer(id string, rating int) *domain.Player {
	now := time.Now()
	return &domain.Player{
		ID:              id,
		RatingRapid:     rating,
		RDRapid:         50,
		LastActiveRapid: now,
	}
}

func TestUpdateRatings_WhiteWins(t *testing.T) {
	white := newTestPlayer("white-1", 1500)
	black := newTestPlayer("black-1", 1500)
	store := newFakeRatingStore(white, black)

	game := domain.NewChessGame("game-1", white.ID, "10|0", "RAPID", domain.VariantStandard)
	if err := game.Start(black.ID); err != nil {
		t.Fatalf("failed to start game: %v", err)
	}
	if err := game.MakeMove(white.ID, "e4"); err != nil {
		t.Fatalf("failed to make white move: %v", err)
	}
	if err := game.MakeMove(black.ID, "e5"); err != nil {
		t.Fatalf("failed to make black move: %v", err)
	}
	if err := game.Resign(black.ID); err != nil {
		t.Fatalf("failed to resign: %v", err)
	}

	UpdateRatings(context.Background(), store, game, game.ID)

	if !store.saved {
		t.Fatalf("expected UpdatePlayerRatings to be called")
	}
	if store.savedA.RatingRapid <= 1500 {
		t.Errorf("expected white's rating to increase after winning, got %d", store.savedA.RatingRapid)
	}
	if store.savedB.RatingRapid >= 1500 {
		t.Errorf("expected black's rating to decrease after losing, got %d", store.savedB.RatingRapid)
	}
}

func TestUpdateRatings_AbortedGameSkipsRatingChange(t *testing.T) {
	white := newTestPlayer("white-1", 1500)
	black := newTestPlayer("black-1", 1500)
	store := newFakeRatingStore(white, black)

	game := domain.NewChessGame("game-1", white.ID, "10|0", "RAPID", domain.VariantStandard)
	if err := game.Start(black.ID); err != nil {
		t.Fatalf("failed to start game: %v", err)
	}

	UpdateRatings(context.Background(), store, game, game.ID)

	if store.saved {
		t.Errorf("expected no rating update for a game with fewer than 2 moves")
	}
}
