package domain

import (
	"testing"
)

func TestNewChessGame(t *testing.T) {
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID")

	if game.ID != "game-1" {
		t.Errorf("expected ID 'game-1', got '%s'", game.ID)
	}
	if game.WhitePlayerID != "white-player" {
		t.Errorf("expected white player 'white-player', got '%s'", game.WhitePlayerID)
	}
	if game.Status != StatusWaiting {
		t.Errorf("expected status 'WAITING', got '%s'", game.Status)
	}
	if game.FEN == "" {
		t.Errorf("expected initial FEN not to be empty")
	}
}

func TestGameStartAndMoves(t *testing.T) {
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID")
	err := game.Start("black-player")
	if err != nil {
		t.Fatalf("failed to start game: %v", err)
	}

	if game.Status != StatusInProgress {
		t.Errorf("expected status 'IN_PROGRESS', got '%s'", game.Status)
	}
	if game.BlackPlayerID != "black-player" {
		t.Errorf("expected black player 'black-player', got '%s'", game.BlackPlayerID)
	}

	err = game.MakeMove("white-player", "e4")
	if err != nil {
		t.Errorf("expected legal move to succeed, got error: %v", err)
	}

	err = game.MakeMove("white-player", "e5")
	if err == nil {
		t.Errorf("expected move out of turn to fail, but it succeeded")
	}

	err = game.MakeMove("black-player", "e5")
	if err != nil {
		t.Errorf("expected legal black move to succeed, got: %v", err)
	}
}

func TestResign(t *testing.T) {
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID")
	_ = game.Start("black-player")

	err := game.Resign("white-player")
	if err != nil {
		t.Fatalf("resign failed: %v", err)
	}

	if game.Status != StatusCompleted {
		t.Errorf("expected status 'COMPLETED', got '%s'", game.Status)
	}
	if game.Winner == nil || *game.Winner != WinnerBlack {
		t.Errorf("expected WinnerBlack, got %v", game.Winner)
	}
}
