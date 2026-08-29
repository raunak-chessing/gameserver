package domain

import (
	"strings"
	"testing"
	"time"

	"github.com/notnil/chess"
)

func TestNewChessGame(t *testing.T) {
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID", VariantStandard)

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
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID", VariantStandard)
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

func TestUndoLastMove(t *testing.T) {
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID", VariantStandard)
	_ = game.Start("black-player")

	_ = game.MakeMove("white-player", "e4")
	_ = game.MakeMove("black-player", "e5")
	fenAfterTwoMoves := game.FEN

	err := game.MakeMove("white-player", "Nf3")
	if err != nil {
		t.Fatalf("failed to make third move: %v", err)
	}

	err = game.UndoLastMove()
	if err != nil {
		t.Fatalf("undo failed: %v", err)
	}
	if game.FEN != fenAfterTwoMoves {
		t.Errorf("expected FEN to match position before undone move, got %q want %q", game.FEN, fenAfterTwoMoves)
	}
	if game.CurrentTurnPlayerID() != "white-player" {
		t.Errorf("expected it to be white's turn again after undoing white's move, got turn for %s", game.CurrentTurnPlayerID())
	}

	err = game.MakeMove("white-player", "Nc3")
	if err != nil {
		t.Errorf("expected board to accept a different move after undo, got: %v", err)
	}
}

func TestUndoLastMoveNoMoves(t *testing.T) {
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID", VariantStandard)
	_ = game.Start("black-player")

	err := game.UndoLastMove()
	if err != ErrNoMovesToUndo {
		t.Errorf("expected ErrNoMovesToUndo, got %v", err)
	}
}

func TestUndoLastMoveGameNotInProgress(t *testing.T) {
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID", VariantStandard)
	_ = game.Start("black-player")
	_ = game.MakeMove("white-player", "e4")
	_ = game.Resign("white-player")

	err := game.UndoLastMove()
	if err != ErrGameNotInProgress {
		t.Errorf("expected ErrGameNotInProgress, got %v", err)
	}
}

func TestResign(t *testing.T) {
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID", VariantStandard)
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

func TestThreeCheckVariantWinsAtThirdCheck(t *testing.T) {
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID", VariantThreeCheck)
	_ = game.Start("black-player")

	_ = game.MakeMove("white-player", "e4")
	_ = game.MakeMove("black-player", "e5")
	_ = game.MakeMove("white-player", "Qh5")
	_ = game.MakeMove("black-player", "Nc6")

	game.WhiteChecks = 2

	err := game.MakeMove("white-player", "Qxe5")
	if err != nil {
		t.Fatalf("failed to make checking move: %v", err)
	}

	if game.WhiteChecks != 3 {
		t.Errorf("expected WhiteChecks to be 3, got %d", game.WhiteChecks)
	}
	if game.Status != StatusCompleted {
		t.Errorf("expected status 'COMPLETED', got '%s'", game.Status)
	}
	if game.Winner == nil || *game.Winner != WinnerWhite {
		t.Errorf("expected WinnerWhite, got %v", game.Winner)
	}
}

func TestThreeCheckVariantDoesNotEndGameBeforeThirdCheck(t *testing.T) {
	game := NewChessGame("game-1", "white-player", "10|0", "RAPID", VariantThreeCheck)
	_ = game.Start("black-player")

	_ = game.MakeMove("white-player", "e4")
	_ = game.MakeMove("black-player", "e5")
	_ = game.MakeMove("white-player", "Qh5")
	_ = game.MakeMove("black-player", "Nc6")

	err := game.MakeMove("white-player", "Qxe5")
	if err != nil {
		t.Fatalf("failed to make checking move: %v", err)
	}

	if game.WhiteChecks != 1 {
		t.Errorf("expected WhiteChecks to be 1, got %d", game.WhiteChecks)
	}
	if game.Status != StatusInProgress {
		t.Errorf("expected status still 'IN_PROGRESS', got '%s'", game.Status)
	}
}

func TestKingOfTheHillVariantWinsOnCenterSquare(t *testing.T) {
	now := time.Now()
	game, err := LoadChessGame(LoadChessGameParams{
		ID:            "game-1",
		WhitePlayerID: "white-player",
		BlackPlayerID: "black-player",
		FEN:           "8/8/8/8/8/3K4/8/7k w - - 0 1",
		Status:        StatusInProgress,
		TimeControl:   "10|0",
		GameType:      "RAPID",
		Variant:       VariantKingOfTheHill,
		CreatedAt:     now,
		UpdatedAt:     now,
		WhiteTimeMs:   600000,
		BlackTimeMs:   600000,
	})
	if err != nil {
		t.Fatalf("failed to load game: %v", err)
	}

	err = game.MakeMove("white-player", "Kd4")
	if err != nil {
		t.Fatalf("failed to make move onto center square: %v", err)
	}

	if game.Status != StatusCompleted {
		t.Errorf("expected status 'COMPLETED', got '%s'", game.Status)
	}
	if game.Winner == nil || *game.Winner != WinnerWhite {
		t.Errorf("expected WinnerWhite, got %v", game.Winner)
	}
}

func TestKingOfTheHillVariantDoesNotEndGameOffCenter(t *testing.T) {
	now := time.Now()
	game, err := LoadChessGame(LoadChessGameParams{
		ID:            "game-1",
		WhitePlayerID: "white-player",
		BlackPlayerID: "black-player",
		FEN:           "6pk/8/8/8/8/3K4/8/6P1 w - - 0 1",
		Status:        StatusInProgress,
		TimeControl:   "10|0",
		GameType:      "RAPID",
		Variant:       VariantKingOfTheHill,
		CreatedAt:     now,
		UpdatedAt:     now,
		WhiteTimeMs:   600000,
		BlackTimeMs:   600000,
	})
	if err != nil {
		t.Fatalf("failed to load game: %v", err)
	}

	err = game.MakeMove("white-player", "Kc3")
	if err != nil {
		t.Fatalf("failed to make move: %v", err)
	}

	if game.Status != StatusInProgress {
		t.Errorf("expected status still 'IN_PROGRESS', got '%s'", game.Status)
	}
}

func TestGenerateChess960FENIsValid(t *testing.T) {
	for i := 0; i < 50; i++ {
		fen := GenerateChess960FEN()

		opt, err := chess.FEN(fen)
		if err != nil {
			t.Fatalf("generated FEN %q failed to parse: %v", fen, err)
		}
		g := chess.NewGame(opt)
		if g == nil {
			t.Fatalf("expected a valid game from FEN %q", fen)
		}

		ranks := strings.Split(fen, " ")[0]
		backRanks := strings.Split(ranks, "/")
		whiteRow := backRanks[7]
		blackRow := backRanks[0]

		if strings.ToUpper(blackRow) != whiteRow {
			t.Errorf("expected black back rank to mirror white's, got white=%q black=%q", whiteRow, blackRow)
		}

		counts := map[rune]int{}
		for _, c := range whiteRow {
			counts[c]++
		}
		if counts['K'] != 1 || counts['Q'] != 1 || counts['R'] != 2 || counts['N'] != 2 || counts['B'] != 2 {
			t.Errorf("expected exactly 1K 1Q 2R 2N 2B on back rank, got %q", whiteRow)
		}

		lightBishop, darkBishop := -1, -1
		for i, c := range whiteRow {
			if c == 'B' {
				if lightBishop == -1 {
					lightBishop = i
				} else {
					darkBishop = i
				}
			}
		}
		if lightBishop%2 == darkBishop%2 {
			t.Errorf("expected bishops on opposite-colored squares, got files %d and %d", lightBishop, darkBishop)
		}
	}
}
