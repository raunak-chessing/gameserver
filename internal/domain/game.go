package domain

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/notnil/chess"
)

var (
	ErrGameNotInProgress = errors.New("game is not in progress")
	ErrNoMovesToUndo     = errors.New("no moves to undo")
)

type GameStatus string

const (
	StatusWaiting    GameStatus = "WAITING"
	StatusInProgress GameStatus = "IN_PROGRESS"
	StatusCompleted  GameStatus = "COMPLETED"
	StatusAbandoned  GameStatus = "ABANDONED"
	StatusDraw       GameStatus = "DRAW"
)

type GameWinner string

const (
	WinnerWhite GameWinner = "WHITE"
	WinnerBlack GameWinner = "BLACK"
	WinnerDraw  GameWinner = "DRAW"
)

type GameVariant string

const (
	VariantStandard      GameVariant = "standard"
	VariantChess960      GameVariant = "chess960"
	VariantThreeCheck    GameVariant = "three-check"
	VariantKingOfTheHill GameVariant = "king-of-the-hill"
)

const checksToWin = 3

var centerSquares = [4]chess.Square{chess.D4, chess.D5, chess.E4, chess.E5}

type ChessGame struct {
	ID            string      `json:"id"`
	WhitePlayerID string      `json:"whitePlayerId"`
	BlackPlayerID string      `json:"blackPlayerId"`
	Status        GameStatus  `json:"status"`
	Winner        *GameWinner `json:"winner,omitempty"`
	FEN           string      `json:"fen"`
	PGN           string      `json:"pgn"`
	TimeControl   string      `json:"timeControl"`
	GameType      string      `json:"gameType"`
	Variant       GameVariant `json:"variant"`
	WhiteChecks   int         `json:"whiteChecks"`
	BlackChecks   int         `json:"blackChecks"`
	CreatedAt     time.Time   `json:"createdAt"`
	UpdatedAt     time.Time   `json:"updatedAt"`

	WhiteTimeMs  int64      `json:"whiteTimeMs"`
	BlackTimeMs  int64      `json:"blackTimeMs"`
	IncrementMs  int64      `json:"incrementMs"`
	LastMoveTime *time.Time `json:"lastMoveTime,omitempty"`

	engineGame *chess.Game
}

func parseClock(tc string) (int64, int64) {
	if tc == "" {
		return 10 * 60 * 1000, 0 // default 10|0
	}
	parts := strings.Split(tc, "|")
	if len(parts) == 0 {
		return 10 * 60 * 1000, 0
	}
	baseMins, _ := strconv.ParseInt(parts[0], 10, 64)
	baseMs := baseMins * 60 * 1000
	var incMs int64 = 0
	if len(parts) > 1 {
		incSecs, _ := strconv.ParseInt(parts[1], 10, 64)
		incMs = incSecs * 1000
	}
	return baseMs, incMs
}

func NewChessGame(id string, whitePlayerID string, timeControl string, gameType string, variant GameVariant) *ChessGame {
	var g *chess.Game
	if variant == VariantChess960 {
		fenOpt, err := chess.FEN(GenerateChess960FEN())
		if err == nil {
			g = chess.NewGame(fenOpt)
		}
	}
	if g == nil {
		g = chess.NewGame()
	}

	baseMs, incMs := parseClock(timeControl)
	return &ChessGame{
		ID:            id,
		WhitePlayerID: whitePlayerID,
		Status:        StatusWaiting,
		FEN:           g.FEN(),
		PGN:           "",
		TimeControl:   timeControl,
		GameType:      gameType,
		Variant:       variant,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
		WhiteTimeMs:   baseMs,
		BlackTimeMs:   baseMs,
		IncrementMs:   incMs,
		engineGame:    g,
	}
}

// GenerateChess960FEN produces a random legal Chess960 starting position.
// Castling rights are emitted as standard "KQkq" rather than Shredder-FEN
// file letters, since the underlying chess engine only parses the former;
// note it also only generates castling moves from the standard e1/e8 king
// squares, so castling will be unavailable in games where the shuffle
// places the king elsewhere.
func GenerateChess960FEN() string {
	pieces := make([]byte, 8)

	lightSquares := [4]int{1, 3, 5, 7}
	pieces[lightSquares[rand.Intn(4)]] = 'B'

	darkSquares := [4]int{0, 2, 4, 6}
	pieces[darkSquares[rand.Intn(4)]] = 'B'

	emptySquares := func() []int {
		empty := make([]int, 0, 8)
		for i, p := range pieces {
			if p == 0 {
				empty = append(empty, i)
			}
		}
		return empty
	}

	empty := emptySquares()
	pieces[empty[rand.Intn(len(empty))]] = 'Q'

	empty = emptySquares()
	pieces[empty[rand.Intn(len(empty))]] = 'N'
	empty = emptySquares()
	pieces[empty[rand.Intn(len(empty))]] = 'N'

	empty = emptySquares()
	pieces[empty[0]] = 'R'
	pieces[empty[1]] = 'K'
	pieces[empty[2]] = 'R'

	whiteRow := string(pieces)
	blackRow := strings.ToLower(whiteRow)

	return fmt.Sprintf("%s/pppppppp/8/8/8/8/PPPPPPPP/%s w KQkq - 0 1", blackRow, whiteRow)
}

type LoadChessGameParams struct {
	ID            string
	WhitePlayerID string
	BlackPlayerID string
	FEN           string
	PGN           string
	Status        GameStatus
	Winner        *GameWinner
	TimeControl   string
	GameType      string
	Variant       GameVariant
	WhiteChecks   int
	BlackChecks   int
	CreatedAt     time.Time
	UpdatedAt     time.Time
	WhiteTimeMs   int64
	BlackTimeMs   int64
	IncrementMs   int64
	LastMoveTime  *time.Time
}

func LoadChessGame(p LoadChessGameParams) (*ChessGame, error) {
	var opts []func(*chess.Game)
	if p.FEN != "" {
		f, err := chess.FEN(p.FEN)
		if err != nil {
			return nil, fmt.Errorf("invalid FEN: %w", err)
		}
		opts = append(opts, f)
	}

	g := chess.NewGame(opts...)

	whiteTimeMs, blackTimeMs, incrementMs := p.WhiteTimeMs, p.BlackTimeMs, p.IncrementMs
	// Backward compatibility for DB loads without clocks
	if whiteTimeMs == 0 && blackTimeMs == 0 {
		base, inc := parseClock(p.TimeControl)
		whiteTimeMs = base
		blackTimeMs = base
		incrementMs = inc
	}

	variant := p.Variant
	if variant == "" {
		variant = VariantStandard
	}

	return &ChessGame{
		ID:            p.ID,
		WhitePlayerID: p.WhitePlayerID,
		BlackPlayerID: p.BlackPlayerID,
		Status:        p.Status,
		Winner:        p.Winner,
		FEN:           p.FEN,
		PGN:           p.PGN,
		TimeControl:   p.TimeControl,
		GameType:      p.GameType,
		Variant:       variant,
		WhiteChecks:   p.WhiteChecks,
		BlackChecks:   p.BlackChecks,
		CreatedAt:     p.CreatedAt,
		UpdatedAt:     p.UpdatedAt,
		WhiteTimeMs:   whiteTimeMs,
		BlackTimeMs:   blackTimeMs,
		IncrementMs:   incrementMs,
		LastMoveTime:  p.LastMoveTime,
		engineGame:    g,
	}, nil
}

func (cg *ChessGame) CheckFlag(now time.Time) bool {
	if cg.Status != StatusInProgress || cg.LastMoveTime == nil {
		return false
	}
	elapsed := now.Sub(*cg.LastMoveTime).Milliseconds()
	turn := cg.engineGame.Position().Turn()
	if turn == chess.White {
		return cg.WhiteTimeMs-elapsed <= 0
	}
	return cg.BlackTimeMs-elapsed <= 0
}

func (cg *ChessGame) Flag(now time.Time) {
	if cg.Status != StatusInProgress {
		return
	}
	cg.Status = StatusCompleted
	var winner GameWinner
	if cg.engineGame.Position().Turn() == chess.White {
		winner = WinnerBlack
		cg.WhiteTimeMs = 0
	} else {
		winner = WinnerWhite
		cg.BlackTimeMs = 0
	}
	cg.Winner = &winner
	cg.UpdatedAt = now
}

func (cg *ChessGame) MakeMove(playerID string, moveStr string) error {
	if cg.Status != StatusInProgress {
		return errors.New("game is not in progress")
	}

	turn := cg.engineGame.Position().Turn()
	if turn == chess.White && playerID != cg.WhitePlayerID {
		return errors.New("not your turn (white's turn)")
	}
	if turn == chess.Black && playerID != cg.BlackPlayerID {
		return errors.New("not your turn (black's turn)")
	}

	now := time.Now()
	if cg.CheckFlag(now) {
		cg.Flag(now)
		return errors.New("flagged (timeout)")
	}

	var move *chess.Move
	var decodeErr error

	uciNotation := chess.UCINotation{}
	move, decodeErr = uciNotation.Decode(cg.engineGame.Position(), moveStr)

	if decodeErr != nil {
		algebraicNotation := chess.AlgebraicNotation{}
		move, decodeErr = algebraicNotation.Decode(cg.engineGame.Position(), moveStr)
	}

	if decodeErr != nil {
		return fmt.Errorf("invalid move format '%s': %w", moveStr, decodeErr)
	}

	if err := cg.engineGame.Move(move); err != nil {
		return fmt.Errorf("illegal move: %w", err)
	}

	// Clock management
	if cg.LastMoveTime != nil {
		elapsed := now.Sub(*cg.LastMoveTime).Milliseconds()
		// Lag compensation: deduct up to 100ms (assumed RTT compensation)
		const maxLagCompMs = 100
		compensated := elapsed - maxLagCompMs
		if compensated < 0 {
			compensated = 0
		}

		if turn == chess.White {
			cg.WhiteTimeMs -= compensated
			cg.WhiteTimeMs += cg.IncrementMs
		} else {
			cg.BlackTimeMs -= compensated
			cg.BlackTimeMs += cg.IncrementMs
		}
	}

	cg.LastMoveTime = &now
	cg.FEN = cg.engineGame.FEN()
	cg.PGN = cg.engineGame.String()
	cg.UpdatedAt = now

	cg.updateOutcome(turn, move.HasTag(chess.Check))

	return nil
}

func (cg *ChessGame) UndoLastMove() error {
	if cg.Status != StatusInProgress {
		return ErrGameNotInProgress
	}

	pgnOpt, err := chess.PGN(strings.NewReader(cg.PGN))
	if err != nil {
		return fmt.Errorf("failed to decode PGN for undo: %w", err)
	}
	recorded := chess.NewGame(pgnOpt)
	moves := recorded.Moves()
	if len(moves) == 0 {
		return ErrNoMovesToUndo
	}

	replayed := chess.NewGame()
	for _, m := range moves[:len(moves)-1] {
		if err := replayed.Move(m); err != nil {
			return fmt.Errorf("failed to replay move during undo: %w", err)
		}
	}

	now := time.Now()
	cg.engineGame = replayed
	cg.FEN = replayed.FEN()
	cg.PGN = replayed.String()
	cg.LastMoveTime = &now
	cg.UpdatedAt = now

	return nil
}

func (cg *ChessGame) Start(blackPlayerID string) error {
	if cg.Status != StatusWaiting {
		return errors.New("game cannot be started from its current status")
	}
	cg.BlackPlayerID = blackPlayerID
	cg.Status = StatusInProgress
	cg.UpdatedAt = time.Now()
	return nil
}

func (cg *ChessGame) Resign(playerID string) error {
	if cg.Status != StatusInProgress {
		return errors.New("game is not in progress")
	}

	var winner GameWinner
	if playerID == cg.WhitePlayerID {
		winner = WinnerBlack
	} else if playerID == cg.BlackPlayerID {
		winner = WinnerWhite
	} else {
		return errors.New("player is not in this game")
	}

	cg.Status = StatusCompleted
	cg.Winner = &winner
	cg.UpdatedAt = time.Now()
	return nil
}

func (cg *ChessGame) Draw() error {
	if cg.Status != StatusInProgress {
		return errors.New("game is already finished")
	}

	cg.Status = StatusDraw
	w := WinnerDraw
	cg.Winner = &w
	cg.UpdatedAt = time.Now()

	// Try to formally draw the underlying engine game if possible
	cg.engineGame.Draw(chess.DrawOffer)

	return nil
}

func (cg *ChessGame) Abort() error {
	if cg.Status != StatusInProgress {
		return errors.New("game is already finished")
	}

	cg.Status = StatusAbandoned
	// We will leave Winner as nil for aborted games.
	cg.UpdatedAt = time.Now()
	return nil
}

func (cg *ChessGame) updateOutcome(mover chess.Color, moverDeliveredCheck bool) {
	if cg.checkVariantOutcome(mover, moverDeliveredCheck) {
		return
	}

	outcome := cg.engineGame.Outcome()
	if outcome == chess.NoOutcome {
		return
	}

	cg.Status = StatusCompleted
	switch outcome {
	case chess.WhiteWon:
		w := WinnerWhite
		cg.Winner = &w
	case chess.BlackWon:
		w := WinnerBlack
		cg.Winner = &w
	case chess.Draw:
		cg.Status = StatusDraw
		w := WinnerDraw
		cg.Winner = &w
	}
}

func (cg *ChessGame) checkVariantOutcome(mover chess.Color, moverDeliveredCheck bool) bool {
	switch cg.Variant {
	case VariantThreeCheck:
		if moverDeliveredCheck {
			if mover == chess.White {
				cg.WhiteChecks++
			} else {
				cg.BlackChecks++
			}
		}
		if cg.WhiteChecks >= checksToWin {
			cg.declareVariantWinner(WinnerWhite)
			return true
		}
		if cg.BlackChecks >= checksToWin {
			cg.declareVariantWinner(WinnerBlack)
			return true
		}
		return false

	case VariantKingOfTheHill:
		if cg.moverKingOnCenter(mover) {
			if mover == chess.White {
				cg.declareVariantWinner(WinnerWhite)
			} else {
				cg.declareVariantWinner(WinnerBlack)
			}
			return true
		}
		return false

	default:
		return false
	}
}

func (cg *ChessGame) declareVariantWinner(winner GameWinner) {
	cg.Status = StatusCompleted
	cg.Winner = &winner
}

func (cg *ChessGame) moverKingOnCenter(mover chess.Color) bool {
	wantPiece := chess.WhiteKing
	if mover == chess.Black {
		wantPiece = chess.BlackKing
	}

	board := cg.engineGame.Position().Board()
	for _, sq := range centerSquares {
		if board.Piece(sq) == wantPiece {
			return true
		}
	}
	return false
}

func (cg *ChessGame) CurrentTurnPlayerID() string {
	if cg.engineGame.Position().Turn() == chess.White {
		return cg.WhitePlayerID
	}
	return cg.BlackPlayerID
}

func (cg *ChessGame) MovesCount() int {
	return len(cg.engineGame.Moves())
}
