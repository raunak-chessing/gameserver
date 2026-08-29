package application

import (
	"context"
	"log"
	"math"
	"time"

	"gameserver/internal/domain"
)

type RatingStore interface {
	GetPlayer(ctx context.Context, playerID string) (*domain.Player, error)
	UpdatePlayerRatings(ctx context.Context, pA *domain.Player, pB *domain.Player, gameType string) error
}

// UpdateRatings processes Glicko-1 rating updates for both players once a game concludes.
func UpdateRatings(ctx context.Context, store RatingStore, game *domain.ChessGame, gameID string) {
	if game.WhitePlayerID == "" || game.BlackPlayerID == "" {
		return
	}

	// Under chess.com rules, a game is aborted if ended before both players made their first move.
	// We check if history has less than 2 moves (White move 1 + Black move 1).
	if game.MovesCount() < 2 {
		log.Printf("Game %s aborted (moves %d < 2). Ratings are unaffected.", gameID, game.MovesCount())
		return
	}

	pA, err := store.GetPlayer(ctx, game.WhitePlayerID)
	if err != nil {
		log.Printf("Failed to load player A (%s) for rating update: %v", game.WhitePlayerID, err)
		return
	}

	pB, err := store.GetPlayer(ctx, game.BlackPlayerID)
	if err != nil {
		log.Printf("Failed to load player B (%s) for rating update: %v", game.BlackPlayerID, err)
		return
	}

	var rA, rdA float64
	var rB, rdB float64
	var lastActiveA, lastActiveB time.Time

	switch game.GameType {
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
	if game.Winner != nil {
		if *game.Winner == domain.WinnerWhite {
			outcomeA = 1.0
		} else if *game.Winner == domain.WinnerBlack {
			outcomeA = 0.0
		}
	}
	outcomeB := 1.0 - outcomeA

	newRA, newRDA := domain.CalculateNewRatingAndRD(rA, rdA, rB, rdB, outcomeA)
	newRB, newRDB := domain.CalculateNewRatingAndRD(rB, rdB, rA, rdA, outcomeB)

	now := time.Now()
	switch game.GameType {
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

	// Update the general rating as the maximum of all rating pools
	pA.Rating = maxInt(pA.RatingBullet, pA.RatingBlitz, pA.RatingRapid, pA.RatingDaily)
	pB.Rating = maxInt(pB.RatingBullet, pB.RatingBlitz, pB.RatingRapid, pB.RatingDaily)

	if err := store.UpdatePlayerRatings(ctx, pA, pB, game.GameType); err != nil {
		log.Printf("Failed to update player ratings: %v", err)
		return
	}

	log.Printf("[Glicko-1] Game %s (%s) concluded. Player A (%s): %d -> %d, Player B (%s): %d -> %d",
		gameID, game.GameType, pA.ID, int(math.Round(rA)), pA.Rating, pB.ID, int(math.Round(rB)), pB.Rating)
}

func maxInt(vals ...int) int {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}
