package domain

import (
	"math"
	"time"
)

const (
	MinRD = 35.0
	MaxRD = 350.0
	cDecay = 15.0 // Uncertainty growth constant (c-value)
)

var qGlicko = math.Ln10 / 400.0

// DecayRD updates the rating deviation based on the elapsed time since the last active game
func DecayRD(lastRD float64, lastActive time.Time) float64 {
	if lastActive.IsZero() {
		return MaxRD
	}
	days := time.Since(lastActive).Hours() / 24.0
	if days <= 0 {
		return lastRD
	}
	newRD := math.Sqrt(lastRD*lastRD + cDecay*cDecay*days)
	if newRD > MaxRD {
		return MaxRD
	}
	return newRD
}

// CalculateG g(RD) is Glicko's scaling factor based on the opponent's rating deviation
func CalculateG(rd float64) float64 {
	return 1.0 / math.Sqrt(1.0+3.0*qGlicko*qGlicko*rd*rd/(math.Pi*math.Pi))
}

// CalculateExpectedScore calculates the expected outcome of a game
func CalculateExpectedScore(r, opponentR, opponentRD float64) float64 {
	g := CalculateG(opponentRD)
	exponent := -g * (r - opponentR) / 400.0
	return 1.0 / (1.0 + math.Pow(10, exponent))
}

// CalculateNewRatingAndRD calculates the updated rating and RD for a single player against one opponent
// outcome is 1.0 for a win, 0.5 for a draw, and 0.0 for a loss.
func CalculateNewRatingAndRD(rA, rdA, rB, rdB float64, outcome float64) (float64, float64) {
	gB := CalculateG(rdB)
	eA := CalculateExpectedScore(rA, rB, rdB)

	// Estimated variance of player A's rating
	dASquared := 1.0 / (qGlicko * qGlicko * gB * gB * eA * (1.0 - eA))

	// Updated rating deviation
	newRDA := math.Sqrt(1.0 / (1.0/(rdA*rdA) + 1.0/dASquared))
	if newRDA < MinRD {
		newRDA = MinRD
	}

	// Updated rating
	newRA := rA + (qGlicko/(1.0/(rdA*rdA)+1.0/dASquared))*gB*(outcome-eA)

	return newRA, newRDA
}
