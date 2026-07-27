package application

import (
	"sync"
	"time"
)

// Tournament represents a 1-hour Skirmish tied to an Overworld Hex
type Tournament struct {
	ID        string
	HexID     string
	EndTime   time.Time
	FactionScores map[string]int
	PlayerStreaks map[string]int // tracks consecutive wins for bonus points
	mu        sync.RWMutex
}

var activeTournaments = make(map[string]*Tournament)
var tMu sync.RWMutex

func CreateTournament(id, hexId string, duration time.Duration) *Tournament {
	t := &Tournament{
		ID:            id,
		HexID:         hexId,
		EndTime:       time.Now().Add(duration),
		FactionScores: make(map[string]int),
		PlayerStreaks: make(map[string]int),
	}
	tMu.Lock()
	activeTournaments[id] = t
	tMu.Unlock()
	return t
}

func GetTournament(id string) *Tournament {
	tMu.RLock()
	defer tMu.RUnlock()
	return activeTournaments[id]
}

// ReportGameResult processes points for a game played within the Arena
func (t *Tournament) ReportGameResult(winnerId, winnerFactionId, loserId string, isDraw bool) {
	if time.Now().After(t.EndTime) {
		return // Tournament ended
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if isDraw {
		t.PlayerStreaks[winnerId] = 0
		t.PlayerStreaks[loserId] = 0
		// Draws might award 1 point
		t.FactionScores[winnerFactionId] += 1
		// Would need loserFactionId to give them 1 point too, simplified here
	} else {
		t.PlayerStreaks[loserId] = 0
		streak := t.PlayerStreaks[winnerId]
		
		points := 2
		if streak >= 2 { // On fire bonus (Chess.com arena style)
			points = 3
			if streak >= 3 {
				points = 4
			}
		}

		t.FactionScores[winnerFactionId] += points
		t.PlayerStreaks[winnerId]++
	}
}

func (t *Tournament) GetLeadingFaction() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var leader string
	maxScore := -1
	for faction, score := range t.FactionScores {
		if score > maxScore {
			maxScore = score
			leader = faction
		}
	}
	return leader
}
