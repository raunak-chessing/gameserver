package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	MatchesCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gameserver_matches_created_total",
		Help: "Total number of matches created by the matchmaker.",
	})

	MovesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gameserver_moves_processed_total",
		Help: "Total number of moves successfully processed.",
	})
)

func init() {
	prometheus.MustRegister(MatchesCreated, MovesProcessed)
}

// RegisterActiveSessionsGauge exposes a gauge computed live from collect
// whenever /metrics is scraped, rather than requiring every session
// creation/removal call site to remember to update a counter.
func RegisterActiveSessionsGauge(collect func() float64) {
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "gameserver_active_game_sessions",
		Help: "Current number of in-memory active game sessions on this instance.",
	}, collect))
}
