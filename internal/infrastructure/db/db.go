package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"gameserver/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	saveQueueCapacity      = 10000
	saveQueueWarnThreshold = 8000
)

type DB struct {
	pool      *pgxpool.Pool
	saveQueue chan *domain.ChessGame
}

func NewDB(databaseURL string) (*DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	config.MaxConns = 50
	config.MinConns = 10
	config.MaxConnIdleTime = 15 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{
		pool:      pool,
		saveQueue: make(chan *domain.ChessGame, saveQueueCapacity),
	}, nil
}

func (db *DB) Close() {
	db.pool.Close()
}

func (db *DB) AuthenticateSession(ctx context.Context, token string) (*domain.Player, error) {
	query := `
		SELECT u.id, u.name, u.rating,
		       u."ratingBullet", u."rdBullet", u."lastActiveBullet",
		       u."ratingBlitz", u."rdBlitz", u."lastActiveBlitz",
		       u."ratingRapid", u."rdRapid", u."lastActiveRapid",
		       u."ratingDaily", u."rdDaily", u."lastActiveDaily",
		       u."isFlaggedForCheating"
		FROM "Session" s
		JOIN "User" u ON s."userId" = u.id
		WHERE s.token = $1 AND s."expiresAt" > $2
	`

	var player domain.Player
	var lastActiveBullet sql.NullTime
	var lastActiveBlitz sql.NullTime
	var lastActiveRapid sql.NullTime
	var lastActiveDaily sql.NullTime

	err := db.pool.QueryRow(ctx, query, token, time.Now()).Scan(
		&player.ID,
		&player.Name,
		&player.Rating,
		&player.RatingBullet,
		&player.RDBullet,
		&lastActiveBullet,
		&player.RatingBlitz,
		&player.RDBlitz,
		&lastActiveBlitz,
		&player.RatingRapid,
		&player.RDRapid,
		&lastActiveRapid,
		&player.RatingDaily,
		&player.RDDaily,
		&lastActiveDaily,
		&player.IsFlaggedForCheating,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("invalid or expired session token")
		}
		return nil, fmt.Errorf("failed to query session: %w", err)
	}

	if lastActiveBullet.Valid {
		player.LastActiveBullet = lastActiveBullet.Time
	}
	if lastActiveBlitz.Valid {
		player.LastActiveBlitz = lastActiveBlitz.Time
	}
	if lastActiveRapid.Valid {
		player.LastActiveRapid = lastActiveRapid.Time
	}
	if lastActiveDaily.Valid {
		player.LastActiveDaily = lastActiveDaily.Time
	}

	return &player, nil
}

func (db *DB) GetPlayer(ctx context.Context, playerID string) (*domain.Player, error) {
	query := `
		SELECT id, name, rating,
		       "ratingBullet", "rdBullet", "lastActiveBullet",
		       "ratingBlitz", "rdBlitz", "lastActiveBlitz",
		       "ratingRapid", "rdRapid", "lastActiveRapid",
		       "ratingDaily", "rdDaily", "lastActiveDaily"
		FROM "User"
		WHERE id = $1
	`

	var player domain.Player
	var lastActiveBullet sql.NullTime
	var lastActiveBlitz sql.NullTime
	var lastActiveRapid sql.NullTime
	var lastActiveDaily sql.NullTime

	err := db.pool.QueryRow(ctx, query, playerID).Scan(
		&player.ID,
		&player.Name,
		&player.Rating,
		&player.RatingBullet,
		&player.RDBullet,
		&lastActiveBullet,
		&player.RatingBlitz,
		&player.RDBlitz,
		&lastActiveBlitz,
		&player.RatingRapid,
		&player.RDRapid,
		&lastActiveRapid,
		&player.RatingDaily,
		&player.RDDaily,
		&lastActiveDaily,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("player not found")
		}
		return nil, fmt.Errorf("failed to query player: %w", err)
	}

	if lastActiveBullet.Valid {
		player.LastActiveBullet = lastActiveBullet.Time
	}
	if lastActiveBlitz.Valid {
		player.LastActiveBlitz = lastActiveBlitz.Time
	}
	if lastActiveRapid.Valid {
		player.LastActiveRapid = lastActiveRapid.Time
	}
	if lastActiveDaily.Valid {
		player.LastActiveDaily = lastActiveDaily.Time
	}

	return &player, nil
}

func (db *DB) GetGame(ctx context.Context, gameID string) (*domain.ChessGame, error) {
	query := `
		SELECT id, "whitePlayerId", "blackPlayerId", fen, pgn, status, winner, "timeControl", "gameType", variant, "whiteChecks", "blackChecks", "createdAt", "updatedAt", "whiteTimeMs", "blackTimeMs", "incrementMs", "lastMoveTime"
		FROM "Game"
		WHERE id = $1
	`

	var id, whitePlayerId string
	var blackPlayerId sql.NullString
	var fen, pgn string
	var statusStr string
	var winnerStr sql.NullString
	var timeControl, gameType, variant string
	var whiteChecks, blackChecks int
	var createdAt, updatedAt time.Time
	var whiteTimeMs, blackTimeMs, incrementMs sql.NullInt64
	var lastMoveTime sql.NullTime

	err := db.pool.QueryRow(ctx, query, gameID).Scan(
		&id,
		&whitePlayerId,
		&blackPlayerId,
		&fen,
		&pgn,
		&statusStr,
		&winnerStr,
		&timeControl,
		&gameType,
		&variant,
		&whiteChecks,
		&blackChecks,
		&createdAt,
		&updatedAt,
		&whiteTimeMs,
		&blackTimeMs,
		&incrementMs,
		&lastMoveTime,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("game not found")
		}
		return nil, fmt.Errorf("failed to query game: %w", err)
	}

	var bpId string
	if blackPlayerId.Valid {
		bpId = blackPlayerId.String
	}

	status := domain.GameStatus(statusStr)

	var winner *domain.GameWinner
	if winnerStr.Valid {
		w := domain.GameWinner(winnerStr.String)
		winner = &w
	}

	var wTime, bTime, incTime int64
	if whiteTimeMs.Valid {
		wTime = whiteTimeMs.Int64
	}
	if blackTimeMs.Valid {
		bTime = blackTimeMs.Int64
	}
	if incrementMs.Valid {
		incTime = incrementMs.Int64
	}
	var lmTime *time.Time
	if lastMoveTime.Valid {
		lmTime = &lastMoveTime.Time
	}

	return domain.LoadChessGame(domain.LoadChessGameParams{
		ID:            id,
		WhitePlayerID: whitePlayerId,
		BlackPlayerID: bpId,
		FEN:           fen,
		PGN:           pgn,
		Status:        status,
		Winner:        winner,
		TimeControl:   timeControl,
		GameType:      gameType,
		Variant:       domain.GameVariant(variant),
		WhiteChecks:   whiteChecks,
		BlackChecks:   blackChecks,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
		WhiteTimeMs:   wTime,
		BlackTimeMs:   bTime,
		IncrementMs:   incTime,
		LastMoveTime:  lmTime,
	})
}

func (db *DB) SaveGame(ctx context.Context, g *domain.ChessGame) error {
	queueLen := len(db.saveQueue)
	if queueLen > saveQueueWarnThreshold {
		log.Printf("Warning: DB save queue at %d/%d (%.0f%% capacity)", queueLen, saveQueueCapacity, float64(queueLen)/float64(saveQueueCapacity)*100)
	}
	select {
	case db.saveQueue <- g:
	default:
		log.Printf("Warning: DB save queue is full. Executing synchronous save for game %s", g.ID)
		return db.saveGameSync(ctx, g)
	}
	return nil
}

func (db *DB) saveGameSync(ctx context.Context, g *domain.ChessGame) error {
	query := `
		INSERT INTO "Game" (id, "whitePlayerId", "blackPlayerId", fen, pgn, status, winner, "timeControl", "gameType", variant, "whiteChecks", "blackChecks", "createdAt", "updatedAt", "whiteTimeMs", "blackTimeMs", "incrementMs", "lastMoveTime")
		VALUES ($1, $2, $3, $4, $5, $6::"GameStatus", $7::"GameWinner", $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
		ON CONFLICT (id) DO UPDATE SET
			"blackPlayerId" = EXCLUDED."blackPlayerId",
			fen = EXCLUDED.fen,
			pgn = EXCLUDED.pgn,
			status = EXCLUDED.status,
			winner = EXCLUDED.winner,
			"timeControl" = EXCLUDED."timeControl",
			"gameType" = EXCLUDED."gameType",
			"whiteChecks" = EXCLUDED."whiteChecks",
			"blackChecks" = EXCLUDED."blackChecks",
			"updatedAt" = EXCLUDED."updatedAt",
			"whiteTimeMs" = EXCLUDED."whiteTimeMs",
			"blackTimeMs" = EXCLUDED."blackTimeMs",
			"incrementMs" = EXCLUDED."incrementMs",
			"lastMoveTime" = EXCLUDED."lastMoveTime"
	`

	var blackPlayerId *string
	if g.BlackPlayerID != "" {
		blackPlayerId = &g.BlackPlayerID
	}

	var winnerStr *string
	if g.Winner != nil {
		s := string(*g.Winner)
		winnerStr = &s
	}

	variant := g.Variant
	if variant == "" {
		variant = domain.VariantStandard
	}

	_, err := db.pool.Exec(
		ctx,
		query,
		g.ID,
		g.WhitePlayerID,
		blackPlayerId,
		g.FEN,
		g.PGN,
		string(g.Status),
		winnerStr,
		g.TimeControl,
		g.GameType,
		string(variant),
		g.WhiteChecks,
		g.BlackChecks,
		g.CreatedAt,
		g.UpdatedAt,
		g.WhiteTimeMs,
		g.BlackTimeMs,
		g.IncrementMs,
		g.LastMoveTime,
	)
	if err != nil {
		return fmt.Errorf("failed to execute sync save: %w", err)
	}
	return nil
}

func (db *DB) StartBatchProcessor(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	dirtyGames := make(map[string]*domain.ChessGame)
	const batchSizeThreshold = 500

	flush := func() {
		if len(dirtyGames) == 0 {
			return
		}

		log.Printf("Flushing %d game updates to PostgreSQL...", len(dirtyGames))

		flushCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		batch := &pgx.Batch{}
		query := `
			INSERT INTO "Game" (id, "whitePlayerId", "blackPlayerId", fen, pgn, status, winner, "timeControl", "gameType", variant, "whiteChecks", "blackChecks", "createdAt", "updatedAt", "whiteTimeMs", "blackTimeMs", "incrementMs", "lastMoveTime")
			VALUES ($1, $2, $3, $4, $5, $6::"GameStatus", $7::"GameWinner", $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
			ON CONFLICT (id) DO UPDATE SET
				"blackPlayerId" = EXCLUDED."blackPlayerId",
				fen = EXCLUDED.fen,
				pgn = EXCLUDED.pgn,
				status = EXCLUDED.status,
				winner = EXCLUDED.winner,
				"timeControl" = EXCLUDED."timeControl",
				"gameType" = EXCLUDED."gameType",
				"whiteChecks" = EXCLUDED."whiteChecks",
				"blackChecks" = EXCLUDED."blackChecks",
				"updatedAt" = EXCLUDED."updatedAt",
				"whiteTimeMs" = EXCLUDED."whiteTimeMs",
				"blackTimeMs" = EXCLUDED."blackTimeMs",
				"incrementMs" = EXCLUDED."incrementMs",
				"lastMoveTime" = EXCLUDED."lastMoveTime"
		`

		for _, g := range dirtyGames {
			var blackPlayerId *string
			if g.BlackPlayerID != "" {
				blackPlayerId = &g.BlackPlayerID
			}

			var winnerStr *string
			if g.Winner != nil {
				s := string(*g.Winner)
				winnerStr = &s
			}

			variant := g.Variant
			if variant == "" {
				variant = domain.VariantStandard
			}

			batch.Queue(
				query,
				g.ID,
				g.WhitePlayerID,
				blackPlayerId,
				g.FEN,
				g.PGN,
				string(g.Status),
				winnerStr,
				g.TimeControl,
				g.GameType,
				string(variant),
				g.WhiteChecks,
				g.BlackChecks,
				g.CreatedAt,
				g.UpdatedAt,
				g.WhiteTimeMs,
				g.BlackTimeMs,
				g.IncrementMs,
				g.LastMoveTime,
			)
		}

		br := db.pool.SendBatch(flushCtx, batch)
		if err := br.Close(); err != nil {
			log.Printf("Failed to execute batch save to PostgreSQL: %v", err)
		} else {
			log.Printf("Successfully flushed batch to PostgreSQL.")
		}

		dirtyGames = make(map[string]*domain.ChessGame)
	}

	for {
		select {
		case g := <-db.saveQueue:
			dirtyGames[g.ID] = g
			if len(dirtyGames) >= batchSizeThreshold {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			log.Println("Stopping batch processor, executing final flush...")
			for {
				select {
				case g := <-db.saveQueue:
					dirtyGames[g.ID] = g
				default:
					goto drained
				}
			}
		drained:
			flush()
			return
		}
	}
}

type gameTypeColumns struct {
	rating     string
	rd         string
	lastActive string
}

var validGameTypes = map[string]gameTypeColumns{
	"BULLET": {rating: "ratingBullet", rd: "rdBullet", lastActive: "lastActiveBullet"},
	"BLITZ":  {rating: "ratingBlitz", rd: "rdBlitz", lastActive: "lastActiveBlitz"},
	"RAPID":  {rating: "ratingRapid", rd: "rdRapid", lastActive: "lastActiveRapid"},
	"DAILY":  {rating: "ratingDaily", rd: "rdDaily", lastActive: "lastActiveDaily"},
}

func gameTypeCols(gt string) (gameTypeColumns, error) {
	cols, ok := validGameTypes[gt]
	if !ok {
		return gameTypeColumns{}, fmt.Errorf("unknown game type: %q", gt)
	}
	return cols, nil
}

func (db *DB) UpdatePlayerRatings(ctx context.Context, pA *domain.Player, pB *domain.Player, gameType string) error {
	cols, err := gameTypeCols(gameType)
	if err != nil {
		return err
	}

	query := `UPDATE "User" SET "` + cols.rating + `" = $1, "` + cols.rd + `" = $2, "` + cols.lastActive + `" = $3, rating = $4 WHERE id = $5`

	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rA, rdA, laA := getGlickoValues(pA, gameType)
	_, err = tx.Exec(ctx, query, rA, rdA, laA, pA.Rating, pA.ID)
	if err != nil {
		return fmt.Errorf("failed to update player A (%s): %w", pA.ID, err)
	}

	rB, rdB, laB := getGlickoValues(pB, gameType)
	_, err = tx.Exec(ctx, query, rB, rdB, laB, pB.Rating, pB.ID)
	if err != nil {
		return fmt.Errorf("failed to update player B (%s): %w", pB.ID, err)
	}

	return tx.Commit(ctx)
}

func getGlickoValues(p *domain.Player, gt string) (int, float64, time.Time) {
	switch gt {
	case "BULLET":
		return p.RatingBullet, p.RDBullet, p.LastActiveBullet
	case "BLITZ":
		return p.RatingBlitz, p.RDBlitz, p.LastActiveBlitz
	case "DAILY":
		return p.RatingDaily, p.RDDaily, p.LastActiveDaily
	default:
		return p.RatingRapid, p.RDRapid, p.LastActiveRapid
	}
}
