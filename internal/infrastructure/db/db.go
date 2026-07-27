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
		saveQueue: make(chan *domain.ChessGame, 50000),
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
		       u."ratingDaily", u."rdDaily", u."lastActiveDaily"
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
		SELECT id, "whitePlayerId", "blackPlayerId", fen, pgn, status, winner, "timeControl", "gameType", "createdAt", "updatedAt"
		FROM "Game"
		WHERE id = $1
	`

	var id, whitePlayerId string
	var blackPlayerId sql.NullString
	var fen, pgn string
	var statusStr string
	var winnerStr sql.NullString
	var timeControl, gameType string
	var createdAt, updatedAt time.Time

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
		&createdAt,
		&updatedAt,
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

	return domain.LoadChessGame(id, whitePlayerId, bpId, fen, pgn, status, winner, timeControl, gameType, createdAt, updatedAt, 0, 0, 0, nil)
}

func (db *DB) SaveGame(ctx context.Context, g *domain.ChessGame) error {
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
		INSERT INTO "Game" (id, "whitePlayerId", "blackPlayerId", fen, pgn, status, winner, "timeControl", "gameType", "createdAt", "updatedAt")
		VALUES ($1, $2, $3, $4, $5, $6::"GameStatus", $7::"GameWinner", $8, $9, $10, $11)
		ON CONFLICT (id) DO UPDATE SET
			"blackPlayerId" = EXCLUDED."blackPlayerId",
			fen = EXCLUDED.fen,
			pgn = EXCLUDED.pgn,
			status = EXCLUDED.status,
			winner = EXCLUDED.winner,
			"timeControl" = EXCLUDED."timeControl",
			"gameType" = EXCLUDED."gameType",
			"updatedAt" = EXCLUDED."updatedAt"
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
		g.CreatedAt,
		g.UpdatedAt,
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
			INSERT INTO "Game" (id, "whitePlayerId", "blackPlayerId", fen, pgn, status, winner, "timeControl", "gameType", "createdAt", "updatedAt")
			VALUES ($1, $2, $3, $4, $5, $6::"GameStatus", $7::"GameWinner", $8, $9, $10, $11)
			ON CONFLICT (id) DO UPDATE SET
				"blackPlayerId" = EXCLUDED."blackPlayerId",
				fen = EXCLUDED.fen,
				pgn = EXCLUDED.pgn,
				status = EXCLUDED.status,
				winner = EXCLUDED.winner,
				"timeControl" = EXCLUDED."timeControl",
				"gameType" = EXCLUDED."gameType",
				"updatedAt" = EXCLUDED."updatedAt"
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
				g.CreatedAt,
				g.UpdatedAt,
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

// UpdatePlayerRatings persists the Glicko-1 ratings and RDs for both players
func (db *DB) UpdatePlayerRatings(ctx context.Context, pA *domain.Player, pB *domain.Player, gameType string) error {
	query := fmt.Sprintf(`
		UPDATE "User"
		SET "%s" = $1, "%s" = $2, "%s" = $3
		WHERE id = $4
	`, ratingCol(gameType), rdCol(gameType), lastActiveCol(gameType))

	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rA, rdA, laA := getGlickoValues(pA, gameType)
	_, err = tx.Exec(ctx, query, rA, rdA, laA, pA.ID)
	if err != nil {
		return fmt.Errorf("failed to update player A (%s): %w", pA.ID, err)
	}

	rB, rdB, laB := getGlickoValues(pB, gameType)
	_, err = tx.Exec(ctx, query, rB, rdB, laB, pB.ID)
	if err != nil {
		return fmt.Errorf("failed to update player B (%s): %w", pB.ID, err)
	}

	return tx.Commit(ctx)
}

func ratingCol(gt string) string {
	switch gt {
	case "BULLET":
		return "ratingBullet"
	case "BLITZ":
		return "ratingBlitz"
	case "DAILY":
		return "ratingDaily"
	default:
		return "ratingRapid"
	}
}

func rdCol(gt string) string {
	switch gt {
	case "BULLET":
		return "rdBullet"
	case "BLITZ":
		return "rdBlitz"
	case "DAILY":
		return "rdDaily"
	default:
		return "rdRapid"
	}
}

func lastActiveCol(gt string) string {
	switch gt {
	case "BULLET":
		return "lastActiveBullet"
	case "BLITZ":
		return "lastActiveBlitz"
	case "DAILY":
		return "lastActiveDaily"
	default:
		return "lastActiveRapid"
	}
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
