package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	TargetURL   string
	DatabaseURL string
	Concurrency int
	Duration    time.Duration
}

type Metrics struct {
	ConnectedCount  int64
	FailedConnCount int64
	MatchedCount    int64
	MovesSentCount  int64
	HandshakeSumMs  int64
}

var localIPs = []string{
	"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4",
	"127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8",
	"127.0.0.9", "127.0.0.10", "127.0.0.11", "127.0.0.12",
	"127.0.0.13", "127.0.0.14", "127.0.0.15", "127.0.0.16",
}

var useAliases = false

func main() {
	target := flag.String("target", "ws://localhost:8082/ws", "gameserver websocket endpoint")
	dbURL := flag.String("db", "postgresql://postgres:postgres@localhost:5432/chessdb", "database URL")
	concurrency := flag.Int("concurrency", 100, "number of concurrent players to simulate")
	duration := flag.Duration("duration", 30*time.Second, "load test duration")
	flag.Parse()

	cfg := Config{
		TargetURL:   *target,
		DatabaseURL: *dbURL,
		Concurrency: *concurrency,
		Duration:    *duration,
	}

	log.Printf("Starting load test. Target: %s, Concurrency: %d, Duration: %v", cfg.TargetURL, cfg.Concurrency, cfg.Duration)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	if err := bootstrapDatabase(ctx, pool); err != nil {
		log.Fatalf("Failed to bootstrap database: %v", err)
	}

	_, _ = pool.Exec(ctx, `DELETE FROM "Game" WHERE "whitePlayerId" IN (SELECT id FROM "User" WHERE email LIKE 'loadtest_%') OR "blackPlayerId" IN (SELECT id FROM "User" WHERE email LIKE 'loadtest_%')`)
	_, _ = pool.Exec(ctx, `DELETE FROM "User" WHERE email LIKE 'loadtest_%'`)

	checkAliases(cfg.TargetURL)

	log.Printf("Seeding %d mock users and sessions via pgx COPY...", cfg.Concurrency)
	seedStart := time.Now()
	sessionTokens, userIDs, err := seedMockSessions(ctx, pool, cfg.Concurrency)
	if err != nil {
		log.Fatalf("Failed to seed sessions: %v", err)
	}
	log.Printf("Successfully seeded %d sessions in %v.", cfg.Concurrency, time.Since(seedStart))

	defer func() {
		log.Println("Cleaning up mock database records...")
		cleanupMockSessions(ctx, pool, userIDs)
		log.Println("Database cleanup completed.")
	}()

	var metrics Metrics
	var wg sync.WaitGroup

	stopChan := make(chan struct{})
	time.AfterFunc(cfg.Duration, func() {
		close(stopChan)
	})

	log.Printf("Launching %d concurrent workers...", cfg.Concurrency)
	startTime := time.Now()

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			runClient(cfg.TargetURL, sessionTokens[index], index, stopChan, &metrics)
		}(i)
		if i%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	wg.Wait()
	testDuration := time.Since(startTime)

	printResults(&metrics, testDuration, cfg.Concurrency)
}

func checkAliases(targetURL string) {
	u, err := url.Parse(targetURL)
	if err != nil {
		return
	}
	
	host := u.Host
	if host == "" {
		host = "localhost:8082"
	}
	
	log.Println("Testing loopback IP alias (127.0.0.2) connectivity...")
	localTCPAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.2:0")
	if err != nil {
		return
	}
	
	dialer := net.Dialer{
		LocalAddr: localTCPAddr,
		Timeout:   1 * time.Second,
	}
	
	conn, err := dialer.Dial("tcp", host)
	if err == nil {
		conn.Close()
		useAliases = true
		log.Println("Loopback IP aliases are functional. Enabling multi-IP round-robin dialing.")
	} else {
		log.Println("Loopback IP aliases are not functional or not configured. Falling back to default loopback (127.0.0.1).")
		log.Println("Note: To support >16k concurrent connections from a single host, please register IP aliases on your loopback adapter.")
	}
}

func seedMockSessions(ctx context.Context, pool *pgxpool.Pool, count int) ([]string, []string, error) {
	tokens := make([]string, count)
	userIDs := make([]string, count)
	sessionIDs := make([]string, count)

	for i := 0; i < count; i++ {
		userIDs[i] = uuid.New().String()
		tokens[i] = fmt.Sprintf("loadtest_token_%s", uuid.New().String())
		sessionIDs[i] = fmt.Sprintf("session_%s", uuid.New().String())
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	userRows := make([][]interface{}, count)
	for i := 0; i < count; i++ {
		userName := fmt.Sprintf("LoadTest_User_%d", i)
		userEmail := fmt.Sprintf("loadtest_%s_%d@example.com", uuid.New().String()[:8], i)
		userRows[i] = []interface{}{userIDs[i], userName, userEmail, false, nil, 1500, time.Now(), time.Now()}
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"User"},
		[]string{"id", "name", "email", "emailVerified", "image", "rating", "createdAt", "updatedAt"},
		pgx.CopyFromRows(userRows),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to bulk copy Users: %w", err)
	}

	sessionRows := make([][]interface{}, count)
	for i := 0; i < count; i++ {
		sessionRows[i] = []interface{}{sessionIDs[i], tokens[i], time.Now().Add(24 * time.Hour), userIDs[i], nil, nil, time.Now(), time.Now()}
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"Session"},
		[]string{"id", "token", "expiresAt", "userId", "ipAddress", "userAgent", "createdAt", "updatedAt"},
		pgx.CopyFromRows(sessionRows),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to bulk copy Sessions: %w", err)
	}

	return tokens, userIDs, tx.Commit(ctx)
}

func cleanupMockSessions(ctx context.Context, pool *pgxpool.Pool, userIDs []string) {
	_, err := pool.Exec(ctx, `DELETE FROM "Game" WHERE "whitePlayerId" = ANY($1) OR "blackPlayerId" = ANY($1)`, userIDs)
	if err != nil {
		log.Printf("Error cleaning up mock games: %v", err)
	}

	query := `DELETE FROM "User" WHERE id = ANY($1)`
	_, err = pool.Exec(ctx, query, userIDs)
	if err != nil {
		log.Printf("Error cleaning up database records: %v", err)
	}
}

func runClient(targetURL string, token string, index int, stopChan chan struct{}, m *Metrics) {
	u, err := url.Parse(targetURL)
	if err != nil {
		atomic.AddInt64(&m.FailedConnCount, 1)
		return
	}
	q := u.Query()
	q.Set("token", token)
	u.RawQuery = q.Encode()

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		NetDial: func(network, addr string) (net.Conn, error) {
			var localTCPAddr *net.TCPAddr
			if useAliases {
				localIP := localIPs[index%len(localIPs)]
				var err error
				localTCPAddr, err = net.ResolveTCPAddr("tcp", localIP+":0")
				if err != nil {
					return nil, err
				}
			}
			d := net.Dialer{
				LocalAddr: localTCPAddr,
				Timeout:   10 * time.Second,
			}
			return d.Dial(network, addr)
		},
	}

	dialStart := time.Now()
	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		atomic.AddInt64(&m.FailedConnCount, 1)
		return
	}
	defer conn.Close()

	handshakeDuration := time.Since(dialStart).Milliseconds()
	atomic.AddInt64(&m.HandshakeSumMs, handshakeDuration)
	atomic.AddInt64(&m.ConnectedCount, 1)

	var connMu sync.Mutex
	writeMsg := func(messageType int, data []byte) error {
		connMu.Lock()
		defer connMu.Unlock()
		return conn.WriteMessage(messageType, data)
	}

	joinReq := map[string]interface{}{
		"type": "join_matchmaking",
	}
	joinBytes, _ := json.Marshal(joinReq)
	if err := writeMsg(websocket.TextMessage, joinBytes); err != nil {
		return
	}

	var gameID string
	var writeWG sync.WaitGroup
	defer writeWG.Wait()

	readWG := sync.WaitGroup{}
	readWG.Add(1)
	go func() {
		defer readWG.Done()
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var response struct {
				Type   string `json:"type"`
				GameID string `json:"gameId"`
			}
			if err := json.Unmarshal(message, &response); err == nil {
				if response.Type == "match_found" {
					atomic.AddInt64(&m.MatchedCount, 1)
					gameID = response.GameID
				}
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	writeWG.Add(1)
	go func() {
		defer writeWG.Done()
		for {
			select {
			case <-ticker.C:
				if gameID != "" {
					moveReq := map[string]interface{}{
						"type":   "make_move",
						"gameId": gameID,
						"payload": map[string]string{
							"move": "e4",
						},
					}
					moveBytes, _ := json.Marshal(moveReq)
					_ = writeMsg(websocket.TextMessage, moveBytes)
					atomic.AddInt64(&m.MovesSentCount, 1)
				}
			case <-stopChan:
				return
			}
		}
	}()

	<-stopChan
	_ = writeMsg(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	readWG.Wait()
}

func printResults(m *Metrics, testDuration time.Duration, target int) {
	fmt.Println("\n==================================================")
	fmt.Println("             LOAD TEST RESULTS SUMMARY            ")
	fmt.Println("==================================================")
	fmt.Printf("Duration of Test:          %v\n", testDuration)
	fmt.Printf("Target Concurrency:        %d active players\n", target)
	fmt.Printf("Successful Connections:    %d\n", atomic.LoadInt64(&m.ConnectedCount))
	fmt.Printf("Failed Connections:        %d\n", atomic.LoadInt64(&m.FailedConnCount))
	fmt.Printf("Matchmaking Pairs Made:    %d\n", atomic.LoadInt64(&m.MatchedCount)/2)
	fmt.Printf("Total Chess Moves Sent:    %d\n", atomic.LoadInt64(&m.MovesSentCount))
	
	connCount := atomic.LoadInt64(&m.ConnectedCount)
	if connCount > 0 {
		avgHandshake := atomic.LoadInt64(&m.HandshakeSumMs) / connCount
		fmt.Printf("Avg Socket Handshake Latency: %d ms\n", avgHandshake)
	}
	fmt.Println("==================================================")
}

func bootstrapDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	_, _ = pool.Exec(ctx, `
		DO $$ 
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'GameStatus') THEN
				CREATE TYPE "GameStatus" AS ENUM ('WAITING', 'IN_PROGRESS', 'COMPLETED', 'ABANDONED', 'DRAW');
			END IF;
			IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'GameWinner') THEN
				CREATE TYPE "GameWinner" AS ENUM ('WHITE', 'BLACK', 'DRAW');
			END IF;
		END $$;
	`)

	queries := []string{
		`CREATE TABLE IF NOT EXISTS "User" (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE NOT NULL,
			"emailVerified" BOOLEAN DEFAULT FALSE,
			image TEXT,
			rating INT DEFAULT 1200,
			"createdAt" TIMESTAMP DEFAULT NOW(),
			"updatedAt" TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS "Session" (
			id TEXT PRIMARY KEY,
			token TEXT UNIQUE NOT NULL,
			"expiresAt" TIMESTAMP NOT NULL,
			"userId" TEXT NOT NULL REFERENCES "User"(id) ON DELETE CASCADE,
			"ipAddress" TEXT,
			"userAgent" TEXT,
			"createdAt" TIMESTAMP DEFAULT NOW(),
			"updatedAt" TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS "Game" (
			id TEXT PRIMARY KEY,
			"whitePlayerId" TEXT NOT NULL REFERENCES "User"(id),
			"blackPlayerId" TEXT REFERENCES "User"(id),
			fen TEXT DEFAULT 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1',
			pgn TEXT DEFAULT '',
			status "GameStatus" DEFAULT 'WAITING',
			winner "GameWinner",
			"createdAt" TIMESTAMP DEFAULT NOW(),
			"updatedAt" TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS "idx_game_white" ON "Game" ("whitePlayerId")`,
		`CREATE INDEX IF NOT EXISTS "idx_game_black" ON "Game" ("blackPlayerId")`,
		`CREATE INDEX IF NOT EXISTS "idx_session_user" ON "Session" ("userId")`,
	}

	for _, q := range queries {
		if _, err := pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}
	return nil
}
