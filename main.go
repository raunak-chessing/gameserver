package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"gameserver/internal/application"
	"gameserver/internal/config"
	"gameserver/internal/infrastructure/cache"
	"gameserver/internal/infrastructure/db"
	"gameserver/internal/infrastructure/websocket"
	"gameserver/internal/metrics"

	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func recoverAndLog(goroutine string) {
	if r := recover(); r != nil {
		log.Printf("recovered from panic in %s: %v\n%s", goroutine, r, debug.Stack())
		sentry.CurrentHub().Recover(r)
		sentry.Flush(2 * time.Second)
	}
}

func main() {
	log.Println("Starting Chess Game Server...")

	appEnv := os.Getenv("APP_ENV")
	if appEnv == "" {
		appEnv = "development"
	}
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:              os.Getenv("SENTRY_DSN"),
		Environment:      appEnv,
		TracesSampleRate: 0,
	}); err != nil {
		log.Printf("Sentry initialization failed: %v", err)
	}
	defer sentry.Flush(2 * time.Second)

	cfg := config.Load()

	database, err := db.NewDB(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() {
		log.Println("Closing database connection pool...")
		database.Close()
	}()
	log.Println("PostgreSQL connection pool initialized successfully.")

	redisClient, err := cache.NewRedisClient(cfg.RedisURL)
	if err != nil {
		log.Fatalf("Failed to initialize Redis: %v", err)
	}
	defer func() {
		log.Println("Closing Redis client...")
		redisClient.Close()
	}()
	log.Println("Redis client initialized successfully.")

	gameService := application.NewGameService(database, redisClient)
	matchmaker := application.NewMatchmaker(gameService, redisClient.Client())
	hub := websocket.NewHub(matchmaker, gameService, redisClient, database)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Println("Starting Database Batch Processor...")
	go func() {
		defer recoverAndLog("database batch processor")
		database.StartBatchProcessor(ctx)
	}()

	log.Println("Starting Matchmaker loop...")
	go func() {
		defer recoverAndLog("matchmaker loop")
		matchmaker.Start(ctx)
	}()

	log.Println("Starting Session Hub loop...")
	go func() {
		defer recoverAndLog("session hub loop")
		hub.Run(ctx)
	}()

	wsHandler := websocket.NewHandler(hub, database)

	metrics.RegisterActiveSessionsGauge(func() float64 {
		return float64(hub.ActiveGameSessionCount())
	})

	mux := http.NewServeMux()
	mux.Handle("/ws", wsHandler)
	mux.HandleFunc("/health", websocket.HealthHandler)
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		defer recoverAndLog("http listener")
		log.Printf("Chess Game Server listening on port %s...", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server ListenAndServe failed: %v", err)
		}
	}()

	sig := <-shutdownChan
	log.Printf("Received signal %v. Initiating graceful shutdown...", sig)

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Chess Game Server stopped cleanly.")
}
