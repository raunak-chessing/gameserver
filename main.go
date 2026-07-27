package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gameserver/internal/application"
	"gameserver/internal/config"
	"gameserver/internal/infrastructure/cache"
	"gameserver/internal/infrastructure/db"
	"gameserver/internal/infrastructure/websocket"
)

func main() {
	log.Println("Starting Chess Game Server...")

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
	go database.StartBatchProcessor(ctx)

	log.Println("Starting Matchmaker loop...")
	go matchmaker.Start(ctx)

	log.Println("Starting Session Hub loop...")
	go hub.Run(ctx)

	wsHandler := websocket.NewHandler(hub, database)
	
	mux := http.NewServeMux()
	mux.Handle("/ws", wsHandler)
	mux.HandleFunc("/health", websocket.HealthHandler)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	go func() {
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