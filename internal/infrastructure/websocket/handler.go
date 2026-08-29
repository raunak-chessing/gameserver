package websocket

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"gameserver/internal/infrastructure/db"
)

type Handler struct {
	hub *Hub
	db  *db.DB
}

func NewHandler(hub *Hub, db *db.DB) *Handler {
	return &Handler{
		hub: hub,
		db:  db,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := h.extractToken(r)
	if token == "" {
		http.Error(w, "Unauthorized: Session token missing", http.StatusUnauthorized)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	player, err := h.db.AuthenticateSession(ctx, token)
	if err != nil {
		log.Printf("Authentication failed: %v", err)
		http.Error(w, "Unauthorized: "+err.Error(), http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade connection: %v", err)
		return
	}

	client := &Client{
		hub:    h.hub,
		conn:   conn,
		player: player,
	}

	h.hub.Register <- client

	go func() {
		defer recoverAndLog(fmt.Sprintf("ReadPump for player %s", player.ID))
		client.ReadPump()
	}()
}

func (h *Handler) extractToken(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		parts := strings.Split(authHeader, " ")
		if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
			return parts[1]
		}
	}

	cookie, err := r.Cookie("better-auth.session_token")
	if err == nil {
		return cookie.Value
	}
	cookieAlt, err := r.Cookie("__Secure-better-auth.session_token")
	if err == nil {
		return cookieAlt.Value
	}

	return ""
}

func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"OK"}`))
}
