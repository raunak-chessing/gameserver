package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(redisURL string) (*RedisClient, error) {
	var options *redis.Options
	var err error

	options, err = redis.ParseURL(redisURL)
	if err != nil {
		options = &redis.Options{
			Addr:     redisURL,
			Password: "",
			DB:       0,
		}
	}

	client := redis.NewClient(options)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	return &RedisClient{client: client}, nil
}

func (r *RedisClient) Close() {
	r.client.Close()
}

func (r *RedisClient) Client() *redis.Client {
	return r.client
}

func (r *RedisClient) PublishGameEvent(ctx context.Context, gameID string, payload []byte) error {
	channel := fmt.Sprintf("game:%s", gameID)
	err := r.client.SPublish(ctx, channel, payload).Err()
	if err != nil {
		return fmt.Errorf("failed to sharded-publish to Redis channel %s: %w", channel, err)
	}
	return nil
}

func (r *RedisClient) SubscribeGameEvent(ctx context.Context, gameID string) *redis.PubSub {
	channel := fmt.Sprintf("game:%s", gameID)
	return r.client.SSubscribe(ctx, channel)
}

func (r *RedisClient) CacheGameState(ctx context.Context, gameID string, fen string, expiration time.Duration) error {
	key := fmt.Sprintf("game_state:%s", gameID)
	err := r.client.Set(ctx, key, fen, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to cache game state: %w", err)
	}
	return nil
}

func (r *RedisClient) GetCachedGameState(ctx context.Context, gameID string) (string, error) {
	key := fmt.Sprintf("game_state:%s", gameID)
	val, err := r.client.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}
	return val, nil
}

func (r *RedisClient) SaveFullGameState(ctx context.Context, gameID string, state []byte, expiration time.Duration) error {
	key := fmt.Sprintf("game_state_full:%s", gameID)
	err := r.client.Set(ctx, key, state, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to save full game state: %w", err)
	}
	return nil
}

func (r *RedisClient) LoadFullGameState(ctx context.Context, gameID string) ([]byte, error) {
	key := fmt.Sprintf("game_state_full:%s", gameID)
	val, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}
	return val, nil
}
