// The redisutils package simplifies and automates recurring operations like
// connecting to, formatting for, and parsing from Redis.
package redisutils

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// SetupRedis() initializes a new Redis client for testing purposes.
func SetupClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})
}

// CleanupRedis() cleans up the Redis database between tests to ensure isolation.
func CleanupRedis(client *redis.Client) {
	client.FlushAll(context.Background())
}
