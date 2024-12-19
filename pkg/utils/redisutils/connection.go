// The redisutils package simplifies and automates recurring operations like
// connecting to, formatting for, and parsing from Redis.
package redisutils

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// SetupProdRedis() initializes a new Redis client for production.
func SetupProdClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}

// SetupProdRedis() initializes a new Redis client for production.
func SetupTestClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6380",
	})
}

// CleanupRedis() cleans up the Redis database between tests to ensure isolation.
func CleanupRedis(client *redis.Client) {
	client.FlushAll(context.Background())
}
