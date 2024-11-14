package redisutils

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// SetupRedis() initializes a new Redis client for testing purposes.
func SetupClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Update if needed for your test Redis server
	})
}

// CleanupRedis() cleans up the Redis database between tests to ensure isolation.
func CleanupRedis(client *redis.Client) {
	client.FlushAll(context.Background())
}
