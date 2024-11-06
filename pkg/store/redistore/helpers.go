package redistore

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// SetupRedis() initializes a new Redis client for testing purposes.
func SetupRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Update if needed for your test Redis server
	})
}

// CleanupRedis() cleans up the Redis database between tests to ensure isolation.
func CleanupRedis(client *redis.Client) {
	client.FlushAll(context.Background())
}

// GetAndParse fetches a key from redis and parse it according to the specified datatype
func GetAndParse(ctx context.Context, cl *redis.Client,
	key string, datatype string) (interface{}, error) {

	val, err := cl.Get(context.Background(), key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load %v: %v", key, err)
	}

	switch datatype {
	case "float32":
		parsedVal, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %v as float32: %v", key, err)
		}
		return float32(parsedVal), nil

	case "float64":
		parsedVal, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %v as float32: %v", key, err)
		}
		return parsedVal, nil

	case "uint16":
		parsedVal, err := strconv.ParseUint(val, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %v as float32: %v", key, err)
		}
		return uint16(parsedVal), nil

	default:
		return nil, ErrUnsupportedType
	}
}

//---------------------------------ERROR-CODES---------------------------------

var ErrUnsupportedType = errors.New("unsupported datatype")
