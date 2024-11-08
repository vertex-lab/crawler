package redistore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	"github.com/redis/go-redis/v9"
)

// The standard Redis keys to be used across Set and Get methods
const KeyWalk = "walk"
const KeyNode = "node"
const KeyRWS = "RWS"
const KeyNodeWalkIDs = "nodeWalkIDs"

// KeyRedis() concatenate the KeyType with the ID (with the ":" in between)
func KeyRedis(KeyType string, ID uint32) string {
	return fmt.Sprintf("%s:%d", KeyType, ID)
}

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

// FormatWalk() formats a RandomWalk into a string ready to be stored in Redis.
func FormatWalk(walk models.RandomWalk) string {
	strVals := make([]string, len(walk))
	for i, val := range walk {
		strVals[i] = strconv.FormatUint(uint64(val), 10)
	}

	return strings.Join(strVals, ",")
}

// ParseWalk() parses a string to a RandomWalk
func ParseWalk(strWalk string) (models.RandomWalk, error) {

	if len(strWalk) == 0 {
		return models.RandomWalk{}, nil
	}

	strVals := strings.Split(strWalk, ",")
	walk := make(models.RandomWalk, len(strVals))

	for i, str := range strVals {
		val, err := strconv.ParseUint(str, 10, 32)
		if err != nil {
			return nil, err
		}

		walk[i] = uint32(val)
	}
	return walk, nil
}

// ParseID() parses a nodeID or walkID (uint32) from the specified string
func ParseID(strVal string) (uint32, error) {
	parsedVal, err := strconv.ParseUint(strVal, 10, 32)
	return uint32(parsedVal), err
}

// ParseUint16() parses an uint16 from the specified string
func ParseUint16(strVal string) (uint16, error) {
	parsedVal, err := strconv.ParseUint(strVal, 10, 16)
	return uint16(parsedVal), err
}

// ParseFloat32() parses a float32 from the specified string
func ParseFloat32(strVal string) (float32, error) {
	parsedVal, err := strconv.ParseFloat(strVal, 32)
	return float32(parsedVal), err
}

// ParseFloat64() parses a float64 from the specified string
func ParseFloat64(strVal string) (float64, error) {
	parsedVal, err := strconv.ParseFloat(strVal, 64)
	return parsedVal, err
}

// SetupRWS returns a RandomWalkStore ready to be used in tests
func SetupRWS(cl *redis.Client, RWSType string) *RandomWalkStore {
	if cl == nil {
		return nil
	}

	switch RWSType {
	case "nil":
		return nil

	case "empty":
		RWS, _ := NewRWS(context.Background(), cl, 0.85, 1)
		return RWS

	case "one-walk0":
		ctx := context.Background()
		RWS, _ := NewRWS(ctx, cl, 0.85, 1)
		if err := cl.Set(ctx, "walk:0", "0,1,2,3", 0).Err(); err != nil {
			return nil
		}
		return RWS

	default:
		return nil
	}
}

//---------------------------------ERROR-CODES---------------------------------

var ErrUnsupportedType = errors.New("unsupported datatype")
