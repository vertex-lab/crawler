package redistore

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
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

// FormatWalk() formats a RandomWalk into a string, ready to be stored on Redis
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

// FormatToString() formats a specified value to a string, based on its type
func FormatToString(value interface{}) interface{} {
	switch v := value.(type) {
	case models.RandomWalk:
		return FormatWalk(v)
	default:
		// floats and int can be written directly to Redis without convertion
		return v
	}
}

// ParseFromString parses a string to a specified type
func ParseFromString(strVal, datatype string) (interface{}, error) {
	switch datatype {
	case "RandomWalk":
		walk, err := ParseWalk(strVal)
		if err != nil {
			return nil, err
		}
		return walk, nil

	case "float32":
		parsedVal, err := strconv.ParseFloat(strVal, 32)
		if err != nil {
			return nil, err
		}
		return float32(parsedVal), nil

	case "float64":
		parsedVal, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return nil, err
		}
		return parsedVal, nil

	case "uint16":
		parsedVal, err := strconv.ParseUint(strVal, 10, 16)
		if err != nil {
			return nil, err
		}
		return uint16(parsedVal), nil

	case "uint32":
		parsedVal, err := strconv.ParseUint(strVal, 10, 32)
		if err != nil {
			return nil, err
		}
		return uint32(parsedVal), nil

	default:
		return nil, ErrUnsupportedType
	}
}

// FormatAndSet formats a specified value to string and stores the key-value pair in redis.
func FormatAndSetString(ctx context.Context, cl *redis.Client,
	key string, value interface{}) error {
	formattedValue := FormatToString(value)
	if err := cl.Set(ctx, key, formattedValue, 0).Err(); err != nil {
		return err
	}
	return nil
}

// GetStringAndParse fetches a key from redis and parse it according to the specified datatype
func GetStringAndParse(ctx context.Context, cl *redis.Client,
	key string, datatype string) (interface{}, error) {

	strVal, err := cl.Get(context.Background(), key).Result()
	if err != nil {
		return nil, err
	}

	return ParseFromString(strVal, datatype)
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
		FormatAndSetString(ctx, cl, "walk:0", models.RandomWalk{0, 1, 2})
		return RWS

	default:
		return nil
	}
}

//---------------------------------ERROR-CODES---------------------------------

var ErrUnsupportedType = errors.New("unsupported datatype")
