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

const KeyRWS string = "RWS"
const KeyAlpha string = "alpha"
const KeyWalksPerNode string = "walksPerNode"
const KeyLastWalkID string = "lastWalkID"
const KeyWalkPrefix string = "walk:"
const KeyNodeWalkIDsPrefix string = "nodeWalkIDs:"

// KeyWalk() returns the Redis key for the walk with specified walkID
func KeyWalk(walkID uint32) string {
	return fmt.Sprintf("%v%d", KeyWalkPrefix, walkID)
}

// KeyNodeWalkIDs() returns the Redis key for the nodeWalkIDs with specified nodeID
func KeyNodeWalkIDs(nodeID uint32) string {
	return fmt.Sprintf("%v%d", KeyNodeWalkIDsPrefix, nodeID)
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

// ParseWalkMap parses the result of a Redis Lua script into a map[uint32]models.RandomWalk.
// The input `result` should be a Redis result containing two slices:
// - strWalkIDs as strings, and
// - strWalks as serialized strings, which will be parsed into models.RandomWalk.
func ParseWalkMap(result interface{}) (map[uint32]models.RandomWalk, error) {

	resultList, ok := result.([]interface{})
	if !ok || len(resultList) != 2 {
		return nil, fmt.Errorf("unexpected result format: %v", result)
	}

	// convert the interfaces to slices of strings
	var strWalkIDs []string
	for _, v := range resultList[0].([]interface{}) {
		strWalkID, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected format for walkID: %v", v)
		}
		strWalkIDs = append(strWalkIDs, strWalkID)
	}

	var strWalks []string
	for _, v := range resultList[1].([]interface{}) {
		strWalk, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected format for walk: %v", v)
		}
		strWalks = append(strWalks, strWalk)
	}

	// create the walkMap with parsed RandomWalks
	walkMap := make(map[uint32]models.RandomWalk, len(strWalkIDs))
	for i, strWalkID := range strWalkIDs {
		walkID, err := ParseID(strWalkID)
		if err != nil {
			return nil, err
		}

		walk, err := ParseWalk(strWalks[i])
		if err != nil {
			return nil, err
		}

		walkMap[uint32(walkID)] = walk
	}

	return walkMap, nil
}

// SetupRWS returns a RandomWalkStore ready to be used in tests
func SetupRWS(cl *redis.Client, RWSType string) (*RandomWalkStore, error) {
	if cl == nil {
		return nil, ErrNilClientPointer
	}

	switch RWSType {
	case "nil":
		return nil, nil

	case "empty":
		RWS, err := NewRWS(context.Background(), cl, 0.85, 1)
		if err != nil {
			return nil, err
		}
		return RWS, nil

	case "one-node0":
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walk := models.RandomWalk{0}
		if err := cl.Set(ctx, KeyWalk(0), FormatWalk(walk), 0).Err(); err != nil {
			return nil, err
		}

		if err := cl.SAdd(ctx, KeyNodeWalkIDs(0), 0).Err(); err != nil {
			return nil, err
		}

		return RWS, nil

	case "one-walk0":
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walk := models.RandomWalk{0, 1, 2, 3}
		if err := cl.Set(ctx, KeyWalk(0), FormatWalk(walk), 0).Err(); err != nil {
			return nil, err
		}

		for _, nodeID := range walk {
			if err := cl.SAdd(ctx, KeyNodeWalkIDs(nodeID), 0).Err(); err != nil {
				return nil, err
			}
		}

		return RWS, nil

	case "triangle":
		// 0 --> 1 --> 2 --> 0
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walks := []models.RandomWalk{{0, 1, 2}, {1, 2, 0}, {2, 0, 1}}
		for _, walk := range walks {
			if err := RWS.AddWalk(walk); err != nil {
				return nil, err
			}
		}

		return RWS, nil

	case "complex":
		// 0 --> 1 --> 2
		// 0 --> 3
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walks := []models.RandomWalk{{0, 1, 2}, {0, 3}, {1, 2}}
		for _, walk := range walks {
			if err := RWS.AddWalk(walk); err != nil {
				return nil, err
			}
		}
		return RWS, nil

	default:
		return nil, nil
	}
}

//---------------------------------ERROR-CODES---------------------------------

var ErrNilClientPointer = errors.New("nil redis client pointer")
