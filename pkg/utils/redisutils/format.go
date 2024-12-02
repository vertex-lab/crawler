package redisutils

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/vertex-lab/crawler/pkg/models"
)

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

// FormatID() formats a nodeID or walkID (uint32) into a string
func FormatID(ID uint32) string {
	return strconv.FormatUint(uint64(ID), 10)
}

// ParseID() parses a nodeID or walkID (uint32) from the specified string
func ParseID(strVal string) (uint32, error) {
	parsedVal, err := strconv.ParseUint(strVal, 10, 32)
	return uint32(parsedVal), err
}

// ParseInt64() parses an int from the specified string
func ParseInt64(strVal string) (int64, error) {
	parsedVal, err := strconv.ParseInt(strVal, 10, 64)
	return parsedVal, err
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
