package redisutils

import (
	"strconv"
	"strings"

	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
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
		return nil, nil
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

// ParseWalks() parses a slice of strings to a slice of random walks
func ParseWalks(strWalks []string) ([]models.RandomWalk, error) {
	if len(strWalks) == 0 {
		return nil, nil
	}

	walks := make([]models.RandomWalk, 0, len(strWalks))
	for _, strWalk := range strWalks {
		walk, err := ParseWalk(strWalk)
		if err != nil {
			return nil, err
		}

		walks = append(walks, walk)
	}

	return walks, nil
}

// FormatID() formats a POSITIVE ID into a string.
// Warning: don't pass negative IDs, or they will be converted incorrectly.
func FormatID[ID uint32 | int64 | int](id ID) string {
	return strconv.FormatUint(uint64(id), 10)
}

// ParseID() parses a nodeID or walkID (uint32) from the specified string
func ParseID(strID string) (uint32, error) {
	ID, err := strconv.ParseUint(strID, 10, 32)
	return uint32(ID), err
}

// FormatIDs() formats a slice of POSITIVE IDs into a slice of string.
// Warning: don't pass negative IDs, or they will be converted incorrectly.
func FormatIDs[ID uint32 | int64 | int](IDs []ID) []string {
	if len(IDs) == 0 {
		return []string{}
	}

	strIDs := make([]string, 0, len(IDs))
	for _, ID := range IDs {
		strIDs = append(strIDs, FormatID(ID))
	}

	return strIDs
}

// ParseIDs() parses a slice of IDs from the specified slice of string.
func ParseIDs(strIDs []string) ([]uint32, error) {
	if len(strIDs) == 0 {
		return nil, nil
	}

	IDs := make([]uint32, 0, len(strIDs))
	for _, strID := range strIDs {
		ID, err := ParseID(strID)
		if err != nil {
			return nil, nil
		}

		IDs = append(IDs, ID)
	}
	return IDs, nil
}

// ParseUniqueIDs() parses a slice of unique IDs (no repetition), from the specified slice of string.
func ParseUniqueIDs(strIDs []string) ([]uint32, error) {
	IDs, err := ParseIDs(strIDs)
	if err != nil {
		return nil, nil
	}

	return sliceutils.Unique(IDs), nil
}

// ParseInt64() parses an int from the specified string
func ParseInt64(strVal string) (int64, error) {
	return strconv.ParseInt(strVal, 10, 64)
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
	return strconv.ParseFloat(strVal, 64)
}
