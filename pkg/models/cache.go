package models

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/puzpuzpuz/xsync/v3"
)

// NodeFilterAttributes contains attributes of a node used to filter a Nostr
// event without querying the Database.
type NodeFilterAttributes struct {
	ID        uint32  `json:"ID"`
	Timestamp int64   `json:"Timestamp"`
	Pagerank  float64 `json:"Pagerank"`
}

// NodeCache is a concurrent-safe map pubkey --> filter attributes of the corrisponding node.
type NodeCache = *xsync.MapOf[string, NodeFilterAttributes]

// NewNodeCache() returns an initialized NodeCache
func NewNodeCache() NodeCache {
	return xsync.NewMapOf[string, NodeFilterAttributes]()
}

// ToMap returns a regular Go map with the same key-value pairs as the NodeCache.
// If the NodeCache is nil, it returns a nil map. This function is used in tests
// to compare two NodeCaches using the reflect.DeepEqual
func ToMap(NC NodeCache) map[string]NodeFilterAttributes {

	if NC == nil {
		return nil
	}

	goMap := make(map[string]NodeFilterAttributes, NC.Size())
	NC.Range(func(key string, value NodeFilterAttributes) bool {
		goMap[key] = value
		return true
	})

	return goMap
}

// FromJSON unmarshal a JSON string and returns the data as a NodeCache.
func FromJSON(JSON string) (NodeCache, error) {

	NC := NewNodeCache()
	if len(JSON) == 0 {
		return NC, nil
	}

	var tempMap map[string]NodeFilterAttributes
	if err := json.Unmarshal([]byte(JSON), &tempMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the JSON: %w", err)
	}

	for key, val := range tempMap {
		NC.Store(key, val)
	}

	return NC, nil
}

//--------------------------ERROR-CODES--------------------------

var ErrNilNCPointer = errors.New("node cache pointer is nil")
var ErrNodeNotFoundNC = errors.New("node not found in the node cache")
