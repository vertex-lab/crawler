// The mock database package allows for testing that are decoupled from a
// particular database implementation.
package mock

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
)

type NodeSet mapset.Set[uint32]

// simulates a simple graph database for testing.
type Database struct {

	// a map that associates each public key with a unique nodeID
	KeyIndex map[string]uint32

	// a map that associates each nodeID with its node data
	NodeIndex map[uint32]*models.Node

	// maps that associate each nodeID with the slice of its follows/mutes...
	Follow   map[uint32]NodeSet
	Follower map[uint32]NodeSet

	// the next nodeID to be used. When a new node is added, this fiels is incremented by one
	LastNodeID int
}

// NewDatabase() creates and returns a new Database instance.
func NewDatabase() *Database {
	return &Database{
		KeyIndex:   make(map[string]uint32),
		NodeIndex:  make(map[uint32]*models.Node),
		Follow:     make(map[uint32]NodeSet),
		Follower:   make(map[uint32]NodeSet),
		LastNodeID: -1, // the first nodeID will be 0
	}
}

// Validate() returns an error if the DB is nil or has no nodes
func (DB *Database) Validate() error {
	if DB == nil {
		return models.ErrNilDB
	}

	return nil
}

// AddNode() adds a node to the database and returns its assigned nodeID.
// In case of errors, it returns MaxUint32 as the nodeID.
func (DB *Database) AddNode(ctx context.Context, pubkey string) (uint32, error) {
	_ = ctx
	if DB == nil {
		return math.MaxUint32, models.ErrNilDB
	}

	if _, exist := DB.KeyIndex[pubkey]; exist {
		return math.MaxUint32, models.ErrNodeAlreadyInDB
	}

	// add the node to the KeyIndex
	DB.LastNodeID++
	nodeID := uint32(DB.LastNodeID)
	DB.KeyIndex[pubkey] = nodeID

	// add follows and followers of the node
	DB.Follow[nodeID] = mapset.NewSet[uint32]()
	DB.Follower[nodeID] = mapset.NewSet[uint32]()

	// add the node to the NodeIndex
	DB.NodeIndex[nodeID] = &models.Node{
		ID:      nodeID,
		Pubkey:  pubkey,
		Status:  models.StatusInactive,
		Records: []models.Record{{Kind: models.Added, Timestamp: time.Now().Unix()}},
	}

	return nodeID, nil
}

// Update() applies the delta to nodeID.
func (DB *Database) Update(ctx context.Context, delta *models.Delta) error {
	_ = ctx
	var err error
	if err = DB.Validate(); err != nil {
		return err
	}

	if delta == nil {
		return models.ErrNilDelta
	}

	if _, exist := DB.NodeIndex[delta.NodeID]; !exist {
		return models.ErrNodeNotFoundDB
	}

	switch delta.Kind {
	case models.Promotion, models.Demotion:
		err = DB.updateStatus(ctx, delta.NodeID, delta.Record)

	case nostr.KindFollowList:
		err = DB.updateFollows(ctx, delta)
	}

	if err != nil {
		return fmt.Errorf("failed to update with delta %v: %w", delta, err)
	}

	return nil
}

// updateStatus updates the status of nodeID
func (DB *Database) updateStatus(ctx context.Context, nodeID uint32, record models.Record) error {
	_ = ctx

	switch record.Kind {
	case models.Promotion:
		DB.NodeIndex[nodeID].Status = models.StatusActive

	case models.Demotion:
		DB.NodeIndex[nodeID].Status = models.StatusInactive

	default:
		return fmt.Errorf("invalid record type: %v", record.Kind)
	}

	DB.NodeIndex[nodeID].Records = append(DB.NodeIndex[nodeID].Records, record)
	return nil
}

// updateFollows adds and removed follow relationships, and adds a record.
func (DB *Database) updateFollows(ctx context.Context, delta *models.Delta) error {
	_ = ctx
	// add all added to the follows of nodeID
	if _, exists := DB.Follow[delta.NodeID]; !exists {
		DB.Follow[delta.NodeID] = mapset.NewSet[uint32]()
	}
	DB.Follow[delta.NodeID].Append(delta.Added...)

	// add nodeID to the followers of added
	for _, ID := range delta.Added {
		if _, exists := DB.Follower[ID]; !exists {
			DB.Follower[ID] = mapset.NewSet[uint32]()
		}
		DB.Follower[ID].Add(delta.NodeID)
	}

	// remove all removed to the follows of nodeID
	if _, exists := DB.Follow[delta.NodeID]; exists {
		DB.Follow[delta.NodeID].RemoveAll(delta.Removed...)
	}

	// remove nodeID to the followers of removed
	for _, ID := range delta.Removed {
		if _, exists := DB.Follower[ID]; exists {
			DB.Follower[ID].Remove(delta.NodeID)
		}
	}

	DB.NodeIndex[delta.NodeID].Records = append(DB.NodeIndex[delta.NodeID].Records, delta.Record)
	return nil
}

// ContainsNode() returns whether nodeID is found in the DB
func (DB *Database) ContainsNode(ctx context.Context, nodeID uint32) bool {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return false
	}

	_, exist := DB.NodeIndex[nodeID]
	return exist
}

// NodeByKey() retrieves a node (Node) by its pubkey.
func (DB *Database) NodeByKey(ctx context.Context, pubkey string) (*models.Node, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	nodeID, exists := DB.KeyIndex[pubkey]
	if !exists {
		return nil, models.ErrNodeNotFoundDB
	}

	return DB.NodeIndex[nodeID], nil
}

// NodeByID() retrieves a node (Node) by its nodeID.
func (DB *Database) NodeByID(ctx context.Context, nodeID uint32) (*models.Node, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	node, exist := DB.NodeIndex[nodeID]
	if !exist {
		return nil, models.ErrNodeNotFoundDB
	}

	return node, nil
}

// Follows() returns the slice of follows of each nodeID
func (DB *Database) Follows(ctx context.Context, nodeIDs ...uint32) ([][]uint32, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	followSlice := make([][]uint32, len(nodeIDs))
	for i, ID := range nodeIDs {
		follows, exists := DB.Follow[ID]
		if !exists {
			return nil, models.ErrNodeNotFoundDB
		}

		followSlice[i] = follows.ToSlice()
	}

	return followSlice, nil
}

// Followers() returns the slice of followers of each nodeID
func (DB *Database) Followers(ctx context.Context, nodeIDs ...uint32) ([][]uint32, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	followerSlice := make([][]uint32, len(nodeIDs))
	for i, ID := range nodeIDs {
		followers, exists := DB.Follower[ID]
		if !exists {
			return nil, models.ErrNodeNotFoundDB
		}

		followerSlice[i] = followers.ToSlice()
	}

	return followerSlice, nil
}

// Pubkeys() returns a slice of pubkeys that correspond with the given slice of nodeIDs.
// If a pubkey is not found, nil is returned.
func (DB *Database) Pubkeys(ctx context.Context, nodeIDs ...uint32) ([]*string, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	pubkeys := make([]*string, len(nodeIDs))
	for i, ID := range nodeIDs {
		node, exist := DB.NodeIndex[ID]
		if !exist {
			pubkeys[i] = nil
			continue
		}

		pubkeys[i] = &node.Pubkey
	}

	return pubkeys, nil
}

// NodeIDs() returns a slice of nodeIDs that correspond with the given slice of pubkeys.
// If a pubkey is not found, nil is returned
func (DB *Database) NodeIDs(ctx context.Context, pubkeys ...string) ([]*uint32, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if len(pubkeys) == 0 {
		return nil, nil
	}

	nodeIDs := make([]*uint32, len(pubkeys))
	for i, pubkey := range pubkeys {
		nodeID, exist := DB.KeyIndex[pubkey]
		if !exist {
			nodeIDs[i] = nil
			continue
		}

		nodeIDs[i] = &nodeID
	}

	return nodeIDs, nil
}

// All returns a slice with the IDs of all the nodes
func (DB *Database) AllNodes(ctx context.Context) ([]uint32, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	nodeIDs := make([]uint32, 0, len(DB.NodeIndex))
	for nodeID := range DB.NodeIndex {
		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, nil
}

// Size() returns the number of nodes in the DB (ignores errors).
func (DB *Database) Size(ctx context.Context) int {
	_ = ctx
	if DB == nil {
		return 0
	}
	return len(DB.NodeIndex)
}

// ScanNodes() scans over the nodes and returns all of the nodeIDs, ignoring the limit.
func (DB *Database) ScanNodes(ctx context.Context, cursor uint64, limit int) ([]uint32, uint64, error) {
	_ = ctx
	_ = limit

	// Cursor simulation: returning 0 as the cursor for simplicity
	nodeIDs, err := DB.AllNodes(ctx)
	return nodeIDs, 0, err
}

// ------------------------------------HELPERS----------------------------------

var (
	// pubkeys for testing
	odell string = "04c915daefee38317fa734444acee390a8269fe5810b2241e5e6dd343dfbecc9"
	calle string = "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"
	pip   string = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
)

// function that returns a DB setup based on the DBType
func SetupDB(DBType string) *Database {
	switch DBType {

	case "nil":
		return nil

	case "empty":
		return NewDatabase()

	case "dandling":
		DB := NewDatabase()
		DB.KeyIndex = map[string]uint32{"0": 0, "1": 1, "2": 2}
		DB.LastNodeID = 2
		DB.NodeIndex[0] = &models.Node{ID: 0, Pubkey: "0"}
		DB.NodeIndex[1] = &models.Node{ID: 1, Pubkey: "1"}
		DB.NodeIndex[2] = &models.Node{ID: 2, Pubkey: "2"}

		DB.Follow[0] = mapset.NewSet[uint32]()
		DB.Follow[1] = mapset.NewSet[uint32](2)
		DB.Follow[2] = mapset.NewSet[uint32](1)
		return DB

	case "one-node0":
		DB := NewDatabase()
		DB.KeyIndex = map[string]uint32{"0": 0}
		DB.LastNodeID = 0
		DB.NodeIndex[0] = &models.Node{ID: 0, Pubkey: "0"}

		DB.Follow[0] = mapset.NewSet[uint32]()
		DB.Follower[0] = mapset.NewSet[uint32]()
		return DB

	case "one-node1":
		DB := NewDatabase()
		DB.KeyIndex = map[string]uint32{"1": 1}
		DB.LastNodeID = 1
		DB.NodeIndex[1] = &models.Node{ID: 1, Pubkey: "1"}

		DB.Follow[1] = mapset.NewSet[uint32]()
		DB.Follower[1] = mapset.NewSet[uint32]()
		return DB

	case "triangle":
		DB := NewDatabase()
		DB.KeyIndex = map[string]uint32{"0": 0, "1": 1, "2": 2}
		DB.LastNodeID = 2
		DB.NodeIndex[0] = &models.Node{ID: 0, Pubkey: "0"}
		DB.NodeIndex[1] = &models.Node{ID: 1, Pubkey: "1"}
		DB.NodeIndex[2] = &models.Node{ID: 2, Pubkey: "2"}

		DB.Follow[0] = mapset.NewSet[uint32](1)
		DB.Follow[1] = mapset.NewSet[uint32](2)
		DB.Follow[2] = mapset.NewSet[uint32](0)

		DB.Follower[0] = mapset.NewSet[uint32](2)
		DB.Follower[1] = mapset.NewSet[uint32](0)
		DB.Follower[2] = mapset.NewSet[uint32](1)
		return DB

	case "simple":
		DB := NewDatabase()
		DB.KeyIndex = map[string]uint32{"0": 0, "1": 1, "2": 2}
		DB.LastNodeID = 2
		DB.NodeIndex[0] = &models.Node{ID: 0, Pubkey: "0", Status: models.StatusInactive}
		DB.NodeIndex[1] = &models.Node{ID: 1, Pubkey: "1", Status: models.StatusActive}
		DB.NodeIndex[2] = &models.Node{ID: 2, Pubkey: "2", Status: models.StatusInactive}

		DB.Follow[0] = mapset.NewSet[uint32](1)
		DB.Follow[1] = mapset.NewSet[uint32]()
		DB.Follow[2] = mapset.NewSet[uint32]()

		DB.Follower[0] = mapset.NewSet[uint32]()
		DB.Follower[1] = mapset.NewSet[uint32](0)
		DB.Follower[2] = mapset.NewSet[uint32]()
		return DB

	case "simple-with-pks":
		DB := NewDatabase()
		DB.KeyIndex = map[string]uint32{odell: 0, calle: 1, pip: 2}
		DB.LastNodeID = 2
		DB.NodeIndex[0] = &models.Node{ID: 0, Pubkey: odell, Status: models.StatusInactive}
		DB.NodeIndex[1] = &models.Node{ID: 1, Pubkey: calle, Status: models.StatusActive}
		DB.NodeIndex[2] = &models.Node{ID: 2, Pubkey: pip, Status: models.StatusInactive}

		DB.Follow[0] = mapset.NewSet[uint32](1)
		DB.Follow[1] = mapset.NewSet[uint32]()
		DB.Follow[2] = mapset.NewSet[uint32]()

		DB.Follower[0] = mapset.NewSet[uint32]()
		DB.Follower[1] = mapset.NewSet[uint32](0)
		DB.Follower[2] = mapset.NewSet[uint32]()
		return DB

	case "pip":
		DB := NewDatabase()
		DB.KeyIndex = map[string]uint32{pip: 0}
		DB.LastNodeID = 0
		DB.NodeIndex[0] = &models.Node{ID: 0, Pubkey: pip, Status: models.StatusActive}
		DB.Follow[0] = mapset.NewSet[uint32]()
		return DB

	default:
		return nil // default to nil
	}
}

// generates a random mock database of a specified number of nodes and successors per node
// the successor of a node won't include itself, and won't have repetitions
func GenerateDB(nodesNum, successorsPerNode int, rng *rand.Rand) *Database {

	DB := NewDatabase()
	if successorsPerNode > nodesNum {
		return nil
	}

	for i := 0; i < nodesNum; i++ {
		// create random successors for each node
		randomFollows := make([]uint32, 0, successorsPerNode)
		for len(randomFollows) != successorsPerNode {

			follow := uint32(rng.Intn(nodesNum))
			if slices.Contains(randomFollows, follow) {
				continue
			}

			randomFollows = append(randomFollows, follow)
		}

		pubkey := strconv.FormatInt(int64(i), 10)
		nodeID := uint32(i)

		DB.KeyIndex[pubkey] = nodeID
		DB.NodeIndex[nodeID] = &models.Node{
			ID:     nodeID,
			Pubkey: pubkey,
			Status: models.StatusActive,
		}

		DB.Follow[nodeID] = mapset.NewSet[uint32](randomFollows...)
	}
	return DB
}
