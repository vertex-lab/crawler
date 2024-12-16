// The mock database package allows for testing that are decoupled from a
// particular database implementation.
package mock

import (
	"context"
	"math"
	"math/rand"
	"slices"

	"github.com/vertex-lab/crawler/pkg/models"
)

// simulates a simple graph database for testing.
type Database struct {

	// a map that associates each public key with a unique nodeID
	KeyIndex map[string]uint32

	// a map that associates each nodeID with its node data
	NodeIndex map[uint32]*models.Node

	// the next nodeID to be used. When a new node is added, this fiels is incremented by one
	LastNodeID int
}

// NewDatabase() creates and returns a new Database instance.
func NewDatabase() *Database {
	return &Database{
		KeyIndex:   make(map[string]uint32),
		NodeIndex:  make(map[uint32]*models.Node),
		LastNodeID: -1, // the first nodeID will be 0
	}
}

// Validate() returns an error if the DB is nil or has no nodes
func (DB *Database) Validate() error {
	if DB == nil {
		return models.ErrNilDBPointer
	}

	return nil
}

// AddNode() adds a node to the database and returns its assigned nodeID.
// In case of errors, it returns MaxUint32 as the nodeID.
func (DB *Database) AddNode(ctx context.Context, node *models.Node) (uint32, error) {
	_ = ctx
	if DB == nil {
		return math.MaxUint32, models.ErrNilDBPointer
	}

	if _, exist := DB.KeyIndex[node.Metadata.Pubkey]; exist {
		return math.MaxUint32, models.ErrNodeAlreadyInDB
	}

	// add the node to the KeyIndex
	nodeID := uint32(DB.LastNodeID + 1)
	DB.LastNodeID++
	DB.KeyIndex[node.Metadata.Pubkey] = nodeID

	// add the node to the NodeIndex
	node.Metadata.ID = nodeID
	DB.NodeIndex[nodeID] = node
	return nodeID, nil
}

// UpdateNode() updates the nodeID using the new values inside node.
func (DB *Database) UpdateNode(ctx context.Context, nodeID uint32, nodeDiff *models.NodeDiff) error {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return err
	}

	if _, exist := DB.NodeIndex[nodeID]; !exist {
		return models.ErrNodeNotFoundDB
	}

	DB.updateNodeMeta(ctx, nodeID, nodeDiff)
	DB.updateNodeFollows(ctx, nodeID, nodeDiff)
	return nil
}

// updateNodeMeta() updates the node metadata using the nodeDiff
func (DB *Database) updateNodeMeta(ctx context.Context, nodeID uint32, nodeDiff *models.NodeDiff) {
	_ = ctx

	// update the fields only if not empty
	if nodeDiff.Metadata.Pubkey != "" {
		DB.NodeIndex[nodeID].Metadata.Pubkey = nodeDiff.Metadata.Pubkey
	}

	if nodeDiff.Metadata.EventTS > 0 {
		DB.NodeIndex[nodeID].Metadata.EventTS = nodeDiff.Metadata.EventTS
	}

	if nodeDiff.Metadata.Status != "" {
		DB.NodeIndex[nodeID].Metadata.Status = nodeDiff.Metadata.Status
	}

	if nodeDiff.Metadata.Pagerank > 0.0 {
		DB.NodeIndex[nodeID].Metadata.Pagerank = nodeDiff.Metadata.Pagerank
	}
}

// updateNodeFollows() updates the successors of nodeID by adding nodeDiff.AddedFollows
// and removing nodeDiff.RemovedFollows.
func (DB *Database) updateNodeFollows(ctx context.Context, nodeID uint32, nodeDiff *models.NodeDiff) {
	_ = ctx
	oldFollows := DB.NodeIndex[nodeID].Follows

	// adding new successors
	for _, addedFollows := range nodeDiff.AddedFollows {
		if !slices.Contains(oldFollows, addedFollows) {
			oldFollows = append(oldFollows, addedFollows)
		}
	}

	// removing successors
	newFollows := make([]uint32, 0, len(oldFollows)-len(nodeDiff.RemovedFollows))
	for _, succ := range oldFollows {

		if !slices.Contains(nodeDiff.RemovedFollows, succ) {
			newFollows = append(newFollows, succ)
		}
	}

	DB.NodeIndex[nodeID].Follows = newFollows
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

// NodeByKey() retrieves a node (NodeMeta) by its pubkey.
func (DB *Database) NodeByKey(ctx context.Context, pubkey string) (*models.NodeMeta, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return &models.NodeMeta{}, err
	}

	nodeID, exists := DB.KeyIndex[pubkey]
	if !exists {
		return &models.NodeMeta{}, models.ErrNodeNotFoundDB
	}

	return &DB.NodeIndex[nodeID].Metadata, nil
}

// NodeByID() retrieves a node (NodeMeta) by its nodeID.
func (DB *Database) NodeByID(ctx context.Context, nodeID uint32) (*models.NodeMeta, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return &models.NodeMeta{}, err
	}

	node, exist := DB.NodeIndex[nodeID]
	if !exist {
		return &models.NodeMeta{}, models.ErrNodeNotFoundDB
	}

	return &node.Metadata, nil
}

// Follows() returns the slice of successors of all nodeIDs
func (DB *Database) Follows(ctx context.Context, nodeIDs ...uint32) ([][]uint32, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	followSlice := make([][]uint32, 0, len(nodeIDs))
	for _, ID := range nodeIDs {
		node, exists := DB.NodeIndex[ID]
		if !exists {
			return nil, models.ErrNodeNotFoundDB
		}

		followSlice = append(followSlice, node.Follows)
	}

	return followSlice, nil
}

// Pubkeys() returns a slice of pubkeys that correspond with the given slice of nodeIDs.
// If a pubkey is not found, nil is returned.
func (DB *Database) Pubkeys(ctx context.Context, nodeIDs []uint32) ([]interface{}, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	pubkeys := make([]interface{}, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {

		node, exist := DB.NodeIndex[nodeID]
		if !exist {
			pubkeys = append(pubkeys, nil)
			continue
		}

		pubkeys = append(pubkeys, node.Metadata.Pubkey)
	}

	return pubkeys, nil
}

// NodeIDs() returns a slice of nodeIDs that correspond with the given slice of pubkeys.
// If a pubkey is not found, nil is returned
func (DB *Database) NodeIDs(ctx context.Context, pubkeys []string) ([]interface{}, error) {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	nodeIDs := make([]interface{}, 0, len(pubkeys))
	for _, pubkey := range pubkeys {

		nodeID, exist := DB.KeyIndex[pubkey]
		if !exist {
			nodeIDs = append(nodeIDs, nil)
			continue
		}

		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, nil
}

// All returns a slice with the IDs of all nodes in the mock GraphDB
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

// SetPagerank() set the pagerank in the database according to the pagerankMap
func (DB *Database) SetPagerank(ctx context.Context, p models.PagerankMap) error {
	_ = ctx
	if err := DB.Validate(); err != nil {
		return err
	}

	for nodeID, rank := range p {
		// if nodeID doesn't exists, skip
		if _, exists := DB.NodeIndex[nodeID]; !exists {
			return models.ErrNodeNotFoundDB
		}

		DB.NodeIndex[nodeID].Metadata.Pagerank = rank
	}

	return nil
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

// function that returns a DB setup based on the DBType
func SetupDB(DBType string) *Database {
	odell := "04c915daefee38317fa734444acee390a8269fe5810b2241e5e6dd343dfbecc9"
	calle := "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"
	pip := "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"

	switch DBType {

	case "nil":
		return nil

	case "empty":
		return NewDatabase()

	case "dandling":
		DB := NewDatabase()
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{2}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{1}}
		DB.LastNodeID = 2
		return DB

	case "one-node0":
		DB := NewDatabase()
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{0}}
		DB.LastNodeID = 0
		return DB

	case "one-node1":
		DB := NewDatabase()
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{1}}
		DB.LastNodeID = 1
		return DB

	case "triangle":
		DB := NewDatabase()
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{1}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{2}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{0}}
		DB.LastNodeID = 2
		return DB

	case "simple":
		DB := NewDatabase()
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{1}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{EventTS: 0}, Follows: []uint32{}}
		DB.LastNodeID = 2
		return DB

	case "simple-with-mock-pks":
		DB := NewDatabase()
		DB.KeyIndex["zero"] = 0
		DB.KeyIndex["one"] = 1
		DB.KeyIndex["two"] = 2
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{ID: 0, Pubkey: "zero", EventTS: 0, Pagerank: 0.26}, Follows: []uint32{1}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{ID: 1, Pubkey: "one", EventTS: 0, Pagerank: 0.48}, Follows: []uint32{}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{ID: 2, Pubkey: "two", EventTS: 0, Pagerank: 0.26}, Follows: []uint32{}}
		DB.LastNodeID = 2
		return DB

	case "simple-with-pks":
		DB := NewDatabase()
		DB.KeyIndex[odell] = 0
		DB.KeyIndex[calle] = 1
		DB.KeyIndex[pip] = 2
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{ID: 0, Pubkey: odell, Status: models.StatusActive, EventTS: 0, Pagerank: 0.26}, Follows: []uint32{1}, Followers: []uint32{}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{ID: 1, Pubkey: calle, Status: models.StatusActive, EventTS: 0, Pagerank: 0.48}, Follows: []uint32{}, Followers: []uint32{0}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{ID: 2, Pubkey: pip, Status: models.StatusActive, EventTS: 0, Pagerank: 0.26}, Follows: []uint32{}, Followers: []uint32{}}
		DB.LastNodeID = 2
		return DB

	case "pip":
		DB := NewDatabase()
		DB.KeyIndex[pip] = 0
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{Pubkey: pip, Status: models.StatusActive, EventTS: 0, Pagerank: 1.0}, Follows: []uint32{}, Followers: []uint32{}}
		DB.LastNodeID = 0
		return DB

	case "promotion-demotion":
		DB := NewDatabase()
		DB.KeyIndex[odell] = 0
		DB.KeyIndex[calle] = 1 // the only active
		DB.KeyIndex[pip] = 2
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{ID: 0, Pubkey: odell, Status: models.StatusInactive, EventTS: 0, Pagerank: 0.26}, Follows: []uint32{1}, Followers: []uint32{}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{ID: 1, Pubkey: calle, Status: models.StatusActive, EventTS: 0, Pagerank: 0.48}, Follows: []uint32{}, Followers: []uint32{0}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{ID: 2, Pubkey: pip, Status: models.StatusInactive, EventTS: 0, Pagerank: 0.26}, Follows: []uint32{}, Followers: []uint32{}}
		DB.LastNodeID = 2
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

			succ := uint32(rng.Intn(nodesNum))
			if slices.Contains(randomFollows, succ) {
				continue
			}

			randomFollows = append(randomFollows, succ)
		}

		DB.NodeIndex[uint32(i)] = &models.Node{
			Metadata: models.NodeMeta{EventTS: 0},
			Follows:  randomFollows}
	}
	return DB
}
