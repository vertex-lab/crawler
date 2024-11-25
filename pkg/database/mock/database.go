// The mock database package allows for testing that are decoupled from a
// particular database implementation.
package mock

import (
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

	if len(DB.NodeIndex) == 0 {
		return models.ErrEmptyDB
	}

	return nil
}

// AddNode() adds a node to the database and returns its assigned nodeID.
// In case of errors, it returns MaxUint32 as the nodeID.
func (DB *Database) AddNode(node *models.Node) (uint32, error) {

	if DB == nil {
		return math.MaxUint32, models.ErrNilDBPointer
	}

	if _, exist := DB.KeyIndex[node.Metadata.PubKey]; exist {
		return math.MaxUint32, models.ErrNodeAlreadyInDB
	}

	// add the node to the KeyIndex
	nodeID := uint32(DB.LastNodeID + 1)
	DB.LastNodeID++
	DB.KeyIndex[node.Metadata.PubKey] = nodeID

	// add the node to the NodeIndex
	DB.NodeIndex[nodeID] = node
	return nodeID, nil
}

// UpdateNode() updates the nodeID using the new values inside node.
func (DB *Database) UpdateNode(nodeID uint32, nodeDiff *models.NodeDiff) error {

	if err := DB.Validate(); err != nil {
		return err
	}

	if _, exist := DB.NodeIndex[nodeID]; !exist {
		return models.ErrNodeNotFoundDB
	}

	DB.updateNodeMeta(nodeID, nodeDiff)
	DB.updateNodeSuccessors(nodeID, nodeDiff)
	return nil
}

// updateNodeMeta() updates the node metadata using the nodeDiff
func (DB *Database) updateNodeMeta(nodeID uint32, nodeDiff *models.NodeDiff) {

	// update the fields only if not empty
	if nodeDiff.Metadata.PubKey != "" {
		DB.NodeIndex[nodeID].Metadata.PubKey = nodeDiff.Metadata.PubKey
	}

	if nodeDiff.Metadata.Timestamp > 0 {
		DB.NodeIndex[nodeID].Metadata.Timestamp = nodeDiff.Metadata.Timestamp
	}

	if nodeDiff.Metadata.Status != "" {
		DB.NodeIndex[nodeID].Metadata.Status = nodeDiff.Metadata.Status
	}

	if nodeDiff.Metadata.Pagerank > 0.0 {
		DB.NodeIndex[nodeID].Metadata.Pagerank = nodeDiff.Metadata.Pagerank
	}
}

// updateNodeSuccessors() updates the successors of nodeID by adding nodeDiff.AddedSucc
// and removing nodeDiff.RemovedSucc.
func (DB *Database) updateNodeSuccessors(nodeID uint32, nodeDiff *models.NodeDiff) {

	oldSucc := DB.NodeIndex[nodeID].Successors

	// adding new successors
	for _, addedSucc := range nodeDiff.AddedSucc {
		if !slices.Contains(oldSucc, addedSucc) {
			oldSucc = append(oldSucc, addedSucc)
		}
	}

	// removing successors
	newSucc := make([]uint32, 0, len(oldSucc)-len(nodeDiff.RemovedSucc))
	for _, succ := range oldSucc {

		if !slices.Contains(nodeDiff.RemovedSucc, succ) {
			newSucc = append(newSucc, succ)
		}
	}

	DB.NodeIndex[nodeID].Successors = newSucc
}

// ContainsNode() returns whether nodeID is found in the DB
func (DB *Database) ContainsNode(nodeID uint32) bool {

	if err := DB.Validate(); err != nil {
		return false
	}

	_, exist := DB.NodeIndex[nodeID]
	return exist
}

// NodeMetaWithID() retrieves a node by its pubkey.
func (DB *Database) NodeMetaWithID(pubkey string) (models.NodeMetaWithID, error) {

	if err := DB.Validate(); err != nil {
		return models.NodeMetaWithID{}, err
	}

	nodeID, exists := DB.KeyIndex[pubkey]
	if !exists {
		return models.NodeMetaWithID{}, models.ErrNodeNotFoundDB
	}

	node := models.NodeMetaWithID{
		ID:       nodeID,
		NodeMeta: &DB.NodeIndex[nodeID].Metadata,
	}

	return node, nil
}

// IsDandling returns whether a node has no successors (dandling).
// In case of errors, returns the default true.
func (DB *Database) IsDandling(nodeID uint32) bool {

	Successors, err := DB.Successors(nodeID)
	if err != nil {
		return true
	}

	return len(Successors) == 0
}

// RandomSuccessor() returns a random successor of nodeID. In case of error
// it returns MaxUint32 as the nodeID.
func (DB *Database) RandomSuccessor(nodeID uint32) (uint32, error) {

	if err := DB.Validate(); err != nil {
		return math.MaxUint32, err
	}

	node, exists := DB.NodeIndex[nodeID]
	if !exists {
		return math.MaxUint32, models.ErrNodeNotFoundDB
	}

	// if it is a dandling node
	if len(node.Successors) == 0 {
		return math.MaxUint32, nil
	}

	randomIndex := rand.Intn((len(node.Successors)))
	return node.Successors[randomIndex], nil
}

// Successors() returns the slice of successors of nodeID.
func (DB *Database) Successors(nodeID uint32) ([]uint32, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	node, exists := DB.NodeIndex[nodeID]
	if !exists {
		return nil, models.ErrNodeNotFoundDB
	}
	return node.Successors, nil
}

// NodeIDs() returns a slice of nodeIDs that correspond with the given slice of pubkeys.
// If a pubkey is not found, nil is returned
func (DB *Database) NodeIDs(pubkeys []string) ([]interface{}, error) {

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
func (DB *Database) AllNodes() ([]uint32, error) {

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
func (DB *Database) Size() int {

	if DB == nil {
		return 0
	}
	return len(DB.NodeIndex)
}

// NodeCache() returns a NodeCache struct.
func (DB *Database) NodeCache() (models.NodeCache, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	NC := models.NewNodeCache()
	for pubkey, nodeID := range DB.KeyIndex {

		nodeAttr := models.NodeFilterAttributes{
			ID:        nodeID,
			Timestamp: DB.NodeIndex[nodeID].Metadata.Timestamp,
			Pagerank:  DB.NodeIndex[nodeID].Metadata.Pagerank,
		}
		NC.Store(pubkey, nodeAttr)
	}

	return NC, nil
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
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{2}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{1}}
		return DB

	case "one-node0":
		DB := NewDatabase()
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{0}}
		return DB

	case "one-node1":
		DB := NewDatabase()
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{1}}
		return DB

	case "triangle":
		DB := NewDatabase()
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{1}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{2}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{0}}
		return DB

	case "simple":
		DB := NewDatabase()
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{1}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{Timestamp: 0}, Successors: []uint32{}}
		return DB

	case "simple-with-mock-pks":
		DB := NewDatabase()
		DB.KeyIndex["zero"] = 0
		DB.KeyIndex["one"] = 1
		DB.KeyIndex["two"] = 2
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{PubKey: "zero", Timestamp: 0, Pagerank: 0.26}, Successors: []uint32{1}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{PubKey: "one", Timestamp: 0, Pagerank: 0.48}, Successors: []uint32{}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{PubKey: "two", Timestamp: 0, Pagerank: 0.26}, Successors: []uint32{}}
		DB.LastNodeID = 2
		return DB

	case "simple-with-pks":
		DB := NewDatabase()
		DB.KeyIndex[odell] = 0
		DB.KeyIndex[calle] = 1
		DB.KeyIndex[pip] = 2
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{PubKey: odell, Status: models.StatusNotCrawled, Timestamp: 0, Pagerank: 0.26}, Successors: []uint32{1}, Predecessors: []uint32{}}
		DB.NodeIndex[1] = &models.Node{Metadata: models.NodeMeta{PubKey: calle, Status: models.StatusNotCrawled, Timestamp: 0, Pagerank: 0.48}, Successors: []uint32{}, Predecessors: []uint32{0}}
		DB.NodeIndex[2] = &models.Node{Metadata: models.NodeMeta{PubKey: pip, Status: models.StatusNotCrawled, Timestamp: 0, Pagerank: 0.26}, Successors: []uint32{}, Predecessors: []uint32{}}
		DB.LastNodeID = 2
		return DB

	case "pip":
		DB := NewDatabase()
		DB.KeyIndex[pip] = 0
		DB.NodeIndex[0] = &models.Node{Metadata: models.NodeMeta{PubKey: pip, Status: models.StatusCrawled, Timestamp: 0, Pagerank: 1.0}, Successors: []uint32{}, Predecessors: []uint32{}}
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
		randomSuccessors := make([]uint32, 0, successorsPerNode)
		for len(randomSuccessors) != successorsPerNode {

			succ := uint32(rng.Intn(nodesNum))
			if slices.Contains(randomSuccessors, succ) {
				continue
			}

			randomSuccessors = append(randomSuccessors, succ)
		}

		DB.NodeIndex[uint32(i)] = &models.Node{
			Metadata:   models.NodeMeta{Timestamp: 0},
			Successors: randomSuccessors}
	}
	return DB
}
