package mock

import (
	"math"
	"math/rand"
	"slices"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

// simulates a simple graph databas for testing.
type Database struct {
	Nodes map[uint32]*models.Node
}

// NewDatabase() creates and returns a new Database instance.
func NewDatabase() *Database {
	return &Database{
		Nodes: make(map[uint32]*models.Node), // Initialize an empty map to store nodes.
	}
}

// Validate() returns an error if the DB is nil or has no nodes
func (DB *Database) Validate() error {

	// handle nil pointer
	if DB == nil {
		return models.ErrNilDBPointer
	}

	if len(DB.Nodes) == 0 {
		return models.ErrEmptyDB
	}

	return nil
}

// ContainsNode() returns whether nodeID is found in the DB
func (DB *Database) ContainsNode(nodeID uint32) bool {

	if err := DB.Validate(); err != nil {
		return false
	}

	_, exist := DB.Nodes[nodeID]
	return exist
}

// Node() retrieves a node by ID.
func (DB *Database) Node(nodeID uint32) (*models.Node, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	node, exists := DB.Nodes[nodeID]
	if !exists {
		return nil, models.ErrNodeNotFoundDB
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
// it returns maxUint32.
func (DB *Database) RandomSuccessor(nodeID uint32) (uint32, error) {

	if err := DB.Validate(); err != nil {
		return math.MaxUint32, err
	}

	node, exists := DB.Nodes[nodeID]
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

	node, exists := DB.Nodes[nodeID]
	if !exists {
		return nil, models.ErrNodeNotFoundDB
	}
	return node.Successors, nil
}

// All returns a slice with the IDs of all nodes in the mock GraphDB
func (DB *Database) AllNodes() ([]uint32, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	nodeIDs := make([]uint32, 0, len(DB.Nodes))
	for nodeID := range DB.Nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, nil
}

// NodeCount() returns the number of nodes in the DB (ignores errors).
func (DB *Database) NodeCount() int {

	if DB == nil {
		return 0
	}
	return len(DB.Nodes)
}

// ------------------------------------HELPERS----------------------------------

// function that returns a DB setup based on the DBType
func SetupDB(DBType string) *Database {
	switch DBType {

	case "nil":
		return nil

	case "empty":
		return NewDatabase()

	case "dandling":
		DB := NewDatabase()
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{}}
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{2}}
		DB.Nodes[2] = &models.Node{ID: 2, Successors: []uint32{1}}
		return DB

	case "one-node0":
		DB := NewDatabase()
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{0}}
		return DB

	case "one-node1":
		DB := NewDatabase()
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{1}}
		return DB

	case "triangle":
		DB := NewDatabase()
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{1}}
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{2}}
		DB.Nodes[2] = &models.Node{ID: 2, Successors: []uint32{0}}
		return DB

	case "simple":
		DB := NewDatabase()
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{1}}
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{}}
		DB.Nodes[2] = &models.Node{ID: 2, Successors: []uint32{}}
		return DB

	default:
		return nil // Default to nil for unrecognized scenarios
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

		DB.Nodes[uint32(i)] = &models.Node{ID: uint32(i), Successors: randomSuccessors}
	}
	return DB
}
