package mock

import (
	"math/rand"
	"slices"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
)

// simulates a simple GraphDB for testing.
type MockGraphDB struct {
	Nodes map[uint32]*graph.Node
}

// creates and returns a new MockGraphDB instance.
func NewMockGraphDB() *MockGraphDB {
	return &MockGraphDB{
		Nodes: make(map[uint32]*graph.Node), // Initialize an empty map to store nodes.
	}
}

// returns an error if the DB is nil or has no nodes
func (DB *MockGraphDB) CheckEmpty() error {

	// handle nil pointer
	if DB == nil {
		return graph.ErrNilGraphDBPointer
	}

	if len(DB.Nodes) == 0 {
		return graph.ErrGraphDBIsEmpty
	}

	return nil
}

// retrieves a node by ID from the mock GraphDB.
func (DB *MockGraphDB) Node(nodeID uint32) (*graph.Node, error) {

	err := DB.CheckEmpty()
	if err != nil {
		return nil, err
	}

	node, exists := DB.Nodes[nodeID]
	if !exists {
		return nil, graph.ErrNodeNotFoundDB
	}
	return node, nil
}

// returns the successors of a node from the mock GraphDB.
func (DB *MockGraphDB) Successors(nodeID uint32) ([]uint32, error) {

	err := DB.CheckEmpty()
	if err != nil {
		return nil, err
	}

	node, exists := DB.Nodes[nodeID]
	if !exists {
		return nil, graph.ErrNodeNotFoundDB
	}
	return node.Successors, nil
}

// returns whether a node has no successors (dandling).
// In case of errors, returns the default true.
func (DB *MockGraphDB) IsDandling(nodeID uint32) bool {

	Successors, err := DB.Successors(nodeID)
	if err != nil {
		return true
	}

	return len(Successors) == 0
}

// returns a slice with the ids of all nodes in the mock GraphDB
func (DB *MockGraphDB) AllIDs() ([]uint32, error) {

	err := DB.CheckEmpty()
	if err != nil {
		return nil, err
	}

	nodeIDs := make([]uint32, 0, len(DB.Nodes))
	for nodeID := range DB.Nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, nil
}

// ------------------------------------HELPERS----------------------------------

// generates a random GraphDB of a specified number of nodes and successors per node
// the successor of a node won't include itself, and won't have repetitions
func GenerateMockDB(nodesNum, successorsPerNode int, rng *rand.Rand) *MockGraphDB {

	DB := NewMockGraphDB()
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

		DB.Nodes[uint32(i)] = &graph.Node{ID: uint32(i), Successors: randomSuccessors}
	}
	return DB
}

// function that returns a DB setup based on the DBType
func SetupDB(DBType string) *MockGraphDB {
	switch DBType {

	case "nil":
		return nil

	case "empty":
		return NewMockGraphDB()

	case "dandling":
		DB := NewMockGraphDB()
		DB.Nodes[0] = &graph.Node{ID: 0, Successors: []uint32{}}
		DB.Nodes[1] = &graph.Node{ID: 1, Successors: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, Successors: []uint32{1}}
		return DB

	case "one-node0":
		DB := NewMockGraphDB()
		DB.Nodes[0] = &graph.Node{ID: 0, Successors: []uint32{0}}
		return DB

	case "one-node1":
		DB := NewMockGraphDB()
		DB.Nodes[1] = &graph.Node{ID: 1, Successors: []uint32{1}}
		return DB

	case "triangle":
		DB := NewMockGraphDB()
		DB.Nodes[0] = &graph.Node{ID: 0, Successors: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, Successors: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, Successors: []uint32{0}}
		return DB

	case "simple":
		DB := NewMockGraphDB()
		DB.Nodes[0] = &graph.Node{ID: 0, Successors: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, Successors: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, Successors: []uint32{}}
		return DB

	default:
		return nil // Default to nil for unrecognized scenarios
	}
}
