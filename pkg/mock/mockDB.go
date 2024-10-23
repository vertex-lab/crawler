package mock

import (
	"math/rand"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
)

// MockDatabase simulates a simple in-memory database for testing.
type MockDatabase struct {
	Nodes map[uint32]*graph.Node
}

// NewMockDatabase creates and returns a new MockDatabase instance.
func NewMockDatabase() *MockDatabase {
	return &MockDatabase{
		Nodes: make(map[uint32]*graph.Node), // Initialize an empty map to store nodes.
	}
}

// CheckEmpty returns an error if the DB is nil or has no nodes
func (DB *MockDatabase) CheckEmpty() error {

	// handle nil pointer
	if DB == nil {
		return graph.ErrNilDatabasePointer
	}

	if len(DB.Nodes) == 0 {
		return graph.ErrDatabaseIsEmpty
	}

	return nil
}

// NodeByID retrieves a node by ID from the mock database.
func (DB *MockDatabase) NodeByID(id uint32) (*graph.Node, error) {

	err := DB.CheckEmpty()
	if err != nil {
		return nil, err
	}

	node, exists := DB.Nodes[id]
	if !exists {
		return nil, graph.ErrNodeNotFoundDB
	}
	return node, nil
}

// GetNodeSuccessors returns the successors of a node from the mock database.
func (DB *MockDatabase) NodeSuccessorIDs(id uint32) ([]uint32, error) {

	err := DB.CheckEmpty()
	if err != nil {
		return nil, err
	}

	node, exists := DB.Nodes[id]
	if !exists {
		return nil, graph.ErrNodeNotFoundDB
	}
	return node.SuccessorIDs, nil
}

// AllNodeIDs returns a slice with the ids of all nodes in the mock database
func (DB *MockDatabase) AllNodeIDs() ([]uint32, error) {

	err := DB.CheckEmpty()
	if err != nil {
		return nil, err
	}

	nodeIDs := make([]uint32, 0, len(DB.Nodes))
	for id := range DB.Nodes {
		nodeIDs = append(nodeIDs, id)
	}

	return nodeIDs, nil

}

// ------------------------------------HELPERS----------------------------------

// Helper function that generates a random database of a specified number of nodes
// and successors per node
func GenerateMockDB(nodesNum, successorsPerNode int, rng *rand.Rand) *MockDatabase {

	DB := NewMockDatabase()
	for i := 0; i < nodesNum; i++ {

		// create random successors
		randomSuccessors := make([]uint32, successorsPerNode)
		for j := 0; j < successorsPerNode; j++ {
			randomSuccessors[j] = uint32(rng.Intn(int(nodesNum)))
		}

		DB.Nodes[uint32(i)] = &graph.Node{ID: uint32(i), SuccessorIDs: randomSuccessors}
	}

	return DB
}

// function that returns a DB setup based on the DBType
func SetupDB(DBType string) *MockDatabase {

	switch DBType {

	case "nil":
		return nil

	case "empty":
		return NewMockDatabase()

	case "one-node0":
		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{0}}
		return DB

	case "one-node1":
		DB := NewMockDatabase()
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{1}}
		return DB

	case "triangle":
		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}
		return DB

	case "simple":
		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}
		return DB

	default:
		return nil // Default to nil for unrecognized scenarios
	}
}
