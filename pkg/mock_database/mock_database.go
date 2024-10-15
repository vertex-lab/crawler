package mock

import (
	"math/rand"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
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

// Helper function that generates a random database of a specified number of nodes
// and successors per node
func GenerateMockDB(nodesNum uint32, successorsPerNode uint32) *MockDatabase {

	DB := NewMockDatabase()
	for i := uint32(0); i < nodesNum; i++ {

		// create random successors
		random_successors := make([]uint32, successorsPerNode)
		for j := uint32(0); j < successorsPerNode; j++ {
			random_successors[j] = uint32(rand.Intn(int(nodesNum)))
		}

		DB.Nodes[i] = &graph.Node{ID: i, SuccessorIDs: random_successors}
	}

	return DB
}
