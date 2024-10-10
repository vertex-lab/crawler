package mock

import (
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

// FetchNodeByID retrieves a node by ID from the mock database.
func (DB *MockDatabase) FetchNodeByID(id uint32) (*graph.Node, error) {

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
func (DB *MockDatabase) GetNodeSuccessorIDs(id uint32) ([]uint32, error) {

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

// GetAllNodeIDs returns a slice with the ids of all nodes in the mock database
func (DB *MockDatabase) GetAllNodeIDs() ([]uint32, error) {

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
