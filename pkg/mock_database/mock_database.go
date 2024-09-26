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

// CheckEmpty returns an error if the db is nil or has no nodes
func (db *MockDatabase) CheckEmpty() error {

	// handle nil pointer
	if db == nil {
		return graph.ErrNilDatabasePointer
	}

	if len(db.Nodes) == 0 {
		return graph.ErrDatabaseIsEmpty
	}

	return nil
}

// FetchNodeByID retrieves a node by ID from the mock database.
func (db *MockDatabase) FetchNodeByID(id uint32) (*graph.Node, error) {

	err := db.CheckEmpty()
	if err != nil {
		return nil, err
	}

	node, exists := db.Nodes[id]
	if !exists {
		return nil, graph.ErrNodeNotFound
	}
	return node, nil
}

// GetNodeSuccessors returns the successors of a node from the mock database.
func (db *MockDatabase) GetNodeSuccessorIDs(id uint32) ([]uint32, error) {

	err := db.CheckEmpty()
	if err != nil {
		return nil, err
	}

	node, exists := db.Nodes[id]
	if !exists {
		return nil, graph.ErrNodeNotFound
	}
	return node.SuccessorsID, nil
}

func (db *MockDatabase) GetAllNodeIDs() ([]uint32, error) {

	err := db.CheckEmpty()
	if err != nil {
		return nil, err
	}

	nodeIDs := make([]uint32, 0, len(db.Nodes))
	for id := range db.Nodes {
		nodeIDs = append(nodeIDs, id)
	}

	return nodeIDs, nil

}
