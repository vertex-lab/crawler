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

// FetchNodeByID retrieves a node by ID from the mock database.
func (db *MockDatabase) FetchNodeByID(id uint32) (*graph.Node, error) {

	// handle nil pointer
	if db == nil {
		return nil, graph.ErrNilDatabasePointer
	}

	// handle empty database
	is_empty, _ := db.IsEmpty()
	if is_empty {
		return nil, graph.ErrDatabaseIsEmpty
	}

	node, exists := db.Nodes[id]
	if !exists {
		return nil, graph.ErrNodeNotFound
	}
	return node, nil
}

// GetNodeSuccessors returns the successors of a node from the mock database.
func (db *MockDatabase) GetNodeSuccessorIDs(id uint32) ([]uint32, error) {

	// handle nil pointer
	if db == nil {
		return nil, graph.ErrNilDatabasePointer
	}

	// handle empty database
	is_empty, _ := db.IsEmpty()
	if is_empty {
		return nil, graph.ErrDatabaseIsEmpty
	}

	node, exists := db.Nodes[id]
	if !exists {
		return nil, graph.ErrNodeNotFound
	}
	return node.SuccessorsID, nil
}

// IsEmpty returns true if the MockDatabase has no Nodes, else false
func (db *MockDatabase) IsEmpty() (bool, error) {

	// handle nil pointer
	if db == nil {
		return true, graph.ErrNilDatabasePointer
	}

	return len(db.Nodes) == 0, nil
}
