package mockdatabase

import (
	"errors"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

var ErrNodeNotFound = errors.New("node not found")

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
	node, exists := db.Nodes[id]
	if !exists {
		return nil, ErrNodeNotFound
	}
	return node, nil
}

// GetNodeSuccessors returns the successors of a node from the mock database.
func (db *MockDatabase) GetNodeSuccessorsID(id uint32) ([]uint32, error) {
	node, exists := db.Nodes[id]
	if !exists {
		return nil, ErrNodeNotFound
	}
	return node.SuccessorsID, nil
}
