package mock

import (
	"errors"
	"reflect"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
)

// grouping properties of a test together
type testCases struct {
	name          string
	DBType        string
	expectedError error
	expectedNode  *graph.Node
	expectedSlice []uint32
}

// function that returns a DB setup based on the DBType
func setupDB(DBType string) *MockDatabase {

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

	default:
		return nil // Default to nil for unrecognized scenarios
	}
}

func TestCheckEmpty(t *testing.T) {

	testCases := []testCases{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: graph.ErrNilDatabasePointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: graph.ErrDatabaseIsEmpty,
		},
		{
			name:          "DB with node 0",
			DBType:        "one-node0",
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := setupDB(test.DBType)
			err := DB.CheckEmpty()

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("CheckEmpty(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestNodeByID(t *testing.T) {

	testCases := []testCases{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: graph.ErrNilDatabasePointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: graph.ErrDatabaseIsEmpty,
		},
		{
			name:          "DB with node 0",
			DBType:        "one-node0",
			expectedError: graph.ErrNodeNotFoundDB,
		},
		{
			name:          "DB with node 1",
			DBType:        "one-node1",
			expectedError: nil,
			expectedNode:  &graph.Node{ID: 1, SuccessorIDs: []uint32{1}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := setupDB(test.DBType)
			node, err := DB.NodeByID(1)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeByID(1): expected %v, got %v", test.expectedError, err)
			}

			// if provided, check that the expected result is equal to the result
			if test.expectedNode != nil {

				if node == nil {
					t.Errorf("NodeByID(1): expected node ID %d, got nil", test.expectedNode.ID)
				}

				if node.ID != test.expectedNode.ID {
					t.Errorf("NodeByID(1): expected node ID %d, got %d", test.expectedNode.ID, node.ID)
				}

				if !reflect.DeepEqual(node.SuccessorIDs, test.expectedNode.SuccessorIDs) {
					t.Errorf("NodeByID(1): expected successors %v, got %v", test.expectedNode.SuccessorIDs, node.SuccessorIDs)
				}
			}
		})
	}
}

func TestNodeSuccessorIDs(t *testing.T) {

	testCases := []testCases{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: graph.ErrNilDatabasePointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: graph.ErrDatabaseIsEmpty,
		},
		{
			name:          "DB with node 0",
			DBType:        "one-node0",
			expectedError: graph.ErrNodeNotFoundDB,
		},
		{
			name:          "DB with node 1",
			DBType:        "one-node1",
			expectedError: nil,
			expectedSlice: []uint32{1},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := setupDB(test.DBType)
			successorIDs, err := DB.NodeSuccessorIDs(1)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeSuccessorIDs(1): expected %v, got %v", test.expectedError, err)
			}

			// if provided, check that the expected result is equal to the result
			if test.expectedSlice != nil {

				if successorIDs == nil {
					t.Errorf("NodeSuccessorIDs(1): expected %v, got nil", test.expectedSlice)
				}

				if !reflect.DeepEqual(successorIDs, test.expectedSlice) {
					t.Errorf("NodeSuccessorIDs(1): expected %v, got %v", test.expectedSlice, successorIDs)
				}
			}
		})
	}
}

func TestAllNodeIDs(t *testing.T) {

	testCases := []testCases{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: graph.ErrNilDatabasePointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: graph.ErrDatabaseIsEmpty,
		},
		{
			name:          "DB with node 0",
			DBType:        "one-node0",
			expectedError: nil,
			expectedSlice: []uint32{0},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := setupDB(test.DBType)
			nodeIDs, err := DB.AllNodeIDs()

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("AllNodeIDs(): expected %v, got %v", test.expectedError, err)
			}

			// if provided, check that the expected result is equal to the result
			if test.expectedSlice != nil {

				if nodeIDs == nil {
					t.Errorf("AllNodeIDs(): expected %v, got nil", test.expectedSlice)
				}

				if !reflect.DeepEqual(nodeIDs, test.expectedSlice) {
					t.Errorf("AllNodeIDs(): expected %v, got %v", test.expectedSlice, nodeIDs)
				}
			}
		})
	}
}
