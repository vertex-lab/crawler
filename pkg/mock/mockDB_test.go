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

			DB := SetupDB(test.DBType)
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

			DB := SetupDB(test.DBType)
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

			DB := SetupDB(test.DBType)
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

func TestIsDandling(t *testing.T) {

	testCases := []struct {
		name       string
		DBType     string
		nodeID     uint32
		isDandling bool
	}{
		{
			name:       "nil DB",
			DBType:     "nil",
			nodeID:     0,
			isDandling: true,
		},
		{
			name:       "empty DB",
			DBType:     "empty",
			nodeID:     0,
			isDandling: true,
		},
		{
			name:       "node 0 not found",
			DBType:     "one-node1",
			nodeID:     0,
			isDandling: true,
		},
		{
			name:       "dandling node 1",
			DBType:     "simple",
			nodeID:     1,
			isDandling: true,
		},
		{
			name:       "non-dandling node 1",
			DBType:     "one-node1",
			nodeID:     1,
			isDandling: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := SetupDB(test.DBType)
			dandling := DB.IsDandling(test.nodeID)

			if dandling != test.isDandling {
				t.Errorf("IsDandling(): expected %v, got %v", test.isDandling, dandling)
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

			DB := SetupDB(test.DBType)
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
