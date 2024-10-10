package mock

import (
	"errors"
	"reflect"
	"testing"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

func TestMockDatabase(t *testing.T) {

	t.Run("test CheckEmpty() nil DB", func(t *testing.T) {

		var nil_DB *MockDatabase // nil pointer
		err := nil_DB.CheckEmpty()

		if !errors.Is(err, graph.ErrNilDatabasePointer) {
			t.Errorf("CheckEmpty(): expected error %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("test CheckEmpty(), empty DB", func(t *testing.T) {

		empty_DB := NewMockDatabase() // empty DB
		err := empty_DB.CheckEmpty()

		if !errors.Is(err, graph.ErrDatabaseIsEmpty) {
			t.Errorf("CheckEmpty(): expected error %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})
	t.Run("negative tests with nil DB", func(t *testing.T) {
		var nil_DB *MockDatabase // nil pointer

		tests := []struct {
			name        string
			call        func() (interface{}, error)
			expectedNil interface{} // Expected nil value based on the method
		}{
			{"FetchNodeByID with nil DB", func() (interface{}, error) {
				return nil_DB.FetchNodeByID(0)
			}, (*graph.Node)(nil)},
			{"GetAllNodeIDs with nil DB", func() (interface{}, error) {
				return nil_DB.GetAllNodeIDs()
			}, ([]uint32)(nil)},
			{"GetNodeSuccessorIDs with nil DB", func() (interface{}, error) {
				return nil_DB.GetNodeSuccessorIDs(0)
			}, ([]uint32)(nil)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := tt.call()

				// Check for the expected error
				if !errors.Is(err, graph.ErrNilDatabasePointer) {
					t.Errorf("expected error %v, got %v", graph.ErrNilDatabasePointer, err)
				}

				// Check that the result is of the expected type and is nil
				if !reflect.DeepEqual(result, tt.expectedNil) {
					t.Errorf("expected %v, got %v", tt.expectedNil, result)
				}
			})
		}
	})

	t.Run("negative tests with empty DB", func(t *testing.T) {
		empty_DB := NewMockDatabase() // create empty mock database

		tests := []struct {
			name        string
			call        func() (interface{}, error)
			expectedNil interface{} // Expected nil value based on the method
		}{
			{"FetchNodeByID with nil DB", func() (interface{}, error) {
				return empty_DB.FetchNodeByID(0)
			}, (*graph.Node)(nil)},
			{"GetAllNodeIDs with nil DB", func() (interface{}, error) {
				return empty_DB.GetAllNodeIDs()
			}, ([]uint32)(nil)},
			{"GetNodeSuccessorIDs with nil DB", func() (interface{}, error) {
				return empty_DB.GetNodeSuccessorIDs(0)
			}, ([]uint32)(nil)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := tt.call()

				// Check for the expected error
				if !errors.Is(err, graph.ErrDatabaseIsEmpty) {
					t.Errorf("expected error %v, got %v", graph.ErrDatabaseIsEmpty, err)
				}

				// Check that the result is of the expected type and is nil
				if !reflect.DeepEqual(result, tt.expectedNil) {
					t.Errorf("expected %v, got %v", tt.expectedNil, result)
				}
			})
		}
	})

	t.Run("positive test FetchNodeByID", func(t *testing.T) {

		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}

		node, err := DB.FetchNodeByID(0)

		if err != nil {
			t.Errorf("FetchNodeByID(0): expected no error, got %v", err)
		}

		if node == nil {
			t.Fatalf("FetchNodeByID(0): expected node ID 0, got node = 'nil'")
		}

		if node.ID != 0 {
			t.Errorf("FetchNodeByID(0): expected node ID 0, got %d", node.ID)
		}

		if !reflect.DeepEqual(node.SuccessorIDs, []uint32{1}) {
			t.Errorf("FetchNodeByID(0): expected successors {1}, got %d", node.SuccessorIDs)
		}
	})

	t.Run("negative test FetchNodeByID, node not in the DB", func(t *testing.T) {

		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{0}}

		node, err := DB.FetchNodeByID(1)

		if !errors.Is(err, graph.ErrNodeNotFoundDB) {
			t.Errorf("FetchNodeByID(0): expected error %v, got %v", graph.ErrNodeNotFoundDB, err)
		}

		if node != nil {
			t.Errorf("FetchNodeByID(0): expected nodeA = nil, got %v", node)
		}
	})

	t.Run("positive GetNodeSuccessorIDs", func(t *testing.T) {

		// initialize mock database
		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}

		successors, err := DB.GetNodeSuccessorIDs(0)

		if err != nil {
			t.Errorf("GetNodeSuccessorIDs(0): expected no error, got %v", err)
		}

		if !reflect.DeepEqual(successors, []uint32{1, 2}) {
			t.Errorf("GetNodeSuccessorIDs(0): expected successors {1, 2}, got %v", successors)
		}
	})

	t.Run("negative test GetNodeSuccessorIDs, node not in DB", func(t *testing.T) {

		// initialize mock database
		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{0}}

		successors, err := DB.GetNodeSuccessorIDs(1)

		if !errors.Is(err, graph.ErrNodeNotFoundDB) {
			t.Errorf("GetNodeSuccessorIDs(0): expected error %v, got %v", graph.ErrNodeNotFoundDB, err)
		}

		if successors != nil {
			t.Errorf("GetNodeSuccessorIDs(0): expected successorsA = nil, got %v", successors)
		}
	})

	t.Run("positive test GetAllNodeIDs", func(t *testing.T) {

		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{0}}

		node_ids, err := DB.GetAllNodeIDs()

		if err != nil {
			t.Errorf("GetAllNodeIDs(): expected err nil, got %v", err)
		}

		if !reflect.DeepEqual(node_ids, []uint32{0}) {
			t.Errorf("GetAllNodeIDs(): expected %v, got %v", []uint32{0}, node_ids)
		}
	})
}
