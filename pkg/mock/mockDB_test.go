package mock

import (
	"errors"
	"reflect"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
)

func TestCheckEmpty(t *testing.T) {

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
}

func TestNilOrEmptyDB(t *testing.T) {

	t.Run("negative tests with nil DB", func(t *testing.T) {
		var nil_DB *MockDatabase // nil pointer

		tests := []struct {
			name        string
			call        func() (interface{}, error)
			expectedNil interface{} // Expected nil value based on the method
		}{
			{"NodeByID with nil DB", func() (interface{}, error) {
				return nil_DB.NodeByID(0)
			}, (*graph.Node)(nil)},
			{"AllNodeIDs with nil DB", func() (interface{}, error) {
				return nil_DB.AllNodeIDs()
			}, ([]uint32)(nil)},
			{"NodeSuccessorIDs with nil DB", func() (interface{}, error) {
				return nil_DB.NodeSuccessorIDs(0)
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
			{"NodeByID with nil DB", func() (interface{}, error) {
				return empty_DB.NodeByID(0)
			}, (*graph.Node)(nil)},
			{"AllNodeIDs with nil DB", func() (interface{}, error) {
				return empty_DB.AllNodeIDs()
			}, ([]uint32)(nil)},
			{"NodeSuccessorIDs with nil DB", func() (interface{}, error) {
				return empty_DB.NodeSuccessorIDs(0)
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
}

func TestNodeByID(t *testing.T) {

	t.Run("negative test NodeByID, node not in the DB", func(t *testing.T) {

		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{0}}

		node, err := DB.NodeByID(1)

		if !errors.Is(err, graph.ErrNodeNotFoundDB) {
			t.Errorf("NodeByID(0): expected error %v, got %v", graph.ErrNodeNotFoundDB, err)
		}

		if node != nil {
			t.Errorf("NodeByID(0): expected nodeA = nil, got %v", node)
		}
	})

	t.Run("positive test NodeByID", func(t *testing.T) {

		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}

		node, err := DB.NodeByID(0)

		if err != nil {
			t.Errorf("NodeByID(0): expected no error, got %v", err)
		}

		if node == nil {
			t.Fatalf("NodeByID(0): expected node ID 0, got node = 'nil'")
		}

		if node.ID != 0 {
			t.Errorf("NodeByID(0): expected node ID 0, got %d", node.ID)
		}

		if !reflect.DeepEqual(node.SuccessorIDs, []uint32{1}) {
			t.Errorf("NodeByID(0): expected successors {1}, got %d", node.SuccessorIDs)
		}
	})
}

func TestNodeSuccessorIDs(t *testing.T) {

	t.Run("negative test NodeSuccessorIDs, node not in DB", func(t *testing.T) {

		// initialize mock database
		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{0}}

		successors, err := DB.NodeSuccessorIDs(1)

		if !errors.Is(err, graph.ErrNodeNotFoundDB) {
			t.Errorf("NodeSuccessorIDs(0): expected error %v, got %v", graph.ErrNodeNotFoundDB, err)
		}

		if successors != nil {
			t.Errorf("NodeSuccessorIDs(0): expected successorsA = nil, got %v", successors)
		}
	})

	t.Run("positive NodeSuccessorIDs", func(t *testing.T) {

		// initialize mock database
		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}

		successors, err := DB.NodeSuccessorIDs(0)

		if err != nil {
			t.Errorf("NodeSuccessorIDs(0): expected no error, got %v", err)
		}

		if !reflect.DeepEqual(successors, []uint32{1, 2}) {
			t.Errorf("NodeSuccessorIDs(0): expected successors {1, 2}, got %v", successors)
		}
	})
}

func TestAllNodeIDs(t *testing.T) {

	t.Run("positive test AllNodeIDs", func(t *testing.T) {

		DB := NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{0}}

		node_ids, err := DB.AllNodeIDs()

		if err != nil {
			t.Errorf("AllNodeIDs(): expected err nil, got %v", err)
		}

		if !reflect.DeepEqual(node_ids, []uint32{0}) {
			t.Errorf("AllNodeIDs(): expected %v, got %v", []uint32{0}, node_ids)
		}
	})
}
