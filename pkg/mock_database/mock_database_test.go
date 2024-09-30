package mock

import (
	"errors"
	"reflect"
	"testing"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

func TestMockDatabase(t *testing.T) {

	t.Run("test CheckEmpty() nil db", func(t *testing.T) {

		var nil_db *MockDatabase // nil pointer
		err := nil_db.CheckEmpty()

		if !errors.Is(err, graph.ErrNilDatabasePointer) {
			t.Errorf("CheckEmpty(): expected error %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("test CheckEmpty(), empty db", func(t *testing.T) {

		empty_db := NewMockDatabase() // empty db
		err := empty_db.CheckEmpty()

		if !errors.Is(err, graph.ErrDatabaseIsEmpty) {
			t.Errorf("CheckEmpty(): expected error %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})
	t.Run("negative tests with nil db", func(t *testing.T) {
		var nil_db *MockDatabase // nil pointer

		tests := []struct {
			name        string
			call        func() (interface{}, error)
			expectedNil interface{} // Expected nil value based on the method
		}{
			{"FetchNodeByID with nil db", func() (interface{}, error) {
				return nil_db.FetchNodeByID(0)
			}, (*graph.Node)(nil)},
			{"GetAllNodeIDs with nil db", func() (interface{}, error) {
				return nil_db.GetAllNodeIDs()
			}, ([]uint32)(nil)},
			{"GetNodeSuccessorIDs with nil db", func() (interface{}, error) {
				return nil_db.GetNodeSuccessorIDs(0)
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

	t.Run("negative tests with empty db", func(t *testing.T) {
		empty_db := NewMockDatabase() // create empty mock database

		tests := []struct {
			name        string
			call        func() (interface{}, error)
			expectedNil interface{} // Expected nil value based on the method
		}{
			{"FetchNodeByID with nil db", func() (interface{}, error) {
				return empty_db.FetchNodeByID(0)
			}, (*graph.Node)(nil)},
			{"GetAllNodeIDs with nil db", func() (interface{}, error) {
				return empty_db.GetAllNodeIDs()
			}, ([]uint32)(nil)},
			{"GetNodeSuccessorIDs with nil db", func() (interface{}, error) {
				return empty_db.GetNodeSuccessorIDs(0)
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

		db := NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		db.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}

		node, err := db.FetchNodeByID(0)

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

	t.Run("negative test FetchNodeByID, node not in the db", func(t *testing.T) {

		db := NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{0}}

		node, err := db.FetchNodeByID(1)

		if !errors.Is(err, graph.ErrNodeNotFound) {
			t.Errorf("FetchNodeByID(0): expected error %v, got %v", graph.ErrNodeNotFound, err)
		}

		if node != nil {
			t.Errorf("FetchNodeByID(0): expected nodeA = nil, got %v", node)
		}
	})

	t.Run("positive GetNodeSuccessorIDs", func(t *testing.T) {

		// initialize mock database
		db := NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		db.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		db.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}

		successors, err := db.GetNodeSuccessorIDs(0)

		if err != nil {
			t.Errorf("GetNodeSuccessorIDs(0): expected no error, got %v", err)
		}

		if !reflect.DeepEqual(successors, []uint32{1, 2}) {
			t.Errorf("GetNodeSuccessorIDs(0): expected successors {1, 2}, got %v", successors)
		}
	})

	t.Run("negative test GetNodeSuccessorIDs, node not in db", func(t *testing.T) {

		// initialize mock database
		db := NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{0}}

		successors, err := db.GetNodeSuccessorIDs(1)

		if !errors.Is(err, graph.ErrNodeNotFound) {
			t.Errorf("GetNodeSuccessorIDs(0): expected error %v, got %v", graph.ErrNodeNotFound, err)
		}

		if successors != nil {
			t.Errorf("GetNodeSuccessorIDs(0): expected successorsA = nil, got %v", successors)
		}
	})

	t.Run("positive test GetAllNodeIDs", func(t *testing.T) {

		db := NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{0}}

		node_ids, err := db.GetAllNodeIDs()

		if err != nil {
			t.Errorf("GetAllNodeIDs(): expected err nil, got %v", err)
		}

		if !reflect.DeepEqual(node_ids, []uint32{0}) {
			t.Errorf("GetAllNodeIDs(): expected %v, got %v", []uint32{0}, node_ids)
		}
	})
}
