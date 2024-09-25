package mock

import (
	"errors"
	"reflect"
	"testing"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

func TestMockDatabase(t *testing.T) {

	t.Run("positive test FetchNodeByID", func(t *testing.T) {

		db := NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{1}}
		db.Nodes[1] = &graph.Node{ID: 1, SuccessorsID: []uint32{}}

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

		if !reflect.DeepEqual(node.SuccessorsID, []uint32{1}) {
			t.Errorf("FetchNodeByID(0): expected successors {1}, got %d", node.SuccessorsID)
		}
	})

	t.Run("negative test FetchNodeByID, nil db", func(t *testing.T) {

		var nil_db *MockDatabase // nil pointer

		node, err := nil_db.FetchNodeByID(0)

		if !errors.Is(err, graph.ErrNilDatabasePointer) {
			t.Errorf("FetchNodeByID(0): expected error %v, got %v", graph.ErrNilDatabasePointer, err)
		}

		if node != nil {
			t.Errorf("FetchNodeByID(0): expected nil, got %v", node)
		}
	})

	t.Run("negative test FetchNodeByID, empty db", func(t *testing.T) {

		db := NewMockDatabase()

		node, err := db.FetchNodeByID(0)

		if !errors.Is(err, graph.ErrDatabaseIsEmpty) {
			t.Errorf("FetchNodeByID(0): expected error %v, got %v", graph.ErrNodeNotFound, err)
		}

		if node != nil {
			t.Errorf("FetchNodeByID(0): expected nodeA = nil, got %v", node)
		}
	})

	t.Run("negative test FetchNodeByID, node not in the db", func(t *testing.T) {

		db := NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{0}}

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
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{1, 2}}
		db.Nodes[1] = &graph.Node{ID: 1, SuccessorsID: []uint32{}}
		db.Nodes[2] = &graph.Node{ID: 2, SuccessorsID: []uint32{}}

		successors, err := db.GetNodeSuccessorIDs(0)

		if err != nil {
			t.Errorf("GetNodeSuccessorIDs(0): expected no error, got %v", err)
		}

		if !reflect.DeepEqual(successors, []uint32{1, 2}) {
			t.Errorf("GetNodeSuccessorIDs(0): expected successors {1, 2}, got %v", successors)
		}
	})

	t.Run("negative test GetNodeSuccessorIDs, nil db", func(t *testing.T) {

		var nil_db *MockDatabase // nil pointer

		successors, err := nil_db.GetNodeSuccessorIDs(0)

		if !errors.Is(err, graph.ErrNilDatabasePointer) {
			t.Errorf("GetNodeSuccessorIDs(0): expected error %v, got %v", graph.ErrNilDatabasePointer, err)
		}

		if successors != nil {
			t.Errorf("GetNodeSuccessorIDs(0): expected nil, got %v", successors)
		}
	})

	t.Run("negative test GetNodeSuccessorIDs, empty db", func(t *testing.T) {

		db := NewMockDatabase()

		successors, err := db.GetNodeSuccessorIDs(0)

		if !errors.Is(err, graph.ErrDatabaseIsEmpty) {
			t.Errorf("GetNodeSuccessorIDs(0): expected error %v, got %v", graph.ErrNodeNotFound, err)
		}

		if successors != nil {
			t.Errorf("GetNodeSuccessorIDs(0): expected successors = nil, got %v", successors)
		}
	})

	t.Run("negative test GetNodeSuccessorIDs, node not in db", func(t *testing.T) {

		// initialize mock database
		db := NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{0}}

		successors, err := db.GetNodeSuccessorIDs(1)

		if !errors.Is(err, graph.ErrNodeNotFound) {
			t.Errorf("GetNodeSuccessorIDs(0): expected error %v, got %v", graph.ErrNodeNotFound, err)
		}

		if successors != nil {
			t.Errorf("GetNodeSuccessorIDs(0): expected successorsA = nil, got %v", successors)
		}
	})

	t.Run("positive test IsEmpty", func(t *testing.T) {

		// initialize mock database
		empty_db := NewMockDatabase()

		got, err := empty_db.IsEmpty()
		want := true

		if got != want {
			t.Errorf("IsEmpty(): expected %v, got %v", want, got)
		}

		if err != nil {
			t.Errorf("IsEmpty(): expected 'nil' got %v", err)
		}
	})

	t.Run("negative test IsEmpty nil db", func(t *testing.T) {

		var nil_db *MockDatabase // nil pointer
		_, err := nil_db.IsEmpty()

		if !errors.Is(err, graph.ErrNilDatabasePointer) {
			t.Errorf("IsEmpty(): expected error %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})
}
