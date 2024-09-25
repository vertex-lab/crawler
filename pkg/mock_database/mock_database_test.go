package mockdatabase

import (
	"errors"
	"reflect"
	"testing"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

func TestMockDatabase(t *testing.T) {

	t.Run("positive test FetchNodeByID", func(t *testing.T) {

		// initialize mock database
		db := NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{1}}
		db.Nodes[1] = &graph.Node{ID: 1, SuccessorsID: []uint32{}}

		nodeA, err := db.FetchNodeByID(0)

		if err != nil {
			t.Errorf("FetchNodeByID(0): expected no error, got %v", err)
		}

		if nodeA == nil {
			t.Fatalf("FetchNodeByID(0): expected node ID 0, got node = 'nil'")
		}

		if nodeA.ID != 0 {
			t.Errorf("FetchNodeByID(0): expected node ID 0, got %d", nodeA.ID)
		}

		if !reflect.DeepEqual(nodeA.SuccessorsID, []uint32{1}) {
			t.Errorf("FetchNodeByID(0): expected successors {1}, got %d", nodeA.SuccessorsID)
		}
	})

	t.Run("negative test FetchNodeByID", func(t *testing.T) {

		// initialize mock database
		db := NewMockDatabase()

		nodeA, err := db.FetchNodeByID(0)

		if !errors.Is(err, ErrNodeNotFound) {
			t.Errorf("FetchNodeByID(0): expected error %v, got %v", ErrNodeNotFound, err)
		}

		if nodeA != nil {
			t.Errorf("FetchNodeByID(0): expected nodeA = nil, got %v", nodeA)
		}
	})

	t.Run("positive GetNodeSuccessorsID", func(t *testing.T) {

		// initialize mock database
		db := NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{1, 2}}
		db.Nodes[1] = &graph.Node{ID: 1, SuccessorsID: []uint32{}}
		db.Nodes[2] = &graph.Node{ID: 2, SuccessorsID: []uint32{}}

		successorsA, err := db.GetNodeSuccessorIDs(0)

		if err != nil {
			t.Errorf("GetNodeSuccessorsID(0): expected no error, got %v", err)
		}

		if !reflect.DeepEqual(successorsA, []uint32{1, 2}) {
			t.Errorf("GetNodeSuccessorsID(0): expected successors {1, 2}, got %v", successorsA)
		}
	})

	t.Run("negative test GetNodeSuccessorsID", func(t *testing.T) {

		// initialize mock database
		db := NewMockDatabase()

		successorsA, err := db.GetNodeSuccessorIDs(0)

		if !errors.Is(err, ErrNodeNotFound) {
			t.Errorf("GetNodeSuccessorsID(0): expected error %v, got %v", ErrNodeNotFound, err)
		}

		if successorsA != nil {
			t.Errorf("GetNodeSuccessorsID(0): expected successorsA = nil, got %v", successorsA)
		}
	})
}
