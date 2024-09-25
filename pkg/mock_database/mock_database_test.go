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
		db.Nodes["A"] = &graph.Node{ID: "A", SuccessorsID: []string{"B"}}
		db.Nodes["B"] = &graph.Node{ID: "B", SuccessorsID: []string{}}

		nodeA, err := db.FetchNodeByID("A")

		if err != nil {
			t.Errorf("FetchNodeByID('A'): expected no error, got %v", err)
		}

		if nodeA == nil {
			t.Fatalf("FetchNodeByID('A'): expected node ID 'A', got node = 'nil'")
		}

		if nodeA.ID != "A" {
			t.Errorf("FetchNodeByID('A'): expected node ID 'A', got %s", nodeA.ID)
		}

		if !reflect.DeepEqual(nodeA.SuccessorsID, []string{"B"}) {
			t.Errorf("FetchNodeByID('A'): expected successors {'B'}, got %s", nodeA.SuccessorsID)
		}
	})

	t.Run("negative test FetchNodeByID", func(t *testing.T) {

		// initialize mock database
		db := NewMockDatabase()

		nodeA, err := db.FetchNodeByID("A")

		if !errors.Is(err, ErrNodeNotFound) {
			t.Errorf("FetchNodeByID('A'): expected error %v, got %v", ErrNodeNotFound, err)
		}

		if nodeA != nil {
			t.Errorf("FetchNodeByID('A'): expected nodeA = nil, got %v", nodeA)
		}
	})

	t.Run("positive GetNodeSuccessorsID", func(t *testing.T) {

		// initialize mock database
		db := NewMockDatabase()
		db.Nodes["A"] = &graph.Node{ID: "A", SuccessorsID: []string{"B", "C"}}
		db.Nodes["B"] = &graph.Node{ID: "B", SuccessorsID: []string{}}
		db.Nodes["C"] = &graph.Node{ID: "C", SuccessorsID: []string{}}

		successorsA, err := db.GetNodeSuccessorsID("A")

		if err != nil {
			t.Errorf("GetNodeSuccessorsID('A'): expected no error, got %v", err)
		}

		if !reflect.DeepEqual(successorsA, []string{"B", "C"}) {
			t.Errorf("GetNodeSuccessorsID('A'): expected successors {'B', 'C'}, got %s", successorsA)
		}
	})

	t.Run("negative test GetNodeSuccessorsID", func(t *testing.T) {

		// initialize mock database
		db := NewMockDatabase()

		successorsA, err := db.GetNodeSuccessorsID("A")

		if !errors.Is(err, ErrNodeNotFound) {
			t.Errorf("GetNodeSuccessorsID('A'): expected error %v, got %v", ErrNodeNotFound, err)
		}

		if successorsA != nil {
			t.Errorf("GetNodeSuccessorsID('A'): expected successorsA = nil, got %v", successorsA)
		}
	})
}
