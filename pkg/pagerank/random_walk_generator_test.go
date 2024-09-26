package pagerank

import (
	"testing"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
	mock "github.com/pippellia-btc/analytic_engine/pkg/mock_database"
)

func TestGenerateRandomWalks(t *testing.T) {

	t.Run("negative GenerateRandomWalks, nil db", func(t *testing.T) {

		var nil_db *mock.MockDatabase
		random_walks_map := NewRandomWalksMap()

		err := random_walks_map.GenerateRandomWalks(nil_db, 0.85, 1)

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative GenerateRandomWalks, empty db", func(t *testing.T) {

		empty_db := mock.NewMockDatabase()
		random_walks_map := NewRandomWalksMap()

		err := random_walks_map.GenerateRandomWalks(empty_db, 0.85, 1)

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative GenerateRandomWalks, nil rwm", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		var random_walks_map *RandomWalksMap
		err := random_walks_map.GenerateRandomWalks(db, 0.85, 1)

		if err != ErrNilRWMPointer {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative GenerateRandomWalks, non-empty rwm", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		// non empty rwm
		random_walks_map := NewRandomWalksMap()
		walk := RandomWalk{NodeIDs: []uint32{0}}
		random_walks_map.AddWalk(&walk)

		err := random_walks_map.GenerateRandomWalks(db, 0.85, 1)

		if err != ErrRWMIsNotEmpty {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrRWMIsNotEmpty, err)
		}
	})

	t.Run("negative GenerateRandomWalks, invalid alpha", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		random_walks_map := NewRandomWalksMap()
		invalid_alphas := []float32{1.0, -0.1, 0} // slice of invalid alphas

		for i := 0; i < 3; i++ {
			err := random_walks_map.GenerateRandomWalks(db, invalid_alphas[i], 1)

			if err != ErrInvalidAlpha {
				t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrInvalidAlpha, err)
			}
		}
	})

	t.Run("negative GenerateRandomWalks, invalid walks_per_node", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		random_walks_map := NewRandomWalksMap()
		err := random_walks_map.GenerateRandomWalks(db, 0.85, 0)

		if err != ErrInvalidWalksPerNode {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrInvalidWalksPerNode, err)
		}
	})

	t.Run("positive GenerateRandomWalks, 1 dandling node", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		random_walks_map := NewRandomWalksMap()
		err := random_walks_map.GenerateRandomWalks(db, 0.85, 1)

		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		err = random_walks_map.CheckEmpty() // check it before accessing rwm
		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		// get the walks of 0
		walks, err_node := random_walks_map.GetWalksByNodeID(0)
		if err_node != nil {
			t.Errorf("GenerateRandomWalks() -> GetWalksByNodeID(0): expected nil, got %v", err_node)
		}

		got := walks[0].NodeIDs
		want := []uint32{0}

		if len(got) != len(want) {
			t.Errorf("GenerateRandomWalks() -> GetWalksByNodeID(0): expected %v, got %v", want, got)
		}

		for i, nodeID := range got {
			if nodeID != want[i] {
				t.Errorf("GenerateRandomWalks() -> GetWalksByNodeID(0): expected %v, got %v", want, got)
			}
		}

	})
}
