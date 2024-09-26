package pagerank

import (
	"testing"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
	mock "github.com/pippellia-btc/analytic_engine/pkg/mock_database"
)

func TestGenerateRandomWalks(t *testing.T) {

	t.Run("negative GenerateRandomWalks, nil db", func(t *testing.T) {

		// generate nil database. It cannot be a mock.MockDatabase because
		// when passed to the method, it gets converted in the interface
		// and the test then fails, because it's not nil anymore
		var nil_db graph.Database
		random_walks_map := NewRandomWalksMap()

		err := random_walks_map.GenerateRandomWalks(nil_db, 0.85, 1)

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative GenerateRandomWalks, empty db", func(t *testing.T) {

		// generate empty database
		empty_db := mock.NewMockDatabase()
		random_walks_map := NewRandomWalksMap()

		err := random_walks_map.GenerateRandomWalks(empty_db, 0.85, 1)

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative GenerateRandomWalks, nil rwm", func(t *testing.T) {

		// generate non-empty database
		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{0}}
		var random_walks_map *RandomWalksMap // nil rwm

		err := random_walks_map.GenerateRandomWalks(db, 0.85, 1)

		if err != ErrNilRWMPointer {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	// t.Run("positive GenerateRandomWalks, deterministic", func(t *testing.T) {

	// 	// generate new mock database with 3 dandling nodes
	// 	db := mock.NewMockDatabase()
	// 	db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}
	// 	db.Nodes[1] = &graph.Node{ID: 1, SuccessorsID: []uint32{}}
	// 	db.Nodes[2] = &graph.Node{ID: 2, SuccessorsID: []uint32{}}

	// 	rand.Seed(42) // set a fixed seed for deterministic behavior in tests

	// 	random_walks_map := NewRandomWalksMap()
	// 	random_walks_map.GenerateRandomWalks(db, 0.85, 1) // just do one walk

	// 	for node := range db.Nodes

	// })
}
