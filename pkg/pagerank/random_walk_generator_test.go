package pagerank

import (
	"testing"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
	mock "github.com/pippellia-btc/analytic_engine/pkg/mock_database"
)

func TestGenerateRandomWalks(t *testing.T) {

	t.Run("negative GenerateRandomWalks, empty database", func(t *testing.T) {

		// generate empty database
		empty_db := mock.NewMockDatabase()
		random_walks_map := NewRandomWalksMap()

		err := random_walks_map.GenerateRandomWalks(empty_db, 0.85, 1)

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	// t.Run("positive GenerateRandomWalks", func(t *testing.T) {

	// 	// generate new mock database with 3 dandling nodes
	// 	db := NewMockDatabase()

	// 	random_walks_map := NewRandomWalksMap()
	// 	random_walks_map.GenerateRandomWalks(0.85, 1)

	// })
}
