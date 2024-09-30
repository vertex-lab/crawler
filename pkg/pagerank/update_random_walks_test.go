package pagerank

import (
	"testing"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
	mock "github.com/pippellia-btc/analytic_engine/pkg/mock_database"
)

func TestUpdateRandomWalks(t *testing.T) {

	t.Run("negative UpdateRandomWalks, nil db", func(t *testing.T) {

		var db *mock.MockDatabase //	nil db

		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		randomWalksMap.AddWalk(&walk)

		err := randomWalksMap.UpdateRandomWalks(db)

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative UpdateRandomWalks, empty db", func(t *testing.T) {

		db := mock.NewMockDatabase() // empty bd

		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		randomWalksMap.AddWalk(&walk)

		err := randomWalksMap.UpdateRandomWalks(db)

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative UpdateRandomWalks, nil rwm", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		var randomWalksMap *RandomWalksMap // nil rwm
		err := randomWalksMap.UpdateRandomWalks(db)

		if err != ErrNilRWMPointer {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative UpdateRandomWalks, empty rwm", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		// empty rwm
		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)

		err := randomWalksMap.UpdateRandomWalks(db)

		if err != ErrEmptyRWM {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

}
