package pagerank

import (
	"math/rand"
	"reflect"
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

		err := randomWalksMap.UpdateRandomWalks(db, 0, []uint32{})

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative UpdateRandomWalks, empty db", func(t *testing.T) {

		db := mock.NewMockDatabase() // empty bd

		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		randomWalksMap.AddWalk(&walk)

		err := randomWalksMap.UpdateRandomWalks(db, 0, []uint32{})

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative UpdateRandomWalks, nil rwm", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		var randomWalksMap *RandomWalksMap // nil rwm
		err := randomWalksMap.UpdateRandomWalks(db, 0, []uint32{})

		if err != ErrNilRWMPointer {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative UpdateRandomWalks, empty rwm", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		// empty rwm
		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)

		err := randomWalksMap.UpdateRandomWalks(db, 0, []uint32{})

		if err != ErrEmptyRWM {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("positive UpdateRandomWalks, simple removal", func(t *testing.T) {

		nodeID := uint32(0)
		oldSuccessorIDs := []uint32{1, 2}

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: oldSuccessorIDs}
		db.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		db.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}

		randomWalksMap, err := NewRandomWalksMap(0.85, 2)
		if err != nil {
			t.Errorf("UpdateRandomWalks(): expected nil, got %v", err)
		}

		// reproducibly generate the random walks
		randomNumGen := rand.New(rand.NewSource(69))
		err = randomWalksMap.generateRandomWalks(db, randomNumGen)
		if err != nil {
			t.Errorf("UpdateRandomWalks() -> generateRandomWalks(): expected nil, got %v", err)
		}

		// now the database changes
		db.Nodes[nodeID].SuccessorIDs = []uint32{1}

		// so we reproducibly update the rwm
		err = randomWalksMap.updateRandomWalks(db, nodeID, oldSuccessorIDs, randomNumGen)
		if err != nil {
			t.Errorf("updateRandomWalks(): expected nil, got %v", err)
		}

		nodeIDs, err := db.GetAllNodeIDs()
		if err != nil {
			t.Errorf("UpdateRandomWalks() -> GetAllNodeIDs(): expected nil, got %v", err)
		}

		// iterate over all nodes in the db
		for _, nodeID := range nodeIDs {

			// get the walks of a node
			walk_pointers, err := randomWalksMap.GetWalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("UpdateRandomWalks() -> GetWalksByNodeID(): expected nil, got %v", err)
			}

			// dereference walks and sort them in lexicographic order
			walks, err := sortWalks(walk_pointers)
			if err != nil {
				t.Errorf("UpdateRandomWalks(): expected nil, got %v", err)
			}

			if walks != nil {
				t.Errorf("UpdateRandomWalks() nodeID = %d: got %v", nodeID, walks)
			}
		}

	})
}

func TestSliceDifference(t *testing.T) {

	t.Run("sliceDifference, empty first slice", func(t *testing.T) {

		sliceA := []uint32{}
		sliceB := []uint32{1}

		diff := sliceDifference(sliceA, sliceB)
		if !reflect.DeepEqual(diff, []uint32{}) {
			t.Errorf("sliceDifference(): expected {}, got %v", diff)
		}
	})

	t.Run("sliceDifference, empty second slice", func(t *testing.T) {

		sliceA := []uint32{1}
		sliceB := []uint32{0}

		diff := sliceDifference(sliceA, sliceB)
		if !reflect.DeepEqual(diff, sliceA) {
			t.Errorf("sliceDifference(): expected %v, got %v", sliceA, diff)
		}
	})

	t.Run("sliceDifference, non-empty slices", func(t *testing.T) {

		sliceA := []uint32{1, 3, 5}
		sliceB := []uint32{0, 5}

		want := []uint32{1, 3}
		diff := sliceDifference(sliceA, sliceB)
		if !reflect.DeepEqual(diff, want) {
			t.Errorf("sliceDifference(): expected %v, got %v", want, diff)
		}
	})
}

func BenchmarkSliceDifference(b *testing.B) {

	sliceA := []uint32{1, 3, 5}
	sliceB := []uint32{0, 5}

	for i := 0; i < b.N; i++ {
		_ = sliceDifference(sliceA, sliceB)
	}
}
