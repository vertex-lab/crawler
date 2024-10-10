package pagerank

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/analytic_engine/pkg/graph"
	mock "github.com/pippellia-btc/analytic_engine/pkg/mock_database"
)

func TestUpdateRandomWalks(t *testing.T) {

	t.Run("negative UpdateRandomWalks, nil DB", func(t *testing.T) {

		var DB *mock.MockDatabase //	nil DB

		RWM, _ := NewRandomWalksMap(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&walk)

		err := RWM.UpdateRandomWalks(DB, 0, []uint32{})

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative UpdateRandomWalks, empty DB", func(t *testing.T) {

		DB := mock.NewMockDatabase() // empty bd

		RWM, _ := NewRandomWalksMap(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&walk)

		err := RWM.UpdateRandomWalks(DB, 0, []uint32{})

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative UpdateRandomWalks, nil RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		var RWM *RandomWalksMap // nil RWM
		err := RWM.UpdateRandomWalks(DB, 0, []uint32{})

		if err != ErrNilRWMPointer {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative UpdateRandomWalks, empty RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		// empty RWM
		RWM, _ := NewRandomWalksMap(0.85, 1)

		err := RWM.UpdateRandomWalks(DB, 0, []uint32{})

		if err != ErrEmptyRWM {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("negative UpdateRandomWalks, nodeID not in DB", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksMap(0.85, 1)
		RWM.GenerateRandomWalks(DB)

		invalidNodeID := uint32(999)
		err := RWM.UpdateRandomWalks(DB, invalidNodeID, []uint32{})

		if err != graph.ErrNodeNotFoundDB {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrNodeNotFoundDB, err)
		}
	})

}

func TestUpdateRemove(t *testing.T) {

	t.Run("positive updateRemove, empty removedNodes", func(t *testing.T) {

		nodeID := uint32(0)
		removedNodes := mapset.NewSet[uint32]() // it shouldn't change anything

		RWM, _ := NewRandomWalksMap(0.85, 1)
		randomWalk := RandomWalk{NodeIDs: []uint32{0, 1}}
		RWM.WalksByNode[nodeID] = []*RandomWalk{&randomWalk}
		RWM.WalksByNode[1] = []*RandomWalk{&randomWalk}

		DB := mock.NewMockDatabase()
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))

		err := RWM.updateRemove(DB, nodeID, removedNodes, rng)
		if err != nil {
			t.Errorf("updateRemove(): expected nil, got %v", err)
		}

		// get the walks of the node 0
		walks, err := RWM.GetWalksByNodeID(0)
		if err != nil {
			t.Fatalf("updateRemove() -> GetWalksByNodeID(): expected nil, got %v", err)
		}

		if walks[0] != &randomWalk {
			t.Errorf("updateRemove(): expected %v, got %v", &randomWalk, walks[0])
		}

		// get the walks of the node 1
		walks, err = RWM.GetWalksByNodeID(1)
		if err != nil {
			t.Fatalf("updateRemove() -> GetWalksByNodeID(): expected nil, got %v", err)
		}

		if walks[0] != &randomWalk {
			t.Errorf("updateRemove(): expected %v, got %v", &randomWalk, walks[0])
		}

	})

	t.Run("positive updateRemove, one walk", func(t *testing.T) {

		nodeID := uint32(0)
		oldSuccessorIDs := []uint32{1, 2}
		SuccessorIDs := []uint32{2}

		// defining the sets
		oldSuccessorSet := mapset.NewSet(oldSuccessorIDs...)
		SuccessorSet := mapset.NewSet(SuccessorIDs...)
		removedNodes := oldSuccessorSet.Difference(SuccessorSet) // = {1}

		RWM, err := NewRandomWalksMap(0.85, 1)
		if err != nil {
			t.Errorf("updateRemove(): expected nil, got %v", err)
		}

		// the old walk in the RWM:   0 --> 1
		randomWalk := RandomWalk{NodeIDs: []uint32{0, 1}}
		RWM.WalksByNode[nodeID] = []*RandomWalk{&randomWalk}
		RWM.WalksByNode[1] = []*RandomWalk{&randomWalk}

		// the new database
		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: SuccessorIDs}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}

		rng := rand.New(rand.NewSource(69)) // for reproducibility

		err = RWM.updateRemove(DB, nodeID, removedNodes, rng)
		if err != nil {
			t.Errorf("updateRemove(): expected nil, got %v", err)
		}

		// get the walks of the node 0
		walks, err := RWM.GetWalksByNodeID(nodeID)
		if err != nil {
			t.Fatalf("updateRemove() -> GetWalksByNodeID(): expected nil, got %v", err)
		}

		// the new walk should be 0 --> 2 (the only possibility)
		want := []uint32{0, 2}

		if !reflect.DeepEqual(walks[0].NodeIDs, want) {
			t.Errorf("updateRemove() nodeID %d: expected %v, got %v", nodeID, want, walks[0].NodeIDs)
		}

		// get the walks of the node 1
		walks, err = RWM.GetWalksByNodeID(1)
		if err != nil {
			t.Fatalf("updateRemove() -> GetWalksByNodeID(): expected nil, got %v", err)
		}

		// which should be empty
		if len(walks) > 0 {
			t.Errorf("updateRemove(): nodeID %d: expected %v, got %v", 1, []*RandomWalk{}, walks)
		}

	})

}
