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

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&walk)

		err := RWM.UpdateRandomWalks(DB, 0, []uint32{})

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative UpdateRandomWalks, empty DB", func(t *testing.T) {

		DB := mock.NewMockDatabase() // empty DB

		RWM, _ := NewRandomWalksManager(0.85, 1)
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

		var RWM *RandomWalksManager // nil RWM
		err := RWM.UpdateRandomWalks(DB, 0, []uint32{})

		if err != ErrNilRWMPointer {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative UpdateRandomWalks, empty RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		// empty RWM
		RWM, _ := NewRandomWalksManager(0.85, 1)

		err := RWM.UpdateRandomWalks(DB, 0, []uint32{})

		if err != ErrEmptyRWM {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("negative UpdateRandomWalks, nodeID not in DB", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksManager(0.85, 1)
		RWM.GenerateRandomWalks(DB)

		invalidNodeID := uint32(999)
		err := RWM.UpdateRandomWalks(DB, invalidNodeID, []uint32{})

		if err != graph.ErrNodeNotFoundDB {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrNodeNotFoundDB, err)
		}
	})

	t.Run("positive UpdateRandomWalks, newly added nodeID", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksManager(0.85, 1)
		RWM.GenerateRandomWalks(DB)

		// new node 1 is added
		newNodeID := uint32(1)
		DB.Nodes[newNodeID] = &graph.Node{ID: newNodeID, SuccessorIDs: []uint32{}}

		err := RWM.UpdateRandomWalks(DB, newNodeID, []uint32{})

		if err != nil {
			t.Errorf("UpdateRandomWalks(): expected nil, got %v", err)
		}

		if _, err = RWM.WalksByNodeID(newNodeID); err != nil {
			t.Errorf("UpdateRandomWalks(): expected nil, got %v", err)
		}
	})

}

func TestUpdateRemove(t *testing.T) {

	t.Run("positive updateRemovedNodes, empty removedNodes", func(t *testing.T) {

		nodeID := uint32(0)
		removedNodes := mapset.NewSet[uint32]() // empty set

		RWM, _ := NewRandomWalksManager(0.85, 1)
		randomWalk := RandomWalk{NodeIDs: []uint32{0, 1}}
		RWM.WalksByNode[nodeID] = mapset.NewSet(&randomWalk)
		RWM.WalksByNode[1] = mapset.NewSet(&randomWalk)

		DB := mock.NewMockDatabase()
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))

		err := RWM.updateRemovedNodes(DB, nodeID, removedNodes, rng)
		if err != nil {
			t.Errorf("updateRemovedNodes(): expected nil, got %v", err)
		}

		// get the walks of the node 0
		walks, err := RWM.WalksByNodeID(0)
		if err != nil {
			t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		// should be unchanged
		if walks.ToSlice()[0] != &randomWalk {
			t.Errorf("updateRemovedNodes(): expected %v, got %v", &randomWalk, walks.ToSlice()[0])
		}

		// get the walks of the node 1
		walks, err = RWM.WalksByNodeID(1)
		if err != nil {
			t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		// should be unchanged
		if walks.ToSlice()[0] != &randomWalk {
			t.Errorf("updateRemovedNodes(): expected %v, got %v", &randomWalk, walks.ToSlice()[0])
		}

	})

	t.Run("positive updateRemovedNodes, multiple removals", func(t *testing.T) {

		nodeID := uint32(0)
		oldSuccessorIDs := []uint32{1, 2, 3}
		SuccessorIDs := []uint32{3}

		// defining the sets
		oldSuccessorSet := mapset.NewSet(oldSuccessorIDs...)
		SuccessorSet := mapset.NewSet(SuccessorIDs...)
		removedNodes := oldSuccessorSet.Difference(SuccessorSet) // = {1, 2}

		RWM, err := NewRandomWalksManager(0.85, 2)
		if err != nil {
			t.Errorf("updateRemovedNodes(): expected nil, got %v", err)
		}

		// the old walks in the RWM:   0 --> 1; 0 --> 2
		randomWalk1 := &RandomWalk{NodeIDs: []uint32{0, 1}}
		randomWalk2 := &RandomWalk{NodeIDs: []uint32{0, 2}}

		RWM.WalksByNode[nodeID] = mapset.NewSet(randomWalk1, randomWalk2)
		RWM.WalksByNode[1] = mapset.NewSet(randomWalk1)
		RWM.WalksByNode[2] = mapset.NewSet(randomWalk2)

		// the new database
		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: SuccessorIDs}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{}}

		rng := rand.New(rand.NewSource(69)) // for reproducibility

		err = RWM.updateRemovedNodes(DB, nodeID, removedNodes, rng)
		if err != nil {
			t.Errorf("updateRemovedNodes(): expected nil, got %v", err)
		}

		// get the walks of the node 0
		walks, err := RWM.WalksByNodeID(nodeID)
		if err != nil {
			t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		for walk := range walks.Iter() {

			// the new walk should be 0 --> 3 (the only possibility)
			want := []uint32{0, 3}

			if !reflect.DeepEqual(walk.NodeIDs, want) {
				t.Errorf("updateRemovedNodes() nodeID %d: expected %v, got %v", nodeID, want, walk.NodeIDs)
			}
		}

		// get the walks of the node 1
		walks, err = RWM.WalksByNodeID(1)
		if err != nil {
			t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		// which should be empty
		if walks.Cardinality() > 0 {
			t.Errorf("updateRemovedNodes(): nodeID %d: expected %v, got %v", 1, []*RandomWalk{}, walks)
		}

		// get the walks of the node 2
		walks, err = RWM.WalksByNodeID(2)
		if err != nil {
			t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		// which should be empty
		if walks.Cardinality() > 0 {
			t.Errorf("updateRemovedNodes(): nodeID %d: expected %v, got %v", 1, []*RandomWalk{}, walks)
		}

	})

}
