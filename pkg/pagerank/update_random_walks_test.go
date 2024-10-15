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

		RWM, _ := NewRandomWalksManager(0.85, 1) // empty RWM
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

		invalidNodeID := uint32(999) // invalid nodeID
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

		newNodeID := uint32(1) // new node1 is added to the DB
		DB.Nodes[newNodeID] = &graph.Node{ID: newNodeID, SuccessorIDs: []uint32{}}

		err := RWM.UpdateRandomWalks(DB, newNodeID, []uint32{})
		if err != nil {
			t.Errorf("UpdateRandomWalks(): expected nil, got %v", err)
		}

		walkSet, err := RWM.WalksByNodeID(newNodeID)
		if err != nil {
			t.Errorf("UpdateRandomWalks(): expected nil, got %v", err)
		}

		want := []uint32{1}
		got := walkSet.ToSlice()[0].NodeIDs

		if !reflect.DeepEqual(got, want) {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", want, got)
		}
	})
}

func TestUpdateRemove(t *testing.T) {

	t.Run("positive updateRemovedNodes, empty removedNodes", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}

		nodeID := uint32(0)
		removedNodes := mapset.NewSet[uint32]() // empty set

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0, 1}})

		RWM.WalksByNode[nodeID] = walkSet
		RWM.WalksByNode[1] = walkSet

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		err := RWM.updateRemovedNodes(DB, nodeID, removedNodes, rng)
		if err != nil {
			t.Errorf("updateRemovedNodes(): expected nil, got %v", err)
		}

		// get the walkSet of node0
		walkSet0, err := RWM.WalksByNodeID(0)
		if err != nil {
			t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		// should be unchanged
		if !walkSet0.Equal(walkSet) {
			t.Errorf("updateRemovedNodes(): expected %v, got %v", walkSet, walkSet0)
		}

		// get the walkSet of node0
		walkSet1, err := RWM.WalksByNodeID(1)
		if err != nil {
			t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		// should be unchanged
		if !walkSet1.Equal(walkSet) {
			t.Errorf("updateRemovedNodes(): expected %v, got %v", walkSet, walkSet1)
		}
	})

	t.Run("positive updateRemovedNodes, multiple removals", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 2)

		// the old walks in the RWM:   0 --> 1; 0 --> 2
		randomWalk1 := &RandomWalk{NodeIDs: []uint32{0, 1}}
		randomWalk2 := &RandomWalk{NodeIDs: []uint32{0, 2}}
		RWM.AddWalk(randomWalk1)
		RWM.AddWalk(randomWalk2)

		nodeID := uint32(0)
		newSuccessorIDs := []uint32{3}
		removedNodes := mapset.NewSet[uint32](1, 2)

		// the new database
		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: newSuccessorIDs}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{0}}

		rng := rand.New(rand.NewSource(69)) // for reproducibility

		err := RWM.updateRemovedNodes(DB, nodeID, removedNodes, rng)
		if err != nil {
			t.Errorf("updateRemovedNodes(): expected nil, got %v", err)
		}

		// get the walks of the node 0
		walkSet0, err := RWM.WalksByNodeID(nodeID)
		if err != nil {
			t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		for walk := range walkSet0.Iter() {

			// the new walk should be 0 --> 3 (the only possibility)
			want := []uint32{0, 3}

			if !reflect.DeepEqual(walk.NodeIDs, want) {
				t.Errorf("updateRemovedNodes() nodeID %d: expected %v, got %v", nodeID, want, walk.NodeIDs)
			}
		}

		// get the walks of the node 1
		walkSet1, err := RWM.WalksByNodeID(1)
		if err != nil {
			t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		// which should be empty
		if walkSet1.Cardinality() > 0 {
			t.Errorf("updateRemovedNodes(): nodeID %d: expected %v, got %v", 1, mapset.NewSet[*RandomWalk](), walkSet1)
		}

		// get the walks of the node 1
		walkSet2, err := RWM.WalksByNodeID(2)
		if err != nil {
			t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		// which should be empty
		if walkSet2.Cardinality() > 0 {
			t.Errorf("updateRemovedNodes(): nodeID %d: expected %v, got %v", 1, mapset.NewSet[*RandomWalk](), walkSet2)
		}

	})

}
