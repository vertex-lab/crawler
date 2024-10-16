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

func TestUpdateNewNode(t *testing.T) {

	t.Run("negative UpdateNewNode, nil DB", func(t *testing.T) {

		var DB *mock.MockDatabase //	nil DB

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&walk)

		err := RWM.UpdateNewNode(DB, 0)

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("UpdateNewNode(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative UpdateNewNode, empty DB", func(t *testing.T) {

		DB := mock.NewMockDatabase() // empty DB

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&walk)

		err := RWM.UpdateNewNode(DB, 0)

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("UpdateNewNode(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative UpdateNewNode, nil RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		var RWM *RandomWalksManager // nil RWM
		err := RWM.UpdateNewNode(DB, 0)

		if err != ErrNilRWMPointer {
			t.Errorf("UpdateNewNode(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative UpdateNewNode, empty RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksManager(0.85, 1) // empty RWM
		err := RWM.UpdateNewNode(DB, 0)

		if err != ErrEmptyRWM {
			t.Errorf("UpdateNewNode(): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("negative UpdateNewNode, nodeID not in DB", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksManager(0.85, 1)
		RWM.GenerateRandomWalks(DB)

		invalidNodeID := uint32(999) // invalid nodeID
		err := RWM.UpdateNewNode(DB, invalidNodeID)

		if err != graph.ErrNodeNotFoundDB {
			t.Errorf("UpdateNewNode(): expected %v, got %v", graph.ErrNodeNotFoundDB, err)
		}
	})

	t.Run("positive UpdateNewNode", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksManager(0.85, 3)
		RWM.GenerateRandomWalks(DB)

		newNodeID := uint32(1) // new node1 is added to the DB
		DB.Nodes[newNodeID] = &graph.Node{ID: newNodeID, SuccessorIDs: []uint32{}}

		err := RWM.UpdateNewNode(DB, newNodeID)
		if err != nil {
			t.Errorf("UpdateNewNode(): expected nil, got %v", err)
		}

		walkSet, err := RWM.WalksByNodeID(newNodeID)
		if err != nil {
			t.Errorf("UpdateNewNode(): expected nil, got %v", err)
		}

		want := []uint32{1}
		for walk := range walkSet.Iter() {

			if !reflect.DeepEqual(walk.NodeIDs, want) {
				t.Fatalf("UpdateNewNode(): expected %v, got %v", want, walkSet)
			}
		}
	})
}

func TestUpdateRandomWalks(t *testing.T) {

	t.Run("negative UpdateRandomWalks, nil DB", func(t *testing.T) {

		var DB *mock.MockDatabase //	nil DB

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&walk)

		err := RWM.UpdateRandomWalks(DB, 0, []uint32{}, []uint32{})

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative UpdateRandomWalks, empty DB", func(t *testing.T) {

		DB := mock.NewMockDatabase() // empty DB

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&walk)

		err := RWM.UpdateRandomWalks(DB, 0, []uint32{}, []uint32{})

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative UpdateRandomWalks, nil RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		var RWM *RandomWalksManager // nil RWM
		err := RWM.UpdateRandomWalks(DB, 0, []uint32{}, []uint32{})

		if err != ErrNilRWMPointer {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative UpdateRandomWalks, empty RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksManager(0.85, 1) // empty RWM
		err := RWM.UpdateRandomWalks(DB, 0, []uint32{}, []uint32{})

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
		err := RWM.UpdateRandomWalks(DB, invalidNodeID, []uint32{}, []uint32{})

		if err != graph.ErrNodeNotFoundDB {
			t.Errorf("UpdateRandomWalks(): expected %v, got %v", graph.ErrNodeNotFoundDB, err)
		}
	})
}

func TestUpdateRemovedNodes(t *testing.T) {

	t.Run("positive updateRemovedNodes, empty removedIDs", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}

		nodeID := uint32(0)
		removedIDs := []uint32{} // empty slice

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0, 1}})

		RWM.WalksByNode[nodeID] = walkSet
		RWM.WalksByNode[1] = walkSet

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		err := RWM.updateRemovedNodes(DB, nodeID, removedIDs, rng)
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

		// get the walkSet of node1
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
		removedIDs := []uint32{1, 2}

		// the new database
		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: newSuccessorIDs}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{0}}

		rng := rand.New(rand.NewSource(69)) // for reproducibility

		err := RWM.updateRemovedNodes(DB, nodeID, removedIDs, rng)
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

func TestUpdateAddedNodes(t *testing.T) {

	t.Run("positive updateAddedNodes, empty addedIDs", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}

		nodeID := uint32(0)
		addedIDs := []uint32{} // empty slice

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0, 1}})

		RWM.WalksByNode[nodeID] = walkSet
		RWM.WalksByNode[1] = walkSet

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		err := RWM.updateAddedNodes(DB, nodeID, addedIDs, 1, rng)
		if err != nil {
			t.Errorf("updateAddedNodes(): expected nil, got %v", err)
		}

		// get the walkSet of node0
		walkSet0, err := RWM.WalksByNodeID(0)
		if err != nil {
			t.Fatalf("updateAddedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		// should be unchanged
		if !walkSet0.Equal(walkSet) {
			t.Errorf("updateAddedNodes(): expected %v, got %v", walkSet, walkSet0)
		}

		// get the walkSet of node1
		walkSet1, err := RWM.WalksByNodeID(1)
		if err != nil {
			t.Fatalf("updateAddedNodes() -> WalksByNodeID(): expected nil, got %v", err)
		}

		// should be unchanged
		if !walkSet1.Equal(walkSet) {
			t.Errorf("updateAddedNodes(): expected %v, got %v", walkSet, walkSet1)
		}
	})

	t.Run("positive updateAddedNodes, multiple addNodes", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)

		// the old walks in the RWM:   0 --> 1;
		randomWalk0 := &RandomWalk{NodeIDs: []uint32{0, 1}}
		randomWalk1 := &RandomWalk{NodeIDs: []uint32{1}}
		randomWalk2 := &RandomWalk{NodeIDs: []uint32{2}}
		randomWalk3 := &RandomWalk{NodeIDs: []uint32{3, 0, 2}}
		randomWalk4 := &RandomWalk{NodeIDs: []uint32{4}}

		RWM.AddWalk(randomWalk0)
		RWM.AddWalk(randomWalk1)
		RWM.AddWalk(randomWalk2)
		RWM.AddWalk(randomWalk3)
		RWM.AddWalk(randomWalk4)

		// the new successors of nodeID
		nodeID := uint32(0)
		addedIDs := []uint32{3, 4}
		newSuccessorIDs := []uint32{1, 2, 3, 4}
		newOutDegree := len(newSuccessorIDs)

		// the new database
		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: newSuccessorIDs}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{0}} // 3 --> 0
		DB.Nodes[4] = &graph.Node{ID: 4, SuccessorIDs: []uint32{}}

		// for reproducibility
		rng := rand.New(rand.NewSource(4))
		expectedWalks := map[uint32][][]uint32{
			0: {
				{0, 3},
				{3, 0, 2},
			},
			1: {
				{1},
			},
			2: {
				{2},
				{3, 0, 2},
			},
			3: {
				{0, 3},
				{3, 0, 2},
			},
			4: {
				{4},
			},
		}

		err := RWM.updateAddedNodes(DB, nodeID, addedIDs, newOutDegree, rng)
		if err != nil {
			t.Fatalf("updateAddedNodes(): expected nil, got %v", err)
		}

		nodeIDs, err := DB.AllNodeIDs()
		if err != nil {
			t.Fatalf("updateAddedNodes() -> AllNodeIDs(): expected nil, got %v", err)
		}

		for _, nodeID := range nodeIDs {

			walkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("updateAddedNodes() -> WalksByNodeID(%d): expected nil, got %v", nodeID, err)
			}

			// dereference walks and sort them in lexicographic order
			walks, err := sortWalks(walkSet)
			if err != nil {
				t.Errorf("GenerateRandomWalks(): expected nil, got %v", err)
			}

			if !reflect.DeepEqual(walks, expectedWalks[nodeID]) {
				t.Errorf("GenerateRandomWalks() nodeID = %d: expected %v, got %v", nodeID, expectedWalks[nodeID], walks)
			}
		}
	})

}

func TestNodeChanges(t *testing.T) {

	oldIDs := []uint32{0, 1, 2, 4}
	newIDs := []uint32{1, 2, 3}

	removedIDs, addedIDs := nodeChanges(oldIDs, newIDs)

	if removedIDs[0] != 0 || removedIDs[1] != 4 {
		t.Errorf("expected [0, 4], got %v", removedIDs)
	}

	if addedIDs[0] != 3 {
		t.Errorf("expected 3, got %v", addedIDs[3])
	}

}

// ---------------------------------BENCHMARKS---------------------------------

func BenchmarkNodeChanges(b *testing.B) {

	size := int32(10000)

	oldIDs := make([]uint32, size)
	newIDs := make([]uint32, size)

	// setup old and current IDs
	for i := int32(0); i < size; i++ {

		oldID := uint32(rand.Int31n(size * 2))
		newID := uint32(rand.Int31n(size * 2))

		oldIDs = append(oldIDs, oldID)
		newIDs = append(newIDs, newID)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		nodeChanges(oldIDs, newIDs)
	}
}

// func BenchmarkUpdateRandomWalks(b *testing.B) {

// 	DB := mock.NewMockDatabase()

// 	oldIDs := make([]uint32, 5000)
// 	currentIDs := make([]uint32, 5000)

// 	// setup old and current IDs
// 	for i := 0; i < 5000; i++ {
// 		oldIDs = append(oldIDs, uint32(i))

// 		currentID := uint32(i + 2000)
// 		currentIDs = append(currentIDs, currentID)
// 		DB.Nodes[currentID] = &graph.Node{ID: currentID, SuccessorIDs: []uint32{}}
// 	}

// 	nodeID := uint32(0)
// 	DB.Nodes[nodeID] = &graph.Node{ID: nodeID, SuccessorIDs: currentIDs}

// 	RWM, _ := NewRandomWalksManager(0.85, 1)
// 	err := RWM.GenerateRandomWalks(DB)
// 	if err != nil {
// 		b.Fatalf("expected nil, got %v", err)
// 	}

// 	b.ResetTimer()

// 	for i := 0; i < b.N; i++ {
// 		err := RWM.UpdateRandomWalks(DB, nodeID, oldIDs)
// 		if err != nil {
// 			b.Fatalf("expected nil, got %v", err)
// 		}
// 	}
// }
