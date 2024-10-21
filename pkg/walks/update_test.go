package walks

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
)

func TestUpdateRemovedNodes(t *testing.T) {

	t.Run("positive updateRemovedNodes, empty removedIDs", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}

		nodeID := uint32(0)
		removedIDs := []uint32{} // empty slice

		RWM, _ := NewRWM(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0, 1}})

		RWM.WalksByNode[nodeID] = walkSet
		RWM.WalksByNode[1] = walkSet

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		err := RWM.updateRemovedNodes(DB, nodeID, removedIDs, rng)
		if err != nil {
			t.Errorf("updateRemovedNodes(): expected nil, got %v", err)
		}

		nodeIDs, _ := DB.AllNodeIDs()
		for _, nodeID := range nodeIDs {

			newWalkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
			}

			// should be unchanged
			if !newWalkSet.Equal(walkSet) {
				t.Errorf("updateRemovedNodes(): expected %v, got %v", newWalkSet, walkSet)
			}
		}
	})

	t.Run("positive updateRemovedNodes, multiple removals", func(t *testing.T) {

		RWM, _ := NewRWM(0.85, 1)

		rWalk0 := &RandomWalk{NodeIDs: []uint32{0, 1}}    // will be updated
		rWalk3 := &RandomWalk{NodeIDs: []uint32{3, 0, 2}} // will be updated

		rWalk1 := &RandomWalk{NodeIDs: []uint32{1}}
		rWalk2 := &RandomWalk{NodeIDs: []uint32{2}}

		RWM.AddWalk(rWalk0)
		RWM.AddWalk(rWalk1)
		RWM.AddWalk(rWalk2)
		RWM.AddWalk(rWalk3)

		nodeID := uint32(0)
		newSuccessorIDs := []uint32{3}
		removedIDs := []uint32{1, 2}

		// the new database
		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: newSuccessorIDs}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{0}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{0}}

		rng := rand.New(rand.NewSource(5)) // for reproducibility
		expectedWalks := map[uint32][][]uint32{
			0: {
				{0, 3},
				{3, 0},
			},
			1: {
				{1},
			},
			2: {
				{2},
			},
			3: {
				{0, 3},
				{3, 0},
			},
		}

		err := RWM.updateRemovedNodes(DB, nodeID, removedIDs, rng)
		if err != nil {
			t.Errorf("updateRemovedNodes(): expected nil, got %v", err)
		}

		nodeIDs, _ := DB.AllNodeIDs()
		for _, nodeID := range nodeIDs {

			walkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("updateRemovedNodes() -> WalksByNodeID(%d): expected nil, got %v", nodeID, err)
			}

			// dereference walks and sort them in lexicographic order
			walks, err := sortWalks(walkSet)
			if err != nil {
				t.Errorf("updateRemovedNodes(): expected nil, got %v", err)
			}

			if !reflect.DeepEqual(walks, expectedWalks[nodeID]) {
				t.Errorf("updateRemovedNodes() nodeID = %d: expected %v, got %v", nodeID, expectedWalks[nodeID], walks)
			}
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

		RWM, _ := NewRWM(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0, 1}})

		RWM.WalksByNode[nodeID] = walkSet
		RWM.WalksByNode[1] = walkSet

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		err := RWM.updateAddedNodes(DB, nodeID, addedIDs, 1, rng)
		if err != nil {
			t.Errorf("updateAddedNodes(): expected nil, got %v", err)
		}

		nodeIDs, _ := DB.AllNodeIDs()
		for _, nodeID := range nodeIDs {

			newWalkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("updateRemovedNodes() -> WalksByNodeID(): expected nil, got %v", err)
			}

			// should be unchanged
			if !newWalkSet.Equal(walkSet) {
				t.Errorf("updateRemovedNodes(): expected %v, got %v", newWalkSet, walkSet)
			}
		}
	})

	t.Run("positive updateAddedNodes, multiple additions", func(t *testing.T) {

		RWM, _ := NewRWM(0.85, 1)

		// the old walks in the RWM:   0 --> 1;
		rWalk0 := &RandomWalk{NodeIDs: []uint32{0, 1}}
		rWalk1 := &RandomWalk{NodeIDs: []uint32{1}}
		rWalk2 := &RandomWalk{NodeIDs: []uint32{2}}
		rWalk3 := &RandomWalk{NodeIDs: []uint32{3, 0, 2}}
		rWalk4 := &RandomWalk{NodeIDs: []uint32{4}}

		RWM.AddWalk(rWalk0)
		RWM.AddWalk(rWalk1)
		RWM.AddWalk(rWalk2)
		RWM.AddWalk(rWalk3)
		RWM.AddWalk(rWalk4)

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

		// walkSet.Iter() returns the walk in an arbitrary order.
		// Because there are two walks, there can be two expected results!
		expectedWalks1 := map[uint32][][]uint32{
			0: {
				{0, 4},
				{3, 0},
			},
			1: {
				{1},
			},
			2: {
				{2},
			},
			3: {
				{3, 0},
			},
			4: {
				{0, 4},
				{4},
			},
		}

		expectedWalks2 := map[uint32][][]uint32{
			0: {
				{0, 3},
				{3, 0, 4},
			},
			1: {
				{1},
			},
			2: {
				{2},
			},
			3: {
				{0, 3},
				{3, 0, 4},
			},
			4: {
				{3, 0, 4},
				{4},
			},
		}

		_ = expectedWalks2

		err := RWM.updateAddedNodes(DB, nodeID, addedIDs, newOutDegree, rng)
		if err != nil {
			t.Fatalf("updateAddedNodes(): expected nil, got %v", err)
		}

		var expectedWalks map[uint32][][]uint32

		// check which expectedWalk to use
		if RWM.WalksByNode[3].Cardinality() == 1 {
			expectedWalks = expectedWalks1
		} else {
			expectedWalks = expectedWalks2
		}

		nodeIDs, _ := DB.AllNodeIDs()
		for _, nodeID := range nodeIDs {

			walkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("updateAddedNodes() -> WalksByNodeID(%d): expected nil, got %v", nodeID, err)
			}

			// dereference walks and sort them in lexicographic order
			walks, err := sortWalks(walkSet)
			if err != nil {
				t.Errorf("updateAddedNodes(): expected nil, got %v", err)
			}

			if !reflect.DeepEqual(walks, expectedWalks[nodeID]) {
				t.Errorf("updateAddedNodes() nodeID = %d: expected %v, got %v", nodeID, expectedWalks[nodeID], walks)
			}
		}
	})
}

func TestUpdate(t *testing.T) {

	t.Run("negative Update, nil DB", func(t *testing.T) {

		var DB *mock.MockDatabase //	nil DB

		RWM, _ := NewRWM(0.85, 1)
		rWalk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&rWalk)

		err := RWM.Update(DB, 0, []uint32{}, []uint32{})

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("Update(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative Update, empty DB", func(t *testing.T) {

		DB := mock.NewMockDatabase() // empty DB

		RWM, _ := NewRWM(0.85, 1)
		rWalk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&rWalk)

		err := RWM.Update(DB, 0, []uint32{}, []uint32{})

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("Update(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative Update, nil RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		var RWM *RandomWalksManager // nil RWM
		err := RWM.Update(DB, 0, []uint32{}, []uint32{})

		if err != ErrNilRWMPointer {
			t.Errorf("Update(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative Update, empty RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRWM(0.85, 1) // empty RWM
		err := RWM.Update(DB, 0, []uint32{}, []uint32{})

		if err != ErrEmptyRWM {
			t.Errorf("Update(): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("negative Update, nodeID not in DB", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRWM(0.85, 1)
		RWM.GenerateAll(DB)

		invalidNodeID := uint32(999) // invalid nodeID
		err := RWM.Update(DB, invalidNodeID, []uint32{}, []uint32{})

		if err != graph.ErrNodeNotFoundDB {
			t.Errorf("Update(): expected %v, got %v", graph.ErrNodeNotFoundDB, err)
		}
	})
}

// ---------------------------------BENCHMARKS---------------------------------

func BenchmarkUpdateAdd(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)

	RWM, _ := NewRWM(0.85, 10)
	RWM.GenerateAll(DB)

	// store the changes here
	oldSuccessorMap := make(map[uint32][]uint32, nodesSize)
	currentSuccessorMap := make(map[uint32][]uint32, nodesSize)

	b.Run("Update(), 10% new successors", func(b *testing.B) {

		// prepare the graph changes
		for nodeID := uint32(0); nodeID < uint32(nodesSize); nodeID++ {

			oldSuccessorIDs, _ := DB.NodeSuccessorIDs(nodeID)
			currentSuccessorIDs := make([]uint32, len(oldSuccessorIDs))
			copy(currentSuccessorIDs, oldSuccessorIDs)

			// add 10% new nodes
			for i := 0; i < edgesPerNode/10; i++ {

				newNode := uint32(rng.Intn(nodesSize))
				currentSuccessorIDs = append(currentSuccessorIDs, newNode)
			}

			oldSuccessorMap[nodeID] = oldSuccessorIDs
			currentSuccessorMap[nodeID] = currentSuccessorIDs
		}

		b.ResetTimer()

		// perform benchmark
		for i := 0; i < b.N; i++ {

			nodeID := uint32(i % nodesSize)
			oldSuccessorIDs := oldSuccessorMap[nodeID]
			currentSuccessorIDs := currentSuccessorMap[nodeID]

			err := RWM.Update(DB, nodeID, oldSuccessorIDs, currentSuccessorIDs)
			if err != nil {
				b.Fatalf("Update() failed: %v", err)
			}
		}
	})
}

/*
!IMPORTANT!

run this benchmark with:

> -benchtime=nodesSizex

each node should only be updated once. Each subsequent update will be
much cheaper because no walk will need an update, thus compromizing the measurement
*/
func BenchmarkUpdateRemove(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)

	RWM, _ := NewRWM(0.85, 10)
	RWM.GenerateAll(DB)

	// store the changes here
	oldSuccessorMap := make(map[uint32][]uint32, nodesSize)
	currentSuccessorMap := make(map[uint32][]uint32, nodesSize)

	b.Run("Update(), 10% removed successors", func(b *testing.B) {

		// prepare the graph changes
		for nodeID := uint32(0); nodeID < uint32(nodesSize); nodeID++ {

			oldSuccessorIDs, _ := DB.NodeSuccessorIDs(nodeID)
			currentSuccessorIDs := make([]uint32, len(oldSuccessorIDs)-edgesPerNode/10)

			// remove 10% of the nodes
			copy(currentSuccessorIDs, oldSuccessorIDs[edgesPerNode/10:])

			oldSuccessorMap[nodeID] = oldSuccessorIDs
			currentSuccessorMap[nodeID] = currentSuccessorIDs
		}

		b.ResetTimer()

		// perform benchmark
		for i := 0; i < b.N; i++ {

			nodeID := uint32(i % nodesSize)
			oldSuccessorIDs := oldSuccessorMap[nodeID]
			currentSuccessorIDs := currentSuccessorMap[nodeID]

			err := RWM.Update(DB, nodeID, oldSuccessorIDs, currentSuccessorIDs)
			if err != nil {
				b.Fatalf("Update() failed: %v", err)
			}

		}
	})
}
