package pagerank

import (
	"errors"
	"reflect"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
)

//------------------------------RANDOM-WALKS-TESTS------------------------------

func TestCheckEmpty(t *testing.T) {

	t.Run("negative CheckEmpty, nil rw", func(t *testing.T) {

		var rw *RandomWalk // nil rw
		err := rw.CheckEmpty()

		if !errors.Is(err, ErrNilRandomWalkPointer) {
			t.Errorf("CheckEmpty(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative CheckEmpty, empty rw", func(t *testing.T) {

		rw := &RandomWalk{NodeIDs: []uint32{}}
		err := rw.CheckEmpty()

		if !errors.Is(err, ErrEmptyRandomWalk) {
			t.Fatalf("CheckEmpty(): expected %v, got %v", ErrEmptyRandomWalk, err)
		}
	})

	t.Run("positive CheckEmpty", func(t *testing.T) {

		rw := &RandomWalk{NodeIDs: []uint32{0}}
		err := rw.CheckEmpty()

		if err != nil {
			t.Errorf("CheckEmpty(): expected nil, got %v", err)
		}
	})
}

func TestNeedsUpdate(t *testing.T) {

	t.Run("NeedsUpdate, nil rw", func(t *testing.T) {

		nodeID := uint32(0)
		var randomWalk *RandomWalk // nil rw
		removedNodes := mapset.NewSet[uint32](5)

		_, _, err := randomWalk.NeedsUpdate(nodeID, removedNodes)
		if err != ErrNilRandomWalkPointer {
			t.Fatalf("NeedsUpdate(): expected %v, got %v", ErrNilRandomWalkPointer, err)
		}

	})

	t.Run("NeedsUpdate, empty rw", func(t *testing.T) {

		nodeID := uint32(0)
		randomWalk := &RandomWalk{NodeIDs: []uint32{}} // empty rw
		removedNodes := mapset.NewSet[uint32](5)

		_, _, err := randomWalk.NeedsUpdate(nodeID, removedNodes)
		if err != ErrEmptyRandomWalk {
			t.Fatalf("NeedsUpdate(): expected %v, got %v", ErrEmptyRandomWalk, err)
		}

	})

	t.Run("NeedsUpdate, no update", func(t *testing.T) {

		nodeID := uint32(0)
		randomWalk := &RandomWalk{NodeIDs: []uint32{0, 1, 2, 3}}
		removedNodes := mapset.NewSet[uint32](5)

		update, cutIndex, err := randomWalk.NeedsUpdate(nodeID, removedNodes)
		if err != nil {
			t.Fatalf("NeedsUpdate(): expected nil, got %v", err)
		}

		if update != false {
			t.Errorf("NeedsUpdate(): expected %v, got %v", false, update)
		}

		if cutIndex != -1 {
			t.Errorf("NeedsUpdate(): expected %v, got %v", -1, cutIndex)
		}

	})

	t.Run("NeedsUpdate, update", func(t *testing.T) {

		nodeID := uint32(0)
		randomWalk := &RandomWalk{NodeIDs: []uint32{0, 1, 2, 3, 0, 5}}
		removedNodes := mapset.NewSet[uint32](5)

		update, cutIndex, err := randomWalk.NeedsUpdate(nodeID, removedNodes)
		if err != nil {
			t.Fatalf("NeedsUpdate(): expected nil, got %v", err)
		}

		if update != true {
			t.Errorf("NeedsUpdate(): expected %v, got %v", true, update)
		}

		if cutIndex != 5 {
			t.Errorf("NeedsUpdate(): expected %v, got %v", 5, cutIndex)
		}
	})
}

// -------------------------RANDOM-WALKS-MANAGER-TESTS--------------------------

func TestNewRandomWalksManager(t *testing.T) {

	t.Run("negative NewRandomWalksManager, invalid alphas", func(t *testing.T) {

		invalidAlphas := []float32{1.01, 1.0, -0.1, -2}
		for _, alpha := range invalidAlphas {

			RWM, err := NewRandomWalksManager(alpha, 1)

			if !errors.Is(err, ErrInvalidAlpha) {
				t.Errorf("NewRandomWalksManager(%f,1): expected %v, got %v", alpha, ErrInvalidAlpha, err)
			}

			if RWM != nil {
				t.Errorf("NewRandomWalksManager(%f,1): expected nil, got %v", alpha, RWM)
			}
		}
	})

	t.Run("negative NewRandomWalksManager, invalid walkPerNode", func(t *testing.T) {

		walksPerNode := uint16(0) // invalid walksPerNode
		RWM, err := NewRandomWalksManager(0.85, walksPerNode)

		if !errors.Is(err, ErrInvalidWalksPerNode) {
			t.Errorf("NewRandomWalksManager(0.85,0): expected %v, got %v", ErrInvalidWalksPerNode, err)
		}

		if RWM != nil {
			t.Errorf("NewRandomWalksManager(0.85,0): expected nil, got %v", RWM)
		}
	})

	t.Run("positive NewRandomWalksManager", func(t *testing.T) {

		alpha := float32(0.85)
		walksPerNode := uint16(1)

		RWM, err := NewRandomWalksManager(alpha, walksPerNode)
		if err != nil {
			t.Errorf("NewRandomWalksManager(): expected nil, got %v", err)
		}

		if RWM.alpha != alpha {
			t.Errorf("NewRandomWalksManager(): expected %v, got %v", alpha, RWM.alpha)
		}

		if RWM.walksPerNode != walksPerNode {
			t.Errorf("NewRandomWalksManager(): expected %v, got %v", walksPerNode, RWM.walksPerNode)
		}
	})
}

func TestIsEmpty(t *testing.T) {

	t.Run("negative IsEmpty, nil RWM", func(t *testing.T) {

		var RWM *RandomWalksManager // nil RWM
		empty, err := RWM.IsEmpty()

		if !errors.Is(err, ErrNilRWMPointer) {
			t.Errorf("IsEmpty(): expected %v, got %v", ErrNilRWMPointer, err)
		}

		if empty != true {
			t.Errorf("IsEmpty(): expected %v, got %v", true, empty)
		}
	})

	t.Run("IsEmpty, empty RWM", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		empty, err := RWM.IsEmpty()

		if err != nil {
			t.Fatalf("IsEmpty(): expected nil, got %v", err)
		}

		if empty != true {
			t.Errorf("IsEmpty(): expected %v, got %v", true, empty)
		}
	})

	t.Run("IsEmpty, non-empty RWM", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{1, 2}})
		RWM.WalksByNode[1] = walkSet

		empty, err := RWM.IsEmpty()

		if err != nil {
			t.Errorf("IsEmpty(): expected nil, got %v", err)
		}

		if empty != false {
			t.Errorf("IsEmpty(): expected %v, got %v", false, empty)
		}
	})
}

func TestCheckState(t *testing.T) {

	t.Run("negative CheckState, nil RWM", func(t *testing.T) {

		var RWM *RandomWalksManager // nil RWM
		err := RWM.CheckState(true)

		if !errors.Is(err, ErrNilRWMPointer) {
			t.Errorf("CheckState(): expected %v, got %v", ErrNilRWMPointer, err)
		}

	})

	t.Run("negative CheckState, empty RWM, expected non-empty", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		expectEmptyRWM := false
		err := RWM.CheckState(expectEmptyRWM)

		if !errors.Is(err, ErrEmptyRWM) {
			t.Fatalf("CheckState(): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("negative CheckState, non-empty RWM, expected empty", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{1, 2}})
		RWM.WalksByNode[1] = walkSet

		expectEmptyRWM := true
		err := RWM.CheckState(expectEmptyRWM)

		if !errors.Is(err, ErrNonEmptyRWM) {
			t.Fatalf("CheckState(): expected %v, got %v", ErrNonEmptyRWM, err)
		}
	})

	t.Run("positive CheckState, empty RWM, expected empty", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		expectEmptyRWM := true
		err := RWM.CheckState(expectEmptyRWM)

		if err != nil {
			t.Fatalf("CheckState(): expected nil, got %v", err)
		}
	})

	t.Run("positive CheckState, non-empty RWM, expected non-empty", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{1, 2}})
		RWM.WalksByNode[1] = walkSet

		expectEmptyRWM := false
		err := RWM.CheckState(expectEmptyRWM)

		if err != nil {
			t.Fatalf("CheckState(): expected nil, got %v", err)
		}
	})
}

func TestWalksByNodeID(t *testing.T) {

	t.Run("negative WalksByNodeID, nil RWM", func(t *testing.T) {

		var RWM *RandomWalksManager // nil RWM
		got, err := RWM.WalksByNodeID(1)

		if !errors.Is(err, ErrNilRWMPointer) {
			t.Errorf("WalksByNodeID(1): expected %v, got %v", ErrNilRWMPointer, err)
		}

		if got != nil {
			t.Fatalf("WalksByNodeID(1): expected nil got %v", got)
		}
	})

	t.Run("negative WalksByNodeID, empty RWM", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		got, err := RWM.WalksByNodeID(1)

		if !errors.Is(err, ErrEmptyRWM) {
			t.Errorf("WalksByNodeID(1): expected %v, got %v", ErrEmptyRWM, err)
		}

		if got != nil {
			t.Fatalf("WalksByNodeID(1): expected nil got %v", got)
		}
	})

	t.Run("negative WalksByNodeID, node not found", func(t *testing.T) {

		// create non empty RWM
		RWM, _ := NewRandomWalksManager(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{}})
		RWM.WalksByNode[0] = walkSet

		got, err := RWM.WalksByNodeID(1)

		if !errors.Is(err, ErrNodeNotFoundRWM) {
			t.Errorf("WalksByNodeID(1): expected %v, got %v", ErrNodeNotFoundRWM, err)
		}

		if got != nil {
			t.Fatalf("WalksByNodeID(1): expected nil got %v", got)
		}
	})

	t.Run("positive WalksByNodeID", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0, 1}})
		RWM.WalksByNode[0] = walkSet

		// get the walkSet of node0
		got, err := RWM.WalksByNodeID(0)

		if err != nil {
			t.Errorf("WalksByNodeID(): expected no error, got %v", err)
		}

		if got == nil {
			t.Fatal("WalksByNodeID(): expected pointer to {1,2}, got nil")
		}
	})

}

func TestAddWalk(t *testing.T) {
	t.Run("negative AddWalk, nil RWM", func(t *testing.T) {

		var RWM *RandomWalksManager // nil RWM
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		err := RWM.AddWalk(&walk)

		if err != ErrNilRWMPointer {
			t.Fatalf("AddWalk(nil): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative AddWalk, nil walk", func(t *testing.T) {
		RWM, _ := NewRandomWalksManager(0.85, 1)

		err := RWM.AddWalk(nil)

		if err != ErrNilRandomWalkPointer {
			t.Fatalf("AddWalk(nil): expected %v, got %v", ErrNilRandomWalkPointer, err)
		}
	})

	t.Run("negative AddWalk, empty walk", func(t *testing.T) {
		RWM, _ := NewRandomWalksManager(0.85, 1)
		emptyWalk := RandomWalk{NodeIDs: []uint32{}}

		err := RWM.AddWalk(&emptyWalk)

		if err != ErrEmptyRandomWalk {
			t.Fatalf("AddWalk({}}): expected %v, got %v", ErrEmptyRandomWalk, err)
		}
	})

	t.Run("positive AddWalk", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		// should add the walk to all nodes that are part of it
		RWM.AddWalk(&walk)

		got1, _ := RWM.WalksByNodeID(1)
		got2, _ := RWM.WalksByNodeID(2)

		if !got1.Equal(got2) {
			t.Errorf("AddWalk({1,2}): node1 stores %v, node2 stores %v", got1, got2)
		}

		if got1.Cardinality() == 0 || got1.ToSlice()[0] != &walk {
			t.Errorf("AddWalk({1,2}): expected %v, got %v", &walk, got1)
		}
	})

}

func TestPruneWalk(t *testing.T) {
	t.Run("negative PruneWalk, nil RWM", func(t *testing.T) {

		var RWM *RandomWalksManager // nil RWM
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		err := RWM.PruneWalk(&walk, 0)

		if err != ErrNilRWMPointer {
			t.Fatalf("PruneWalk(nil): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative PruneWalk, empty RWM", func(t *testing.T) {
		RWM, _ := NewRandomWalksManager(0.85, 1) // empty RWM

		err := RWM.PruneWalk(&RandomWalk{NodeIDs: []uint32{1, 2}}, 0)

		if err != ErrEmptyRWM {
			t.Fatalf("PruneWalk(nil): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("negative PruneWalk, nil walk", func(t *testing.T) {
		RWM, _ := NewRandomWalksManager(0.85, 1)
		RWM.AddWalk(&RandomWalk{NodeIDs: []uint32{1, 2}})

		err := RWM.PruneWalk(nil, 0) // nil walk

		if err != ErrNilRandomWalkPointer {
			t.Fatalf("PruneWalk(nil): expected %v, got %v", ErrNilRandomWalkPointer, err)
		}
	})

	t.Run("negative PruneWalk, empty walk", func(t *testing.T) {
		RWM, _ := NewRandomWalksManager(0.85, 1)
		RWM.AddWalk(&RandomWalk{NodeIDs: []uint32{1, 2}})

		emptyWalk := RandomWalk{NodeIDs: []uint32{}} // empty walk
		err := RWM.PruneWalk(&emptyWalk, 0)

		if err != ErrEmptyRandomWalk {
			t.Fatalf("PruneWalk({}}): expected %v, got %v", ErrEmptyRandomWalk, err)
		}
	})

	t.Run("negative PruneWalk, invalid cutIndex", func(t *testing.T) {
		RWM, _ := NewRandomWalksManager(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}
		RWM.AddWalk(&walk)

		invalidCutIndexes := []int{-1, -20, 50, 2}

		for _, invalidCutIndex := range invalidCutIndexes {

			err := RWM.PruneWalk(&walk, invalidCutIndex)
			if err != ErrInvalidWalkIndex {
				t.Errorf("PruneWalk({}}): expected %v, got %v", ErrInvalidWalkIndex, err)
			}
		}
	})

	t.Run("positive PruneWalk", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{5, 7}}
		RWM.AddWalk(&walk)

		// should remove the walk from the WalkSet of node 7 but not node 5
		RWM.PruneWalk(&walk, 1)

		walkSet5, _ := RWM.WalksByNodeID(5)
		if !walkSet5.ContainsOne(&walk) {
			t.Errorf("PruneWalk() node5: expected %v, got %v", &walk, walkSet5)
		}

		walkSet7, _ := RWM.WalksByNodeID(7)
		if walkSet7.ContainsOne(&walk) {
			t.Errorf("PruneWalk() node7: expected {}, got %v", walkSet7)
		}
	})

}

func TestGraftWalk(t *testing.T) {
	t.Run("negative GraftWalk, nil RWM", func(t *testing.T) {

		var RWM *RandomWalksManager // nil RWM

		walk := RandomWalk{NodeIDs: []uint32{0}}
		newNodeIDs := []uint32{1, 2}

		err := RWM.GraftWalk(&walk, newNodeIDs)

		if err != ErrNilRWMPointer {
			t.Fatalf("GraftWalk(nil): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative GraftWalk, empty RWM", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)

		walk := RandomWalk{NodeIDs: []uint32{0}}
		newNodeIDs := []uint32{1, 2}

		err := RWM.GraftWalk(&walk, newNodeIDs)

		if err != ErrEmptyRWM {
			t.Fatalf("GraftWalk(nil): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("negative GraftWalk, nil walk", func(t *testing.T) {
		RWM, _ := NewRandomWalksManager(0.85, 1)
		RWM.AddWalk(&RandomWalk{NodeIDs: []uint32{1, 2}})

		err := RWM.GraftWalk(nil, []uint32{0}) // nil walk

		if err != ErrNilRandomWalkPointer {
			t.Fatalf("GraftWalk(nil): expected %v, got %v", ErrNilRandomWalkPointer, err)
		}
	})

	t.Run("negative GraftWalk, empty walk", func(t *testing.T) {
		RWM, _ := NewRandomWalksManager(0.85, 1)
		RWM.AddWalk(&RandomWalk{NodeIDs: []uint32{1, 2}})

		emptyWalk := RandomWalk{NodeIDs: []uint32{}} // empty walk
		err := RWM.GraftWalk(&emptyWalk, []uint32{0})

		if err != ErrEmptyRandomWalk {
			t.Fatalf("GraftWalk({}}): expected %v, got %v", ErrEmptyRandomWalk, err)
		}
	})

	t.Run("positive GraftWalk", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walk := &RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(walk)

		nodeIDs := []uint32{1, 2}
		RWM.GraftWalk(walk, nodeIDs)

		// check walkSet0
		walkSet0, _ := RWM.WalksByNodeID(0)
		if !walkSet0.Equal(mapset.NewSet(walk)) {
			t.Errorf("GraftWalk(): expected {%v}, got %v", walk, walkSet0)
		}

		// check walkSet1
		walkSet1, _ := RWM.WalksByNodeID(1)
		if !walkSet1.Equal(mapset.NewSet(walk)) {
			t.Errorf("GraftWalk(): expected {%v}, got %v", walk, walkSet1)
		}

		// check walkSet2
		walkSet2, _ := RWM.WalksByNodeID(2)
		if !walkSet2.Equal(mapset.NewSet(walk)) {
			t.Errorf("GraftWalk(): expected {%v}, got %v", walk, walkSet1)
		}

		// check the walk
		if !reflect.DeepEqual(walk.NodeIDs, []uint32{0, 1, 2}) {
			t.Errorf("GraftWalk(): expected %v, got %v", []uint32{0, 1, 2}, walk.NodeIDs)
		}
	})

}

// ------------------------------BENCHMARKS------------------------------

func BenchmarkAddWalk(b *testing.B) {
	RWM, err := NewRandomWalksManager(0.85, 1)
	if err != nil {
		b.Fatalf("BenchmarkAddWalk(): failed to initialize RWM: %v", err)
	}

	// setup the walks
	walkPointers := []*RandomWalk{}
	for i := uint32(0); i < uint32(b.N); i++ {

		// simple walk pattern
		nodeIDs := []uint32{i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6}
		walk := &RandomWalk{NodeIDs: nodeIDs}
		walkPointers = append(walkPointers, walk)
	}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		err := RWM.AddWalk(walkPointers[i])

		if err != nil {
			b.Fatalf("BenchmarkAddWalk(): expected nil, got %v", err)
		}
	}
}

func BenchmarkPruneWalk(b *testing.B) {
	RWM, err := NewRandomWalksManager(0.85, 1)
	if err != nil {
		b.Fatalf("Failed to initialize RandomWalksManager: %v", err)
	}

	// setup the walks and RWM
	walkPointers := []*RandomWalk{}
	for i := uint32(0); i < uint32(b.N); i++ {

		// simple walk pattern
		nodeIDs := []uint32{i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6}
		walk := &RandomWalk{NodeIDs: nodeIDs}

		walkPointers = append(walkPointers, walk)
		RWM.AddWalk(walk)
	}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		RWM.PruneWalk(walkPointers[i], 0)

		if err != nil {
			b.Fatalf("BenchmarkAddWalk(): expected nil, got %v", err)
		}
	}
}

func BenchmarkGraftWalk(b *testing.B) {
	RWM, err := NewRandomWalksManager(0.85, 1)
	if err != nil {
		b.Fatalf("BenchmarkAddWalk(): failed to initialize RWM: %v", err)
	}

	walk := &RandomWalk{NodeIDs: []uint32{0}}
	RWM.AddWalk(walk)

	// setup the walk segments
	walkSegments := [][]uint32{}
	for i := uint32(0); i < uint32(b.N); i++ {

		// simple walk pattern
		walkSegment := []uint32{i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6}
		walkSegments = append(walkSegments, walkSegment)
	}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		err := RWM.GraftWalk(walk, walkSegments[i])

		if err != nil {
			b.Fatalf("BenchmarkAddWalk(): expected nil, got %v", err)
		}
	}
}
