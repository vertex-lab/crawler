package pagerank

import (
	"errors"
	"reflect"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
)

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

		_, err := NewRandomWalksManager(0.85, 1)
		if err != nil {
			t.Errorf("NewRandomWalksManager(): expected nil, got %v", err)
		}
	})
}

func TestIsEmpty(t *testing.T) {

	t.Run("negative IsEmpty, nil RWM", func(t *testing.T) {

		var RWM *RandomWalksManager
		_, err := RWM.IsEmpty()

		if !errors.Is(err, ErrNilRWMPointer) {
			t.Errorf("IsEmpty(): expected %v, got %v", ErrNilRWMPointer, err)
		}

	})

	t.Run("negative IsEmpty, empty RWM", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		empty, err := RWM.IsEmpty()

		if err != nil {
			t.Fatalf("IsEmpty(): expected nil, got %v", err)
		}

		if !empty {
			t.Errorf("IsEmpty(): expected %v, got %v", true, empty)
		}
	})

	t.Run("positive IsEmpty", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)

		// create one walk
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		// include it in a list of pointers
		walks := mapset.NewSet(&walk)

		// add it to the map
		RWM.WalksByNode[1] = walks
		empty, err := RWM.IsEmpty()

		if err != nil {
			t.Errorf("IsEmpty(): expected nil, got %v", err)
		}

		if empty {
			t.Errorf("IsEmpty(): expected %v, got %v", false, empty)
		}
	})
}

func TestCheckState(t *testing.T) {

	t.Run("negative CheckState, nil RWM", func(t *testing.T) {

		var RWM *RandomWalksManager
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
		rw := &RandomWalk{NodeIDs: []uint32{0, 1}}
		RWM.WalksByNode[0] = mapset.NewSet[*RandomWalk](rw)

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
		RWM.WalksByNode[0] = mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0, 1}})

		expectEmptyRWM := false
		err := RWM.CheckState(expectEmptyRWM)

		if err != nil {
			t.Fatalf("CheckState(): expected nil, got %v", err)
		}
	})
}

func TestWalksByNodeID(t *testing.T) {

	t.Run("negative WalksByNodeID, nil RWM", func(t *testing.T) {

		var RWM *RandomWalksManager
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
		walk := RandomWalk{NodeIDs: []uint32{}}   // create empty walk
		RWM.WalksByNode[0] = mapset.NewSet(&walk) // add it to the RWM

		got, err := RWM.WalksByNodeID(1)

		if !errors.Is(err, ErrNodeNotFoundRWM) {
			t.Errorf("WalksByNodeID(1): expected %v, got %v", ErrNodeNotFoundRWM, err)
		}

		if got != nil {
			t.Fatalf("WalksByNodeID(1): expected nil got %v", got)
		}
	})

	t.Run("positive WalksByNodeID", func(t *testing.T) {

		RWM, err := NewRandomWalksManager(0.85, 1)
		if err != nil {
			t.Errorf("WalksByNodeID(): expected nil, got %v", err)
		}

		// create one walk
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		// include it in a list of pointers
		walks := mapset.NewSet(&walk)

		// add it to the map
		RWM.WalksByNode[1] = walks
		RWM.WalksByNode[2] = walks

		// get back the list of pointers
		got, err := RWM.WalksByNodeID(1)

		if err != nil {
			t.Errorf("WalksByNodeID(1): expected no error, got %v", err)
		}

		if got == nil {
			t.Fatal("WalksByNodeID(1): expected pointer to {1,2}, got nil")
		}

		if !reflect.DeepEqual(got, walks) {
			t.Errorf("WalksByNodeID(1): expected %v, got %v", walks, got)
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

		if err != ErrNilWalkPointer {
			t.Fatalf("AddWalk(nil): expected %v, got %v", ErrNilWalkPointer, err)
		}
	})

	t.Run("negative AddWalk, empty walk", func(t *testing.T) {
		RWM, _ := NewRandomWalksManager(0.85, 1)
		emptyWalk := RandomWalk{NodeIDs: []uint32{}}

		err := RWM.AddWalk(&emptyWalk)

		if err != ErrEmptyWalk {
			t.Fatalf("AddWalk({}}): expected %v, got %v", ErrEmptyWalk, err)
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

/*
func TestRemoveWalks(t *testing.T) {
	t.Run("negative RemoveWalks, nil RWM", func(t *testing.T) {

		var RWM *RandomWalksManager // nil RWM
		WTR := NewWalksToRemoveByNode(mapset.NewSet[uint32]())

		err := RWM.RemoveWalks(WTR)

		if err != ErrNilRWMPointer {
			t.Fatalf("RemoveWalks(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative RemoveWalks, empty RWM", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1) // empty RWM
		WTR := NewWalksToRemoveByNode(mapset.NewSet[uint32]())

		err := RWM.RemoveWalks(WTR)

		if err != ErrEmptyRWM {
			t.Fatalf("RemoveWalks(): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("negative RemoveWalks, nil WTR", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		RWM.WalksByNode[0] = []*RandomWalk{{NodeIDs: []uint32{0, 1}}}

		var WTR *WalksToRemoveByNode // nil WTR
		err := RWM.RemoveWalks(WTR)

		if err != ErrNilWTRPointer {
			t.Fatalf("RemoveWalks(): expected %v, got %v", ErrNilWTRPointer, err)
		}
	})

	t.Run("positive RemoveWalks", func(t *testing.T) {

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walk := &RandomWalk{NodeIDs: []uint32{0, 1, 2, 3}}
		RWM.AddWalk(walk)

		WTR := NewWalksToRemoveByNode(mapset.NewSet[uint32](0))
		WTR.recordWalkRemoval(walk, 1) // should remove the walk from 1,2,3
		err := RWM.RemoveWalks(WTR)

		if err != nil {
			t.Fatalf("RemoveWalks(): expected nil, got %v", err)
		}

		// check if the walk is present under node 0
		walks, err := RWM.WalksByNodeID(0)
		if err != nil {
			t.Fatalf("RemoveWalks() --> WalksByNodeID(0): expected nil, got %v", err)
		}

		if walks[0] != walk {
			t.Errorf("RemoveWalks(): expected %v, got %v", walk, walks[0])
		}

		// check if the walk is NOT present under node 1,2,3
		for _, nodeID := range walk.NodeIDs[1:] {

			walks, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("RemoveWalks() --> WalksByNodeID(%d): expected nil, got %v", nodeID, err)
			}

			if len(walks) != 0 {
				t.Errorf("RemoveWalks(): expected empty list, got %v", walks)
			}
		}
	})
}
*/

// ------------------------------BENCHMARKS------------------------------

func BenchmarkAddWalk(b *testing.B) {
	RWM, _ := NewRandomWalksManager(0.85, 1)

	// Pre-create a RandomWalk
	nodeIDS := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	walk := RandomWalk{NodeIDs: nodeIDS}

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		RWM.AddWalk(&walk)
	}
}

/*
func BenchmarkRemoveWalks(b *testing.B) {

	// variables of the benchmark
	numNodes := uint32(200000)
	numSuccessorsPerNode := uint32(100)

	// initialization of structures
	DB := generateMockDB(numNodes, numSuccessorsPerNode)
	RWM, _ := NewRandomWalksManager(0.85, 10)
	RWM.GenerateRandomWalks(DB)

	nodesSet := mapset.NewSet[uint32]()
	for i := uint32(0); i < uint32(numNodes); i++ {
		nodesSet.Add(i)
	}

	b.ResetTimer()

	WTR := NewWalksToRemoveByNode(nodesSet)

	// include in WTR all the walks in RWM, so everything will be removed
	for node, walks := range RWM.WalksByNode {
		for _, walk := range walks {
			WTR.removals[node][walk] = 1
		}
	}

	err := RWM.RemoveWalks(WTR)
	if err != nil {
		b.Errorf("RemoveWalks(): Error occurred: %v", err)
	}
}

func BenchmarkToSlice(b *testing.B) {
	size := 1000000
	set := mapset.NewSet[int]()

	for i := 0; i < size; i++ {
		set.Add(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		set.ToSlice()
	}
}
*/
