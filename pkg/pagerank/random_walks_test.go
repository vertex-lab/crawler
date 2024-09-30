package pagerank

import (
	"errors"
	"reflect"
	"testing"
)

// ------------------------------TESTS------------------------------
func TestRandomWalks(t *testing.T) {

	t.Run("negative NewRandomWalksMap, invalid alphas", func(t *testing.T) {

		invalidAlphas := []float32{1.01, 1.0, -0.1, -2}

		for _, alpha := range invalidAlphas {

			randomWalksMap, err := NewRandomWalksMap(alpha, 1)

			if !errors.Is(err, ErrInvalidAlpha) {
				t.Errorf("NewRandomWalksMap(%f,1): expected %v, got %v", alpha, ErrInvalidAlpha, err)
			}

			if randomWalksMap != nil {
				t.Errorf("NewRandomWalksMap(%f,1): expected nil, got %v", alpha, randomWalksMap)
			}
		}
	})

	t.Run("negative NewRandomWalksMap, invalid walkPerNode", func(t *testing.T) {

		walksPerNode := uint16(0) // invalid walksPerNode
		randomWalksMap, err := NewRandomWalksMap(0.85, walksPerNode)

		if !errors.Is(err, ErrInvalidWalksPerNode) {
			t.Errorf("NewRandomWalksMap(0.85,0): expected %v, got %v", ErrInvalidWalksPerNode, err)
		}

		if randomWalksMap != nil {
			t.Errorf("NewRandomWalksMap(0.85,0): expected nil, got %v", randomWalksMap)
		}
	})

	t.Run("positive NewRandomWalksMap", func(t *testing.T) {

		_, err := NewRandomWalksMap(0.85, 1)
		if err != nil {
			t.Errorf("NewRandomWalksMap(): expected nil, got %v", err)
		}

	})

	t.Run("negative CheckEmpty, nil rwm", func(t *testing.T) {

		var randomWalksMap *RandomWalksMap
		err := randomWalksMap.CheckEmpty()

		if !errors.Is(err, ErrNilRWMPointer) {
			t.Errorf("CheckEmpty(): expected %v, got %v", ErrNilRWMPointer, err)
		}

	})

	t.Run("negative CheckEmpty, empty rwm", func(t *testing.T) {

		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		err := randomWalksMap.CheckEmpty()

		if !errors.Is(err, ErrEmptyRWM) {
			t.Errorf("CheckEmpty(): expected %v, got %v", ErrEmptyRWM, err)
		}

	})

	t.Run("positive CheckEmpty", func(t *testing.T) {

		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)

		// create one walk
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		// include it in a list of pointers
		walks := []*RandomWalk{&walk}

		// add it to the map
		randomWalksMap.NodeWalkMap[1] = walks
		err := randomWalksMap.CheckEmpty()

		if err != nil {
			t.Errorf("CheckEmpty(): expected nil, got %v", err)
		}
	})

	t.Run("negative GetWalksByNodeID, nil rwm", func(t *testing.T) {

		var randomWalksMap *RandomWalksMap
		got, err := randomWalksMap.GetWalksByNodeID(1)

		if !errors.Is(err, ErrNilRWMPointer) {
			t.Errorf("GetWalksByNodeID(1): expected %v, got %v", ErrNilRWMPointer, err)
		}

		if got != nil {
			t.Fatalf("GetWalksByNodeID(1): expected nil got %v", got)
		}
	})

	t.Run("negative GetWalksByNodeID, empty rwm", func(t *testing.T) {

		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		got, err := randomWalksMap.GetWalksByNodeID(1)

		if !errors.Is(err, ErrEmptyRWM) {
			t.Errorf("GetWalksByNodeID(1): expected %v, got %v", ErrEmptyRWM, err)
		}

		if got != nil {
			t.Fatalf("GetWalksByNodeID(1): expected nil got %v", got)
		}
	})

	t.Run("negative GetWalksByNodeID, node not found", func(t *testing.T) {

		// create non empty rwm
		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{}}              // create empty walk
		randomWalksMap.NodeWalkMap[0] = []*RandomWalk{&walk} // add it to the rwm

		got, err := randomWalksMap.GetWalksByNodeID(1)

		if !errors.Is(err, ErrNodeNotFound) {
			t.Errorf("GetWalksByNodeID(1): expected %v, got %v", ErrNodeNotFound, err)
		}

		if got != nil {
			t.Fatalf("GetWalksByNodeID(1): expected nil got %v", got)
		}
	})

	t.Run("positive GetWalksByNodeID", func(t *testing.T) {

		randomWalksMap, err := NewRandomWalksMap(0.85, 1)
		if err != nil {
			t.Errorf("GetWalksByNodeID(): expected nil, got %v", err)
		}

		// create one walk
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		// include it in a list of pointers
		walks := []*RandomWalk{&walk}

		// add it to the map
		randomWalksMap.NodeWalkMap[1] = walks
		randomWalksMap.NodeWalkMap[2] = walks

		// get back the list of pointers
		got, err := randomWalksMap.GetWalksByNodeID(1)

		if err != nil {
			t.Errorf("GetWalksByNodeID(1): expected no error, got %v", err)
		}

		if got == nil {
			t.Fatal("GetWalksByNodeID(1): expected pointer to {1,2}, got nil")
		}

		if !reflect.DeepEqual(got, walks) {
			t.Errorf("GetWalksByNodeID(1): expected %v, got %v", walks, got)
		}
	})
}

func TestAddWalk(t *testing.T) {
	t.Run("negative AddWalk, nil rwm", func(t *testing.T) {

		var randomWalksMap *RandomWalksMap // nil rwm
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		err := randomWalksMap.AddWalk(&walk)

		if err != ErrNilRWMPointer {
			t.Fatalf("AddWalk(nil): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative AddWalk, nil walk", func(t *testing.T) {
		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)

		err := randomWalksMap.AddWalk(nil)

		if err != ErrNilWalkPointer {
			t.Fatalf("AddWalk(nil): expected %v, got %v", ErrNilWalkPointer, err)
		}
	})

	t.Run("negative AddWalk, empty walk", func(t *testing.T) {
		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		emptyWalk := RandomWalk{NodeIDs: []uint32{}}

		err := randomWalksMap.AddWalk(&emptyWalk)

		if err != ErrEmptyWalk {
			t.Fatalf("AddWalk({}}): expected %v, got %v", ErrEmptyWalk, err)
		}
	})

	t.Run("positive AddWalk", func(t *testing.T) {

		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		// should add the walk to all nodes that are part of it
		randomWalksMap.AddWalk(&walk)

		got1, _ := randomWalksMap.GetWalksByNodeID(1)
		got2, _ := randomWalksMap.GetWalksByNodeID(2)

		if !reflect.DeepEqual(got1, got2) {
			t.Errorf("AddWalk({1,2}): node1 stores %v, node2 stores %v", got1, got2)
		}

		// Compare the first element of the slices instead of the whole slice
		if len(got1) == 0 || got1[0] != &walk {
			t.Errorf("AddWalk({1,2}): expected %v, got %v", &walk, got1)
		}
	})
}

func TestPruneWalk(t *testing.T) {

	t.Run("negative PruneWalk, nil rwm", func(t *testing.T) {

		var randomWalksMap *RandomWalksMap // nil rwm
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		err := randomWalksMap.PruneWalk(0, &walk)

		if err != ErrNilRWMPointer {
			t.Fatalf("PruneWalk(nil): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative PruneWalk, empty rwm", func(t *testing.T) {
		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		err := randomWalksMap.PruneWalk(0, &walk)

		if err != ErrEmptyRWM {
			t.Fatalf("PruneWalk(nil): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("negative PruneWalk, nil walk", func(t *testing.T) {
		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		randomWalksMap.AddWalk(&RandomWalk{NodeIDs: []uint32{1, 2}})

		err := randomWalksMap.PruneWalk(0, nil)

		if err != ErrNilWalkPointer {
			t.Fatalf("PruneWalk(nil): expected %v, got %v", ErrNilWalkPointer, err)
		}
	})

	t.Run("negative PruneWalk, empty walk", func(t *testing.T) {
		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		randomWalksMap.AddWalk(&RandomWalk{NodeIDs: []uint32{1, 2}})

		emptyWalk := RandomWalk{NodeIDs: []uint32{}}

		err := randomWalksMap.PruneWalk(0, &emptyWalk)

		if err != ErrEmptyWalk {
			t.Fatalf("PruneWalk({}}): expected %v, got %v", ErrEmptyWalk, err)
		}
	})

	t.Run("positive PruneWalk, ", func(t *testing.T) {

		randomWalksMap, _ := NewRandomWalksMap(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		randomWalksMap.AddWalk(&walk)

		// should add the walk to all nodes that are part of it
		randomWalksMap.PruneWalk(0, &walk)

		got1, _ := randomWalksMap.GetWalksByNodeID(1)
		got2, _ := randomWalksMap.GetWalksByNodeID(2)
		want := []*RandomWalk{}

		if !reflect.DeepEqual(got1, want) {
			t.Errorf("PruneWalk(): expected %v, got %v", want, got1)
		}

		if !reflect.DeepEqual(got2, want) {
			t.Errorf("PruneWalk(): expected %v, got %v", want, got2)
		}
	})

	// ADD ANOTHER POSITIVE TEST FOR PruneWalk
}

// ------------------------------BENCHMARKS------------------------------

func BenchmarkAddWalk(b *testing.B) {
	randomWalksMap, _ := NewRandomWalksMap(0.85, 1)

	// Pre-create a RandomWalk
	nodeIDS := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	walk := RandomWalk{NodeIDs: nodeIDS}

	// Run the benchmark
	for i := 0; i < b.N; i++ {

		randomWalksMap.AddWalk(&walk)
	}
}

func BenchmarkPruneWalk(b *testing.B) {
	randomWalksMap, _ := NewRandomWalksMap(0.85, 1)

	// Pre-create a RandomWalk
	nodeIDS := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	walk := RandomWalk{NodeIDs: nodeIDS}

	walksToRemove := []*RandomWalk{} // a slice of walks to remove

	for i := 0; i < 1000; i++ {

		// make a copy of walk
		walkCopy := walk
		walkCopyPointer := &walkCopy

		// add it to the rwm
		randomWalksMap.AddWalk(walkCopyPointer)

		// add it to the walksToRemove
		walksToRemove = append(walksToRemove, walkCopyPointer)
	}

	b.ResetTimer()
	// Run the benchmark
	for _, walkToRemove := range walksToRemove {
		err := randomWalksMap.PruneWalk(0, walkToRemove)

		if err != nil {
			b.Fatalf("PruneWalk() failed: %v", err)
		}
	}
}
