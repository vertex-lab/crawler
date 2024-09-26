package pagerank

import (
	"errors"
	"reflect"
	"testing"
)

// ------------------------------TESTS------------------------------
func TestRandomWalks(t *testing.T) {

	t.Run("test CheckEmpty, nil rwm", func(t *testing.T) {

		var random_walks_map *RandomWalksMap
		err := random_walks_map.CheckEmpty()

		if !errors.Is(err, ErrNilRWMPointer) {
			t.Errorf("CheckEmpty(): expected %v, got %v", ErrNilRWMPointer, err)
		}

	})

	t.Run("test CheckEmpty, empty rwm", func(t *testing.T) {

		random_walks_map := NewRandomWalksMap()
		err := random_walks_map.CheckEmpty()

		if !errors.Is(err, ErrEmptyRWM) {
			t.Errorf("CheckEmpty(): expected %v, got %v", ErrEmptyRWM, err)
		}

	})

	t.Run("positive GetWalksByNodeID", func(t *testing.T) {

		random_walks_map := NewRandomWalksMap()

		// create one walk
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		// include it in a list of pointers
		walks := []*RandomWalk{&walk}

		// add it to the map
		random_walks_map.NodeWalkMap[1] = walks
		random_walks_map.NodeWalkMap[2] = walks

		// get back the list of pointers
		got, err := random_walks_map.GetWalksByNodeID(1)

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

	t.Run("negative GetWalksByNodeID, nil rwm", func(t *testing.T) {

		var random_walks_map *RandomWalksMap
		got, err := random_walks_map.GetWalksByNodeID(1)

		if !errors.Is(err, ErrNilRWMPointer) {
			t.Errorf("GetWalksByNodeID(1): expected %v, got %v", ErrNilRWMPointer, err)
		}

		if got != nil {
			t.Fatalf("GetWalksByNodeID(1): expected nil got %v", got)
		}
	})

	t.Run("negative GetWalksByNodeID, empty rwm", func(t *testing.T) {

		random_walks_map := NewRandomWalksMap()
		got, err := random_walks_map.GetWalksByNodeID(1)

		if !errors.Is(err, ErrEmptyRWM) {
			t.Errorf("GetWalksByNodeID(1): expected %v, got %v", ErrEmptyRWM, err)
		}

		if got != nil {
			t.Fatalf("GetWalksByNodeID(1): expected nil got %v", got)
		}
	})

	t.Run("negative GetWalksByNodeID, node not found", func(t *testing.T) {

		// create non empty rwm
		random_walks_map := NewRandomWalksMap()
		walk := RandomWalk{NodeIDs: []uint32{}}                // create empty walk
		random_walks_map.NodeWalkMap[0] = []*RandomWalk{&walk} // add it to the rwm

		got, err := random_walks_map.GetWalksByNodeID(1)

		if !errors.Is(err, ErrNodeNotFound) {
			t.Errorf("GetWalksByNodeID(1): expected %v, got %v", ErrNodeNotFound, err)
		}

		if got != nil {
			t.Fatalf("GetWalksByNodeID(1): expected nil got %v", got)
		}
	})

	t.Run("positive AddWalk", func(t *testing.T) {

		random_walks_map := NewRandomWalksMap()
		walk := RandomWalk{NodeIDs: []uint32{1, 2}}

		// should add the walk to all nodes that are part of it
		random_walks_map.AddWalk(&walk)

		got1, _ := random_walks_map.GetWalksByNodeID(1)
		got2, _ := random_walks_map.GetWalksByNodeID(2)

		if !reflect.DeepEqual(got1, got2) {
			t.Errorf("AddWalk({1,2}): node1 stores %v, node2 stores %v", got1, got2)
		}

		// Compare the first element of the slices instead of the whole slice
		if len(got1) == 0 || got1[0] != &walk {
			t.Errorf("AddWalk({1,2}): expected %v, got %v", &walk, got1)
		}

		t.Run("negative AddWalk, nil rwm", func(t *testing.T) {

			var random_walks_map *RandomWalksMap // nil rwm
			walk := RandomWalk{NodeIDs: []uint32{1, 2}}

			err := random_walks_map.AddWalk(&walk)

			if err != ErrNilRWMPointer {
				t.Fatalf("AddWalk(nil): expected %v, got %v", ErrNilRWMPointer, err)
			}
		})

		t.Run("negative AddWalk, nil walk", func(t *testing.T) {
			random_walks_map := NewRandomWalksMap()

			err := random_walks_map.AddWalk(nil)

			if err != ErrNilWalkPointer {
				t.Fatalf("AddWalk(nil): expected %v, got %v", ErrNilWalkPointer, err)
			}
		})

		t.Run("negative AddWalk, empty walk", func(t *testing.T) {
			random_walks_map := NewRandomWalksMap()
			empty_walk := RandomWalk{NodeIDs: []uint32{}}

			err := random_walks_map.AddWalk(&empty_walk)

			if err != ErrEmptyWalk {
				t.Fatalf("AddWalk({}}): expected %v, got %v", ErrEmptyWalk, err)
			}
		})
	})
}

// ------------------------------BENCHMARKS------------------------------

func BenchmarkAddWalk(b *testing.B) {
	// Set up a RandomWalksMap before the benchmark starts
	random_walks_map := NewRandomWalksMap()

	// Pre-create a RandomWalk
	nodeIDS := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	walk := RandomWalk{NodeIDs: nodeIDS}

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// We use &walk since AddWalk expects a pointer to a RandomWalk
		random_walks_map.AddWalk(&walk)
	}
}
