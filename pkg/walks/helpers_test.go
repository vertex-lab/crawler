package walks

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
)

func TestDifferences(t *testing.T) {

	t.Run("positive, Differences", func(t *testing.T) {

		oldSlice := []uint32{0, 1, 2, 4}
		newSlice := []uint32{1, 2, 3}

		removed, added := Differences(oldSlice, newSlice)

		if removed[0] != 0 || removed[1] != 4 {
			t.Errorf("expected [0, 4], got %v", removed)
		}

		if added[0] != 3 {
			t.Errorf("expected 3, got %v", added[3])
		}
	})

	t.Run("Differences, empty slices", func(t *testing.T) {

		oldSlice := []uint32{}
		newSlice := []uint32{}

		removed, added := Differences(oldSlice, newSlice)

		if removed == nil || len(removed) > 0 {
			t.Errorf("Differences(): expected [], got %v", removed)
		}

		if added == nil || len(added) > 0 {
			t.Errorf("Differences(): expected [], got %v", added)
		}
	})
}

func TestRemoveCycles(t *testing.T) {

	t.Run("removeCycles, empty slices", func(t *testing.T) {

		oldWalk := []uint32{}
		newWalkSegment := []uint32{}

		got := removeCycles(oldWalk, newWalkSegment)

		if got == nil || len(got) > 0 {
			t.Errorf("removeCycles(): expected [], got %v", got)
		}
	})

	t.Run("removeCycles, immediate cycle", func(t *testing.T) {

		oldWalk := []uint32{0, 1, 2, 3}
		newWalkSegment := []uint32{3, 4, 5, 6}

		got := removeCycles(oldWalk, newWalkSegment)

		if got == nil || len(got) > 0 {
			t.Errorf("removeCycles(): expected [], got %v", got)
		}
	})

	t.Run("removeCycles, cycle", func(t *testing.T) {

		oldWalk := []uint32{0, 1, 2, 3}
		newWalkSegment := []uint32{4, 2, 6}

		want := []uint32{4}
		got := removeCycles(oldWalk, newWalkSegment)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("removeCycles(): expected %v, got %v", want, got)
		}
	})
}

func TestSortWalks(t *testing.T) {

	t.Run("sortWalks, empty walkSet", func(t *testing.T) {

		walkSet := mapset.NewSet[*RandomWalk]()
		_, err := sortWalks(walkSet)

		if !errors.Is(err, ErrEmptyRandomWalk) {
			t.Errorf("sortWalks(): expected %v, got %v", ErrEmptyRandomWalk, err)
		}
	})

	t.Run("sortWalks, empty walkSet", func(t *testing.T) {

		expected := map[uint32][]uint32{
			0: {0, 1},
			1: {0, 2},
			2: {1, 0},
		}

		rWalk1 := &RandomWalk{NodeIDs: expected[1]}
		rWalk2 := &RandomWalk{NodeIDs: expected[0]}
		rWalk3 := &RandomWalk{NodeIDs: expected[2]}

		walkSet := mapset.NewSet[*RandomWalk](rWalk1, rWalk2, rWalk3)
		got, err := sortWalks(walkSet)

		if err != nil {
			t.Errorf("sortWalks(): expected nil, got %v", err)
		}

		for key, val := range expected {
			if !reflect.DeepEqual(got[key], val) {
				t.Errorf("sortWalks(): expected %v, got %v", val, got[key])
			}
		}

	})
}

// ---------------------------------BENCHMARKS---------------------------------

func BenchmarkDifferences(b *testing.B) {

	size := int32(1000)

	oldSlice := make([]uint32, size)
	newSlice := make([]uint32, size)

	// setup old and current IDs
	for i := int32(0); i < size; i++ {

		old := uint32(rand.Int31n(size * 2))
		new := uint32(rand.Int31n(size * 2))

		oldSlice = append(oldSlice, old)
		newSlice = append(newSlice, new)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Differences(oldSlice, newSlice)
	}
}
