package walks

import (
	"math/rand"
	"reflect"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
)

func TestPartition(t *testing.T) {

	t.Run("empty slices", func(t *testing.T) {

		oldSlice := []uint32{}
		newSlice := []uint32{}

		removed, common, added := Partition(oldSlice, newSlice)

		if removed == nil || len(removed) > 0 {
			t.Errorf("Partition(): expected [], got %v", removed)
		}

		if common == nil || len(common) > 0 {
			t.Errorf("Partition(): expected [], got %v", common)
		}

		if added == nil || len(added) > 0 {
			t.Errorf("Partition(): expected [], got %v", added)
		}
	})

	t.Run("normal", func(t *testing.T) {

		oldSlice := []uint32{0, 1, 2, 4}
		newSlice := []uint32{1, 2, 3}

		removed, common, added := Partition(oldSlice, newSlice)

		if removed[0] != 0 || removed[1] != 4 {
			t.Errorf("expected [0, 4], got %v", removed)
		}

		if !reflect.DeepEqual(common, []uint32{1, 2}) {
			t.Errorf("expected [1, 2], got %v", common)
		}

		if added[0] != 3 {
			t.Errorf("expected 3, got %v", added[3])
		}
	})
}

func TestRemoveCycles(t *testing.T) {

	t.Run("empty slices", func(t *testing.T) {

		oldWalk := []uint32{}
		newWalkSegment := []uint32{}

		got := RemoveCycles(oldWalk, newWalkSegment)

		if got == nil || len(got) > 0 {
			t.Errorf("RemoveCycles(): expected [], got %v", got)
		}
	})

	t.Run("immediate cycle", func(t *testing.T) {

		oldWalk := []uint32{0, 1, 2, 3}
		newWalkSegment := []uint32{3, 4, 5, 6}

		got := RemoveCycles(oldWalk, newWalkSegment)

		if got == nil || len(got) > 0 {
			t.Errorf("RemoveCycles(): expected [], got %v", got)
		}
	})

	t.Run("cycle", func(t *testing.T) {

		oldWalk := []uint32{0, 1, 2, 3}
		newWalkSegment := []uint32{4, 2, 6}

		want := []uint32{4}
		got := RemoveCycles(oldWalk, newWalkSegment)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("RemoveCycles(): expected %v, got %v", want, got)
		}
	})
}

func TestSortWalkSet(t *testing.T) {

	t.Run("empty walkSet", func(t *testing.T) {

		walkSet := mapset.NewSet[*RandomWalk]()
		walks := SortWalkSet(walkSet)

		if !reflect.DeepEqual(walks, [][]uint32{}) {
			t.Errorf("SortWalkSet(): expected %v, got %v", [][]uint32{}, walks)
		}
	})

	t.Run("normal", func(t *testing.T) {

		expected := map[uint32][]uint32{
			0: {0, 1},
			1: {0, 2},
			2: {1, 0},
		}

		rWalk1 := &RandomWalk{NodeIDs: expected[1]}
		rWalk2 := &RandomWalk{NodeIDs: expected[0]}
		rWalk3 := &RandomWalk{NodeIDs: expected[2]}

		walkSet := mapset.NewSet(rWalk1, rWalk2, rWalk3)
		got := SortWalkSet(walkSet)

		for key, val := range expected {
			if !reflect.DeepEqual(got[key], val) {
				t.Errorf("SortWalkSet(): expected %v, got %v", val, got[key])
			}
		}
	})
}

// ---------------------------------BENCHMARKS---------------------------------

func BenchmarkPartition(b *testing.B) {

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
		Partition(oldSlice, newSlice)
	}
}
