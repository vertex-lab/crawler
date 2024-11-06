package walks

import (
	"math/rand"
	"reflect"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

// REFACTOR
func TestDifference(t *testing.T) {

	t.Run("empty slices", func(t *testing.T) {

		slice1 := []uint32{}
		slice2 := []uint32{}

		difference := Difference(slice1, slice2)

		if !reflect.DeepEqual(difference, []uint32{}) {
			t.Errorf("Partition(): expected [], got %v", difference)
		}
	})

	t.Run("normal", func(t *testing.T) {

		slice1 := []uint32{0, 1, 2, 4}
		slice2 := []uint32{1, 2, 3}

		difference := Difference(slice1, slice2)

		if !reflect.DeepEqual(difference, []uint32{0, 4}) {
			t.Errorf("Partition(): expected [], got %v", difference)
		}
	})
}

// REFACTOR
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

func TestTrimCycles(t *testing.T) {

	testCases := []struct {
		name         string
		oldWalk      []uint32
		newWalk      []uint32
		expectedWalk []uint32
	}{
		{
			name:         "empty slices",
			oldWalk:      []uint32{},
			newWalk:      []uint32{},
			expectedWalk: []uint32{},
		},
		{
			name:         "immediate cycle",
			oldWalk:      []uint32{0, 1, 2, 3},
			newWalk:      []uint32{3, 4, 5, 6},
			expectedWalk: []uint32{},
		},
		{
			name:         "cycle",
			oldWalk:      []uint32{0, 1, 2, 3},
			newWalk:      []uint32{4, 2, 6},
			expectedWalk: []uint32{4},
		},
		{
			name:         "no cycle",
			oldWalk:      []uint32{0, 1, 2},
			newWalk:      []uint32{3, 4, 5},
			expectedWalk: []uint32{3, 4, 5},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			oldWalkCopy := make([]uint32, len(test.oldWalk))
			newWalkCopy := make([]uint32, len(test.newWalk))
			copy(oldWalkCopy, test.oldWalk)
			copy(newWalkCopy, test.newWalk)

			walk := TrimCycles(test.oldWalk, test.newWalk)
			if !reflect.DeepEqual(walk, test.expectedWalk) {
				t.Fatalf("TrimCycles(): expected %v, got %v", test.expectedWalk, walk)
			}

			// test the oldWalk and newWalk haven't changed
			if !reflect.DeepEqual(oldWalkCopy, test.oldWalk) {
				t.Errorf("TrimCycles(): oldWalk changed from %v to %v", oldWalkCopy, test.oldWalk)
			}

			if !reflect.DeepEqual(newWalkCopy, test.newWalk) {
				t.Errorf("TrimCycles(): newWalk changed from %v to %v", newWalkCopy, test.newWalk)
			}
		})
	}
}

func TestDeleteCyclesInPlace(t *testing.T) {

	testCases := []struct {
		name         string
		oldWalk      []uint32
		newWalk      []uint32
		expectedWalk []uint32
	}{
		{
			name:         "empty slices",
			oldWalk:      []uint32{},
			newWalk:      []uint32{},
			expectedWalk: []uint32{},
		},
		{
			name:         "immediate cycle",
			oldWalk:      []uint32{0, 1, 2, 3},
			newWalk:      []uint32{3, 4, 5, 6},
			expectedWalk: []uint32{},
		},
		{
			name:         "cycle",
			oldWalk:      []uint32{0, 1, 2, 3},
			newWalk:      []uint32{4, 2, 6},
			expectedWalk: []uint32{4},
		},
		{
			name:         "no cycle",
			oldWalk:      []uint32{0, 1, 2},
			newWalk:      []uint32{3, 4, 5},
			expectedWalk: []uint32{3, 4, 5},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			oldWalkCopy := make([]uint32, len(test.oldWalk))
			copy(oldWalkCopy, test.oldWalk)

			walk := DeleteCyclesInPlace(test.oldWalk, test.newWalk)
			if !reflect.DeepEqual(walk, test.expectedWalk) {
				t.Fatalf("DeleteCyclesInPlace(): expected %v, got %v", test.expectedWalk, walk)
			}

			// test the oldWalk hasn't changed
			if !reflect.DeepEqual(oldWalkCopy, test.oldWalk) {
				t.Errorf("DeleteCyclesInPlace(): oldWalk changed from %v to %v", oldWalkCopy, test.oldWalk)
			}

			/*
				test.newWalk should be the same as walk, but can have a different
				lenght. Example: test.newWalk = [4,0,0,0]; walk = [4]
				this is expected (0 is the default value in Go)
			*/
			for i, el := range walk {
				if el != test.newWalk[i] {
					t.Errorf("DeleteCyclesInPlace(): walk: %v test.newWalk %v", walk, test.newWalk)
				}
			}

			for _, el := range test.newWalk[len(walk):] {
				if el != 0 {
					t.Errorf("DeleteCyclesInPlace(): walk: %v test.newWalk %v", walk, test.newWalk)
				}
			}
		})
	}
}

func TestSortWalks(t *testing.T) {
	testCases := []struct {
		name                string
		walks               []models.RandomWalk
		expectedSortedWalks []models.RandomWalk
	}{
		{
			name:                "empty walks",
			walks:               []models.RandomWalk{},
			expectedSortedWalks: []models.RandomWalk{},
		},
		{
			name:                "normal walks",
			walks:               []models.RandomWalk{{0, 1}, {2, 0}, {1, 2}},
			expectedSortedWalks: []models.RandomWalk{{0, 1}, {1, 2}, {2, 0}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			sortedWalks := SortWalks(test.walks)
			if !reflect.DeepEqual(sortedWalks, test.expectedSortedWalks) {
				t.Errorf("SortWalks(): expected %v, got %v", test.expectedSortedWalks, sortedWalks)
			}
		})
	}
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

func BenchmarkDifference(b *testing.B) {

	size := int32(1000)
	slice1 := make([]uint32, size)
	slice2 := make([]uint32, size)

	// setup up the two slices
	for i := int32(0); i < size; i++ {

		element1 := uint32(rand.Int31n(size * 2))
		element2 := uint32(rand.Int31n(size * 2))

		slice1 = append(slice1, element1)
		slice2 = append(slice2, element2)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Difference(slice1, slice2)
	}
}

func BenchmarkSetDifference(b *testing.B) {

	size := int32(1000)
	set1 := mapset.NewSetWithSize[uint32](int(size))
	set2 := mapset.NewSetWithSize[uint32](int(size))

	// setup up the two sets
	for i := int32(0); i < size; i++ {

		element1 := uint32(rand.Int31n(size * 2))
		element2 := uint32(rand.Int31n(size * 2))

		set1.Add(element1)
		set2.Add(element2)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		set1.Difference(set2)
	}
}

func BenchmarkSetToSlice(b *testing.B) {

	size := int32(1000)
	set := mapset.NewSetWithSize[uint32](int(size))

	for i := int32(0); i < size; i++ {
		element := uint32(rand.Int31n(size * 2))
		set.Add(element)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		set.ToSlice()
	}
}
