package sliceutils

import (
	"math/rand"
	"reflect"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/vertex-lab/crawler/pkg/models"
)

func TestEqualElements(t *testing.T) {
	testCases := []struct {
		name          string
		slice1        []uint32
		slice2        []uint32
		expectedEqual bool
	}{
		{
			name:          "both nil",
			slice1:        nil,
			slice2:        []uint32{},
			expectedEqual: true,
		},
		{
			name:          "both empty",
			slice1:        []uint32{},
			slice2:        []uint32{},
			expectedEqual: true,
		},
		{
			name:          "one nil, one empty",
			slice1:        nil,
			slice2:        []uint32{},
			expectedEqual: true,
		},
		{
			name:          "same elements",
			slice1:        []uint32{0, 2, 1},
			slice2:        []uint32{1, 0, 2},
			expectedEqual: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			equal := EqualElements(test.slice1, test.slice2)
			if equal != test.expectedEqual {
				t.Fatalf("EqualElements(): expected %v, got %v", test.expectedEqual, equal)
			}
		})
	}
}

func TestDifference(t *testing.T) {
	testCases := []struct {
		name          string
		slice1        []uint32
		slice2        []uint32
		expectedSlice []uint32
	}{
		{
			name:          "empty slices",
			slice1:        []uint32{},
			slice2:        []uint32{},
			expectedSlice: []uint32{},
		},
		{
			name:          "normal",
			slice1:        []uint32{0, 1, 2, 4},
			slice2:        []uint32{1, 2, 3},
			expectedSlice: []uint32{0, 4},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			diff := Difference(test.slice1, test.slice2)
			if !reflect.DeepEqual(diff, test.expectedSlice) {
				t.Errorf("Partition(): expected %v, got %v", test.expectedSlice, diff)
			}
		})
	}
}

func TestPartition(t *testing.T) {
	testCases := []struct {
		name            string
		slice1          []uint32
		slice2          []uint32
		expectedRemoved []uint32
		expectedCommon  []uint32
		expectedAdded   []uint32
	}{
		{
			name:            "empty slices",
			slice1:          []uint32{},
			slice2:          []uint32{},
			expectedRemoved: []uint32{},
			expectedCommon:  []uint32{},
			expectedAdded:   []uint32{},
		},
		{
			name:            "normal",
			slice1:          []uint32{0, 1, 2, 4},
			slice2:          []uint32{1, 2, 3},
			expectedRemoved: []uint32{0, 4},
			expectedCommon:  []uint32{1, 2},
			expectedAdded:   []uint32{3},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			removed, common, added := Partition(test.slice1, test.slice2)
			if !reflect.DeepEqual(removed, test.expectedRemoved) {
				t.Errorf("Partition(): expected %v, got %v", test.expectedRemoved, removed)
			}

			if !reflect.DeepEqual(common, test.expectedCommon) {
				t.Errorf("Partition(): expected %v, got %v", test.expectedCommon, common)
			}

			if !reflect.DeepEqual(added, test.expectedAdded) {
				t.Errorf("Partition(): expected %v, got %v", test.expectedAdded, added)
			}
		})
	}
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
