package sliceutils

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/vertex-lab/crawler/pkg/models"
)

func TestUnique(t *testing.T) {
	testCases := []struct {
		name          string
		slice         []uint32
		expectedSlice []uint32
	}{
		{
			name: "nil slice",
		},
		{
			name:  "empty slice",
			slice: []uint32{},
		},
		{
			name:          "already unique",
			slice:         []uint32{1, 2, 3},
			expectedSlice: []uint32{1, 2, 3},
		},
		{
			name:          "valid",
			slice:         []uint32{1, 2, 3, 4, 2, 3, 2},
			expectedSlice: []uint32{1, 2, 3, 4},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			unique := Unique(test.slice)
			if !reflect.DeepEqual(unique, test.expectedSlice) {
				t.Errorf("Unique(): expected %v, got %v", test.expectedSlice, unique)
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
			name: "nil slices",
		},
		{
			name:   "empty slices",
			slice1: []uint32{},
			slice2: []uint32{}},
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
				t.Errorf("Difference(): expected %v, got %v", test.expectedSlice, diff)
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
			name: "nil slices",
		},
		{
			name:   "empty slices",
			slice1: []uint32{},
			slice2: []uint32{},
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

func TestSplitSlice(t *testing.T) {
	testCases := []struct {
		name          string
		slice         []string
		batchSize     int
		expectedSplit [][]string
	}{
		{
			name:      "nil slice",
			slice:     nil,
			batchSize: 1,
		},
		{
			name:      "empty slice",
			slice:     []string{},
			batchSize: 1,
		},
		{
			name:          "valid slice",
			slice:         []string{"a", "b", "c", "d", "e", "f", "g"},
			batchSize:     3,
			expectedSplit: [][]string{{"a", "b", "c"}, {"d", "e", "f"}, {"g"}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			split := SplitSlice(test.slice, test.batchSize)
			if !reflect.DeepEqual(split, test.expectedSplit) {
				t.Fatalf("SplitSlice(): expected %v, got %v", test.expectedSplit, split)
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

func BenchmarkUnique(b *testing.B) {
	size := 1000000
	slice := make([]uint32, size)
	for i := 0; i < size; i++ {
		slice[i] = uint32(rand.Intn(10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Unique(slice)
	}
}

func BenchmarkSplitSlice(b *testing.B) {
	size := 1000000
	slice := make([]string, size)
	for i := 0; i < size; i++ {
		slice[i] = "a"
	}

	for i := 0; i < b.N; i++ {
		SplitSlice(slice, 50000)
	}
}

func BenchmarkDifference(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size %d", size), func(b *testing.B) {
			slice1 := make([]uint32, 0, size)
			slice2 := make([]uint32, 0, size)

			// setup up the two slices
			for i := 0; i < size; i++ {
				slice1 = append(slice1, rand.Uint32()%uint32(size))
				slice2 = append(slice2, rand.Uint32()%uint32(size))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Difference(slice1, slice2)
			}
		})
	}
}

func BenchmarkPartition(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size %d", size), func(b *testing.B) {
			slice1 := make([]uint32, 0, size)
			slice2 := make([]uint32, 0, size)

			// setup up the two slices
			for i := 0; i < size; i++ {
				slice1 = append(slice1, rand.Uint32()%uint32(size))
				slice2 = append(slice2, rand.Uint32()%uint32(size))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Partition(slice1, slice2)
			}
		})
	}
}
