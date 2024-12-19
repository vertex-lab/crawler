// The sliceutils package adds helper functions that deal with slices and walks.
package sliceutils

import (
	"slices"
	"sort"

	"github.com/vertex-lab/crawler/pkg/models"
)

// Unique() returns a slice of unique elements of the input slice.
func Unique(slice []uint32) []uint32 {
	if len(slice) == 0 {
		return []uint32{}
	}

	slices.Sort(slice)
	unique := make([]uint32, 0, len(slice))
	unique = append(unique, slice[0])

	for i := 1; i < len(slice); i++ {
		if slice[i] != slice[i-1] {
			unique = append(unique, slice[i])
		}
	}

	return unique
}

// SplitSlice splits a slice into a slice of slices with a maximum size of batchSize
func SplitSlice(slice []string, batchSize int) [][]string {
	if len(slice) == 0 {
		return [][]string{}
	}

	split := make([][]string, 0, len(slice)/batchSize)
	for batchSize < len(slice) {
		split, slice = append(split, slice[:batchSize]), slice[batchSize:]
	}
	return append(split, slice)
}

// EqualElements() returns whether slice1 and slice2 are equal if they have
// the same elements (possibly in different positions).
// Empty and nil slices are considered equal.
// Time complexity is O(n * logn + m * logm)  where n and m are the lengths of the slices.
func EqualElements(slice1, slice2 []uint32) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	slices.Sort(slice1)
	slices.Sort(slice2)

	for i := range slice1 {
		if slice1[i] != slice2[i] {
			return false
		}
	}

	return true
}

/*
Difference() returns the difference between slice1 and slice2; in set notation:

- difference = slice1 - slice2

Time complexity O(n * logn + m * logm), where n and m are the lengths of the slices.
This function is much faster than converting to sets for sizes (n, m) smaller than ~10^6.
*/
func Difference(slice1, slice2 []uint32) []uint32 {
	slices.Sort(slice1)
	slices.Sort(slice2)
	difference := []uint32{}

	i, j := 0, 0
	lenOld, lenNew := len(slice1), len(slice2)
	for i < lenOld && j < lenNew {
		if slice1[i] < slice2[j] {
			// the element is in slice1 but not in slice2
			difference = append(difference, slice1[i])
			i++
		} else if slice1[i] > slice2[j] {
			j++
		} else {
			i++
			j++
		}
	}

	// Add all elements not traversed
	difference = append(difference, slice1[i:]...)
	return difference
}

/*
Partition() returns removed, commond and added elements, using set notation:

removed = slice1 - slice2
common = slice1 ^ slice2
added = slice2 - slice1

Time complexity O(n * logn + m * logm), where n and m are the lengths of the slices.
This function is much faster than converting to sets for sizes (n, m) smaller than ~10^6.
*/
func Partition(slice1, slice2 []uint32) (removed, common, added []uint32) {
	slices.Sort(slice1)
	slices.Sort(slice2)

	i, j := 0, 0
	lenOld, lenNew := len(slice1), len(slice2)

	for i < lenOld && j < lenNew {

		if slice1[i] < slice2[j] {
			// oldID is not in slice2, so it was removed
			removed = append(removed, slice1[i])
			i++

		} else if slice1[i] > slice2[j] {
			// newID is not in slice1, so it was added
			added = append(added, slice2[j])
			j++

		} else {
			// oldID = newID, so it's common
			common = append(common, slice1[i])
			i++
			j++
		}
	}

	// Add all elements not traversed
	removed = append(removed, slice1[i:]...)
	added = append(added, slice2[j:]...)
	return removed, common, added
}

/*
returns a new slice up to the first occurrence of a cycle by checking against
oldWalk. TrimCycles doesn't change newWalk in the caller. If that's wanted,
use DeleteCyclesInPlace instead.
*/
func TrimCycles(oldWalk []uint32, newWalk []uint32) []uint32 {
	for i, newNodeID := range newWalk {
		if slices.Contains(oldWalk, newNodeID) {
			return newWalk[:i]
		}
	}
	return newWalk
}

/*
removes (in place) all elements from newWalk after the first occurrence of a
cycle by checking against oldWalk. DeleteCyclesInPlace changes newWalk in the
caller. If that's not wanted, use TrimCycles instead.
*/
func DeleteCyclesInPlace(oldWalk []uint32, newWalk []uint32) []uint32 {
	for i, newNodeID := range newWalk {
		if slices.Contains(oldWalk, newNodeID) {
			return slices.Delete(newWalk, i, len(newWalk))
		}
	}
	return newWalk
}

// SortWalks() sorts walks lexicographically.
func SortWalks(walks []models.RandomWalk) []models.RandomWalk {
	sort.Slice(walks, func(i, j int) bool {
		// Compare slices lexicographically
		for x := 0; x < len(walks[i]) && x < len(walks[j]); x++ {
			if walks[i][x] < walks[j][x] {
				return true
			} else if walks[i][x] > walks[j][x] {
				return false
			}
		}
		return len(walks[i]) < len(walks[j])
	})

	return walks
}
