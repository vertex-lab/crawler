// The sliceutils package adds helper functions that deal with slices and walks.
package sliceutils

import (
	"cmp"
	"slices"
	"sort"

	"github.com/vertex-lab/crawler/pkg/models"
)

// Unique() returns a slice of unique elements of the input slice.
func Unique[S ~[]E, E cmp.Ordered](slice S) S {
	if len(slice) == 0 {
		return nil
	}

	slices.Sort(slice)
	unique := make(S, 0, len(slice))
	unique = append(unique, slice[0])

	for i := 1; i < len(slice); i++ {
		if slice[i] != slice[i-1] {
			unique = append(unique, slice[i])
		}
	}

	return unique
}

/*
Difference() returns the difference between slice1 and slice2; in set notation:

- difference = slice1 - slice2

Time complexity O(n * logn + m * logm), where n and m are the lengths of the slices.
This function is much faster than converting to sets for sizes (n, m) smaller than ~10^6.
*/
func Difference[S ~[]E, E cmp.Ordered](slice1, slice2 S) S {
	slices.Sort(slice1)
	slices.Sort(slice2)
	len1, len2 := len(slice1), len(slice2)

	var difference S
	var i, j int
	for i < len1 && j < len2 {
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
func Partition[S ~[]E, E cmp.Ordered](slice1, slice2 S) (removed, common, added S) {
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

// SplitSlice splits a slice into a slice of slices, each with a maximum size of batchSize
func SplitSlice(slice []string, batchSize int) [][]string {
	if len(slice) == 0 {
		return nil
	}

	split := make([][]string, 0, len(slice)/batchSize)
	for batchSize < len(slice) {
		split, slice = append(split, slice[:batchSize]), slice[batchSize:]
	}
	return append(split, slice)
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
