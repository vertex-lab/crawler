package sliceutils

import (
	"slices"
	"sort"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

/*
returns the difference between slice1 and slice2; in set notation:

- difference = slice1 - slice2

Time complexity O(n * logn + m * logm), where n and m are the lengths of the slices.
This function is much faster than converting to sets for sizes (n, m) smaller than ~10^6.
*/
func Difference(slice1, slice2 []uint32) []uint32 {

	// Sort both slices first
	slices.Sort(slice1)
	slices.Sort(slice2)
	difference := []uint32{}

	i, j := 0, 0
	lenOld, lenNew := len(slice1), len(slice2)

	// Use two pointers to compare both sorted lists
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
returns removed, commond and added elements, using set notation:

removed = slice1 - slice2
common = slice1 ^ slice2
added = slice2 - slice1

Time complexity O(n * logn + m * logm), where n and m are the lengths of the slices.
This function is much faster than converting to sets for sizes (n, m) smaller than ~10^6.
*/
func Partition(slice1, slice2 []uint32) ([]uint32, []uint32, []uint32) {

	// Sort both slices first
	slices.Sort(slice1)
	slices.Sort(slice2)
	removed := []uint32{}
	common := []uint32{}
	added := []uint32{}

	i, j := 0, 0
	lenOld, lenNew := len(slice1), len(slice2)

	// Use two pointers to compare both sorted lists
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
		// if it was already visited, we've found a cycle
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
		// if it was already visited, we've found a cycle
		if slices.Contains(oldWalk, newNodeID) {
			return slices.Delete(newWalk, i, len(newWalk))
		}
	}
	return newWalk
}

func SortWalks(walks []models.RandomWalk) []models.RandomWalk {

	// Sort the walks lexicographically
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
