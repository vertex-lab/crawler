package walks

import (
	"slices"
	"sort"
)

/*
returns removed and added elements, using set notation:

removed = oldSlice - newSlice
added = newSlice - oldSlice

Time complexity O(n * logn + m * logm), where n and m are the lengths of the slices.
This function is much faster than converting to sets for sizes (n, m) smaller than ~10^6.
*/
func Differences(oldSlice, newSlice []uint32) ([]uint32, []uint32) {

	// Sort both slices first
	slices.Sort(oldSlice)
	slices.Sort(newSlice)

	removed := []uint32{}
	added := []uint32{}

	i, j := 0, 0
	lenOld, lenNew := len(oldSlice), len(newSlice)

	// Use two pointers to compare both sorted lists
	for i < lenOld && j < lenNew {

		if oldSlice[i] < newSlice[j] {
			// oldID is not in newSlice, so it was removed
			removed = append(removed, oldSlice[i])
			i++

		} else if oldSlice[i] > newSlice[j] {
			// newID is not in oldSlice, so it was added
			added = append(added, newSlice[j])
			j++

		} else {
			// Both are equal, move both pointers forward
			i++
			j++
		}
	}

	// Add all elements not traversed
	removed = append(removed, oldSlice[i:]...)
	added = append(added, newSlice[j:]...)

	return removed, added
}

// returns newWalkSegment[i] with the highest i such that
// oldWalk + newWalkSegment[i] doesn't contain a cycle.
func removeCycles(oldWalk []uint32, newWalkSegment []uint32) []uint32 {

	for i, newNodeID := range newWalkSegment {

		// if it was already visited, we've found a cycle
		if slices.Contains(oldWalk, newNodeID) {
			newWalkSegment = slices.Delete(newWalkSegment, i, len(newWalkSegment))
			break
		}
	}

	return newWalkSegment
}

// dereferences the random walks and sorts them in lexicographic order
func SortWalks(walkSet WalkSet) ([][]uint32, error) {

	if walkSet.Cardinality() == 0 {
		return nil, ErrEmptyRandomWalk
	}

	walks := [][]uint32{}

	// dereference the pointers
	for rWalk := range walkSet.Iter() {
		walks = append(walks, rWalk.NodeIDs)
	}

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

	return walks, nil
}
