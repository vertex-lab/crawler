package walks

import (
	"math/rand"
	"slices"
	"sort"

	mapset "github.com/deckarep/golang-set/v2"
)

/*
returns removed, commond and added elements, using set notation:

removed = oldSlice - newSlice
common = oldSlice ^ newSlice
added = newSlice - oldSlice

Time complexity O(n * logn + m * logm), where n and m are the lengths of the slices.
This function is much faster than converting to sets for sizes (n, m) smaller than ~10^6.
*/
func Partition(oldSlice, newSlice []uint32) ([]uint32, []uint32, []uint32) {

	// Sort both slices first
	slices.Sort(oldSlice)
	slices.Sort(newSlice)

	removed := []uint32{}
	common := []uint32{}
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
			// oldID = newID, so it's common
			common = append(common, oldSlice[i])
			i++
			j++
		}
	}

	// Add all elements not traversed
	removed = append(removed, oldSlice[i:]...)
	added = append(added, newSlice[j:]...)

	return removed, common, added
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
func SortWalks(walkSet WalkSet) [][]uint32 {

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

	return walks
}

// function that returns a RWM setup based on the RWMType
func SetupRWM(RWMType string) *RandomWalksManager {

	switch RWMType {

	case "nil":
		return nil

	case "empty":
		RWM, _ := NewRWM(0.85, 1)
		return RWM

	case "invalid-alpha":
		invalidAlphas := []float32{1.01, 1.0, -0.1, -2}
		size := len(invalidAlphas)

		RWM, _ := NewRWM(0.85, 1)
		RWM.alpha = invalidAlphas[rand.Intn(size)]
		return RWM

	case "invalid-walksPerNode":
		RWM, _ := NewRWM(0.85, 1)
		RWM.walksPerNode = 0
		return RWM

	case "one-node0":
		RWM, _ := NewRWM(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0}})
		RWM.WalksByNode[0] = walkSet
		return RWM

	case "one-node1":
		RWM, _ := NewRWM(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{1}})
		RWM.WalksByNode[1] = walkSet
		return RWM

	case "triangle":
		RWM, _ := NewRWM(0.85, 1)
		rWalk0 := &RandomWalk{NodeIDs: []uint32{0, 1, 2}}
		rWalk1 := &RandomWalk{NodeIDs: []uint32{1, 2, 0}}
		rWalk2 := &RandomWalk{NodeIDs: []uint32{2, 0, 1}}

		RWM.AddWalk(rWalk0)
		RWM.AddWalk(rWalk1)
		RWM.AddWalk(rWalk2)
		return RWM

	case "simple":
		RWM, _ := NewRWM(0.85, 1)
		rWalk := &RandomWalk{NodeIDs: []uint32{0, 1}}
		RWM.AddWalk(rWalk)

		return RWM

	default:
		return nil // Default to nil for unrecognized scenarios
	}
}
