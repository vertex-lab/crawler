package walks

import (
	"math/rand"
	"slices"
	"time"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
)

/*
Update updates the RandomWalksManager when a node's successors change from
oldSuccessorIDs to currentSuccessorIDs.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (RWM *RandomWalksManager) Update(DB graph.Database, nodeID uint32,
	oldSuccessorIDs []uint32, currentSuccessorIDs []uint32) error {

	// checking the inputs
	const expectEmptyRWM = false
	err := checkInputs(RWM, DB, expectEmptyRWM)
	if err != nil {
		return err
	}

	// checking that nodeID is in the DB
	if _, err := DB.NodeByID(nodeID); err != nil {
		return err
	}

	removedIDs, addedIDs := Differences(oldSuccessorIDs, currentSuccessorIDs)

	// for reproducibility in tests
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	err = RWM.updateRemovedNodes(DB, nodeID, removedIDs, rng)
	if err != nil {
		return err
	}

	err = RWM.updateAddedNodes(DB, nodeID, addedIDs, len(currentSuccessorIDs), rng)
	if err != nil {
		return err
	}

	return nil
}

/*
a method that updates the RWM by "pruning" and "grafting" all the walks that
contain the hop nodeID --> removedID in removedIDs
*/
func (RWM *RandomWalksManager) updateRemovedNodes(DB graph.Database, nodeID uint32,
	removedIDs []uint32, rng *rand.Rand) error {

	if len(removedIDs) == 0 {
		return nil
	}

	walkSet, err := RWM.WalksByNodeID(nodeID)
	if err != nil {
		return err
	}

	// iterate over the walks
	for rWalk := range walkSet.Iter() {

		update, cutIndex, err := rWalk.NeedsUpdate(nodeID, removedIDs)
		if err != nil {
			return err
		}

		// if it doesn't need an update, skip
		if !update {
			continue
		}

		// prune the walk
		err = RWM.PruneWalk(rWalk, cutIndex)
		if err != nil {
			return err
		}

		// generate new walk from nodeID
		newWalkSegment, err := generateWalk(DB, nodeID, RWM.alpha, rng)
		if err != nil {
			return err
		}

		// remove the first element (nodeID), because that's already in the walk and other potential cycles
		newWalkSegment = removeCycles(rWalk.NodeIDs, newWalkSegment[1:])

		// graft the walk with the new walk segment
		err = RWM.GraftWalk(rWalk, newWalkSegment)
		if err != nil {
			return err
		}
	}

	return nil
}

/*
a method that updates the RWM by "pruning" some randomly selected walks and
by "grafting" them using the newly added nodes
*/
func (RWM *RandomWalksManager) updateAddedNodes(DB graph.Database, nodeID uint32,
	addedIDs []uint32, newOutDegree int, rng *rand.Rand) error {

	lenAddedIDs := len(addedIDs)
	if lenAddedIDs == 0 {
		return nil
	}

	walkSet, err := RWM.WalksByNodeID(nodeID)
	if err != nil {
		return err
	}

	// skip with probability 1 - len(addedIDs)/newOutDegree
	probabilityThreshold := float32(lenAddedIDs) / float32(newOutDegree)

	// iterate over the walks
	for rWalk := range walkSet.Iter() {

		if rng.Float32() > probabilityThreshold {
			continue
		}

		// prune the walk AFTER the position of nodeID
		cutIndex := slices.Index(rWalk.NodeIDs, nodeID) + 1

		err := RWM.PruneWalk(rWalk, cutIndex)
		if err != nil {
			return err
		}

		// stop with probability 1-alpha
		if rng.Float32() > RWM.alpha {
			continue
		}

		// select the successor among the newly added nodes
		randomIndex := rng.Intn(lenAddedIDs)
		successorID := addedIDs[randomIndex]

		// generate a new walk from the successor
		newWalkSegment, err := generateWalk(DB, successorID, RWM.alpha, rng)
		if err != nil {
			return err
		}

		// remove potential cycles
		newWalkSegment = removeCycles(rWalk.NodeIDs, newWalkSegment)

		err = RWM.GraftWalk(rWalk, newWalkSegment)
		if err != nil {
			return err
		}

	}

	return nil
}

// -----------------------------HELPER-FUNCTIONS---------------------------

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
