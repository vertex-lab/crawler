package pagerank

import (
	"math/rand"
	"slices"
	"time"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

// if nodeID isn't in the RWM, generate new walks from scratch
func (RWM *RandomWalksManager) UpdateNewNode(DB graph.Database, nodeID uint32) error {

	// checking the inputs
	const expectEmptyRWM = false
	err := checkInputs(RWM, DB, expectEmptyRWM)
	if err != nil {
		return err
	}

	// checking that nodeID exists in the DB
	if _, err := DB.NodeByID(nodeID); err != nil {
		return err
	}

	// if nodeID is already in the RWM, exit
	if _, exist := RWM.WalksByNode[nodeID]; exist {
		return nil
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	err = RWM.generateRandomWalks(DB, []uint32{nodeID}, rng)
	return err
}

/*
UpdateRandomWalks updates the RandomWalksManager when a node's successors change from
oldSuccessorIDs to currentSuccessorIDs.

INPUTS
------

	> DB: graph.Database
	The database where nodes are stored

	> nodeID: uint32
	The ID of the node that changed his successors from oldSuccessorIDs to currentSuccessorIDs

	> oldSuccessorIDs: []uint32
	The slice that contains the node IDs of the old successors of nodeID

	> currentSuccessorIDs: []uint32
	The slice that contains the node IDs of the current successors of nodeID

OUTPUT
------

	> error: look at checkInputs() to read all the errors

NOTE
----

REFERENCES
----------

	[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
	URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (RWM *RandomWalksManager) UpdateRandomWalks(DB graph.Database, nodeID uint32,
	oldSuccessorIDs []uint32, currentSuccessorIDs []uint32) error {

	// checking the inputs
	const expectEmptyRWM = false
	err := checkInputs(RWM, DB, expectEmptyRWM)
	if err != nil {
		return err
	}

	// checking that nodeID exists in the DB
	if _, err := DB.NodeByID(nodeID); err != nil {
		return err
	}

	// checking that nodeID exist in the RWM
	if _, exist := RWM.WalksByNode[nodeID]; !exist {
		return ErrNodeNotFoundRWM
	}

	removedIDs, addedIDs := nodeChanges(oldSuccessorIDs, currentSuccessorIDs)

	// pass a random number generator for reproducibility in tests
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
updateRemovedNodes is a method that updates the RWM by "pruning" and "grafting"
all the walks that contain the hop (nodeID --> removedNode).
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
	for walk := range walkSet.Iter() {

		update, cutIndex, err := walk.NeedsUpdate(nodeID, removedIDs)
		if err != nil {
			return err
		}

		// if it doesn't need update, skip
		if !update {
			continue
		}

		// prune the walk
		err = RWM.PruneWalk(walk, cutIndex)
		if err != nil {
			return err
		}

		/* generate a new walk starting from nodeID; This walk is guaranteed
		to contain nodeID only in the first position, as walks can't
		have cycles. This is REQUIRED to avoid potential deadlocks
		(remember that we are accessing the WalkSet of nodeID, so we can't change it) */
		newWalkSegment, err := generateWalk(DB, nodeID, RWM.alpha, rng)
		if err != nil {
			return err
		}

		// drop the fist element (nodeID) because it's already in the walk
		newWalkSegment = slices.Delete(newWalkSegment, 0, 1)

		// graft the walk with the new walk segment
		err = RWM.GraftWalk(walk, newWalkSegment)
		if err != nil {
			return err
		}
	}

	return nil
}

/*
updateAddedNodes is a method that updates the RWM by "pruning" some randomly
selected walks and by "grafting" them using the newly added nodes
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
	for walk := range walkSet.Iter() {

		if rng.Float32() > probabilityThreshold {
			continue
		}

		// prune the walk after the position of nodeID
		cutIndex := slices.Index(walk.NodeIDs, nodeID) + 1

		err := RWM.PruneWalk(walk, cutIndex)
		if err != nil {
			return err
		}

		// select the successor among the newly added nodes
		randomIndex := rng.Intn(lenAddedIDs)
		successorID := addedIDs[randomIndex]

		// if it was already visited in the walk, we've found a cycle and we stop
		if slices.Contains(walk.NodeIDs, successorID) {
			continue
		}

		// generate a new walk from the successor
		newWalk, err := generateWalk(DB, successorID, RWM.alpha, rng)
		if err != nil {
			return err
		}

		// Remove subsequent cycles in the extended walk [... nodeID, successorID, ...]
		for i, newNodeID := range newWalk {

			// if it was already visited in the walk, we've found a cycle
			if slices.Contains(walk.NodeIDs, newNodeID) {
				newWalk = slices.Delete(newWalk, i, len(newWalk))
				break
			}
		}

		// graft the walk
		err = RWM.GraftWalk(walk, newWalk)
		if err != nil {
			return err
		}

	}

	return nil
}

// -----------------------------HELPER-FUNCTIONS---------------------------

/*
nodeChanges returns removed and added IDs, using set notation:

removedIDs = oldIDs - newIDs
addedIDs = newIDs - oldIDs

Time complexity O(n * logn + m * logm), where n and m are the lengths of the slices.
This function is much faster than converting to sets for sizes (n, m) smaller than ~10^6.
*/
func nodeChanges(oldIDs, newIDs []uint32) ([]uint32, []uint32) {

	// Sort both slices first
	slices.Sort(oldIDs)
	slices.Sort(newIDs)

	removedIDs := []uint32{}
	addedIDs := []uint32{}

	i, j := 0, 0
	lenOld, lenNew := len(oldIDs), len(newIDs)

	// Use two pointers to compare both sorted lists
	for i < lenOld && j < lenNew {

		if oldIDs[i] < newIDs[j] {
			// oldID is not in newIDs, so it was removed
			removedIDs = append(removedIDs, oldIDs[i])
			i++

		} else if oldIDs[i] > newIDs[j] {
			// newID is not in oldIDs, so it was added
			addedIDs = append(addedIDs, newIDs[j])
			j++

		} else {
			// Both are equal, move both pointers forward
			i++
			j++
		}
	}

	// Add all elements not traversed
	removedIDs = append(removedIDs, oldIDs[i:]...)
	addedIDs = append(addedIDs, newIDs[j:]...)

	return removedIDs, addedIDs
}
