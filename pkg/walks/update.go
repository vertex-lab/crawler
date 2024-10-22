package walks

import (
	"math/rand"
	"slices"
	"time"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
)

/*
Update updates the RandomWalksManager when a node's successors change from
oldSucc to currentSucc.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (RWM *RandomWalksManager) Update(DB graph.Database, nodeID uint32,
	oldSucc []uint32, currentSucc []uint32) error {

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

	removedSucc, commonSucc, addesSucc := Partition(oldSucc, currentSucc)

	// for reproducibility in tests
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	err = RWM.updateRemovedNodes(DB, nodeID, removedSucc, commonSucc, rng)
	if err != nil {
		return err
	}

	err = RWM.updateAddedNodes(DB, nodeID, addesSucc, len(currentSucc), rng)
	if err != nil {
		return err
	}

	return nil
}

/*
a method that updates the RWM by "pruning" and "grafting" all the walks that
contain the hop `nodeID` --> `removedID` in `removedSucc`.

After the execution of this method, the state of the walks of nodeID is as if nodeID
only successors are the common successors `commonSucc`.
*/
func (RWM *RandomWalksManager) updateRemovedNodes(DB graph.Database, nodeID uint32,
	removedSucc, commonSucc []uint32, rng *rand.Rand) error {

	if len(removedSucc) == 0 {
		return nil
	}

	walkSet, err := RWM.WalksByNodeID(nodeID)
	if err != nil {
		return err
	}

	// iterate over the walks
	for rWalk := range walkSet.Iter() {

		update, cutIndex, err := rWalk.NeedsUpdate(nodeID, removedSucc)
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

		// select the new next node among the common successors
		successorID, shouldStop := walkStep(commonSucc, rWalk.NodeIDs, rng)
		if shouldStop {
			continue
		}

		// generate the new walk segment
		newWalkSegment, err := generateWalk(DB, successorID, RWM.alpha, rng)
		if err != nil {
			return err
		}

		// remove potential cycles
		newWalkSegment = removeCycles(rWalk.NodeIDs, newWalkSegment)

		// graft the walk with the new walk segment
		err = RWM.GraftWalk(rWalk, newWalkSegment)
		if err != nil {
			return err
		}
	}

	return nil
}

/*
a method that updates the RWM by "pruning" some randomly selected walks of nodeID
and by "grafting" them using the newly added nodes
*/
func (RWM *RandomWalksManager) updateAddedNodes(DB graph.Database, nodeID uint32,
	addesSucc []uint32, newOutDegree int, rng *rand.Rand) error {

	if len(addesSucc) == 0 {
		return nil
	}

	walkSet, err := RWM.WalksByNodeID(nodeID)
	if err != nil {
		return err
	}

	// probabilistic update check
	probabilityThreshold := float32(len(addesSucc)) / float32(newOutDegree)

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

		// select the new next node
		addedID, shouldStop := walkStep(addesSucc, rWalk.NodeIDs, rng)
		if shouldStop {
			continue
		}

		// generate a new walk from the successor
		newWalkSegment, err := generateWalk(DB, addedID, RWM.alpha, rng)
		if err != nil {
			return err
		}

		// remove potential cycles
		newWalkSegment = removeCycles(rWalk.NodeIDs, newWalkSegment)

		// graft the walk with the new walk segment
		err = RWM.GraftWalk(rWalk, newWalkSegment)
		if err != nil {
			return err
		}

	}

	return nil
}
