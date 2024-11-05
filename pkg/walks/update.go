package walks

import (
	"math/rand"
	"slices"
	"time"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

/*
Update() updates the RandomWalkManager when a node's successors change from
oldSucc to currentSucc.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (RWM *RandomWalkManager) Update(DB models.Database, nodeID uint32,
	oldSucc []uint32, currentSucc []uint32) error {

	if err := checkInputs(RWM, DB, false); err != nil {
		return err
	}

	if !DB.ContainsNode(nodeID) {
		return models.ErrNodeNotFoundDB
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RWM.update(DB, nodeID, oldSucc, currentSucc, rng)
}

// update() implements the internal logic of the Update method. It accepts a
// random number generator for reproducibility in tests.
func (RWM *RandomWalkManager) update(DB models.Database, nodeID uint32,
	oldSucc []uint32, currentSucc []uint32, rng *rand.Rand) error {

	removedSucc, commonSucc, addesSucc := Partition(oldSucc, currentSucc)

	if err := RWM.updateRemovedNodes(DB, nodeID, removedSucc, commonSucc, rng); err != nil {
		return err
	}

	if err := RWM.updateAddedNodes(DB, nodeID, addesSucc, len(currentSucc), rng); err != nil {
		return err
	}

	return nil
}

/*
updateRemovedNodes() updates the RWM by "pruning" and "grafting" all the walks that
contain the hop `nodeID` --> `removedID` in `removedSucc`.

After the execution of this method, the state of the walks of nodeID is as if nodeID
only successors are the common successors `commonSucc`.
*/
func (RWM *RandomWalkManager) updateRemovedNodes(DB models.Database, nodeID uint32,
	removedSucc, commonSucc []uint32, rng *rand.Rand) error {

	if len(removedSucc) == 0 {
		return nil
	}

	walkMap, err := RWM.Store.NodeWalks(nodeID)
	if err != nil {
		return err
	}

	for walkID, walk := range walkMap {

		cutIndex, update, err := NeedsUpdate(walk, nodeID, removedSucc)
		if err != nil {
			return err
		}

		// if it doesn't need an update, skip
		if !update {
			continue
		}

		// prune the walk REFACTOR
		walk = walk[:cutIndex]
		if err = RWM.Store.PruneWalk(walkID, cutIndex); err != nil {
			return err
		}

		// select the new next node among the common successors
		successorID, shouldStop := WalkStep(commonSucc, walk, rng)
		if shouldStop {
			continue
		}

		// generate the new walk segment
		newWalkSegment, err := generateWalk(DB, successorID, RWM.Store.Alpha(), rng)
		if err != nil {
			return err
		}

		// remove potential cycles
		newWalkSegment = DeleteCyclesInPlace(walk, newWalkSegment)

		// graft the walk with the new walk segment
		if err := RWM.Store.GraftWalk(walkID, newWalkSegment); err != nil {
			return err
		}
	}

	return nil
}

/*
a method that updates the RWM by "pruning" some randomly selected walks of nodeID
and by "grafting" them using the newly added nodes
*/
func (RWM *RandomWalkManager) updateAddedNodes(DB models.Database, nodeID uint32,
	addesSucc []uint32, newOutDegree int, rng *rand.Rand) error {

	if len(addesSucc) == 0 {
		return nil
	}

	walkMap, err := RWM.Store.NodeWalks(nodeID)
	if err != nil {
		return err
	}

	// probabilistic update check
	probabilityThreshold := float32(len(addesSucc)) / float32(newOutDegree)

	// iterate over the walks
	for walkID, walk := range walkMap {

		if rng.Float32() > probabilityThreshold {
			continue
		}

		// prune the walk AFTER the position of nodeID
		cutIndex := slices.Index(walk, nodeID) + 1
		walk = walk[:cutIndex]
		if err := RWM.Store.PruneWalk(walkID, cutIndex); err != nil {
			return err
		}

		// stop with probability 1-alpha
		if rng.Float32() > RWM.Store.Alpha() {
			continue
		}

		// select the new next node
		addedID, shouldStop := WalkStep(addesSucc, walk, rng)
		if shouldStop {
			continue
		}

		// generate a new walk from the successor
		newWalkSegment, err := generateWalk(DB, addedID, RWM.Store.Alpha(), rng)
		if err != nil {
			return err
		}

		// remove potential cycles
		newWalkSegment = DeleteCyclesInPlace(walk, newWalkSegment)

		// graft the walk with the new walk segment
		if err = RWM.Store.GraftWalk(walkID, newWalkSegment); err != nil {
			return err
		}
	}

	return nil
}

/*
NeedsUpdate returns whether the RandomWalk needs to be updated, and the index
where to implement the update (pruning and grafting).

This happens if the walk contains an invalid hop nodeID --> removedNode in removedNodes.
The average lenght of removedNodes is supposed to be quite small, meaning that
sets are less performing than slices.

cutIndex = -1 signals avoid proceeding
*/
func NeedsUpdate(walk models.RandomWalk, nodeID uint32,
	removedNodes []uint32) (cutIndex int, needsUpdate bool, err error) {

	if err := models.Validate(walk); err != nil {
		return -1, true, err
	}

	// iterate over the elements of the walk
	for i := 0; i < len(walk)-1; i++ {

		// if it contains a hop (nodeID --> removedNode)
		if walk[i] == nodeID && slices.Contains(removedNodes, walk[i+1]) {
			// it needs to be updated from (i+1)th element (included) onwards
			cutIndex := i + 1
			return cutIndex, true, nil
		}
	}
	return -1, false, nil
}
