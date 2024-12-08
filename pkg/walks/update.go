package walks

import (
	"math/rand"
	"slices"
	"time"

	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
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

	removedSucc, commonSucc, addedSucc := sliceutils.Partition(oldSucc, currentSucc)

	if err := RWM.updateRemovedNodes(DB, nodeID, removedSucc, commonSucc, rng); err != nil {
		return err
	}

	if err := RWM.updateAddedNodes(DB, nodeID, addedSucc, len(currentSucc), rng); err != nil {
		return err
	}

	return nil
}

/*
updateRemovedNodes() updates the RWM by "pruning" and "grafting" all the walks that
contain the invalid step `nodeID` --> `removedID` in `removedSucc`.

After the execution of this method, the state of the walks of nodeID is as if nodeID
only successors are the common successors `commonSucc`.
*/
func (RWM *RandomWalkManager) updateRemovedNodes(DB models.Database, nodeID uint32,
	removedSucc, commonSucc []uint32, rng *rand.Rand) error {

	if len(removedSucc) == 0 {
		return nil
	}

	walkMap, err := RWM.Store.CommonWalks(nodeID, removedSucc)
	if err != nil {
		return err
	}

	for walkID, walk := range walkMap {

		cutIndex, err := containsInvalidStep(walk, nodeID, removedSucc)
		if err != nil {
			return err
		}

		// if it doesn't need an update, skip
		if cutIndex < 0 {
			continue
		}

		// generate a new walk segment that will replace the invalid segment of the walk
		newWalkSegment, err := generateWalkSegment(DB, commonSucc, walk[:cutIndex], RWM.Store.Alpha(), rng)
		if err != nil {
			return err
		}

		// prune and graft the walk with the new walk segment
		if err = RWM.Store.PruneGraftWalk(walkID, cutIndex, newWalkSegment); err != nil {
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
	addedSucc []uint32, currentSuccSize int, rng *rand.Rand) error {

	if len(addedSucc) == 0 {
		return nil
	}

	limit, err := estimateWalksToUpdate(RWM, nodeID, len(addedSucc), currentSuccSize)
	if err != nil {
		return err
	}

	walkMap, err := RWM.Store.Walks(nodeID, limit)
	if err != nil {
		return err
	}

	for walkID, walk := range walkMap {

		// prune the walk AFTER the position of nodeID
		cutIndex := slices.Index(walk, nodeID) + 1

		// with probability alpha, generate a new walk segment that will replace the old segment
		var newWalkSegment models.RandomWalk
		if rng.Float32() < RWM.Store.Alpha() {

			newWalkSegment, err = generateWalkSegment(DB, addedSucc, walk[:cutIndex], RWM.Store.Alpha(), rng)
			if err != nil {
				return err
			}
		}

		// prune and graft the walk with the new walk segment
		if err := RWM.Store.PruneGraftWalk(walkID, cutIndex, newWalkSegment); err != nil {
			return err
		}
	}

	return nil
}

/*
generateWalkSegment() is responsible for generating a walk segment that will be
grafted (appended) to the currentWalk. It selectes the next node from a slice of
candidateNodes, and ensures that the currentWalk + newWalkSegment doesn't contain any cycle.
*/
func generateWalkSegment(DB models.Database, candidateNodes []uint32, currentWalk models.RandomWalk,
	alpha float32, rng *rand.Rand) (models.RandomWalk, error) {

	// select the next node
	successorID, shouldStop := WalkStep(candidateNodes, currentWalk, rng)
	if shouldStop {
		return models.RandomWalk{}, nil
	}

	// generate the new walk segment
	newWalkSegment, err := generateWalk(DB, successorID, alpha, rng)
	if err != nil {
		return nil, err
	}

	// remove potential cycles
	return sliceutils.DeleteCyclesInPlace(currentWalk, newWalkSegment), nil
}

/*
containsInvalidStep() returns the index or position where the RandomWalk needs to be
Pruned and Grafted.

This happens if the walk contains an invalid hop nodeID --> removedNode in removedNodes.

cutIndex = -1 signals no need to update.
*/
func containsInvalidStep(walk models.RandomWalk, nodeID uint32,
	removedNodes []uint32) (int, error) {

	if err := models.Validate(walk); err != nil {
		return -1, err
	}

	for i := 0; i < len(walk)-1; i++ {
		// if it contains a hop (nodeID --> removedNode)
		if walk[i] == nodeID && slices.Contains(removedNodes, walk[i+1]) {
			// it needs to be updated from (i+1)th element (included) onwards
			cutIndex := i + 1
			return cutIndex, nil
		}
	}
	return -1, nil
}

// estimateWalksToUpdate() returns the number of walks that needs to be updated in updateAddedNodes().
// This number is (addedSize / currentSize) * numberOfWalks.
func estimateWalksToUpdate(RWM *RandomWalkManager, nodeID uint32, addedSize int, currentSize int) (int, error) {
	walkMap, err := RWM.Store.VisitCounts([]uint32{nodeID})
	if err != nil {
		return 1, err
	}

	walksNum := float32(walkMap[nodeID])
	p := float32(addedSize) / float32(currentSize)

	return int(p * walksNum), nil
}

// probabilityOfSelection() returns the probability of a walk to be updated by
// the method RWM.updateAddedNodes().
func probabilityOfSelection(addedSuccSize int, currentSuccSize int) float32 {
	return float32(addedSuccSize) / float32(currentSuccSize)
}
