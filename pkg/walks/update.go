package walks

import (
	"context"
	"math/rand"
	"slices"
	"time"

	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
)

/*
Update() updates the RandomWalkManager when a node's follows changes.
These changes are represented by some removed follows, common follows and added follows

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (RWM *RandomWalkManager) Update(
	ctx context.Context,
	DB models.Database,
	nodeID uint32,
	removed, common, added []uint32) error {

	if err := DB.Validate(); err != nil {
		return err
	}

	if err := RWM.Store.Validate(); err != nil {
		return err
	}

	if !DB.ContainsNode(nodeID) {
		return models.ErrNodeNotFoundDB
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	if err := RWM.updateRemovedNodes(ctx, DB, nodeID, removed, common, rng); err != nil {
		return err
	}

	followsCount := len(common) + len(added)
	if err := RWM.updateAddedNodes(ctx, DB, nodeID, added, followsCount, rng); err != nil {
		return err
	}

	return nil
}

/*
updateRemovedNodes() updates the RWM by "pruning" and "grafting" all the walks that
contain the invalid step `nodeID` --> `removedID` in `removed`.

After the execution of this method, the state of the walks of nodeID is as if nodeID
only follows are the common follows `common`.
*/
func (RWM *RandomWalkManager) updateRemovedNodes(
	ctx context.Context,
	DB models.Database,
	nodeID uint32,
	removed, common []uint32,
	rng *rand.Rand) error {

	if len(removed) == 0 {
		return nil
	}

	walkMap, err := RWM.Store.Walks(ctx, nodeID, -1)
	if err != nil {
		return err
	}

	for walkID, walk := range walkMap {
		cutIndex, contains := containsInvalidStep(walk, nodeID, removed)
		if !contains {
			continue
		}

		// generate a new walk segment that will replace the invalid segment of the walk
		newWalkSegment, err := generateWalkSegment(ctx, DB, common, walk[:cutIndex], RWM.Store.Alpha(ctx), rng)
		if err != nil {
			return err
		}

		// prune and graft the walk with the new walk segment
		if err = RWM.Store.PruneGraftWalk(ctx, walkID, cutIndex, newWalkSegment); err != nil {
			return err
		}
	}

	return nil
}

/*
a method that updates the RWM by "pruning" some randomly selected walks of nodeID
and by "grafting" them using the newly added nodes as the starting points.
*/
func (RWM *RandomWalkManager) updateAddedNodes(
	ctx context.Context,
	DB models.Database,
	nodeID uint32,
	added []uint32,
	followsCount int,
	rng *rand.Rand) error {

	if len(added) == 0 {
		return nil
	}

	limit, err := estimateWalksToUpdate(ctx, RWM, nodeID, len(added), followsCount)
	if err != nil {
		return err
	}

	walkMap, err := RWM.Store.Walks(ctx, nodeID, limit)
	if err != nil {
		return err
	}

	for walkID, walk := range walkMap {

		// prune the walk AFTER the position of nodeID
		cutIndex := slices.Index(walk, nodeID) + 1

		// with probability alpha, generate a new walk segment that will replace the old segment
		var newWalkSegment models.RandomWalk
		if rng.Float32() < RWM.Store.Alpha(ctx) {

			newWalkSegment, err = generateWalkSegment(ctx, DB, added, walk[:cutIndex], RWM.Store.Alpha(ctx), rng)
			if err != nil {
				return err
			}
		}

		// prune and graft the walk with the new walk segment
		if err := RWM.Store.PruneGraftWalk(ctx, walkID, cutIndex, newWalkSegment); err != nil {
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
func generateWalkSegment(
	ctx context.Context,
	DB models.Database,
	candidateNodes []uint32,
	currentWalk models.RandomWalk,
	alpha float32,
	rng *rand.Rand) (models.RandomWalk, error) {

	successorID, stop := WalkStep(candidateNodes, currentWalk, rng)
	if stop {
		return models.RandomWalk{}, nil
	}

	newWalkSegment, err := generateWalk(ctx, DB, successorID, alpha, rng)
	if err != nil {
		return nil, err
	}

	return sliceutils.DeleteCyclesInPlace(currentWalk, newWalkSegment), nil
}

// containsInvalidStep() returns the index or position where the RandomWalk needs to be
// Pruned and Grafted. This happens if the walk contains an invalid hop nodeID --> removedNode in removedNodes.
func containsInvalidStep(walk models.RandomWalk, nodeID uint32, removedNodes []uint32) (int, bool) {
	for i := 0; i < len(walk)-1; i++ {
		// if it contains a hop (nodeID --> removedNode)
		if walk[i] == nodeID && slices.Contains(removedNodes, walk[i+1]) {
			// it needs to be updated from (i+1)th element (included) onwards
			cutIndex := i + 1
			return cutIndex, true
		}
	}
	return -1, false
}

// estimateWalksToUpdate() returns the number of walks that needs to be updated in updateAddedNodes().
// This number is (addedSize / currentSize) * numberOfWalks.
func estimateWalksToUpdate(
	ctx context.Context,
	RWM *RandomWalkManager,
	nodeID uint32,
	addedSize, currentSize int) (int, error) {

	walkMap, err := RWM.Store.VisitCounts(ctx, []uint32{nodeID})
	if err != nil {
		return 1, err
	}

	walksNum := float32(walkMap[nodeID])
	p := float32(addedSize) / float32(currentSize)
	return int(p * walksNum), nil
}
