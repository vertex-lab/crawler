package walks

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"time"

	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
)

/*
Update() updates the RandomWalkManager when a node's follows changes.
These changes are represented by some removed follows, common follows and added follows.
It returns the number of walks that have been updated, and an error.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (RWM *RandomWalkManager) Update(
	ctx context.Context,
	DB models.Database,
	nodeID uint32,
	removed, common, added []uint32) (int, error) {

	if err := DB.Validate(); err != nil {
		return 0, fmt.Errorf("Update(): %w", err)
	}

	if err := RWM.Store.Validate(); err != nil {
		return 0, fmt.Errorf("Update(): %w", err)
	}

	if !DB.ContainsNode(ctx, nodeID) {
		return 0, fmt.Errorf("Update(): %w: %v", models.ErrNodeNotFoundDB, nodeID)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	updated1, err := RWM.updateRemovedNodes(ctx, DB, nodeID, removed, common, rng)
	if err != nil {
		return updated1, fmt.Errorf("Update(): %w", err)
	}

	followsCount := len(common) + len(added)
	updated2, err := RWM.updateAddedNodes(ctx, DB, nodeID, added, followsCount, rng)
	if err != nil {
		return updated1 + updated2, fmt.Errorf("Update(): %w", err)
	}

	return updated1 + updated2, nil
}

/*
updateRemovedNodes() updates the RWM by "pruning" and "grafting" all the walks that
contain the invalid step `nodeID` --> `removedID` in `removed`.

After the execution of this method, the state of the walks of nodeID is as if nodeID
only follows are the common follows `common`.

It returns the number of walks updated, and an error.
*/
func (RWM *RandomWalkManager) updateRemovedNodes(
	ctx context.Context,
	DB models.Database,
	nodeID uint32,
	removed, common []uint32,
	rng *rand.Rand) (int, error) {

	if len(removed) == 0 {
		return 0, nil
	}

	// fetching only the walks that contain nodeID AND at least one of the removed nodes.
	var walkIDs []uint32
	for _, r := range removed {
		IDs, err := RWM.Store.WalksVisitingAll(ctx, nodeID, r)
		if err != nil {
			return 0, fmt.Errorf("here: updateRemovedNodes(): %w", err)
		}

		walkIDs = append(walkIDs, IDs...)
	}

	walkIDs = sliceutils.Unique(walkIDs) // removing duplicates
	walks, err := RWM.Store.Walks(ctx, walkIDs...)
	if err != nil {
		return 0, fmt.Errorf("updateRemovedNodes(): %w", err)
	}

	var updated int
	for i, ID := range walkIDs {
		walk := walks[i]

		cutIndex, contains := containsInvalidStep(walk, nodeID, removed)
		if !contains {
			continue
		}

		// generate a new walk segment that will replace the invalid segment of the walk
		newSegment, err := generateWalkSegment(ctx, DB, common, walk[:cutIndex], RWM.Store.Alpha(ctx), rng)
		if err != nil {
			return updated, fmt.Errorf("updateRemovedNodes(): %w", err)
		}

		// prune and graft the walk with the new walk segment
		if err = RWM.Store.PruneGraftWalk(ctx, ID, cutIndex, newSegment); err != nil {
			return updated, fmt.Errorf("updateRemovedNodes(): %w", err)
		}

		updated++
	}

	return updated, nil
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
	rng *rand.Rand) (int, error) {

	if len(added) == 0 {
		return 0, nil
	}

	limit, err := estimateWalksToUpdate(ctx, RWM, nodeID, len(added), followsCount)
	if err != nil {
		return 0, fmt.Errorf("updateAddedNodes(): %w", err)
	}

	walkIDs, err := RWM.Store.WalksVisiting(ctx, limit, nodeID)
	if err != nil {
		return 0, fmt.Errorf("updateAddedNodes(): %w", err)
	}

	walks, err := RWM.Store.Walks(ctx, walkIDs...)
	if err != nil {
		return 0, fmt.Errorf("updateAddedNodes(): %w", err)
	}

	var updated int
	for i, ID := range walkIDs {
		walk := walks[i]

		// prune the walk AFTER the position of nodeID
		cutIndex := slices.Index(walk, nodeID) + 1

		// with probability alpha, generate a new walk segment that will replace the old segment
		var newSegment models.RandomWalk
		if rng.Float32() < RWM.Store.Alpha(ctx) {

			newSegment, err = generateWalkSegment(ctx, DB, added, walk[:cutIndex], RWM.Store.Alpha(ctx), rng)
			if err != nil {
				return updated, fmt.Errorf("updateAddedNodes(): %w", err)
			}
		}

		// prune and graft the walk with the new walk segment
		if err := RWM.Store.PruneGraftWalk(ctx, ID, cutIndex, newSegment); err != nil {
			return updated, fmt.Errorf("updateAddedNodes(): %w", err)
		}

		updated++
	}

	return updated, nil
}

/*
generateWalkSegment() is responsible for generating a walk segment that will be
grafted (appended) to the currentWalk. It selectes the next node from a slice of
candidates, and ensures that the currentWalk + newSegment doesn't contain any cycle.
*/
func generateWalkSegment(
	ctx context.Context,
	DB models.Database,
	candidates []uint32,
	currentWalk models.RandomWalk,
	alpha float32,
	rng *rand.Rand) (models.RandomWalk, error) {

	nextID, stop := WalkStep(candidates, currentWalk, rng)
	if stop {
		return models.RandomWalk{}, nil
	}

	newSegment, err := generateWalk(ctx, DB, nextID, alpha, rng)
	if err != nil {
		return nil, fmt.Errorf("generateWalkSegment(): %w", err)
	}

	return sliceutils.DeleteCyclesInPlace(currentWalk, newSegment), nil
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

	visitsByNode, err := RWM.Store.VisitCounts(ctx, nodeID)
	if err != nil {
		return 0, fmt.Errorf("estimateWalksToUpdate(): %w", err)
	}

	if len(visitsByNode) != 1 {
		return 0, fmt.Errorf("estimateWalksToUpdate(): visitsByNode has len %d instead of 1", len(visitsByNode))
	}

	visits := float32(visitsByNode[0])             // the number of walks that visit (go through) nodeID
	p := float32(addedSize) / float32(currentSize) // the ratio of walks that are impacted by the added follows

	return int(p*visits + 0.5), nil // int() rounds to the smaller nearest integer. +0.5 makes sure it rounds to the nearest overall.
}
