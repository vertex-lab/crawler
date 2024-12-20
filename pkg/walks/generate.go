package walks

import (
	"context"
	"math"
	"math/rand"
	"slices"
	"time"

	"github.com/vertex-lab/crawler/pkg/models"
)

/*
Generate() generates `walksPerNode` random walks for a single node using dampening
factor `alpha`. The walks are added to the RandomWalkStore.
*/
func (RWM *RandomWalkManager) Generate(
	ctx context.Context,
	DB models.Database,
	nodeID uint32) error {

	if err := DB.Validate(); err != nil {
		return err
	}

	if err := RWM.Store.Validate(); err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RWM.generateWalks(ctx, DB, []uint32{nodeID}, rng)
}

/*
GenerateAll() generates `walksPerNode` random walks for ALL nodes in the database
using dampening factor `alpha`. The walk pointers are added to the RandomWalkStore.

# NOTE:

This function is computationally expensive and should be called only when
the RandomWalkManager is empty. During the normal execution of the program,
there should always be random walks, so we should not re-do them from scratch,
but just update them when necessary (e.g. when there is a graph update), using
the Update() method.
*/
func (RWM *RandomWalkManager) GenerateAll(ctx context.Context, DB models.Database) error {

	if err := DB.Validate(); err != nil {
		return err
	}

	if err := RWM.Store.Validate(); err != nil {
		return err
	}

	nodeIDs, err := DB.AllNodes(ctx)
	if err != nil {
		return err
	}

	if len(nodeIDs) == 0 {
		return models.ErrEmptyDB
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RWM.generateWalks(ctx, DB, nodeIDs, rng)
}

/*
generateRandomWalks implement the logic that generates `walksPerNode` random walks,
starting from each node in the slice nodeIDs. The walks are added to the RandomWalkStore.

It accepts a random number generator for reproducibility in tests.
*/
func (RWM *RandomWalkManager) generateWalks(ctx context.Context,
	DB models.Database, nodeIDs []uint32, rng *rand.Rand) error {

	if len(nodeIDs) == 0 {
		return nil
	}

	alpha := RWM.Store.Alpha(ctx)
	walksPerNode := RWM.Store.WalksPerNode(ctx)

	// for each node, perform `walksPerNode` random walks and add them to the RWS
	for _, nodeID := range nodeIDs {

		walks := make([]models.RandomWalk, 0, walksPerNode)
		for i := uint16(0); i < walksPerNode; i++ {
			walk, err := generateWalk(ctx, DB, nodeID, alpha, rng)
			if err != nil {
				return err
			}

			walks = append(walks, walk)
		}

		if err := RWM.Store.AddWalks(ctx, walks...); err != nil {
			return err
		}
	}

	return nil
}

/*
generateWalk() generates a single walk from a specified starting node.
The function returns an error if the DB cannot find the successorIDs of a node.

It's important to note that the walk breaks early when a cycle is encountered.
This behaviour simplifies the data structure (now a walk visits a node only once,
so we can use Sets) and helps with mitigating self-boosting spam networks.

At the same time this doesn't influence much the ranking of normal users
since a cycle occurance is very improbable.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf

[2] Pippellia; To-be-written-paper on acyclic Monte-Carlo Pagerank
*/
func generateWalk(
	ctx context.Context,
	DB models.Database,
	startingNodeID uint32,
	alpha float32,
	rng *rand.Rand) (models.RandomWalk, error) {

	if !DB.ContainsNode(ctx, startingNodeID) {
		return nil, models.ErrNodeNotFoundDB
	}

	var shouldBreak bool
	currentNodeID := startingNodeID
	walk := models.RandomWalk{currentNodeID}

	for {
		// stop with probability 1-alpha
		if rng.Float32() > alpha {
			break
		}

		// get the successorIDs of the current node. This can be improved by checking a successor cache first.
		successorIDs, err := DB.Follows(ctx, currentNodeID)
		if err != nil {
			return nil, err
		}

		currentNodeID, shouldBreak = WalkStep(successorIDs[0], walk, rng)
		if shouldBreak {
			break
		}

		walk = append(walk, currentNodeID)
	}

	return walk, nil
}

/*
performs a walk step nodeID --> nextNodeID in successorIDs and returns
`nextID` and `stop`.

`stop` is true if and only if:

- successorIDs is empty

- nextNodeID was already visited in one of the previous steps (walk). In other
words, when a cycle is found.
*/
func WalkStep(successorIDs, walk []uint32, rng *rand.Rand) (nextID uint32, stop bool) {

	// if it is a dandling node, stop
	succLenght := len(successorIDs)
	if succLenght == 0 {
		return math.MaxUint32, true
	}

	randomIndex := rng.Intn(succLenght)
	nextID = successorIDs[randomIndex]

	// if there is a cycle, stop
	if slices.Contains(walk, nextID) {
		return math.MaxUint32, true
	}

	return nextID, false
}

// Remove() removes all the walks that originated from nodeID.
func (RWM *RandomWalkManager) Remove(ctx context.Context, nodeID uint32) error {

	if err := RWM.Store.Validate(); err != nil {
		return err
	}

	walkIDs, err := RWM.Store.WalksVisiting(ctx, -1, nodeID)
	if err != nil {
		return err
	}

	walks, err := RWM.Store.Walks(ctx, walkIDs...)
	if err != nil {
		return err
	}

	walksToRemove := make([]uint32, 0, RWM.Store.WalksPerNode(ctx))
	for i, ID := range walkIDs {
		if !startsWith(walks[i], nodeID) {
			continue
		}

		walksToRemove = append(walksToRemove, ID)
	}

	return RWM.Store.RemoveWalks(ctx, walksToRemove...)
}

// startsWith() returns whether walk starts with nodeID.
func startsWith(walk models.RandomWalk, nodeID uint32) bool {
	if len(walk) == 0 {
		return false
	}

	return walk[0] == nodeID
}
