package walks

import (
	"context"
	"fmt"
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
func Generate(
	ctx context.Context,
	DB models.Database,
	RWS models.RandomWalkStore,
	nodeID uint32) error {

	if err := DB.Validate(); err != nil {
		return fmt.Errorf("failed to generate the walks of nodeID %d: DB validation failed: %w", nodeID, err)
	}

	if err := RWS.Validate(); err != nil {
		return fmt.Errorf("failed to generate the walks of nodeID %d: RWS validation failed: %w", nodeID, err)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	if err := generateWalks(ctx, rng, DB, RWS, nodeID); err != nil {
		return fmt.Errorf("failed to generate the walks of nodeID %d: %w", nodeID, err)
	}

	return nil
}

/*
GenerateAll() generates `walksPerNode` random walks for ALL nodes in the database
using dampening factor `alpha`.

# NOTE:

This function is computationally expensive and should be called only when
the RandomWalkManager is empty. During the normal execution of the program,
there should always be random walks, so we should not re-do them from scratch,
but just update them when necessary (e.g. when there is a graph update), using
the Update() method.
*/
func GenerateAll(
	ctx context.Context,
	DB models.Database,
	RWS models.RandomWalkStore) error {
	if err := DB.Validate(); err != nil {
		return fmt.Errorf("failed to generate the walks: DB validation failed: %w", err)
	}

	if err := RWS.Validate(); err != nil {
		return fmt.Errorf("failed to generate the walks: RWS validation failed: %w", err)
	}

	nodeIDs, err := DB.AllNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to generate the walks: %w", err)
	}

	if len(nodeIDs) == 0 {
		return fmt.Errorf("failed to generate the walks: %w", models.ErrEmptyDB)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	if err := generateWalks(ctx, rng, DB, RWS, nodeIDs...); err != nil {
		return fmt.Errorf("failed to generate the walks: %w", err)
	}

	return nil
}

/*
generateRandomWalks implement the logic that generates `walksPerNode` random walks,
starting from each node in the slice nodeIDs. The walks are added to the RandomWalkStore.

It accepts a random number generator for reproducibility in tests.
*/
func generateWalks(
	ctx context.Context,
	rng *rand.Rand,
	DB models.Database,
	RWS models.RandomWalkStore,
	nodeIDs ...uint32) error {

	if len(nodeIDs) == 0 {
		return nil
	}

	alpha := RWS.Alpha(ctx)
	walksPerNode := RWS.WalksPerNode(ctx)

	// for each node, perform `walksPerNode` walks and add them to the RWS
	for _, ID := range nodeIDs {
		if !DB.ContainsNode(ctx, ID) {
			return fmt.Errorf("generateWalks(): %w: %v", models.ErrNodeNotFoundDB, ID)
		}

		walks := make([]models.RandomWalk, walksPerNode)
		for i := uint16(0); i < walksPerNode; i++ {
			walk, err := generateWalk(ctx, rng, DB, ID, alpha)
			if err != nil {
				return fmt.Errorf("failed to generate walk: %w", err)
			}

			walks[i] = walk
		}

		if err := RWS.AddWalks(ctx, walks...); err != nil {
			return fmt.Errorf("failed to add walks: %w", err)
		}
	}

	return nil
}

/*
generateWalk() generates a single walk from a specified starting node.
The function returns an error if the DB cannot find the successorIDs of a node.

It's important to note that the walk breaks early when a cycle is encountered.
This behaviour simplifies the data structure (now a walk visits a node only once,
so we can use sets) and helps with mitigating self-boosting spam networks.

At the same time this doesn't influence much the ranking of normal users
since a cycle occurance is very improbable.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf

[2] Pippellia; To-be-written-paper on acyclic Monte-Carlo Pagerank
*/
func generateWalk(
	ctx context.Context,
	rng *rand.Rand,
	DB models.Database,
	startingID uint32,
	alpha float32) (models.RandomWalk, error) {

	var shouldBreak bool
	currentID := startingID
	walk := models.RandomWalk{currentID}

	for {
		// stop with probability 1-alpha
		if rng.Float32() > alpha {
			break
		}

		follows, err := DB.Follows(ctx, currentID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch the follows of nodeID %d: %w", currentID, err)
		}

		currentID, shouldBreak = WalkStep(rng, follows[0], walk)
		if shouldBreak {
			break
		}

		walk = append(walk, currentID)
	}

	return walk, nil
}

/*
performs a walk step nodeID --> nextID in successorIDs and returns
`nextID` and `stop`.

`stop` is true if and only if:

- successorIDs is empty

- nextNodeID was already visited in one of the previous steps (walk). In other
words, when a cycle is found.
*/
func WalkStep(rng *rand.Rand, follows, walk []uint32) (nextID uint32, stop bool) {

	// if it's a dandling node, stop
	followSize := len(follows)
	if followSize == 0 {
		return math.MaxUint32, true
	}

	randomIndex := rng.Intn(followSize)
	nextID = follows[randomIndex]

	// if there is a cycle, stop
	if slices.Contains(walk, nextID) {
		return math.MaxUint32, true
	}

	return nextID, false
}

// Remove() removes all the walks that originated from nodeID.
func Remove(ctx context.Context, RWS models.RandomWalkStore, nodeID uint32) error {
	if err := RWS.Validate(); err != nil {
		return fmt.Errorf("failed to remove the walks of nodeID %d: RWS validation failed: %w", nodeID, err)
	}

	walkIDs, err := RWS.WalksVisiting(ctx, -1, nodeID)
	if err != nil {
		return fmt.Errorf("failed to remove the walks of nodeID %d: failed to fetch walksVisitingAll: %w", nodeID, err)
	}

	walks, err := RWS.Walks(ctx, walkIDs...)
	if err != nil {
		return fmt.Errorf("failed to remove the walks of nodeID %d: failed to fetch walks from IDs: %w", nodeID, err)
	}

	walksToRemove := make([]uint32, 0, RWS.WalksPerNode(ctx))
	for i, ID := range walkIDs {
		if startsWith(walks[i], nodeID) {
			walksToRemove = append(walksToRemove, ID)
		}
	}

	if err := RWS.RemoveWalks(ctx, walksToRemove...); err != nil {
		return fmt.Errorf("failed to remove the walks of nodeID %d: %w", nodeID, err)
	}

	return nil
}

// startsWith() returns whether walk starts with nodeID.
func startsWith(walk models.RandomWalk, nodeID uint32) bool {
	if len(walk) == 0 {
		return false
	}

	return walk[0] == nodeID
}
