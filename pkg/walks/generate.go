package walks

import (
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
func (RWM *RandomWalkManager) Generate(DB models.Database, nodeID uint32) error {

	if err := checkInputs(RWM, DB, false); err != nil {
		return err
	}

	// if the node is already in the RWS, don't do anything
	// if RWM.Store.ContainsNode(nodeID) {
	// 	return nil
	// }

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RWM.generateWalks(DB, []uint32{nodeID}, rng)
}

/*
GenerateAll() generates `walksPerNode` random walks for ALL nodes in the database
using dampening factor `alpha`. The walk pointers are added to the RandomWalkStore.

NOTE:

This function is computationally expensive and should be called only when
the RandomWalkManager is empty. During the normal execution of the program,
there should always be random walks, so we should not re-do them from scratch,
but just update them when necessary (e.g. when there is a graph update), using
the Update() method.
*/
func (RWM *RandomWalkManager) GenerateAll(DB models.Database) error {

	if err := checkInputs(RWM, DB, true); err != nil {
		return err
	}

	nodeIDs, err := DB.AllNodes()
	if err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RWM.generateWalks(DB, nodeIDs, rng)
}

/*
generateRandomWalks implement the logic that generates `walksPerNode` random walks,
starting from each node in the slice nodeIDs. The walks are added to the RandomWalkStore.

It accepts a random number generator for reproducibility in tests.
*/
func (RWM *RandomWalkManager) generateWalks(DB models.Database,
	nodeIDs []uint32, rng *rand.Rand) error {

	// unpack the parameters
	alpha := RWM.Store.Alpha()
	walksPerNode := RWM.Store.WalksPerNode()

	// for each node, perform `walksPerNode` random walks and add them to the RWS
	for _, nodeID := range nodeIDs {

		walks := make([]models.RandomWalk, 0, walksPerNode)
		for i := uint16(0); i < walksPerNode; i++ {
			walk, err := generateWalk(DB, nodeID, alpha, rng)
			if err != nil {
				return err
			}

			walks = append(walks, walk)
		}

		if err := RWM.Store.AddWalks(walks); err != nil {
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
func generateWalk(DB models.Database, startingNodeID uint32,
	alpha float32, rng *rand.Rand) (models.RandomWalk, error) {

	if !DB.ContainsNode(startingNodeID) {
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

		// get the successorIDs of the current node. This can be improved
		// by checking a successor cache first.
		successorIDs, err := DB.Successors(currentNodeID)
		if err != nil {
			return nil, err
		}

		// perform a walk step; break if one of the condition in WalkStep is triggered
		currentNodeID, shouldBreak = WalkStep(successorIDs, walk, rng)
		if shouldBreak {
			break
		}

		// else, append to the walk
		walk = append(walk, currentNodeID)
	}

	return walk, nil
}

/*
performs a walk step nodeID --> nextNodeID in successorIDs and returns
`nextNodeID` and `shouldStop`.

`shouldStop` is true if and only if:

- successorIDs is empty

- nextNodeID was already visited in one of the previous steps (walk). In other
words, when a cycle is found.
*/
func WalkStep(successorIDs, walk []uint32, rng *rand.Rand) (uint32, bool) {

	// if it is a dandling node, stop
	succLenght := len(successorIDs)
	if succLenght == 0 {
		return math.MaxUint32, true
	}

	// randomly select the next node
	randomIndex := rng.Intn(succLenght)
	nextNodeID := successorIDs[randomIndex]

	// if there is a cycle, stop
	if slices.Contains(walk, nextNodeID) {
		return math.MaxUint32, true
	}

	return nextNodeID, false
}

// checkInputs function is used to check whether the inputs are valid.
// If not, an appropriate error is returned
func checkInputs(RWM *RandomWalkManager, DB models.Database, expectEmptyRWM bool) error {

	// checks if DB is nil or an empty database
	err := DB.Validate()
	if err != nil {
		return err
	}

	// checks if RWM is not nil and whether it should be empty or not
	err = RWM.Store.Validate(expectEmptyRWM)
	if err != nil {
		return err
	}

	return nil
}
