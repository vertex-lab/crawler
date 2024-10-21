package walks

import (
	"math/rand"
	"slices"
	"time"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
)

/*
generates a single walk []uint32 from a specified starting node.
The function returns an error if the DB cannot find the successorIDs of a node.

It's important to note that the walk breaks early when a cycle is encountered.
This behaviour simplifies the data structure (now a walk visits a node only once,
so we can use Sets) and helps with mitigating self-boosting spam networks.

At the same time this doesn't influence much the ranking of normal users
since a cycle occurance is very inprobable.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func generateWalk(DB graph.Database, startingNodeID uint32,
	alpha float32, rng *rand.Rand) ([]uint32, error) {

	// check if startingNodeID is in the DB
	if _, err := DB.NodeByID(startingNodeID); err != nil {
		return nil, err
	}

	currentNodeID := startingNodeID
	walk := []uint32{currentNodeID}

	for {
		// stop with probability 1-alpha
		if rng.Float32() > alpha {
			break
		}

		// get the successorIDs of the current node
		// This can be improved by fetching directly a random successor
		successorIDs, err := DB.NodeSuccessorIDs(currentNodeID)
		if err != nil {
			return nil, err
		}

		// if it is a dandling node, stop the walk
		succLenght := len(successorIDs)
		if succLenght == 0 {
			break
		}

		// randomly select the next node, and set is as the current one
		randomIndex := rng.Intn(succLenght)
		currentNodeID = successorIDs[randomIndex]

		// if there is a cycle, break the walk generation
		if slices.Contains(walk, currentNodeID) {
			break
		}

		// else, append to the walk
		walk = append(walk, currentNodeID)
	}

	return walk, nil
}

/*
generateRandomWalks implement the logic that generates `walksPerNode` random walks,
starting from each node in the slice nodeIDs. The walk pointers are added to the RandomWalksManager.

It accepts a random number generator for reproducibility in tests.
*/
func (RWM *RandomWalksManager) generateRandomWalks(DB graph.Database,
	nodeIDs []uint32, rng *rand.Rand) error {

	// unpack the parameters
	alpha := RWM.alpha
	walksPerNode := RWM.walksPerNode

	// for each node, perform `walksPerNode` random walks
	for _, nodeID := range nodeIDs {
		for i := uint16(0); i < walksPerNode; i++ {

			walk, err := generateWalk(DB, nodeID, alpha, rng)
			if err != nil {
				return err
			}

			// add the RandomWalk's pointer to the RWM
			RWM.AddWalk(&RandomWalk{NodeIDs: walk})
		}
	}

	return nil
}

/*
generates `walksPerNode` random walks for a single node using dampening
factor `alpha`. The walk pointers are added to the RandomWalksManager.
*/
func (RWM *RandomWalksManager) Generate(DB graph.Database, nodeID uint32) error {

	// checking the inputs
	const expectEmptyRWM = false
	err := checkInputs(RWM, DB, expectEmptyRWM)
	if err != nil {
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
generates `walksPerNode` random walks for ALL nodes in the database using dampening
factor `alpha`. The walk pointers are added to the RandomWalksManager.

NOTE:

This function is computationally expensive and should be called only when
the RandomWalksManager is empty. During the normal execution of the program,
there should always be random walks, so we should not re-do them from scratch,
but just update them when necessary (e.g. when there is a graph update), using
the Update() method.
*/
func (RWM *RandomWalksManager) GenerateAll(DB graph.Database) error {

	const expectEmptyRWM = true
	err := checkInputs(RWM, DB, expectEmptyRWM)
	if err != nil {
		return err
	}

	nodeIDs, err := DB.AllNodeIDs()
	if err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RWM.generateRandomWalks(DB, nodeIDs, rng)
}

// checkInputs function is used to check whether the inputs are valid.
// If not, an appropriate error is returned
func checkInputs(RWM *RandomWalksManager, DB graph.Database, expectEmptyRWM bool) error {

	// checks if DB is nil or an empty database
	err := DB.CheckEmpty()
	if err != nil {
		return err
	}

	// checks if RWM is not nil and whether it should be empty or not
	err = RWM.CheckState(expectEmptyRWM)
	if err != nil {
		return err
	}

	return nil
}
