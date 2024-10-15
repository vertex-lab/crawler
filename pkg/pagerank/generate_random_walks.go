package pagerank

import (
	"math/rand"
	"time"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

/*
generateWalk; generates a single walk ([]uint32) from a specified starting node.
The function returns an error if the DB cannot find the successorIDs of a node.

It's important to note that the walk breaks early when a cycle is encountered.
This behaviour simplifies the data structure (now a walk visits a node only once)
and helps with mitigating self-boosting spam networks. At the same time this doesn't
influence much the ranking of normal users since a cycle occurance is very inprobable.

Read more here:
*/
func generateWalk(DB graph.Database, startingNodeID uint32,
	alpha float32, rng *rand.Rand) ([]uint32, error) {

	// check if startingNodeID is in the DB
	if _, err := DB.NodeByID(startingNodeID); err != nil {
		return nil, err
	}

	currentNodeID := startingNodeID
	walk := []uint32{currentNodeID}

walkGeneration:
	for {
		// stop with probability 1-alpha
		if rng.Float32() > alpha {
			break walkGeneration
		}

		// get the successorIDs of the current node
		successorIDs, err := DB.NodeSuccessorIDs(currentNodeID)
		if err != nil {
			return nil, err
		}

		// if it is a dandling node, stop the walk
		succLenght := len(successorIDs)
		if succLenght == 0 {
			break walkGeneration
		}

		// randomly select the next node, and set is as the current one
		randomIndex := rng.Intn(succLenght)
		currentNodeID = successorIDs[randomIndex]

		/* if there is a cycle (node already visited), break the walk generation.
		Traversing the slice is faster than using sets because walks are short
		(1/(1-alpha) long on average). */
		for _, prevNodeID := range walk {

			if currentNodeID == prevNodeID {
				break walkGeneration
			}
		}

		// else, add to the walk
		walk = append(walk, currentNodeID)
	}

	return walk, nil
}

/*
GenerateRandomWalks generates `walksPerNode` random walks for each node in
the database using dampening factor `alpha`. The walks pointers are stored in
the RandomWalksManager struct.

INPUTS
------

	> DB: graph.Database
	The database where nodes are stored

OUTPUT
------

	> error: look at checkInputs() to view all the errors codes

NOTE
----

	This function is computationally expensive and should be called only when
	the RandomWalksManager is empty. During the normal execution of the program,
	there should always be random walks, so we should not re-do them from scratch,
	but just update them when necessary (e.g. when there is a graph update), using
	the UpdateRandomWalks method.

REFERENCES
----------

	[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
	URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (RWM *RandomWalksManager) GenerateRandomWalks(DB graph.Database) error {

	const expectEmptyRWM = true
	err := checkInputs(RWM, DB, expectEmptyRWM)
	if err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RWM.generateRandomWalks(DB, nil, rng)
}

/*
generateRandomWalks implement the logic that generates `walksPerNode“ random walks,
starting from each node in the slice nodeIDs. These walks are then added to the
RandomWalksManager struct.

It accepts a random number generator for reproducibility in tests.
*/
func (RWM *RandomWalksManager) generateRandomWalks(DB graph.Database,
	nodeIDs []uint32, rng *rand.Rand) error {

	// If no specific nodeIDs are provided, retrieve all nodes from the DB
	if nodeIDs == nil {
		var err error
		nodeIDs, err = DB.AllNodeIDs()
		if err != nil {
			return err
		}
	}

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

			// add the RandomWalk's pointer to the RandomWalksManager
			randomWalk := RandomWalk{NodeIDs: walk}
			RWM.AddWalk(&randomWalk)
		}
	}

	// TODO; store the RWM on an in-memory database (e.g. Redis)
	return nil
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

	if RWM.alpha <= 0 || RWM.alpha >= 1 {
		return ErrInvalidAlpha
	}

	if RWM.walksPerNode <= 0 {
		return ErrInvalidWalksPerNode
	}

	return nil
}
