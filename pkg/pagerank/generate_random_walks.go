package pagerank

import (
	"errors"
	"math/rand"
	"time"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

/*
GenerateRandomWalks generates `walksPerNode` random walks for each node in
the database using dampening factor `alpha`. The walks pointers are stored in
the RandomWalksMap struct.

INPUTS
------

	> DB: graph.Database
	The database where nodes are stored

OUTPUT
------

	> error: look at checkInputs() to read all the errors

NOTE
----

	This function is computationally expensive and should be called only when
	the RandomWalksMap is empty. During the normal execution of the program,
	there should always be random walks, so we should not re-do them from scratch,
	but just update them when necessary (e.g. when there is a graph update).
	checkInputs checks if the RandomWalksMap is empty.

REFERENCES
----------

	[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
	link: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (RWM *RandomWalksMap) GenerateRandomWalks(DB graph.Database) error {

	const expectEmptyRWM = true

	// checking all the inputs
	err := checkInputs(RWM, DB, expectEmptyRWM)
	if err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RWM.generateRandomWalks(DB, rng)
}

func (RWM *RandomWalksMap) generateRandomWalks(DB graph.Database,
	rng *rand.Rand) error {

	// unpack the parameters of the random walks
	alpha := RWM.alpha
	walksPerNode := RWM.walksPerNode

	// get all the nodes ids to iterate over them
	nodeIDs, err := DB.GetAllNodeIDs()
	if err != nil {
		return err
	}

	// for each node, perform `walksPerNode` random walks
	for _, nodeID := range nodeIDs {
		for i := uint16(0); i < walksPerNode; i++ {

			walk, err := generateWalk(DB, nodeID, alpha, rng)
			if err != nil {
				return err
			}

			// add the RandomWalk's pointer to the RandomWalksMap
			randomWalk := RandomWalk{NodeIDs: walk}
			RWM.AddWalk(&randomWalk)
		}
	}

	// TODO; store the rwn on an in-memory database (e.g. Redis)
	return nil
}

// generateWalk; generate a single walk ([]uint32) from a specified starting node.
// The function returns an error if the DB cannot find the successors of a node
func generateWalk(DB graph.Database, startingNodeID uint32,
	alpha float32, rng *rand.Rand) ([]uint32, error) {

	walk := []uint32{startingNodeID}
	currentNodeID := startingNodeID

	for {
		// stop with probability 1-alpha
		if rng.Float32() > alpha {
			break
		}

		// get the successors of the current node
		successors, err := DB.GetNodeSuccessorIDs(currentNodeID)
		if err != nil {
			return nil, err
		}

		// if it is a dandling node, stop the walk
		succSize := len(successors)
		if succSize == 0 {
			break
		}

		// randomly select the next node, and set is as the current one
		randomIndex := rng.Intn(succSize)
		currentNodeID = successors[randomIndex]

		walk = append(walk, currentNodeID)
	}

	return walk, nil
}

// checkInputs function is used to check whether the inputs are valid.
// If not, an appropriate error is returned
func checkInputs(RWM *RandomWalksMap, DB graph.Database, expectEmptyRWM bool) error {

	// checks if DB is nil or an empty database
	err := DB.CheckEmpty()
	if err != nil {
		return err
	}

	// checks if RWM is valid and whether it should be empty or not
	err = checkRWMState(RWM, expectEmptyRWM)
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

// checkRWMState checks if the RWM is empty or non-empty based on the requirement.
func checkRWMState(RWM *RandomWalksMap, expectEmptyRWM bool) error {
	err := RWM.CheckEmpty()

	// if the RWM is nil, return the appropriate error
	if errors.Is(err, ErrNilRWMPointer) {
		return err
	}

	// When the RWM is empty but it shouldn't be
	if errors.Is(err, ErrEmptyRWM) && !expectEmptyRWM {
		return ErrEmptyRWM
	}

	// Handle the case when the RWM is not empty but it should be
	if !errors.Is(err, ErrEmptyRWM) && expectEmptyRWM {
		return ErrRWMIsNotEmpty
	}

	// If the error isn't related to being empty, return the original error
	if err != nil && !errors.Is(err, ErrEmptyRWM) {
		return err
	}

	return nil
}

//--------------------------------- ERROR-CODES---------------------------------

var ErrRWMIsNotEmpty = errors.New("the RWM is NOT empty")
