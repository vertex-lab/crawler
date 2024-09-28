package pagerank

import (
	"errors"
	"math/rand"
	"time"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

/*
GenerateRandomWalks generates `walk_per_node` random walks for each node in
the database using dampening factor `alpha`. The walks pointers are stored in
the RandomWalksMap struct.

INPUTS
------

	> db: graph.Database
	The database where nodes are stored

	> alpha: float32
	The dampening factor, which is the probability of stopping at each step
	of the random walk. Default is 0.85

	> walksPerNode: uint16
	The number of random walks to be performed for each node. Default is 10

OUTPUT
------

	> error: look at checkInputs() to read all the errors

NOTE
----

	This function is computationally expensive and should be called only when
	the RandomWalksMap is empty. During the normal execution of the program,
	there should always be random walks, so we shouldn't re-do them from scratch,
	but just update them when necessary (e.g. when there is a graph update).
	checkInputs checks if the RandomWalksMap is empty.

REFERENCES
----------

	[1] B. Bahmani, A. Chowdhury, A. Goel, Fast Incremental and Personalized PageRank
	link: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (rwm *RandomWalksMap) GenerateRandomWalks(db graph.Database,
	alpha float32, walksPerNode uint16) error {

	// checking all the inputs
	err := checkInputs(rwm, db, alpha, walksPerNode)
	if err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return rwm._generateRandomWalks(db, alpha, walksPerNode, rng)
}

func (rwm *RandomWalksMap) _generateRandomWalks(db graph.Database,
	alpha float32, walksPerNode uint16, rng *rand.Rand) error {

	// get all the nodes ids to iterate over them
	nodeIDs, err := db.GetAllNodeIDs()
	if err != nil {
		return err
	}

	// for each node, perform `walksPerNode` random walks
	for _, nodeID := range nodeIDs {
		for i := uint16(0); i < walksPerNode; i++ {

			walk := []uint32{nodeID}
			currentNodeID := nodeID

			for {
				// stop with probability 1-alpha
				if rng.Float32() > alpha {
					break
				}

				// get the successors of the current node
				successors, err := db.GetNodeSuccessorIDs(currentNodeID)
				if err != nil {
					return err
				}

				// if it is a dandling node, stop the walk
				if len(successors) == 0 {
					break
				}

				// randomly select the next node, and set is as the current one
				random_index := rng.Intn(len(successors))
				currentNodeID = successors[random_index]

				walk = append(walk, currentNodeID)
			}

			// add the RandomWalk's pointer to the RandomWalksMap
			random_walk := RandomWalk{NodeIDs: walk}
			rwm.AddWalk(&random_walk)
		}
	}

	return nil
}

// checkInputs function is used in GenerateRandomWalks to check if the inputs
// are valid. If not, an appropriate error is returned
func checkInputs(rwm *RandomWalksMap, db graph.Database,
	alpha float32, walksPerNode uint16) error {

	// checks if db is nil or an empty database
	err := db.CheckEmpty()
	if err != nil {
		return err
	}

	// checks if rwm is nil
	err = rwm.CheckEmpty()
	if err == nil {
		return ErrRWMIsNotEmpty
	}

	// checks if rwm is NOT Empty
	if !errors.Is(err, ErrEmptyRWM) {
		return err
	}

	if alpha <= 0 || alpha >= 1 {
		return ErrInvalidAlpha
	}

	if walksPerNode <= 0 {
		return ErrInvalidWalksPerNode
	}

	return nil
}

//--------------------------------- ERROR-CODES---------------------------------

var ErrRWMIsNotEmpty = errors.New("the rwm is NOT empty")
var ErrInvalidAlpha = errors.New("alpha should be a number between 0 and 1 (excluded)")
var ErrInvalidWalksPerNode = errors.New("walksPerNode should be greater than zero")
