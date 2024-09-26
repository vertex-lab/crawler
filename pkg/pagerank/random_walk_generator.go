package pagerank

import (
	"errors"
	"math/rand"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

/*
GenerateRandomWalks generates `walk_per_node` random walks for each node in
the database using dampening factor `alpha`. The walks pointers are stored in
the RandomWalksMap struct.

INPUTS
------

	> db graph.Database:
	The database where nodes are stored

	> alpha:
	The dampening factor, which is also the probability of stopping at each step
	of the random walk. Default is 0.85

	> walks_per_node:
	The number of random walks to be performed for each node. Default is 10

OUTPUT
------

	> error:

NOTE
----

	This function is computationally expensive and should be called only when
	the RandomWalksMap is empty. During the normal execution of the program,
	there should always be random walks, so we shouldn't re-do them from scratch,
	but just update them when necessary (e.g. when there is a graph update)

REFERENCES
----------

	[1] B. Bahmani, A. Chowdhury, A. Goel, Fast Incremental and Personalized PageRank
	link: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (rwm *RandomWalksMap) GenerateRandomWalks(db graph.Database,
	alpha float32, walks_per_node uint16) error {

	// checking all the inputs
	err := checkInputs(rwm, db, alpha, walks_per_node)
	if err != nil {
		return err
	}

	// get all the nodes ids to iterate over them
	nodeIDs, err := db.GetAllNodeIDs()
	if err != nil {
		return err
	}

	// for each node, perform `walks_per_node` random walks
	for _, nodeID := range nodeIDs {
		for i := uint16(0); i < walks_per_node; i++ {

			walk := []uint32{nodeID}
			current_nodeID := nodeID

			for {
				// stop with probability 1-alpha
				if rand.Float32() > alpha {
					break
				}

				// get the successors of the current node
				successors, err := db.GetNodeSuccessorIDs(current_nodeID)
				if err != nil {
					return err
				}

				// if it is a dandling node, stop the walk
				if len(successors) == 0 {
					break
				}

				// randomly select the next node, and set is as the current one
				random_index := rand.Intn(len(successors))
				current_nodeID = successors[random_index]

				walk = append(walk, current_nodeID)
			}

			random_walk := RandomWalk{NodeIDs: walk}
			rwm.AddWalk(&random_walk)
		}
	}

	return nil
}

// checkInputs function is used in GenerateRandomWalks to check if the inputs
// are valid. If not, an appropriate error is returned
func checkInputs(rwm *RandomWalksMap, db graph.Database,
	alpha float32, walks_per_node uint16) error {

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

	if walks_per_node <= 0 {
		return ErrInvalidWalksPerNode
	}

	return nil
}

var ErrRWMIsNotEmpty = errors.New("the rwm is NOT empty")
var ErrInvalidAlpha = errors.New("alpha should be a number between 0 and 1 (excluded)")
var ErrInvalidWalksPerNode = errors.New("walks_per_node should be greater than zero")
