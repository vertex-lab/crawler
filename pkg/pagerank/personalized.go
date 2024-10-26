package pagerank

import (
	"errors"
	"math/rand"
	"time"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

/*
computes the personalized pagerank of nodeID by simulating a long random walkSegment starting at
and resetting to itself. This long walkSegment is generated from the
random walks stored in the RandomWalkManager.

# INPUTS

	> DB graph.Database
	The interface of the graph database

	> RWM *walks.RandomWalksManager
	The structure that manages the random walks for each node

	> nodeID uint32
	The ID of the node we are going to compute the personalized pagerank

	> topK uint16
	The number of nodes with the highest personalized pagerank that the
	algorithm aims to identify and converge on. Increasing this parameter
	improves the precision for all nodes but increases the computational cost.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func Personalized(DB graph.Database, RWM *walks.RandomWalksManager,
	nodeID uint32, topK uint16) (PagerankMap, error) {

	err := checkInputs(DB, RWM, nodeID, topK)
	if err != nil {
		return nil, err
	}

	// for reproducibility in tests
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return personalized(DB, RWM, nodeID, topK, rng)
}

// implement the internal logic of the Personalized Pagerank function
func personalized(DB graph.Database, RWM *walks.RandomWalksManager,
	nodeID uint32, topK uint16, rng *rand.Rand) (PagerankMap, error) {

	if DB.IsDandling(nodeID) {
		return PagerankMap{nodeID: 1.0}, nil
	}

	pWalk, err := personalizedWalk(DB, RWM, nodeID, requiredLenght(topK), rng)
	if err != nil {
		return nil, err
	}

	_ = pWalk
	// pp := countAndNormalize(pWalk, RWM.Alpha)
	// return pp, nil

	return nil, nil
}

/*
simulates a long personalized random walkSegment starting from startingNodeID with reset to itself.
This personalized walkSegment is generated using the random walks stored in the in the RandomWalkManager.

To avoid the overhead of continually fetching walks from the RWM, the requests
are batched and the walks are stored in the WalkCache struct.
*/
func personalizedWalk(DB graph.Database, RWM *walks.RandomWalksManager,
	startingNodeID uint32, requiredLenght int, rng *rand.Rand) ([][]uint32, error) {

	WC := NewWalkCache()
	pWalk := make([][]uint32, 0, requiredLenght)

	err := WC.Load(RWM, startingNodeID, estimateWalksNum(requiredLenght, RWM.Alpha))
	if err != nil {
		return nil, err
	}

	// initializing the first walk
	currentNodeID := startingNodeID
	currentWalk := []uint32{startingNodeID}

	for {

		// the exit condition
		if len(pWalk) >= requiredLenght {
			break
		}

		// append the current walk and reset
		if rng.Float32() > RWM.Alpha {
			pWalk = append(pWalk, currentWalk)
			currentNodeID = startingNodeID
			currentWalk = []uint32{startingNodeID}
			continue
		}

		// if there are no walks, load them
		if !WC.Contains(currentNodeID) {
			err := WC.Load(RWM, currentNodeID, 1000)
			if err != nil {
				return nil, err
			}
		}

		// if all walks have been used, do a walk step
		if WC.FullyUsed(currentNodeID) {

			// fetch the successors
			successorIDs, err := DB.NodeSuccessorIDs(currentNodeID)
			if err != nil {
				return nil, err
			}

			// perform a walk step
			currentNodeID, shouldStop := walks.WalkStep(successorIDs, currentWalk, rng)

			if shouldStop {
				pWalk = append(pWalk, currentWalk)
				currentNodeID = startingNodeID
				currentWalk = []uint32{startingNodeID}
				continue
			}

			// append and continue
			currentWalk = append(currentWalk, currentNodeID)
			continue
		}

		// else, get the next walk
		walkSegment, err := WC.NextWalk(currentNodeID)
		if err != nil {
			return nil, err
		}

		// remove potential cycles
		walkSegment = walks.RemoveCycles(currentWalk, walkSegment)

		// append current walk
		currentWalk = append(currentWalk, walkSegment...)
		pWalk = append(pWalk, currentWalk)

		// reset
		currentNodeID = startingNodeID
		currentWalk = []uint32{startingNodeID}
		continue
	}

	return pWalk, nil
}

// count the number of times each node is visited in the pWalk and computes their frequencies.
// Returns an empty map if pWalk is nil or empty.
// It panics if alpha = 1 (division by )
func countAndNormalize(pWalk []uint32, alpha float32) PagerankMap {

	estimateCapacity := int(float32(len(pWalk)) / (1 - alpha))
	pp := make(PagerankMap, estimateCapacity)

	// count the frequency of each nodeID
	for _, node := range pWalk {
		pp[node]++
	}

	// normalize
	totalVisits := float64(len(pWalk))
	for node, visits := range pp {
		pp[node] = visits / totalVisits
	}

	return pp
}

func estimateWalksNum(lenght int, alpha float32) int {
	return int(float32(lenght) / (1 - alpha))
}

// returns the required lenght of the walkSegment for the Personalized pagerank.
// the result has to be strictly positive
func requiredLenght(topK uint16) int {

	_ = topK
	return 300000
}

// function that checks the inputs of Personalized Pagerank;
func checkInputs(DB graph.Database, RWM *walks.RandomWalksManager,
	nodeID uint32, topK uint16) error {

	err := DB.CheckEmpty()
	if err != nil {
		return err
	}

	const expectedEmpty = false
	err = RWM.CheckState(expectedEmpty)
	if err != nil {
		return err
	}

	// check if nodeID is in the DB
	if _, err := DB.NodeByID(nodeID); err != nil {
		return err
	}

	// check if nodeID is in the RWM
	if _, err := RWM.WalksByNodeID(nodeID); err != nil {
		return err
	}

	if topK <= 0 {
		return ErrInvalidTopN
	}

	return nil
}

// ---------------------------------ERROR-CODES--------------------------------

var ErrInvalidTopN = errors.New("topK shoud be greater than 0")
