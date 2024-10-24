package pagerank

import (
	"errors"
	"math/rand"
	"time"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

/*
computes the personalized pagerank of nodeID by simulating a long random walk starting at
and resetting to itself. This long walk is generated from the
random walks stored in the RandomWalkManager.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func Personalized(DB graph.Database, RWM *walks.RandomWalksManager,
	nodeID uint32, topN uint16) (PagerankMap, error) {

	err := checkInputs(DB, RWM, topN)
	if err != nil {
		return nil, err
	}

	// for reproducibility in tests
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return personalized(DB, RWM, nodeID, topN, rng)
}

// implement the internal logic of the Personalized Pagerank function
func personalized(DB graph.Database, RWM *walks.RandomWalksManager,
	nodeID uint32, topN uint16, rng *rand.Rand) (PagerankMap, error) {

	pp := PagerankMap{}

	if DB.IsDandling(nodeID) {
		pp[nodeID] = 1.0
		return pp, nil
	}

	requiredLenght := requiredLenght(topN)
	longWalk, err := personalizedWalk(DB, RWM, nodeID, requiredLenght, rng)
	if err != nil {
		return nil, err
	}

	// count the frequency of each nodeID
	for _, node := range longWalk {
		pp[node]++
	}

	// normalize
	totalVisits := float64(len(longWalk))
	for node, visits := range pp {
		pp[node] = visits / totalVisits
	}

	return pp, nil
}

/*
simulates a long random walk starting from nodeID with reset to itself.
This long walk is generated from the already executed random walks stored in the
in the RandomWalkManager.

To avoid the overhead of continually fetching walks from the RWM, the requests
are batched and the walks are stored in the WalkCache struct.
*/
func personalizedWalk(DB graph.Database, RWM *walks.RandomWalksManager,
	startingNodeID uint32, requiredLenght int, rng *rand.Rand) ([]uint32, error) {

	WC := NewWalkCache()
	longWalk := make([]uint32, 0, requiredLenght)

	err := WC.Load(RWM, startingNodeID, requiredLenght)
	if err != nil {
		return nil, err
	}

	currentNodeID := startingNodeID

	for {

		// the exit condition
		if len(longWalk) >= requiredLenght {
			break
		}

		// append the current node
		longWalk = append(longWalk, currentNodeID)

		// reset to the starting node with probability 1 - alpha
		if rng.Float32() > RWM.Alpha {
			currentNodeID = startingNodeID
			continue
		}

		// get the next walk to be appended to the long walk
		walk, err := WC.NextWalk(currentNodeID)
		if err != nil {
			return nil, err
		}

		// if the walks have finished, do one walk step
		if walk == nil {
		}

		// append and reset the walk
		longWalk = append(longWalk, walk...)
		currentNodeID = startingNodeID

	}

	return nil, nil
}

// computes the required lenght of the walk for the Personalized pagerank
func requiredLenght(topN uint16) int {

	_ = topN
	return 300000
}

// function that checks the inputs of Personalized Pagerank;
func checkInputs(DB graph.Database, RWM *walks.RandomWalksManager, topN uint16) error {

	err := DB.CheckEmpty()
	if err != nil {
		return err
	}

	const expectedEmpty = false
	err = RWM.CheckState(expectedEmpty)
	if err != nil {
		return err
	}

	if topN <= 0 {
		return ErrInvalidTopN
	}

	return nil
}

// ---------------------------------ERROR-CODES--------------------------------

var ErrInvalidTopN = errors.New("topN shoud be greater than 0")
