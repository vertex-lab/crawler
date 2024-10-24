package pagerank

import (
	"errors"
	"math/rand"
	"time"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)


type WalkCache struc {
	NodeWalks: map[uint32][]*walks.RandomWalk
}

/*
computes the personalized pagerank by simulating a long random walk starting from nodeID.
This long walk is generated from the already performed random walks stored in the
RandomWalkManager.

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

// implement the internal logic of the Personalized function
func personalized(DB graph.Database, RWM *walks.RandomWalksManager,
	nodeID uint32, topN uint16, rng *rand.Rand) (PagerankMap, error) {

	return nil, nil
}

// function that checks the inputs of Personalized;
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

var ErrInvalidTopN = errors.New("topN shoud be greater than 0")
