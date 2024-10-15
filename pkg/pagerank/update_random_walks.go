package pagerank

import (
	"math/rand"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

/*
UpdateRandomWalks updates the RandomWalksManager when a node's successors change from
oldSuccessorIDs to currentSuccessorIDs.

INPUTS
------

	> DB: graph.Database
	The database where nodes are stored

	> nodeID: uint32
	The ID of the node that changed his successors from oldSuccessorIDs to currentSuccessorIDs

	> oldSuccessorIDs: []uint32
	The slice that contains the node IDs of the old successors of nodeID

OUTPUT
------

	> error: look at checkInputs() to read all the errors

NOTE
----

REFERENCES
----------

	[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
	URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (RWM *RandomWalksManager) UpdateRandomWalks(DB graph.Database,
	nodeID uint32, oldSuccessorIDs []uint32) error {

	// checking the inputs
	const expectEmptyRWM = false
	err := checkInputs(RWM, DB, expectEmptyRWM)
	if err != nil {
		return err
	}

	// checking that nodeID exists in the DB
	if _, err := DB.NodeByID(nodeID); err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RWM.updateRandomWalks(DB, nodeID, oldSuccessorIDs, rng)
}

// Implements the logic of updating the random walks. It accepts a random number
// generator for reproducibility in tests.
func (RWM *RandomWalksManager) updateRandomWalks(DB graph.Database,
	nodeID uint32, oldSuccessorIDs []uint32, rng *rand.Rand) error {

	// if nodeID isn't in the RWM, generate new walks from scratch
	if _, exist := RWM.WalksByNode[nodeID]; !exist {
		err := RWM.generateRandomWalks(DB, []uint32{nodeID}, rng)
		return err
	}

	// if nodeID is in the RWM, update the walks, starting by fetching the current successors
	currentSuccessorIDs, err := DB.NodeSuccessorIDs(nodeID)
	if err != nil {
		return err
	}

	// transform to Sets for more efficient operations and lookups
	oldSuccessorSet := mapset.NewSet(oldSuccessorIDs...)
	currentSuccessorSet := mapset.NewSet(currentSuccessorIDs...)

	// compute the removed and added nodes
	removedNodes := oldSuccessorSet.Difference(currentSuccessorSet)
	addedNodes := currentSuccessorSet.Difference(oldSuccessorSet)

	err = RWM.updateRemovedNodes(DB, nodeID, removedNodes, rng)
	if err != nil {
		return err
	}

	err = RWM.updateAddedNodes(DB, nodeID, addedNodes, rng)
	if err != nil {
		return err
	}

	return nil
}

/*
updateRemovedNodes is a method that updates the RWM by "pruning" and "grafting"
all the walks that contain the hop (nodeID --> removedNode).
*/
func (RWM *RandomWalksManager) updateRemovedNodes(DB graph.Database, nodeID uint32,
	removedNodes mapset.Set[uint32], rng *rand.Rand) error {

	if removedNodes.Cardinality() == 0 {
		return nil
	}

	walkSet, err := RWM.WalksByNodeID(nodeID)
	if err != nil {
		return err
	}

	// iterate over the walks
	for walk := range walkSet.Iter() {

		update, cutIndex, err := walk.NeedsUpdate(nodeID, removedNodes)
		if err != nil {
			return err
		}

		// if it doesn't need update, skip
		if !update {
			continue
		}

		// prune the walk
		err = RWM.PruneWalk(walk, cutIndex)
		if err != nil {
			return err
		}

		/* generate a new walk starting from nodeID; This walk is guaranteed
		to contain nodeID only in the first position, as walks can't
		have cycles. This is REQUIRED to avoid potential deadlocks
		(remember that we are accessing the WalkSet of nodeID, so we can't change it) */
		newWalk, err := generateWalk(DB, nodeID, RWM.alpha, rng)
		if err != nil {
			return err
		}

		// drop the fist element (nodeID) because it's already in the walk
		newWalkSegment := newWalk[1:]

		// graft the walk with the new walk segment
		err = RWM.GraftWalk(walk, newWalkSegment)
		if err != nil {
			return err
		}
	}

	return nil
}

// method that updates the RWM by "pruning" some randomly selected walks and
// by "grafting" them using the newly added nodes
func (RWM *RandomWalksManager) updateAddedNodes(DB graph.Database, nodeID uint32,
	removedNodes mapset.Set[uint32], rng *rand.Rand) error {

	return nil
}
