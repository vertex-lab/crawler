package pagerank

import (
	"math/rand"
	"time"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

/*
UpdateRandomWalks updates the RandomWalksMap when a node's successors change from
succOld to succNew.

INPUTS
------

	> db: graph.Database
	The database where nodes are stored

	> nodeID: uint32
	The ID of the node that changed his successors from oldSuccessorIDs to SuccessorIDs

	> oldSuccessorIDs: []uint32
	The slice that contains the node ids of the old successors of nodeID

OUTPUT
------

	> error: look at checkInputs() to read all the errors

NOTE
----

REFERENCES
----------

	[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
	link: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (rwm *RandomWalksMap) UpdateRandomWalks(db graph.Database,
	nodeID uint32, oldSuccessorIDs []uint32) error {

	const expectEmptyRWM = false

	// checking all the inputs
	err := checkInputs(rwm, db, expectEmptyRWM)
	if err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return rwm.updateRandomWalks(db, nodeID, oldSuccessorIDs, rng)
}

func (rwm *RandomWalksMap) updateRandomWalks(db graph.Database,
	nodeID uint32, oldSuccessorIDs []uint32, rng *rand.Rand) error {

	// // fetch the current successors
	// SuccessorIDs, err := db.GetNodeSuccessorIDs(nodeID)
	// if err != nil {
	// 	return err
	// }

	// // transform to Sets for more efficient operations and lookups
	// oldSuccessorSet := mapset.NewSet(oldSuccessorIDs...)
	// SuccessorSet := mapset.NewSet(SuccessorIDs...)

	// // compute the removed and added nodes
	// removedNodesSet := oldSuccessorSet.Difference(SuccessorSet)

	// walks, err := rwm.GetWalksByNodeID(nodeID)
	// if err != nil {
	// 	return err
	// }

	// // iterate over the walks
	// for _, walk := range walks {

	// 	// iterate over the elements of one walk
	// 	for i, node := range walk.NodeIDs {

	// 		// if it goes through a node that has been removed
	// 		if node == nodeID && removedNodesSet.ContainsOne(walk.NodeIDs[i+1]) {

	// 			// prune the walk
	// 			newWalk := walk.NodeIDs[:i]

	// 			// remove references from
	// 			for node

	// 		}
	// 	}
	// }

	return nil
}

func sliceDifference(sliceA, sliceB []uint32) []uint32 {

	if len(sliceA) == 0 {
		return []uint32{}
	}

	if len(sliceB) == 0 {
		return sliceA
	}

	// Create a map to track elements in slice b
	bMap := make(map[uint32]struct{}, len(sliceB))

	for _, item := range sliceB {
		bMap[item] = struct{}{} // use an empty struct to save space
	}

	// Find elements in sliceA that are not in sliceB
	var diff []uint32
	for _, item := range sliceA {
		if _, found := bMap[item]; !found {
			diff = append(diff, item)
		}
	}

	return diff
}
