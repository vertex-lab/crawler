package pagerank

import (
	"math/rand"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

/*
UpdateRandomWalks updates the RandomWalksMap when a node's successors change from
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
func (RWM *RandomWalksMap) UpdateRandomWalks(DB graph.Database,
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
func (RWM *RandomWalksMap) updateRandomWalks(DB graph.Database,
	nodeID uint32, oldSuccessorIDs []uint32, rng *rand.Rand) error {

	// if nodeID isn't in RWM, generate new walks from scratch
	if _, exist := RWM.WalksByNode[nodeID]; !exist {
		err := RWM.generateRandomWalks(DB, []uint32{nodeID}, rng)
		return err
	}

	// if nodeID is in RWM, update the walks, starting by fetching the current successors
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
At the end, these changes are incorporated into the RandomWalksMap (RWM)

The fundamental data structure of this method is walksToRemoveByNode (WTR)
*/
func (RWM *RandomWalksMap) updateRemovedNodes(DB graph.Database, nodeID uint32,
	removedNodes mapset.Set[uint32], rng *rand.Rand) error {

	if removedNodes.Cardinality() == 0 {
		return nil
	}

	WTR := NewWalksToRemoveByNode(removedNodes)

	// get all the walks that go through nodeID
	walks, err := RWM.WalksByNodeID(nodeID)
	if err != nil {
		return err
	}

	// iterate over the walks
	for _, walk := range walks {

		// iterate over the elements of each walk
		for i := 0; i < len(walk.NodeIDs)-1; i++ {

			// if it contains a hop (nodeID --> removedNode)
			if walk.NodeIDs[i] == nodeID && removedNodes.ContainsOne(walk.NodeIDs[i+1]) {

				// record the removal of the walks in the WRT
				WTR.recordWalkRemoval(walk, i+1)

				// graft walk
				newWalkSegment, err := generateWalk(DB, nodeID, RWM.alpha, rng)
				if err != nil {
					return err
				}

				for _, graftedNode := range newWalkSegment {
					RWM.WalksByNode[graftedNode] = append(RWM.WalksByNode[graftedNode], walk)
				}

				// prune the walk and graft it
				walk.NodeIDs = append(walk.NodeIDs[:i], newWalkSegment...)
			}
		}
	}

	// remove all walks the correct number of times as specified in the WTR
	err = RWM.RemoveWalks(WTR)
	if err != nil {
		return err
	}

	return nil
}

func pruneAndGraftRandomWalk(DB graph.Database, startingNodeID uint32,
	index int, alpha float32, rng *rand.Rand) error {

	return nil
}

// func pruneAndGraftRandomWalk(DB graph.Database, nodeID uint32, walk *RandomWalk,
// 	removedNodes mapset.Set[uint32], rng *rand.Rand) {

// 	// iterate over the elements of the walk
// 	for i := 0; i < len(walk.NodeIDs)-1; i++ {

// 		// if it contains nodeID --> removedNode
// 		if walk.NodeIDs[i] == nodeID && removedNodes.ContainsOne(walk.NodeIDs[i+1]) {

// 			// graft walk
// 			newWalkSegment, err := generateWalk(DB, nodeID, RWM.alpha, rng)
// 			if err != nil {
// 				return err
// 			}

// 			for _, graftedNode := range newWalkSegment {
// 				RWM.WalksByNode[graftedNode] = append(RWM.WalksByNode[graftedNode], walk)
// 			}

// 			// adds this walk to each of the pruned nodes, and increase the counter
// 			for _, prunedNode := range walk.NodeIDs[i+1:] {
// 				walksToRemoveByNode[prunedNode][walk]++
// 			}

// 			// prune the walk and graft it
// 			walk.NodeIDs = append(walk.NodeIDs[:i], newWalkSegment...)
// 		}
// 	}
// }

// method that updates the RWM by "pruning" some randomly selected walks and
// by "grafting" them using the newly added nodes
func (RWM *RandomWalksMap) updateAddedNodes(DB graph.Database, nodeID uint32,
	removedNodes mapset.Set[uint32], rng *rand.Rand) error {

	return nil
}
