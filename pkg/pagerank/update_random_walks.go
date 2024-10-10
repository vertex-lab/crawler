package pagerank

import (
	"math/rand"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

/*
UpdateRandomWalks updates the RandomWalksMap when a node's successors change from
oldSuccessorIDs to SuccessorIDs.

INPUTS
------

	> DB: graph.Database
	The database where nodes are stored

	> nodeID: uint32
	The ID of the node that changed his successors from oldSuccessorIDs to SuccessorIDs

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
	link: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (RWM *RandomWalksMap) UpdateRandomWalks(DB graph.Database,
	nodeID uint32, oldSuccessorIDs []uint32) error {

	const expectEmptyRWM = false

	// checking the inputs
	err := checkInputs(RWM, DB, expectEmptyRWM)
	if err != nil {
		return err
	}

	// checking that nodeID exists in the DB
	if _, err := DB.FetchNodeByID(nodeID); err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RWM.updateRandomWalks(DB, nodeID, oldSuccessorIDs, rng)
}

// Implements the logic of updating the random walks. It accepts a random number
// generator for reproducibility in tests.
func (RWM *RandomWalksMap) updateRandomWalks(DB graph.Database,
	nodeID uint32, oldSuccessorIDs []uint32, rng *rand.Rand) error {

	// fetch the current successors
	SuccessorIDs, err := DB.GetNodeSuccessorIDs(nodeID)
	if err != nil {
		return err
	}

	// transform to Sets for more efficient operations and lookups
	oldSuccessorSet := mapset.NewSet(oldSuccessorIDs...)
	SuccessorSet := mapset.NewSet(SuccessorIDs...)

	// compute the removed and added nodes
	removedNodes := oldSuccessorSet.Difference(SuccessorSet)
	addedNodes := SuccessorSet.Difference(oldSuccessorSet)

	err = RWM.updateRemove(DB, nodeID, removedNodes, rng)
	if err != nil {
		return err
	}

	err = RWM.updateAdd(DB, nodeID, addedNodes, rng)
	if err != nil {
		return err
	}

	return nil
}

/*
updateRemove is a method that updates the RWM by "pruning" the all the walks
that contain nodeID --> removedNode and by "grafting" them.

The fundamentel data structure of this method is walksToRemoveByNode which is
a map that associates each nodeID with a map of walk pointers and their respective counts.

Let's see why this data structure is useful with an example:

  - nodeID = 0

  - removedNodes = {5}

  - RWM.WalksByNode[0] = { {0,5,3}, {5,3,0,5,3,0,5} }

    The first walk will be pruned, becoming {0} (and then will be grafted)
    The second walk will be pruned becoming {5,3,0} (and then will be grafted)

    So, walksToRemoveByNode = {5: {walkPointer1 : 1, walkPointer2 : 2} ... }

    This means that we'll have to:

  - remove walkPointer1 one time from RWM.WalksByNode[5]

  - remove walkPointer2 two times from RWM.WalksByNode[5] (NOT three times!)
*/
func (RWM *RandomWalksMap) updateRemove(DB graph.Database, nodeID uint32,
	removedNodes mapset.Set[uint32], rng *rand.Rand) error {

	if removedNodes.Cardinality() == 0 {
		return nil
	}

	// initialize walksToRemove
	walksToRemoveByNode := make(map[uint32]map[*RandomWalk]int)
	for removedNode := range removedNodes.Iter() {
		walksToRemoveByNode[removedNode] = make(map[*RandomWalk]int)
	}

	// get all the walks that go through nodeID
	walks, err := RWM.GetWalksByNodeID(nodeID)
	if err != nil {
		return err
	}

	// iterate over the walks
	for _, walk := range walks {

		// iterate over the elements of each walk
		for i := 0; i < len(walk.NodeIDs)-1; i++ {

			// if it contains nodeID --> removedNode
			if walk.NodeIDs[i] == nodeID && removedNodes.ContainsOne(walk.NodeIDs[i+1]) {

				// graft walk
				newWalkSegment, err := generateWalk(DB, nodeID, RWM.alpha, rng)
				if err != nil {
					return err
				}

				for _, graftedNode := range newWalkSegment {
					RWM.WalksByNode[graftedNode] = append(RWM.WalksByNode[graftedNode], walk)
				}

				// adds this walk to each of the pruned nodes, and increase the counter
				for _, prunedNode := range walk.NodeIDs[i+1:] {
					walksToRemoveByNode[prunedNode][walk]++
				}

				// prune the walk and graft it
				walk.NodeIDs = append(walk.NodeIDs[:i], newWalkSegment...)
			}
		}
	}

	// remove all the walksToRemove in one Go!
	for node, walksToRemoveMap := range walksToRemoveByNode {

		// get the current walks of a node
		currentWalks, err := RWM.GetWalksByNodeID(node)
		if err != nil {
			return err
		}

		// remove all the walks that need to be removed, the correct number of times
		newWalks := []*RandomWalk{}
		for _, walk := range currentWalks {

			if walksToRemoveMap[walk] > 0 {
				// don't add it, which counts as if it was removed
				walksToRemoveMap[walk]--

			} else {
				// add it
				newWalks = append(newWalks, walk)
			}
		}

		// change the RWM
		RWM.WalksByNode[node] = newWalks
	}

	return nil
}

// method that updates the RWM by "pruning" some randomly selected walks and
// by "grafting" them using the newly added nodes
func (RWM *RandomWalksMap) updateAdd(DB graph.Database, nodeID uint32,
	removedNodes mapset.Set[uint32], rng *rand.Rand) error {

	return nil
}
