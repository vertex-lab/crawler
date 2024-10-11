package pagerank

import (
	"errors"

	mapset "github.com/deckarep/golang-set/v2"
)

/*
WalksToRemoveByNode is the fundamental structure of this file.
The field removals is a map that associates a nodeID with another map that
tracks the number of times a given walk should be removed from that node.
Let's see why this data structure is useful with an example.

EXAMPLE:
-------

  - nodeID = 0

  - removedNodes = {5}

  - RWM.WalksByNode[0] = { {0,5,3}, {5,3,0,5,3,0,5} }

The first walk will be pruned, and become {0}
The second walk will be pruned becoming {5,3,0}

So, WalksToRemoveByNode.removals = {5: {walkPointer1 : 1, walkPointer2 : 2} ... }
This ensures walkPointer1 is removed from RWM.WalksByNode[5] only once, and walkPointer2
is removed twice (not three times, as the first '5' is before the "cut").

NOTE:
-----

Why don't we remove the walk as soon as we can, instead of recording their removal in
this new structure? The answer is efficiency. To remove an element from a slice,
we need to traverse the slice, so it's a good idea to traverse the slice only once and
delete multiple walks in one Go.
*/
type WalksToRemoveByNode struct {
	removals map[uint32]map[*RandomWalk]uint16
}

// NewWalksToRemoveByNode initializes a WalksToRemoveByNode for the given set of node IDs.
// Each node ID will have its own map of walks to be removed initialized.
func NewWalksToRemoveByNode(nodesSet mapset.Set[uint32]) *WalksToRemoveByNode {

	WTR := &WalksToRemoveByNode{removals: make(map[uint32]map[*RandomWalk]uint16)}

	for node := range nodesSet.Iter() {
		WTR.removals[node] = make(map[*RandomWalk]uint16)
	}
	return WTR
}

// recordWalkRemoval increments the removal counter associated with the walk
// for each node in walk[index:]
func (WTR *WalksToRemoveByNode) recordWalkRemoval(walk *RandomWalk, index int) error {

	if WTR == nil {
		return ErrNilWTRPointer
	}

	err := checkWalk(walk)
	if err != nil {
		return err
	}

	if index < 0 || index >= len(walk.NodeIDs) {
		return ErrInvalidWalkIndex
	}

	// adds this walk to each of the pruned nodes, and increase the counter
	for _, prunedNode := range walk.NodeIDs[index:] {

		if _, exists := WTR.removals[prunedNode]; !exists {
			WTR.removals[prunedNode] = make(map[*RandomWalk]uint16)
		}

		WTR.removals[prunedNode][walk]++
	}

	return nil
}

//---------------------------------ERROR-CODES---------------------------------

var ErrNilWTRPointer = errors.New("nil WTR pointer")
