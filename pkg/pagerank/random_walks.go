package pagerank

import (
	"errors"

	mapset "github.com/deckarep/golang-set/v2"
)

// Random walk structure; NodeIDs contains a slice of node IDs e.g. {1,2,6,77,2}
type RandomWalk struct {
	NodeIDs []uint32
}

/*
WalksSet; a set of pointers to RandomWalks.
In the RandomWalksManager structure, each node is mapped to its set of incident walks.
A walk is said to be incidental to a node if that walk passes through that node.
*/
type WalksSet mapset.Set[*RandomWalk]

/*
RandomWalksManager structure; The fundamental structure of the pagerank package.

FIELDS
------

	> WalksByNode: map[uint32] WalksSet
	Associate a node ID to the set of walks that pass through tat node. Each walk
	is uniquly added to a node set because we break the walk when a cycle is encountered.
	e.g. {0: { {0}, {2,3,4,0} } ...}; this means that the only walks that passed
	through node_0 are {0} and {2,3,4,0}

	> alpha: float32
	The dampening factor, which is the probability of stopping at each step
	of the random walk. Default is 0.85

	> walksPerNode: uint16
	The number of random walks to be performed for each node. Default is 10

NOTE
----

 1. The number of walksPerNode is not to be confused with the number of walks
    associated with each node in the WalksByNode map. In fact, the former will always
    be smaller or equal to the latter.
*/
type RandomWalksManager struct {
	WalksByNode  map[uint32]WalksSet
	alpha        float32
	walksPerNode uint16
}

// Creates a new RandomWalksManager
func NewRandomWalksManager(alpha float32, walksPerNode uint16) (*RandomWalksManager, error) {

	if alpha <= 0 || alpha >= 1 {
		return nil, ErrInvalidAlpha
	}

	if walksPerNode <= 0 {
		return nil, ErrInvalidWalksPerNode
	}

	RWM := &RandomWalksManager{
		WalksByNode:  make(map[uint32]WalksSet),
		alpha:        alpha,
		walksPerNode: walksPerNode,
	}

	return RWM, nil
}

// IsEmpty returns whether RWM is empty
func (RWM *RandomWalksManager) IsEmpty() (bool, error) {

	if RWM == nil {
		return true, ErrNilRWMPointer
	}

	if len(RWM.WalksByNode) == 0 {
		return true, nil
	}

	return false, nil
}

// CheckState checks whether the RWM is empty or non-empty and returns
// an appropriate error based on the requirement
func (RWM *RandomWalksManager) CheckState(expectEmptyRWM bool) error {

	if RWM == nil {
		return ErrNilRWMPointer
	}

	if len(RWM.WalksByNode) == 0 && !expectEmptyRWM {
		return ErrEmptyRWM
	}

	if len(RWM.WalksByNode) > 0 && expectEmptyRWM {
		return ErrNonEmptyRWM
	}

	return nil
}

// WalksByNodeID; pass a node ID, returns all the RandomWalks that pass
// through that node, as a WalksSet
func (RWM *RandomWalksManager) WalksByNodeID(nodeID uint32) (WalksSet, error) {

	const expectEmptyRWM = false
	err := RWM.CheckState(expectEmptyRWM)
	if err != nil {
		return nil, err
	}

	walkSet, exist := RWM.WalksByNode[nodeID]

	if !exist {
		return nil, ErrNodeNotFoundRWM
	}
	return walkSet, nil
}

// AddWalk; adds the pointer to the walk to the RandomWalksManager. This means that
// for each node visited by the walk, the walk pointer will be added on
// RandomWalksManager.WalksByNode[node]
func (RWM *RandomWalksManager) AddWalk(walk *RandomWalk) error {

	// if the RWM is a nil pointer
	if RWM == nil {
		return ErrNilRWMPointer
	}

	err := checkWalk(walk)
	if err != nil {
		return err
	}

	// add it to the RandomWalksManager, under each node it passes through
	for _, nodeID := range walk.NodeIDs {

		// Initialize the set for the nodeID if it doesn't exist
		if _, exists := RWM.WalksByNode[nodeID]; !exists {
			RWM.WalksByNode[nodeID] = mapset.NewSet[*RandomWalk]()
		}

		RWM.WalksByNode[nodeID].Add(walk)
	}

	return nil
}

// // RemoveWalks removes all walks the correct number of times as specified
// // in the WalksToRemoveByNode in one Go!
// func (RWM *RandomWalksManager) RemoveWalks(WTR *WalksToRemoveByNode) error {

// 	// RWM should NOT be empty or nil
// 	const expectEmptyRWM = false
// 	err := RWM.CheckState(expectEmptyRWM)
// 	if err != nil {
// 		return err
// 	}

// 	// WRT should NOT be nil
// 	if WTR == nil {
// 		return ErrNilWTRPointer
// 	}

// 	for nodeID, walksToRemove := range WTR.removals {

// 		// get the current walks of a nodeID
// 		currentWalks, err := RWM.WalksByNodeID(nodeID)
// 		if err != nil {
// 			return err
// 		}

// 		// remove all the walks that need to be removed, the correct number of times
// 		newWalks := []*RandomWalk{}
// 		for _, walk := range currentWalks {

// 			if walksToRemove[walk] > 0 {
// 				// don't add it, which counts as if it was removed once
// 				walksToRemove[walk]--

// 			} else {
// 				// add it
// 				newWalks = append(newWalks, walk)
// 			}
// 		}

// 		// change the RWM
// 		RWM.WalksByNode[nodeID] = newWalks
// 	}

// 	return nil
// }

// helper function that returns the appropriate error if the walk pointer is nil
// or if the walk is empty. Else returns nil
func checkWalk(walk *RandomWalk) error {

	// if the walk is a nil pointer
	if walk == nil {
		return ErrNilWalkPointer
	}

	// if the walk is empty
	if len(walk.NodeIDs) == 0 {
		return ErrEmptyWalk
	}

	return nil
}

//---------------------------------ERROR-CODES---------------------------------

var ErrInvalidAlpha = errors.New("alpha should be a number between 0 and 1 (excluded)")
var ErrInvalidWalksPerNode = errors.New("walksPerNode should be greater than zero")

var ErrNilRWMPointer = errors.New("nil RWM pointer")
var ErrEmptyRWM = errors.New("RWM is empty")
var ErrNonEmptyRWM = errors.New("the RWM is NOT empty")

var ErrNilWalkPointer = errors.New("nil walk pointer")
var ErrEmptyWalk = errors.New("passed empty walk")
var ErrInvalidWalkIndex = errors.New("the index is bigger than the lenght of the walk")

var ErrNodeNotFoundRWM = errors.New("nodeID not found in the RWM")
