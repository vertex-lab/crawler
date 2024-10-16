package pagerank

import (
	"errors"
	"slices"

	mapset "github.com/deckarep/golang-set/v2"
)

// Random walk structure; NodeIDs contains a slice of node IDs e.g. {1,2,6,77,2}
type RandomWalk struct {
	NodeIDs []uint32
}

// CheckEmpty returns whether RandomWalk is empty
func (rw *RandomWalk) CheckEmpty() error {

	if rw == nil {
		return ErrNilRandomWalkPointer
	}

	if len(rw.NodeIDs) == 0 {
		return ErrEmptyRandomWalk
	}

	return nil
}

/*
NeedsUpdate returns whether the RandomWalk needs to be updated, and the index
where to implement the update (pruning and grafting).

This happens if the walk contains an invalid hop nodeID --> removedNode in removedNodes.
The average lenght of removedNodes is supposed to be quite small, meaning that sets are less
performing.
*/
func (rw *RandomWalk) NeedsUpdate(nodeID uint32,
	removedNodes []uint32) (bool, int, error) {

	err := rw.CheckEmpty()
	if err != nil {
		return true, -1, err
	}

	// iterate over the elements of the walk
	for i := 0; i < len(rw.NodeIDs)-1; i++ {

		// if it contains a hop (nodeID --> removedNode)
		if rw.NodeIDs[i] == nodeID && slices.Contains(removedNodes, rw.NodeIDs[i+1]) {

			// it needs to be updated from (i+1)th element (included) onwards
			cutIndex := i + 1
			return true, cutIndex, nil
		}
	}

	return false, -1, nil
}

// WalkSet; a set of pointers to RandomWalks.
type WalkSet mapset.Set[*RandomWalk]

/*
RandomWalksManager structure; The fundamental structure of the pagerank package.

FIELDS
------

	> WalksByNode: map[uint32] WalkSet
	Associate a node ID to the set of walks that pass through tat node. Each walk
	is uniquely added to a node set because we break the walk when a cycle is encountered.
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
	WalksByNode  map[uint32]WalkSet
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
		WalksByNode:  make(map[uint32]WalkSet),
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

// CheckState checks whether the RWM is nil, empty or non-empty and returns
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
// through that node, as a WalkSet
func (RWM *RandomWalksManager) WalksByNodeID(nodeID uint32) (WalkSet, error) {

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

// AddWalk; adds the walk pointer to the WalkSet of each node in the walk.
// This means that for each node visited by the walk, the walk pointer will be
// added to its WalkSet
func (RWM *RandomWalksManager) AddWalk(walk *RandomWalk) error {

	if RWM == nil {
		return ErrNilRWMPointer
	}

	err := walk.CheckEmpty()
	if err != nil {
		return err
	}

	// add the pointer to the WalkSet of each node
	for _, nodeID := range walk.NodeIDs {

		// Initialize the WalkSet for nodeID if it doesn't exist
		if _, exists := RWM.WalksByNode[nodeID]; !exists {
			RWM.WalksByNode[nodeID] = mapset.NewSet[*RandomWalk]()
		}

		RWM.WalksByNode[nodeID].Add(walk)
	}

	return nil
}

// PruneWalk; removes the walk pointer from each node in the walk after cutIndex.
// This means that for each prunedNode in walk[cutIndex:], the walk pointer will
// be removed from its WalkSet
func (RWM *RandomWalksManager) PruneWalk(walk *RandomWalk, cutIndex int) error {

	const expectEmptyRWM = false
	err := RWM.CheckState(expectEmptyRWM)
	if err != nil {
		return err
	}

	err = walk.CheckEmpty()
	if err != nil {
		return err
	}

	// the cut must decrease the lenght of the walk, or it's pointless
	if cutIndex < 0 || cutIndex >= len(walk.NodeIDs) {
		return ErrInvalidWalkIndex
	}

	// remove the pointer from the WalkSet of each node
	for _, prunedNodeID := range walk.NodeIDs[cutIndex:] {
		RWM.WalksByNode[prunedNodeID].Remove(walk)
	}

	// prune the walk
	walk.NodeIDs = slices.Delete(walk.NodeIDs, cutIndex, len(walk.NodeIDs))

	return nil
}

// GraftWalk; graft (extend) the walk with the new walkSegment, and add the walk
// pointer to the WalkSet of each node in the new walkSegment
func (RWM *RandomWalksManager) GraftWalk(walk *RandomWalk, walkSegment []uint32) error {

	// If there isn't anything to graft
	if len(walkSegment) == 0 {
		return nil
	}

	const expectEmptyRWM = false
	err := RWM.CheckState(expectEmptyRWM)
	if err != nil {
		return err
	}

	err = walk.CheckEmpty()
	if err != nil {
		return err
	}

	// add the pointer to the WalkSet of each node
	for _, nodeID := range walkSegment {

		// Initialize the WalkSet for nodeID if it doesn't exist
		if _, exists := RWM.WalksByNode[nodeID]; !exists {
			RWM.WalksByNode[nodeID] = mapset.NewSet[*RandomWalk]()
		}

		RWM.WalksByNode[nodeID].Add(walk)
	}

	// graft the walk
	walk.NodeIDs = append(walk.NodeIDs, walkSegment...)

	return nil
}

//---------------------------------ERROR-CODES---------------------------------

var ErrNilRandomWalkPointer = errors.New("nil RandomWalk pointer")
var ErrEmptyRandomWalk = errors.New("RandomWalk is empty")
var ErrInvalidWalkIndex = errors.New("the index is bigger than the lenght of the walk")

var ErrInvalidAlpha = errors.New("alpha should be a number between 0 and 1 (excluded)")
var ErrInvalidWalksPerNode = errors.New("walksPerNode should be greater than zero")

var ErrNilRWMPointer = errors.New("nil RWM pointer")
var ErrEmptyRWM = errors.New("RWM is empty")
var ErrNonEmptyRWM = errors.New("the RWM is NOT empty")

var ErrNodeNotFoundRWM = errors.New("nodeID not found in the RWM")
