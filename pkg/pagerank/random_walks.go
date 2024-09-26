package pagerank

import "errors"

// ------------------------------ERROR-CODES------------------------------

var ErrNilRWMPointer = errors.New("nil rwm pointer")
var ErrEmptyRWM = errors.New("rwm is empty")

var ErrNilWalkPointer = errors.New("nil walk pointer")
var ErrEmptyWalk = errors.New("passed empty walk")

var ErrNodeNotFound = errors.New("node not found")

// -----------------------------------------------------------------------

// Random walk structure; contains a slice of node IDs e.g. {1,2,6,77,2}
type RandomWalk struct {
	NodeIDs []uint32
}

// Random WalksMap structure; maps node ids to a slice that contains all
// random walks that passed through that node
// e.g. {0: { {0}, {2,3,4,0} } ...}   this means that the only walks that
// passed through node_0 are {0} and {2,3,4,0}
type RandomWalksMap struct {
	NodeWalkMap map[uint32][]*RandomWalk
}

// Creates a new RandomWalkMap
func NewRandomWalksMap() *RandomWalksMap {
	return &RandomWalksMap{
		NodeWalkMap: make(map[uint32][]*RandomWalk),
	}
}

func (rwm *RandomWalksMap) CheckEmpty() error {

	if rwm == nil {
		return ErrNilRWMPointer
	}

	if len(rwm.NodeWalkMap) == 0 {
		return ErrEmptyRWM
	}

	return nil
}

// GetWalksByNodeID; pass a node ID, returns all the RandomWalks that pass
// through that node, as a slice []*RandomWalk
func (rwm *RandomWalksMap) GetWalksByNodeID(nodeID uint32) ([]*RandomWalk, error) {

	err := rwm.CheckEmpty()
	if err != nil {
		return nil, err
	}

	walks, exist := rwm.NodeWalkMap[nodeID]

	if !exist {
		return nil, ErrNodeNotFound
	}
	return walks, nil
}

func (rwm *RandomWalksMap) AddWalk(walk *RandomWalk) error {

	// if the rwm is a nil pointer
	if rwm == nil {
		return ErrNilRWMPointer
	}

	// if the walk is a nil pointer
	if walk == nil {
		return ErrNilWalkPointer
	}

	// if the walk is empty
	if len(walk.NodeIDs) == 0 {
		return ErrEmptyWalk
	}

	// add it to the RandomWalksMap, under each node it passes through
	for _, nodeID := range walk.NodeIDs {
		rwm.NodeWalkMap[nodeID] = append(rwm.NodeWalkMap[nodeID], walk)
	}

	return nil
}
