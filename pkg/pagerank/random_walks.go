package pagerank

import "errors"

// Random walk structure; contains a slice of node IDs e.g. {1,2,6,77,2}
type RandomWalk struct {
	NodeIDs []uint32
}

// Random WalksMap structure; maps node ids to a slice that contains all
// random walks that pass through that node
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

// GetWalksByNodeID; pass a node ID, returns all the RandomWalks that pass
// through that node, as a slice []*RandomWalk
func (rwm *RandomWalksMap) GetWalksByNodeID(nodeID uint32) ([]*RandomWalk, error) {
	walks, exist := rwm.NodeWalkMap[nodeID]

	if !exist {
		return nil, ErrNodeNotFound
	}
	return walks, nil
}

func (rwm *RandomWalksMap) AddWalk(walk *RandomWalk) error {

	if walk == nil {
		return ErrNilPointer
	}

	if len(walk.NodeIDs) == 0 {
		return ErrEmptyWalk
	}

	for _, nodeID := range walk.NodeIDs {
		rwm.NodeWalkMap[nodeID] = append(rwm.NodeWalkMap[nodeID], walk)
	}

	return nil
}

// ------------------------------ERROR-CODES------------------------------

var ErrNodeNotFound = errors.New("node not found")
var ErrNilPointer = errors.New("passed nil pointer")
var ErrEmptyWalk = errors.New("passed empty walk")
