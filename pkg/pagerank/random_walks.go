package pagerank

import "errors"

// ------------------------------ERROR-CODES------------------------------

var ErrInvalidAlpha = errors.New("alpha should be a number between 0 and 1 (excluded)")
var ErrInvalidWalksPerNode = errors.New("walksPerNode should be greater than zero")

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

/*
RandomWalksMap structure; The fundamental structure of the pagerank package.

FIELDS
------

	> NodeWalkMap: map[uint32][]*RandomWalk
	Maps node ids to a slice that contains all random walks that passed through that node.
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
    associated with each node in the NodeWalkMap. In fact, the former will always
    be smaller or equal to the latter.

 2. The pagerank of node_j will be approximated by

    > pagerank(node_j) ~ len(NodeWalkMap[node_j]) / totalVisits;

    totalVisits is the sum of the lenght of each walk, which is approximately

    > totalVisits ~ walksPerNode * N * 1/(1-alpha)

    where N is the number of nodes in the graph, and 1/(1-alpha) is the average
    lenght of a walk.
*/
type RandomWalksMap struct {
	NodeWalkMap  map[uint32][]*RandomWalk
	alpha        float32
	walksPerNode uint16
}

// Creates a new RandomWalkMap
func NewRandomWalksMap(alpha float32, walksPerNode uint16) (*RandomWalksMap, error) {

	if alpha <= 0 || alpha >= 1 {
		return nil, ErrInvalidAlpha
	}

	if walksPerNode <= 0 {
		return nil, ErrInvalidWalksPerNode
	}

	RWM := &RandomWalksMap{
		NodeWalkMap:  make(map[uint32][]*RandomWalk),
		alpha:        alpha,
		walksPerNode: walksPerNode,
	}

	return RWM, nil
}

// CheckEmpty returns an error if rwm is nil or empty. Otherwise it returns nil
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

// AddWalk; adds the pointer to the walk to the RandomWalksMap. This means that
// for each node visited by the walk, the walk pointer will be added on
// RandomWalksMap.NodeWalkMap(node)
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
