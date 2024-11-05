package mock

import (
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

// the in-memory version of the RandomWalkStore interface.
type RandomWalkStore struct {
	//Associates a walkID to the corrisponding RandomWalk
	WalkIndex map[uint32]models.RandomWalk

	// Associates a nodeID to the set of walkIDs that visited that node.
	NodeWalkIDSet map[uint32]models.WalkIDSet

	// The dampening factor, which is the probability of stopping at each step of the random walk. Default is 0.85
	alpha float32

	// The number of random walks to be performed for each node. Default is 10
	walksPerNode uint16
}

// Creates a new RandomWalkStore.
func NewRWS(alpha float32, walksPerNode uint16) (*RandomWalkStore, error) {

	if alpha <= 0 || alpha >= 1 {
		return nil, models.ErrInvalidAlpha
	}

	if walksPerNode <= 0 {
		return nil, models.ErrInvalidWalksPerNode
	}

	RWS := &RandomWalkStore{
		WalkIndex:     make(map[uint32]models.RandomWalk),
		NodeWalkIDSet: make(map[uint32]models.WalkIDSet),
		alpha:         alpha,
		walksPerNode:  walksPerNode,
	}
	return RWS, nil
}

// Alpha() returns the dampening factor used for the RandomWalks
func (RWS *RandomWalkStore) Alpha() float32 {
	return RWS.alpha
}

// WalkPerNode() returns the number of walks to be generated for each node in the DB
func (RWS *RandomWalkStore) WalksPerNode() uint16 {
	return RWS.walksPerNode
}

// IsEmpty() returns whether RWS is empty (ignores errors).
func (RWS *RandomWalkStore) IsEmpty() bool {
	return RWS == nil || len(RWS.WalkIndex) == 0
}

// NodeCount() returns the number of nodes in the RWS (ignores errors).
func (RWS *RandomWalkStore) NodeCount() int {
	if RWS.IsEmpty() {
		return 0
	}

	return len(RWS.NodeWalkIDSet)
}

// All() returns a slice with all the nodeIDs in the RWS.
func (RWS *RandomWalkStore) AllNodes() []uint32 {
	if RWS.IsEmpty() {
		return []uint32{}
	}

	nodeIDs := make([]uint32, 0, RWS.NodeCount())
	for nodeID := range RWS.NodeWalkIDSet {
		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs
}

// ContainsWalk() returns whether RWS contains a walkID in the WalkIndex.
func (RWS *RandomWalkStore) ContainsWalk(walkID uint32) bool {
	if RWS.IsEmpty() {
		return false
	}

	_, exist := RWS.WalkIndex[walkID]
	return exist
}

// ContainsNode() returns whether RWS contains a nodeID in the NodeWalkIDSet.
func (RWS *RandomWalkStore) ContainsNode(nodeID uint32) bool {
	if RWS.IsEmpty() {
		return false
	}

	_, exist := RWS.NodeWalkIDSet[nodeID]
	return exist
}

// Validate() checks the fields alpha, walksPerNode and whether the RWS is nil, empty or
// non-empty and returns an appropriate error based on the requirement.
func (RWS *RandomWalkStore) Validate(expectEmptyRWS bool) error {

	if RWS == nil {
		return models.ErrNilRWSPointer
	}

	if RWS.alpha <= 0.0 || RWS.alpha >= 1.0 {
		return models.ErrInvalidAlpha
	}

	if RWS.walksPerNode <= 0 {
		return models.ErrInvalidWalksPerNode
	}

	if len(RWS.WalkIndex) == 0 && !expectEmptyRWS {
		return models.ErrEmptyRWS
	}

	if len(RWS.WalkIndex) > 0 && expectEmptyRWS {
		return models.ErrNonEmptyRWS
	}

	return nil
}

// VisitCount() returns the number of times nodeID has been visited by a walk.
func (RWS *RandomWalkStore) VisitCount(nodeID uint32) int {
	if RWS == nil || RWS.NodeWalkIDSet == nil {
		return 0
	}

	if walkIDs, exist := RWS.NodeWalkIDSet[nodeID]; exist {
		return walkIDs.Cardinality()
	}
	return 0
}

// WalkIDs() returns up to `limit` RandomWalks that visit nodeID as a WalkIDSet, up to
func (RWS *RandomWalkStore) NodeWalkIDs(nodeID uint32) (models.WalkIDSet, error) {

	if err := RWS.Validate(false); err != nil {
		return nil, err
	}

	walkIDs, exist := RWS.NodeWalkIDSet[nodeID]
	if !exist {
		return nil, models.ErrNodeNotFoundRWS
	}
	return walkIDs, nil
}

// Walks() returns a map of walks by walkID that visit nodeID.
func (RWS *RandomWalkStore) NodeWalks(nodeID uint32) (map[uint32]models.RandomWalk, error) {

	walkIDs, err := RWS.NodeWalkIDs(nodeID)
	if err != nil {
		return nil, err
	}

	walkMap := make(map[uint32]models.RandomWalk, walkIDs.Cardinality())
	for walkID := range walkIDs.Iter() {
		walkMap[walkID] = RWS.WalkIndex[walkID]
	}

	return walkMap, nil
}

// AddWalk() adds the walk to the WalkIndex. It also adds the walkID to the
// WalkIDSet of each node the walk visited. This means that for each node
// visited by the walk, the walk ID will be added to its WalkSet.
func (RWS *RandomWalkStore) AddWalk(walk models.RandomWalk) error {

	if RWS == nil {
		return models.ErrNilRWSPointer
	}

	if err := models.Validate(walk); err != nil {
		return err
	}

	// add the walk to the WalkIndex
	newWalkID := uint32(len(RWS.WalkIndex))
	RWS.WalkIndex[newWalkID] = walk

	// add the walkID to each node
	for _, nodeID := range walk {
		// Initialize the WalkIDSet for nodeID if it doesn't exist
		if _, exists := RWS.NodeWalkIDSet[nodeID]; !exists {
			RWS.NodeWalkIDSet[nodeID] = mapset.NewSet[uint32]()
		}

		RWS.NodeWalkIDSet[nodeID].Add(newWalkID)
	}

	return nil
}

// PruneWalk() removes the walkID from each node in the walk after cutIndex.
// This means that for each prunedNode in walk[cutIndex:], the walk ID will
// be removed from its WalkSet.
func (RWS *RandomWalkStore) PruneWalk(walkID uint32, cutIndex int) error {

	if err := RWS.Validate(false); err != nil {
		return err
	}

	if !RWS.ContainsWalk(walkID) {
		return models.ErrWalkNotFound
	}

	oldWalk := RWS.WalkIndex[walkID]
	if cutIndex < 0 || cutIndex > len(oldWalk) {
		return models.ErrInvalidWalkIndex
	}

	// change the WalkIndex
	RWS.WalkIndex[walkID] = oldWalk[:cutIndex]

	// remove the walkID from each node
	for _, prunedNodeID := range oldWalk[cutIndex:] {
		RWS.NodeWalkIDSet[prunedNodeID].Remove(walkID)
	}

	return nil
}

// GraftWalk() grafts (extends) the walk with the walkSegment, and adds
// the walkID to the WalkSet of each node in the new walkSegment.
func (RWS *RandomWalkStore) GraftWalk(walkID uint32, walkSegment []uint32) error {

	// If there is nothing to graft
	if len(walkSegment) == 0 {
		return nil
	}

	if err := RWS.Validate(false); err != nil {
		return err
	}

	if !RWS.ContainsWalk(walkID) {
		return models.ErrWalkNotFound
	}

	// graft the walk
	RWS.WalkIndex[walkID] = append(RWS.WalkIndex[walkID], walkSegment...)

	// add the walkID to each node
	for _, nodeID := range walkSegment {
		// Initialize the WalkIDSet for nodeID if it doesn't exist
		if _, exists := RWS.NodeWalkIDSet[nodeID]; !exists {
			RWS.NodeWalkIDSet[nodeID] = mapset.NewSet[uint32]()
		}

		RWS.NodeWalkIDSet[nodeID].Add(walkID)
	}

	return nil
}

// ------------------------------------HELPERS----------------------------------

// function that returns a RWS setup based on the RWSType.
func SetupRWS(RWSType string) *RandomWalkStore {
	switch RWSType {
	case "nil":
		return nil

	case "empty":
		RWS, _ := NewRWS(0.85, 1)
		return RWS

	case "one-node0":
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{0}
		RWS.NodeWalkIDSet[0] = mapset.NewSet[uint32](0)
		return RWS

	case "one-node1":
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{1}
		RWS.NodeWalkIDSet[1] = mapset.NewSet[uint32](0)
		return RWS

	case "triangle":
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{0, 1, 2}
		RWS.WalkIndex[1] = models.RandomWalk{1, 2, 0}
		RWS.WalkIndex[2] = models.RandomWalk{2, 0, 1}
		RWS.NodeWalkIDSet[0] = mapset.NewSet[uint32](0, 1, 2)
		RWS.NodeWalkIDSet[1] = mapset.NewSet[uint32](0, 1, 2)
		RWS.NodeWalkIDSet[2] = mapset.NewSet[uint32](0, 1, 2)
		return RWS

	case "simple":
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{0, 1}
		RWS.NodeWalkIDSet[0] = mapset.NewSet[uint32](0)
		RWS.NodeWalkIDSet[1] = mapset.NewSet[uint32](0)
		return RWS

	default:
		return nil // Default to nil for unrecognized scenarios
	}
}
