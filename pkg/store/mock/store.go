// The mock store package allows for testing that are decoupled from a
// particular RandomWalkStore implementation.
package mock

import (
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/vertex-lab/crawler/pkg/models"
)

// WalkSet is a set of IDs of RandomWalks. Each node in the RandomWalkStore
// is associated with the set of IDs of walks that visit that node.
type WalkSet mapset.Set[uint32]

// the in-memory version of the RandomWalkStore interface.
type RandomWalkStore struct {
	//Associates a walkID to the corrisponding RandomWalk
	WalkIndex map[uint32]models.RandomWalk

	// Associates a nodeID to the set of walkIDs that visited that node.
	WalksVisiting map[uint32]WalkSet

	//mu sync.RWMutex

	// The dampening factor, which is the probability of stopping at each step of the random walk. Default is 0.85
	alpha float32

	// The number of random walks to be performed for each node. Default is 10
	walksPerNode uint16

	// The total number of visits, meaning the sum of how many times each node
	// was visited by a walk
	totalVisits int
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
		WalksVisiting: make(map[uint32]WalkSet),
		//mu:            sync.RWMutex{},
		alpha:        alpha,
		walksPerNode: walksPerNode,
		totalVisits:  0,
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

// TotalVisits() returns the total number of visits.
func (RWS *RandomWalkStore) TotalVisits() int {
	visits := 0
	for _, walkSet := range RWS.WalksVisiting {
		visits += walkSet.Cardinality()
	}

	return visits
}

// IsEmpty() returns whether RWS is empty (ignores errors).
func (RWS *RandomWalkStore) IsEmpty() bool {
	return RWS == nil || len(RWS.WalkIndex) == 0
}

// DEPRECATED NodeCount() returns the number of nodes in the RWS (ignores errors).
func (RWS *RandomWalkStore) NodeCount() int {
	if RWS.IsEmpty() {
		return 0
	}
	return len(RWS.WalksVisiting)
}

// DEPRECATED AllNodes() returns a slice with all the nodeIDs in the RWS.
func (RWS *RandomWalkStore) AllNodes() []uint32 {
	if RWS.IsEmpty() {
		return []uint32{}
	}

	nodeIDs := make([]uint32, 0, RWS.NodeCount())
	for nodeID := range RWS.WalksVisiting {
		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs
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

	empty := RWS.IsEmpty()
	if empty && !expectEmptyRWS {
		return models.ErrEmptyRWS
	}

	if !empty && expectEmptyRWS {
		return models.ErrNonEmptyRWS
	}

	return nil
}

// VisitCounts() returns a map that associates each nodeID with the number of
// times it was visited by a walk.
func (RWS *RandomWalkStore) VisitCounts(nodeIDs []uint32) (map[uint32]int, error) {
	if RWS == nil || RWS.WalksVisiting == nil {
		return map[uint32]int{}, models.ErrNilRWSPointer
	}

	if len(nodeIDs) == 0 {
		return map[uint32]int{}, nil
	}

	visitMap := make(map[uint32]int, len(nodeIDs))
	for _, nodeID := range nodeIDs {

		walkSet, exists := RWS.WalksVisiting[nodeID]
		if !exists {
			visitMap[nodeID] = 0
			continue
		}

		visitMap[nodeID] = walkSet.Cardinality()
	}

	return visitMap, nil
}

// WalkIDs() returns up to `limit` RandomWalks that visit nodeID as a WalkIDSet, up to
func (RWS *RandomWalkStore) WalkIDs(nodeID uint32) (WalkSet, error) {

	if err := RWS.Validate(false); err != nil {
		return nil, err
	}

	walkIDs, exist := RWS.WalksVisiting[nodeID]
	if !exist {
		return nil, models.ErrNodeNotFoundRWS
	}
	return walkIDs, nil
}

// Walks() returns a map of walks by walkID that visit nodeID.
// - if limit > 0, the map contains up to that many key-value pairs.
// - if limit < 0, all walks are returned
func (RWS *RandomWalkStore) Walks(nodeID uint32, limit int) (map[uint32]models.RandomWalk, error) {

	walkIDs, err := RWS.WalkIDs(nodeID)
	if err != nil {
		return nil, err
	}

	if limit <= 0 || limit > walkIDs.Cardinality() {
		limit = walkIDs.Cardinality()
	}

	// extract into the map format
	walkMap := make(map[uint32]models.RandomWalk, limit)
	for _, walkID := range walkIDs.ToSlice() { // we use ToSlice instead of Iter() to avoid potential for deadlocks
		if len(walkMap) >= limit {
			break
		}

		walkMap[walkID] = RWS.WalkIndex[walkID]
	}

	return walkMap, nil
}

// AddWalks() adds all the specified walks to the RWS. If at least one of the walks
// is invalid, no one gets added.
func (RWS *RandomWalkStore) AddWalks(walks []models.RandomWalk) error {

	if RWS == nil {
		return models.ErrNilRWSPointer
	}

	if len(walks) == 0 {
		return nil
	}

	for _, walk := range walks {
		if err := models.Validate(walk); err != nil {
			return err
		}
	}

	for _, walk := range walks {
		walkID := uint32(len(RWS.WalkIndex))
		RWS.WalkIndex[walkID] = walk
		RWS.totalVisits += len(walk)

		// add the walkID to each node
		for _, nodeID := range walk {
			// Initialize the WalkIDSet for nodeID if it doesn't exist
			if _, exists := RWS.WalksVisiting[nodeID]; !exists {
				RWS.WalksVisiting[nodeID] = mapset.NewSet[uint32]()
			}

			RWS.WalksVisiting[nodeID].Add(walkID)
		}
	}

	return nil
}

// RemoveWalks() removes the all the specified walks from the RWS. If one walkID
// is not found, no walk gets removed.
func (RWS *RandomWalkStore) RemoveWalks(walkIDs []uint32) error {

	if err := RWS.Validate(false); err != nil {
		return err
	}

	for _, walkID := range walkIDs {
		_, exists := RWS.WalkIndex[walkID]
		if !exists {
			return models.ErrWalkNotFound
		}
	}

	for _, walkID := range walkIDs {
		walk := RWS.WalkIndex[walkID]
		delete(RWS.WalkIndex, walkID)
		RWS.totalVisits -= len(walk)

		for _, nodeID := range walk {
			RWS.WalksVisiting[nodeID].Remove(walkID)
		}
	}

	return nil
}

// PruneWalk() prunes the specified walk, cutting at cutIndex (walk[:cutIndex])
func (RWS *RandomWalkStore) PruneWalk(walkID uint32, cutIndex int) error {

	if err := RWS.Validate(false); err != nil {
		return err
	}

	oldWalk, exists := RWS.WalkIndex[walkID]
	if !exists {
		return models.ErrWalkNotFound
	}

	if cutIndex < 0 || cutIndex > len(oldWalk) {
		return models.ErrInvalidWalkIndex
	}

	// change the WalkIndex
	RWS.WalkIndex[walkID] = oldWalk[:cutIndex]

	// decrease the total visits
	RWS.totalVisits -= len(oldWalk) - cutIndex

	// remove the walkID from each pruned node
	for _, prunedNodeID := range oldWalk[cutIndex:] {
		RWS.WalksVisiting[prunedNodeID].Remove(walkID)
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

	if _, exists := RWS.WalkIndex[walkID]; !exists {
		return models.ErrWalkNotFound
	}

	// graft the walk
	RWS.WalkIndex[walkID] = append(RWS.WalkIndex[walkID], walkSegment...)

	// increase the total visits
	RWS.totalVisits += len(walkSegment)

	// add the walkID to each grafted node
	for _, nodeID := range walkSegment {
		// Initialize the WalkIDSet for nodeID if it doesn't exist
		if _, exists := RWS.WalksVisiting[nodeID]; !exists {
			RWS.WalksVisiting[nodeID] = mapset.NewSet[uint32]()
		}

		RWS.WalksVisiting[nodeID].Add(walkID)
	}

	return nil
}

/*
PruneGraftWalk() encapsulates the functions of Pruning and Grafting a walk.
These functions need to be coupled together to leverage the atomicity of
Redis transactions. This ensures that a walk is either uneffected or is both
pruned and grafted successfully.
*/
func (RWS *RandomWalkStore) PruneGraftWalk(walkID uint32, cutIndex int,
	walkSegment models.RandomWalk) error {

	// prune the walk
	if err := RWS.PruneWalk(walkID, cutIndex); err != nil {
		return err
	}

	// graft the walk with the new walk segment
	if err := RWS.GraftWalk(walkID, walkSegment); err != nil {
		return err
	}

	return nil
}

// SetupRWS() returns a RWS setup based on the RWSType.
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
		RWS.WalksVisiting[0] = mapset.NewSet[uint32](0)
		RWS.totalVisits = 1
		return RWS

	case "one-node1":
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{1}
		RWS.WalksVisiting[1] = mapset.NewSet[uint32](0)
		RWS.totalVisits = 1
		return RWS

	case "simple":
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{0, 1}
		RWS.WalksVisiting[0] = mapset.NewSet[uint32](0)
		RWS.WalksVisiting[1] = mapset.NewSet[uint32](0)
		RWS.totalVisits = 2
		return RWS

	case "triangle":
		// 0 --> 1 --> 2 --> 0
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{0, 1, 2}
		RWS.WalkIndex[1] = models.RandomWalk{1, 2, 0}
		RWS.WalkIndex[2] = models.RandomWalk{2, 0, 1}
		RWS.WalksVisiting[0] = mapset.NewSet[uint32](0, 1, 2)
		RWS.WalksVisiting[1] = mapset.NewSet[uint32](0, 1, 2)
		RWS.WalksVisiting[2] = mapset.NewSet[uint32](0, 1, 2)
		RWS.totalVisits = 9
		return RWS

	case "complex":
		// 0 --> 1 --> 2
		// 0 --> 3
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{0, 1, 2}
		RWS.WalkIndex[1] = models.RandomWalk{0, 3}
		RWS.WalkIndex[2] = models.RandomWalk{1, 2}
		RWS.WalksVisiting[0] = mapset.NewSet[uint32](0, 1)
		RWS.WalksVisiting[1] = mapset.NewSet[uint32](0, 2)
		RWS.WalksVisiting[2] = mapset.NewSet[uint32](0, 2)
		RWS.WalksVisiting[3] = mapset.NewSet[uint32](1)
		RWS.totalVisits = 7
		return RWS

	default:
		return nil // Default to nil for unrecognized scenarios
	}
}
