// The mock store package allows for testing that are decoupled from a
// particular RandomWalkStore implementation.
package mock

import (
	"context"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
)

// WalkSet is a set of IDs of RandomWalks. Each node in the RandomWalkStore
// is associated with the set of IDs of walks that visit that node.
type WalkSet mapset.Set[uint32]

// the in-memory version of the RandomWalkStore interface.
type RandomWalkStore struct {
	//Associates a walkID to the corrisponding RandomWalk
	WalkIndex map[uint32]models.RandomWalk

	// Associates a nodeID to the set of walkIDs that visited that node.
	walksVisiting map[uint32]WalkSet

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
		walksVisiting: make(map[uint32]WalkSet),
		alpha:         alpha,
		walksPerNode:  walksPerNode,
		totalVisits:   0,
	}
	return RWS, nil
}

// Alpha() returns the dampening factor used for the RandomWalks
func (RWS *RandomWalkStore) Alpha(ctx context.Context) float32 {
	_ = ctx
	return RWS.alpha
}

// WalkPerNode() returns the number of walks to be generated for each node in the DB
func (RWS *RandomWalkStore) WalksPerNode(ctx context.Context) uint16 {
	_ = ctx
	return RWS.walksPerNode
}

// TotalVisits() returns the total number of visits.
func (RWS *RandomWalkStore) TotalVisits(ctx context.Context) int {
	_ = ctx
	var visits int
	for _, walkSet := range RWS.walksVisiting {
		visits += walkSet.Cardinality()
	}

	return visits
}

// Validate() that RWS is not nil, and the fields alpha and  walksPerNode
func (RWS *RandomWalkStore) Validate() error {

	if RWS == nil {
		return models.ErrNilRWSPointer
	}

	if RWS.alpha <= 0.0 || RWS.alpha >= 1.0 {
		return models.ErrInvalidAlpha
	}

	if RWS.walksPerNode <= 0 {
		return models.ErrInvalidWalksPerNode
	}

	return nil
}

// VisitCounts() returns a map that associates each nodeID with the number of
// times it was visited by a walk.
func (RWS *RandomWalkStore) VisitCounts(ctx context.Context, nodeIDs ...uint32) ([]int, error) {
	_ = ctx
	if RWS == nil || RWS.walksVisiting == nil {
		return []int{}, models.ErrNilRWSPointer
	}

	if len(nodeIDs) == 0 {
		return []int{}, nil
	}

	visits := make([]int, len(nodeIDs))
	for i, ID := range nodeIDs {

		walkSet, exists := RWS.walksVisiting[ID]
		if !exists {
			visits[i] = 0
			continue
		}

		visits[i] = walkSet.Cardinality()
	}

	return visits, nil
}

// Walks() returns a map of walks by walkID that visit nodeID.
// - if limit > 0, the map contains up to that many key-value pairs.
// - if limit < 0, all walks are returned
// - if no walks are found for nodeID, an error is returned
func (RWS *RandomWalkStore) Walks(ctx context.Context, walkIDs ...uint32) ([]models.RandomWalk, error) {

	if err := RWS.Validate(); err != nil {
		return nil, err
	}

	walks := make([]models.RandomWalk, 0, len(walkIDs))
	for _, ID := range walkIDs {
		walk, exist := RWS.WalkIndex[ID]
		if !exist {
			return nil, models.ErrWalkNotFound
		}

		walks = append(walks, walk)
	}

	return walks, nil
}

/*
WalksVisiting() returns up to limit UNIQUE walkIDs evenly distributed among the specified nodeIDs.
In other words, it returns up to limit/len(nodeIDs) walkIDs for each of the nodes.

Note:
- If limit = 0, no walk is returned
- If limit < nodeIDs, no walk is returned
- If limit = -1, all walks for all nodes are returned (USE WITH CAUTION).
*/
func (RWS *RandomWalkStore) WalksVisiting(ctx context.Context, limit int, nodeIDs ...uint32) ([]uint32, error) {
	_ = ctx
	if err := RWS.Validate(); err != nil {
		return nil, err
	}

	var limitPerNode int64
	switch limit {
	case 0:
		return nil, nil

	case -1:
		limitPerNode = 1000000000 // a very big number to return all
		limit = 100000

	default:
		limitPerNode = int64(limit) / int64(len(nodeIDs))
	}

	walkIDs := make([]uint32, 0, limit)
	for _, ID := range nodeIDs {
		walkSet, exists := RWS.walksVisiting[ID]
		if !exists {
			return nil, models.ErrNodeNotFoundRWS
		}

		IDs := walkSet.ToSlice()
		if limitPerNode <= 0 || limitPerNode > int64(len(IDs)) {
			walkIDs = append(walkIDs, IDs...)
		} else {
			walkIDs = append(walkIDs, IDs[:limitPerNode]...)
		}
	}

	return sliceutils.Unique(walkIDs), nil
}

// WalksVisitingAll() returns all the IDs of the walk that visit ALL specified nodes.
func (RWS *RandomWalkStore) WalksVisitingAll(ctx context.Context, nodeIDs ...uint32) ([]uint32, error) {
	_ = ctx
	if err := RWS.Validate(); err != nil {
		return nil, err
	}

	intersection, exists := RWS.walksVisiting[nodeIDs[0]]
	if !exists {
		return nil, models.ErrNodeNotFoundRWS
	}

	for _, ID := range nodeIDs[1:] {
		walkSet, exists := RWS.walksVisiting[ID]
		if !exists {
			return nil, models.ErrNodeNotFoundRWS
		}

		intersection = intersection.Intersect(walkSet)
	}

	return intersection.ToSlice(), nil
}

// AddWalks() adds all the specified walks to the RWS. If at least one of the walks
// is invalid, no one gets added.
func (RWS *RandomWalkStore) AddWalks(ctx context.Context, walks ...models.RandomWalk) error {
	_ = ctx
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
			if _, exists := RWS.walksVisiting[nodeID]; !exists {
				RWS.walksVisiting[nodeID] = mapset.NewSet[uint32]()
			}

			RWS.walksVisiting[nodeID].Add(walkID)
		}
	}

	return nil
}

// RemoveWalks() removes the all the specified walks from the RWS. If one walkID
// is not found, no walk gets removed.
func (RWS *RandomWalkStore) RemoveWalks(ctx context.Context, walkIDs ...uint32) error {
	_ = ctx
	if err := RWS.Validate(); err != nil {
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
			RWS.walksVisiting[nodeID].Remove(walkID)
		}
	}

	return nil
}

// PruneWalk() prunes the specified walk, cutting at cutIndex (walk[:cutIndex])
func (RWS *RandomWalkStore) PruneWalk(ctx context.Context, walkID uint32, cutIndex int) error {
	_ = ctx
	if err := RWS.Validate(); err != nil {
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
		RWS.walksVisiting[prunedNodeID].Remove(walkID)
	}

	return nil
}

// GraftWalk() grafts (extends) the walk with the walkSegment, and adds
// the walkID to the WalkSet of each node in the new walkSegment.
func (RWS *RandomWalkStore) GraftWalk(ctx context.Context, walkID uint32, walkSegment []uint32) error {
	_ = ctx
	if err := RWS.Validate(); err != nil {
		return err
	}

	if len(walkSegment) == 0 {
		return nil
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
		if _, exists := RWS.walksVisiting[nodeID]; !exists {
			RWS.walksVisiting[nodeID] = mapset.NewSet[uint32]()
		}

		RWS.walksVisiting[nodeID].Add(walkID)
	}

	return nil
}

// PruneGraftWalk() encapsulates the functions of Pruning and Grafting a walk.
func (RWS *RandomWalkStore) PruneGraftWalk(ctx context.Context, walkID uint32, cutIndex int,
	walkSegment models.RandomWalk) error {

	if err := RWS.PruneWalk(ctx, walkID, cutIndex); err != nil {
		return err
	}

	if err := RWS.GraftWalk(ctx, walkID, walkSegment); err != nil {
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
		RWS.walksVisiting[0] = mapset.NewSet[uint32](0)
		RWS.totalVisits = 1
		return RWS

	case "one-node1":
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{1}
		RWS.walksVisiting[1] = mapset.NewSet[uint32](0)
		RWS.totalVisits = 1
		return RWS

	case "simple":
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{0, 1}
		RWS.walksVisiting[0] = mapset.NewSet[uint32](0)
		RWS.walksVisiting[1] = mapset.NewSet[uint32](0)
		RWS.totalVisits = 2
		return RWS

	case "triangle":
		// 0 --> 1 --> 2 --> 0
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{0, 1, 2}
		RWS.WalkIndex[1] = models.RandomWalk{1, 2, 0}
		RWS.WalkIndex[2] = models.RandomWalk{2, 0, 1}
		RWS.walksVisiting[0] = mapset.NewSet[uint32](0, 1, 2)
		RWS.walksVisiting[1] = mapset.NewSet[uint32](0, 1, 2)
		RWS.walksVisiting[2] = mapset.NewSet[uint32](0, 1, 2)
		RWS.totalVisits = 9
		return RWS

	case "complex":
		// 0 --> 1 --> 2
		// 0 --> 3
		RWS, _ := NewRWS(0.85, 1)
		RWS.WalkIndex[0] = models.RandomWalk{0, 1, 2}
		RWS.WalkIndex[1] = models.RandomWalk{0, 3}
		RWS.WalkIndex[2] = models.RandomWalk{1, 2}
		RWS.walksVisiting[0] = mapset.NewSet[uint32](0, 1)
		RWS.walksVisiting[1] = mapset.NewSet[uint32](0, 2)
		RWS.walksVisiting[2] = mapset.NewSet[uint32](0, 2)
		RWS.walksVisiting[3] = mapset.NewSet[uint32](1)
		RWS.totalVisits = 7
		return RWS

	default:
		return nil
	}
}
