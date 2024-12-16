package pagerank

import (
	"context"
	"errors"

	"github.com/vertex-lab/crawler/pkg/models"
)

// FollowCache contains a map nodeID --> follows, and the DB as a fallback mechanism.
type FollowCache struct {
	follows map[uint32][]uint32
	DB      models.Database // used as a fallback
}

// NewFollowCache() returns an empty FollowCache interfacing with the specified Database.
func NewFollowCache(DB models.Database, size int) *FollowCache {
	return &FollowCache{
		follows: make(map[uint32][]uint32, size),
		DB:      DB,
	}
}

// Follows() returns the follows associated with nodeID. If not present in the
// cache, they are loaded from the Database.
func (FC *FollowCache) Follows(ctx context.Context, nodeID uint32) ([]uint32, error) {
	if FC == nil {
		return []uint32{}, ErrNilFCPointer
	}

	follows, exists := FC.follows[nodeID]
	if !exists {
		followSlice, err := FC.DB.Follows(ctx, nodeID)
		if err != nil {
			return []uint32{}, err
		}

		follows = followSlice[0]
		FC.follows[nodeID] = follows
	}

	return follows, nil
}

// Load() loads the follows of a slice of nodeIDs from the DB.
func (FC *FollowCache) Load(ctx context.Context, nodeIDs ...uint32) error {
	if FC == nil {
		return ErrNilFCPointer
	}

	followSlice, err := FC.DB.Follows(ctx, nodeIDs...)
	if err != nil {
		return err
	}

	for i, follows := range followSlice {
		ID := nodeIDs[i]
		FC.follows[ID] = follows
	}

	return nil
}

// The NodeState contains:
// - positions: for each pos in positions, walks[pos] is a walk that visited nodeID
// - lastIndex: the index of the last position used. When lastIndex >= len(positions)
// all walks have been used for nodeID.
type NodeState struct {
	positions []int
	lastIndex int
}

// The WalkCache contains a slice of walks, and keeps track of which node was
// visited by which walks.
type WalkCache struct {
	walks  []models.RandomWalk
	states map[uint32]*NodeState
}

func NewWalkCache(size int) *WalkCache {
	return &WalkCache{
		walks:  make([]models.RandomWalk, 0, size),
		states: make(map[uint32]*NodeState, size),
	}
}

// Next() returns which walk to use for nodeID and whether that exists or not.
func (WC *WalkCache) Next(nodeID uint32) (models.RandomWalk, bool) {
	if WC == nil {
		return nil, false
	}

	state, exists := WC.states[nodeID]
	if !exists {
		return nil, false
	}

	for i, pos := range state.positions[state.lastIndex:] {
		if len(WC.walks[pos]) == 0 {
			continue
		}

		walk := WC.walks[pos]
		WC.walks[nodeID] = nil // zeroing the walk, so it can't be reused by other nodes

		state.lastIndex = i
		WC.states[nodeID] = state
		return walk, true
	}

	return nil, false
}

func (WC *WalkCache) Load(ctx context.Context, RWS models.RandomWalkStore, nodeIDs ...uint32) error {
	if WC == nil {
		return ErrNilWCPointer
	}

	walkMap, err := RWS.WalksUnion(ctx, nodeIDs)
	if err != nil {
		return err
	}

	WC.walks = make([]models.RandomWalk, len(walkMap))
	for _, ID := range nodeIDs {
		WC.states[ID] = &NodeState{
			positions: []int{},
			lastIndex: 0,
		}
	}

	var pos int
	for _, walk := range walkMap {
		WC.walks[pos] = walk

		// add the position of the walk in walks to each node visited by it,
		// excluding the last one (which will be cropped out anyway)
		for _, ID := range walk[:len(walk)-1] {
			state, exists := WC.states[ID]
			if !exists {
				state = &NodeState{
					positions: []int{},
					lastIndex: 0,
				}

				WC.states[ID] = state
			}

			WC.states[ID].positions = append(state.positions, pos)
		}

		pos++
	}

	return nil
}

// SetupFC() sets up a FollowCache based on the provided type.
func SetupFC(DB models.Database, FCType string) *FollowCache {
	switch FCType {
	case "nil":
		return nil

	case "empty":
		return NewFollowCache(DB, 1)

	case "one-node0":
		FC := NewFollowCache(DB, 1)
		FC.follows[0] = []uint32{0}
		return FC

	default:
		return nil
	}
}

// SetupWC() sets up a WalkCache based on the provided type.
func SetupWC(WCType string) *WalkCache {
	switch WCType {

	case "nil":
		return nil

	case "empty":
		return NewWalkCache(1)

	case "one-node0":
		WC := NewWalkCache(1)
		WC.walks = []models.RandomWalk{{0}}
		WC.states[0] = &NodeState{
			positions: []int{0},
			lastIndex: 0,
		}
		return WC

	case "all-used":
		WC := NewWalkCache(1)
		WC.walks = []models.RandomWalk{nil}
		WC.states[0] = &NodeState{
			positions: []int{0},
			lastIndex: 0,
		}
		return WC

	case "triangle":
		WC := NewWalkCache(3)
		WC.walks = []models.RandomWalk{{0, 1, 2}, {1, 2, 0}, {2, 0, 1}}
		for ID := uint32(0); ID < 3; ID++ {
			WC.states[0] = &NodeState{
				positions: []int{0, 1, 2},
				lastIndex: 0,
			}
		}
		return WC

	default:
		return nil
	}
}

// ---------------------------------ERROR-CODES--------------------------------

var ErrNilWCPointer = errors.New("nil walk cache pointer")
var ErrNilFCPointer = errors.New("nil follow cache pointer")
var ErrNodeNotInWalk = errors.New("nodeID not found in the walk")
