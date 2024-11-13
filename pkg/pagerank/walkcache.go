package pagerank

import (
	"errors"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

/*
WalkCache keeps track of the walks associated with each node and the walks that
have been used in the personalized walk.

# FIELDS

	> NodeWalks map[uint32][]models.RandomWalk
	A map that associates each nodeID with a slice of walks that go through that node

	> NodeWalkIndex map[uint32]int
	A map that associates each nodeID with the last used walk index.
	The personalized walk will use the first walk in NodeWalks[nodeID] (index = 0),
	then the second (index = 1) and so on. When the index reaches the lenght of
	the walkSlice, all walks have been used.

	> LoadedWalkIDs models.WalkIDSet
	A set of walk IDs that have been already fetched from the RWS. Because walks
	should not be reused in the personalized walk, we won't fetch walks that
	have already been fetched (even if not yet used).
*/
type WalkCache struct {
	NodeWalks     map[uint32][]models.RandomWalk
	NodeWalkIndex map[uint32]int
	LoadedWalkIDs models.WalkIDSet
}

// initializes an empty WC
func NewWalkCache() *WalkCache {
	return &WalkCache{
		NodeWalks:     make(map[uint32][]models.RandomWalk),
		NodeWalkIndex: make(map[uint32]int),
		LoadedWalkIDs: mapset.NewSet[uint32](),
	}
}

// Validate() returns the appropriate error if WC is nil or empty
func (WC *WalkCache) Validate() error {

	if WC == nil {
		return ErrNilWCPointer
	}

	if len(WC.NodeWalks) == 0 {
		return ErrEmptyWC
	}

	return nil
}

// ContainsNode returns whether WC contains walks associated with nodeID
func (WC *WalkCache) ContainsNode(nodeID uint32) bool {

	if WC == nil {
		return false
	}

	_, exist := WC.NodeWalks[nodeID]
	return exist
}

// FullyUsed() returns whether all walks of nodeID have been used. if WC is nil, returns true
func (WC *WalkCache) FullyUsed(nodeID uint32) bool {

	if !WC.ContainsNode(nodeID) {
		return true
	}

	return WC.NodeWalkIndex[nodeID] >= len(WC.NodeWalks[nodeID])
}

/*
fetches the WalkIDSet of nodeID from the RWS and stores up to `limit` walks
in the WalkCache. It avoid storing walks that have already been fetched from other nodes.

The Load method will fetch the walks for nodeID only once.
Subsequent fetching will result in an error.

If limit is <= 0, all walks will be fetched.
*/
func (WC *WalkCache) Load(RWS models.RandomWalkStore, nodeID uint32, limit int) error {

	if WC == nil {
		return ErrNilWCPointer
	}

	if WC.ContainsNode(nodeID) {
		return ErrNodeAlreadyLoadedWC
	}

	walkMap, err := RWS.Walks(nodeID, -1)
	if err != nil {
		return err
	}

	if limit <= 0 {
		limit = len(walkMap)
	}

	walks := make([]models.RandomWalk, 0, limit)
	for walkID, walk := range walkMap {

		// the exit condition
		if len(walks) == limit {
			break
		}

		// skip walks already loaded
		if WC.LoadedWalkIDs.ContainsOne(walkID) {
			continue
		}

		walkSegment, err := CropWalk(walk, nodeID)
		if err != nil {
			return err
		}

		// skip empty walk segments
		if len(walkSegment) == 0 {
			continue
		}

		WC.LoadedWalkIDs.Add(walkID)
		walks = append(walks, walkSegment)
	}

	WC.NodeWalks[nodeID] = walks
	return nil
}

// returns the walk from nodeID onward (excluded). If nodeID is not found, returns an error
func CropWalk(walk models.RandomWalk, nodeID uint32) (models.RandomWalk, error) {

	// return the walk after nodeID (excluded)
	for i, ID := range walk {
		if ID == nodeID {
			return walk[i+1:], nil
		}
	}

	return nil, ErrNodeNotInWalk
}

/*
returns the next walk of nodeID from the WalkCache.
It returns errors if:
- the WalkCache is nil
- no walks exist for nodeID
- all walks for nodeID have been used
*/
func (WC *WalkCache) NextWalk(nodeID uint32) (models.RandomWalk, error) {

	if err := WC.Validate(); err != nil {
		return nil, err
	}

	if !WC.ContainsNode(nodeID) {
		return nil, ErrNodeNotFoundWC
	}

	if WC.NodeWalkIndex[nodeID] >= len(WC.NodeWalks[nodeID]) {
		return nil, ErrAllWalksUsedWC
	}

	index := WC.NodeWalkIndex[nodeID]
	nextWalk := WC.NodeWalks[nodeID][index]
	WC.NodeWalkIndex[nodeID]++
	return nextWalk, nil
}

// ---------------------------------ERROR-CODES--------------------------------

var ErrNilWCPointer = errors.New("nil walk cache pointer")
var ErrEmptyWC = errors.New("walk cache is empty")

var ErrNodeNotFoundWC = errors.New("nodeID not found in WC")
var ErrNodeAlreadyLoadedWC = errors.New("walk cache for nodeID already populated")
var ErrAllWalksUsedWC = errors.New("all walks of nodeID have been used")
var ErrNodeNotInWalk = errors.New("nodeID not found in the walk")
