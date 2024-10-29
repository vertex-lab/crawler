package pagerank

import (
	"errors"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

// a slice of walks
type WalkSlice [][]uint32

/*
WalkCache keeps track of the walks associated with each node and the walks that
have been used in the personalized walk.

# FIELDS

	> NodeWalkSlice map[uint32]WalkSlice
	A map that associates each nodeID with a slice of walks that go through that node

	> NodeWalkIndex map[uint32]int
	A map that associates each nodeID with the last used walk index.
	The personalized walk will use the first walk in NodeWalkSlice[nodeID] (index = 0),
	then the second (index = 1) and so on. When the index reaches the lenght of
	the walkSlice, all walks have been used.

	> FetchedWalks walks.WalkSet
	A set of walk IDs that have been already fetched from the RWM. Because walks
	should not be reused in the personalized walk, we won't fetch walks that
	have already been fetched (even if not yet used).
*/
type WalkCache struct {
	NodeWalkSlice map[uint32]WalkSlice
	NodeWalkIndex map[uint32]int
	FetchedWalks  walks.WalkSet
}

// initializes an empty WC
func NewWalkCache() *WalkCache {
	return &WalkCache{
		NodeWalkSlice: make(map[uint32]WalkSlice),
		NodeWalkIndex: make(map[uint32]int),
		FetchedWalks:  mapset.NewSet[*walks.RandomWalk](),
	}
}

// returns the appropriate error if WC is nil or empty
func (WC *WalkCache) CheckEmpty() error {

	if WC == nil {
		return ErrNilWCPointer
	}

	if len(WC.NodeWalkSlice) == 0 {
		return ErrEmptyWC
	}

	return nil
}

// returns whether WC contains walks associated with nodeID
func (WC *WalkCache) Contains(nodeID uint32) bool {

	if WC == nil {
		return false
	}

	_, exist := WC.NodeWalkSlice[nodeID]
	return exist
}

// returns whether all walks of nodeID have been used. if WC is nil, returns true
func (WC *WalkCache) FullyUsed(nodeID uint32) bool {

	if !WC.Contains(nodeID) {
		return true
	}

	return WC.NodeWalkIndex[nodeID] >= len(WC.NodeWalkSlice[nodeID])
}

/*
fetches the WalkSet of nodeID from the RWM and stores up to `walkNum` walks
in the WalkCache. It avoid storing walks that have already been fetched (by other nodes).

The Load method will fetch the walks for nodeID only once.
Subsequent fetching will result in an error.

If walkNum is <= 0, all walks will be fetched.
*/
func (WC *WalkCache) Load(RWM *walks.RandomWalksManager,
	nodeID uint32, walksNum int) error {

	if WC == nil {
		return ErrNilWCPointer
	}

	if WC.Contains(nodeID) {
		return ErrNodeAlreadyLoadedWC
	}

	walkSet, err := RWM.WalksByNodeID(nodeID)
	if err != nil {
		return err
	}

	// remove previously fetched walks
	walkDiffSet := walkSet.Difference(WC.FetchedWalks)

	totalWalks := walkDiffSet.Cardinality()
	if totalWalks == 0 {
		// adding an empty slice signals that it was Loaded
		WC.NodeWalkSlice[nodeID] = [][]uint32{}
		return nil
	}

	// determine the number of walks to be fetched
	if walksNum <= 0 || walksNum > totalWalks {
		walksNum = totalWalks
	}

	walkSlice := make([][]uint32, 0, walksNum)
	for rWalk := range walkDiffSet.Iter() {
		if len(walkSlice) == walksNum {
			break
		}

		walkSegment, err := CropWalk(rWalk, nodeID)
		if err != nil {
			return err
		}

		// skip empty walk segments
		if len(walkSegment) == 0 {
			continue
		}

		WC.FetchedWalks.Add(rWalk)
		walkSlice = append(walkSlice, walkSegment)
	}

	WC.NodeWalkSlice[nodeID] = walkSlice
	return nil
}

// returns the walk from nodeID onward (excluded). If nodeID is not found, returns an error
func CropWalk(rWalk *walks.RandomWalk, nodeID uint32) ([]uint32, error) {

	// return the walk after nodeID (excluded)
	for i, ID := range rWalk.NodeIDs {
		if ID == nodeID {

			// make a copy for higher safety
			walkSegment := make([]uint32, len(rWalk.NodeIDs)-(i+1))
			copy(walkSegment, rWalk.NodeIDs[i+1:])
			return walkSegment, nil
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
func (WC *WalkCache) NextWalk(nodeID uint32) ([]uint32, error) {

	if err := WC.CheckEmpty(); err != nil {
		return nil, err
	}

	if !WC.Contains(nodeID) {
		return nil, ErrNodeNotFoundWC
	}

	if WC.NodeWalkIndex[nodeID] >= len(WC.NodeWalkSlice[nodeID]) {
		return nil, ErrAllWalksUsedWC
	}

	index := WC.NodeWalkIndex[nodeID]
	nextWalk := WC.NodeWalkSlice[nodeID][index]
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
