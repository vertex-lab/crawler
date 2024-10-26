package pagerank

import (
	"errors"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

// a slice of random walks pointers
type WalkSlice []*walks.RandomWalk

/*
WalkCache keeps track of the walks associated with each node and the walks that
have been used in the personalized walk.

If all walks for a node have been used, we set NodeFullyUsed[nodeID] = true for
faster checking.

The WalkCache is used in the Personalized pagerank function to select a walk
associated with nodeID that has never been used before.
*/
type WalkCache struct {
	NodeWalkSlice map[uint32]WalkSlice
	UsedWalks     walks.WalkSet
	NodeFullyUsed map[uint32]bool
}

// initializes an empty WC
func NewWalkCache() *WalkCache {
	return &WalkCache{
		NodeWalkSlice: make(map[uint32]WalkSlice),
		UsedWalks:     mapset.NewSet[*walks.RandomWalk](),
		NodeFullyUsed: make(map[uint32]bool),
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

	walkSlice, exists := WC.NodeWalkSlice[nodeID]
	return exists && len(walkSlice) > 0
}

// returns whether all walks of nodeID have been used. if WC is nil, returns true
func (WC *WalkCache) FullyUsed(nodeID uint32) bool {

	if !WC.Contains(nodeID) {
		return true
	}

	if WC.NodeFullyUsed[nodeID] {
		return true
	}

	return false
}

/*
fetches the WalkSet of nodeID from the RWM and stores up to `walkNum` random walks
pointers in the WalkCache.

To avoid complexity, the load method will fetch the walks for nodeID only once.
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

	// if it's <= 0, fetch all the walks
	if walksNum <= 0 || walksNum > walkSet.Cardinality() {

		WC.NodeWalkSlice[nodeID] = walkSet.ToSlice()
		return nil
	}

	// add the first walksNum elements to the walkSlice
	walkSlice := make([]*walks.RandomWalk, 0, walksNum)
	for rWalk := range walkSet.Iter() {

		if len(walkSlice) == walksNum {
			break
		}
		walkSlice = append(walkSlice, rWalk)
	}

	WC.NodeWalkSlice[nodeID] = walkSlice
	return nil
}

/*
returns the next walk of nodeID from the WalkCache. It returns an empty slice if all walks have been used.
An error is returned if the WalkCache is nil or if no walks exist for the nodeID.
*/
func (WC *WalkCache) NextWalk(nodeID uint32) ([]uint32, error) {

	err := WC.CheckEmpty()
	if err != nil {
		return nil, err
	}

	if !WC.Contains(nodeID) {
		return nil, ErrNodeNotFoundWC
	}

	// try to find an unused walk
	walkSlice := WC.NodeWalkSlice[nodeID]
	for i, rWalk := range walkSlice {

		if !WC.UsedWalks.ContainsOne(rWalk) {

			// if this was the last available walk, now all are fully used
			if i == len(walkSlice)-1 {
				WC.NodeFullyUsed[nodeID] = true
			}

			WC.UsedWalks.Add(rWalk)
			walk, err := CropWalk(rWalk, nodeID)
			if err != nil {
				return nil, err
			}

			return walk, nil
		}
	}

	return nil, ErrAllWalksUsedWC

}

// returns the walk from nodeID onward (included). If nodeID is not found, returns an error
func CropWalk(rWalk *walks.RandomWalk, nodeID uint32) ([]uint32, error) {

	// return the walk after nodeID
	for i, ID := range rWalk.NodeIDs {
		if ID == nodeID {
			return rWalk.NodeIDs[i+1:], nil
		}
	}

	return nil, ErrNodeNotInWalk
}

// ---------------------------------ERROR-CODES--------------------------------

var ErrNilWCPointer = errors.New("nil walk cache pointer")
var ErrEmptyWC = errors.New("walk cache is empty")

var ErrNodeNotFoundWC = errors.New("nodeID not found in WC")
var ErrNodeAlreadyLoadedWC = errors.New("walk cache for nodeID already populated")
var ErrAllWalksUsedWC = errors.New("all walks of nodeID have been used")
var ErrNodeNotInWalk = errors.New("nodeID not found in the walk")
