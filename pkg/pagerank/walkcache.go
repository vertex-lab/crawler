package pagerank

import (
	"errors"

	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

// a slice of random walks pointers
type WalkSlice []*walks.RandomWalk

/*
WalkCache keeps track of the walks associated with each node and the last
used index.

It's used in the Personalized pagerank function to select a walk
associated with nodeID that has never been used before:

# Example

index := WC.NodeIndex[nodeID]

walk := WC.NodeWalkSlice[index]

WC.NodeIndex[nodeID]++
*/
type WalkCache struct {
	NodeWalkSlice map[uint32]WalkSlice
	NodeIndex     map[uint32]int
}

// initializes an empty WalkCache;
func NewWalkCache() *WalkCache {
	return &WalkCache{
		NodeWalkSlice: make(map[uint32]WalkSlice),
		NodeIndex:     make(map[uint32]int),
	}
}

// returns the appropriate error if WC is empty or if it already contains some walks of nodeID
func (WC *WalkCache) CheckEmpty(nodeID uint32) error {

	if WC == nil {
		return ErrNilWCPointer
	}

	if _, exists := WC.NodeWalkSlice[nodeID]; exists {
		return ErrNonEmptyNodeWalkSlice
	}

	return nil
}

/*
fetches the WalkSet of nodeID from the RWM and stores up to `walkNum` random walks
pointers in the WalkCache.

If walkNum is <= 0, all walks will be fetched
*/
func (WC *WalkCache) Load(RWM *walks.RandomWalksManager,
	nodeID uint32, walksNum int) error {

	err := WC.CheckEmpty(nodeID)
	if err != nil {
		return err
	}

	walkSet, err := RWM.WalksByNodeID(nodeID)
	if err != nil {
		return err
	}

	// if it's 0 or negative, fetch all the walks
	if walksNum <= 0 || walksNum > walkSet.Cardinality() {
		walksNum = walkSet.Cardinality()
	}

	walkSlice := make([]*walks.RandomWalk, 0, walksNum)

	// add the first walksNum elements to the walkSlice
	for rWalk := range walkSet.Iter() {

		if len(walkSlice) == walksNum {
			break
		}

		// TODO; don't add walks that have nodeID as the last node!
		walkSlice = append(walkSlice, rWalk)
	}

	// add the walkSlice to the WalkCache
	WC.NodeWalkSlice[nodeID] = walkSlice

	return nil
}

/*
returns the next walks of nodeID from the WalkCache.
It returns a nil slice if there are no walks left for the nodeID.
An error is returned if the WalkCache is nil or if no walks exist for the nodeID.
*/
func (WC *WalkCache) NextWalk(nodeID uint32) ([]uint32, error) {

	if WC == nil {
		return nil, ErrNilWCPointer
	}

	walkSlice, exist := WC.NodeWalkSlice[nodeID]
	if !exist || len(walkSlice) == 0 {
		return nil, ErrEmptyNodeWalkSlice
	}

	// get the current index for the nodeID
	index := WC.NodeIndex[nodeID]

	// check if the index exceeds the available walks
	if index >= len(walkSlice) {
		return nil, nil
	}

	// get the current walk
	walk := walkSlice[index].NodeIDs

	// find the occurrance of nodeID
	for i, currentID := range walk {
		if currentID == nodeID {

			// increase the index and return the walk from nodeID onwards (excluded)
			WC.NodeIndex[nodeID]++
			return walk[i+1:], nil
		}
	}

	return walk, nil
}

// ---------------------------------ERROR-CODES--------------------------------

var ErrNilWCPointer = errors.New("nil walk cache pointer")
var ErrEmptyNodeWalkSlice = errors.New("walk cache for nodeID is empty")
var ErrNonEmptyNodeWalkSlice = errors.New("walk cache for nodeID already populated")
