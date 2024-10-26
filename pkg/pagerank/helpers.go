package pagerank

import (
	"math"

	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

// computes the L1 distance between two maps who are supposed to have the same keys.
// if map 1 is nil or empty, it returns 0.0
func Distance(map1, map2 PagerankMap) float64 {

	distance := 0.0

	for key, val1 := range map1 {
		distance += math.Abs(val1 - map2[key])
	}

	return distance
}

// function that set up a WalkCache based on the provided WalkCache type
func SetupWC(WCType string) *WalkCache {

	switch WCType {

	case "nil":
		return nil

	case "empty":
		return NewWalkCache()

	case "one-node0":
		WC := NewWalkCache()
		WC.NodeWalkSlice[0] = []*walks.RandomWalk{{NodeIDs: []uint32{0}}}
		return WC

	case "all-used":
		WC := NewWalkCache()
		rWalk := &walks.RandomWalk{NodeIDs: []uint32{0}}

		WC.NodeWalkSlice[0] = []*walks.RandomWalk{rWalk}
		WC.UsedWalks.Add(rWalk)
		WC.NodeFullyUsed[0] = true
		return WC

	case "triangle":
		WC := NewWalkCache()
		rWalk0 := &walks.RandomWalk{NodeIDs: []uint32{0, 1, 2}}
		rWalk1 := &walks.RandomWalk{NodeIDs: []uint32{1, 2, 0}}
		rWalk2 := &walks.RandomWalk{NodeIDs: []uint32{2, 0, 1}}

		WC.NodeWalkSlice[0] = []*walks.RandomWalk{rWalk0, rWalk1, rWalk2}
		WC.UsedWalks.Add(rWalk0)
		WC.UsedWalks.Add(rWalk1)
		return WC

	default:
		return nil
	}
}
