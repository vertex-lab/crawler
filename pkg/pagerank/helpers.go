package pagerank

import (
	"math"

	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

// computes the L1 distance between two maps who are supposed to have the same keys
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
		WC.NodeWalkSlice[0] = []*walks.RandomWalk{{NodeIDs: []uint32{0}}}
		WC.NodeIndex[0] = 1
		return WC

	case "triangle":
		WC := NewWalkCache()

		WC.NodeWalkSlice[0] = []*walks.RandomWalk{
			{NodeIDs: []uint32{0, 1, 2}},
			{NodeIDs: []uint32{1, 2, 0}},
			{NodeIDs: []uint32{2, 0, 1}},
		}

		WC.NodeIndex[0] = 2
		return WC

	default:
		return nil
	}
}
