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
		rWalk := walks.RandomWalk{NodeIDs: []uint32{0}}
		WC.NodeWalkSlice[0] = [][]uint32{rWalk.NodeIDs}
		WC.FetchedWalks.Add(&rWalk)
		return WC

	case "all-used":
		WC := NewWalkCache()
		rWalk := walks.RandomWalk{NodeIDs: []uint32{0}}
		WC.NodeWalkSlice[0] = [][]uint32{rWalk.NodeIDs}
		WC.FetchedWalks.Add(&rWalk)

		WC.NodeWalkIndex[0] = 1 // all used
		return WC

	case "triangle":
		WC := NewWalkCache()
		rWalk0 := walks.RandomWalk{NodeIDs: []uint32{0, 1, 2}}
		rWalk1 := walks.RandomWalk{NodeIDs: []uint32{1, 2, 0}}
		rWalk2 := walks.RandomWalk{NodeIDs: []uint32{2, 0, 1}}

		WC.NodeWalkSlice[0] = [][]uint32{rWalk0.NodeIDs[1:], rWalk2.NodeIDs[2:]}
		WC.FetchedWalks.Append(&rWalk0, &rWalk1, &rWalk2)
		return WC

	default:
		return nil
	}
}

// function that set up a PersonalizedWalk based on the provided type and required lenght
func SetupPWalk(pWalkType string, targetLenght int) *PersonalizedWalk {

	switch pWalkType {

	case "one-node0":
		return NewPersonalizedWalk(0, targetLenght)

	case "triangle":
		pWalk := NewPersonalizedWalk(0, targetLenght)
		pWalk.currentNodeID = 2
		pWalk.currentWalk = []uint32{0, 1, 2}
		return pWalk

	default:
		return NewPersonalizedWalk(0, targetLenght)
	}
}
