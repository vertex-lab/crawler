package pagerank

import (
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

// a map that associates each nodeID with its corrisponding pagerank value
type PagerankMap map[uint32]float64

func Pagerank(RWM *walks.RandomWalksManager) (PagerankMap, error) {

	const expectEmptyRWM = false
	err := RWM.CheckState(expectEmptyRWM)
	if err != nil {
		return nil, err
	}

	// initialize
	pagerank := make(PagerankMap, len(RWM.WalksByNode))
	totalVisits := 0.0

	// iterate over the RWM
	for nodeID, walkSet := range RWM.WalksByNode {

		// this can be made more efficient by fetching the nodeVisits directly
		nodeVisits := float64(walkSet.Cardinality())
		totalVisits += nodeVisits

		pagerank[nodeID] = nodeVisits
	}

	// normalize
	for nodeID, nodeVisits := range pagerank {
		pagerank[nodeID] = nodeVisits / totalVisits
	}

	return pagerank, nil
}
