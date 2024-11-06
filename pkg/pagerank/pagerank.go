package pagerank

import (
	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

// a map that associates each nodeID with its corrisponding pagerank value
type PagerankMap map[uint32]float64

func Pagerank(RWS models.RandomWalkStore) (PagerankMap, error) {

	if err := RWS.Validate(false); err != nil {
		return nil, err
	}

	pagerank := make(PagerankMap, RWS.NodeCount())
	totalVisits := 0.0

	// iterate over the RWS
	for _, nodeID := range RWS.AllNodes() {
		nodeVisits := float64(RWS.VisitCount(nodeID))
		pagerank[nodeID] = nodeVisits
		totalVisits += nodeVisits
	}

	// normalize
	for nodeID, nodeVisits := range pagerank {
		pagerank[nodeID] = nodeVisits / totalVisits
	}

	return pagerank, nil
}
