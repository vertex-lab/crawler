package pagerank

import (
	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

// a map that associates each nodeID with its corrisponding pagerank value
type PagerankMap map[uint32]float64

func Pagerank(DB models.Database, RWS models.RandomWalkStore) (PagerankMap, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if err := RWS.Validate(false); err != nil {
		return nil, err
	}

	pagerank := make(PagerankMap, DB.NodeCount())
	totalVisits := 0.0

	// iterate over the RWS
	nodeIDs, err := DB.AllNodes()
	if err != nil {
		return nil, err
	}

	for _, nodeID := range nodeIDs {
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
