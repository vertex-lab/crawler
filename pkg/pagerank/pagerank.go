// The pagerank package defines the algorithms that use the previously generated random walks.
package pagerank

import (
	"github.com/vertex-lab/crawler/pkg/models"
)

func Pagerank(DB models.Database, RWS models.RandomWalkStore) (models.PagerankMap, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if err := RWS.Validate(false); err != nil {
		return nil, err
	}

	nodeIDs, err := DB.AllNodes()
	if err != nil {
		return nil, err
	}

	visitMap, err := RWS.VisitCounts(nodeIDs)
	if err != nil {
		return nil, err
	}

	// get the total visits
	totalVisits := 0.0
	for _, visits := range visitMap {
		totalVisits += float64(visits)
	}

	// compute the pagerank as the frequency of visits
	pagerank := make(models.PagerankMap, len(nodeIDs))
	for nodeID, visits := range visitMap {
		pagerank[nodeID] = float64(visits) / totalVisits
	}

	return pagerank, nil
}
