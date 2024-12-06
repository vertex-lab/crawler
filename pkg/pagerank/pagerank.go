// The pagerank package defines the algorithms that use the previously generated random walks.
package pagerank

import (
	"math"

	"github.com/vertex-lab/crawler/pkg/models"
)

func Distance(map1, map2 models.PagerankMap) float64 {
	distance := 0.0
	for key := range map1 {
		distance += math.Abs(map1[key] - map2[key])
	}
	return distance
}

// Pagerank() computes the pagerank score for each node in the database.
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

	// update the value of totalVisits
	if err := RWS.SetTotalVisits(int(totalVisits)); err != nil {
		return nil, err
	}

	// compute the pagerank as the frequency of visits
	pagerank := make(models.PagerankMap, len(nodeIDs))
	for nodeID, visits := range visitMap {
		pagerank[nodeID] = float64(visits) / totalVisits
	}

	return pagerank, nil
}

// LazyPagerank() computes the pagerank scores of only the specified nodes nodeIDs.
// It fetches the current value of totalVisits, which could be slightly
func LazyPagerank(DB models.Database, RWS models.RandomWalkStore, nodeIDs []uint32) (models.PagerankMap, error) {

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if err := RWS.Validate(false); err != nil {
		return nil, err
	}

	totalVisits := RWS.TotalVisits()
	visitMap, err := RWS.VisitCounts(nodeIDs)
	if err != nil {
		return nil, err
	}

	// compute the pagerank as the frequency of visits
	pagerank := make(models.PagerankMap, len(nodeIDs))
	for nodeID, visits := range visitMap {
		pagerank[nodeID] = float64(visits) / float64(totalVisits)
	}

	return pagerank, nil
}
