// The pagerank package defines the algorithms that use the previously generated random walks.
package pagerank

import (
	"context"
	"math"

	"github.com/vertex-lab/crawler/pkg/models"
)

// Distance() returns the L1 distance between two maps.
func Distance(map1, map2 models.PagerankMap) float64 {
	var distance float64
	for key := range map1 {
		distance += math.Abs(map1[key] - map2[key])
	}
	return distance
}

// Pagerank() computes the pagerank score for each node in the database.
func Pagerank(
	ctx context.Context,
	DB models.Database,
	RWS models.RandomWalkStore) (models.PagerankMap, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if err := RWS.Validate(); err != nil {
		return nil, err
	}

	nodeIDs, err := DB.AllNodes()
	if err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return nil, models.ErrEmptyDB
	}

	visitMap, err := RWS.VisitCounts(ctx, nodeIDs)
	if err != nil {
		return nil, err
	}

	var totalVisits float64
	for _, visits := range visitMap {
		totalVisits += float64(visits)
	}

	if totalVisits == 0 {
		return nil, models.ErrEmptyRWS
	}

	// compute the pagerank as the frequency of visits
	pagerank := make(models.PagerankMap, len(nodeIDs))
	for nodeID, visits := range visitMap {
		pagerank[nodeID] = float64(visits) / totalVisits
	}

	return pagerank, nil
}

// LazyPagerank() computes the pagerank scores of only the specified nodeIDs.
func LazyPagerank(
	ctx context.Context,
	DB models.Database,
	RWS models.RandomWalkStore,
	nodeIDs []uint32) (models.PagerankMap, error) {

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if err := RWS.Validate(); err != nil {
		return nil, err
	}

	totalVisits := RWS.TotalVisits(ctx)
	if totalVisits == 0 {
		return nil, models.ErrEmptyRWS
	}

	visitMap, err := RWS.VisitCounts(ctx, nodeIDs)
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
