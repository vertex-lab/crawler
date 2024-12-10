package stochastictest

import (
	"context"
	"testing"

	"github.com/vertex-lab/crawler/pkg/pagerank"
	"github.com/vertex-lab/crawler/pkg/walks"
)

func TestPersonalizedPagerank(t *testing.T) {
	const maxExpectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 10000
	ctx := context.Background()

	tests := []struct {
		name      string
		graphType string
	}{
		{
			name:      "Personalized Pagerank, all dandling nodes",
			graphType: "dandlings",
		},
		{
			name:      "Personalized Pagerank, acyclic graph 1",
			graphType: "acyclic1",
		},
		{
			name:      "Personalized Pagerank, acyclic graph 2",
			graphType: "acyclic2",
		},
		{
			name:      "Personalized Pagerank, acyclic graph 3",
			graphType: "acyclic3",
		},
		{
			name:      "Personalized Pagerank, acyclic graph 4",
			graphType: "acyclic4",
		},
		{
			name:      "Personalized Pagerank, single cycle long 30",
			graphType: "cyclicLong50",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// setup the graph and pagerank
			setup := SetupGraph(test.graphType)
			DB := setup.DB
			expectedPPR0 := setup.ExpectedPPR0

			// generate walks
			RWM, _ := walks.NewMockRWM(alpha, walkPerNode)
			err := RWM.GenerateAll(ctx, DB)
			if err != nil {
				t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
			}

			// compute pagerank
			got, err := pagerank.Personalized(ctx, DB, RWM.Store, 0, 5)
			if err != nil {
				t.Errorf("Pagerank(): expected nil, got %v", err)
			}

			// compute the error
			distance := pagerank.Distance(expectedPPR0, got)
			if distance > maxExpectedDistance {
				t.Errorf("Pagerank(): expected pagerank.Distance %v, got %v\n", maxExpectedDistance, distance)
				t.Errorf("expected %v \ngot %v", expectedPPR0, got)
			}
		})
	}
}
