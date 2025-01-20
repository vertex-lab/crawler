package stochastictest

import (
	"context"
	"testing"

	"github.com/vertex-lab/crawler/pkg/pagerank"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

func TestPagerankStatic(t *testing.T) {
	ctx := context.Background()
	const maxExpectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 5000

	tests := []struct {
		name      string
		graphType string
	}{
		{
			name:      "static Pagerank, all dandling nodes",
			graphType: "dandlings",
		},
		{
			name:      "static Pagerank, triangle graph",
			graphType: "triangle",
		},
		{
			name:      "static Pagerank, cyclic graph 1",
			graphType: "cyclic1",
		},
		{
			name:      "static Pagerank, acyclic graph 1",
			graphType: "acyclic1",
		},
		{
			name:      "static Pagerank, acyclic graph 2",
			graphType: "acyclic2",
		},
		{
			name:      "static Pagerank, acyclic graph 3",
			graphType: "acyclic3",
		},
		{
			name:      "static Pagerank, acyclic graph 4",
			graphType: "acyclic4",
		},
		{
			name:      "static Pagerank, single cycle long 30",
			graphType: "cyclicLong50",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setup := SetupGraph(test.graphType)
			DB := setup.DB
			expectedPR := setup.ExpectedPR

			RWM, _ := walks.NewMockRWM(alpha, walkPerNode)
			if err := RWM.GenerateAll(ctx, DB); err != nil {
				t.Fatalf("GenerateAll: expected nil, pr %v", err)
			}

			nodeIDs, err := DB.AllNodes(ctx)
			if err != nil {
				t.Fatalf("AllNodes: expected nil, pr %v", err)
			}

			pr, err := pagerank.Global(ctx, RWM.Store, nodeIDs...)
			if err != nil {
				t.Errorf("Global(): expected nil, pr %v", err)
			}

			distance := pagerank.Distance(expectedPR, pr)
			if distance > maxExpectedDistance {
				t.Errorf("Global(): expected distance %v, pr %v\n", maxExpectedDistance, distance)
				t.Errorf("expected %v \npr %v", expectedPR, pr)
			}
		})
	}
}

/*
It is a known phenomenon that updateRemovedNodes does not return correct results
when the probability of cycles involving nodeID --> removedNode is high.

Therefore, test only with acyclic graphs, or graphs large enough that the
probability of such cycles is very low.
*/
func TestPagerankDynamic(t *testing.T) {
	ctx := context.Background()
	const maxExpectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 5000

	tests := []struct {
		name      string
		graphType string
	}{
		{
			name:      "dynamic Pagerank, all dandling nodes",
			graphType: "dandlings",
		},
		{
			name:      "dynamic Pagerank, acyclic graph 1",
			graphType: "acyclic1",
		},
		{
			name:      "dynamic Pagerank, acyclic graph 2",
			graphType: "acyclic2",
		},
		{
			name:      "dynamic Pagerank, acyclic graph 3",
			graphType: "acyclic3",
		},
		{
			name:      "dynamic Pagerank, acyclic graph 4",
			graphType: "acyclic4",
		},
		{
			name:      "dynamic Pagerank, single cycle long 30",
			graphType: "cyclicLong50",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setup := SetupGraph(test.graphType)
			DB := setup.DB
			expectedPR := setup.ExpectedPR
			changes := setup.PotentialChanges

			nodeID, oldFollows, currentFollows := SetupOldState(DB, changes)
			DB.NodeIndex[nodeID].Follows = oldFollows

			RWM, _ := walks.NewMockRWM(alpha, walkPerNode)
			err := RWM.GenerateAll(ctx, DB)
			if err != nil {
				t.Fatalf("GenerateAll: expected nil, pr %v", err)
			}

			// update the graph to the current state
			DB.NodeIndex[nodeID].Follows = currentFollows
			removed, common, added := sliceutils.Partition(oldFollows, currentFollows)
			if _, err = RWM.Update(ctx, DB, nodeID, removed, common, added); err != nil {
				t.Fatalf("Update: expected nil, pr %v", err)
			}

			nodeIDs, err := DB.AllNodes(ctx)
			if err != nil {
				t.Fatalf("AllNodes: expected nil, pr %v", err)
			}

			pr, err := pagerank.Global(ctx, RWM.Store, nodeIDs...)
			if err != nil {
				t.Errorf("Global(): expected nil, pr %v", err)
			}

			distance := pagerank.Distance(expectedPR, pr)
			if distance > maxExpectedDistance {
				t.Errorf("Global(): expected distance %v, pr %v\n\n", maxExpectedDistance, distance)
				t.Errorf("expected %v\n; pr %v\n\n", expectedPR, pr)

				t.Errorf("nodeID: %v", nodeID)
				t.Errorf("oldSucc: %v", oldFollows)
				t.Errorf("currentSucc: %v", currentFollows)
			}
		})
	}
}

func TestPersonalizedPagerank(t *testing.T) {
	ctx := context.Background()
	const nodeID = 0
	const maxExpectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 1000
	const topk = 200

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
			setup := SetupGraph(test.graphType)
			DB := setup.DB
			expectedPPR0 := setup.ExpectedPPR0

			RWM, _ := walks.NewMockRWM(alpha, walkPerNode)
			err := RWM.GenerateAll(ctx, DB)
			if err != nil {
				t.Fatalf("GenerateAll: expected nil, got %v", err)
			}

			got, err := pagerank.Personalized(ctx, DB, RWM.Store, nodeID, topk)
			if err != nil {
				t.Errorf("Personalized(): expected nil, got %v", err)
			}

			distance := pagerank.Distance(expectedPPR0, got)
			if distance > maxExpectedDistance {
				t.Errorf("Personalized(): expected distance %v, got %v\n", maxExpectedDistance, distance)
				t.Errorf("expected %v \ngot %v", expectedPPR0, got)
			}
		})
	}
}
