package stochastictest

import (
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/pagerank"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestPagerankStatic(t *testing.T) {

	const maxExpectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 10000

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

			// setup the graph and pagerank
			setup := SetupGraph(test.graphType)
			DB := setup.DB
			expectedPR := setup.ExpectedPR

			// generate walks
			RWM, _ := walks.NewRWM("mock", alpha, walkPerNode)
			if err := RWM.GenerateAll(DB); err != nil {
				t.Fatalf("dynamic Pagerank: expected nil, pr %v", err)
			}

			// compute pagerank
			pr, err := pagerank.Pagerank(DB, RWM.Store)
			if err != nil {
				t.Errorf("Pagerank(): expected nil, pr %v", err)
			}

			// compute the error
			distance := pagerank.Distance(expectedPR, pr)
			if distance > maxExpectedDistance {
				t.Errorf("Pagerank(): expected distance %v, pr %v\n", maxExpectedDistance, distance)
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

	const maxExpectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 10000

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

			// setup the graph, pagerank and changes
			setup := SetupGraph(test.graphType)
			DB := setup.DB
			expectedPR := setup.ExpectedPR
			changes := setup.PotentialChanges

			// setup the old state
			nodeID, oldSuccessors, currentSuccessors := SetupOldState(DB, changes)
			DB.NodeIndex[nodeID].Successors = oldSuccessors

			// generate walks
			RWM, _ := walks.NewRWM("mock", alpha, walkPerNode)
			err := RWM.GenerateAll(DB)
			if err != nil {
				t.Fatalf("dynamic Pagerank: expected nil, pr %v", err)
			}

			// update the graph to the current state
			DB.NodeIndex[nodeID].Successors = currentSuccessors

			// update the random walks
			if err = RWM.Update(DB, nodeID, oldSuccessors, currentSuccessors); err != nil {
				t.Fatalf("dynamic Pagerank: expected nil, pr %v", err)
			}

			// compute pagerank
			pr, err := pagerank.Pagerank(DB, RWM.Store)
			if err != nil {
				t.Errorf("Pagerank(): expected nil, pr %v", err)
			}

			// check the error
			distance := pagerank.Distance(expectedPR, pr)
			if distance > maxExpectedDistance {
				t.Errorf("Pagerank(): expected distance %v, pr %v\n\n", maxExpectedDistance, distance)
				t.Errorf("expected %v\n; pr %v\n\n", expectedPR, pr)

				t.Errorf("nodeID: %v", nodeID)
				t.Errorf("oldSucc: %v", oldSuccessors)
				t.Errorf("currentSucc: %v", currentSuccessors)
			}
		})
	}
}
