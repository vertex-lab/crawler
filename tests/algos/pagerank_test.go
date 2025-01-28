package stochastictest

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/walks"
)

func TestPagerankStatic(t *testing.T) {
	const maxExpectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 5000

	tests := []struct {
		name  string
		setup func() (StaticSetup, []*models.Delta)
	}{
		{
			name:  "static Pagerank, all dandling nodes",
			setup: Dandlings,
		},
		{
			name:  "static Pagerank, triangle graph",
			setup: Triangle,
		},
		{
			name:  "static Pagerank, triangle plus one",
			setup: TrianglePlusOne,
		},
		{
			name:  "static Pagerank, acyclic graph 1",
			setup: Acyclic1,
		},
		{
			name:  "static Pagerank, acyclic graph 2",
			setup: Acyclic2,
		},
		{
			name:  "static Pagerank, acyclic graph 3",
			setup: Acyclic3,
		},
		{
			name:  "static Pagerank, acyclic graph 4",
			setup: Acyclic4,
		},
		{
			name:  "static Pagerank, single cycle long 30",
			setup: CyclicLong50,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			setup, _ := test.setup()
			DB, expectedGlobal := setup.DB, setup.expectedGlobal

			RWS, _ := mockstore.NewRWS(alpha, walkPerNode)
			if err := walks.GenerateAll(ctx, DB, RWS); err != nil {
				t.Fatalf("GenerateAll: expected nil, got %v", err)
			}

			nodeIDs, err := DB.AllNodes(ctx)
			if err != nil {
				t.Fatalf("AllNodes: expected nil, pr %v", err)
			}

			pr, err := pagerank.Global(ctx, RWS, nodeIDs...)
			if err != nil {
				t.Errorf("Global(): expected nil, pr %v", err)
			}

			distance := pagerank.Distance(expectedGlobal, pr)
			if distance > maxExpectedDistance {
				t.Errorf("Global(): expected distance %v, pr %v\n", maxExpectedDistance, distance)
				t.Errorf("expected %v \npr %v", expectedGlobal, pr)
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
	const walkPerNode = 5000

	tests := []struct {
		name  string
		setup func() (StaticSetup, []*models.Delta)
	}{
		{
			name:  "dynamic Pagerank, all dandling nodes",
			setup: Dandlings,
		},
		{
			name:  "dynamic Pagerank, acyclic graph 1",
			setup: Acyclic1,
		},
		{
			name:  "dynamic Pagerank, acyclic graph 2",
			setup: Acyclic2,
		},
		{
			name:  "dynamic Pagerank, acyclic graph 3",
			setup: Acyclic3,
		},
		{
			name:  "dynamic Pagerank, acyclic graph 4",
			setup: Acyclic4,
		},
		{
			name:  "dynamic Pagerank, single cycle long 30",
			setup: CyclicLong50,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			setup, deltas := test.setup()
			DB, expectedGlobal := setup.DB, setup.expectedGlobal

			// randomly select one of the deltas
			index := rand.IntN(len(deltas))
			delta := deltas[index]

			if err := DB.Update(ctx, delta); err != nil {
				t.Fatalf("Update(%v): expected nil, got %v", delta, err)
			}

			RWS, _ := mockstore.NewRWS(alpha, walkPerNode)
			if err := walks.GenerateAll(ctx, DB, RWS); err != nil {
				t.Fatalf("GenerateAll: expected nil, pr %v", err)
			}

			// make the DB return to the initial state
			inverse := Inverse(delta)
			common := Unchanged(DB, inverse)
			if err := DB.Update(ctx, inverse); err != nil {
				t.Fatalf("Update(%v): expected nil, got %v", delta, err)
			}

			if _, err := walks.Update(ctx, DB, RWS, inverse.NodeID, inverse.Removed, common, inverse.Added); err != nil {
				t.Fatalf("Update: expected nil, pr %v", err)
			}

			nodeIDs, err := DB.AllNodes(ctx)
			if err != nil {
				t.Fatalf("AllNodes: expected nil, pr %v", err)
			}

			pr, err := pagerank.Global(ctx, RWS, nodeIDs...)
			if err != nil {
				t.Errorf("Global(): expected nil, pr %v", err)
			}

			distance := pagerank.Distance(expectedGlobal, pr)
			if distance > maxExpectedDistance {
				t.Errorf("Global(): expected distance %v, pr %v\n\n", maxExpectedDistance, distance)
				t.Errorf("expected %v\n; pr %v\n\n", expectedGlobal, pr)
				t.Errorf("delta: %v", delta)
			}
		})
	}
}

func TestPersonalizedPagerank(t *testing.T) {
	const nodeID = 0
	const maxExpectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 1000
	const topk = 200

	tests := []struct {
		name  string
		setup func() (StaticSetup, []*models.Delta)
	}{
		{
			name:  "Personalized Pagerank, all dandling nodes",
			setup: Dandlings,
		},
		{
			name:  "Personalized Pagerank, acyclic graph 1",
			setup: Acyclic1,
		},
		{
			name:  "Personalized Pagerank, acyclic graph 2",
			setup: Acyclic2,
		},
		{
			name:  "Personalized Pagerank, acyclic graph 3",
			setup: Acyclic3,
		},
		{
			name:  "Personalized Pagerank, acyclic graph 4",
			setup: Acyclic4,
		},
		{
			name:  "Personalized Pagerank, single cycle long 30",
			setup: CyclicLong50,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			setup, _ := test.setup()
			DB, expectedPersonalized0 := setup.DB, setup.expectedPersonalized0

			RWS, _ := mockstore.NewRWS(alpha, walkPerNode)
			if err := walks.GenerateAll(ctx, DB, RWS); err != nil {
				t.Fatalf("GenerateAll: expected nil, got %v", err)
			}

			pp, err := pagerank.Personalized(ctx, DB, RWS, nodeID, topk)
			if err != nil {
				t.Errorf("Personalized(): expected nil, got %v", err)
			}

			distance := pagerank.Distance(expectedPersonalized0, pp)
			if distance > maxExpectedDistance {
				t.Errorf("Personalized(): expected distance %v, got %v\n", maxExpectedDistance, distance)
				t.Errorf("expected %v \ngot %v", expectedPersonalized0, pp)
			}
		})
	}
}
