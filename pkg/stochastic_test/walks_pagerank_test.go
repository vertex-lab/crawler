package stochastictest

import (
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/pagerank"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestPagerankStatic(t *testing.T) {

	t.Run("static Pagerank, test graph 1", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 3}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{}}

		// generate walks
		RWM, _ := walks.NewRWM(0.85, 1000)
		RWM.GenerateAll(DB)

		// the expected acyclic pagerank
		pr := pagerank.PagerankMap{
			0: 0.29700319989476004,
			1: 0.20616253803697476,
			2: 0.2552206288779828,
			3: 0.24161363319028237,
		}

		got, err := pagerank.Pagerank(RWM)
		if err != nil {
			t.Errorf("Pagerank(): expected nil, got %v", err)
		}

		expectedDistance := 0.02
		if distance(pr, got) > expectedDistance {
			t.Errorf("Pagerank(): expected distance %v, got %v\n", expectedDistance, distance(pr, got))
			t.Errorf("expected %v \ngot %v", pr, got)
		}

	})

	t.Run("static Pagerank, static test graph 2", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{3}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{1}}
		DB.Nodes[4] = &graph.Node{ID: 4, SuccessorIDs: []uint32{}}

		// generate walks
		RWM, _ := walks.NewRWM(0.85, 1000)
		RWM.GenerateAll(DB)

		// the expected acyclic pagerank
		pr := pagerank.PagerankMap{
			0: 0.11185368285521291,
			1: 0.36950360789646736,
			2: 0.15943176539450626,
			3: 0.24735726099860061,
			4: 0.11185368285521291,
		}

		got, err := pagerank.Pagerank(RWM)
		if err != nil {
			t.Errorf("Pagerank(): expected nil, got %v", err)
		}

		expectedDistance := 0.02
		if distance(pr, got) > expectedDistance {
			t.Errorf("Pagerank(): expected distance %v, got %v\n", expectedDistance, distance(pr, got))
			t.Errorf("expected %v \ngot %v", pr, got)
		}

	})
}
