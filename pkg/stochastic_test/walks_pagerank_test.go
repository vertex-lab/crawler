package stochastictest

import (
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/pagerank"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestPagerankStatic(t *testing.T) {

	const expectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 4000

	t.Run("static Pagerank, triangle graph", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}

		// generate walks
		RWM, _ := walks.NewRWM(alpha, walkPerNode)
		err := RWM.GenerateAll(DB)
		if err != nil {
			t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
		}

		// the expected pagerank
		pr := pagerank.PagerankMap{
			0: 0.3333,
			1: 0.3333,
			2: 0.3333,
		}

		got, err := pagerank.Pagerank(RWM)
		if err != nil {
			t.Errorf("Pagerank(): expected nil, got %v", err)
		}

		if distance(pr, got) > expectedDistance {
			t.Errorf("Pagerank(): expected distance %v, got %v\n", expectedDistance, distance(pr, got))
			t.Errorf("expected %v \ngot %v", pr, got)
		}

	})

	t.Run("static Pagerank, test graph 1", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 3}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{}}

		// generate walks
		RWM, _ := walks.NewRWM(alpha, walkPerNode)
		err := RWM.GenerateAll(DB)
		if err != nil {
			t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
		}

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

		if distance(pr, got) > expectedDistance {
			t.Errorf("Pagerank(): expected distance %v, got %v\n", expectedDistance, distance(pr, got))
			t.Errorf("expected %v \ngot %v", pr, got)
		}

	})

	t.Run("static Pagerank, test graph 2 (acyclic)", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{3}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{1}}
		DB.Nodes[4] = &graph.Node{ID: 4, SuccessorIDs: []uint32{}}

		// generate walks
		RWM, _ := walks.NewRWM(alpha, walkPerNode)
		err := RWM.GenerateAll(DB)
		if err != nil {
			t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
		}

		// the expected pagerank
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

		if distance(pr, got) > expectedDistance {
			t.Errorf("Pagerank(): expected distance %v, got %v\n", expectedDistance, distance(pr, got))
			t.Errorf("expected %v \ngot %v", pr, got)
		}

	})
}

func TestPagerankDynamic(t *testing.T) {

	const expectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 4000

	t.Run("dynamic Pagerank, triangle graph", func(t *testing.T) {

		oldSuccessors := []uint32{1}

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: oldSuccessors}

		// generate walks
		RWM, _ := walks.NewRWM(alpha, walkPerNode)
		err := RWM.GenerateAll(DB)
		if err != nil {
			t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
		}

		// graph update
		newSuccessors := []uint32{0}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: newSuccessors}

		// the expected pagerank
		pr := pagerank.PagerankMap{
			0: 0.3333,
			1: 0.3333,
			2: 0.3333,
		}

		// update the random walks
		err = RWM.Update(DB, 2, oldSuccessors, newSuccessors)
		if err != nil {
			t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
		}

		got, err := pagerank.Pagerank(RWM)
		if err != nil {
			t.Errorf("dynamic Pagerank: expected nil, got %v", err)
		}

		if distance(pr, got) > expectedDistance {
			t.Errorf("dynamic Pagerank: expected distance %v, got %v\n", expectedDistance, distance(pr, got))
			t.Errorf("expected %v \ngot %v", pr, got)
		}

	})

	t.Run("dynamic Pagerank, test graph 1", func(t *testing.T) {

		oldSuccessors := []uint32{1, 2}

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 3}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: oldSuccessors}

		// generate walks
		RWM, _ := walks.NewRWM(alpha, walkPerNode)
		err := RWM.GenerateAll(DB)
		if err != nil {
			t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
		}

		// graph update
		newSuccessors := []uint32{}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: newSuccessors}

		// the expected acyclic pagerank
		pr := pagerank.PagerankMap{
			0: 0.29700319989476004,
			1: 0.20616253803697476,
			2: 0.2552206288779828,
			3: 0.24161363319028237,
		}

		// update the random walks
		err = RWM.Update(DB, 3, oldSuccessors, newSuccessors)
		if err != nil {
			t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
		}

		got, err := pagerank.Pagerank(RWM)
		if err != nil {
			t.Errorf("dynamic Pagerank: expected nil, got %v", err)
		}

		if distance(pr, got) > expectedDistance {
			t.Errorf("dynamic Pagerank: expected distance %v, got %v\n", expectedDistance, distance(pr, got))
			t.Errorf("expected %v \ngot %v", pr, got)
		}

	})

	t.Run("dynamic Pagerank, test graph 2 (acyclic)", func(t *testing.T) {

		oldSuccessors := []uint32{3, 4}

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: oldSuccessors}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{3}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{1}}
		DB.Nodes[4] = &graph.Node{ID: 4, SuccessorIDs: []uint32{}}

		// generate walks
		RWM, _ := walks.NewRWM(alpha, walkPerNode)
		err := RWM.GenerateAll(DB)
		if err != nil {
			t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
		}

		// graph update
		newSuccessors := []uint32{1, 2}
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: newSuccessors}

		// the expected pagerank
		pr := pagerank.PagerankMap{
			0: 0.11185368285521291,
			1: 0.36950360789646736,
			2: 0.15943176539450626,
			3: 0.24735726099860061,
			4: 0.11185368285521291,
		}

		err = RWM.Update(DB, 0, oldSuccessors, newSuccessors)
		if err != nil {
			t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
		}

		got, err := pagerank.Pagerank(RWM)
		if err != nil {
			t.Errorf("dynamic Pagerank: expected nil, got %v", err)
		}

		if distance(pr, got) > expectedDistance {
			t.Errorf("dynamic Pagerank: expected distance %v, got %v\n", expectedDistance, distance(pr, got))
			t.Errorf("expected %v \ngot %v", pr, got)
		}

	})
}
