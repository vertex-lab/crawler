package stochastictest

import (
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/pagerank"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestPagerankStatic(t *testing.T) {

	const expectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 4000

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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// setup the graph and pagerank
			setup := SetupGraph(test.graphType)
			DB := setup.DB
			expectedPR := setup.ExpectedPR

			// generate walks
			RWM, _ := walks.NewRWM(alpha, walkPerNode)
			err := RWM.GenerateAll(DB)
			if err != nil {
				t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
			}

			// compute pagerank
			got, err := pagerank.Pagerank(RWM)
			if err != nil {
				t.Errorf("Pagerank(): expected nil, got %v", err)
			}

			// compute the error
			if distance(expectedPR, got) > expectedDistance {
				t.Errorf("Pagerank(): expected distance %v, got %v\n", expectedDistance, distance(expectedPR, got))
				t.Errorf("expected %v \ngot %v", expectedPR, got)
			}
		})
	}
}

/*
It's a known phenomena that updateRemovedNodes doesn't yield correct results
when the probability of cycles that involve nodeID --> removedNode is high.

Therefore test with only acyclic graphs, or graphs large enough that the probability of
cycles is very low.
*/
func TestPagerankDynamic(t *testing.T) {

	const expectedDistance = 0.01
	const alpha = 0.85
	const walkPerNode = 4000

	tests := []struct {
		name      string
		graphType string
	}{
		// {
		// 	name:      "dynamic Pagerank, all dandling nodes",
		// 	graphType: "dandlings",
		// },
		// {
		// 	name:      "dynamic Pagerank, triangle graph",
		// 	graphType: "triangle",
		// },
		// {
		// 	name:      "dynamic Pagerank, cyclic graph 1",
		// 	graphType: "cyclic1",
		// },
		{
			name:      "dynamic Pagerank, acyclic graph 1",
			graphType: "acyclic1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// setup the graph and pagerank
			setup := SetupGraph(test.graphType)
			DB := setup.DB
			expectedPR := setup.ExpectedPR

			// prepare the precedent state
			nodeID := uint32(2)
			oldSuccessors := []uint32{3, 1}
			currentSuccessors := []uint32{3}

			DB.Nodes[nodeID].SuccessorIDs = oldSuccessors

			// generate walks
			RWM, _ := walks.NewRWM(alpha, walkPerNode)
			err := RWM.GenerateAll(DB)
			if err != nil {
				t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
			}

			// compute pagerank
			got, err := pagerank.Pagerank(RWM)
			if err != nil {
				t.Errorf("Pagerank(): expected nil, got %v", err)
			}

			t.Errorf("\npagerank old: %v", got)

			// // save here the walks that will be updated
			// oldWalks := [][]uint32{}
			// updatedWalks := []*walks.RandomWalk{}

			// // print walks that needs to be updated
			// walkSet, _ := RWM.WalksByNodeID(nodeID)
			// for rWalk := range walkSet.Iter() {

			// 	updates, _, err := rWalk.NeedsUpdate(nodeID, []uint32{0})
			// 	if err != nil {
			// 		t.Fatalf("expected nil, got %v", err)
			// 	}

			// 	if updates {
			// 		oldWalks = append(oldWalks, rWalk.NodeIDs)
			// 		updatedWalks = append(updatedWalks, rWalk)
			// 	}
			// }

			// graph update
			DB.Nodes[nodeID] = &graph.Node{ID: nodeID, SuccessorIDs: currentSuccessors}

			// update the random walks
			err = RWM.Update(DB, nodeID, oldSuccessors, currentSuccessors)
			if err != nil {
				t.Fatalf("dynamic Pagerank: expected nil, got %v", err)
			}

			// compute pagerank
			got, err = pagerank.Pagerank(RWM)
			if err != nil {
				t.Errorf("Pagerank(): expected nil, got %v", err)
			}

			t.Errorf("\npagerank new: %v", got)
			t.Errorf("\npagerank expected: %v", expectedPR)

			// compute the error
			// if distance(expectedPR, got) > expectedDistance {
			t.Errorf("Pagerank(): expected distance %v, got %v\n\n", expectedDistance, distance(expectedPR, got))
			t.Errorf("expected %v; got %v\n\n", expectedPR, got)

			t.Errorf("nodeID: %v", nodeID)
			t.Errorf("oldSucc: %v", oldSuccessors)
			t.Errorf("currentSucc: %v", currentSuccessors)

			// for i, rWalk := range updatedWalks {
			// 	t.Errorf("(old) %v --> %v (new)\n", oldWalks[i], rWalk.NodeIDs)
			// }
			// }
		})
	}
}

/*
func TestTri(t *testing.T) {

	// setup the graph and pagerank
	setup := SetupGraph("acyclic1")
	DB := setup.DB
	expectedPR := setup.ExpectedPR
	_ = expectedPR

	// setup the old state
	nodeID := uint32(2)
	oldSucc := []uint32{0, 1}
	currentSucc := []uint32{0}
	removed := []uint32{1}
	DB.Nodes[nodeID].SuccessorIDs = oldSucc

	// generate walks
	RWM, _ := walks.NewRWM(0.85, 10000)
	err := RWM.GenerateAll(DB)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	// compute pagerank
	pr, err := pagerank.Pagerank(RWM)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	t.Errorf("pagerank old: %v\n\n", pr)

	// save here the walks that will be updated
	updatedWalks := []*walks.RandomWalk{}

	// add walks that needs to be updated
	walkSet, _ := RWM.WalksByNodeID(nodeID)
	for rWalk := range walkSet.Iter() {

		updates, _, err := rWalk.NeedsUpdate(nodeID, removed)
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}

		if updates {
			updatedWalks = append(updatedWalks, rWalk)
		}
	}

	// for nodeID, walkSet := range RWM.WalksByNode {

	// 	walks, _ := walks.SortWalks(walkSet)
	// 	t.Errorf("nodeID: %v, walks: %v\n\n", nodeID, walks)
	// }

	// update the DB
	DB.Nodes[nodeID].SuccessorIDs = currentSucc

	// update the random walks
	err = RWM.Update(DB, nodeID, oldSucc, currentSucc)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	// compute pagerank
	pr, err = pagerank.Pagerank(RWM)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	t.Errorf("pagerank new: %v\n\n", pr)

	two_counter := 0
	two_zero_counter := 0
	two_zero_one_counter := 0

	for _, rWalk := range updatedWalks {

		switch len(rWalk.NodeIDs) {
		case 1:
			two_counter++
		case 2:
			two_zero_counter++
		case 3:
			two_zero_one_counter++
		}
	}

	t.Errorf("A total of %v out of %v walks have been updated", len(updatedWalks), RWM.WalksByNode[nodeID].Cardinality())
	t.Errorf("[2] : %v ", two_counter)
	t.Errorf("[2,0] : %v ", two_zero_counter)
	t.Errorf("[2,0,1] : %v ", two_zero_one_counter)

	// t.Errorf("A total of %v out of %v walks have been updated", len(updatedWalks), RWM.WalksByNode[nodeID].Cardinality())
	// t.Errorf("[2] : %v ", float64(two_counter)/float64(len(updatedWalks)))
	// t.Errorf("[2,0] : %v ", float64(two_zero_counter)/float64(len(updatedWalks)))
	// t.Errorf("[2,0,1] : %v ", float64(two_zero_one_counter)/float64(len(updatedWalks)))

	// for nodeID, walkSet := range RWM.WalksByNode {

	// 	walks, _ := walks.SortWalks(walkSet)
	// 	t.Errorf("nodeID: %v, walks: %v\n\n", nodeID, walks)
	// }

	// // see the changes in the walks
	// for _, rWalk := range updatedWalks {
	// 	t.Errorf("walk: %v", rWalk.NodeIDs)
	// }
}

func TestPagerankDynamicOLD(t *testing.T) {

	const expectedDistance = 0.04
	const alpha = 0.85
	const walkPerNode = 100

	t.Run("dynamic Pagerank, triangle graph", func(t *testing.T) {

		nodeID := uint32(2)
		oldSuccessors := []uint32{0, 1}

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
		DB.Nodes[nodeID] = &graph.Node{ID: nodeID, SuccessorIDs: newSuccessors}

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
*/
