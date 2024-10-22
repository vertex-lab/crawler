package stochastictest

import (
	"math"
	"math/rand"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/pagerank"
)

// computes the L1 distance between two maps who are supposed to have the same keys
func distance(map1, map2 pagerank.PagerankMap) float64 {

	distance := 0.0

	for key, val1 := range map1 {
		distance += math.Abs(val1 - map2[key])
	}

	return distance
}

func SetupOldState(DB *mock.MockDatabase,
	potentialChanges map[uint32][]Change) (uint32, []uint32, []uint32) {

	numChanges := len(potentialChanges)

	// randomly select a key in potentialChanges (which is a nodeID)
	randomPos := rand.Intn(numChanges)
	randomNodeID := uint32(0)
	pos := 0
	for nodeID := range potentialChanges {

		if randomPos == pos {
			randomNodeID = nodeID
			break
		}
		pos++
	}

	currentSucc := DB.Nodes[randomNodeID].SuccessorIDs
	changes := potentialChanges[randomNodeID]

	// randomly select one of the potential changes as the old successors
	randomIndex := rand.Intn(len(changes))
	oldSucc := changes[randomIndex].OldSuccessors

	return randomNodeID, oldSucc, currentSucc
}

// TestSetup now includes potential changes that you can randomly select from.
type TestSetup struct {
	DB               *mock.MockDatabase
	ExpectedPR       pagerank.PagerankMap
	PotentialChanges map[uint32][]Change
}

// A struct to represent a change in successors, from OldSuccessors to CurrentSuccessors,
// which is present in the DB
type Change struct {
	OldSuccessors []uint32
}

/*
SetupGraph prepares the database, expected Pagerank and potential changes
based on the graph type.

# NOTE

potentialChanges is nil for graphs that contains short cycles. The reason is that
updateRemovedNodes is known to return incorrect results when the probability
of cycles involving nodeID --> removedNode is high. For the same reason,
potentialChanges should not include oldSuccessors that make the corrisponding graph cyclic.
*/
func SetupGraph(graphType string) TestSetup {
	DB := mock.NewMockDatabase()
	var expectedPR pagerank.PagerankMap
	potentialChanges := make(map[uint32][]Change)

	switch graphType {
	case "dandlings":
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{}}
		DB.Nodes[4] = &graph.Node{ID: 4, SuccessorIDs: []uint32{}}
		expectedPR = pagerank.PagerankMap{
			0: 0.20,
			1: 0.20,
			2: 0.20,
			3: 0.20,
			4: 0.20,
		}

		// because of symmetry, these are all the possible changes
		potentialChanges[0] = []Change{
			{OldSuccessors: []uint32{1}},
			{OldSuccessors: []uint32{1, 2}},
			{OldSuccessors: []uint32{1, 2, 3}},
			{OldSuccessors: []uint32{1, 2, 3, 4}},
		}

	case "triangle":
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}
		expectedPR = pagerank.PagerankMap{
			0: 0.3333,
			1: 0.3333,
			2: 0.3333,
		}
	case "cyclic1":
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 3}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{}}
		expectedPR = pagerank.PagerankMap{
			0: 0.29700319989476004,
			1: 0.20616253803697476,
			2: 0.2552206288779828,
			3: 0.24161363319028237,
		}
	case "acyclic1":
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{3}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{1}}
		DB.Nodes[4] = &graph.Node{ID: 4, SuccessorIDs: []uint32{}}
		expectedPR = pagerank.PagerankMap{
			0: 0.11185368285521291,
			1: 0.36950360789646736,
			2: 0.15943176539450626,
			3: 0.24735726099860061,
			4: 0.11185368285521291,
		}

		potentialChanges[0] = []Change{
			// simple additions
			{OldSuccessors: []uint32{}},
			{OldSuccessors: []uint32{1}},
			{OldSuccessors: []uint32{2}},

			// simple removals
			{OldSuccessors: []uint32{1, 2, 4}},
			{OldSuccessors: []uint32{1, 2, 3}},

			// addition and removals
			{OldSuccessors: []uint32{1, 3}},
			{OldSuccessors: []uint32{2, 3}},
			{OldSuccessors: []uint32{4}},
		}

		potentialChanges[4] = []Change{
			// simple removals
			{OldSuccessors: []uint32{0}},
			{OldSuccessors: []uint32{1}},
			{OldSuccessors: []uint32{2}},
			{OldSuccessors: []uint32{3}},
			{OldSuccessors: []uint32{0, 1}},
		}
	case "acyclic2":
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{}}
		DB.Nodes[4] = &graph.Node{ID: 4, SuccessorIDs: []uint32{3, 5}}
		DB.Nodes[5] = &graph.Node{ID: 5, SuccessorIDs: []uint32{}}
		expectedPR = pagerank.PagerankMap{
			0: 0.12987025255292317,
			1: 0.18506487372353833,
			2: 0.18506487372353833,
			4: 0.12987025255292317,
			3: 0.18506487372353833,
			5: 0.18506487372353833,
		}

		potentialChanges[0] = []Change{
			// simple additions
			{OldSuccessors: []uint32{}},
			{OldSuccessors: []uint32{1}},

			// simple removals
			{OldSuccessors: []uint32{1, 2, 3}},
			{OldSuccessors: []uint32{1, 2, 4}},

			// addition and removals
			{OldSuccessors: []uint32{1, 3}},
			{OldSuccessors: []uint32{1, 4}},
			{OldSuccessors: []uint32{3}},
			{OldSuccessors: []uint32{4}},
		}

		potentialChanges[1] = []Change{
			// simple removals
			{OldSuccessors: []uint32{2}},
			{OldSuccessors: []uint32{3}},
			{OldSuccessors: []uint32{4}},
		}

	case "acyclic3":
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{1, 2}}
		expectedPR = pagerank.PagerankMap{
			0: 0.17543839772251532,
			1: 0.32456160227748454,
			2: 0.32456160227748454,
			3: 0.17543839772251532,
		}

		potentialChanges[0] = []Change{
			// simple additions
			{OldSuccessors: []uint32{}},
			{OldSuccessors: []uint32{1}},

			// simple removals
			{OldSuccessors: []uint32{1, 2, 3}},

			// addition and removals
			{OldSuccessors: []uint32{1, 3}},
		}

	case "acyclic4":
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{}}
		DB.Nodes[3] = &graph.Node{ID: 3, SuccessorIDs: []uint32{1}}
		expectedPR = pagerank.PagerankMap{
			0: 0.17543839772251535,
			1: 0.3991232045549693,
			2: 0.25,
			3: 0.17543839772251535,
		}

		potentialChanges[0] = []Change{
			// simple additions
			{OldSuccessors: []uint32{}},
			{OldSuccessors: []uint32{1}},
			{OldSuccessors: []uint32{2}},

			// simple removals
			{OldSuccessors: []uint32{1, 2, 3}},

			// addition and removals
			{OldSuccessors: []uint32{1, 3}},
			{OldSuccessors: []uint32{2, 3}},
		}

	case "cyclicLong30":
		// it implements the simple cyclic graph with 30 nodes.
		// 0 --> 1 --> 2 --> ... --> 28 --> 29 --> 0

		expectedPR = make(pagerank.PagerankMap, 30)

		for nodeID := uint32(0); nodeID < 29; nodeID++ {
			DB.Nodes[nodeID] = &graph.Node{ID: nodeID, SuccessorIDs: []uint32{nodeID + 1}}

			expectedPR[nodeID] = 1.0 / 30.0
		}

		// closing the big cycle
		DB.Nodes[29] = &graph.Node{ID: 29, SuccessorIDs: []uint32{0}}
		expectedPR[29] = 1.0 / 30.0

		// because of symmetry, these are all the possible changes
		// that produce cycles longer than 15
		potentialChanges[0] = []Change{
			// simple additions
			{OldSuccessors: []uint32{}},

			// simple removals
			{OldSuccessors: []uint32{1, 15}},

			// addition and removals
			{OldSuccessors: []uint32{15}},
		}

	default:
		// just one node
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}
		expectedPR = pagerank.PagerankMap{
			0: 1.0,
		}
	}

	return TestSetup{
		DB:               DB,
		ExpectedPR:       expectedPR,
		PotentialChanges: potentialChanges,
	}
}
