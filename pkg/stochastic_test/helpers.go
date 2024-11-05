package stochastictest

import (
	"math"
	"math/rand"

	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	"github.com/pippellia-btc/Nostrcrawler/pkg/pagerank"
)

/*
randomly selects one of the potentialChanges, and returns:

- nodeID: the node whose successors got changed

- oldSucc: the old state of the successors of nodeID

- currentSucc: the current state of the successors of nodeID
*/
func SetupOldState(DB *mock.MockGraphDB,
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

	currentSucc := DB.Nodes[randomNodeID].Successors
	changes := potentialChanges[randomNodeID]

	// randomly select one of the potential changes as the old successors
	randomIndex := rand.Intn(len(changes))
	oldSucc := changes[randomIndex].OldSuccessors

	return randomNodeID, oldSucc, currentSucc
}

// TestSetup now includes potential changes that you can randomly select from.
type TestSetup struct {
	DB               *mock.MockGraphDB
	ExpectedPR       pagerank.PagerankMap
	ExpectedPPR0     pagerank.PagerankMap
	PotentialChanges map[uint32][]Change
}

// A struct to represent a change in successors, from OldSuccessors to CurrentSuccessors,
// which is present in the DB
type Change struct {
	OldSuccessors []uint32
}

/*
SetupGraph prepares the GraphDB, expected Pagerank, expected Personalized
Pagerank (alpha = 0.85) of 0 and potential changes based on the graph type.

# NOTE

potentialChanges is nil for graphs that contains short cycles. The reason is that
updateRemovedNodes is known to return incorrect results when the probability
of cycles involving nodeID --> removedNode is high. For the same reason,
potentialChanges should not include oldSuccessors that make the corrisponding graph cyclic.
*/
func SetupGraph(graphType string) TestSetup {
	DB := mock.NewMockGraphDB()
	var expectedPR pagerank.PagerankMap
	var expectedPPR0 pagerank.PagerankMap
	potentialChanges := make(map[uint32][]Change)

	switch graphType {
	case "dandlings":
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{}}
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{}}
		DB.Nodes[2] = &models.Node{ID: 2, Successors: []uint32{}}
		DB.Nodes[3] = &models.Node{ID: 3, Successors: []uint32{}}
		DB.Nodes[4] = &models.Node{ID: 4, Successors: []uint32{}}
		expectedPR = pagerank.PagerankMap{
			0: 0.20,
			1: 0.20,
			2: 0.20,
			3: 0.20,
			4: 0.20,
		}
		expectedPPR0 = pagerank.PagerankMap{
			0: 1.0,
			1: 0.0,
			2: 0.0,
			3: 0.0,
			4: 0.0,
		}
		// because of symmetry, these are all the possible changes
		potentialChanges[0] = []Change{
			{OldSuccessors: []uint32{1}},
			{OldSuccessors: []uint32{1, 2}},
			{OldSuccessors: []uint32{1, 2, 3}},
			{OldSuccessors: []uint32{1, 2, 3, 4}},
		}

	case "triangle":
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{1}}
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{2}}
		DB.Nodes[2] = &models.Node{ID: 2, Successors: []uint32{0}}
		expectedPR = pagerank.PagerankMap{
			0: 0.3333,
			1: 0.3333,
			2: 0.3333,
		}

	case "cyclic1":
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{1, 3}}
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{2}}
		DB.Nodes[2] = &models.Node{ID: 2, Successors: []uint32{0}}
		DB.Nodes[3] = &models.Node{ID: 3, Successors: []uint32{}}
		expectedPR = pagerank.PagerankMap{
			0: 0.29700319989476004,
			1: 0.20616253803697476,
			2: 0.2552206288779828,
			3: 0.24161363319028237,
		}

	case "acyclic1":
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{1, 2}}
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{}}
		DB.Nodes[2] = &models.Node{ID: 2, Successors: []uint32{3}}
		DB.Nodes[3] = &models.Node{ID: 3, Successors: []uint32{1}}
		DB.Nodes[4] = &models.Node{ID: 4, Successors: []uint32{}}
		expectedPR = pagerank.PagerankMap{
			0: 0.11185368285521291,
			1: 0.36950360789646736,
			2: 0.15943176539450626,
			3: 0.24735726099860061,
			4: 0.11185368285521291,
		}
		expectedPPR0 = pagerank.PagerankMap{
			0: 0.39709199748768864,
			1: 0.2906949630265446,
			2: 0.16876345947470478,
			3: 0.14344958001106195,
			4: 0.0,
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
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{1, 2}}
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{}}
		DB.Nodes[2] = &models.Node{ID: 2, Successors: []uint32{}}
		DB.Nodes[3] = &models.Node{ID: 3, Successors: []uint32{}}
		DB.Nodes[4] = &models.Node{ID: 4, Successors: []uint32{3, 5}}
		DB.Nodes[5] = &models.Node{ID: 5, Successors: []uint32{}}
		expectedPR = pagerank.PagerankMap{
			0: 0.12987025255292317,
			1: 0.18506487372353833,
			2: 0.18506487372353833,
			4: 0.12987025255292317,
			3: 0.18506487372353833,
			5: 0.18506487372353833,
		}
		expectedPPR0 = pagerank.PagerankMap{
			0: 0.5405393205897051,
			1: 0.22973033970514745,
			2: 0.22973033970514745,
			4: 0.0,
			3: 0.0,
			5: 0.0,
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
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{1, 2}}
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{}}
		DB.Nodes[2] = &models.Node{ID: 2, Successors: []uint32{}}
		DB.Nodes[3] = &models.Node{ID: 3, Successors: []uint32{1, 2}}
		expectedPR = pagerank.PagerankMap{
			0: 0.17543839772251532,
			1: 0.32456160227748454,
			2: 0.32456160227748454,
			3: 0.17543839772251532,
		}
		expectedPPR0 = pagerank.PagerankMap{
			0: 0.5405396591260619,
			1: 0.22973017043696903,
			2: 0.22973017043696903,
			3: 0.0,
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
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{1, 2}}
		DB.Nodes[1] = &models.Node{ID: 1, Successors: []uint32{}}
		DB.Nodes[2] = &models.Node{ID: 2, Successors: []uint32{}}
		DB.Nodes[3] = &models.Node{ID: 3, Successors: []uint32{1}}
		expectedPR = pagerank.PagerankMap{
			0: 0.17543839772251535,
			1: 0.3991232045549693,
			2: 0.25,
			3: 0.17543839772251535,
		}
		expectedPPR0 = pagerank.PagerankMap{
			0: 0.5405396591260619,
			1: 0.22973017043696903,
			2: 0.22973017043696903,
			3: 0.0,
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

	case "cyclicLong50":
		// it implements the simple cyclic graph with 50 nodes.
		// 0 --> 1 --> 2 --> ... --> 48 --> 49 --> 0
		expectedPR = make(pagerank.PagerankMap, 50)
		expectedPPR0 = make(pagerank.PagerankMap, 50)
		for nodeID := uint32(0); nodeID < 49; nodeID++ {
			DB.Nodes[nodeID] = &models.Node{ID: nodeID, Successors: []uint32{nodeID + 1}}

			expectedPR[nodeID] = 1.0 / 50.0
			expectedPPR0[nodeID] = 0.15 * math.Pow(0.85, float64(nodeID))
		}

		// closing the big cycle
		DB.Nodes[49] = &models.Node{ID: 49, Successors: []uint32{0}}
		expectedPR[49] = 1.0 / 50.0
		expectedPPR0[49] = 0.15 * math.Pow(0.85, float64(49))

		// because of symmetry, these are all the possible changes
		// that produce cycles non shorter than 25
		potentialChanges[0] = []Change{
			// simple additions
			{OldSuccessors: []uint32{}},

			// simple removals
			{OldSuccessors: []uint32{1, 25}},

			// addition and removals
			{OldSuccessors: []uint32{25}},
		}

	default:
		// just one node
		DB.Nodes[0] = &models.Node{ID: 0, Successors: []uint32{}}
		expectedPR = pagerank.PagerankMap{0: 1.0}
		expectedPPR0 = pagerank.PagerankMap{0: 1.0}
	}

	return TestSetup{
		DB:               DB,
		ExpectedPR:       expectedPR,
		ExpectedPPR0:     expectedPPR0,
		PotentialChanges: potentialChanges,
	}
}
