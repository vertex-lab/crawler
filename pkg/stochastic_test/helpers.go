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
	probability float64) (uint32, []uint32, []uint32) {

	// select one node to change
	nodeID := uint32(rand.Intn(len(DB.Nodes)))

	currentSuccessors := DB.Nodes[nodeID].SuccessorIDs
	randomOldSuccessor := []uint32{}

	for node := range DB.Nodes {

		if node == nodeID {
			continue
		}

		// randomly select which successor to add
		if rand.Float64() < probability {
			randomOldSuccessor = append(randomOldSuccessor, node)
		}
	}

	return nodeID, randomOldSuccessor, currentSuccessors
}

// TestSetup holds the mock database and expected Pagerank values.
type TestSetup struct {
	DB         *mock.MockDatabase
	ExpectedPR pagerank.PagerankMap
}

// SetupGraph prepares the database and expected Pagerank values based on the graph type.
func SetupGraph(graphType string) TestSetup {
	DB := mock.NewMockDatabase()
	var expectedPR pagerank.PagerankMap

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
	default: // just one node
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}
		expectedPR = pagerank.PagerankMap{
			0: 1.0,
		}
	}

	return TestSetup{
		DB:         DB,
		ExpectedPR: expectedPR,
	}
}
