package pagerank

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestPagerankDeterministic(t *testing.T) {

	t.Run("negative Pagerank, nil RWM", func(t *testing.T) {

		var RWM *walks.RandomWalksManager
		_, err := Pagerank(RWM)

		if !errors.Is(err, walks.ErrNilRWMPointer) {
			t.Errorf("Pagerank(): expected %v, got %v", walks.ErrNilRWMPointer, err)
		}
	})

	t.Run("negative Pagerank, empty RWM", func(t *testing.T) {

		RWM, _ := walks.NewRWM(0.85, 1)
		_, err := Pagerank(RWM)

		if !errors.Is(err, walks.ErrEmptyRWM) {
			t.Errorf("Pagerank(): expected %v, got %v", walks.ErrEmptyRWM, err)
		}
	})

	t.Run("static Pagerank, all dandling nodes", func(t *testing.T) {

		size := uint32(5)
		DB := mock.NewMockDatabase()

		// all dandling node DB
		for i := uint32(0); i < size; i++ {
			DB.Nodes[i] = &graph.Node{ID: i, SuccessorIDs: []uint32{}}
		}

		// generate walks
		RWM, _ := walks.NewRWM(0.85, 1)
		RWM.GenerateAll(DB)

		// the expected pagerank
		pr := float64(1) / float64(size)

		got, err := Pagerank(RWM)
		if err != nil {
			t.Errorf("Pagerank(): expected nil, got %v", err)
		}

		for i := uint32(0); i < size; i++ {

			if got[i] != pr {
				t.Errorf("Pagerank(): expected %v, got %v", pr, got[i])
			}
		}

	})

	t.Run("static Pagerank, 0 <--> 1", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{0}}

		// generate walks
		RWM, _ := walks.NewRWM(0.85, 1000)
		RWM.GenerateAll(DB)

		// the expected pagerank
		pr := 0.5

		got, err := Pagerank(RWM)
		if err != nil {
			t.Errorf("Pagerank(): expected nil, got %v", err)
		}

		for nodeID := uint32(0); nodeID < 2; nodeID++ {

			distance := math.Abs(got[nodeID] - pr)
			if distance > 0.01 {
				t.Errorf("Pagerank(): expected distance %v, got %v\n", 0.01, distance)
				t.Fatalf("expected %v \ngot %v", pr, got)
			}
		}

	})

}

func TestPagerankProbabilistic(t *testing.T) {

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
		pr := PagerankMap{
			0: 0.29700319989476004,
			1: 0.20616253803697476,
			2: 0.2552206288779828,
			3: 0.24161363319028237,
		}

		got, err := Pagerank(RWM)
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
		pr := PagerankMap{
			0: 0.11185368285521291,
			1: 0.36950360789646736,
			2: 0.15943176539450626,
			3: 0.24735726099860061,
			4: 0.11185368285521291,
		}

		got, err := Pagerank(RWM)
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

// ---------------------------------BENCHMARK----------------------------------

func BenchmarkPagerank(b *testing.B) {

	b.Run("FixedDB", func(b *testing.B) {

		// initial setup
		nodesSize := 2000
		edgesPerNode := 100
		rng := rand.New(rand.NewSource(69))
		DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)

		// Different walksPerNode
		for _, walksPerNode := range []uint16{1, 10, 100} {

			b.Run(fmt.Sprintf("walksPerNode=%d", walksPerNode), func(b *testing.B) {

				RWM, _ := walks.NewRWM(0.85, walksPerNode)
				RWM.GenerateAll(DB)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {

					_, err := Pagerank(RWM)
					if err != nil {
						b.Fatalf("Benchmark failed: %v", err)
					}
				}
			})
		}
	})

	b.Run("FixedWalksPerNode", func(b *testing.B) {

		edgesPerNode := 100
		rng := rand.New(rand.NewSource(69))

		// Different DB sizes
		for _, nodesSize := range []int{1000, 2000, 5000} {

			b.Run(fmt.Sprintf("DBSize=%d", nodesSize), func(b *testing.B) {

				DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)
				RWM, _ := walks.NewRWM(0.85, 10)
				RWM.GenerateAll(DB)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {

					_, err := Pagerank(RWM)
					if err != nil {
						b.Fatalf("Benchmark failed: %v", err)
					}
				}

			})
		}
	})
}

// ---------------------------------HELPER-FUNC---------------------------------

// computes the L1 distance between two maps who are supposed to have the same keys
func distance(map1, map2 PagerankMap) float64 {

	distance := 0.0

	for key, val1 := range map1 {
		distance += math.Abs(val1 - map2[key])
	}

	return distance
}
