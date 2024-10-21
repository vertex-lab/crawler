package pagerank

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestPagerankDummy(t *testing.T) {

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

	t.Run("positive Pagerank, all dandling nodes", func(t *testing.T) {

		size := uint32(5)
		RWM, _ := walks.NewRWM(0.85, 1)

		// all dandling nodes
		for i := uint32(0); i < size; i++ {
			rWalk := &walks.RandomWalk{NodeIDs: []uint32{i}}
			RWM.WalksByNode[i] = mapset.NewSet[*walks.RandomWalk](rWalk)
		}

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

	t.Run("positive Pagerank, triangle graph", func(t *testing.T) {

		RWM, _ := walks.NewRWM(0.85, 1)

		triangleWalks := map[uint32][]uint32{

			0: {0, 1, 2},
			1: {1, 2, 0},
			2: {2, 0, 1},
		}

		// adding triangle walks
		for i := uint32(0); i < 3; i++ {

			rWalk := &walks.RandomWalk{NodeIDs: triangleWalks[i]}
			RWM.WalksByNode[i] = mapset.NewSet[*walks.RandomWalk](rWalk)
		}

		// the expected pagerank
		pr := float64(1) / float64(3)

		got, err := Pagerank(RWM)
		if err != nil {
			t.Errorf("Pagerank(): expected nil, got %v", err)
		}

		for i := uint32(0); i < 3; i++ {

			if got[i] != pr {
				t.Errorf("Pagerank(): expected %v, got %v", pr, got[i])
			}
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
