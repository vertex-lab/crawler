package pagerank

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestPagerank(t *testing.T) {
	testCases := []struct {
		name             string
		RWSType          string
		expectedPagerank PagerankMap
		expectedError    error
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "empty RWS",
			RWSType:       "empty",
			expectedError: models.ErrEmptyRWS,
		},
		{
			name:          "just one node",
			RWSType:       "one-node0",
			expectedError: nil,
			expectedPagerank: PagerankMap{
				0: 1.0,
			},
		},
		{
			name:          "simple RWS",
			RWSType:       "simple",
			expectedError: nil,
			expectedPagerank: PagerankMap{
				0: 0.5,
				1: 0.5,
				2: 0.0,
			},
		},
		{
			name:          "triangle RWS",
			RWSType:       "triangle",
			expectedError: nil,
			expectedPagerank: PagerankMap{
				0: 1.0 / 3.0,
				1: 1.0 / 3.0,
				2: 1.0 / 3.0,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RWS := mock.SetupRWS(test.RWSType)
			pagerank, err := Pagerank(RWS)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("Pagerank(): expected %v, got %v", test.expectedError, err)
			}

			// if provided, check the pagerank is equal to the expected
			if test.expectedPagerank != nil {
				if Distance(pagerank, test.expectedPagerank) > 1e-10 {
					t.Errorf("Pagerank(): expected %v, got %v", test.expectedPagerank, pagerank)
				}
			}
		})
	}
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
				RWM, _ := walks.NewRWM("mock", 0.85, walksPerNode)
				RWM.GenerateAll(DB)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {

					_, err := Pagerank(RWM.Store)
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
				RWM, _ := walks.NewRWM("mock", 0.85, 10)
				RWM.GenerateAll(DB)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {

					_, err := Pagerank(RWM.Store)
					if err != nil {
						b.Fatalf("Benchmark failed: %v", err)
					}
				}
			})
		}
	})
}
