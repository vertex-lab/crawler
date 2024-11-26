package pagerank

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/walks"
)

func TestPagerank(t *testing.T) {
	testCases := []struct {
		name             string
		DBType           string
		RWSType          string
		expectedPagerank models.PagerankMap
		expectedError    error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			RWSType:       "one-node0",
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			RWSType:       "one-node0",
			expectedError: models.ErrEmptyDB,
		},
		{
			name:          "nil RWS",
			DBType:        "one-node0",
			RWSType:       "nil",
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "empty RWS",
			DBType:        "one-node0",
			RWSType:       "empty",
			expectedError: models.ErrEmptyRWS,
		},
		{
			name:          "just one node",
			DBType:        "one-node0",
			RWSType:       "one-node0",
			expectedError: nil,
			expectedPagerank: models.PagerankMap{
				0: 1.0,
			},
		},
		{
			name:          "simple RWS",
			DBType:        "simple",
			RWSType:       "simple",
			expectedError: nil,
			expectedPagerank: models.PagerankMap{
				0: 0.5,
				1: 0.5,
				2: 0.0,
			},
		},
		{
			name:          "triangle RWS",
			DBType:        "triangle",
			RWSType:       "triangle",
			expectedError: nil,
			expectedPagerank: models.PagerankMap{
				0: 1.0 / 3.0,
				1: 1.0 / 3.0,
				2: 1.0 / 3.0,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := mockdb.SetupDB(test.DBType)
			RWS := mockstore.SetupRWS(test.RWSType)
			pagerank, err := Pagerank(DB, RWS)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("Pagerank(): expected %v, got %v", test.expectedError, err)
			}

			// if provided, check the pagerank is equal to the expected
			if test.expectedPagerank != nil {
				if models.Distance(pagerank, test.expectedPagerank) > 1e-10 {
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
		DB := mockdb.GenerateDB(nodesSize, edgesPerNode, rng)

		// Different walksPerNode
		for _, walksPerNode := range []uint16{1, 10, 100} {
			b.Run(fmt.Sprintf("walksPerNode=%d", walksPerNode), func(b *testing.B) {
				RWM, _ := walks.NewRWM("mock", 0.85, walksPerNode)
				RWM.GenerateAll(DB)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {

					_, err := Pagerank(DB, RWM.Store)
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
		for _, nodesSize := range []int{100, 1000, 10000} {
			b.Run(fmt.Sprintf("DBSize=%d", nodesSize), func(b *testing.B) {

				DB := mockdb.GenerateDB(nodesSize, edgesPerNode, rng)
				RWM, _ := walks.NewRWM("mock", 0.85, 10)
				RWM.GenerateAll(DB)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {

					_, err := Pagerank(DB, RWM.Store)
					if err != nil {
						b.Fatalf("Benchmark failed: %v", err)
					}
				}
			})
		}
	})
}
