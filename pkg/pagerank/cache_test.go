package pagerank

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

func TestFollows(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name            string
			DBType          string
			FCType          string
			expectedFollows []uint32
			expectedError   error
		}{
			{
				name:            "nil FC",
				DBType:          "one-node0",
				FCType:          "nil",
				expectedFollows: []uint32{},
				expectedError:   ErrNilFCPointer,
			},
			{
				name:            "nil DB",
				DBType:          "nil",
				FCType:          "empty",
				expectedFollows: []uint32{},
				expectedError:   models.ErrNilDBPointer,
			},
			{
				name:            "node not found FC and DB",
				DBType:          "one-node0",
				FCType:          "empty",
				expectedFollows: []uint32{},
				expectedError:   models.ErrNodeNotFoundDB,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mock.SetupDB(test.DBType)
				FC := SetupFC(DB, test.FCType)

				follows, err := FC.Follows(context.Background(), 1)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("Follows(): expected %v, got %v", test.expectedError, err)
				}

				if !reflect.DeepEqual(follows, test.expectedFollows) {
					t.Fatalf("Follows(): expected %v, got %v", test.expectedFollows, follows)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		DB := mock.SetupDB("triangle")
		FC := SetupFC(DB, "empty")

		follows, err := FC.Follows(context.Background(), 1)
		if err != nil {
			t.Fatalf("Follows(): expected nil, got %v", err)
		}

		expectedFollows := []uint32{2}
		if !reflect.DeepEqual(follows, expectedFollows) {
			t.Fatalf("Follows(): expected follows %v, got %v", expectedFollows, follows)
		}

		follows, exists := FC.follows[1] // check the cache was updated
		if !exists || !reflect.DeepEqual(follows, expectedFollows) {
			t.Fatalf("failed to update the FollowCache: expected follows %v, got %v", expectedFollows, follows)
		}
	})
}

func TestFCLoad(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			DBType        string
			FCType        string
			expectedError error
		}{
			{
				name:          "nil FC",
				DBType:        "one-node0",
				FCType:        "nil",
				expectedError: ErrNilFCPointer,
			},
			{
				name:          "nil DB",
				DBType:        "nil",
				FCType:        "empty",
				expectedError: models.ErrNilDBPointer,
			},
			{
				name:          "one node not found FC and DB",
				DBType:        "one-node0",
				FCType:        "empty",
				expectedError: models.ErrNodeNotFoundDB,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mock.SetupDB(test.DBType)
				FC := SetupFC(DB, test.FCType)
				nodeIDs := []uint32{0, 1}

				err := FC.Load(context.Background(), nodeIDs...)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("Follows(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		DB := mock.SetupDB("triangle")
		FC := SetupFC(DB, "empty")
		nodeIDs := []uint32{0, 1}

		err := FC.Load(context.Background(), nodeIDs...)
		if err != nil {
			t.Fatalf("Follows(): expected nil, got %v", err)
		}

		expectedFollows := [][]uint32{{1}, {2}}
		for i, nodeID := range nodeIDs {
			follows, exists := FC.follows[nodeID] // check the cache was updated
			if !exists || !reflect.DeepEqual(follows, expectedFollows[i]) {
				t.Fatalf("failed to update the FollowCache: expected follows %v, got %v", expectedFollows, follows)
			}
		}
	})
}

func TestNext(t *testing.T) {
	testCases := []struct {
		name            string
		WCType          string
		nodeID          uint32
		expectedWalk    models.RandomWalk
		expectedExists  bool
		expectedWCWalks []models.RandomWalk
		expectedPos     []int
	}{
		{
			name:           "nil WC",
			WCType:         "nil",
			nodeID:         0,
			expectedExists: false,
		},
		{
			name:            "all used WC",
			WCType:          "all-used",
			nodeID:          0,
			expectedExists:  false,
			expectedWCWalks: []models.RandomWalk{nil},
			expectedPos:     []int{},
		},
		{
			name:            "valid",
			WCType:          "triangle",
			nodeID:          0,
			expectedWalk:    models.RandomWalk{0, 1, 2},
			expectedExists:  true,
			expectedWCWalks: []models.RandomWalk{nil, {1, 2, 0}, {2, 0, 1}},
			expectedPos:     []int{1, 2},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			WC := SetupWC(test.WCType)
			walk, exists := WC.Next(test.nodeID)

			if exists != test.expectedExists {
				t.Fatalf("Next(): expected %v, got %v", test.expectedExists, exists)
			}

			if !reflect.DeepEqual(walk, test.expectedWalk) {
				t.Errorf("Next(): expected %v, got %v", test.expectedWalk, walk)
			}

			if WC != nil {
				if !reflect.DeepEqual(WC.walks, test.expectedWCWalks) {
					t.Errorf("walks: expected %v, got %v", test.expectedWCWalks, WC.walks)
				}

				positions := WC.positions[test.nodeID]
				if !reflect.DeepEqual(positions, test.expectedPos) {
					t.Errorf("positions(%d): expected %v, got %v", test.nodeID, test.expectedPos, positions)
				}
			}
		})
	}
}

func TestWCLoad(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			RWSType       string
			nodeIDs       []uint32
			expectedError error
		}{
			{
				name:          "nil RWS",
				RWSType:       "nil",
				nodeIDs:       []uint32{0},
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "empty RWS",
				RWSType:       "empty",
				nodeIDs:       []uint32{0},
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "node not found RWS",
				RWSType:       "triangle",
				nodeIDs:       []uint32{0, 69},
				expectedError: models.ErrNodeNotFoundRWS,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				RWS := mockstore.SetupRWS(test.RWSType)
				WC := NewWalkCache(1)

				err := WC.Load(context.Background(), RWS, test.nodeIDs...)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("Load(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		RWS := mockstore.SetupRWS("complex")
		WC := NewWalkCache(2)
		nodeIDs := []uint32{0, 3}

		err := WC.Load(context.Background(), RWS, nodeIDs...)
		if err != nil {
			t.Fatalf("Load(): expected nil, got %v", err)
		}

		expectedwalks := []models.RandomWalk{{0, 1, 2}, {0, 3}}
		walks := sliceutils.SortWalks(WC.walks)
		if !reflect.DeepEqual(walks, expectedwalks) {
			t.Errorf("Load(): expected %v, got %v", expectedwalks, WC.walks)
		}

		expectedPos := map[uint32][]int{
			0: {0, 1},
			3: {},
		}

		for _, ID := range nodeIDs {
			pos := WC.positions[ID]
			if !reflect.DeepEqual(pos, expectedPos[ID]) {
				t.Errorf("positions(%d): expected %v, got %v", ID, expectedPos[ID], pos)
			}
		}
	})
}

func TestCropWalk(t *testing.T) {
	testCases := []struct {
		name          string
		walk          models.RandomWalk
		nodeID        uint32
		expectedWalk  models.RandomWalk
		expectedError error
	}{
		{
			name:          "empty random walk",
			walk:          models.RandomWalk{},
			nodeID:        0,
			expectedError: ErrNodeNotInWalk,
		},
		{
			name:          "node not in random walk",
			walk:          models.RandomWalk{1, 2, 3, 4},
			nodeID:        0,
			expectedError: ErrNodeNotInWalk,
		},
		{
			name:          "node in random walk",
			walk:          models.RandomWalk{0, 1, 2, 3},
			nodeID:        0,
			expectedError: nil,
			expectedWalk:  models.RandomWalk{1, 2, 3},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			croppedWalk, err := CropWalk(test.walk, test.nodeID)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("CropWalk(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(croppedWalk, test.expectedWalk) {
				t.Errorf("CropWalk(): expected %v, got %v", test.expectedWalk, croppedWalk)
			}
		})
	}
}

// ---------------------------------BENCHMARKS---------------------------------

func BenchmarkWCLoad(b *testing.B) {
	ctx := context.Background()
	nodesNum := 2000
	succPerNode := 100
	rng := rand.New(rand.NewSource(42))

	DB := mock.GenerateDB(nodesNum, succPerNode, rng)
	RWM, err := walks.NewMockRWM(0.85, 100)
	if err != nil {
		b.Fatalf("benchmark failed: %v", err)
	}

	if err := RWM.GenerateAll(ctx, DB); err != nil {
		b.Fatalf("benchmark failed: %v", err)
	}

	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("nodeIDs size = %d", size), func(b *testing.B) {
			nodeIDs := make([]uint32, size)
			for i := 0; i < size; i++ {
				nodeIDs[i] = uint32(i)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				WC := NewWalkCache(1)

				if err := WC.Load(ctx, RWM.Store, nodeIDs...); err != nil {
					b.Errorf("Benchmark Load() failed: %v", err)
				}
			}
		})
	}
}

func BenchmarkWCLoadRedis(b *testing.B) {
	b.Run("fixed number of nodes", func(b *testing.B) {
		nodesNum := 100
		for _, walksNum := range []int{1000, 10000, 100000} {
			b.Run(fmt.Sprintf("walksNum=%d", walksNum), func(b *testing.B) {
				cl := redisutils.SetupClient()
				defer redisutils.CleanupRedis(cl)

				RWS, err := redistore.GenerateRWS(cl, nodesNum, walksNum)
				if err != nil {
					b.Fatalf("GenerateRWS() benchmark failed: %v", err)
				}

				nodeIDs := make([]uint32, 100)
				for i := 0; i < 100; i++ {
					nodeIDs[i] = uint32(i)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					WC := NewWalkCache(1)

					if err := WC.Load(context.Background(), RWS, nodeIDs...); err != nil {
						b.Fatalf("benchmark failed: %v", err)
					}
				}
			})
		}
	})
}

func BenchmarkFCLoad(b *testing.B) {
	ctx := context.Background()
	nodesNum := 2000
	succPerNode := 100
	rng := rand.New(rand.NewSource(42))
	DB := mock.GenerateDB(nodesNum, succPerNode, rng)

	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("nodeIDs size = %d", size), func(b *testing.B) {
			nodeIDs := make([]uint32, size)
			for i := 0; i < size; i++ {
				nodeIDs[i] = uint32(i)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				FC := NewFollowCache(DB, 1)

				if err := FC.Load(ctx, nodeIDs...); err != nil {
					b.Fatalf("Benchmark Load() failed: %v", err)
				}
			}
		})
	}
}
