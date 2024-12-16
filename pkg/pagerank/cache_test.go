package pagerank

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
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
		name              string
		WCType            string
		nodeID            uint32
		expectedWalk      models.RandomWalk
		expectedExists    bool
		expectedLastIndex int
	}{
		{
			name:           "nil WC",
			WCType:         "nil",
			nodeID:         0,
			expectedExists: false,
		},
		{
			name:           "all used WC",
			WCType:         "all-used",
			nodeID:         0,
			expectedExists: false,
		},
		{
			name:              "valid",
			WCType:            "triangle",
			nodeID:            0,
			expectedWalk:      models.RandomWalk{0, 1, 2},
			expectedExists:    true,
			expectedLastIndex: 0,
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
				lastIndex := WC.states[test.nodeID].lastIndex
				if lastIndex != test.expectedLastIndex {
					t.Fatalf("Next(): expected lastIndex %v, got %v", test.expectedLastIndex, lastIndex)
				}

				if WC.walks[lastIndex] != nil {
					t.Fatalf("Next(): expected zeroed walk, got %v (type %T) ", WC.walks[lastIndex], WC.walks[lastIndex])
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

		expectedState0 := &NodeState{
			positions: []int{0, 1},
			lastIndex: 0,
		}
		if !reflect.DeepEqual(WC.states[0], expectedState0) {
			t.Errorf("Load(): expected %v, got %v", expectedState0, WC.states[0])
		}

		expectedState3 := &NodeState{
			positions: []int{},
			lastIndex: 0,
		}
		if !reflect.DeepEqual(WC.states[3], expectedState3) {
			t.Errorf("Load(): expected %v, got %v", expectedState3, WC.states[3])
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

// func BenchmarkNextWalk(b *testing.B) {
// 	ctx := context.Background()
// 	RWS := mockstore.SetupRWS("triangle")
// 	WC := NewWalkCache()
// 	WC.Load(ctx, RWS, 0, 10)

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		if _, err := WC.NextWalk(0); err != nil {
// 			b.Fatalf("Benchmark NextWalk(0) failed: %v", err)
// 		}

// 		// reset the index to avoid the error
// 		if (i+1)%2 == 0 {
// 			WC.NodeWalkIndex[0] = 0
// 		}
// 	}
// }
