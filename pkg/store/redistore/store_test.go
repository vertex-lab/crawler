package redistore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

func TestNewRWS(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name          string
		alphas        []float32
		walksPerNode  uint16
		expectedError error
		// ADD CONTEXT TESTS
	}{
		{
			name:          "invalid alphas",
			alphas:        []float32{1.01, 1.0, -0.1, -2},
			walksPerNode:  1,
			expectedError: models.ErrInvalidAlpha,
		},
		{
			name:          "invalid walksPerNode",
			alphas:        []float32{0.99, 0.11, 0.57, 0.0001},
			walksPerNode:  0,
			expectedError: models.ErrInvalidWalksPerNode,
		},
		{
			name:          "both valid",
			alphas:        []float32{0.99, 0.11, 0.57, 0.0001},
			walksPerNode:  1,
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			// iterate over the alphas
			for _, alpha := range test.alphas {

				RWS, err := NewRWS(context.Background(), cl, alpha, test.walksPerNode)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("NewRWS(): expected %v, got %v", test.expectedError, err)
				}

				// check if the parameters have been added correctly
				if RWS != nil {
					if RWS.Alpha(context.Background()) != alpha {
						t.Errorf("NewRWS(): expected %v, got %v", alpha, RWS.Alpha(context.Background()))
					}

					if RWS.WalksPerNode(context.Background()) != test.walksPerNode {
						t.Errorf("NewRWS(): expected %v, got %v", test.walksPerNode, RWS.WalksPerNode(context.Background()))
					}
				}
			}
		})
	}
}

func TestLoadRWS(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name          string
		RWSType       string
		expectedError error
		// ADD CONTEXT TESTS
	}{
		{
			name:          "RWS not set",
			RWSType:       "nil",
			expectedError: models.ErrEmptyRWS,
		},
		{
			name:          "valid",
			RWSType:       "empty",
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := SetupRWS(cl, test.RWSType); err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			RWS, err := NewRWSConnection(context.Background(), cl)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NewRWSConnection(): expected %v, got %v", test.expectedError, err)
			}

			// check if the parameters have been added correctly
			if RWS != nil {
				if RWS.Alpha(context.Background()) != float32(0.85) {
					t.Errorf("NewRWSConnection(): expected %v, got %v", 0.85, RWS.Alpha(context.Background()))
				}

				if RWS.WalksPerNode(context.Background()) != uint16(1) {
					t.Errorf("NewRWSConnection(): expected %v, got %v", 1, RWS.WalksPerNode(context.Background()))
				}
			}
		})
	}
}

func TestTotalVisits(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name                string
		RWSType             string
		expectedTotalVisits int
	}{
		{
			name:                "nil RWS",
			RWSType:             "nil",
			expectedTotalVisits: 0,
		},
		{
			name:                "empty RWS",
			RWSType:             "empty",
			expectedTotalVisits: 0,
		},
		{
			name:                "non-empty RWS",
			RWSType:             "one-node0",
			expectedTotalVisits: 1,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			visits := RWS.TotalVisits(context.Background())
			if visits != test.expectedTotalVisits {
				t.Errorf("TotalVisits(): expected %v, got %v", test.expectedTotalVisits, visits)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	t.Run("nil RWS", func(t *testing.T) {
		RWS, err := SetupRWS(cl, "nil")
		if err != nil {
			t.Fatalf("SetupRWS(): expected nil, got %v", err)
		}

		err = RWS.Validate()
		if !errors.Is(err, models.ErrNilRWSPointer) {
			t.Errorf("Validate(): expected %v, got %v", models.ErrNilRWSPointer, err)
		}
	})

	t.Run("invalid walksPerNode", func(t *testing.T) {
		RWS, _ := NewRWS(context.Background(), cl, 0.85, 1)
		RWS.walksPerNode = 0

		err := RWS.Validate()
		if !errors.Is(err, models.ErrInvalidWalksPerNode) {
			t.Errorf("Validate(): expected %v, got %v", models.ErrInvalidWalksPerNode, err)
		}
	})

	t.Run("invalid alphas", func(t *testing.T) {
		RWS, _ := NewRWS(context.Background(), cl, 0.85, 1)
		invalidAlphas := []float32{1.1, 0.0, -1.0, -0.11, 55}

		for _, alpha := range invalidAlphas {
			RWS.alpha = alpha

			err := RWS.Validate()
			if !errors.Is(err, models.ErrInvalidAlpha) {
				t.Errorf("Validate(): expected %v, got %v", models.ErrInvalidAlpha, err)
			}
		}
	})
}

func TestVisitCounts(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name           string
		RWSType        string
		nodeIDs        []uint32
		expectedVisits map[uint32]int
		expectedError  error
	}{
		{
			name:           "nil RWS",
			RWSType:        "nil",
			nodeIDs:        []uint32{0},
			expectedVisits: map[uint32]int{},
			expectedError:  models.ErrNilRWSPointer,
		},
		{
			name:           "empty RWS",
			RWSType:        "empty",
			nodeIDs:        []uint32{0},
			expectedVisits: map[uint32]int{0: 0},
			expectedError:  nil,
		},
		{
			name:           "empty nodeIDs",
			RWSType:        "one-node0",
			nodeIDs:        []uint32{},
			expectedVisits: map[uint32]int{},
			expectedError:  nil,
		},
		{
			name:           "one node RWS",
			RWSType:        "one-node0",
			nodeIDs:        []uint32{0},
			expectedVisits: map[uint32]int{0: 1},
			expectedError:  nil,
		},
		{
			name:           "triangle RWS",
			RWSType:        "triangle",
			nodeIDs:        []uint32{0, 1, 2, 99}, // 99 is not in the RWS
			expectedVisits: map[uint32]int{0: 3, 1: 3, 2: 3, 99: 0},
			expectedError:  nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			visits, err := RWS.VisitCounts(context.Background(), test.nodeIDs)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("VisitCounts(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(visits, test.expectedVisits) {
				t.Errorf("VisitCount(0): expected %v, got %v", test.expectedVisits, visits)
			}
		})
	}
}

func TestWalks(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name          string
		RWSType       string
		walkIDs       []uint32
		expectedWalks []models.RandomWalk
		expectedError error
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			walkIDs:       []uint32{0},
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "empty RWS",
			RWSType:       "empty",
			walkIDs:       []uint32{0},
			expectedError: models.ErrWalkNotFound,
		},
		{
			name:          "walkID not found in RWS",
			RWSType:       "one-node0",
			walkIDs:       []uint32{1},
			expectedError: models.ErrWalkNotFound,
		},
		{
			name:          "one walkID",
			RWSType:       "triangle",
			walkIDs:       []uint32{0},
			expectedWalks: []models.RandomWalk{{0, 1, 2}},
		},
		{
			name:          "multiple walkID",
			RWSType:       "triangle",
			walkIDs:       []uint32{0, 2},
			expectedWalks: []models.RandomWalk{{0, 1, 2}, {2, 0, 1}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			walks, err := RWS.Walks(context.Background(), test.walkIDs...)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Walks(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walks, test.expectedWalks) {
				t.Errorf("Walks(): expected %v, got %v", test.expectedWalks, walks)
			}
		})
	}
}

func TestWalksVisitingAny(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name          string
		RWSType       string
		limit         int
		nodeIDs       []uint32
		expectedIDs   []uint32
		expectedError error
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			limit:         1,
			nodeIDs:       []uint32{0},
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "empty RWS",
			RWSType:       "empty",
			limit:         1,
			nodeIDs:       []uint32{0},
			expectedError: models.ErrNodeNotFoundRWS,
		},
		{
			name:          "nodeID not found in RWS",
			RWSType:       "one-node0",
			limit:         1,
			nodeIDs:       []uint32{1},
			expectedError: models.ErrNodeNotFoundRWS,
		},
		{
			name:        "one nodeID",
			RWSType:     "complex",
			limit:       1,
			nodeIDs:     []uint32{3},
			expectedIDs: []uint32{1},
		},
		{
			name:        "multiple nodeIDs",
			RWSType:     "triangle",
			limit:       6,
			nodeIDs:     []uint32{0, 1},
			expectedIDs: []uint32{0, 1, 2},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			walkIDs, err := RWS.WalksVisitingAny(context.Background(), test.limit, test.nodeIDs...)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Walks(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walkIDs, test.expectedIDs) {
				t.Errorf("Walks(): expected %v, got %v", test.expectedIDs, walkIDs)
			}
		})
	}
}

func TestAddWalks(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			RWSType       string
			walks         []models.RandomWalk
			expectedError error
		}{
			{
				name:          "nil RWS",
				RWSType:       "nil",
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "nil walks",
				RWSType:       "empty",
				walks:         nil,
				expectedError: nil,
			},
			{
				name:          "empty walks",
				RWSType:       "empty",
				walks:         []models.RandomWalk{},
				expectedError: nil,
			},
			{
				name:          "one nil walk",
				RWSType:       "triangle",
				walks:         []models.RandomWalk{{0}, nil},
				expectedError: models.ErrNilWalkPointer,
			},
			{
				name:          "one empty walk",
				RWSType:       "triangle",
				walks:         []models.RandomWalk{{0}, {}},
				expectedError: models.ErrEmptyWalk,
			},
		}

		for _, test := range testCases {
			cl := redisutils.SetupTestClient()
			defer redisutils.CleanupRedis(cl)

			t.Run(test.name, func(t *testing.T) {
				RWS, err := SetupRWS(cl, test.RWSType)
				if err != nil {
					t.Fatalf("SetupRWS(): expected nil, got %v", err)
				}

				err = RWS.AddWalks(context.Background(), test.walks)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("AddWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		cl := redisutils.SetupTestClient()
		defer redisutils.CleanupRedis(cl)

		RWS, err := SetupRWS(cl, "empty")
		if err != nil {
			t.Fatalf("SetupRWS(): expected nil, got %v", err)
		}

		walks := []models.RandomWalk{{1, 2, 3}, {1, 5}}
		expectedLastWalkID := uint32(1)
		expectedTotalVisits := len(walks[0]) + len(walks[1])
		expectedWalksVisiting := map[uint32][]uint32{
			1: {0, 1},
			2: {0},
			3: {0},
			5: {1},
		}

		if err := RWS.AddWalks(context.Background(), walks); err != nil {
			t.Fatalf("AddWalks(): expected nil, got %v", err)
		}

		// check the last walkID has been incremented
		strLastWalkID, err := cl.HGet(context.Background(), KeyRWS, KeyLastWalkID).Result()
		if err != nil {
			t.Fatalf("HGet(): expected nil, got %v", err)
		}
		lastWalkID, err := redisutils.ParseID(strLastWalkID)
		if err != nil {
			t.Fatalf("ParseID(): expected nil, got %v", err)
		}
		if lastWalkID != expectedLastWalkID {
			t.Errorf("AddWalks(): expected walkID = %v, got %v", expectedLastWalkID, lastWalkID)
		}

		// check if the loaded walks match the originals
		for i, walk := range walks {
			walkID := redisutils.FormatID(uint32(i))
			strWalk, err := RWS.client.HGet(context.Background(), KeyWalks, walkID).Result()
			if err != nil {
				t.Fatalf("Get(): expected nil, got %v", err)
			}
			loadedWalk, err := redisutils.ParseWalk(strWalk)
			if err != nil {
				t.Fatalf("ParseWalk(): expected nil, got %v", err)
			}
			if !reflect.DeepEqual(walk, loadedWalk) {
				t.Errorf("AddWalks(): expected %v, got %v", walk, loadedWalk)
			}
		}

		// check that each node is associated with the expected walkIDs
		for _, nodeID := range []uint32{1, 2, 3, 5} {
			strIDs, err := RWS.client.SMembers(context.Background(), KeyWalksVisiting(nodeID)).Result()
			if err != nil {
				t.Fatalf("SMembers(): expected nil, got %v", err)
			}

			walkIDs := []uint32{}
			for _, strID := range strIDs {
				walkID, err := redisutils.ParseID(strID)
				if err != nil {
					t.Fatalf("ParseID(): expected nil, got %v", err)
				}

				walkIDs = append(walkIDs, walkID)
			}
			slices.Sort(walkIDs)

			if !reflect.DeepEqual(walkIDs, expectedWalksVisiting[nodeID]) {
				t.Errorf("AddWalks(): nodeID %d expected %v, got %v", nodeID, expectedWalksVisiting[nodeID], walkIDs)
			}
		}

		// check the total visits
		strVisits, err := cl.HGet(context.Background(), KeyRWS, KeyTotalVisits).Result()
		if err != nil {
			t.Errorf("TotalVisits(): expected nil, got %v", err)
		}
		visits, err := redisutils.ParseInt64(strVisits)
		if err != nil {
			t.Errorf("unexpected result type: %v", strVisits)
		}

		if visits != int64(expectedTotalVisits) {
			t.Errorf("AddWalk(): expected total visits %v, got %v", expectedTotalVisits, visits)
		}
	})
}

func TestRemoveWalks(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			RWSType       string
			expectedError error
		}{
			{
				name:          "nil RWS",
				RWSType:       "nil",
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "walk not found",
				RWSType:       "one-node0",
				expectedError: models.ErrWalkNotFound,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				RWS, err := SetupRWS(cl, test.RWSType)
				if err != nil {
					t.Fatalf("SetupRWS(): expected nil, got %v", err)
				}

				err = RWS.RemoveWalks(context.Background(), []uint32{0, 69})
				if !errors.Is(err, test.expectedError) {
					t.Errorf("RemoveWalks(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		RWS, err := SetupRWS(cl, "triangle")
		if err != nil {
			t.Fatalf("SetupRWS(): expected nil, got %v", err)
		}

		nodeIDs := []uint32{0, 1, 2}
		walkIDs := []uint32{0, 1}
		expectedTotalVisits := 3

		if err := RWS.RemoveWalks(context.Background(), walkIDs); err != nil {
			t.Fatalf("RemoveWalks(%d): expected nil, got %v", walkIDs, err)
		}

		// check the walks have been removed from the WalkIndex
		for _, walkID := range walkIDs {
			if walk, err := cl.HGet(context.Background(), KeyWalks, redisutils.FormatID(walkID)).Result(); !errors.Is(err, redis.Nil) {
				t.Fatalf("RemoveWalk(%d): expected walk %v to be removed: %v", walkID, walk, err)
			}
		}

		// check the walkID has been removed from each node
		for _, nodeID := range nodeIDs {
			strIDs, err := cl.SMembers(context.Background(), KeyWalksVisiting(nodeID)).Result()
			if err != nil {
				t.Errorf("SIsMember(): expected nil, got %v", err)
			}

			if !reflect.DeepEqual(strIDs, []string{"2"}) {
				t.Errorf("RemoveWalk(): nodeID %d, expected %v, got %v", nodeID, []string{"2"}, strIDs)
			}
		}

		// check that the total visits have been decreased by len(walk)
		visits := RWS.TotalVisits(context.Background())
		if visits != expectedTotalVisits {
			t.Errorf("RemoveWalk(): expected totalVisits = %v, got %v", expectedTotalVisits, visits)
		}
	})
}

func TestPruneGraftWalk(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name           string
			RWSType        string
			walkID         uint32
			cutIndex       int
			newWalkSegment models.RandomWalk
			expectedError  error
		}{
			{
				name:           "nil RWS",
				RWSType:        "nil",
				walkID:         0,
				cutIndex:       1,
				newWalkSegment: models.RandomWalk{0},
				expectedError:  models.ErrNilRWSPointer,
			},
			{
				name:           "walk not found in RWS",
				RWSType:        "empty",
				walkID:         0,
				cutIndex:       1,
				newWalkSegment: models.RandomWalk{0},
				expectedError:  redis.Nil,
			},
			{
				name:           "invalid cutIndex",
				RWSType:        "one-walk0",
				walkID:         0,
				cutIndex:       99,
				newWalkSegment: models.RandomWalk{},
				expectedError:  models.ErrInvalidWalkIndex,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				RWS, err := SetupRWS(cl, test.RWSType)
				if err != nil {
					t.Fatalf("SetupRWS(): expected nil, got %v", err)
				}

				err = RWS.PruneGraftWalk(context.Background(), test.walkID, test.cutIndex, test.newWalkSegment)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("PruneGraftWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		testCases := []struct {
			name                string
			RWSType             string
			walkID              uint32
			cutIndex            int
			oldWalk             models.RandomWalk
			newWalkSegment      models.RandomWalk
			expectedWalk        models.RandomWalk
			expectedTotalVisits int
		}{
			{
				name:                "pruning only",
				RWSType:             "one-walk0",
				walkID:              0,
				cutIndex:            2,
				oldWalk:             models.RandomWalk{0, 1, 2, 3},
				newWalkSegment:      models.RandomWalk{},
				expectedWalk:        models.RandomWalk{0, 1},
				expectedTotalVisits: 2,
			},
			{
				name:                "grafting only",
				RWSType:             "one-walk0",
				walkID:              0,
				cutIndex:            4, //  the lenght of the walk
				oldWalk:             models.RandomWalk{0, 1, 2, 3},
				newWalkSegment:      models.RandomWalk{4, 5},
				expectedWalk:        models.RandomWalk{0, 1, 2, 3, 4, 5},
				expectedTotalVisits: 6,
			},
			{
				name:                "pruning and grafting",
				RWSType:             "one-walk0",
				walkID:              0,
				cutIndex:            1,
				oldWalk:             models.RandomWalk{0, 1, 2, 3},
				newWalkSegment:      models.RandomWalk{4, 5},
				expectedWalk:        models.RandomWalk{0, 4, 5},
				expectedTotalVisits: 3,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				RWS, err := SetupRWS(cl, test.RWSType)
				if err != nil {
					t.Fatalf("SetupRWS(): expected nil, got %v", err)
				}

				if err := RWS.PruneGraftWalk(context.Background(), test.walkID, test.cutIndex, test.newWalkSegment); err != nil {
					t.Fatalf("PruneGraftWalk(): expected nil, got %v", err)
				}

				// check the walk has been changed correctly
				strWalk, err := RWS.client.HGet(context.Background(), KeyWalks, redisutils.FormatID(test.walkID)).Result()
				if err != nil {
					t.Fatalf("Get(): expected nil, got %v", err)
				}
				walk, err := redisutils.ParseWalk(strWalk)
				if err != nil {
					t.Errorf("ParseWalk(%v): expected nil, got %v", strWalk, err)
				}

				if !reflect.DeepEqual(walk, test.expectedWalk) {
					t.Errorf("PruneGraftWalk(): excepted %v, got %v", test.expectedWalk, walk)
				}

				// check that each node in walk contains only walkID
				expectedWalkIDs := []string{strconv.FormatUint(uint64(test.walkID), 10)}
				for _, nodeID := range test.expectedWalk {
					strWalkIDs, err := RWS.client.SMembers(context.Background(), KeyWalksVisiting(nodeID)).Result()
					if err != nil {
						t.Fatalf("SMembers(%d): expected nil, got %v", nodeID, err)
					}

					if !reflect.DeepEqual(strWalkIDs, expectedWalkIDs) {
						t.Errorf("PruneGraftWalk(): expected %v, got %v", expectedWalkIDs, strWalkIDs)
					}
				}

				// check that each pruned node doesn't contain walkID
				for _, nodeID := range test.oldWalk[test.cutIndex:] {
					size, err := RWS.client.SCard(context.Background(), KeyWalksVisiting(nodeID)).Result()
					if err != nil {
						t.Fatalf("SCard(%d): expected nil, got %v", nodeID, err)
					}

					if size != 0 {
						t.Errorf("PruneGraftWalk(%d): expected empty set, got carinality = %v", nodeID, size)
					}
				}

				// check the total visits
				strVisits, err := cl.HGet(context.Background(), KeyRWS, KeyTotalVisits).Result()
				if err != nil {
					t.Errorf("TotalVisits(): expected nil, got %v", err)
				}

				visits, err := redisutils.ParseInt64(strVisits)
				if err != nil {
					t.Errorf("unexpected result type: %v", strVisits)
				}

				if visits != int64(test.expectedTotalVisits) {
					t.Errorf("TotalVisits(): expected %v, got %v", test.expectedTotalVisits, visits)
				}

			})
		}
	})
}

// func TestInterface(t *testing.T) {
// 	var _ models.RandomWalkStore = &RandomWalkStore{}
// }

// ------------------------------------BENCHMARKS------------------------------

func BenchmarkVisitCounts(b *testing.B) {
	b.Run("fixed number of nodes", func(b *testing.B) {
		nodesNum := 100
		for _, walksNum := range []int{100, 1000, 10000} {
			b.Run(fmt.Sprintf("walksNum=%d", walksNum), func(b *testing.B) {
				cl := redisutils.SetupTestClient()
				defer redisutils.CleanupRedis(cl)

				RWS, err := GenerateRWS(cl, nodesNum, walksNum)
				if err != nil {
					b.Fatalf("GenerateRWS() benchmark failed: %v", err)
				}

				nodeIDs := make([]uint32, 0, nodesNum)
				for i := 0; i < nodesNum; i++ {
					nodeIDs = append(nodeIDs, uint32(i))
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := RWS.VisitCounts(context.Background(), nodeIDs); err != nil {
						b.Fatalf("benchmark failed: %v", err)
					}
				}
			})
		}
	})

	b.Run("fixed number of walks", func(b *testing.B) {
		walksNum := 10000
		for _, nodesNum := range []int{100, 1000, 10000} {
			b.Run(fmt.Sprintf("nodesNum=%d", nodesNum), func(b *testing.B) {
				cl := redisutils.SetupTestClient()
				defer redisutils.CleanupRedis(cl)

				RWS, err := GenerateRWS(cl, nodesNum, walksNum)
				if err != nil {
					b.Fatalf("GenerateRWS() benchmark failed: %v", err)
				}

				nodeIDs := make([]uint32, 0, nodesNum)
				for i := 0; i < nodesNum; i++ {
					nodeIDs = append(nodeIDs, uint32(i))
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := RWS.VisitCounts(context.Background(), nodeIDs); err != nil {
						b.Fatalf("benchmark failed: %v", err)
					}
				}
			})
		}
	})
}

// func BenchmarkWalks(b *testing.B) {
// 	b.Run("fixed number of nodes", func(b *testing.B) {
// 		nodesNum := 100
// 		for _, walksNum := range []int{100, 1000, 10000} {
// 			b.Run(fmt.Sprintf("walksNum=%d", walksNum), func(b *testing.B) {
// 				cl := redisutils.SetupTestClient()
// 				defer redisutils.CleanupRedis(cl)

// 				RWS, err := GenerateRWS(cl, nodesNum, walksNum)
// 				if err != nil {
// 					b.Fatalf("GenerateRWS() benchmark failed: %v", err)
// 				}

// 				b.ResetTimer()
// 				for i := 0; i < b.N; i++ {
// 					if _, err := RWS.Walks(context.Background(), 0, -1); err != nil {
// 						b.Fatalf("benchmark failed: %v", err)
// 					}
// 				}
// 			})
// 		}
// 	})
// }
