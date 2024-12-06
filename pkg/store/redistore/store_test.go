package redistore

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

func TestNewRWS(t *testing.T) {
	cl := redisutils.SetupClient()
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
					if RWS.Alpha() != alpha {
						t.Errorf("NewRWS(): expected %v, got %v", alpha, RWS.Alpha())
					}

					if RWS.WalksPerNode() != test.walksPerNode {
						t.Errorf("NewRWS(): expected %v, got %v", test.walksPerNode, RWS.WalksPerNode())
					}
				}
			}
		})
	}
}

func TestLoadRWS(t *testing.T) {
	cl := redisutils.SetupClient()
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

			RWS, err := LoadRWS(context.Background(), cl)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("LoadRWS(): expected %v, got %v", test.expectedError, err)
			}

			// check if the parameters have been added correctly
			if RWS != nil {
				if RWS.Alpha() != float32(0.85) {
					t.Errorf("LoadRWS(): expected %v, got %v", 0.85, RWS.Alpha())
				}

				if RWS.WalksPerNode() != uint16(1) {
					t.Errorf("LoadRWS(): expected %v, got %v", 1, RWS.WalksPerNode())
				}
			}
		})
	}
}

func TestTotalVisits(t *testing.T) {
	cl := redisutils.SetupClient()
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

			visits := RWS.TotalVisits()
			if visits != test.expectedTotalVisits {
				t.Errorf("TotalVisits(): expected %v, got %v", test.expectedTotalVisits, visits)
			}
		})
	}
}

func TestSetTotalVisits(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name          string
		RWSType       string
		totalVisits   int
		expectedError error
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "incorrect totalVisits value",
			RWSType:       "empty",
			totalVisits:   -10,
			expectedError: models.ErrInvalidTotalVisits,
		},
		{
			name:          "non-empty RWS",
			RWSType:       "one-node0",
			totalVisits:   69,
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			err = RWS.SetTotalVisits(test.totalVisits)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("SetTotalVisits(): expected %v, got %v", test.expectedError, err)
			}

			// check that the new value has been written
			if err == nil {
				visits := RWS.TotalVisits()
				if visits != test.totalVisits {
					t.Errorf("SetTotalVisits(): expected %v, got %v", test.totalVisits, visits)
				}
			}
		})
	}
}

func TestIsEmpty(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name          string
		RWSType       string
		expectedEmpty bool
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			expectedEmpty: true,
		},
		{
			name:          "empty RWS",
			RWSType:       "empty",
			expectedEmpty: true,
		},
		{
			name:          "non-empty RWS",
			RWSType:       "one-walk0",
			expectedEmpty: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			empty := RWS.IsEmpty()
			if empty != test.expectedEmpty {
				t.Errorf("IsEmpty(): expected %v, got %v", test.expectedEmpty, empty)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name          string
		RWSType       string
		expectedEmpty bool
		expectedError error
	}{
		{
			name:          "nil RWS, expected empty",
			RWSType:       "nil",
			expectedEmpty: true,
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "nil RWS, expected non-empty",
			RWSType:       "nil",
			expectedEmpty: false,
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "empty RWS, expected empty",
			RWSType:       "empty",
			expectedEmpty: true,
			expectedError: nil,
		},
		{
			name:          "empty RWS, expected non-empty",
			RWSType:       "empty",
			expectedEmpty: false,
			expectedError: models.ErrEmptyRWS,
		},
		{
			name:          "non-empty RWS, expected empty",
			RWSType:       "one-walk0",
			expectedEmpty: true,
			expectedError: models.ErrNonEmptyRWS,
		},
		{
			name:          "non-empty RWS, expected non-empty",
			RWSType:       "one-walk0",
			expectedEmpty: false,
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			err = RWS.Validate(test.expectedEmpty)
			if !errors.Is(err, test.expectedError) {
				t.Errorf("Validate(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestContainsNode(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name             string
		RWSType          string
		nodeID           uint32
		expectedContains bool
	}{
		{
			name:             "nil RWS",
			RWSType:          "nil",
			nodeID:           0,
			expectedContains: false,
		},
		{
			name:             "empty RWS",
			RWSType:          "empty",
			nodeID:           0,
			expectedContains: false,
		},
		{
			name:             "nodeID not in RWS",
			RWSType:          "one-walk0",
			nodeID:           5,
			expectedContains: false,
		},
		{
			name:             "nodeID in RWS",
			RWSType:          "one-walk0",
			nodeID:           0,
			expectedContains: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			contains := RWS.ContainsNode(test.nodeID)
			if contains != test.expectedContains {
				t.Errorf("IsEmpty(): expected %v, got %v", test.expectedContains, contains)
			}
		})
	}
}

func TestVisitCounts(t *testing.T) {
	cl := redisutils.SetupClient()
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

			visits, err := RWS.VisitCounts(test.nodeIDs)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("VisitCounts(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(visits, test.expectedVisits) {
				t.Errorf("VisitCount(0): expected %v, got %v", test.expectedVisits, visits)
			}
		})
	}
}

func TestVisitCountsLUA(t *testing.T) {
	cl := redisutils.SetupClient()
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

			visits, err := RWS.VisitCountsLUA(test.nodeIDs)
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
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name            string
		RWSType         string
		nodeID          uint32
		expectedWalkMap map[uint32]models.RandomWalk
		expectedError   error
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			nodeID:        0,
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "empty RWS",
			RWSType:       "empty",
			nodeID:        0,
			expectedError: models.ErrEmptyRWS,
		},
		{
			name:            "node not found in RWS",
			RWSType:         "one-node0",
			nodeID:          1,
			expectedWalkMap: map[uint32]models.RandomWalk{},
			expectedError:   nil,
		},
		{
			name:    "normal",
			RWSType: "triangle",
			nodeID:  0,
			expectedWalkMap: map[uint32]models.RandomWalk{
				0: {0, 1, 2},
				1: {1, 2, 0},
				2: {2, 0, 1},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			walkMap, err := RWS.Walks(test.nodeID, -1)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Walks(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walkMap, test.expectedWalkMap) {
				t.Errorf("Walks(): expected %v, got %v", test.expectedWalkMap, walkMap)
			}
		})
	}
}

func TestCommonWalks(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name            string
		RWSType         string
		nodeID          uint32
		removedNodes    []uint32
		expectedWalkMap map[uint32]models.RandomWalk
		expectedError   error
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			nodeID:        0,
			removedNodes:  []uint32{1},
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:            "node not found in RWS",
			RWSType:         "one-node0",
			nodeID:          1,
			removedNodes:    []uint32{2},
			expectedWalkMap: map[uint32]models.RandomWalk{},
			expectedError:   nil,
		},
		{
			name:         "valid triangle",
			RWSType:      "triangle",
			nodeID:       0,
			removedNodes: []uint32{1},
			expectedWalkMap: map[uint32]models.RandomWalk{
				0: {0, 1, 2},
				1: {1, 2, 0},
				2: {2, 0, 1},
			},
		},
		{
			name:         "valid complex",
			RWSType:      "complex",
			nodeID:       0,
			removedNodes: []uint32{3},
			expectedWalkMap: map[uint32]models.RandomWalk{
				1: {0, 3},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}

			walkMap, err := RWS.CommonWalks(test.nodeID, test.removedNodes)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("CommonWalks(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walkMap, test.expectedWalkMap) {
				t.Errorf("CommonWalks(): expected %v, got %v", test.expectedWalkMap, walkMap)
			}
		})
	}
}

func TestWalksRand(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name                   string
		RWSType                string
		nodeID                 uint32
		probabilityOfSelection float32
		expectedWalkMap        map[uint32]models.RandomWalk
		expectedError          error
	}{
		{
			name:                   "nil RWS",
			RWSType:                "nil",
			nodeID:                 0,
			probabilityOfSelection: 1.0,
			expectedError:          models.ErrNilRWSPointer,
		},
		{
			name:                   "node not found in RWS",
			RWSType:                "one-node0",
			nodeID:                 1,
			probabilityOfSelection: 1.0,
			expectedWalkMap:        map[uint32]models.RandomWalk{},
			expectedError:          nil,
		},
		{
			name:                   "valid, all walks returned",
			RWSType:                "triangle",
			nodeID:                 0,
			probabilityOfSelection: 1.0,
			expectedWalkMap: map[uint32]models.RandomWalk{
				0: {0, 1, 2},
				1: {1, 2, 0},
				2: {2, 0, 1},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS, err := SetupRWS(cl, test.RWSType)
			if err != nil {
				t.Fatalf("SetupRWS(): expected nil, got %v", err)
			}
			walkMap, err := RWS.WalksRand(test.nodeID, test.probabilityOfSelection)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("WalksRand(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walkMap, test.expectedWalkMap) {
				t.Errorf("WalksRand(): expected %v, got %v", test.expectedWalkMap, walkMap)
			}
		})
	}
}

func TestAddWalk(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			RWSType       string
			walk          models.RandomWalk
			expectedError error
		}{
			{
				name:          "nil RWS",
				RWSType:       "nil",
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "nil walk",
				RWSType:       "empty",
				walk:          nil,
				expectedError: models.ErrNilWalkPointer,
			},
			{
				name:          "empty walk",
				RWSType:       "empty",
				walk:          models.RandomWalk{},
				expectedError: models.ErrEmptyWalk,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				RWS, err := SetupRWS(cl, test.RWSType)
				if err != nil {
					t.Fatalf("SetupRWS(): expected nil, got %v", err)
				}

				err = RWS.AddWalk(test.walk)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("AddWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		RWS, err := SetupRWS(cl, "empty")
		if err != nil {
			t.Fatalf("SetupRWS(): expected nil, got %v", err)
		}

		walk := models.RandomWalk{1, 2, 3}
		if err := RWS.AddWalk(walk); err != nil {
			t.Fatalf("AddWalk(): expected nil, got %v", err)
		}

		// check the walkID has been incremented
		strLastWalkID, err := cl.HGet(RWS.ctx, KeyRWS, KeyLastWalkID).Result()
		if err != nil {
			t.Fatalf("HGet(): expected nil, got %v", err)
		}

		lastWalkID, err := redisutils.ParseID(strLastWalkID)
		if err != nil {
			t.Fatalf("ParseID(): expected nil, got %v", err)
		}

		if lastWalkID != 0 {
			t.Errorf("AddWalk(): expected walkID = 1, got %v", lastWalkID)
		}

		// Load the walk from Redis
		strWalk, err := RWS.client.HGet(RWS.ctx, KeyWalks, "0").Result()
		if err != nil {
			t.Fatalf("Get(): expected nil, got %v", err)
		}

		loadedWalk, err := redisutils.ParseWalk(strWalk)
		if err != nil {
			t.Fatalf("ParseWalk(): expected nil, got %v", err)
		}

		// check if the two walks match
		if !reflect.DeepEqual(walk, loadedWalk) {
			t.Errorf("AddWalk(): expected %v, got %v", walk, loadedWalk)
		}

		// check that each node is associated with the walkID = 0
		for _, nodeID := range walk {

			// Get the only walkID associated with nodeID
			strWalkID, err := RWS.client.SRandMember(RWS.ctx, KeyWalksVisiting(nodeID)).Result()
			if err != nil {
				t.Fatalf("SRandMember(): expected nil, got %v", err)
			}

			// Parse it to a walkID
			loadedWalkID, err := redisutils.ParseID(strWalkID)
			if err != nil {
				t.Fatalf("ParseID(): expected nil, got %v", err)
			}

			// check it matches the intended walkID
			if loadedWalkID != 0 {
				t.Errorf("AddWalk(): expected %v, got %v", 0, loadedWalkID)
			}
		}
	})
}

func TestPruneGraftWalk(t *testing.T) {
	cl := redisutils.SetupClient()
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

				err = RWS.PruneGraftWalk(test.walkID, test.cutIndex, test.newWalkSegment)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("PruneGraftWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		testCases := []struct {
			name           string
			RWSType        string
			walkID         uint32
			cutIndex       int
			oldWalk        models.RandomWalk
			newWalkSegment models.RandomWalk
			expectedWalk   models.RandomWalk
		}{
			{
				name:           "pruning only",
				RWSType:        "one-walk0",
				walkID:         0,
				cutIndex:       2,
				oldWalk:        models.RandomWalk{0, 1, 2, 3},
				newWalkSegment: models.RandomWalk{},
				expectedWalk:   models.RandomWalk{0, 1},
			},
			{
				name:           "grafting only",
				RWSType:        "one-walk0",
				walkID:         0,
				cutIndex:       4, //  the lenght of the walk
				oldWalk:        models.RandomWalk{0, 1, 2, 3},
				newWalkSegment: models.RandomWalk{4, 5},
				expectedWalk:   models.RandomWalk{0, 1, 2, 3, 4, 5},
			},
			{
				name:           "pruning and grafting",
				RWSType:        "one-walk0",
				walkID:         0,
				cutIndex:       1, //  the lenght of the walk
				oldWalk:        models.RandomWalk{0, 1, 2, 3},
				newWalkSegment: models.RandomWalk{4, 5},
				expectedWalk:   models.RandomWalk{0, 4, 5},
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				RWS, err := SetupRWS(cl, test.RWSType)
				if err != nil {
					t.Fatalf("SetupRWS(): expected nil, got %v", err)
				}

				if err := RWS.PruneGraftWalk(test.walkID, test.cutIndex, test.newWalkSegment); err != nil {
					t.Fatalf("PruneGraftWalk(): expected nil, got %v", err)
				}

				// check the walk has been changed correctly
				strWalk, err := RWS.client.HGet(RWS.ctx, KeyWalks, redisutils.FormatID(test.walkID)).Result()
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
					strWalkIDs, err := RWS.client.SMembers(RWS.ctx, KeyWalksVisiting(nodeID)).Result()
					if err != nil {
						t.Fatalf("SMembers(%d): expected nil, got %v", nodeID, err)
					}

					if !reflect.DeepEqual(strWalkIDs, expectedWalkIDs) {
						t.Errorf("PruneGraftWalk(): expected %v, got %v", expectedWalkIDs, strWalkIDs)
					}
				}

				// check that each pruned node doesn't contain walkID
				for _, nodeID := range test.oldWalk[test.cutIndex:] {
					size, err := RWS.client.SCard(RWS.ctx, KeyWalksVisiting(nodeID)).Result()
					if err != nil {
						t.Fatalf("SCard(%d): expected nil, got %v", nodeID, err)
					}

					if size != 0 {
						t.Errorf("PruneGraftWalk(%d): expected empty set, got carinality = %v", nodeID, size)
					}
				}

			})
		}
	})
}

func TestInterface(t *testing.T) {
	var _ models.RandomWalkStore = &RandomWalkStore{}
}

// ------------------------------------BENCHMARKS------------------------------

func BenchmarkVisitCounts(b *testing.B) {
	b.Run("FixedWalksPerNode", func(b *testing.B) {
		edgesPerNode := 100
		rng := rand.New(rand.NewSource(69))

		// Different DB sizes
		for _, nodesSize := range []int{100, 1000, 10000} {
			b.Run(fmt.Sprintf("DBSize=%d", nodesSize), func(b *testing.B) {
				cl := redisutils.SetupClient()
				defer redisutils.CleanupRedis(cl)

				nodeIDs := make([]uint32, 0, nodesSize)
				for i := 0; i < nodesSize; i++ {
					nodeIDs = append(nodeIDs, uint32(i))
				}

				// Setup DB and RWS
				DB, err := redisdb.GenerateDB(cl, nodesSize, edgesPerNode, rng)
				if err != nil {
					b.Fatalf("GenerateDB(): expected nil, got %v", err)
				}
				RWS, err := NewRWS(context.Background(), cl, 0.85, 10)
				if err != nil {
					b.Fatalf("NewRWS(): expected nil, got %v", err)
				}
				RWM := walks.RandomWalkManager{Store: RWS}
				if err := RWM.GenerateAll(DB); err != nil {
					b.Fatalf("GenerateAll(): expected nil, got %v", err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := RWS.VisitCounts(nodeIDs); err != nil {
						b.Fatalf("benchmark failed: %v", err)
					}
				}
			})
		}
	})
}

func BenchmarkVisitCountsLUA(b *testing.B) {
	b.Run("FixedWalksPerNode", func(b *testing.B) {
		edgesPerNode := 100
		rng := rand.New(rand.NewSource(69))

		// Different DB sizes
		for _, nodesSize := range []int{100, 1000, 10000} {
			b.Run(fmt.Sprintf("DBSize=%d", nodesSize), func(b *testing.B) {
				cl := redisutils.SetupClient()
				defer redisutils.CleanupRedis(cl)

				nodeIDs := make([]uint32, 0, nodesSize)
				for i := 0; i < nodesSize; i++ {
					nodeIDs = append(nodeIDs, uint32(i))
				}

				// Setup DB and RWS
				DB, err := redisdb.GenerateDB(cl, nodesSize, edgesPerNode, rng)
				if err != nil {
					b.Fatalf("GenerateDB(): expected nil, got %v", err)
				}
				RWS, err := NewRWS(context.Background(), cl, 0.85, 10)
				if err != nil {
					b.Fatalf("NewRWS(): expected nil, got %v", err)
				}
				RWM := walks.RandomWalkManager{Store: RWS}
				if err := RWM.GenerateAll(DB); err != nil {
					b.Fatalf("GenerateAll(): expected nil, got %v", err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := RWS.VisitCountsLUA(nodeIDs); err != nil {
						b.Fatalf("benchmark failed: %v", err)
					}
				}
			})
		}
	})
}

// WAY TOO SLOW. BETTER TO HAVE THIS FILTERING DONE CLIENT SIDE, WHICH MEANS ONE
// LESS METHOD OF THE INTERFACE!
func BenchmarkCommonWalks(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	RWS, err := LoadRWS(ctx, cl)
	if err != nil {
		b.Fatalf("LoadRWS(): expected nil, got %v", err)
	}

	removedNodes := []uint32{0, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := 0; i < b.N; i++ {
		if _, err := RWS.CommonWalks(1, removedNodes); err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}

func BenchmarkWalksRand(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	RWS, err := LoadRWS(ctx, cl)
	if err != nil {
		b.Fatalf("LoadRWS(): expected nil, got %v", err)
	}

	var probabilityOfSelection float32 = 0.03
	for i := 0; i < b.N; i++ {
		if _, err := RWS.WalksRand(1, probabilityOfSelection); err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}

func BenchmarkWalks(b *testing.B) {
	b.Run("FixedDB", func(b *testing.B) {
		nodesSize := 1000
		edgesPerNode := 100
		rng := rand.New(rand.NewSource(69))

		// Different DB sizes
		for _, walkPerNode := range []int{1, 10, 100} {
			b.Run(fmt.Sprintf("walksPerNode=%d", walkPerNode), func(b *testing.B) {
				cl := redisutils.SetupClient()
				defer redisutils.CleanupRedis(cl)

				// Setup DB and RWS
				DB, err := redisdb.GenerateDB(cl, nodesSize, edgesPerNode, rng)
				if err != nil {
					b.Fatalf("GenerateDB(): expected nil, got %v", err)
				}
				RWS, err := NewRWS(context.Background(), cl, 0.85, uint16(walkPerNode))
				if err != nil {
					b.Fatalf("NewRWS(): expected nil, got %v", err)
				}
				RWM := walks.RandomWalkManager{Store: RWS}
				if err := RWM.GenerateAll(DB); err != nil {
					b.Fatalf("GenerateAll(): expected nil, got %v", err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := RWS.Walks(0, -1); err != nil {
						b.Fatalf("benchmark failed: %v", err)
					}
				}
			})
		}
	})
}
