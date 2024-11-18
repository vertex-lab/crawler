package mock

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/vertex-lab/crawler/pkg/models"
)

func TestNewRWS(t *testing.T) {
	testCases := []struct {
		name          string
		alphas        []float32
		walksPerNode  uint16
		expectedError error
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

				RWS, err := NewRWS(alpha, test.walksPerNode)
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

func TestIsEmpty(t *testing.T) {
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
			RWSType:       "one-node0",
			expectedEmpty: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RWS := SetupRWS(test.RWSType)
			empty := RWS.IsEmpty()

			if empty != test.expectedEmpty {
				t.Errorf("IsEmpty(): expected %v, got %v", test.expectedEmpty, empty)
			}
		})
	}
}

func TestNodeCount(t *testing.T) {
	testCases := []struct {
		name          string
		RWSType       string
		expectedCount int
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			expectedCount: 0,
		},
		{
			name:          "empty RWS",
			RWSType:       "empty",
			expectedCount: 0,
		},
		{
			name:          "one node RWS",
			RWSType:       "one-node0",
			expectedCount: 1,
		},
		{
			name:          "triangle RWS",
			RWSType:       "triangle",
			expectedCount: 3,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS := SetupRWS(test.RWSType)

			if RWS.NodeCount() != test.expectedCount {
				t.Errorf("NodeCount(): expected %v, got %v", test.expectedCount, RWS.NodeCount())
			}
		})
	}
}

func TestAllNodes(t *testing.T) {
	testCases := []struct {
		name            string
		RWSType         string
		expectedNodeIDs []uint32
	}{
		{
			name:            "nil RWS",
			RWSType:         "nil",
			expectedNodeIDs: []uint32{},
		},
		{
			name:            "empty RWS",
			RWSType:         "empty",
			expectedNodeIDs: []uint32{},
		},
		{
			name:            "one node RWS",
			RWSType:         "one-node0",
			expectedNodeIDs: []uint32{0},
		},
		{
			name:            "triangle RWS",
			RWSType:         "triangle",
			expectedNodeIDs: []uint32{0, 1, 2},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS := SetupRWS(test.RWSType)
			nodeIDs := RWS.AllNodes()
			slices.Sort(nodeIDs)

			if !reflect.DeepEqual(nodeIDs, test.expectedNodeIDs) {
				t.Errorf("NodeCount(): expected %v, got %v", test.expectedNodeIDs, nodeIDs)
			}
		})
	}
}

func TestContainsWalk(t *testing.T) {
	testCases := []struct {
		name             string
		RWSType          string
		walkID           uint32
		expectedContains bool
	}{
		{
			name:             "nil RWS",
			RWSType:          "nil",
			walkID:           0,
			expectedContains: false,
		},
		{
			name:             "empty RWS",
			RWSType:          "empty",
			walkID:           0,
			expectedContains: false,
		},
		{
			name:             "walkID not in RWS",
			RWSType:          "one-node0",
			walkID:           1,
			expectedContains: false,
		},
		{
			name:             "walkID in RWS",
			RWSType:          "one-node0",
			walkID:           0,
			expectedContains: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS := SetupRWS(test.RWSType)

			contains := RWS.ContainsWalk(test.walkID)
			if contains != test.expectedContains {
				t.Errorf("IsEmpty(): expected %v, got %v", test.expectedContains, contains)
			}
		})
	}
}

func TestContainsNode(t *testing.T) {
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
			RWSType:          "one-node0",
			nodeID:           1,
			expectedContains: false,
		},
		{
			name:             "nodeID in RWS",
			RWSType:          "one-node0",
			nodeID:           0,
			expectedContains: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS := SetupRWS(test.RWSType)

			contains := RWS.ContainsNode(test.nodeID)
			if contains != test.expectedContains {
				t.Errorf("IsEmpty(): expected %v, got %v", test.expectedContains, contains)
			}
		})
	}
}

func TestValidate(t *testing.T) {

	t.Run("simple errors", func(t *testing.T) {
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
				RWSType:       "one-node0",
				expectedEmpty: true,
				expectedError: models.ErrNonEmptyRWS,
			},
			{
				name:          "non-empty RWS, expected non-empty",
				RWSType:       "one-node0",
				expectedEmpty: false,
				expectedError: nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				RWS := SetupRWS(test.RWSType)
				err := RWS.Validate(test.expectedEmpty)

				if !errors.Is(err, test.expectedError) {
					t.Errorf("Validate(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("invalid walksPerNode", func(t *testing.T) {

		RWS, _ := NewRWS(0.85, 1)
		RWS.walksPerNode = 0

		err := RWS.Validate(true)
		if !errors.Is(err, models.ErrInvalidWalksPerNode) {
			t.Errorf("Validate(): expected %v, got %v", models.ErrInvalidWalksPerNode, err)
		}
	})

	t.Run("invalid alphas", func(t *testing.T) {

		RWS, _ := NewRWS(0.85, 1)
		invalidAlphas := []float32{1.1, 0.0, -1.0, -0.11, 55}

		for _, alpha := range invalidAlphas {
			RWS.alpha = alpha

			err := RWS.Validate(true)
			if !errors.Is(err, models.ErrInvalidAlpha) {
				t.Errorf("Validate(): expected %v, got %v", models.ErrInvalidAlpha, err)
			}
		}
	})
}

func TestVisitCount(t *testing.T) {
	testCases := []struct {
		name           string
		RWSType        string
		expectedVisits int
	}{
		{
			name:           "nil RWS",
			RWSType:        "nil",
			expectedVisits: 0,
		},
		{
			name:           "empty RWS",
			RWSType:        "empty",
			expectedVisits: 0,
		},
		{
			name:           "one node RWS",
			RWSType:        "one-node0",
			expectedVisits: 1,
		},
		{
			name:           "triangle RWS",
			RWSType:        "triangle",
			expectedVisits: 3,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS := SetupRWS(test.RWSType)

			if RWS.VisitCount(0) != test.expectedVisits {
				t.Errorf("VisitCount(0): expected %v, got %v", test.expectedVisits, RWS.VisitCount(0))
			}
		})
	}
}

func TestWalkIDs(t *testing.T) {

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
				name:          "empty RWS",
				RWSType:       "empty",
				expectedError: models.ErrEmptyRWS,
			},
			{
				name:          "non-empty RWS, node not found",
				RWSType:       "one-node0",
				expectedError: models.ErrNodeNotFoundRWS,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				RWS := SetupRWS(test.RWSType)

				_, err := RWS.WalkIDs(1)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("WalkIDs(1): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		RWS := SetupRWS("one-node1")

		walkSet, err := RWS.WalkIDs(1)
		if err != nil {
			t.Fatalf("WalkIDs(1): expected nil, got %v", err)
		}
		if !walkSet.Equal(mapset.NewSet[uint32](0)) {
			t.Errorf("WalkIDs(1): expected %v, got %v", 0, walkSet)
		}
	})
}

func TestWalks(t *testing.T) {
	testCases := []struct {
		name            string
		RWSType         string
		nodeID          uint32
		limit           int
		expectedWalkMap map[uint32]models.RandomWalk
		expectedError   error
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			nodeID:        0,
			limit:         1,
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "empty RWS",
			RWSType:       "empty",
			nodeID:        0,
			limit:         1,
			expectedError: models.ErrEmptyRWS,
		},
		{
			name:          "node not found in RWS",
			RWSType:       "one-node0",
			nodeID:        1,
			limit:         1,
			expectedError: models.ErrNodeNotFoundRWS,
		},
		{
			name:    "valid, all",
			RWSType: "triangle",
			nodeID:  0,
			limit:   -1,
			expectedWalkMap: map[uint32]models.RandomWalk{
				0: {0, 1, 2},
				1: {1, 2, 0},
				2: {2, 0, 1},
			},
		},
		{
			name:    "valid",
			RWSType: "triangle",
			nodeID:  0,
			limit:   2,
			expectedWalkMap: map[uint32]models.RandomWalk{
				0: {0, 1, 2},
				1: {1, 2, 0},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS := SetupRWS(test.RWSType)
			walkMap, err := RWS.Walks(test.nodeID, test.limit)

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
			name:          "empty RWS",
			RWSType:       "empty",
			nodeID:        0,
			removedNodes:  []uint32{1},
			expectedError: models.ErrEmptyRWS,
		},
		{
			name:          "node not found in RWS",
			RWSType:       "one-node0",
			nodeID:        1,
			removedNodes:  []uint32{2},
			expectedError: models.ErrNodeNotFoundRWS,
		},
		{
			name:         "valid",
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
			name:         "valid",
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
			RWS := SetupRWS(test.RWSType)
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
			name:                   "empty RWS",
			RWSType:                "empty",
			nodeID:                 0,
			probabilityOfSelection: 1.0,
			expectedError:          models.ErrEmptyRWS,
		},
		{
			name:                   "node not found in RWS",
			RWSType:                "one-node0",
			nodeID:                 1,
			probabilityOfSelection: 1.0,
			expectedError:          models.ErrNodeNotFoundRWS,
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
		{
			name:                   "valid, no walks returned",
			RWSType:                "triangle",
			nodeID:                 0,
			probabilityOfSelection: 0.0,
			expectedWalkMap:        map[uint32]models.RandomWalk{},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS := SetupRWS(test.RWSType)
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
				RWS := SetupRWS(test.RWSType)

				err := RWS.AddWalk(test.walk)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("AddWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {

		RWS := SetupRWS("empty")
		walk := models.RandomWalk{1, 2, 3}

		if err := RWS.AddWalk(walk); err != nil {
			t.Fatalf("AddWalk(): expected nil, got %v", err)
		}

		// check walk has been added to the WalkIndex
		if !reflect.DeepEqual(walk, RWS.WalkIndex[0]) {
			t.Errorf("AddWalk(): expected %v, got %v", walk, RWS.WalkIndex[0])
		}

		// check that each node is associated with the walkID = 0
		for _, nodeID := range walk {
			walkSet, err := RWS.WalkIDs(nodeID)
			if err != nil {
				t.Fatalf("WalkIDs(%d): expected nil, got %v", nodeID, err)
			}

			if !walkSet.Equal(mapset.NewSet[uint32](0)) {
				t.Errorf("AddWalk(): nodeID = %d; expected {0}, got %v", nodeID, walkSet)
			}
		}
	})
}

func TestPruneWalk(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			RWSType       string
			walkID        uint32
			expectedError error
		}{
			{
				name:          "nil RWS",
				RWSType:       "nil",
				walkID:        0,
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "empty RWS",
				RWSType:       "empty",
				walkID:        0,
				expectedError: models.ErrEmptyRWS,
			},
			{
				name:          "walkID not found",
				RWSType:       "one-node0",
				walkID:        1,
				expectedError: models.ErrWalkNotFound,
			},
			{
				name:          "invalid cutIndex",
				RWSType:       "one-node0",
				walkID:        0,
				expectedError: models.ErrInvalidWalkIndex,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				RWS := SetupRWS(test.RWSType)
				err := RWS.PruneWalk(test.walkID, 2)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("PruneWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		RWS := SetupRWS("simple")
		walkID := uint32(0)
		expectedPrunedWalk := models.RandomWalk{0}
		if err := RWS.PruneWalk(walkID, 1); err != nil {
			t.Fatalf("PruneWalk(): expected nil, got %v", err)
		}

		// check the walk was pruned
		if !reflect.DeepEqual(RWS.WalkIndex[walkID], expectedPrunedWalk) {
			t.Errorf("PruneWalk(): expected %v, got %v", expectedPrunedWalk, RWS.WalkIndex[walkID])
		}

		// check the walk remains
		walkSet0, err := RWS.WalkIDs(0)
		if err != nil {
			t.Fatalf("WalkIDs(0): expected nil, got %v", err)
		}

		if !walkSet0.Equal(mapset.NewSet[uint32](0)) {
			t.Errorf("PruneWalk(): expected {{1}}, got %v", walkSet0)
		}

		// check the walks was removed
		walkSet2, err := RWS.WalkIDs(1)
		if err != nil {
			t.Fatalf("WalkIDs(1): expected nil, got %v", err)
		}

		if !walkSet2.IsEmpty() {
			t.Errorf("PruneWalk(): expected empty set, got %v", walkSet2)
		}
	})
}

func TestGraftWalk(t *testing.T) {

	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			RWSType       string
			walkID        uint32
			expectedError error
		}{
			{
				name:          "nil RWS",
				RWSType:       "nil",
				walkID:        0,
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "empty RWS",
				RWSType:       "empty",
				walkID:        0,
				expectedError: models.ErrEmptyRWS,
			},
			{
				name:          "walkID not found",
				RWSType:       "one-node0",
				walkID:        1,
				expectedError: models.ErrWalkNotFound,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				RWS := SetupRWS(test.RWSType)
				err := RWS.GraftWalk(test.walkID, []uint32{1})

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("GraftWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {

		RWS := SetupRWS("simple")
		walkSegment := []uint32{2, 3, 4}
		expectedGraftedWalk := models.RandomWalk{0, 1, 2, 3, 4}
		expectedWalkSet := mapset.NewSet[uint32](0)

		if err := RWS.GraftWalk(0, walkSegment); err != nil {
			t.Fatalf("GraftWalk(): expected nil, got %v", err)
		}

		// check the walk has been grafted
		if !reflect.DeepEqual(RWS.WalkIndex[0], expectedGraftedWalk) {
			t.Fatalf("GraftWalk(): expected %v, got %v", expectedGraftedWalk, RWS.WalkIndex[0])
		}

		// check if the walk is present in all walkSets
		for _, nodeID := range expectedGraftedWalk {

			walkSet, err := RWS.WalkIDs(nodeID)
			if err != nil {
				t.Fatalf("WalkIDs(): expected nil, got %v", err)
			}

			if !walkSet.Equal(expectedWalkSet) {
				t.Errorf("GraftWalk(): nodeID = %d; expected %v, got %v", nodeID, expectedWalkSet, walkSet)
			}
		}
	})
}

func TestInterface(t *testing.T) {
	var _ models.RandomWalkStore = &RandomWalkStore{}
}
