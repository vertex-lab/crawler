package mock

import (
	"context"
	"errors"
	"reflect"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/vertex-lab/crawler/pkg/models"
)

func TestNewRWS(t *testing.T) {
	ctx := context.Background()
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
					if RWS.Alpha(ctx) != alpha {
						t.Errorf("NewRWS(): expected %v, got %v", alpha, RWS.Alpha(ctx))
					}

					if RWS.WalksPerNode(ctx) != test.walksPerNode {
						t.Errorf("NewRWS(): expected %v, got %v", test.walksPerNode, RWS.WalksPerNode(ctx))
					}
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	t.Run("nil RWS", func(t *testing.T) {
		RWS := SetupRWS("nil")
		err := RWS.Validate()

		if !errors.Is(err, models.ErrNilRWS) {
			t.Errorf("Validate(): expected %v, got %v", models.ErrNilRWS, err)
		}
	})

	t.Run("invalid walksPerNode", func(t *testing.T) {
		RWS, _ := NewRWS(0.85, 1)
		RWS.walksPerNode = 0

		err := RWS.Validate()
		if !errors.Is(err, models.ErrInvalidWalksPerNode) {
			t.Errorf("Validate(): expected %v, got %v", models.ErrInvalidWalksPerNode, err)
		}
	})

	t.Run("invalid alphas", func(t *testing.T) {
		RWS, _ := NewRWS(0.85, 1)
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
	testCases := []struct {
		name           string
		RWSType        string
		nodeIDs        []uint32
		expectedVisits []int
		expectedError  error
	}{
		{
			name:           "nil RWS",
			RWSType:        "nil",
			nodeIDs:        []uint32{0},
			expectedVisits: []int{},
			expectedError:  models.ErrNilRWS,
		},
		{
			name:           "empty RWS",
			RWSType:        "empty",
			nodeIDs:        []uint32{0},
			expectedVisits: []int{0},
			expectedError:  nil,
		},
		{
			name:           "empty nodeIDs",
			RWSType:        "one-node0",
			nodeIDs:        []uint32{},
			expectedVisits: []int{},
			expectedError:  nil,
		},
		{
			name:           "one node RWS",
			RWSType:        "one-node0",
			nodeIDs:        []uint32{0},
			expectedVisits: []int{1},
			expectedError:  nil,
		},
		{
			name:           "triangle RWS, one node not in the RWS",
			RWSType:        "triangle",
			nodeIDs:        []uint32{0, 1, 2, 99},
			expectedVisits: []int{3, 3, 3, 0},
			expectedError:  nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS := SetupRWS(test.RWSType)
			visits, err := RWS.VisitCounts(context.Background(), test.nodeIDs...)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("VisitCounts(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(visits, test.expectedVisits) {
				t.Errorf("VisitCounts(): expected %v, got %v", test.expectedVisits, visits)
			}
		})
	}
}

func TestWalks(t *testing.T) {
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
			expectedError: models.ErrNilRWS,
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
			RWS := SetupRWS(test.RWSType)
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

func TestWalksVisiting(t *testing.T) {
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
			expectedError: models.ErrNilRWS,
		},
		{
			name:    "empty RWS",
			RWSType: "empty",
			limit:   1,
			nodeIDs: []uint32{0},
		},
		{
			name:    "nodeID not found in RWS",
			RWSType: "one-node0",
			limit:   1,
			nodeIDs: []uint32{1},
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
			RWS := SetupRWS(test.RWSType)
			walkIDs, err := RWS.WalksVisiting(context.Background(), test.limit, test.nodeIDs...)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("WalksVisiting(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walkIDs, test.expectedIDs) {
				t.Errorf("WalksVisiting(): expected %v, got %v", test.expectedIDs, walkIDs)
			}
		})
	}
}

func TestWalksVisitingAll(t *testing.T) {
	testCases := []struct {
		name          string
		RWSType       string
		nodeIDs       []uint32
		expectedIDs   []uint32
		expectedError error
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			nodeIDs:       []uint32{0},
			expectedError: models.ErrNilRWS,
		},
		{
			name:          "empty RWS",
			RWSType:       "empty",
			nodeIDs:       []uint32{0},
			expectedError: nil,
		},
		{
			name:          "nodeID not found in RWS",
			RWSType:       "one-node0",
			nodeIDs:       []uint32{1},
			expectedError: nil,
		},
		{
			name:        "one nodeID",
			RWSType:     "complex",
			nodeIDs:     []uint32{3},
			expectedIDs: []uint32{1},
		},
		{
			name:        "multiple nodeIDs",
			RWSType:     "complex",
			nodeIDs:     []uint32{0, 2},
			expectedIDs: []uint32{0},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS := SetupRWS(test.RWSType)

			walkIDs, err := RWS.WalksVisitingAll(context.Background(), test.nodeIDs...)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("WalksVisitingAll(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walkIDs, test.expectedIDs) {
				t.Errorf("WalksVisitingAll(): expected %v, got %v", test.expectedIDs, walkIDs)
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
				expectedError: models.ErrNilRWS,
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
				RWSType:       "empty",
				walks:         []models.RandomWalk{{0, 1}, nil},
				expectedError: models.ErrNilWalk,
			},
			{
				name:          "one empty walk",
				RWSType:       "empty",
				walks:         []models.RandomWalk{{0, 1}, {}},
				expectedError: models.ErrEmptyWalk,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				RWS := SetupRWS(test.RWSType)

				err := RWS.AddWalks(context.Background(), test.walks...)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("AddWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		RWS := SetupRWS("empty")
		walks := []models.RandomWalk{{1, 2, 3}, {4, 5}}

		if err := RWS.AddWalks(context.Background(), walks...); err != nil {
			t.Fatalf("AddWalk(): expected nil, got %v", err)
		}

		for i, walk := range walks {
			walkID := uint32(i)

			// check walk has been added to the WalkIndex
			if !reflect.DeepEqual(walk, RWS.WalkIndex[walkID]) {
				t.Errorf("AddWalk(): expected %v, got %v", walk, RWS.WalkIndex[walkID])
			}

			// check that each node is associated with the walkID
			for _, nodeID := range walk {
				walkSet := RWS.walksVisiting[nodeID]
				if !walkSet.Equal(mapset.NewSet[uint32](walkID)) {
					t.Errorf("AddWalk(): nodeID = %d; expected {%d}, got %v", nodeID, walkID, walkSet)
				}
			}
		}

		// check that the total visits have been increased
		visits := len(walks[0]) + len(walks[1])
		if RWS.totalVisits != visits {
			t.Errorf("AddWalk(): expected totalVisits = %v, got %v", visits, RWS.totalVisits)
		}
	})
}
func TestRemoveWalks(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			RWSType       string
			expectedError error
		}{
			{
				name:          "nil RWS",
				RWSType:       "nil",
				expectedError: models.ErrNilRWS,
			},
			{
				name:          "walk node found",
				RWSType:       "simple",
				expectedError: models.ErrWalkNotFound,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				RWS := SetupRWS(test.RWSType)

				err := RWS.RemoveWalks(context.Background(), 0, 69)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("RemoveWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		RWS := SetupRWS("triangle")
		nodeIDs := []uint32{0, 1, 2}
		walkIDs := []uint32{0, 1}
		expectedTotalVisits := 3

		if err := RWS.RemoveWalks(context.Background(), walkIDs...); err != nil {
			t.Fatalf("RemoveWalks(%d): expected nil, got %v", walkIDs, err)
		}

		for _, walkID := range walkIDs {
			// check the walk has been removed from the WalkIndex
			if _, exists := RWS.WalkIndex[walkID]; exists {
				t.Fatalf("RemoveWalk(%d): the walk %v should have been removed", walkID, RWS.WalkIndex[walkID])
			}
		}

		// check the walkID has been removed from each node
		expectedWalkSet := mapset.NewSet[uint32](2)
		for _, nodeID := range nodeIDs {
			walkSet := RWS.walksVisiting[nodeID]

			if !walkSet.Equal(expectedWalkSet) {
				t.Errorf("Expected walkset %v, got %v", expectedWalkSet, walkSet)
			}
		}

		// check that the total visits have been decreased by len(walk)
		if RWS.totalVisits != expectedTotalVisits {
			t.Errorf("RemoveWalk(): expected totalVisits = %v, got %v", expectedTotalVisits, RWS.totalVisits)
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
				expectedError: models.ErrNilRWS,
			},
			{
				name:          "empty RWS",
				RWSType:       "empty",
				walkID:        0,
				expectedError: models.ErrWalkNotFound,
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
				err := RWS.PruneWalk(context.Background(), test.walkID, 2)

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
		expectedTotalVisits := 1

		if err := RWS.PruneWalk(context.Background(), walkID, 1); err != nil {
			t.Fatalf("PruneWalk(): expected nil, got %v", err)
		}

		// check the walk was pruned
		if !reflect.DeepEqual(RWS.WalkIndex[walkID], expectedPrunedWalk) {
			t.Errorf("PruneWalk(): expected %v, got %v", expectedPrunedWalk, RWS.WalkIndex[walkID])
		}

		// check the walk remains
		walkSet0 := RWS.walksVisiting[0]
		if !walkSet0.Equal(mapset.NewSet[uint32](0)) {
			t.Errorf("PruneWalk(): expected {{0}}, got %v", walkSet0)
		}

		// check the walks was removed
		walkSet1 := RWS.walksVisiting[1]
		if !walkSet1.IsEmpty() {
			t.Errorf("PruneWalk(): expected empty set, got %v", walkSet1)
		}

		// check the totalVisits
		if RWS.totalVisits != expectedTotalVisits {
			t.Errorf("PruneWalk(): expected total visits %v, got %v", expectedTotalVisits, RWS.totalVisits)
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
				expectedError: models.ErrNilRWS,
			},
			{
				name:          "empty RWS",
				RWSType:       "empty",
				walkID:        0,
				expectedError: models.ErrWalkNotFound,
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
				err := RWS.GraftWalk(context.Background(), test.walkID, []uint32{1})

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("GraftWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		RWS := SetupRWS("simple")
		walkID := uint32(0)
		walkSegment := []uint32{2, 3, 4}
		expectedGraftedWalk := models.RandomWalk{0, 1, 2, 3, 4}
		expectedTotalVisits := len(expectedGraftedWalk)
		expectedWalkSet := mapset.NewSet[uint32](0)

		if err := RWS.GraftWalk(context.Background(), walkID, walkSegment); err != nil {
			t.Fatalf("GraftWalk(): expected nil, got %v", err)
		}

		// check the walk has been grafted
		if !reflect.DeepEqual(RWS.WalkIndex[0], expectedGraftedWalk) {
			t.Fatalf("GraftWalk(): expected %v, got %v", expectedGraftedWalk, RWS.WalkIndex[0])
		}

		// check if the walk is present in all walkSets
		for _, nodeID := range expectedGraftedWalk {

			walkSet := RWS.walksVisiting[nodeID]
			if !walkSet.Equal(expectedWalkSet) {
				t.Errorf("GraftWalk(): nodeID = %d; expected %v, got %v", nodeID, expectedWalkSet, walkSet)
			}
		}

		// check the total visits
		if RWS.totalVisits != expectedTotalVisits {
			t.Errorf("GraftWalk(): expected total visits %v, got %v", expectedTotalVisits, RWS.totalVisits)
		}
	})
}

func TestInterface(t *testing.T) {
	var _ models.RandomWalkStore = &RandomWalkStore{}
}
