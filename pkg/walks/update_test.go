package walks

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"slices"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
)

func TestContainsInvalidStep(t *testing.T) {
	testCases := []struct {
		name             string
		walk             models.RandomWalk
		expectedCutIndex int
		expectedContains bool
	}{
		{
			name:             "nil random walk",
			walk:             nil,
			expectedCutIndex: -1,
			expectedContains: false,
		},
		{
			name:             "empty random walk",
			walk:             models.RandomWalk{},
			expectedCutIndex: -1,
			expectedContains: false,
		},
		{
			name:             "normal random walk, no updates",
			walk:             models.RandomWalk{1},
			expectedCutIndex: -1,
			expectedContains: false,
		},
		{
			name:             "normal random walk, updates",
			walk:             models.RandomWalk{1, 2, 3},
			expectedCutIndex: 1,
			expectedContains: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			cutIndex, contains := containsInvalidStep(test.walk, 1, []uint32{2})

			if contains != test.expectedContains {
				t.Fatalf("containsInvalidStep(): expected %v, got %v", test.expectedContains, contains)
			}

			if cutIndex != test.expectedCutIndex {
				t.Errorf("containsInvalidStep(): expected %v, got %v", test.expectedCutIndex, cutIndex)
			}
		})
	}
}

func TestUpdateRemovedNodes(t *testing.T) {
	ctx := context.Background()
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name            string
			DBType          string
			RWMType         string
			removed         []uint32
			expectedError   error
			expectedUpdated int
		}{
			{
				name:            "nil RWM",
				DBType:          "one-node0",
				RWMType:         "nil",
				removed:         []uint32{0},
				expectedError:   models.ErrNilRWS,
				expectedUpdated: 0,
			},
			{
				name:            "empty RWM",
				DBType:          "one-node0",
				RWMType:         "empty",
				removed:         []uint32{0},
				expectedError:   nil,
				expectedUpdated: 0,
			},
			{
				name:            "node not found in the RWM",
				DBType:          "one-node0",
				RWMType:         "one-node1",
				removed:         []uint32{1},
				expectedError:   nil,
				expectedUpdated: 0,
			},
			{
				name:            "empty removedFollows",
				DBType:          "triangle",
				RWMType:         "triangle",
				removed:         []uint32{},
				expectedError:   nil,
				expectedUpdated: 0,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				DB := mockdb.SetupDB(test.DBType)
				RWS := mockstore.SetupRWS(test.RWMType)
				updated, err := updateRemovedNodes(ctx, rng, DB, RWS, 0, test.removed, []uint32{2})

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("updateRemovedNodes(): expected %v, got %v", test.expectedError, err)
				}

				if updated != test.expectedUpdated {
					t.Fatalf("updateRemovedNodes(): expected %v, got %v", test.expectedUpdated, updated)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		DB := mockdb.SetupDB("triangle")
		RWS := mockstore.SetupRWS("triangle")

		nodeID := uint32(0)
		removeFollows := []uint32{1}

		// update the DB
		commonFollows := []uint32{2}
		DB.Follow[nodeID] = mapset.NewSet[uint32](commonFollows...)

		rng := rand.New(rand.NewSource(5))
		expectedUpdated := 2
		expectedWalks := map[uint32]map[uint32]models.RandomWalk{
			0: {
				0: {0, 2},
				1: {1, 2, 0},
				2: {2, 0},
			},
			1: {
				1: {1, 2, 0},
			},
			2: {
				0: {0, 2},
				1: {1, 2, 0},
				2: {2, 0},
			},
		}

		updated, err := updateRemovedNodes(ctx, rng, DB, RWS, nodeID, removeFollows, commonFollows)
		if err != nil {
			t.Fatalf("updateRemovedNodes(): expected nil, got %v", err)
		}

		if updated != expectedUpdated {
			t.Fatalf("updateRemovedNodes(): expected updated %v, got %v", expectedUpdated, updated)
		}

		for nodeID, expectedWalk := range expectedWalks {
			walkIDs, err := RWS.WalksVisiting(ctx, -1, nodeID)
			if err != nil {
				t.Fatalf("WalksVisiting(%d): expected nil, got %v", nodeID, err)
			}

			walks, err := RWS.Walks(ctx, walkIDs...)
			if err != nil {
				t.Fatalf("Walks(%d): expected nil, got %v", nodeID, err)
			}

			if len(walkIDs) != len(expectedWalk) {
				t.Fatalf("expected %v, got %v, %v", expectedWalk, walkIDs, walks)
			}

			for i, ID := range walkIDs {
				if !reflect.DeepEqual(walks[i], expectedWalk[ID]) {
					t.Fatalf("expected %v, got %v, %v", expectedWalk, walkIDs, walks)
				}
			}
		}
	})
}

func TestUpdateAddedNodes(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name            string
			DBType          string
			RWMType         string
			addedFollows    []uint32
			newOutDegree    int
			expectedError   error
			expectedUpdated int
		}{
			{
				name:            "nil RWM",
				DBType:          "one-node0",
				RWMType:         "nil",
				addedFollows:    []uint32{3},
				newOutDegree:    1,
				expectedError:   models.ErrNilRWS,
				expectedUpdated: 0,
			},
			{
				name:            "empty RWM",
				DBType:          "one-node0",
				RWMType:         "empty",
				addedFollows:    []uint32{3},
				newOutDegree:    1,
				expectedError:   nil,
				expectedUpdated: 0,
			},
			{
				name:            "node not found in the RWM",
				DBType:          "one-node0",
				RWMType:         "one-node1",
				addedFollows:    []uint32{3},
				newOutDegree:    1,
				expectedError:   nil,
				expectedUpdated: 0,
			},
			{
				name:            "empty addedFollows",
				DBType:          "triangle",
				RWMType:         "triangle",
				addedFollows:    []uint32{},
				newOutDegree:    1,
				expectedError:   nil,
				expectedUpdated: 0,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				DB := mockdb.SetupDB(test.DBType)
				RWS := mockstore.SetupRWS(test.RWMType)

				updated, err := updateAddedNodes(ctx, rng, DB, RWS, 0, test.addedFollows, test.newOutDegree)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("updateRemovedNodes(): expected %v, got %v", test.expectedError, err)
				}

				if updated != test.expectedUpdated {
					t.Fatalf("updateRemovedNodes(): expected %v, got %v", test.expectedUpdated, updated)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		ctx := context.Background()
		rng := rand.New(rand.NewSource(5))
		DB := mockdb.SetupDB("simple")
		RWS := mockstore.SetupRWS("simple")
		nodeID := uint32(0)
		addedFollows := []uint32{2}

		// update the DB
		currentFollows := []uint32{2}
		DB.Follow[nodeID] = mapset.NewSet[uint32](currentFollows...)

		expectedUpdated := 1
		expectedWalks := map[uint32]map[uint32]models.RandomWalk{
			0: {
				0: {0, 2},
			},
			1: nil,
			2: {
				0: {0, 2},
			},
		}

		updated, err := updateAddedNodes(ctx, rng, DB, RWS, nodeID, addedFollows, len(currentFollows))
		if err != nil {
			t.Fatalf("updateAddedNodes(): expected nil, got %v", err)
		}

		if updated != expectedUpdated {
			t.Fatalf("updateAddedNodes(): expected updated %v, got %v", expectedUpdated, updated)
		}

		for nodeID, expectedWalk := range expectedWalks {
			walkIDs, err := RWS.WalksVisiting(ctx, -1, nodeID)
			if err != nil {
				t.Fatalf("WalksVisiting(%d): expected nil, got %v", nodeID, err)
			}

			walks, err := RWS.Walks(ctx, walkIDs...)
			if err != nil {
				t.Fatalf("Walks(%d): expected nil, got %v", nodeID, err)
			}

			if len(walkIDs) != len(expectedWalk) {
				t.Fatalf("expected %v, got %v, %v", expectedWalk, walkIDs, walks)
			}

			for i, ID := range walkIDs {
				if !reflect.DeepEqual(walks[i], expectedWalk[ID]) {
					t.Fatalf("expected %v, got %v, %v", expectedWalk, walkIDs, walks)
				}
			}
		}
	})
}

func TestUpdate(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name            string
			DBType          string
			RWMType         string
			nodeID          uint32
			oldFollows      []uint32
			currentFollows  []uint32
			expectedError   error
			expectedUpdated int
		}{
			{
				name:            "nil DB",
				DBType:          "nil",
				RWMType:         "triangle",
				nodeID:          0,
				oldFollows:      []uint32{0},
				currentFollows:  []uint32{1},
				expectedError:   models.ErrNilDB,
				expectedUpdated: 0,
			},
			{
				name:            "empty DB",
				DBType:          "empty",
				RWMType:         "triangle",
				nodeID:          0,
				oldFollows:      []uint32{0},
				currentFollows:  []uint32{1},
				expectedError:   models.ErrNodeNotFoundDB,
				expectedUpdated: 0,
			},
			{
				name:            "nil RWM",
				DBType:          "one-node0",
				RWMType:         "nil",
				nodeID:          0,
				oldFollows:      []uint32{0},
				currentFollows:  []uint32{1},
				expectedError:   models.ErrNilRWS,
				expectedUpdated: 0,
			},
			{
				name:            "empty RWM",
				DBType:          "one-node0",
				RWMType:         "empty",
				nodeID:          0,
				oldFollows:      []uint32{0},
				currentFollows:  []uint32{1},
				expectedError:   nil,
				expectedUpdated: 0,
			},
			{
				name:            "node not found in the DB",
				DBType:          "one-node1",
				RWMType:         "one-node1",
				nodeID:          0,
				oldFollows:      []uint32{0},
				currentFollows:  []uint32{1},
				expectedError:   models.ErrNodeNotFoundDB,
				expectedUpdated: 0,
			},
			{
				name:            "node not found in the RWM",
				DBType:          "one-node0",
				RWMType:         "one-node1",
				nodeID:          0,
				oldFollows:      []uint32{0},
				currentFollows:  []uint32{1},
				expectedError:   nil,
				expectedUpdated: 0,
			},
			{
				name:            "oldFollows == currentFollows",
				DBType:          "triangle",
				RWMType:         "triangle",
				nodeID:          0,
				oldFollows:      []uint32{1},
				currentFollows:  []uint32{1},
				expectedError:   nil,
				expectedUpdated: 0,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				DB := mockdb.SetupDB(test.DBType)
				RWS := mockstore.SetupRWS(test.RWMType)

				removed, common, added := sliceutils.Partition(test.oldFollows, test.currentFollows)
				updated, err := Update(ctx, DB, RWS, test.nodeID, removed, common, added)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("Update(): expected %v, got %v", test.expectedError, err)
				}

				if updated != test.expectedUpdated {
					t.Fatalf("Update(): expected %v, got %v", test.expectedUpdated, updated)
				}
			})
		}
	})

	t.Run("fuzzy test", func(t *testing.T) {
		ctx := context.Background()
		nodesNum := 200
		edgesPerNode := 20

		// generate the first DB
		rng1 := rand.New(rand.NewSource(time.Now().UnixNano()))
		DB1 := mockdb.GenerateDB(nodesNum, edgesPerNode, rng1)
		RWS, _ := mockstore.NewRWS(0.85, 10)

		if err := GenerateAll(ctx, DB1, RWS); err != nil {
			t.Fatalf("GenerateAll(): expected nil got %v", err)
		}

		// generate another DB
		rng2 := rand.New(rand.NewSource(time.Now().UnixNano()))
		DB2 := mockdb.GenerateDB(nodesNum, edgesPerNode, rng2)

		// update one node at the time
		for nodeID := uint32(0); nodeID < uint32(nodesNum); nodeID++ {
			oldFollows := DB1.Follow[nodeID]
			newFollows := DB2.Follow[nodeID]
			DB1.Follow[nodeID] = newFollows

			removed, common, added := sliceutils.Partition(oldFollows.ToSlice(), newFollows.ToSlice())

			if _, err := Update(ctx, DB1, RWS, nodeID, removed, common, added); err != nil {
				t.Fatalf("Update(%d): expected nil, got %v", nodeID, err)
			}
		}

		// check that each walk in the Walks of nodeID contains nodeID
		for nodeID := uint32(0); nodeID < uint32(nodesNum); nodeID++ {
			walkIDs, err := RWS.WalksVisiting(ctx, -1, nodeID)
			if err != nil {
				t.Fatalf("WalksVisiting(%d): expected nil, got %v", nodeID, err)
			}

			walks, err := RWS.Walks(ctx, walkIDs...)
			if err != nil {
				t.Fatalf("Walks(): expected nil, got %v", err)
			}

			for _, walk := range walks {
				if !slices.Contains(walk, nodeID) {
					t.Fatalf("walk %v should contain nodeID = %d", walk, nodeID)
				}
			}
		}
	})
}
