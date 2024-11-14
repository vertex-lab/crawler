package pagerank

import (
	"errors"
	"reflect"
	"testing"

	mockdb "github.com/pippellia-btc/Nostrcrawler/pkg/database/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	mockstore "github.com/pippellia-btc/Nostrcrawler/pkg/store/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/utils/sliceutils"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestValidate(t *testing.T) {
	testCases := []struct {
		name          string
		WCType        string
		expectedError error
	}{
		{
			name:          "nil WC",
			WCType:        "nil",
			expectedError: ErrNilWCPointer,
		},
		{
			name:          "empty WC",
			WCType:        "empty",
			expectedError: ErrEmptyWC,
		},
		{
			name:          "non empty WC",
			WCType:        "one-node0",
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			WC := SetupWC(test.WCType)
			err := WC.Validate()

			if !errors.Is(err, test.expectedError) {
				t.Errorf("Validate(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestContainsNode(t *testing.T) {
	testCases := []struct {
		name        string
		WCType      string
		nodeID      uint32
		expectedRes bool
	}{
		{
			name:        "nil WC",
			WCType:      "nil",
			nodeID:      0,
			expectedRes: false,
		},
		{
			name:        "empty WC",
			WCType:      "empty",
			nodeID:      0,
			expectedRes: false,
		},
		{
			name:        "contains node 0",
			WCType:      "one-node0",
			nodeID:      0,
			expectedRes: true,
		},
		{
			name:        "doesn't contain node 0",
			WCType:      "one-node1",
			nodeID:      0,
			expectedRes: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			WC := SetupWC(test.WCType)
			contains := WC.ContainsNode(test.nodeID)

			if contains != test.expectedRes {
				t.Errorf("Contains(): expected %v, got %v", test.expectedRes, contains)
			}
		})
	}
}

func TestFullyUsed(t *testing.T) {
	testCases := []struct {
		name        string
		WCType      string
		nodeID      uint32
		expectedRes bool
	}{
		{
			name:        "nil WC",
			WCType:      "nil",
			nodeID:      0,
			expectedRes: true,
		},
		{
			name:        "empty WC",
			WCType:      "empty",
			nodeID:      0,
			expectedRes: true,
		},
		{
			name:        "doesn't contain node 0",
			WCType:      "one-node1",
			nodeID:      0,
			expectedRes: true,
		},
		{
			name:        "some walks left",
			WCType:      "one-node0",
			nodeID:      0,
			expectedRes: false,
		},
		{
			name:        "all used",
			WCType:      "all-used",
			nodeID:      0,
			expectedRes: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			WC := SetupWC(test.WCType)
			contains := WC.FullyUsed(test.nodeID)

			if contains != test.expectedRes {
				t.Errorf("FullyUsed(): expected %v, got %v", test.expectedRes, contains)
			}
		})
	}
}

func TestLoad(t *testing.T) {

	t.Run("simple cases", func(t *testing.T) {
		testCases := []struct {
			name          string
			RWSType       string
			WCType        string
			nodeID        uint32
			limit         int
			expectedWalks []models.RandomWalk
			expectedError error
		}{
			{
				name:          "nil RWS",
				RWSType:       "nil",
				WCType:        "empty",
				nodeID:        0,
				limit:         100,
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "empty RWS",
				RWSType:       "empty",
				WCType:        "empty",
				nodeID:        0,
				limit:         100,
				expectedError: models.ErrEmptyRWS,
			},
			{
				name:          "node not found RWS",
				RWSType:       "one-node1",
				WCType:        "empty",
				nodeID:        0,
				limit:         100,
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "nil WC",
				RWSType:       "one-node1",
				WCType:        "nil",
				nodeID:        0,
				limit:         100,
				expectedError: ErrNilWCPointer,
			},
			{
				name:          "node already in WC",
				RWSType:       "one-node0",
				WCType:        "one-node0",
				nodeID:        0,
				limit:         100,
				expectedError: ErrNodeAlreadyLoadedWC,
			},
			{
				name:          "valid, negative limit",
				RWSType:       "triangle",
				WCType:        "empty",
				nodeID:        0,
				limit:         -1,
				expectedWalks: []models.RandomWalk{{1}, {1, 2}},
				expectedError: nil,
			},
			{
				name:          "valid",
				RWSType:       "simple",
				WCType:        "empty",
				nodeID:        0,
				limit:         1,
				expectedWalks: []models.RandomWalk{{1}},
				expectedError: nil,
			},
		}

		for _, test := range testCases {

			t.Run(test.name, func(t *testing.T) {
				RWS := mockstore.SetupRWS(test.RWSType)
				WC := SetupWC(test.WCType)
				err := WC.Load(RWS, test.nodeID, test.limit)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("Load(): expected %v, got %v", test.expectedError, err)
				}

				// check if the walks have been correctly added to the WC
				if expectedWalks := test.expectedWalks; expectedWalks != nil {

					if WC.LoadedWalkIDs.Cardinality() != len(expectedWalks) {
						t.Errorf("Load(): expected %v, got len(%v)", len(expectedWalks), WC.LoadedWalkIDs)
					}

					// sort walks in lexographic order
					walks := sliceutils.SortWalks(WC.NodeWalks[test.nodeID])

					if !reflect.DeepEqual(walks, expectedWalks) {
						t.Errorf("Load(): expected %v, got %v", expectedWalks, walks)
					}
				}
			})
		}
	})

	t.Run("multiple loads", func(t *testing.T) {
		nodeIDs := []uint32{0, 1, 2}
		expectedWalks := map[uint32][]models.RandomWalk{
			0: {{1}, {1, 2}},
			1: {{2, 0}},
			2: {},
		}

		RWS := mockstore.SetupRWS("triangle")
		WC := SetupWC("empty")

		// load for all the nodes
		for _, nodeID := range nodeIDs {
			if err := WC.Load(RWS, nodeID, 100); err != nil {
				t.Fatalf("Load(): nodeID = %d; expected nil, got %v", nodeID, err)
			}
		}

		// checked the fetched walks
		if WC.LoadedWalkIDs.Cardinality() != 3 {
			t.Errorf("Load(): expected %v, got len(%v)", 3, WC.LoadedWalkIDs)
		}

		// check each walkSlice (sorted in lexographic order)
		for _, nodeID := range nodeIDs {
			walkSlice := sliceutils.SortWalks(WC.NodeWalks[nodeID])
			if !reflect.DeepEqual(walkSlice, expectedWalks[nodeID]) {
				t.Errorf("Load(): nodeID = %v, expected %v, got %v", nodeID, expectedWalks[nodeID], walkSlice)
			}
		}
	})
}

func TestNextWalk(t *testing.T) {

	testCases := []struct {
		name          string
		WCType        string
		nodeID        uint32
		expectedWalk  models.RandomWalk
		expectedIndex int
		expectedError error
	}{
		{
			name:          "nil WC",
			WCType:        "nil",
			nodeID:        0,
			expectedIndex: 0,
			expectedError: ErrNilWCPointer,
		},
		{
			name:          "empty WC",
			WCType:        "empty",
			nodeID:        0,
			expectedIndex: 0,
			expectedError: ErrEmptyWC,
		},
		{
			name:          "all walks used for node 0",
			WCType:        "all-used",
			nodeID:        0,
			expectedIndex: 1,
			expectedWalk:  nil,
			expectedError: ErrAllWalksUsedWC,
		},
		{
			name:          "triangle walks",
			WCType:        "triangle",
			nodeID:        0,
			expectedIndex: 1,
			expectedWalk:  models.RandomWalk{0, 1, 2},
			expectedError: nil,
		},
	}

	for _, test := range testCases {

		t.Run(test.name, func(t *testing.T) {

			WC := SetupWC(test.WCType)
			walk, err := WC.NextWalk(test.nodeID)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NextWalk(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walk, test.expectedWalk) {
				t.Errorf("NextWalk(): expected %v, got %v", test.expectedWalk, walk)
			}

			if WC != nil && WC.NodeWalkIndex[test.nodeID] != test.expectedIndex {
				t.Errorf("NextWalk(): expected %v, got %v", test.expectedIndex, WC.NodeWalkIndex[test.nodeID])
			}
		})
	}
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

func BenchmarkLoad(b *testing.B) {
	DB := mockdb.SetupDB("triangle")
	RWM, _ := walks.NewRWM("mock", 0.85, 1000)
	RWM.GenerateAll(DB)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		WC := NewWalkCache()
		nodeID := uint32(i % 3)
		if err := WC.Load(RWM.Store, nodeID, 1000); err != nil {
			b.Fatalf("Benchmark Load() failed: %v", err)
		}
	}
}

func BenchmarkNextWalk(b *testing.B) {
	RWS := mockstore.SetupRWS("triangle")
	WC := NewWalkCache()
	WC.Load(RWS, 0, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := WC.NextWalk(0); err != nil {
			b.Fatalf("Benchmark NextWalk(0) failed: %v", err)
		}

		// reset the index to avoid the error
		if (i+1)%2 == 0 {
			WC.NodeWalkIndex[0] = 0
		}
	}
}
