package pagerank

import (
	"errors"
	"reflect"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestCheckEmpty(t *testing.T) {

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
			err := WC.CheckEmpty()

			if !errors.Is(err, test.expectedError) {
				t.Errorf("CheckEmpty(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestContains(t *testing.T) {
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
			contains := WC.Contains(test.nodeID)

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
			RWMType       string
			WCType        string
			nodeID        uint32
			walksNum      int
			expectedWalks [][]uint32
			expectedError error
		}{
			{
				name:          "nil RWM",
				RWMType:       "nil",
				WCType:        "empty",
				nodeID:        0,
				walksNum:      100,
				expectedError: walks.ErrNilRWMPointer,
			},
			{
				name:          "empty RWM",
				RWMType:       "empty",
				WCType:        "empty",
				nodeID:        0,
				walksNum:      100,
				expectedError: walks.ErrEmptyRWM,
			},
			{
				name:          "node not found RWM",
				RWMType:       "one-node1",
				WCType:        "empty",
				nodeID:        0,
				walksNum:      100,
				expectedError: walks.ErrNodeNotFoundRWM,
			},
			{
				name:          "nil WC",
				RWMType:       "one-node1",
				WCType:        "nil",
				nodeID:        0,
				walksNum:      100,
				expectedError: ErrNilWCPointer,
			},
			{
				name:          "node already in WC",
				RWMType:       "one-node0",
				WCType:        "one-node0",
				nodeID:        0,
				walksNum:      100,
				expectedError: ErrNodeAlreadyLoadedWC,
			},
			{
				name:          "valid, negative walksNum",
				RWMType:       "triangle",
				WCType:        "empty",
				nodeID:        0,
				walksNum:      -1,
				expectedWalks: [][]uint32{{1}, {1, 2}},
				expectedError: nil,
			},
			{
				name:          "valid",
				RWMType:       "simple",
				WCType:        "empty",
				nodeID:        0,
				walksNum:      1,
				expectedWalks: [][]uint32{{1}},
				expectedError: nil,
			},
		}

		for _, test := range testCases {

			t.Run(test.name, func(t *testing.T) {

				RWM := walks.SetupRWM(test.RWMType)
				WC := SetupWC(test.WCType)

				err := WC.Load(RWM, test.nodeID, test.walksNum)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("Load(): expected %v, got %v", test.expectedError, err)
				}

				// check if the walks have been correctly added to the WC
				if expectedWalks := test.expectedWalks; expectedWalks != nil {

					if WC.FetchedWalks.Cardinality() != len(expectedWalks) {
						t.Errorf("Load(): expected %v, got len(%v)", len(expectedWalks), WC.FetchedWalks)
					}

					// dereference walks and sort them in lexographic order
					walks := walks.SortWalks(WC.NodeWalkSlice[test.nodeID])

					if !reflect.DeepEqual(walks, expectedWalks) {
						t.Errorf("Load(): expected %v, got %v", expectedWalks, walks)
					}
				}
			})
		}
	})

	t.Run("multiple loads", func(t *testing.T) {

		nodeIDs := []uint32{0, 1, 2}
		expectedWalks := map[uint32][][]uint32{
			0: {{1}, {1, 2}},
			1: {{2, 0}},
			2: {},
		}

		RWM := walks.SetupRWM("triangle")
		WC := SetupWC("empty")

		// load for all the nodes
		for _, nodeID := range nodeIDs {
			if err := WC.Load(RWM, nodeID, 10); err != nil {
				t.Fatalf("Load(): nodeID = %d; expected nil, got %v", nodeID, err)
			}
		}

		// checked the fetched walks
		if WC.FetchedWalks.Cardinality() != 3 {
			t.Errorf("Load(): expected %v, got len(%v)", 3, WC.FetchedWalks)
		}

		for _, nodeID := range nodeIDs {
			walkSlice := walks.SortWalks(WC.NodeWalkSlice[nodeID])
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
		expectedWalk  []uint32
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
			expectedWalk:  []uint32{1, 2},
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
		rWalk         *walks.RandomWalk
		nodeID        uint32
		expectedWalk  []uint32
		expectedError error
	}{
		{
			name:          "empty random walk",
			rWalk:         &walks.RandomWalk{},
			nodeID:        0,
			expectedError: ErrNodeNotInWalk,
		},
		{
			name:          "node not in random walk",
			rWalk:         &walks.RandomWalk{NodeIDs: []uint32{1, 2, 3, 4}},
			nodeID:        0,
			expectedError: ErrNodeNotInWalk,
		},
		{
			name:          "node in random walk",
			rWalk:         &walks.RandomWalk{NodeIDs: []uint32{0, 1, 2, 3}},
			nodeID:        0,
			expectedError: nil,
			expectedWalk:  []uint32{1, 2, 3},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			walk, err := CropWalk(test.rWalk, test.nodeID)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("CropWalk(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walk, test.expectedWalk) {
				t.Errorf("CropWalk(): expected %v, got %v", test.expectedWalk, walk)
			}
		})
	}
}

// ---------------------------------BENCHMARKS---------------------------------

func BenchmarkLoad(b *testing.B) {
	DB := mock.SetupDB("triangle")
	RWM, _ := walks.NewRWM(0.85, 1000)
	RWM.GenerateAll(DB)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		WC := NewWalkCache()
		nodeID := uint32(i % 3)
		if err := WC.Load(RWM, nodeID, 1000); err != nil {
			b.Fatalf("Benchmark Load() failed: %v", err)
		}
	}
}

func BenchmarkNextWalk(b *testing.B) {

	RWM := walks.SetupRWM("triangle")
	WC := NewWalkCache()
	WC.Load(RWM, 0, 10)

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
