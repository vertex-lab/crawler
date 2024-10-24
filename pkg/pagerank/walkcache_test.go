package pagerank

import (
	"errors"
	"reflect"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestCheckEmpty(t *testing.T) {

	testCases := []struct {
		name          string
		WCType        string
		nodeID        uint32
		expectedError error
	}{
		{
			name:          "nil WC",
			WCType:        "nil",
			nodeID:        0,
			expectedError: ErrNilWCPointer,
		},
		{
			name:          "node already in WC",
			WCType:        "one-node0",
			nodeID:        0,
			expectedError: ErrNonEmptyNodeWalkSlice,
		},
		{
			name:          "valid",
			WCType:        "one-node0",
			nodeID:        1,
			expectedError: nil,
		},
	}

	for _, test := range testCases {

		t.Run(test.name, func(t *testing.T) {

			WC := SetupWC(test.WCType)
			err := WC.CheckEmpty(test.nodeID)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("CheckEmpty(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestLoad(t *testing.T) {

	testCases := []struct {
		name          string
		RWMType       string
		WCType        string
		nodeID        uint32
		walksNum      int
		expectedLen   int
		expectedError error
	}{
		{
			name:          "nil RWM",
			RWMType:       "nil",
			WCType:        "empty",
			nodeID:        0,
			walksNum:      100,
			expectedLen:   100,
			expectedError: walks.ErrNilRWMPointer,
		},
		{
			name:          "empty RWM",
			RWMType:       "empty",
			WCType:        "empty",
			nodeID:        0,
			walksNum:      100,
			expectedLen:   100,
			expectedError: walks.ErrEmptyRWM,
		},
		{
			name:          "node not found RWM",
			RWMType:       "one-node1",
			WCType:        "empty",
			nodeID:        0,
			walksNum:      100,
			expectedLen:   100,
			expectedError: walks.ErrNodeNotFoundRWM,
		},
		{
			name:          "nil WC",
			RWMType:       "one-node1",
			WCType:        "nil",
			nodeID:        0,
			walksNum:      100,
			expectedLen:   100,
			expectedError: ErrNilWCPointer,
		},
		{
			name:          "node already in WC",
			RWMType:       "one-node0",
			WCType:        "one-node0",
			nodeID:        0,
			walksNum:      100,
			expectedLen:   100,
			expectedError: ErrNonEmptyNodeWalkSlice,
		},
		{
			name:          "valid, negative walksNum",
			RWMType:       "triangle",
			WCType:        "empty",
			nodeID:        0,
			walksNum:      -1,
			expectedLen:   3,
			expectedError: nil,
		},
		{
			name:          "valid, positive walksNum",
			RWMType:       "triangle",
			WCType:        "empty",
			nodeID:        0,
			walksNum:      1,
			expectedLen:   1,
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
			if test.expectedError == nil {

				if len(WC.NodeWalkSlice[test.nodeID]) != test.expectedLen {
					t.Errorf("Load(): expected %v, got %v", test.expectedLen, WC.NodeWalkSlice[test.nodeID])
				}
			}
		})
	}
}

func TestNextWalk(t *testing.T) {

	testCases := []struct {
		name          string
		WCType        string
		nodeID        uint32
		expectedWalk  []uint32
		expectedError error
	}{
		{
			name:          "nil WC",
			WCType:        "nil",
			nodeID:        0,
			expectedError: ErrNilWCPointer,
		},
		{
			name:          "node not in WC",
			WCType:        "one-node0",
			nodeID:        1,
			expectedError: ErrEmptyNodeWalkSlice,
		},
		{
			name:          "all walks used for node 0",
			WCType:        "all-used",
			nodeID:        0,
			expectedWalk:  nil,
			expectedError: nil,
		},
		{
			name:          "triangle walks, cut",
			WCType:        "triangle",
			nodeID:        0,
			expectedWalk:  []uint32{1},
			expectedError: nil,
		},
	}

	for _, test := range testCases {

		t.Run(test.name, func(t *testing.T) {

			WC := SetupWC(test.WCType)
			walk, err := WC.NextWalk(test.nodeID)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NextWalk(): expected %v got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walk, test.expectedWalk) {
				t.Errorf("NextWalk(): expected %v got %v", test.expectedWalk, walk)
			}

		})
	}
}
