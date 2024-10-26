package pagerank

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestCheckInputs(t *testing.T) {

	testCases := []struct {
		name          string
		DBType        string
		RWMType       string
		nodeID        uint32
		topK          uint16
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			RWMType:       "one-node0",
			nodeID:        0,
			topK:          5,
			expectedError: graph.ErrNilDatabasePointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			RWMType:       "one-node0",
			nodeID:        0,
			topK:          5,
			expectedError: graph.ErrDatabaseIsEmpty,
		},
		{
			name:          "nil RWM",
			DBType:        "one-node0",
			RWMType:       "nil",
			nodeID:        0,
			topK:          5,
			expectedError: walks.ErrNilRWMPointer,
		},
		{
			name:          "empty RWM",
			DBType:        "one-node0",
			RWMType:       "empty",
			nodeID:        0,
			topK:          5,
			expectedError: walks.ErrEmptyRWM,
		},
		{
			name:          "node not in DB",
			DBType:        "one-node0",
			RWMType:       "one-node1",
			nodeID:        1,
			topK:          5,
			expectedError: graph.ErrNodeNotFoundDB,
		},
		{
			name:          "node not in RWM",
			DBType:        "one-node1",
			RWMType:       "one-node0",
			nodeID:        1,
			topK:          5,
			expectedError: walks.ErrNodeNotFoundRWM,
		},
		{
			name:          "invalid topK",
			DBType:        "one-node0",
			RWMType:       "one-node0",
			topK:          0,
			expectedError: ErrInvalidTopN,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := mock.SetupDB(test.DBType)
			RWM := walks.SetupRWM(test.RWMType)

			err := checkInputs(DB, RWM, test.nodeID, test.topK)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("Pagerank(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestCountAndNormalize(t *testing.T) {
	testCases := []struct {
		name       string
		longWalk   []uint32
		alpha      float32
		expectedPP PagerankMap
	}{
		{
			name:       "nil walk",
			longWalk:   nil,
			alpha:      0.85,
			expectedPP: PagerankMap{},
		},
		{
			name:       "empty walk",
			longWalk:   []uint32{},
			alpha:      0.85,
			expectedPP: PagerankMap{},
		},
		{
			name:     "normal walk",
			longWalk: []uint32{0, 1, 2, 0, 1},
			alpha:    0.85,
			expectedPP: PagerankMap{
				0: 2.0 / 5.0,
				1: 2.0 / 5.0,
				2: 1.0 / 5.0,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			pp := countAndNormalize(test.longWalk, test.alpha)

			if Distance(test.expectedPP, pp) > 1e-10 {
				t.Fatalf("countAndNormalize(): expected %v, got %v", test.expectedPP, pp)
			}
		})
	}
}

func TestPersonalizedWalk(t *testing.T) {
	testCases := []struct {
		name           string
		DBType         string
		RWMType        string
		startingNodeID uint32
		requiredLenght int
		expectedWalk   [][]uint32
		expectedError  error
	}{
		{
			name:           "empty RWM",
			DBType:         "one-node0",
			RWMType:        "empty",
			startingNodeID: 0,
			requiredLenght: 5,
			expectedError:  walks.ErrEmptyRWM,
		},
		{
			name:           "node not found RWM",
			DBType:         "one-node0",
			RWMType:        "one-node1",
			startingNodeID: 0,
			requiredLenght: 5,
			expectedError:  walks.ErrNodeNotFoundRWM,
		},
		{
			name:           "required Lenght = 0; empty slice returned",
			DBType:         "one-node0",
			RWMType:        "one-node0",
			startingNodeID: 0,
			requiredLenght: 0,
			expectedWalk:   [][]uint32{},
			expectedError:  nil,
		},
		{
			name:           "single walk added",
			DBType:         "triangle",
			RWMType:        "triangle",
			startingNodeID: 0,
			requiredLenght: 1,
			expectedWalk:   [][]uint32{{0, 1, 2}},
			expectedError:  nil,
		},
		{
			name:           "multiple walks added",
			DBType:         "triangle",
			RWMType:        "triangle",
			startingNodeID: 0,
			requiredLenght: 20,
			expectedWalk:   [][]uint32{{0, 1, 2}},
			expectedError:  nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := mock.SetupDB(test.DBType)
			RWM := walks.SetupRWM(test.RWMType)
			rng := rand.New(rand.NewSource(42))

			pWalk, err := personalizedWalk(DB, RWM, test.startingNodeID, test.requiredLenght, rng)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("personalizedWalk(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(pWalk, test.expectedWalk) {
				t.Errorf("personalizedWalk(): expected %v, got %v", test.expectedWalk, pWalk)
			}
		})
	}
}

// func TestPersonalizedPagerank(t *testing.T) {

// 	t.Run("simple errors", func(t *testing.T) {

// 		testCases := []struct {
// 			name          string
// 			DBType        string
// 			RWMType       string
// 			nodeID        uint32
// 			topK          uint16
// 			expectedError error
// 		}{
// 			{
// 				name:          "nil DB",
// 				DBType:        "nil",
// 				RWMType:       "one-node0",
// 				nodeID:        0,
// 				topK:          5,
// 				expectedError: graph.ErrNilDatabasePointer,
// 			},
// 			{
// 				name:          "empty DB",
// 				DBType:        "empty",
// 				RWMType:       "one-node0",
// 				nodeID:        0,
// 				topK:          5,
// 				expectedError: graph.ErrDatabaseIsEmpty,
// 			},
// 			{
// 				name:          "nil RWM",
// 				DBType:        "one-node0",
// 				RWMType:       "nil",
// 				nodeID:        0,
// 				topK:          5,
// 				expectedError: walks.ErrNilRWMPointer,
// 			},
// 			{
// 				name:          "empty RWM",
// 				RWMType:       "empty",
// 				DBType:        "one-node0",
// 				nodeID:        0,
// 				topK:          5,
// 				expectedError: walks.ErrEmptyRWM,
// 			},
// 			{
// 				name:          "node not in the RWM",
// 				DBType:        "triangle",
// 				RWMType:       "one-node0",
// 				nodeID:        1,
// 				topK:          5,
// 				expectedError: walks.ErrNodeNotFoundRWM,
// 			},
// 			{
// 				name:          "invalid topK",
// 				DBType:        "one-node0",
// 				RWMType:       "one-node0",
// 				nodeID:        0,
// 				topK:          0,
// 				expectedError: ErrInvalidTopN,
// 			},
// 		}

// 		for _, test := range testCases {
// 			t.Run(test.name, func(t *testing.T) {

// 				DB := mock.SetupDB(test.DBType)
// 				RWM := walks.SetupRWM(test.RWMType)

// 				_, err := Personalized(DB, RWM, test.nodeID, test.topK)

// 				if !errors.Is(err, test.expectedError) {
// 					t.Errorf("Pagerank(): expected %v, got %v", test.expectedError, err)
// 				}
// 			})
// 		}
// 	})

// 	t.Run("valid", func(t *testing.T) {

// 	})

// }
