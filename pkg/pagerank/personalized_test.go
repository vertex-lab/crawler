package pagerank

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

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

func TestReached(t *testing.T) {

	testCases := []struct {
		name            string
		targetLength    int
		expectedReached bool
	}{
		{
			name:            "target Lenght reached",
			targetLength:    0,
			expectedReached: true,
		},
		{
			name:            "target Lenght not reached",
			targetLength:    10,
			expectedReached: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			pWalk := NewPersonalizedWalk(0, test.targetLength)
			reached := pWalk.Reached(test.targetLength)
			if reached != test.expectedReached {
				t.Errorf("Reached(): expected %v, got %v", test.expectedReached, reached)
			}
		})
	}
}

func TestReset(t *testing.T) {

	testCases := []struct {
		name            string
		pWalkType       string
		expectedNodeIDs []uint32
	}{
		{
			name:            "one-node0",
			pWalkType:       "one-node0",
			expectedNodeIDs: []uint32{0},
		},
		{
			name:            "triangle",
			pWalkType:       "triangle",
			expectedNodeIDs: []uint32{0, 1, 2},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			pWalk := SetupPWalk(test.pWalkType, 10)
			pWalk.Reset()

			if pWalk.currentNodeID != pWalk.startingNodeID {
				t.Errorf("Reset(): expected %v, got %v", pWalk.startingNodeID, pWalk.currentNodeID)
			}

			if !reflect.DeepEqual(pWalk.currentWalk, []uint32{pWalk.startingNodeID}) {
				t.Errorf("Reset(): expected %v, got %v", []uint32{pWalk.startingNodeID}, pWalk.currentWalk)
			}

			if !reflect.DeepEqual(pWalk.nodeIDs, test.expectedNodeIDs) {
				t.Errorf("Reset(): expected %v, got %v", test.expectedNodeIDs, pWalk.nodeIDs)
			}
		})
	}
}

func TestAppendNode(t *testing.T) {

	testCases := []struct {
		name                string
		pWalkType           string
		nextNodeID          uint32
		expectedCurrentWalk []uint32
	}{
		{
			name:                "one-node0",
			pWalkType:           "one-node0",
			nextNodeID:          1,
			expectedCurrentWalk: []uint32{0, 1},
		},
		{
			name:                "triangle",
			pWalkType:           "triangle",
			nextNodeID:          3,
			expectedCurrentWalk: []uint32{0, 1, 2, 3},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			pWalk := SetupPWalk(test.pWalkType, 10)
			pWalk.AppendNode(test.nextNodeID)

			if pWalk.currentNodeID != test.nextNodeID {
				t.Errorf("AppendNode(): expected %v, got %v", test.nextNodeID, pWalk.currentNodeID)
			}

			if !reflect.DeepEqual(pWalk.currentWalk, test.expectedCurrentWalk) {
				t.Errorf("AppendNode(): expected %v, got %v", test.expectedCurrentWalk, pWalk.currentWalk)
			}
		})
	}
}

func TestAppendWalkSegment(t *testing.T) {

	testCases := []struct {
		name            string
		pWalkType       string
		walkSegment     []uint32
		expectedNodeIDs []uint32
	}{
		{
			name:            "one-node0",
			pWalkType:       "one-node0",
			walkSegment:     []uint32{1, 2},
			expectedNodeIDs: []uint32{0, 1, 2},
		},
		{
			name:            "triangle",
			pWalkType:       "triangle",
			walkSegment:     []uint32{3, 1},
			expectedNodeIDs: []uint32{0, 1, 2, 3},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			pWalk := SetupPWalk(test.pWalkType, 10)
			pWalk.AppendWalk(test.walkSegment)

			if !reflect.DeepEqual(pWalk.nodeIDs, test.expectedNodeIDs) {
				t.Errorf("AppendNode(): expected %v, got %v", test.expectedNodeIDs, pWalk.currentWalk)
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
		expectedVisits map[uint32]int
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
			expectedError:  nil,
		},
		{
			name:           "single walk added",
			DBType:         "simple",
			RWMType:        "simple",
			startingNodeID: 0,
			requiredLenght: 1,
			expectedVisits: map[uint32]int{0: 1, 1: 1},
			expectedError:  nil,
		},
		{
			name:           "multiple walks added",
			DBType:         "triangle",
			RWMType:        "triangle",
			startingNodeID: 0,
			requiredLenght: 11,
			expectedVisits: map[uint32]int{0: 4, 1: 4, 2: 3},
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

			// check the visits match the expected ones.
			// Note: Order of visits cannot be inforced due to walkSet.Iter()
			if test.expectedVisits != nil {

				// count the visits
				visits := make(map[uint32]int, 3)
				for _, nodeID := range pWalk {
					visits[nodeID]++
				}

				for _, nodeID := range []uint32{0, 1, 2} {
					if visits[nodeID] != test.expectedVisits[nodeID] {
						t.Errorf("personalizedWalk(): expected %v, got %v", test.expectedVisits, pWalk)
					}
				}
			}
		})
	}
}

func TestPersonalizedPagerank(t *testing.T) {

	t.Run("simple errors", func(t *testing.T) {

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
				RWMType:       "empty",
				DBType:        "one-node0",
				nodeID:        0,
				topK:          5,
				expectedError: walks.ErrEmptyRWM,
			},
			{
				name:          "node not in the RWM",
				DBType:        "triangle",
				RWMType:       "one-node0",
				nodeID:        1,
				topK:          5,
				expectedError: walks.ErrNodeNotFoundRWM,
			},
			{
				name:          "invalid topK",
				DBType:        "one-node0",
				RWMType:       "one-node0",
				nodeID:        0,
				topK:          0,
				expectedError: ErrInvalidTopN,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				DB := mock.SetupDB(test.DBType)
				RWM := walks.SetupRWM(test.RWMType)

				_, err := Personalized(DB, RWM, test.nodeID, test.topK)

				if !errors.Is(err, test.expectedError) {
					t.Errorf("Personalized(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("simple graphs", func(t *testing.T) {

		const alpha = 0.85
		const walksPerNode = 1000
		const expectedError = 0.01

		testCases := []struct {
			name       string
			DBType     string
			nodeID     uint32
			topK       uint16
			expectedPP PagerankMap
		}{
			{
				name:       "dandling",
				DBType:     "dandling",
				nodeID:     0,
				topK:       5,
				expectedPP: PagerankMap{0: 1.0},
			},
			{
				name:       "simple",
				DBType:     "simple",
				nodeID:     0,
				topK:       5,
				expectedPP: PagerankMap{0: 0.5405399037185797, 1: 0.4594600962814203, 2: 0.0},
			},
			{
				name:       "triangle",
				DBType:     "triangle",
				nodeID:     0,
				topK:       5,
				expectedPP: PagerankMap{0: 0.3887264613719621, 1: 0.3304174921661678, 2: 0.28085604646187007},
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				DB := mock.SetupDB(test.DBType)
				RWM, _ := walks.NewRWM(alpha, walksPerNode)
				RWM.GenerateAll(DB)

				pp, err := Personalized(DB, RWM, test.nodeID, test.topK)
				if err != nil {
					t.Fatalf("Personalized(): expected nil, got %v", err)
				}

				distance := Distance(test.expectedPP, pp)
				if distance > expectedError {
					t.Errorf("Personalized(): expected distance %v, got %v\n", expectedError, distance)
					t.Errorf("Personalized(): expected %v\n, got %v", test.expectedPP, pp)
				}
			})
		}
	})

	t.Run("fuzzy test", func(t *testing.T) {

		nodesNum := 200
		edgesPerNode := 100
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))

		DB := mock.GenerateMockDB(nodesNum, edgesPerNode, rng)
		RWM, _ := walks.NewRWM(0.85, 10)
		RWM.GenerateAll(DB)

		if _, err := Personalized(DB, RWM, 0, 100); err != nil {
			t.Fatalf("Personalized() expected nil, got %v", err)
		}

		// doing it two times to check that it donesn't change the DB or RWM
		if _, err := Personalized(DB, RWM, 0, 100); err != nil {
			t.Errorf("Personalized() expected nil, got %v", err)
		}
	})
}

func BenchmarkPersonalized(b *testing.B) {
	nodesNum := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesNum, edgesPerNode, rng)

	for _, walksPerNode := range []uint16{1, 10, 100, 1000} {
		RWM, _ := walks.NewRWM(0.85, walksPerNode)
		RWM.GenerateAll(DB)

		b.Run(fmt.Sprintf("walksPerNode: %d", walksPerNode), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Personalized(DB, RWM, 0, 100); err != nil {
					b.Fatalf("Benchmark failed: %v", err)
				}
			}
		})
	}
}
