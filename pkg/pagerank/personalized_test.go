package pagerank

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/walks"
)

func TestCheckInputs(t *testing.T) {

	testCases := []struct {
		name          string
		DBType        string
		RWSType       string
		nodeID        uint32
		topK          uint16
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			RWSType:       "one-node0",
			nodeID:        0,
			topK:          5,
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			RWSType:       "one-node0",
			nodeID:        0,
			topK:          5,
			expectedError: models.ErrEmptyDB,
		},
		{
			name:          "nil RWS",
			DBType:        "one-node0",
			RWSType:       "nil",
			nodeID:        0,
			topK:          5,
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "empty RWS",
			DBType:        "one-node0",
			RWSType:       "empty",
			nodeID:        0,
			topK:          5,
			expectedError: models.ErrEmptyRWS,
		},
		{
			name:          "node not in DB",
			DBType:        "one-node0",
			RWSType:       "one-node1",
			nodeID:        1,
			topK:          5,
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "node not in RWS",
			DBType:        "one-node1",
			RWSType:       "one-node0",
			nodeID:        1,
			topK:          5,
			expectedError: models.ErrNodeNotFoundRWS,
		},
		{
			name:          "invalid topK",
			DBType:        "one-node0",
			RWSType:       "one-node0",
			topK:          0,
			expectedError: ErrInvalidTopN,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := mockdb.SetupDB(test.DBType)
			RWS := mockstore.SetupRWS(test.RWSType)

			err := checkInputs(DB, RWS, test.nodeID, test.topK)

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
		expectedPP models.PagerankMap
	}{
		{
			name:       "nil walk",
			longWalk:   nil,
			expectedPP: models.PagerankMap{},
		},
		{
			name:       "empty walk",
			longWalk:   []uint32{},
			expectedPP: models.PagerankMap{},
		},
		{
			name:     "normal walk",
			longWalk: []uint32{0, 1, 2, 0, 1},
			expectedPP: models.PagerankMap{
				0: 2.0 / 5.0,
				1: 2.0 / 5.0,
				2: 1.0 / 5.0,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			pp := countAndNormalize(test.longWalk)
			if models.Distance(test.expectedPP, pp) > 1e-10 {
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
		expectedNodeIDs models.RandomWalk
	}{
		{
			name:            "one-node0",
			pWalkType:       "one-node0",
			expectedNodeIDs: models.RandomWalk{0},
		},
		{
			name:            "triangle",
			pWalkType:       "triangle",
			expectedNodeIDs: models.RandomWalk{0, 1, 2},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			pWalk := SetupPWalk(test.pWalkType, 10)
			pWalk.Reset()

			if pWalk.currentNodeID != pWalk.startingNodeID {
				t.Errorf("Reset(): expected %v, got %v", pWalk.startingNodeID, pWalk.currentNodeID)
			}

			if !reflect.DeepEqual(pWalk.currentWalk, models.RandomWalk{pWalk.startingNodeID}) {
				t.Errorf("Reset(): expected %v, got %v", models.RandomWalk{pWalk.startingNodeID}, pWalk.currentWalk)
			}

			if !reflect.DeepEqual(pWalk.walk, test.expectedNodeIDs) {
				t.Errorf("Reset(): expected %v, got %v", test.expectedNodeIDs, pWalk.walk)
			}
		})
	}
}

func TestAppendNode(t *testing.T) {

	testCases := []struct {
		name                string
		pWalkType           string
		nextNodeID          uint32
		expectedCurrentWalk models.RandomWalk
	}{
		{
			name:                "one-node0",
			pWalkType:           "one-node0",
			nextNodeID:          1,
			expectedCurrentWalk: models.RandomWalk{0, 1},
		},
		{
			name:                "triangle",
			pWalkType:           "triangle",
			nextNodeID:          3,
			expectedCurrentWalk: models.RandomWalk{0, 1, 2, 3},
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

func TestAppendWalk(t *testing.T) {

	testCases := []struct {
		name            string
		pWalkType       string
		walkSegment     models.RandomWalk
		expectedNodeIDs models.RandomWalk
	}{
		{
			name:            "one-node0",
			pWalkType:       "one-node0",
			walkSegment:     models.RandomWalk{1, 2},
			expectedNodeIDs: models.RandomWalk{0, 1, 2},
		},
		{
			name:            "triangle",
			pWalkType:       "triangle",
			walkSegment:     models.RandomWalk{3, 1},
			expectedNodeIDs: models.RandomWalk{0, 1, 2, 3},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			pWalk := SetupPWalk(test.pWalkType, 10)
			pWalk.AppendWalk(test.walkSegment)

			if !reflect.DeepEqual(pWalk.walk, test.expectedNodeIDs) {
				t.Errorf("AppendNode(): expected %v, got %v", test.expectedNodeIDs, pWalk.currentWalk)
			}
		})
	}
}

func TestPersonalizedWalk(t *testing.T) {
	testCases := []struct {
		name           string
		DBType         string
		RWSType        string
		startingNodeID uint32
		requiredLenght int
		expectedVisits map[uint32]int
		expectedError  error
	}{
		{
			name:           "empty RWS",
			DBType:         "one-node0",
			RWSType:        "empty",
			startingNodeID: 0,
			requiredLenght: 5,
			expectedError:  models.ErrEmptyRWS,
		},
		{
			name:           "node not found RWS",
			DBType:         "one-node0",
			RWSType:        "one-node1",
			startingNodeID: 0,
			requiredLenght: 5,
			expectedError:  models.ErrNodeNotFoundRWS,
		},
		{
			name:           "required Lenght = 0; empty slice returned",
			DBType:         "one-node0",
			RWSType:        "one-node0",
			startingNodeID: 0,
			requiredLenght: 0,
			expectedError:  nil,
		},
		{
			name:           "single walk added",
			DBType:         "simple",
			RWSType:        "simple",
			startingNodeID: 0,
			requiredLenght: 1,
			expectedVisits: map[uint32]int{0: 1, 1: 1},
			expectedError:  nil,
		},
		{
			name:           "multiple walks added",
			DBType:         "triangle",
			RWSType:        "triangle",
			startingNodeID: 0,
			requiredLenght: 11,
			expectedVisits: map[uint32]int{0: 4, 1: 4, 2: 3},
			expectedError:  nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := mockdb.SetupDB(test.DBType)
			RWS := mockstore.SetupRWS(test.RWSType)
			rng := rand.New(rand.NewSource(42))

			pWalk, err := personalizedWalk(DB, RWS, test.startingNodeID, test.requiredLenght, rng)

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
			RWSType       string
			nodeID        uint32
			topK          uint16
			expectedError error
		}{
			{
				name:          "nil DB",
				DBType:        "nil",
				RWSType:       "one-node0",
				nodeID:        0,
				topK:          5,
				expectedError: models.ErrNilDBPointer,
			},
			{
				name:          "empty DB",
				DBType:        "empty",
				RWSType:       "one-node0",
				nodeID:        0,
				topK:          5,
				expectedError: models.ErrEmptyDB,
			},
			{
				name:          "nil RWS",
				DBType:        "one-node0",
				RWSType:       "nil",
				nodeID:        0,
				topK:          5,
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "empty RWS",
				RWSType:       "empty",
				DBType:        "one-node0",
				nodeID:        0,
				topK:          5,
				expectedError: models.ErrEmptyRWS,
			},
			{
				name:          "node not in the RWS",
				DBType:        "triangle",
				RWSType:       "one-node0",
				nodeID:        1,
				topK:          5,
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "invalid topK",
				DBType:        "one-node0",
				RWSType:       "one-node0",
				nodeID:        0,
				topK:          0,
				expectedError: ErrInvalidTopN,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				DB := mockdb.SetupDB(test.DBType)
				RWS := mockstore.SetupRWS(test.RWSType)

				_, err := Personalized(DB, RWS, test.nodeID, test.topK)

				if !errors.Is(err, test.expectedError) {
					t.Errorf("Personalized(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("fuzzy test", func(t *testing.T) {
		nodesNum := 200
		edgesPerNode := 20
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))

		DB := mockdb.GenerateDB(nodesNum, edgesPerNode, rng)
		RWM, _ := walks.NewRWM("mock", 0.85, 10)
		RWM.GenerateAll(DB)

		if _, err := Personalized(DB, RWM.Store, 0, 5); err != nil {
			t.Fatalf("Personalized() expected nil, got %v", err)
		}

		// doing it two times to check that it donesn't change the DB or RWS
		if _, err := Personalized(DB, RWM.Store, 0, 5); err != nil {
			t.Errorf("Personalized() expected nil, got %v", err)
		}
	})
}

func BenchmarkPersonalized(b *testing.B) {
	nodesNum := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mockdb.GenerateDB(nodesNum, edgesPerNode, rng)

	for _, walksPerNode := range []uint16{1, 10, 100, 1000} {
		RWM, _ := walks.NewRWM("mock", 0.85, walksPerNode)
		RWM.GenerateAll(DB)

		b.Run(fmt.Sprintf("walksPerNode: %d", walksPerNode), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Personalized(DB, RWM.Store, 0, 100); err != nil {
					b.Fatalf("Benchmark failed: %v", err)
				}
			}
		})
	}
}
