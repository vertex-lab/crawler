package walks

import (
	"errors"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
)

func TestWalkStep(t *testing.T) {

	testCases := []struct {
		name           string
		successorIDs   []uint32
		walk           []uint32
		expectedNodeID uint32
		expectedStop   bool
	}{
		{
			name:           "empty successors",
			successorIDs:   []uint32{},
			walk:           []uint32{0},
			expectedNodeID: math.MaxUint32,
			expectedStop:   true,
		},
		{
			name:           "found cycle",
			successorIDs:   []uint32{0},
			walk:           []uint32{0},
			expectedNodeID: math.MaxUint32,
			expectedStop:   true,
		},
		{
			name:           "normal",
			successorIDs:   []uint32{1},
			walk:           []uint32{0},
			expectedNodeID: 1,
			expectedStop:   false,
		},
	}

	for _, test := range testCases {

		t.Run(test.name, func(t *testing.T) {

			rng := rand.New(rand.NewSource(42))
			nodeID, shouldStop := WalkStep(test.successorIDs, test.walk, rng)

			if shouldStop != test.expectedStop {
				t.Errorf("WalkStep(): expected %v, got %v", test.expectedStop, shouldStop)
			}

			if nodeID != test.expectedNodeID {
				t.Errorf("WalkStep(): expected %v, got %v", test.expectedNodeID, nodeID)
			}
		})
	}
}

func TestGenerateWalk(t *testing.T) {

	testCases := []struct {
		name          string
		DBType        string
		expectedWalk  []uint32
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedWalk:  nil,
			expectedError: graph.ErrNilDatabasePointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedWalk:  nil,
			expectedError: graph.ErrDatabaseIsEmpty,
		},
		{
			name:          "node not found",
			DBType:        "one-node0",
			expectedWalk:  nil,
			expectedError: graph.ErrNodeNotFoundDB,
		},
		{
			name:          "valid, triangle",
			DBType:        "triangle",
			expectedWalk:  []uint32{1, 2, 0},
			expectedError: nil,
		},
	}

	for _, test := range testCases {

		DB := mock.SetupDB(test.DBType)
		rng := rand.New(rand.NewSource(42))

		walk, err := generateWalk(DB, 1, 0.85, rng)

		if !errors.Is(err, test.expectedError) {
			t.Errorf("generateWalk(): expected %v, got %v", test.expectedError, err)
		}

		if !reflect.DeepEqual(walk, test.expectedWalk) {
			t.Errorf("generateWalk(): expected %v, got %v", test.expectedWalk, walk)
		}
	}
}

func TestGenerateRandomWalks(t *testing.T) {

	testCases := []struct {
		name          string
		DBType        string
		expectedWalks map[uint32][][]uint32
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedWalks: nil,
			expectedError: graph.ErrNilDatabasePointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedWalks: nil,
			expectedError: graph.ErrDatabaseIsEmpty,
		},
		{
			name:          "node not found",
			DBType:        "one-node1",
			expectedWalks: nil,
			expectedError: graph.ErrNodeNotFoundDB,
		},
		{
			name:          "valid, triangle",
			DBType:        "triangle",
			expectedError: nil,
			expectedWalks: map[uint32][][]uint32{
				0: {
					{0, 1, 2},
					{0, 1, 2},
					{1, 2, 0},
					{2, 0, 1},
					{2, 0, 1},
				},
				1: {
					{0, 1, 2},
					{0, 1, 2},
					{1, 2},
					{1, 2, 0},
					{2, 0, 1},
					{2, 0, 1},
				},
				2: {
					{0, 1, 2},
					{0, 1, 2},
					{1, 2},
					{1, 2, 0},
					{2, 0, 1},
					{2, 0, 1},
				},
			},
		},
	}

	for _, test := range testCases {

		t.Run(test.name, func(t *testing.T) {

			DB := mock.SetupDB(test.DBType)
			nodeIDs := []uint32{0, 1, 2}

			RWM, _ := NewRWM(0.85, 2)
			rng := rand.New(rand.NewSource(69))

			err := RWM.generateRandomWalks(DB, nodeIDs, rng)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("generateRandomWalks(): expected %v, got %v", test.expectedError, err)
			}

			// check if the walk was added to each node
			for _, nodeID := range nodeIDs {
				if RWM.IsEmpty() {
					break
				}

				walkSet, err := RWM.WalksByNodeID(nodeID)
				if err != nil {
					t.Fatalf("WalksByNodeID(%d): expected nil, got %v", nodeID, err)
				}

				// dereference and sort walks in lexicographic order
				walks := SortWalkSet(walkSet)

				// check if the walk is as expected
				if !reflect.DeepEqual(walks, test.expectedWalks[nodeID]) {
					t.Errorf("generateRandomWalks(): nodeID = %d; expected %v, got %v", nodeID, test.expectedWalks[nodeID], walks)
				}
			}
		})
	}
}

func TestGenerate(t *testing.T) {

	testCases := []struct {
		name          string
		DBType        string
		RWMType       string
		expectedWalks map[uint32][][]uint32
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			RWMType:       "one-node1",
			expectedError: graph.ErrNilDatabasePointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			RWMType:       "one-node1",
			expectedError: graph.ErrDatabaseIsEmpty,
		},
		{
			name:          "node not in DB",
			DBType:        "one-node1",
			RWMType:       "one-node1",
			expectedError: graph.ErrNodeNotFoundDB,
		},
		{
			name:          "nil RWM",
			DBType:        "one-node0",
			RWMType:       "nil",
			expectedError: ErrNilRWMPointer,
		},
		{
			name:          "empty RWM",
			DBType:        "one-node0",
			RWMType:       "empty",
			expectedError: ErrEmptyRWM,
		},
		{
			name:          "valid",
			DBType:        "one-node0",
			RWMType:       "one-node1",
			expectedError: nil,
			expectedWalks: map[uint32][][]uint32{0: {{0}}},
		},
	}

	for _, test := range testCases {

		t.Run(test.name, func(t *testing.T) {

			DB := mock.SetupDB(test.DBType)
			RWM := SetupRWM(test.RWMType)

			err := RWM.Generate(DB, 0)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("generateRandomWalks(): expected %v, got %v", test.expectedError, err)
			}

			if test.expectedWalks != nil {

				walkSet, err := RWM.WalksByNodeID(0)
				if err != nil {
					t.Fatalf("WalksByNodeID(0): expected nil, got %v", err)
				}

				// dereference and sort walks in lexicographic order
				walks := SortWalkSet(walkSet)

				if !reflect.DeepEqual(walks, test.expectedWalks[0]) {
					t.Errorf("generateRandomWalks(): nodeID = %d; expected %v, got %v", 0, test.expectedWalks[0], walks)
				}
			}
		})
	}
}

func TestGenerateAll(t *testing.T) {

	t.Run("simple errors", func(t *testing.T) {

		testCases := []struct {
			name          string
			DBType        string
			RWMType       string
			expectedError error
		}{
			{
				name:          "nil DB",
				DBType:        "nil",
				RWMType:       "one-node1",
				expectedError: graph.ErrNilDatabasePointer,
			},
			{
				name:          "empty DB",
				DBType:        "empty",
				RWMType:       "one-node1",
				expectedError: graph.ErrDatabaseIsEmpty,
			},
			{
				name:          "nil RWM",
				DBType:        "one-node0",
				RWMType:       "nil",
				expectedError: ErrNilRWMPointer,
			},
			{
				name:          "non-empty RWM",
				DBType:        "one-node0",
				RWMType:       "one-node0",
				expectedError: ErrNonEmptyRWM,
			},
		}

		for _, test := range testCases {

			t.Run(test.name, func(t *testing.T) {

				DB := mock.SetupDB(test.DBType)
				RWM := SetupRWM(test.RWMType)

				err := RWM.GenerateAll(DB)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("GenerateAll(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("fuzzy test", func(t *testing.T) {

		nodesNum := 2000
		edgesPerNode := 100
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))

		DB := mock.GenerateMockDB(nodesNum, edgesPerNode, rng)
		RWM, _ := NewRWM(0.85, 10)
		RWM.GenerateAll(DB)

		// check that each walk in the WalkSet of nodeID contains nodeID
		for nodeID := uint32(0); nodeID < uint32(nodesNum); nodeID++ {

			walkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("WalksByNodeID(%d): expected nil, got %v", nodeID, err)
			}

			for rWalk := range walkSet.Iter() {
				if !slices.Contains(rWalk.NodeIDs, nodeID) {
					t.Fatalf("walk %v should contain nodeID = %d", rWalk.NodeIDs, nodeID)
				}
			}
		}
	})
}

// ---------------------------------BENCHMARKS---------------------------------

func BenchmarkGenerateWalk(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		_, err := generateWalk(DB, 0, 0.85, rng)
		if err != nil {
			b.Fatalf("generateWalk() failed: %v", err)
		}
	}
}

func BenchmarkGenerateRandomWalks(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)
	RWM, _ := NewRWM(0.85, 10)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		err := RWM.generateRandomWalks(DB, []uint32{0}, rng)
		if err != nil {
			b.Fatalf("Generate() failed: %v", err)
		}
	}
}

func BenchmarkGenerateAll(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		RWM, _ := NewRWM(0.85, 10)
		err := RWM.GenerateAll(DB)
		if err != nil {
			b.Fatalf("GenerateAll() failed: %v", err)
		}
	}
}
