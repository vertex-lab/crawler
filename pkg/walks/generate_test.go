package walks

import (
	"errors"
	"math"
	rand "math/rand"
	randv2 "math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	mock "github.com/pippellia-btc/Nostrcrawler/pkg/database/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
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
		expectedWalk  models.RandomWalk
		expectedError error
	}{
		{
			name:          "node not found",
			DBType:        "one-node0",
			expectedWalk:  nil,
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "valid, triangle",
			DBType:        "triangle",
			expectedWalk:  models.RandomWalk{1, 2, 0},
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := mock.SetupDB(test.DBType)
			rng := rand.New(rand.NewSource(42))

			walk, err := generateWalk(DB, 1, 0.85, rng)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("generateWalk(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walk, test.expectedWalk) {
				t.Errorf("generateWalk(): expected %v, got %v", test.expectedWalk, walk)
			}
		})
	}
}

func TestGenerateWalks(t *testing.T) {

	testCases := []struct {
		name          string
		DBType        string
		expectedWalks map[uint32]map[uint32]models.RandomWalk
		expectedError error
	}{
		{
			name:          "node not found",
			DBType:        "one-node1",
			expectedWalks: nil,
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "valid, triangle",
			DBType:        "triangle",
			expectedError: nil,
			expectedWalks: map[uint32]map[uint32]models.RandomWalk{
				0: {
					0: {0, 1, 2},
					1: {0, 1, 2},
					3: {1, 2, 0},
					4: {2, 0, 1},
					5: {2, 0, 1},
				},
				1: {
					0: {0, 1, 2},
					1: {0, 1, 2},
					2: {1, 2},
					3: {1, 2, 0},
					4: {2, 0, 1},
					5: {2, 0, 1},
				},
				2: {
					0: {0, 1, 2},
					1: {0, 1, 2},
					2: {1, 2},
					3: {1, 2, 0},
					4: {2, 0, 1},
					5: {2, 0, 1},
				},
			},
		},
	}

	for _, test := range testCases {

		t.Run(test.name, func(t *testing.T) {

			DB := mock.SetupDB(test.DBType)
			nodeIDs := []uint32{0, 1, 2}
			RWM, _ := NewRWM("mock", 0.85, 2)
			rng := rand.New(rand.NewSource(69))

			if err := RWM.generateWalks(DB, nodeIDs, rng); !errors.Is(err, test.expectedError) {
				t.Errorf("generateWalks(): expected %v, got %v", test.expectedError, err)
			}

			// check if the walk was added to each node
			for _, nodeID := range nodeIDs {
				if RWM.Store.IsEmpty() {
					break
				}

				walkMap, err := RWM.Store.NodeWalks(nodeID)
				if err != nil {
					t.Fatalf("WalkSet(%d): expected nil, got %v", nodeID, err)
				}

				// check if the walk is as expected
				if !reflect.DeepEqual(walkMap, test.expectedWalks[nodeID]) {
					t.Errorf("generateWalks(): nodeID = %d; expected %v, got %v", nodeID, test.expectedWalks[nodeID], walkMap)
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
		expectedWalks map[uint32]map[uint32]models.RandomWalk
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			RWMType:       "one-node1",
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			RWMType:       "one-node1",
			expectedError: models.ErrEmptyDB,
		},
		{
			name:          "node not in DB",
			DBType:        "one-node1",
			RWMType:       "one-node1",
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "nil RWM",
			DBType:        "one-node0",
			RWMType:       "nil",
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "empty RWM",
			DBType:        "one-node0",
			RWMType:       "empty",
			expectedError: models.ErrEmptyRWS,
		},
		{
			name:          "valid",
			DBType:        "one-node0",
			RWMType:       "one-node1",
			expectedError: nil,
			expectedWalks: map[uint32]map[uint32]models.RandomWalk{0: {1: {0}}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := mock.SetupDB(test.DBType)
			RWM := SetupRWM(test.RWMType)

			if err := RWM.Generate(DB, 0); !errors.Is(err, test.expectedError) {
				t.Errorf("generateWalks(): expected %v, got %v", test.expectedError, err)
			}

			if test.expectedWalks != nil {
				walkMap, err := RWM.Store.NodeWalks(0)
				if err != nil {
					t.Fatalf("WalkSet(0): expected nil, got %v", err)
				}

				if !reflect.DeepEqual(walkMap, test.expectedWalks[0]) {
					t.Errorf("generateWalks(): nodeID = %d; expected %v, got %v", 0, test.expectedWalks[0], walkMap)
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
				expectedError: models.ErrNilDBPointer,
			},
			{
				name:          "empty DB",
				DBType:        "empty",
				RWMType:       "one-node1",
				expectedError: models.ErrEmptyDB,
			},
			{
				name:          "nil RWM",
				DBType:        "one-node0",
				RWMType:       "nil",
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "non-empty RWM",
				DBType:        "one-node0",
				RWMType:       "one-node0",
				expectedError: models.ErrNonEmptyRWS,
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

		nodesNum := 200
		edgesPerNode := 20
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))

		DB := mock.GenerateDB(nodesNum, edgesPerNode, rng)
		RWM, _ := NewRWM("mock", 0.85, 10)
		RWM.GenerateAll(DB)

		// check that each walk in the WalkSet of nodeID contains nodeID
		for nodeID := uint32(0); nodeID < uint32(nodesNum); nodeID++ {

			walks, err := RWM.Store.NodeWalks(nodeID)
			if err != nil {
				t.Fatalf("WalkSet(%d): expected nil, got %v", nodeID, err)
			}

			for _, walk := range walks {
				if !slices.Contains(walk, nodeID) {
					t.Fatalf("walk %v should contain nodeID = %d", walk, nodeID)
				}
			}
		}
	})
}

// ---------------------------------BENCHMARKS---------------------------------

func BenchmarkRNGv1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		rand.Intn(1000)
	}
}

func BenchmarkRNGChaCha(b *testing.B) {
	for i := 0; i < b.N; i++ {
		randv2.IntN(1000) // if not seeded, it used the ChaCha algo
	}
}

func BenchmarkRNGPCG(b *testing.B) {
	rng := randv2.New(randv2.NewPCG(1, 2))
	for i := 0; i < b.N; i++ {
		rng.IntN(1000)
	}
}

func BenchmarkGenerateWalk(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateDB(nodesSize, edgesPerNode, rng)

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
	DB := mock.GenerateDB(nodesSize, edgesPerNode, rng)
	RWM, _ := NewRWM("mock", 0.85, 10)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		err := RWM.generateWalks(DB, []uint32{0}, rng)
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
	DB := mock.GenerateDB(nodesSize, edgesPerNode, rng)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		RWM, _ := NewRWM("mock", 0.85, 10)
		err := RWM.GenerateAll(DB)
		if err != nil {
			b.Fatalf("GenerateAll() failed: %v", err)
		}
	}
}
