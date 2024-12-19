package walks

import (
	"context"
	"errors"
	"math"
	rand "math/rand"
	randv2 "math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
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

			DB := mockdb.SetupDB(test.DBType)
			rng := rand.New(rand.NewSource(42))

			walk, err := generateWalk(context.Background(), DB, 1, 0.85, rng)

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
		nodeIDs       []uint32
		expectedWalks map[uint32]map[uint32]models.RandomWalk
		expectedError error
	}{
		{
			name:          "node not found",
			DBType:        "one-node1",
			nodeIDs:       []uint32{0, 1},
			expectedWalks: nil,
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "valid, triangle",
			DBType:        "triangle",
			nodeIDs:       []uint32{0, 1, 2},
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
			ctx := context.Background()
			DB := mockdb.SetupDB(test.DBType)
			RWM, _ := NewMockRWM(0.85, 2)
			rng := rand.New(rand.NewSource(69))

			err := RWM.generateWalks(context.Background(), DB, test.nodeIDs, rng)
			if !errors.Is(err, test.expectedError) {
				t.Errorf("generateWalks(): expected %v, got %v", test.expectedError, err)
			}

			for nodeID, expectedWalk := range test.expectedWalks {
				walkIDs, err := RWM.Store.WalksVisiting(ctx, -1, nodeID)
				if err != nil {
					t.Fatalf("WalksVisiting(%d): expected nil, got %v", nodeID, err)
				}

				walks, err := RWM.Store.Walks(ctx, walkIDs...)
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
			expectedError: models.ErrNodeNotFoundDB,
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
			name:          "valid",
			DBType:        "one-node0",
			RWMType:       "one-node1",
			expectedError: nil,
			expectedWalks: map[uint32]map[uint32]models.RandomWalk{0: {1: {0}}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			DB := mockdb.SetupDB(test.DBType)
			RWM := SetupMockRWM(test.RWMType)

			if err := RWM.Generate(context.Background(), DB, 0); !errors.Is(err, test.expectedError) {
				t.Errorf("generateWalks(): expected %v, got %v", test.expectedError, err)
			}

			for nodeID, expectedWalk := range test.expectedWalks {
				walkIDs, err := RWM.Store.WalksVisiting(ctx, -1, nodeID)
				if err != nil {
					t.Fatalf("WalksVisiting(%d): expected nil, got %v", nodeID, err)
				}

				walks, err := RWM.Store.Walks(ctx, walkIDs...)
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
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mockdb.SetupDB(test.DBType)
				RWM := SetupMockRWM(test.RWMType)

				err := RWM.GenerateAll(context.Background(), DB)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("GenerateAll(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("fuzzy test", func(t *testing.T) {
		ctx := context.Background()
		nodesNum := 200
		edgesPerNode := 20
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		DB := mockdb.GenerateDB(nodesNum, edgesPerNode, rng)
		RWM, _ := NewMockRWM(0.85, 10)
		RWM.GenerateAll(context.Background(), DB)

		// check that each walk in the WalkSet of nodeID contains nodeID
		for nodeID := uint32(0); nodeID < uint32(nodesNum); nodeID++ {
			walkIDs, err := RWM.Store.WalksVisiting(ctx, -1, nodeID)
			if err != nil {
				t.Fatalf("WalksVisiting(%d): expected nil, got %v", nodeID, err)
			}

			walks, err := RWM.Store.Walks(ctx, walkIDs...)
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

func TestStartsWith(t *testing.T) {
	testCases := []struct {
		name         string
		walk         models.RandomWalk
		nodeID       uint32
		expectedBool bool
	}{
		{
			name:         "nil random walk",
			walk:         nil,
			nodeID:       0,
			expectedBool: false,
		},
		{
			name:         "empty random walk",
			walk:         models.RandomWalk{},
			nodeID:       0,
			expectedBool: false,
		},
		{
			name:         "valid walk, doesn't start with 0",
			walk:         models.RandomWalk{1, 2, 3},
			nodeID:       0,
			expectedBool: false,
		},
		{
			name:         "valid walk, starts with 0",
			walk:         models.RandomWalk{1, 2, 3},
			nodeID:       1,
			expectedBool: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if startsWith(test.walk, test.nodeID) != test.expectedBool {
				t.Fatalf("containsInvalidStep(): expected %v, got %v", test.expectedBool, startsWith(test.walk, test.nodeID))
			}
		})
	}
}

func TestRemove(t *testing.T) {
	testCases := []struct {
		name          string
		RWMType       string
		expectedWalks map[uint32]models.RandomWalk
		expectedError error
	}{
		{
			name:          "nil RWM",
			RWMType:       "nil",
			expectedError: models.ErrNilRWSPointer,
			expectedWalks: nil,
		},
		{
			name:          "node not found RWS",
			RWMType:       "one-node1",
			expectedError: models.ErrNodeNotFoundRWS,
			expectedWalks: map[uint32]models.RandomWalk{0: {1}},
		},
		{
			name:          "valid",
			RWMType:       "triangle",
			expectedError: nil,
			expectedWalks: map[uint32]models.RandomWalk{
				1: {1, 2, 0},
				2: {2, 0, 1},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			RWS := mockstore.SetupRWS(test.RWMType)
			RWM := &RandomWalkManager{Store: RWS}
			if err := RWM.Remove(context.Background(), 0); !errors.Is(err, test.expectedError) {
				t.Fatalf("Remove(0): expected %v, got %v", test.expectedError, err)
			}

			if RWS != nil && !reflect.DeepEqual(RWS.WalkIndex, test.expectedWalks) {
				t.Errorf("Remove(0): expected %v, got %v", test.expectedWalks, RWS.WalkIndex)
			}
		})
	}
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
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mockdb.GenerateDB(nodesSize, edgesPerNode, rng)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := generateWalk(context.Background(), DB, 0, 0.85, rng); err != nil {
			b.Fatalf("generateWalk() failed: %v", err)
		}
	}
}

func BenchmarkGenerateRandomWalks(b *testing.B) {
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mockdb.GenerateDB(nodesSize, edgesPerNode, rng)
	RWM, _ := NewMockRWM(0.85, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := RWM.generateWalks(context.Background(), DB, []uint32{0}, rng); err != nil {
			b.Fatalf("Generate() failed: %v", err)
		}
	}
}

func BenchmarkGenerateAll(b *testing.B) {
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mockdb.GenerateDB(nodesSize, edgesPerNode, rng)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RWM, _ := NewMockRWM(0.85, 10)
		if err := RWM.GenerateAll(context.Background(), DB); err != nil {
			b.Fatalf("GenerateAll() failed: %v", err)
		}
	}
}
