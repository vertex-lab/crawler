package walks

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
)

//------------------------------RANDOM-WALKS-TESTS------------------------------

// grouping properties of a test together
type testCasesWalk struct {
	name             string
	rWalk            *RandomWalk
	expectedError    error
	expectedUpdate   bool
	expectedCutIndex int
}

func TestCheckEmpty(t *testing.T) {

	testCases := []testCasesWalk{
		{
			name:          "nil random walk",
			rWalk:         nil,
			expectedError: ErrNilRandomWalkPointer,
		},
		{
			name:          "empty random walk",
			rWalk:         &RandomWalk{NodeIDs: []uint32{}},
			expectedError: ErrEmptyRandomWalk,
		},
		{
			name:          "normal random walk",
			rWalk:         &RandomWalk{NodeIDs: []uint32{1, 2, 3}},
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			rWalk := test.rWalk
			err := rWalk.CheckEmpty()

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("CheckEmpty(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestNeedsUpdate(t *testing.T) {

	testCases := []testCasesWalk{
		{
			name:             "nil random walk",
			rWalk:            nil,
			expectedError:    ErrNilRandomWalkPointer,
			expectedUpdate:   true,
			expectedCutIndex: -1,
		},
		{
			name:             "empty random walk",
			rWalk:            &RandomWalk{NodeIDs: []uint32{}},
			expectedError:    ErrEmptyRandomWalk,
			expectedUpdate:   true,
			expectedCutIndex: -1,
		},
		{
			name:             "normal random walk, no updates",
			rWalk:            &RandomWalk{NodeIDs: []uint32{1}},
			expectedError:    nil,
			expectedUpdate:   false,
			expectedCutIndex: -1,
		},
		{
			name:             "normal random walk, updates",
			rWalk:            &RandomWalk{NodeIDs: []uint32{1, 2, 3}},
			expectedError:    nil,
			expectedUpdate:   true,
			expectedCutIndex: 1,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			rWalk := test.rWalk
			update, cutIndex, err := rWalk.NeedsUpdate(1, []uint32{2})

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NeedsUpdate(): expected %v, got %v", test.expectedError, err)
			}

			if update != test.expectedUpdate {
				t.Errorf("NeedsUpdate(): expected %v, got %v", test.expectedUpdate, update)
			}

			if cutIndex != test.expectedCutIndex {
				t.Errorf("NeedsUpdate(): expected %v, got %v", test.expectedCutIndex, cutIndex)
			}
		})
	}
}

// -------------------------RANDOM-WALKS-MANAGER-TESTS--------------------------

// grouping properties of a test together
type testCasesRWM struct {
	name          string
	RWMType       string
	expectedError error
	expectedEmpty bool
	alphas        []float32
	walksPerNode  uint16
	rWalk         *RandomWalk
}

// function that returns a RWM setup based on the RWMType
func setupRWM(RWMType string) *RandomWalksManager {

	switch RWMType {

	case "nil":
		return nil

	case "empty":
		RWM, _ := NewRWM(0.85, 1)
		return RWM

	case "invalid-alpha":
		invalidAlphas := []float32{1.01, 1.0, -0.1, -2}
		size := len(invalidAlphas)

		RWM, _ := NewRWM(0.85, 1)
		RWM.alpha = invalidAlphas[rand.Intn(size)]
		return RWM

	case "invalid-walksPerNode":
		RWM, _ := NewRWM(0.85, 1)
		RWM.walksPerNode = 0
		return RWM

	case "one-node0":
		RWM, _ := NewRWM(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0}})
		RWM.WalksByNode[0] = walkSet
		return RWM

	case "one-node1":
		RWM, _ := NewRWM(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{1}})
		RWM.WalksByNode[1] = walkSet
		return RWM

	default:
		return nil // Default to nil for unrecognized scenarios
	}
}

func TestNewRandomWalksManager(t *testing.T) {

	testCases := []testCasesRWM{
		{
			name:          "invalid alphas",
			alphas:        []float32{1.01, 1.0, -0.1, -2},
			walksPerNode:  1,
			expectedError: ErrInvalidAlpha,
		},
		{
			name:          "invalid walksPerNode",
			alphas:        []float32{0.99, 0.11, 0.57, 0.0001},
			walksPerNode:  0,
			expectedError: ErrInvalidWalksPerNode,
		},
		{
			name:          "both valid",
			alphas:        []float32{0.99, 0.11, 0.57, 0.0001},
			walksPerNode:  1,
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			// iterate over the alphas
			for _, alpha := range test.alphas {

				RWM, err := NewRWM(alpha, test.walksPerNode)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("NewRWM(): expected %v, got %v", test.expectedError, err)
				}

				// check if the parameters have been added correctly
				if RWM != nil {

					if RWM.alpha != alpha {
						t.Errorf("NewRWM(): expected %v, got %v", alpha, RWM.alpha)
					}

					if RWM.walksPerNode != test.walksPerNode {
						t.Errorf("NewRWM(): expected %v, got %v", test.walksPerNode, RWM.walksPerNode)
					}
				}
			}
		})
	}
}

func TestIsEmpty(t *testing.T) {

	testCases := []testCasesRWM{
		{
			name:          "nil RWM",
			RWMType:       "nil",
			expectedEmpty: true,
		},
		{
			name:          "empty RWM",
			RWMType:       "empty",
			expectedEmpty: true,
		},
		{
			name:          "non-empty RWM",
			RWMType:       "one-node0",
			expectedEmpty: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RWM := setupRWM(test.RWMType)
			empty := RWM.IsEmpty()

			if empty != test.expectedEmpty {
				t.Errorf("IsEmpty(): expected %v, got %v", test.expectedEmpty, empty)
			}
		})
	}
}

func TestCheckState(t *testing.T) {

	testCases := []testCasesRWM{
		{
			name:          "nil RWM, expected empty",
			RWMType:       "nil",
			expectedEmpty: true,
			expectedError: ErrNilRWMPointer,
		},
		{
			name:          "nil RWM, expected non-empty",
			RWMType:       "nil",
			expectedEmpty: false,
			expectedError: ErrNilRWMPointer,
		},
		{
			name:          "empty RWM, expected empty",
			RWMType:       "empty",
			expectedEmpty: true,
			expectedError: nil,
		},
		{
			name:          "empty RWM, expected non-empty",
			RWMType:       "empty",
			expectedEmpty: false,
			expectedError: ErrEmptyRWM,
		},
		{
			name:          "non-empty RWM, expected empty",
			RWMType:       "one-node0",
			expectedEmpty: true,
			expectedError: ErrNonEmptyRWM,
		},
		{
			name:          "non-empty RWM, expected non-empty",
			RWMType:       "one-node0",
			expectedEmpty: false,
			expectedError: nil,
		},
		{
			name:          "invalid alpha RWM",
			RWMType:       "invalid-alpha",
			expectedEmpty: false,
			expectedError: ErrInvalidAlpha,
		},
		{
			name:          "invalid walksPerNode RWM",
			RWMType:       "invalid-walksPerNode",
			expectedEmpty: false,
			expectedError: ErrInvalidWalksPerNode,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RWM := setupRWM(test.RWMType)
			err := RWM.CheckState(test.expectedEmpty)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("CheckState(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestWalksByNodeID(t *testing.T) {

	t.Run("simple errors", func(t *testing.T) {

		testCases := []testCasesRWM{
			{
				name:          "nil RWM",
				RWMType:       "nil",
				expectedError: ErrNilRWMPointer,
			},
			{
				name:          "empty RWM",
				RWMType:       "empty",
				expectedError: ErrEmptyRWM,
			},
			{
				name:          "non-empty RWM, node not found",
				RWMType:       "one-node0",
				expectedError: ErrNodeNotFoundRWM,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				RWM := setupRWM(test.RWMType)
				_, err := RWM.WalksByNodeID(1)

				if !errors.Is(err, test.expectedError) {
					t.Errorf("WalksByNodeID(1): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {

		RWM := setupRWM("one-node1")

		walkSet, err := RWM.WalksByNodeID(1)
		if err != nil {
			t.Fatalf("WalksByNodeID(1): expected nil, got %v", err)
		}

		walk := walkSet.ToSlice()[0].NodeIDs
		if !reflect.DeepEqual(walk, []uint32{1}) {
			t.Errorf("WalksByNodeID(1): expected %v, got %v", []uint32{1}, walk)
		}
	})
}

func TestAddWalk(t *testing.T) {

	t.Run("simple errors", func(t *testing.T) {

		testCases := []testCasesRWM{
			{
				name:          "nil RWM",
				RWMType:       "nil",
				expectedError: ErrNilRWMPointer,
			},
			{
				name:          "nil walk",
				RWMType:       "empty",
				rWalk:         nil,
				expectedError: ErrNilRandomWalkPointer,
			},
			{
				name:          "empty walk",
				RWMType:       "empty",
				rWalk:         &RandomWalk{NodeIDs: []uint32{}},
				expectedError: ErrEmptyRandomWalk,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				RWM := setupRWM(test.RWMType)
				err := RWM.AddWalk(test.rWalk)

				if !errors.Is(err, test.expectedError) {
					t.Errorf("AddWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {

		RWM := setupRWM("empty")
		rWalk := &RandomWalk{NodeIDs: []uint32{1, 2, 3}}

		err := RWM.AddWalk(rWalk)
		if err != nil {
			t.Fatalf("AddWalk(): expected nil, got %v", err)
		}

		for _, nodeID := range rWalk.NodeIDs {

			walkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("WalksByNodeID(%d): expected nil, got %v", nodeID, err)
			}

			if !walkSet.ContainsOne(rWalk) {
				t.Errorf("AddWalk(): nodeID = %d; expected {[1,2, 3]}, got %v", nodeID, walkSet)
			}
		}
	})
}

func TestPruneWalk(t *testing.T) {

	t.Run("simple errors", func(t *testing.T) {

		testCases := []testCasesRWM{
			{
				name:          "nil RWM",
				RWMType:       "nil",
				expectedError: ErrNilRWMPointer,
			},
			{
				name:          "empty RWM",
				RWMType:       "empty",
				expectedError: ErrEmptyRWM,
			},
			{
				name:          "nil walk",
				RWMType:       "one-node0",
				rWalk:         nil,
				expectedError: ErrNilRandomWalkPointer,
			},
			{
				name:          "empty walk",
				RWMType:       "one-node0",
				rWalk:         &RandomWalk{NodeIDs: []uint32{}},
				expectedError: ErrEmptyRandomWalk,
			},
			{
				name:          "invalid cutIndex",
				RWMType:       "one-node0",
				rWalk:         &RandomWalk{NodeIDs: []uint32{1}},
				expectedError: ErrInvalidWalkIndex,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				RWM := setupRWM(test.RWMType)
				err := RWM.PruneWalk(test.rWalk, 2)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("PruneWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {

		RWM := setupRWM("empty")
		rWalk := &RandomWalk{NodeIDs: []uint32{1, 2}}

		RWM.AddWalk(rWalk)
		err := RWM.PruneWalk(rWalk, 1)
		if err != nil {
			t.Fatalf("PruneWalk(): expected nil, got %v", err)
		}

		// check the walk remains
		walkSet1, err := RWM.WalksByNodeID(1)
		if err != nil {
			t.Fatalf("WalksByNodeID(): expected nil, got %v", err)
		}

		if !walkSet1.ContainsOne(rWalk) {
			t.Errorf("PruneWalk(): expected {{1}}, got %v", walkSet1)
		}

		// check the walks was removed
		walkSet2, err := RWM.WalksByNodeID(2)
		if err != nil {
			t.Fatalf("WalksByNodeID(): expected nil, got %v", err)
		}

		if walkSet2.ContainsOne(rWalk) {
			t.Errorf("PruneWalk(): expected {{}}, got %v", walkSet2)
		}

		// check the walk was pruned
		if !reflect.DeepEqual(rWalk.NodeIDs, []uint32{1}) {
			t.Errorf("PruneWalk(): expected %v, got %v", []uint32{1}, rWalk.NodeIDs)
		}
	})

}

func TestGraftWalk(t *testing.T) {

	t.Run("simple errors", func(t *testing.T) {

		testCases := []testCasesRWM{
			{
				name:          "nil RWM",
				RWMType:       "nil",
				expectedError: ErrNilRWMPointer,
			},
			{
				name:          "empty RWM",
				RWMType:       "empty",
				expectedError: ErrEmptyRWM,
			},
			{
				name:          "nil walk",
				RWMType:       "one-node0",
				rWalk:         nil,
				expectedError: ErrNilRandomWalkPointer,
			},
			{
				name:          "empty walk",
				RWMType:       "one-node0",
				rWalk:         &RandomWalk{NodeIDs: []uint32{}},
				expectedError: ErrEmptyRandomWalk,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				RWM := setupRWM(test.RWMType)
				err := RWM.GraftWalk(test.rWalk, []uint32{1})

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("GraftWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {

		RWM := setupRWM("empty")
		rWalk := &RandomWalk{NodeIDs: []uint32{0}}
		nodeIDs := []uint32{1, 2}
		expectedGraftedWalk := append(rWalk.NodeIDs, nodeIDs...)

		RWM.AddWalk(rWalk)
		err := RWM.GraftWalk(rWalk, nodeIDs)
		if err != nil {
			t.Fatalf("GraftWalk(): expected nil, got %v", err)
		}

		// check if the walk is present in all walkSets
		for _, nodeID := range expectedGraftedWalk {

			walkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("WalksByNodeID(): expected nil, got %v", err)
			}

			if !walkSet.ContainsOne(rWalk) {
				t.Errorf("GraftWalk(): nodeID = %d; expected %v, got %v", nodeID, expectedGraftedWalk, rWalk.NodeIDs)
			}
		}

		// check the walk
		if !reflect.DeepEqual(rWalk.NodeIDs, expectedGraftedWalk) {
			t.Errorf("GraftWalk(): expected %v, got %v", expectedGraftedWalk, rWalk.NodeIDs)
		}
	})

}

// ------------------------------BENCHMARKS------------------------------

func BenchmarkNeedsUpdate(b *testing.B) {

	rWalk := &RandomWalk{NodeIDs: []uint32{0, 1, 2, 3, 4, 5, 6}}
	nodeID := uint32(2)

	// setup unusually big removedNodes, in opposite order for worst case scenario
	removedNodes := make([]uint32, 101)
	for i := uint32(0); i < 100; i++ {
		removedNodes[i] = 100 - i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rWalk.NeedsUpdate(nodeID, removedNodes)
	}
}

func BenchmarkAddWalk(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)
	RWM, _ := NewRWM(0.85, 1)

	// setup the walks
	rWalks := []*RandomWalk{}
	for i := uint32(0); i < uint32(b.N); i++ {

		startingNodeID := uint32(rng.Intn(nodesSize))
		walk, _ := generateWalk(DB, startingNodeID, RWM.alpha, rng)
		rWalk := &RandomWalk{NodeIDs: walk}
		rWalks = append(rWalks, rWalk)
	}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		err := RWM.AddWalk(rWalks[i])

		if err != nil {
			b.Fatalf("BenchmarkAddWalk(): expected nil, got %v", err)
		}
	}
}

func BenchmarkPruneWalk(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)
	RWM, _ := NewRWM(0.85, 1)

	// setup the walks
	rWalks := []*RandomWalk{}
	for i := uint32(0); i < uint32(b.N); i++ {

		startingNodeID := uint32(rng.Intn(nodesSize))
		walk, _ := generateWalk(DB, startingNodeID, RWM.alpha, rng)
		rWalk := &RandomWalk{NodeIDs: walk}
		RWM.AddWalk(rWalk)

		rWalks = append(rWalks, rWalk)
	}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {

		err := RWM.PruneWalk(rWalks[i], 0)
		if err != nil {
			b.Fatalf("BenchmarkAddWalk(): expected nil, got %v", err)
		}
	}
}

func BenchmarkGraftWalk(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)
	RWM, _ := NewRWM(0.85, 1)

	// setup the walks and walk segments
	rWalks := []*RandomWalk{}
	walkSegments := [][]uint32{}
	for i := uint32(0); i < uint32(b.N); i++ {

		startingNodeID := uint32(rng.Intn(nodesSize))
		walk, _ := generateWalk(DB, startingNodeID, RWM.alpha, rng)
		rWalk := &RandomWalk{NodeIDs: walk}
		RWM.AddWalk(rWalk)
		rWalks = append(rWalks, rWalk)

		startingNodeID = uint32(rng.Intn(nodesSize))
		walkSegment, _ := generateWalk(DB, startingNodeID, RWM.alpha, rng)
		walkSegments = append(walkSegments, walkSegment)
	}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {

		err := RWM.GraftWalk(rWalks[i], walkSegments[i])
		if err != nil {
			b.Fatalf("BenchmarkAddWalk(): expected nil, got %v", err)
		}
	}
}
