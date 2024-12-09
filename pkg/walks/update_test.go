package walks

import (
	"errors"
	"math/rand"
	"reflect"
	"slices"
	"testing"
	"time"

	mock "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
)

func TestContainsInvalidStep(t *testing.T) {
	testCases := []struct {
		name             string
		walk             models.RandomWalk
		expectedCutIndex int
		expectedContains bool
	}{
		{
			name:             "nil random walk",
			walk:             nil,
			expectedCutIndex: -1,
			expectedContains: false,
		},
		{
			name:             "empty random walk",
			walk:             models.RandomWalk{},
			expectedCutIndex: -1,
			expectedContains: false,
		},
		{
			name:             "normal random walk, no updates",
			walk:             models.RandomWalk{1},
			expectedCutIndex: -1,
			expectedContains: false,
		},
		{
			name:             "normal random walk, updates",
			walk:             models.RandomWalk{1, 2, 3},
			expectedCutIndex: 1,
			expectedContains: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			cutIndex, contains := containsInvalidStep(test.walk, 1, []uint32{2})

			if contains != test.expectedContains {
				t.Fatalf("containsInvalidStep(): expected %v, got %v", test.expectedContains, contains)
			}

			if cutIndex != test.expectedCutIndex {
				t.Errorf("containsInvalidStep(): expected %v, got %v", test.expectedCutIndex, cutIndex)
			}
		})
	}
}

func TestUpdateRemovedNodes(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			DBType        string
			RWMType       string
			removedSucc   []uint32
			expectedError error
		}{
			{
				name:          "nil RWM",
				DBType:        "one-node0",
				RWMType:       "nil",
				removedSucc:   []uint32{0},
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "empty RWM",
				DBType:        "one-node0",
				RWMType:       "empty",
				removedSucc:   []uint32{0},
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "node not found in the RWM",
				DBType:        "one-node0",
				RWMType:       "one-node1",
				removedSucc:   []uint32{1},
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "empty removedSucc",
				DBType:        "triangle",
				RWMType:       "triangle",
				removedSucc:   []uint32{},
				expectedError: nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mock.SetupDB(test.DBType)
				RWM := SetupRWM(test.RWMType)
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				err := RWM.updateRemovedNodes(DB, 0, test.removedSucc, []uint32{2}, rng)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("updateRemovedNodes(): expected %v, got %v", test.expectedError, err)
				}

			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		DB := mock.SetupDB("triangle")
		RWM := SetupRWM("triangle")

		nodeID := uint32(0)
		removeSucc := []uint32{1}

		// update the DB
		commonSucc := []uint32{2}
		DB.NodeIndex[nodeID].Successors = commonSucc

		rng := rand.New(rand.NewSource(5))
		expectedWalks := map[uint32]map[uint32]models.RandomWalk{
			0: {
				0: {0, 2},
				1: {1, 2, 0},
				2: {2, 0},
			},
			1: {
				1: {1, 2, 0},
			},
			2: {
				0: {0, 2},
				1: {1, 2, 0},
				2: {2, 0},
			},
		}

		if err := RWM.updateRemovedNodes(DB, nodeID, removeSucc, commonSucc, rng); err != nil {
			t.Errorf("updateRemovedNodes(): expected nil, got %v", err)
		}

		for nodeID, expectedWalk := range expectedWalks {

			walkMap, err := RWM.Store.Walks(nodeID, -1)
			if err != nil {
				t.Fatalf("Walks(%d): expected nil, got %v", nodeID, err)
			}

			if !reflect.DeepEqual(walkMap, expectedWalk) {
				t.Errorf("updateRemovedNodes() nodeID = %d: expected %v, got %v", nodeID, expectedWalk, walkMap)
			}
		}
	})
}

func TestUpdateAddedNodes(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			DBType        string
			RWMType       string
			addedSucc     []uint32
			newOutDegree  int
			expectedError error
		}{
			{
				name:          "nil RWM",
				DBType:        "one-node0",
				RWMType:       "nil",
				addedSucc:     []uint32{3},
				newOutDegree:  1,
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "empty RWM",
				DBType:        "one-node0",
				RWMType:       "empty",
				addedSucc:     []uint32{3},
				newOutDegree:  1,
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "node not found in the RWM",
				DBType:        "one-node0",
				RWMType:       "one-node1",
				addedSucc:     []uint32{3},
				newOutDegree:  1,
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "empty addedSucc",
				DBType:        "triangle",
				RWMType:       "triangle",
				addedSucc:     []uint32{},
				newOutDegree:  1,
				expectedError: nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mock.SetupDB(test.DBType)
				RWM := SetupRWM(test.RWMType)
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				err := RWM.updateAddedNodes(DB, 0, test.addedSucc, test.newOutDegree, rng)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("updateRemovedNodes(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		DB := mock.SetupDB("simple")
		RWM := SetupRWM("simple")
		nodeID := uint32(0)
		addedSucc := []uint32{2}

		// update the DB
		currentSucc := []uint32{2}
		DB.NodeIndex[nodeID].Successors = currentSucc

		rng := rand.New(rand.NewSource(5))
		expectedWalks := map[uint32]map[uint32]models.RandomWalk{
			0: {
				0: {0, 2},
			},
			1: {},
			2: {
				0: {0, 2},
			},
		}

		if err := RWM.updateAddedNodes(DB, nodeID, addedSucc, len(currentSucc), rng); err != nil {
			t.Errorf("updateAddedNodes(): expected nil, got %v", err)
		}

		for nodeID, expectedWalk := range expectedWalks {

			walkMap, err := RWM.Store.Walks(nodeID, -1)
			if err != nil {
				t.Fatalf("Walks(%d): expected nil, got %v", nodeID, err)
			}
			if !reflect.DeepEqual(walkMap, expectedWalk) {
				t.Errorf("updateAddedNodes() nodeID = %d: expected %v, got %v", nodeID, expectedWalk, walkMap)
			}
		}
	})
}

func TestUpdate(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {

		testCases := []struct {
			name          string
			DBType        string
			RWMType       string
			nodeID        uint32
			oldSucc       []uint32
			currentSucc   []uint32
			expectedError error
		}{
			{
				name:          "nil DB",
				DBType:        "nil",
				RWMType:       "triangle",
				nodeID:        0,
				oldSucc:       []uint32{0},
				currentSucc:   []uint32{1},
				expectedError: models.ErrNilDBPointer,
			},
			{
				name:          "empty DB",
				DBType:        "empty",
				RWMType:       "triangle",
				nodeID:        0,
				oldSucc:       []uint32{0},
				currentSucc:   []uint32{1},
				expectedError: models.ErrNodeNotFoundDB,
			},
			{
				name:          "nil RWM",
				DBType:        "one-node0",
				RWMType:       "nil",
				nodeID:        0,
				oldSucc:       []uint32{0},
				currentSucc:   []uint32{1},
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "empty RWM",
				DBType:        "one-node0",
				RWMType:       "empty",
				nodeID:        0,
				oldSucc:       []uint32{0},
				currentSucc:   []uint32{1},
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "node not found in the DB",
				DBType:        "one-node1",
				RWMType:       "one-node1",
				nodeID:        0,
				oldSucc:       []uint32{0},
				currentSucc:   []uint32{1},
				expectedError: models.ErrNodeNotFoundDB,
			},
			{
				name:          "node not found in the RWM",
				DBType:        "one-node0",
				RWMType:       "one-node1",
				nodeID:        0,
				oldSucc:       []uint32{0},
				currentSucc:   []uint32{1},
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "oldSucc == currentSucc",
				DBType:        "triangle",
				RWMType:       "triangle",
				nodeID:        0,
				oldSucc:       []uint32{1},
				currentSucc:   []uint32{1},
				expectedError: nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mock.SetupDB(test.DBType)
				RWM := SetupRWM(test.RWMType)

				err := RWM.Update(DB, test.nodeID, test.oldSucc, test.currentSucc)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("updateAddedNodes(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("fuzzy test", func(t *testing.T) {
		nodesNum := 200
		edgesPerNode := 20

		// generate the first DB
		rng1 := rand.New(rand.NewSource(time.Now().UnixNano()))
		DB1 := mock.GenerateDB(nodesNum, edgesPerNode, rng1)
		RWM, _ := NewMockRWM(0.85, 10)
		RWM.GenerateAll(DB1)

		// generate another DB
		rng2 := rand.New(rand.NewSource(time.Now().UnixNano()))
		DB2 := mock.GenerateDB(nodesNum, edgesPerNode, rng2)

		// update one node at the time
		for nodeID := uint32(0); nodeID < uint32(nodesNum); nodeID++ {
			oldSucc := DB1.NodeIndex[nodeID].Successors
			newSucc := DB2.NodeIndex[nodeID].Successors
			DB1.NodeIndex[nodeID].Successors = newSucc

			if err := RWM.Update(DB1, nodeID, oldSucc, newSucc); err != nil {
				t.Fatalf("Update(%d): expected nil, got %v", nodeID, err)
			}
		}

		// check that each walk in the Walks of nodeID contains nodeID
		for nodeID := uint32(0); nodeID < uint32(nodesNum); nodeID++ {
			walks, err := RWM.Store.Walks(nodeID, -1)
			if err != nil {
				t.Fatalf("Walks(%d): expected nil, got %v", nodeID, err)
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

func BenchmarkUpdateAddedNodes(b *testing.B) {
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateDB(nodesSize, edgesPerNode, rng)

	RWM, _ := NewMockRWM(0.85, 10)
	RWM.GenerateAll(DB)

	oldSuccessorMap := make(map[uint32][]uint32, nodesSize)
	currentSuccessorMap := make(map[uint32][]uint32, nodesSize)
	b.Run("Update(), 10% new successors", func(b *testing.B) {
		// prepare the graph changes
		for nodeID := uint32(0); nodeID < uint32(nodesSize); nodeID++ {
			oldSuccessors, _ := DB.Successors(nodeID)
			currentSuccessors := make([]uint32, len(oldSuccessors))
			copy(currentSuccessors, oldSuccessors)

			// add 10% new nodes
			for i := 0; i < edgesPerNode/10; i++ {

				newNode := uint32(rng.Intn(nodesSize))
				currentSuccessors = append(currentSuccessors, newNode)
			}
			oldSuccessorMap[nodeID] = oldSuccessors
			currentSuccessorMap[nodeID] = currentSuccessors
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nodeID := uint32(i % nodesSize)
			oldSuccessors := oldSuccessorMap[nodeID]
			currentSuccessors := currentSuccessorMap[nodeID]

			err := RWM.Update(DB, nodeID, oldSuccessors, currentSuccessors)
			if err != nil {
				b.Fatalf("Update() failed: %v", err)
			}
		}
	})
}

/*
!IMPORTANT!

run this benchmark with:

> -benchtime=<nodesSize>x

each node should only be updated once. Each subsequent update will be
much cheaper because no walk will need an update, thus compromizing the measurement
*/
func BenchmarkUpdateRemovedNodes(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateDB(nodesSize, edgesPerNode, rng)

	RWM, _ := NewMockRWM(0.85, 10)
	RWM.GenerateAll(DB)

	// Store the changes here
	oldSuccessorMap := make(map[uint32][]uint32, nodesSize)
	currentSuccessorMap := make(map[uint32][]uint32, nodesSize)

	b.Run("Update(), 10% removed successors", func(b *testing.B) {

		// prepare the graph changes
		for nodeID := uint32(0); nodeID < uint32(nodesSize); nodeID++ {

			oldSuccessors, _ := DB.Successors(nodeID)
			currentSuccessors := make([]uint32, len(oldSuccessors)-edgesPerNode/10)

			// remove 10% of the nodes
			copy(currentSuccessors, oldSuccessors[edgesPerNode/10:])

			oldSuccessorMap[nodeID] = oldSuccessors
			currentSuccessorMap[nodeID] = currentSuccessors
		}

		b.ResetTimer()

		// perform benchmark
		for i := 0; i < b.N; i++ {

			nodeID := uint32(i % nodesSize)
			oldSuccessors := oldSuccessorMap[nodeID]
			currentSuccessors := currentSuccessorMap[nodeID]

			err := RWM.Update(DB, nodeID, oldSuccessors, currentSuccessors)
			if err != nil {
				b.Fatalf("Update() failed: %v", err)
			}

		}
	})
}
