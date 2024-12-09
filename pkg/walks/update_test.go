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
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
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
			name           string
			DBType         string
			RWMType        string
			removedFollows []uint32
			expectedError  error
		}{
			{
				name:           "nil RWM",
				DBType:         "one-node0",
				RWMType:        "nil",
				removedFollows: []uint32{0},
				expectedError:  models.ErrNilRWSPointer,
			},
			{
				name:           "empty RWM",
				DBType:         "one-node0",
				RWMType:        "empty",
				removedFollows: []uint32{0},
				expectedError:  models.ErrNodeNotFoundRWS,
			},
			{
				name:           "node not found in the RWM",
				DBType:         "one-node0",
				RWMType:        "one-node1",
				removedFollows: []uint32{1},
				expectedError:  models.ErrNodeNotFoundRWS,
			},
			{
				name:           "empty removedFollows",
				DBType:         "triangle",
				RWMType:        "triangle",
				removedFollows: []uint32{},
				expectedError:  nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mock.SetupDB(test.DBType)
				RWM := SetupRWM(test.RWMType)
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				err := RWM.updateRemovedNodes(DB, 0, test.removedFollows, []uint32{2}, rng)

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
		removeFollows := []uint32{1}

		// update the DB
		commonFollows := []uint32{2}
		DB.NodeIndex[nodeID].Follows = commonFollows

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

		if err := RWM.updateRemovedNodes(DB, nodeID, removeFollows, commonFollows, rng); err != nil {
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
			addedFollows  []uint32
			newOutDegree  int
			expectedError error
		}{
			{
				name:          "nil RWM",
				DBType:        "one-node0",
				RWMType:       "nil",
				addedFollows:  []uint32{3},
				newOutDegree:  1,
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "empty RWM",
				DBType:        "one-node0",
				RWMType:       "empty",
				addedFollows:  []uint32{3},
				newOutDegree:  1,
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "node not found in the RWM",
				DBType:        "one-node0",
				RWMType:       "one-node1",
				addedFollows:  []uint32{3},
				newOutDegree:  1,
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "empty addedFollows",
				DBType:        "triangle",
				RWMType:       "triangle",
				addedFollows:  []uint32{},
				newOutDegree:  1,
				expectedError: nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mock.SetupDB(test.DBType)
				RWM := SetupRWM(test.RWMType)
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				err := RWM.updateAddedNodes(DB, 0, test.addedFollows, test.newOutDegree, rng)
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
		addedFollows := []uint32{2}

		// update the DB
		currentFollows := []uint32{2}
		DB.NodeIndex[nodeID].Follows = currentFollows

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

		if err := RWM.updateAddedNodes(DB, nodeID, addedFollows, len(currentFollows), rng); err != nil {
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
			name           string
			DBType         string
			RWMType        string
			nodeID         uint32
			oldFollows     []uint32
			currentFollows []uint32
			expectedError  error
		}{
			{
				name:           "nil DB",
				DBType:         "nil",
				RWMType:        "triangle",
				nodeID:         0,
				oldFollows:     []uint32{0},
				currentFollows: []uint32{1},
				expectedError:  models.ErrNilDBPointer,
			},
			{
				name:           "empty DB",
				DBType:         "empty",
				RWMType:        "triangle",
				nodeID:         0,
				oldFollows:     []uint32{0},
				currentFollows: []uint32{1},
				expectedError:  models.ErrNodeNotFoundDB,
			},
			{
				name:           "nil RWM",
				DBType:         "one-node0",
				RWMType:        "nil",
				nodeID:         0,
				oldFollows:     []uint32{0},
				currentFollows: []uint32{1},
				expectedError:  models.ErrNilRWSPointer,
			},
			{
				name:           "empty RWM",
				DBType:         "one-node0",
				RWMType:        "empty",
				nodeID:         0,
				oldFollows:     []uint32{0},
				currentFollows: []uint32{1},
				expectedError:  models.ErrNodeNotFoundRWS,
			},
			{
				name:           "node not found in the DB",
				DBType:         "one-node1",
				RWMType:        "one-node1",
				nodeID:         0,
				oldFollows:     []uint32{0},
				currentFollows: []uint32{1},
				expectedError:  models.ErrNodeNotFoundDB,
			},
			{
				name:           "node not found in the RWM",
				DBType:         "one-node0",
				RWMType:        "one-node1",
				nodeID:         0,
				oldFollows:     []uint32{0},
				currentFollows: []uint32{1},
				expectedError:  models.ErrNodeNotFoundRWS,
			},
			{
				name:           "oldFollows == currentFollows",
				DBType:         "triangle",
				RWMType:        "triangle",
				nodeID:         0,
				oldFollows:     []uint32{1},
				currentFollows: []uint32{1},
				expectedError:  nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mock.SetupDB(test.DBType)
				RWM := SetupRWM(test.RWMType)

				removed, common, added := sliceutils.Partition(test.oldFollows, test.currentFollows)

				err := RWM.Update(DB, test.nodeID, removed, common, added)
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
			oldFollows := DB1.NodeIndex[nodeID].Follows
			newFollows := DB2.NodeIndex[nodeID].Follows
			DB1.NodeIndex[nodeID].Follows = newFollows

			removed, common, added := sliceutils.Partition(oldFollows, newFollows)

			if err := RWM.Update(DB1, nodeID, removed, common, added); err != nil {
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

	removedMap := make(map[uint32][]uint32, nodesSize)
	addedMap := make(map[uint32][]uint32, nodesSize)
	commonMap := make(map[uint32][]uint32, nodesSize)
	b.Run("Update(), 10% new successors", func(b *testing.B) {
		// prepare the graph changes
		for nodeID := uint32(0); nodeID < uint32(nodesSize); nodeID++ {
			oldFollows, _ := DB.Follows(nodeID)
			currentFollows := make([]uint32, len(oldFollows))
			copy(currentFollows, oldFollows)

			// add 10% new nodes
			for i := 0; i < edgesPerNode/10; i++ {
				newNode := uint32(rng.Intn(nodesSize))
				addedMap[nodeID] = append(addedMap[nodeID], newNode)
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nodeID := uint32(i % nodesSize)

			err := RWM.Update(DB, nodeID, removedMap[nodeID], commonMap[nodeID], addedMap[nodeID])
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
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateDB(nodesSize, edgesPerNode, rng)
	RWM, _ := NewMockRWM(0.85, 10)
	RWM.GenerateAll(DB)

	b.Run("Update(), 10% removed successors", func(b *testing.B) {
		removedMap := make(map[uint32][]uint32, nodesSize)
		addedMap := make(map[uint32][]uint32, nodesSize)
		commonMap := make(map[uint32][]uint32, nodesSize)

		for nodeID := uint32(0); nodeID < uint32(nodesSize); nodeID++ {
			oldFollows, _ := DB.Follows(nodeID)

			// remove 10% of the nodes
			removedMap[nodeID] = oldFollows[edgesPerNode/10:]
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nodeID := uint32(i % nodesSize)

			err := RWM.Update(DB, nodeID, removedMap[nodeID], commonMap[nodeID], addedMap[nodeID])
			if err != nil {
				b.Fatalf("Update() failed: %v", err)
			}

		}
	})
}
