package walks

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
)

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
				name:          "nil DB",
				DBType:        "nil",
				RWMType:       "triangle",
				removedSucc:   []uint32{1},
				expectedError: graph.ErrNilDatabasePointer,
			},
			{
				name:          "empty DB",
				DBType:        "empty",
				RWMType:       "triangle",
				removedSucc:   []uint32{1},
				expectedError: graph.ErrDatabaseIsEmpty,
			},
			{
				name:          "nil RWM",
				DBType:        "one-node0",
				RWMType:       "nil",
				removedSucc:   []uint32{3},
				expectedError: ErrNilRWMPointer,
			},
			{
				name:          "empty RWM",
				DBType:        "one-node0",
				RWMType:       "empty",
				removedSucc:   []uint32{3},
				expectedError: ErrEmptyRWM,
			},
			{
				name:          "node not found in the RWM",
				DBType:        "one-node0",
				RWMType:       "one-node1",
				removedSucc:   []uint32{3},
				expectedError: ErrNodeNotFoundRWM,
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
				RWM := setupRWM(test.RWMType)
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
		RWM := setupRWM("triangle")

		nodeID := uint32(0)
		removeSucc := []uint32{1}

		// update the DB
		commonSucc := []uint32{2}
		DB.Nodes[nodeID].SuccessorIDs = commonSucc

		rng := rand.New(rand.NewSource(5))
		expectedWalks := map[uint32][][]uint32{
			0: {
				{0, 2},
				{1, 2, 0},
				{2, 0},
			},
			1: {
				{1, 2, 0},
			},
			2: {
				{0, 2},
				{1, 2, 0},
				{2, 0},
			},
		}

		err := RWM.updateRemovedNodes(DB, nodeID, removeSucc, commonSucc, rng)
		if err != nil {
			t.Errorf("updateRemovedNodes(): expected nil, got %v", err)
		}

		for nodeID, expectedWalk := range expectedWalks {

			walkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("WalksByNodeID(%d): expected nil, got %v", nodeID, err)
			}

			// dereference walks and sort them in lexicographic order
			walks := SortWalks(walkSet)

			if !reflect.DeepEqual(walks, expectedWalk) {
				t.Errorf("updateRemovedNodes() nodeID = %d: expected %v, got %v", nodeID, expectedWalk, walks)
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
				name:          "nil DB",
				DBType:        "nil",
				RWMType:       "triangle",
				addedSucc:     []uint32{1},
				newOutDegree:  1,
				expectedError: graph.ErrNilDatabasePointer,
			},
			{
				name:          "empty DB",
				DBType:        "empty",
				RWMType:       "triangle",
				addedSucc:     []uint32{1},
				newOutDegree:  1,
				expectedError: graph.ErrDatabaseIsEmpty,
			},
			{
				name:          "nil RWM",
				DBType:        "one-node0",
				RWMType:       "nil",
				addedSucc:     []uint32{3},
				newOutDegree:  1,
				expectedError: ErrNilRWMPointer,
			},
			{
				name:          "empty RWM",
				DBType:        "one-node0",
				RWMType:       "empty",
				addedSucc:     []uint32{3},
				newOutDegree:  1,
				expectedError: ErrEmptyRWM,
			},
			{
				name:          "node not found in the RWM",
				DBType:        "one-node0",
				RWMType:       "one-node1",
				addedSucc:     []uint32{3},
				newOutDegree:  1,
				expectedError: ErrNodeNotFoundRWM,
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
				RWM := setupRWM(test.RWMType)
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				err := RWM.updateRemovedNodes(DB, 0, test.addedSucc, []uint32{2}, rng)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("updateAddedNodes(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {

		DB := mock.SetupDB("simple")
		RWM := setupRWM("simple")

		nodeID := uint32(0)
		addedSucc := []uint32{2}

		// update the DB
		currentSucc := []uint32{2}
		DB.Nodes[nodeID].SuccessorIDs = currentSucc

		rng := rand.New(rand.NewSource(5))
		expectedWalks := map[uint32][][]uint32{
			0: {
				{0, 2},
			},
			1: {},
			2: {
				{0, 2},
			},
		}

		err := RWM.updateAddedNodes(DB, nodeID, addedSucc, len(currentSucc), rng)
		if err != nil {
			t.Errorf("updateAddedNodes(): expected nil, got %v", err)
		}

		for nodeID, expectedWalk := range expectedWalks {

			walkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("WalksByNodeID(%d): expected nil, got %v", nodeID, err)
			}

			// dereference walks and sort them in lexicographic order
			walks := SortWalks(walkSet)

			if !reflect.DeepEqual(walks, expectedWalk) {
				t.Errorf("updateAddedNodes() nodeID = %d: expected %v, got %v", nodeID, expectedWalk, walks)
			}
		}
	})
}

func TestUpdate2(t *testing.T) {

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
				expectedError: graph.ErrNilDatabasePointer,
			},
			{
				name:          "empty DB",
				DBType:        "empty",
				RWMType:       "triangle",
				nodeID:        0,
				expectedError: graph.ErrDatabaseIsEmpty,
			},
			{
				name:          "nil RWM",
				DBType:        "one-node0",
				RWMType:       "nil",
				nodeID:        0,
				expectedError: ErrNilRWMPointer,
			},
			{
				name:          "empty RWM",
				DBType:        "one-node0",
				RWMType:       "empty",
				nodeID:        0,
				expectedError: ErrEmptyRWM,
			},
			{
				name:          "node not found in the DB",
				DBType:        "one-node1",
				RWMType:       "one-node1",
				nodeID:        0,
				expectedError: graph.ErrNodeNotFoundDB,
			},
			{
				name:          "node not found in the RWM",
				DBType:        "one-node0",
				RWMType:       "one-node1",
				nodeID:        0,
				oldSucc:       []uint32{0},
				currentSucc:   []uint32{1},
				expectedError: ErrNodeNotFoundRWM,
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
				RWM := setupRWM(test.RWMType)

				err := RWM.Update(DB, test.nodeID, test.oldSucc, test.currentSucc)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("updateAddedNodes(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})
}

// ---------------------------------BENCHMARKS---------------------------------

func BenchmarkUpdateAddedNodes(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)

	RWM, _ := NewRWM(0.85, 10)
	RWM.GenerateAll(DB)

	// store the changes here
	oldSuccessorMap := make(map[uint32][]uint32, nodesSize)
	currentSuccessorMap := make(map[uint32][]uint32, nodesSize)

	b.Run("Update(), 10% new successors", func(b *testing.B) {

		// prepare the graph changes
		for nodeID := uint32(0); nodeID < uint32(nodesSize); nodeID++ {

			oldSuccessorIDs, _ := DB.NodeSuccessorIDs(nodeID)
			currentSuccessorIDs := make([]uint32, len(oldSuccessorIDs))
			copy(currentSuccessorIDs, oldSuccessorIDs)

			// add 10% new nodes
			for i := 0; i < edgesPerNode/10; i++ {

				newNode := uint32(rng.Intn(nodesSize))
				currentSuccessorIDs = append(currentSuccessorIDs, newNode)
			}

			oldSuccessorMap[nodeID] = oldSuccessorIDs
			currentSuccessorMap[nodeID] = currentSuccessorIDs
		}

		b.ResetTimer()

		// perform benchmark
		for i := 0; i < b.N; i++ {

			nodeID := uint32(i % nodesSize)
			oldSuccessorIDs := oldSuccessorMap[nodeID]
			currentSuccessorIDs := currentSuccessorMap[nodeID]

			err := RWM.Update(DB, nodeID, oldSuccessorIDs, currentSuccessorIDs)
			if err != nil {
				b.Fatalf("Update() failed: %v", err)
			}
		}
	})
}

/*
!IMPORTANT!

run this benchmark with:

> -benchtime=nodesSizex

each node should only be updated once. Each subsequent update will be
much cheaper because no walk will need an update, thus compromizing the measurement
*/
func BenchmarkUpdateRemovedNodes(b *testing.B) {

	// initial setup
	nodesSize := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mock.GenerateMockDB(nodesSize, edgesPerNode, rng)

	RWM, _ := NewRWM(0.85, 10)
	RWM.GenerateAll(DB)

	// store the changes here
	oldSuccessorMap := make(map[uint32][]uint32, nodesSize)
	currentSuccessorMap := make(map[uint32][]uint32, nodesSize)

	b.Run("Update(), 10% removed successors", func(b *testing.B) {

		// prepare the graph changes
		for nodeID := uint32(0); nodeID < uint32(nodesSize); nodeID++ {

			oldSuccessorIDs, _ := DB.NodeSuccessorIDs(nodeID)
			currentSuccessorIDs := make([]uint32, len(oldSuccessorIDs)-edgesPerNode/10)

			// remove 10% of the nodes
			copy(currentSuccessorIDs, oldSuccessorIDs[edgesPerNode/10:])

			oldSuccessorMap[nodeID] = oldSuccessorIDs
			currentSuccessorMap[nodeID] = currentSuccessorIDs
		}

		b.ResetTimer()

		// perform benchmark
		for i := 0; i < b.N; i++ {

			nodeID := uint32(i % nodesSize)
			oldSuccessorIDs := oldSuccessorMap[nodeID]
			currentSuccessorIDs := currentSuccessorMap[nodeID]

			err := RWM.Update(DB, nodeID, oldSuccessorIDs, currentSuccessorIDs)
			if err != nil {
				b.Fatalf("Update() failed: %v", err)
			}

		}
	})
}
