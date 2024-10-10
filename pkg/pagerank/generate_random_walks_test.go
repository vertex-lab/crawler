package pagerank

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
	mock "github.com/pippellia-btc/analytic_engine/pkg/mock_database"
)

func TestGenerateRandomWalks(t *testing.T) {

	t.Run("negative GenerateRandomWalks, nil DB", func(t *testing.T) {

		var DB *mock.MockDatabase // nil DB
		RWM, _ := NewRandomWalksMap(0.85, 1)

		err := RWM.GenerateRandomWalks(DB)

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative GenerateRandomWalks, empty DB", func(t *testing.T) {

		DB := mock.NewMockDatabase() // empty DB
		RWM, _ := NewRandomWalksMap(0.85, 1)

		err := RWM.GenerateRandomWalks(DB)

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative GenerateRandomWalks, nil RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		var RWM *RandomWalksMap // nil RWM
		err := RWM.GenerateRandomWalks(DB)

		if err != ErrNilRWMPointer {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative GenerateRandomWalks, non-empty RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		// non empty RWM
		RWM, _ := NewRandomWalksMap(0.85, 1)
		walk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&walk)

		err := RWM.GenerateRandomWalks(DB)

		if err != ErrNonEmptyRWM {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrNonEmptyRWM, err)
		}
	})

	t.Run("positive GenerateRandomWalks, 1 dandling node", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksMap(0.85, 1)
		err := RWM.GenerateRandomWalks(DB)

		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		// check before accessing RWM
		empty, err := RWM.IsEmpty()
		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		if empty {
			t.Fatalf("GenerateRandomWalks(): expected false, got %v", empty)
		}

		// get the walks of node 0
		walks, err_node := RWM.GetWalksByNodeID(0)
		if err_node != nil {
			t.Errorf("GenerateRandomWalks() -> GetWalksByNodeID(0): expected nil, got %v", err_node)
		}

		got := walks[0].NodeIDs
		want := []uint32{0}

		if len(got) != len(want) {
			t.Errorf("GenerateRandomWalks() -> GetWalksByNodeID(0): expected %v, got %v", want, got)
		}

		for i, nodeID := range got {
			if nodeID != want[i] {
				t.Fatalf("GenerateRandomWalks() -> GetWalksByNodeID(0): expected %v, got %v", want, got)
			}
		}
	})

	t.Run("positive GenerateRandomWalks, multiple nodes and walks", func(t *testing.T) {

		// There is something strange in this test. Roughly 85% of the times, this test passes.
		// However, ~15% of the times, it returns completely valid but unexpected walks.
		// This means there is likely an issue with the random number generator, which should
		// not be problematic in production. Further investigation is needed.

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}

		// 	to get the same results
		randomNumGen := rand.New(rand.NewSource(69))
		expectedWalks := map[uint32][][]uint32{
			0: {
				{0, 1},
				{0, 2},
				{2, 0, 1},
				{2, 0, 1},
			},
			1: {
				{0, 1},
				{1},
				{1},
				{2, 0, 1},
				{2, 0, 1},
			},
			2: {
				{0, 2},
				{2, 0, 1},
				{2, 0, 1},
			},
		}

		RWM, err := NewRandomWalksMap(0.85, 2)
		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		err = RWM.generateRandomWalks(DB, nil, randomNumGen)
		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		// check before accessing RWM
		empty, err := RWM.IsEmpty()
		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		if empty {
			t.Fatalf("GenerateRandomWalks(): expected false, got %v", empty)
		}

		nodeIDs, err := DB.GetAllNodeIDs()
		if err != nil {
			t.Errorf("GenerateRandomWalks() -> GetAllNodeIDs(): expected nil, got %v", err)
		}

		// iterate over all nodes in the DB
		for _, nodeID := range nodeIDs {

			// get the walks of a node
			walk_pointers, err := RWM.GetWalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("GenerateRandomWalks() -> GetWalksByNodeID(): expected nil, got %v", err)
			}

			// dereference walks and sort them in lexicographic order
			walks, err := sortWalks(walk_pointers)
			if err != nil {
				t.Errorf("GenerateRandomWalks(): expected nil, got %v", err)
			}

			if !reflect.DeepEqual(walks, expectedWalks[nodeID]) {
				t.Errorf("GenerateRandomWalks() nodeID = %d: expected %v, got %v", nodeID, expectedWalks[nodeID], walks)
			}
		}
	})
}

func TestGenerateWalk(t *testing.T) {

	t.Run("positive generateWalk(), triangle", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}

		rng := rand.New(rand.NewSource(420))

		walk, err := generateWalk(DB, 0, 0.85, rng)
		expectedWalk := []uint32{0, 1}

		if err != nil {
			t.Errorf("generateWalk(): expected nil, got %v", err)
		}

		if !reflect.DeepEqual(walk, expectedWalk) {
			t.Errorf("generateWalk(): expected %v, got %v", expectedWalk, walk)
		}
	})
}

func BenchmarkGenerateRandomWalks(b *testing.B) {

	DB := generateDB(200, 100)
	b.ResetTimer() // to exclude the time to set up

	for i := 0; i < b.N; i++ {

		RWM, _ := NewRandomWalksMap(0.85, 100)
		err := RWM.GenerateRandomWalks(DB)
		if err != nil {
			b.Fatalf("GenerateRandomWalks() failed: %v", err)
		}
	}
}

//-------------------------------HELPER-FUNCTIONS-------------------------------

// generates a randomly generated database of a specified number of nodes nodesNum
// and number of successors per node successorsPerNode
func generateDB(nodesNum uint32, successorsPerNode uint32) *mock.MockDatabase {

	DB := mock.NewMockDatabase()
	for i := uint32(0); i < nodesNum; i++ {

		// create random successors
		random_successors := make([]uint32, successorsPerNode)
		for j := uint32(0); j < successorsPerNode; j++ {
			random_successors[j] = uint32(rand.Intn(int(nodesNum)))
		}

		DB.Nodes[i] = &graph.Node{ID: i, SuccessorIDs: random_successors}
	}

	return DB
}

// dereferences the random walks and sorts them in lexicographic order
func sortWalks(walk_pointers []*RandomWalk) ([][]uint32, error) {

	if len(walk_pointers) == 0 {
		return nil, ErrEmptyWalk
	}

	// dereferencing the slice of pointers
	walks := [][]uint32{}

	for _, walk_pointer := range walk_pointers {
		walks = append(walks, walk_pointer.NodeIDs)
	}

	// Sort the walks lexicographically
	sort.Slice(walks, func(i, j int) bool {
		// Compare slices lexicographically
		for x := 0; x < len(walks[i]) && x < len(walks[j]); x++ {
			if walks[i][x] < walks[j][x] {
				return true
			} else if walks[i][x] > walks[j][x] {
				return false
			}
		}
		return len(walks[i]) < len(walks[j])
	})

	return walks, nil
}
