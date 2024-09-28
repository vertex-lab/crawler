package pagerank

import (
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
	mock "github.com/pippellia-btc/analytic_engine/pkg/mock_database"
)

func TestGenerateRandomWalks(t *testing.T) {

	t.Run("negative GenerateRandomWalks, nil db", func(t *testing.T) {

		var nil_db *mock.MockDatabase
		randomWalksMap := NewRandomWalksMap()

		err := randomWalksMap.GenerateRandomWalks(nil_db, 0.85, 1)

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative GenerateRandomWalks, empty db", func(t *testing.T) {

		empty_db := mock.NewMockDatabase()
		randomWalksMap := NewRandomWalksMap()

		err := randomWalksMap.GenerateRandomWalks(empty_db, 0.85, 1)

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative GenerateRandomWalks, nil rwm", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		var randomWalksMap *RandomWalksMap
		err := randomWalksMap.GenerateRandomWalks(db, 0.85, 1)

		if err != ErrNilRWMPointer {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative GenerateRandomWalks, non-empty rwm", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		// non empty rwm
		randomWalksMap := NewRandomWalksMap()
		walk := RandomWalk{NodeIDs: []uint32{0}}
		randomWalksMap.AddWalk(&walk)

		err := randomWalksMap.GenerateRandomWalks(db, 0.85, 1)

		if err != ErrRWMIsNotEmpty {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrRWMIsNotEmpty, err)
		}
	})

	t.Run("negative GenerateRandomWalks, invalid alphas", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		randomWalksMap := NewRandomWalksMap()
		invalidAlphas := []float32{1.0, -0.1, 0} // slice of invalid alphas

		for i := 0; i < 3; i++ {
			err := randomWalksMap.GenerateRandomWalks(db, invalidAlphas[i], 1)

			if err != ErrInvalidAlpha {
				t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrInvalidAlpha, err)
			}
		}
	})

	t.Run("negative GenerateRandomWalks, invalid walksPerNode", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		randomWalksMap := NewRandomWalksMap()
		err := randomWalksMap.GenerateRandomWalks(db, 0.85, 0)

		if err != ErrInvalidWalksPerNode {
			t.Errorf("GenerateRandomWalks(): expected %v, got %v", ErrInvalidWalksPerNode, err)
		}
	})

	t.Run("positive GenerateRandomWalks, 1 dandling node", func(t *testing.T) {

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{}}

		randomWalksMap := NewRandomWalksMap()
		err := randomWalksMap.GenerateRandomWalks(db, 0.85, 1)

		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		err = randomWalksMap.CheckEmpty() // check it before accessing rwm
		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		// get the walks of node 0
		walks, err_node := randomWalksMap.GetWalksByNodeID(0)
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

		db := mock.NewMockDatabase()
		db.Nodes[0] = &graph.Node{ID: 0, SuccessorsID: []uint32{1, 2}}
		db.Nodes[1] = &graph.Node{ID: 1, SuccessorsID: []uint32{}}
		db.Nodes[2] = &graph.Node{ID: 2, SuccessorsID: []uint32{0}}

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

		randomWalksMap := NewRandomWalksMap()
		err := randomWalksMap._generateRandomWalks(db, 0.85, 2, randomNumGen)

		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		err = randomWalksMap.CheckEmpty() // check it before accessing rwm
		if err != nil {
			t.Fatalf("GenerateRandomWalks(): expected nil, got %v", err)
		}

		nodeIDs, err := db.GetAllNodeIDs()
		if err != nil {
			t.Errorf("GenerateRandomWalks() -> GetAllNodeIDs(): expected nil, got %v", err)
		}

		// iterate over all nodes in the db
		for _, nodeID := range nodeIDs {

			// get the walks of a node
			walk_pointers, err := randomWalksMap.GetWalksByNodeID(nodeID)
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

func BenchmarkGenerateRandomWalks(b *testing.B) {

	db := generateBigDB(200000)
	b.ResetTimer() // to exclude the time to set up

	for i := 0; i < 1; i++ {

		rwm := NewRandomWalksMap()
		err := rwm.GenerateRandomWalks(db, 0.85, 10)
		if err != nil {
			b.Fatalf("GenerateRandomWalks() failed: %v", err)
		}
	}
}

// generates a randomly generated database of a specified size where the number
// of successors for each node is ~sqrt(size)
func generateBigDB(size uint32) *mock.MockDatabase {

	successorsPerNode := uint32(math.Round(math.Sqrt(float64(size))))

	db := mock.NewMockDatabase()
	for i := uint32(0); i < size; i++ {

		// create 10 random successors
		random_successors := make([]uint32, successorsPerNode)
		for j := uint32(0); j < successorsPerNode; j++ {
			random_successors[j] = uint32(rand.Intn(int(size)))
		}

		db.Nodes[i] = &graph.Node{ID: i, SuccessorsID: random_successors}
	}

	return db
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
