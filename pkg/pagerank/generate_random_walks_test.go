package pagerank

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pippellia-btc/analytic_engine/pkg/graph"
	mock "github.com/pippellia-btc/analytic_engine/pkg/mock_database"
)

func TestGenerateWalk(t *testing.T) {

	t.Run("positive generateWalk(), triangle", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}

		rng := rand.New(rand.NewSource(42))

		walk, err := generateWalk(DB, 0, 0.85, rng)
		expectedWalk := []uint32{0, 1, 2}

		if err != nil {
			t.Errorf("generateWalk(): expected nil, got %v", err)
		}

		if !reflect.DeepEqual(walk, expectedWalk) {
			t.Errorf("generateWalk(): expected %v, got %v", expectedWalk, walk)
		}
	})
}

func TestGenerateRandomWalks(t *testing.T) {

	t.Run("positive generateRandomWalks(), single node", func(t *testing.T) {

		RWM, err := NewRandomWalksManager(0.85, 2)
		if err != nil {
			t.Fatalf("generateRandomWalks(): expected nil, got %v", err)
		}

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{2}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}

		// for reproducibility
		rng := rand.New(rand.NewSource(42))
		expectedWalk := []uint32{0, 1, 2}

		err = RWM.generateRandomWalks(DB, []uint32{0}, rng)
		if err != nil {
			t.Fatalf("generateRandomWalks(): expected nil, got %v", err)
		}

		nodeIDs, _ := DB.AllNodeIDs()
		for _, nodeID := range nodeIDs {

			walkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Errorf("generateRandomWalks(): expected nil, got %v", err)
			}

			for walk := range walkSet.Iter() {
				if !reflect.DeepEqual(walk.NodeIDs, expectedWalk) {
					t.Errorf("generateRandomWalks(): expected %v, got %v", expectedWalk, walk)
				}
			}
		}
	})

	t.Run("positive generateRandomWalks(), multiple nodes", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{1, 2}}
		DB.Nodes[1] = &graph.Node{ID: 1, SuccessorIDs: []uint32{}}
		DB.Nodes[2] = &graph.Node{ID: 2, SuccessorIDs: []uint32{0}}

		// for reproducibility
		rng := rand.New(rand.NewSource(69))
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

		RWM, err := NewRandomWalksManager(0.85, 2)
		if err != nil {
			t.Fatalf("generateRandomWalks(): expected nil, got %v", err)
		}

		err = RWM.generateRandomWalks(DB, []uint32{0, 1, 2}, rng)
		if err != nil {
			t.Fatalf("generateRandomWalks(): expected nil, got %v", err)
		}

		nodeIDs, _ := DB.AllNodeIDs()
		for _, nodeID := range nodeIDs {

			walkSet, err := RWM.WalksByNodeID(nodeID)
			if err != nil {
				t.Fatalf("generateRandomWalks() -> WalksByNodeID(): expected nil, got %v", err)
			}

			// dereference walks and sort them in lexicographic order
			walks, err := sortWalks(walkSet)
			if err != nil {
				t.Errorf("generateRandomWalks(): expected nil, got %v", err)
			}

			if !reflect.DeepEqual(walks, expectedWalks[nodeID]) {
				t.Errorf("generateRandomWalks() nodeID = %d: expected %v, got %v", nodeID, expectedWalks[nodeID], walks)
			}
		}
	})
}

func TestGenerate(t *testing.T) {

	t.Run("negative Generate, nil DB", func(t *testing.T) {

		var DB *mock.MockDatabase //	nil DB

		RWM, _ := NewRandomWalksManager(0.85, 1)
		rWalk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&rWalk)

		err := RWM.Generate(DB, 0)

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("Generate(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative Generate, empty DB", func(t *testing.T) {

		DB := mock.NewMockDatabase() // empty DB

		RWM, _ := NewRandomWalksManager(0.85, 1)
		rWalk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&rWalk)

		err := RWM.Generate(DB, 0)

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("Generate(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative Generate, nil RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		var RWM *RandomWalksManager // nil RWM
		err := RWM.Generate(DB, 0)

		if err != ErrNilRWMPointer {
			t.Errorf("Generate(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative Generate, empty RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksManager(0.85, 1) // empty RWM
		err := RWM.Generate(DB, 0)

		if err != ErrEmptyRWM {
			t.Errorf("Generate(): expected %v, got %v", ErrEmptyRWM, err)
		}
	})

	t.Run("negative Generate, nodeID not in DB", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0}})
		RWM.WalksByNode[0] = walkSet

		invalidNodeID := uint32(999) // invalid nodeID
		err := RWM.Generate(DB, invalidNodeID)

		if err != graph.ErrNodeNotFoundDB {
			t.Errorf("Generate(): expected %v, got %v", graph.ErrNodeNotFoundDB, err)
		}
	})

	t.Run("positive Generate", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		RWM, _ := NewRandomWalksManager(0.85, 1)
		walkSet := mapset.NewSet(&RandomWalk{NodeIDs: []uint32{0}})
		RWM.WalksByNode[0] = walkSet

		// new node1 is added to the DB
		newNodeID := uint32(1)
		DB.Nodes[newNodeID] = &graph.Node{ID: newNodeID, SuccessorIDs: []uint32{}}

		err := RWM.Generate(DB, newNodeID)
		if err != nil {
			t.Errorf("Generate(): expected nil, got %v", err)
		}

		walkSet, err = RWM.WalksByNodeID(newNodeID)
		if err != nil {
			t.Errorf("Generate(): expected nil, got %v", err)
		}

		want := []uint32{1}
		for rWalk := range walkSet.Iter() {

			if !reflect.DeepEqual(rWalk.NodeIDs, want) {
				t.Errorf("Generate(): expected %v, got %v", want, walkSet)
			}
		}
	})
}

func TestGenerateAll(t *testing.T) {

	t.Run("negative GenerateAll, nil DB", func(t *testing.T) {

		var DB *mock.MockDatabase // nil DB
		RWM, _ := NewRandomWalksManager(0.85, 1)

		err := RWM.GenerateAll(DB)

		if err != graph.ErrNilDatabasePointer {
			t.Errorf("GenerateAll(): expected %v, got %v", graph.ErrNilDatabasePointer, err)
		}
	})

	t.Run("negative GenerateAll, empty DB", func(t *testing.T) {

		DB := mock.NewMockDatabase() // empty DB
		RWM, _ := NewRandomWalksManager(0.85, 1)

		err := RWM.GenerateAll(DB)

		if err != graph.ErrDatabaseIsEmpty {
			t.Errorf("GenerateAll(): expected %v, got %v", graph.ErrDatabaseIsEmpty, err)
		}
	})

	t.Run("negative GenerateAll, nil RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		var RWM *RandomWalksManager // nil RWM
		err := RWM.GenerateAll(DB)

		if err != ErrNilRWMPointer {
			t.Errorf("GenerateAll(): expected %v, got %v", ErrNilRWMPointer, err)
		}
	})

	t.Run("negative GenerateAll, non-empty RWM", func(t *testing.T) {

		DB := mock.NewMockDatabase()
		DB.Nodes[0] = &graph.Node{ID: 0, SuccessorIDs: []uint32{}}

		// non empty RWM
		RWM, _ := NewRandomWalksManager(0.85, 1)
		rWalk := RandomWalk{NodeIDs: []uint32{0}}
		RWM.AddWalk(&rWalk)

		err := RWM.GenerateAll(DB)
		if err != ErrNonEmptyRWM {
			t.Errorf("GenerateAll(): expected %v, got %v", ErrNonEmptyRWM, err)
		}
	})

}

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
	RWM, _ := NewRandomWalksManager(0.85, 10)

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

		RWM, _ := NewRandomWalksManager(0.85, 10)
		err := RWM.GenerateAll(DB)
		if err != nil {
			b.Fatalf("GenerateAll() failed: %v", err)
		}
	}
}

//--------------------------------HELPER-FUNCTION-------------------------------

// dereferences the random walks and sorts them in lexicographic order
func sortWalks(walkSet WalkSet) ([][]uint32, error) {

	walkPointers := walkSet.ToSlice()

	if len(walkPointers) == 0 {
		return nil, ErrEmptyRandomWalk
	}

	// dereferencing the slice of pointers
	walks := [][]uint32{}

	for _, walk_pointer := range walkPointers {
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
