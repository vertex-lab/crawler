package stochastictest

import (
	"math"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
)

// StaticSetup contains a graph database, and it's expected pagerank and personalized pagerank of node0.
type StaticSetup struct {
	DB                    *mock.Database
	expectedGlobal        models.PagerankMap
	expectedPersonalized0 models.PagerankMap
}

// Inverse() returns the inverse of the specified delta. If a delta and it's inverse
// are applied, the graph returns to its original state.
func Inverse(d *models.Delta) *models.Delta {
	return &models.Delta{
		Kind:    d.Kind,
		NodeID:  d.NodeID,
		Removed: d.Added,
		Added:   d.Removed,
	}
}

// Unchanged() returns the relationships of the specified node that won't be changed by the delta.
func Unchanged(DB *mock.Database, delta *models.Delta) []uint32 {
	follows := DB.Follow[delta.NodeID].ToSlice()
	changed := append(delta.Added, delta.Removed...)
	return sliceutils.Difference(follows, changed)
}

// Dandlings() returns a graph of 5 dandling nodes.
func Dandlings() (ST StaticSetup, Deltas []*models.Delta) {
	ST.DB = mock.NewDatabase()
	ST.DB.NodeIndex[0] = &models.Node{ID: 0}
	ST.DB.NodeIndex[1] = &models.Node{ID: 1}
	ST.DB.NodeIndex[2] = &models.Node{ID: 2}
	ST.DB.NodeIndex[3] = &models.Node{ID: 3}
	ST.DB.NodeIndex[4] = &models.Node{ID: 4}

	ST.DB.Follow[0] = mapset.NewSet[uint32]()
	ST.DB.Follow[1] = mapset.NewSet[uint32]()
	ST.DB.Follow[2] = mapset.NewSet[uint32]()
	ST.DB.Follow[3] = mapset.NewSet[uint32]()
	ST.DB.Follow[4] = mapset.NewSet[uint32]()

	ST.expectedGlobal = models.PagerankMap{0: 0.20, 1: 0.20, 2: 0.20, 3: 0.20, 4: 0.20}
	ST.expectedPersonalized0 = models.PagerankMap{0: 1.0, 1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0}

	// Because of symmetry, these are all the possible deltas.
	Deltas = []*models.Delta{
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{1}},
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{1, 2}},
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{1, 2, 3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{1, 2, 3, 4}},
	}

	return ST, Deltas
}

// Triangle() returns a graph of 3 nodes connected as a triangle (0 --> 1 --> 2 --> 0)
func Triangle() (ST StaticSetup, Deltas []*models.Delta) {
	ST.DB = mock.NewDatabase()
	ST.DB.NodeIndex[0] = &models.Node{ID: 0}
	ST.DB.NodeIndex[1] = &models.Node{ID: 1}
	ST.DB.NodeIndex[2] = &models.Node{ID: 2}

	ST.DB.Follow[0] = mapset.NewSet[uint32](1)
	ST.DB.Follow[1] = mapset.NewSet[uint32](2)
	ST.DB.Follow[2] = mapset.NewSet[uint32](0)

	ST.expectedGlobal = models.PagerankMap{0: 0.33333, 1: 0.33333, 2: 0.33333}
	ST.expectedPersonalized0 = models.PagerankMap{0: 0.38873, 1: 0.33042, 2: 0.28086}
	return ST, nil // no deltas since this graph is small and cyclic, and therefore shouldn't be used on the dynamic tests.
}

// TrianglePlusOne() returns a cyclic graph of 4 nodes. 3 nodes are in a triangle, and the last one is only followed by node0.
func TrianglePlusOne() (ST StaticSetup, Deltas []*models.Delta) {
	ST.DB = mock.NewDatabase()
	ST.DB.NodeIndex[0] = &models.Node{ID: 0}
	ST.DB.NodeIndex[1] = &models.Node{ID: 1}
	ST.DB.NodeIndex[2] = &models.Node{ID: 2}
	ST.DB.NodeIndex[3] = &models.Node{ID: 3}

	ST.DB.Follow[0] = mapset.NewSet[uint32](1, 3)
	ST.DB.Follow[1] = mapset.NewSet[uint32](2)
	ST.DB.Follow[2] = mapset.NewSet[uint32](0)
	ST.DB.Follow[3] = mapset.NewSet[uint32]()

	ST.expectedGlobal = models.PagerankMap{0: 0.29700319989476004, 1: 0.20616253803697476, 2: 0.2552206288779828, 3: 0.24161363319028237}
	ST.expectedPersonalized0 = models.PagerankMap{0: 0.45223, 1: 0.19220, 2: 0.16337, 3: 0.19220}
	return ST, nil // no deltas since this graph is small and cyclic, and therefore shouldn't be used on the dynamic tests.
}

func Acyclic1() (ST StaticSetup, Deltas []*models.Delta) {
	ST.DB = mock.NewDatabase()
	ST.DB.NodeIndex[0] = &models.Node{ID: 0}
	ST.DB.NodeIndex[1] = &models.Node{ID: 1}
	ST.DB.NodeIndex[2] = &models.Node{ID: 2}
	ST.DB.NodeIndex[3] = &models.Node{ID: 3}
	ST.DB.NodeIndex[4] = &models.Node{ID: 4}

	ST.DB.Follow[0] = mapset.NewSet[uint32](1, 2)
	ST.DB.Follow[1] = mapset.NewSet[uint32]()
	ST.DB.Follow[2] = mapset.NewSet[uint32](3)
	ST.DB.Follow[3] = mapset.NewSet[uint32](1)
	ST.DB.Follow[4] = mapset.NewSet[uint32]()

	ST.expectedGlobal = models.PagerankMap{0: 0.11185368285521291, 1: 0.36950360789646736, 2: 0.15943176539450626, 3: 0.24735726099860061, 4: 0.11185368285521291}
	ST.expectedPersonalized0 = models.PagerankMap{0: 0.39709199748768864, 1: 0.2906949630265446, 2: 0.16876345947470478, 3: 0.14344958001106195, 4: 0.0}

	Deltas = []*models.Delta{
		// simple removals
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1, 2}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{2}},

		// simple additions
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{4}},
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{3}},

		// additions and removals
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}, Added: []uint32{3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{2}, Added: []uint32{3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}, Added: []uint32{4}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{2}, Added: []uint32{4}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1, 2}, Added: []uint32{3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1, 2}, Added: []uint32{4}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1, 2}, Added: []uint32{3, 4}},

		// simple additions
		{NodeID: 4, Kind: nostr.KindFollowList, Added: []uint32{0}},
		{NodeID: 4, Kind: nostr.KindFollowList, Added: []uint32{1}},
		{NodeID: 4, Kind: nostr.KindFollowList, Added: []uint32{2}},
		{NodeID: 4, Kind: nostr.KindFollowList, Added: []uint32{3}},
		{NodeID: 4, Kind: nostr.KindFollowList, Added: []uint32{0, 1}},
	}

	return ST, Deltas
}

func Acyclic2() (ST StaticSetup, Deltas []*models.Delta) {
	ST.DB = mock.NewDatabase()
	ST.DB.NodeIndex[0] = &models.Node{ID: 0}
	ST.DB.NodeIndex[1] = &models.Node{ID: 1}
	ST.DB.NodeIndex[2] = &models.Node{ID: 2}
	ST.DB.NodeIndex[3] = &models.Node{ID: 3}
	ST.DB.NodeIndex[4] = &models.Node{ID: 4}
	ST.DB.NodeIndex[5] = &models.Node{ID: 5}

	ST.DB.Follow[0] = mapset.NewSet[uint32](1, 2)
	ST.DB.Follow[1] = mapset.NewSet[uint32]()
	ST.DB.Follow[2] = mapset.NewSet[uint32]()
	ST.DB.Follow[3] = mapset.NewSet[uint32]()
	ST.DB.Follow[4] = mapset.NewSet[uint32](3, 5)
	ST.DB.Follow[5] = mapset.NewSet[uint32]()

	ST.expectedGlobal = models.PagerankMap{0: 0.12987025255292317, 1: 0.18506487372353833, 2: 0.18506487372353833, 3: 0.18506487372353833, 4: 0.12987025255292317, 5: 0.18506487372353833}
	ST.expectedPersonalized0 = models.PagerankMap{0: 0.5405393205897051, 1: 0.22973033970514745, 2: 0.22973033970514745, 3: 0.0, 4: 0.0, 5: 0.0}

	Deltas = []*models.Delta{
		// simple removals
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{2}},

		// simple additions
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{4}},
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{5}},

		// additions and removals
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}, Added: []uint32{3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}, Added: []uint32{4}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}, Added: []uint32{3, 4}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{2}, Added: []uint32{3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}, Added: []uint32{5}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}, Added: []uint32{3, 5}},

		// simple additions
		{NodeID: 1, Kind: nostr.KindFollowList, Added: []uint32{2}},
		{NodeID: 1, Kind: nostr.KindFollowList, Added: []uint32{3}},
		{NodeID: 1, Kind: nostr.KindFollowList, Added: []uint32{4}},
	}

	return ST, Deltas
}

func Acyclic3() (ST StaticSetup, Deltas []*models.Delta) {
	ST.DB = mock.NewDatabase()
	ST.DB.NodeIndex[0] = &models.Node{ID: 0}
	ST.DB.NodeIndex[1] = &models.Node{ID: 1}
	ST.DB.NodeIndex[2] = &models.Node{ID: 2}
	ST.DB.NodeIndex[3] = &models.Node{ID: 3}

	ST.DB.Follow[0] = mapset.NewSet[uint32](1, 2)
	ST.DB.Follow[1] = mapset.NewSet[uint32]()
	ST.DB.Follow[2] = mapset.NewSet[uint32]()
	ST.DB.Follow[3] = mapset.NewSet[uint32](1, 2)

	ST.expectedGlobal = models.PagerankMap{0: 0.17543839772251532, 1: 0.32456160227748454, 2: 0.32456160227748454, 3: 0.17543839772251532}
	ST.expectedPersonalized0 = models.PagerankMap{0: 0.5405396591260619, 1: 0.22973017043696903, 2: 0.22973017043696903, 3: 0.0}

	Deltas = []*models.Delta{
		// simple removals
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{2}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1, 2}},

		// simple additions
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{3}},

		// additions and removals
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}, Added: []uint32{3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{2}, Added: []uint32{3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1, 2}, Added: []uint32{3}},
	}

	return ST, Deltas
}

func Acyclic4() (ST StaticSetup, Deltas []*models.Delta) {
	ST.DB = mock.NewDatabase()
	ST.DB.NodeIndex[0] = &models.Node{ID: 0}
	ST.DB.NodeIndex[1] = &models.Node{ID: 1}
	ST.DB.NodeIndex[2] = &models.Node{ID: 2}
	ST.DB.NodeIndex[3] = &models.Node{ID: 3}

	ST.DB.Follow[0] = mapset.NewSet[uint32](1, 2)
	ST.DB.Follow[1] = mapset.NewSet[uint32]()
	ST.DB.Follow[2] = mapset.NewSet[uint32]()
	ST.DB.Follow[3] = mapset.NewSet[uint32](1)

	ST.expectedGlobal = models.PagerankMap{0: 0.17543839772251535, 1: 0.3991232045549693, 2: 0.25, 3: 0.17543839772251535}
	ST.expectedPersonalized0 = models.PagerankMap{0: 0.5405396591260619, 1: 0.22973017043696903, 2: 0.22973017043696903, 3: 0.0}

	Deltas = []*models.Delta{
		// simple removals
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{2}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1, 2}},

		// simple additions
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{3}},

		// additions and removals
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}, Added: []uint32{3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{2}, Added: []uint32{3}},
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1, 2}, Added: []uint32{3}},
	}

	return ST, Deltas
}

// CyclicLong50() returns a cyclic graph with 50 nodes (0 --> 1 --> 2 --> ... --> 48 --> 49 --> 0)
func CyclicLong50() (ST StaticSetup, Deltas []*models.Delta) {
	ST.DB = mock.NewDatabase()
	ST.expectedGlobal = make(models.PagerankMap, 50)
	ST.expectedPersonalized0 = make(models.PagerankMap, 50)

	for ID := uint32(0); ID < 49; ID++ {
		ST.DB.NodeIndex[ID] = &models.Node{ID: ID}
		ST.DB.Follow[ID] = mapset.NewSet[uint32](ID + 1)

		ST.expectedGlobal[ID] = 1.0 / 50.0
		ST.expectedPersonalized0[ID] = 0.15 * math.Pow(0.85, float64(ID))
	}

	// closing the big cycle
	ST.DB.NodeIndex[49] = &models.Node{ID: 49}
	ST.DB.Follow[49] = mapset.NewSet[uint32](0)
	ST.expectedGlobal[49] = 1.0 / 50.0
	ST.expectedPersonalized0[49] = 0.15 * math.Pow(0.85, float64(49))

	// because of symmetry, these are all the possible changes that produce cycles non shorter than 25
	Deltas = []*models.Delta{
		// simple removals
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}},

		// simple additions
		{NodeID: 0, Kind: nostr.KindFollowList, Added: []uint32{25}},

		// additions and removals
		{NodeID: 0, Kind: nostr.KindFollowList, Removed: []uint32{1}, Added: []uint32{25}},
	}

	return ST, Deltas
}
