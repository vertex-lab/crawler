package mock

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/vertex-lab/crawler/pkg/models"
)

func TestValidate(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: models.ErrEmptyDB,
		},
		{
			name:          "DB with node 0",
			DBType:        "one-node0",
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)
			err := DB.Validate()

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Validate(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestContainsNode(t *testing.T) {
	testCases := []struct {
		name             string
		DBType           string
		expectedContains bool
	}{
		{
			name:             "nil DB",
			DBType:           "nil",
			expectedContains: false,
		},
		{
			name:             "empty DB",
			DBType:           "empty",
			expectedContains: false,
		},
		{
			name:             "node not found in DB",
			DBType:           "one-node0",
			expectedContains: false,
		},
		{
			name:             "node found in DB",
			DBType:           "one-node1",
			expectedContains: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			contains := DB.ContainsNode(1)
			if contains != test.expectedContains {
				t.Errorf("ContainsNode(1): expected %v, got %v", test.expectedContains, contains)
			}
		})
	}
}

func TestAddNode(t *testing.T) {
	testCases := []struct {
		name               string
		DBType             string
		Node               *models.Node
		expectedNodeID     uint32
		expectedLastNodeID int
		expectedError      error
	}{
		{
			name:               "nil DB",
			DBType:             "nil",
			expectedNodeID:     math.MaxUint32,
			expectedLastNodeID: -1,
			expectedError:      models.ErrNilDBPointer,
		},
		{
			name:               "node already in the DB",
			DBType:             "simple-with-mock-pks",
			expectedNodeID:     math.MaxUint32,
			expectedLastNodeID: 2,
			expectedError:      models.ErrNodeAlreadyInDB,
			Node: &models.Node{
				Metadata: models.NodeMeta{Pubkey: "one"}},
		},
		{
			name:               "valid",
			DBType:             "simple-with-mock-pks",
			expectedNodeID:     3,
			expectedLastNodeID: 3,
			Node: &models.Node{
				Metadata: models.NodeMeta{Pubkey: "three"}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			nodeID, err := DB.AddNode(test.Node)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("AddNode(%v): expected %v, got %v", test.Node, test.expectedError, err)
			}

			// check if nodeID has been assigned correctly
			if nodeID != test.expectedNodeID {
				t.Errorf("AddNode(%v): expected nodeID = %v, got %v", test.Node, test.expectedNodeID, nodeID)
			}

			// check if DB internals have been changed correctly
			if DB != nil {
				if DB.LastNodeID != test.expectedLastNodeID {
					t.Errorf("AddNode(%v): expected LastNodeID = %v, got %v", test.Node, test.expectedLastNodeID, DB.LastNodeID)
				}

				if _, exist := DB.KeyIndex[test.Node.Metadata.Pubkey]; !exist {
					t.Errorf("AddNode(%v): node was not added to the KeyIndex", test.Node.Metadata.Pubkey)
				}
			}

			// check if data was added correctly
			if nodeID != math.MaxUint32 {
				node := DB.NodeIndex[nodeID]

				if !reflect.DeepEqual(node, test.Node) {
					t.Errorf("AddNode(%v): expected node %v \n got %v", test.Node, test.Node, node)
				}
			}
		})
	}
}

func TestUpdateNode(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		nodeID        uint32
		nodeDiff      *models.NodeDiff
		expectedNode  *models.Node
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			nodeID:        0,
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "node not found",
			DBType:        "one-node0",
			nodeID:        1,
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:   "valid",
			DBType: "simple",
			nodeID: 0,
			nodeDiff: &models.NodeDiff{
				Metadata:    models.NodeMeta{Pubkey: "zero", EventTS: 11},
				AddedSucc:   []uint32{2},
				RemovedSucc: []uint32{1},
			},

			expectedNode: &models.Node{
				Metadata:   models.NodeMeta{Pubkey: "zero", EventTS: 11},
				Successors: []uint32{2},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			err := DB.UpdateNode(test.nodeID, test.nodeDiff)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("UpdateNode(%v): expected %v, got %v", test.nodeDiff, test.expectedError, err)
			}

			// check if node was updated correctly
			if err == nil {
				node := DB.NodeIndex[test.nodeID]

				if !reflect.DeepEqual(node, test.expectedNode) {
					t.Errorf("UpdateNode(%v): expected node %v \n got %v", test.nodeID, test.expectedNode, node)
				}
			}
		})
	}
}

func TestNodeByKey(t *testing.T) {
	testCases := []struct {
		name             string
		DBType           string
		pubkey           string
		expectedError    error
		expectedNodeMeta *models.NodeMeta
	}{
		{
			name:             "nil DB",
			DBType:           "nil",
			pubkey:           "zero",
			expectedError:    models.ErrNilDBPointer,
			expectedNodeMeta: &models.NodeMeta{},
		},
		{
			name:             "empty DB",
			DBType:           "empty",
			pubkey:           "zero",
			expectedError:    models.ErrEmptyDB,
			expectedNodeMeta: &models.NodeMeta{},
		},
		{
			name:             "node not found",
			DBType:           "simple-with-mock-pks",
			pubkey:           "three",
			expectedError:    models.ErrNodeNotFoundDB,
			expectedNodeMeta: &models.NodeMeta{},
		},
		{
			name:          "node found",
			DBType:        "simple-with-mock-pks",
			pubkey:        "zero",
			expectedError: nil,
			expectedNodeMeta: &models.NodeMeta{
				ID:       0,
				Pubkey:   "zero",
				EventTS:  0,
				Pagerank: 0.26,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)
			node, err := DB.NodeByKey(test.pubkey)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeByKey(1): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(test.expectedNodeMeta, node) {
				t.Errorf("NodeByKey(1): expected %v, got %v", test.expectedNodeMeta, node)
			}
		})
	}
}

func TestNodeByID(t *testing.T) {
	testCases := []struct {
		name             string
		DBType           string
		nodeID           uint32
		expectedError    error
		expectedNodeMeta *models.NodeMeta
	}{
		{
			name:             "nil DB",
			DBType:           "nil",
			nodeID:           0,
			expectedError:    models.ErrNilDBPointer,
			expectedNodeMeta: &models.NodeMeta{},
		},
		{
			name:             "empty DB",
			DBType:           "empty",
			nodeID:           0,
			expectedError:    models.ErrEmptyDB,
			expectedNodeMeta: &models.NodeMeta{},
		},
		{
			name:             "node not found",
			DBType:           "simple-with-mock-pks",
			nodeID:           3,
			expectedError:    models.ErrNodeNotFoundDB,
			expectedNodeMeta: &models.NodeMeta{},
		},
		{
			name:          "node found",
			DBType:        "simple-with-mock-pks",
			nodeID:        0,
			expectedError: nil,
			expectedNodeMeta: &models.NodeMeta{
				ID:       0,
				Pubkey:   "zero",
				EventTS:  0,
				Pagerank: 0.26,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)
			node, err := DB.NodeByID(test.nodeID)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeByID(1): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(test.expectedNodeMeta, node) {
				t.Errorf("NodeByID(1): expected %v, got %v", test.expectedNodeMeta, node)
			}
		})
	}
}

func TestRandomSuccessor(t *testing.T) {
	testCases := []struct {
		name           string
		DBType         string
		expectedError  error
		expectedNodeID uint32
	}{
		{
			name:           "nil DB",
			DBType:         "nil",
			expectedError:  models.ErrNilDBPointer,
			expectedNodeID: math.MaxUint32,
		},
		{
			name:           "empty DB",
			DBType:         "empty",
			expectedError:  models.ErrEmptyDB,
			expectedNodeID: math.MaxUint32,
		},
		{
			name:           "node not found",
			DBType:         "one-node1",
			expectedError:  models.ErrNodeNotFoundDB,
			expectedNodeID: math.MaxUint32,
		},
		{
			name:           "dandling",
			DBType:         "dandling",
			expectedError:  nil,
			expectedNodeID: math.MaxUint32,
		},
		{
			name:           "valid",
			DBType:         "one-node0",
			expectedError:  nil,
			expectedNodeID: 0,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := SetupDB(test.DBType)
			nodeID, err := DB.RandomSuccessor(0)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("RandomSuccessor(0): expected %v, got %v", test.expectedError, err)
			}

			if nodeID != test.expectedNodeID {
				t.Errorf("RandomSuccessor(0): expected %v, got %v", test.expectedNodeID, nodeID)
			}
		})
	}
}

func TestSuccessors(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		expectedError error
		expectedSlice []uint32
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: models.ErrEmptyDB,
		},
		{
			name:          "DB with node 0",
			DBType:        "one-node0",
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "DB with node 1",
			DBType:        "one-node1",
			expectedError: nil,
			expectedSlice: []uint32{1},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := SetupDB(test.DBType)
			Successors, err := DB.Successors(1)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Successors(1): expected %v, got %v", test.expectedError, err)
			}

			// if provided, check that the expected result is equal to the result
			if test.expectedSlice != nil {

				if Successors == nil {
					t.Errorf("Successors(1): expected %v, got nil", test.expectedSlice)
				}

				if !reflect.DeepEqual(Successors, test.expectedSlice) {
					t.Errorf("Successors(1): expected %v, got %v", test.expectedSlice, Successors)
				}
			}
		})
	}
}

func TestIsDandling(t *testing.T) {
	testCases := []struct {
		name       string
		DBType     string
		nodeID     uint32
		isDandling bool
	}{
		{
			name:       "nil DB",
			DBType:     "nil",
			nodeID:     0,
			isDandling: true,
		},
		{
			name:       "empty DB",
			DBType:     "empty",
			nodeID:     0,
			isDandling: true,
		},
		{
			name:       "node 0 not found",
			DBType:     "one-node1",
			nodeID:     0,
			isDandling: true,
		},
		{
			name:       "dandling node 1",
			DBType:     "simple",
			nodeID:     1,
			isDandling: true,
		},
		{
			name:       "non-dandling node 1",
			DBType:     "one-node1",
			nodeID:     1,
			isDandling: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := SetupDB(test.DBType)
			dandling := DB.IsDandling(test.nodeID)

			if dandling != test.isDandling {
				t.Errorf("IsDandling(): expected %v, got %v", test.isDandling, dandling)
			}
		})
	}
}

func TestNodeIDs(t *testing.T) {
	testCases := []struct {
		name            string
		DBType          string
		pubkeys         []string
		expectedError   error
		expectedNodeIDs []interface{}
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: models.ErrEmptyDB,
		},
		{
			name:            "one pubkey not found DB",
			DBType:          "simple-with-mock-pks",
			pubkeys:         []string{"four"},
			expectedError:   nil,
			expectedNodeIDs: []interface{}{nil},
		},
		{
			name:            "one pubkey found DB",
			DBType:          "simple-with-mock-pks",
			pubkeys:         []string{"one"},
			expectedError:   nil,
			expectedNodeIDs: []interface{}{uint32(1)},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			nodeIDs, err := DB.NodeIDs(test.pubkeys)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeIDs(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(nodeIDs, test.expectedNodeIDs) {
				t.Errorf("NodeIDs(): expected %v, got %v", test.expectedNodeIDs, nodeIDs)
			}
		})
	}
}

func TestPubkeys(t *testing.T) {
	testCases := []struct {
		name            string
		DBType          string
		nodeIDs         []uint32
		expectedError   error
		expectedPubkeys []interface{}
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: models.ErrEmptyDB,
		},
		{
			name:            "one nodeID not found DB",
			DBType:          "simple-with-mock-pks",
			nodeIDs:         []uint32{4},
			expectedError:   nil,
			expectedPubkeys: []interface{}{nil},
		},
		{
			name:            "one pubkey found DB",
			DBType:          "simple-with-mock-pks",
			nodeIDs:         []uint32{1},
			expectedError:   nil,
			expectedPubkeys: []interface{}{"one"},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			pubkeys, err := DB.Pubkeys(test.nodeIDs)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Pubkeys(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(pubkeys, test.expectedPubkeys) {
				t.Errorf("Pubkeys(): expected %v, got %v", test.expectedPubkeys, pubkeys)
			}
		})
	}
}

func TestAllNodes(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		expectedError error
		expectedSlice []uint32
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: models.ErrEmptyDB,
		},
		{
			name:          "DB with node 0",
			DBType:        "one-node0",
			expectedError: nil,
			expectedSlice: []uint32{0},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := SetupDB(test.DBType)
			nodeIDs, err := DB.AllNodes()

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("AllNodes(): expected %v, got %v", test.expectedError, err)
			}

			// if provided, check that the expected result is equal to the result
			if test.expectedSlice != nil {

				if nodeIDs == nil {
					t.Errorf("AllNodes(): expected %v, got nil", test.expectedSlice)
				}

				if !reflect.DeepEqual(nodeIDs, test.expectedSlice) {
					t.Errorf("AllNodes(): expected %v, got %v", test.expectedSlice, nodeIDs)
				}
			}
		})
	}
}

func TestSize(t *testing.T) {
	testCases := []struct {
		name         string
		DBType       string
		expectedSize int
	}{
		{
			name:         "nil DB",
			DBType:       "nil",
			expectedSize: 0,
		},
		{
			name:         "empty DB",
			DBType:       "empty",
			expectedSize: 0,
		},
		{
			name:         "DB with node 0",
			DBType:       "one-node0",
			expectedSize: 1,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)
			size := DB.Size()

			if size != test.expectedSize {
				t.Errorf("Size(): expected %v, got %v", test.expectedSize, size)
			}
		})
	}
}

func TestSetPagerank(t *testing.T) {

	type testCases struct {
		name          string
		DBType        string
		pagerank      models.PagerankMap
		expectedError error
	}

	t.Run("simple errors", func(t *testing.T) {
		testCases := []testCases{
			{
				name:          "nil DB",
				DBType:        "nil",
				pagerank:      models.PagerankMap{0: 1.0},
				expectedError: models.ErrNilDBPointer,
			},
			{
				name:          "empty DB",
				DBType:        "empty",
				pagerank:      models.PagerankMap{0: 1.0},
				expectedError: models.ErrEmptyDB,
			},
			{
				name:          "node not found DB",
				DBType:        "one-node0",
				pagerank:      models.PagerankMap{99: 1.0},
				expectedError: models.ErrNodeNotFoundDB,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				DB := SetupDB(test.DBType)
				err := DB.SetPagerank(test.pagerank)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("SetPagerank(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		testCases := []testCases{
			{
				name:          "valid",
				DBType:        "triangle",
				pagerank:      models.PagerankMap{0: 0.33, 1: 0.44, 2: 1.0}, // random values
				expectedError: nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := SetupDB(test.DBType)
				err := DB.SetPagerank(test.pagerank)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("SetPagerank(): expected %v, got %v", test.expectedError, err)
				}

				for nodeID, rank := range test.pagerank {
					p := DB.NodeIndex[nodeID].Metadata.Pagerank
					if p != rank {
						t.Errorf("Pagerank(%d): expected %v, got %v", nodeID, rank, p)
					}
				}

			})
		}
	})
}

func TestInterface(t *testing.T) {
	var _ models.Database = &Database{}
}
