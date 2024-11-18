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
				Metadata: models.NodeMeta{PubKey: "one"}},
		},
		{
			name:               "valid",
			DBType:             "simple-with-mock-pks",
			expectedNodeID:     3,
			expectedLastNodeID: 3,
			Node: &models.Node{
				Metadata: models.NodeMeta{PubKey: "three"}},
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

				if _, exist := DB.KeyIndex[test.Node.Metadata.PubKey]; !exist {
					t.Errorf("AddNode(%v): node was not added to the KeyIndex", test.Node.Metadata.PubKey)
				}
			}

			// check if data was added correctly
			if nodeID != math.MaxUint32 {
				node, err := DB.Node(nodeID)
				if err != nil {
					t.Fatalf("Node(%d): expected nil, got %v", nodeID, err)
				}

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
		node          *models.Node
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
			node: &models.Node{
				Metadata: models.NodeMeta{PubKey: "zero", Timestamp: 11}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			err := DB.UpdateNode(test.nodeID, test.node)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("UpdateNode(%v): expected %v, got %v", test.node, test.expectedError, err)
			}

			// check if node was updated correctly
			if err == nil {
				node, err := DB.Node(test.nodeID)
				if err != nil {
					t.Fatalf("Node(%d): expected nil, got %v", test.nodeID, err)
				}

				if !reflect.DeepEqual(node, test.node) {
					t.Errorf("UpdateNode(%v): expected node %v \n got %v", test.nodeID, test.node, node)
				}
			}
		})
	}
}

func TestNode(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		expectedError error
		expectedNode  *models.Node
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
			expectedNode: &models.Node{
				Metadata:   models.NodeMeta{Timestamp: 0},
				Successors: []uint32{1}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)
			node, err := DB.Node(1)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Node(1): expected %v, got %v", test.expectedError, err)
			}

			// if provided, check that the expected result is equal to the result
			if test.expectedNode != nil {

				if node == nil {
					t.Fatalf("Node(1): expected node, got nil")
				}

				if !reflect.DeepEqual(test.expectedNode, node) {
					t.Errorf("Node(1): expected %v, got %v", test.expectedNode, node)
				}
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

func TestNodeCache(t *testing.T) {
	testCases := []struct {
		name              string
		DBType            string
		expectedError     error
		expectedNodeCache models.NodeCache
	}{
		{
			name:              "nil DB",
			DBType:            "nil",
			expectedError:     models.ErrNilDBPointer,
			expectedNodeCache: nil,
		},
		{
			name:              "empty DB",
			DBType:            "empty",
			expectedError:     models.ErrEmptyDB,
			expectedNodeCache: nil,
		},
		{
			name:          "valid DB",
			DBType:        "simple-with-mock-pks",
			expectedError: nil,
			expectedNodeCache: models.NodeCache{
				"zero": models.NodeFilterAttributes{ID: 0, Timestamp: 0, Pagerank: 0.26},
				"one":  models.NodeFilterAttributes{ID: 1, Timestamp: 0, Pagerank: 0.48},
				"two":  models.NodeFilterAttributes{ID: 2, Timestamp: 0, Pagerank: 0.26},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			NC, err := DB.NodeCache()
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeCache(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(NC, test.expectedNodeCache) {
				t.Errorf("NodeCache(): expected %v, got %v", test.expectedNodeCache, NC)
			}
		})
	}
}

func TestInterface(t *testing.T) {
	var _ models.Database = &Database{}
}
