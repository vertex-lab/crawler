package mock

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
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
		PubKey             string
		expectedNodeID     uint32
		expectedNextNodeID uint32
		expectedError      error
		expectedNode       *models.Node
	}{
		{
			name:               "nil DB",
			DBType:             "nil",
			expectedNodeID:     math.MaxUint32,
			expectedNextNodeID: 0,
			expectedError:      models.ErrNilDBPointer,
		},
		{
			name:               "node already in the DB",
			DBType:             "simple-with-mock-pks",
			PubKey:             "one",
			expectedNodeID:     math.MaxUint32,
			expectedNextNodeID: 3,
			expectedError:      models.ErrNodeAlreadyInDB,
		},
		{
			name:               "valid",
			DBType:             "simple-with-mock-pks",
			PubKey:             "three",
			expectedNodeID:     3,
			expectedNextNodeID: 4,
			expectedNode:       &models.Node{PubKey: "three"},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			nodeID, err := DB.AddNode(test.PubKey, 0, nil, nil)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("AddNode(%v): expected %v, got %v", test.PubKey, test.expectedError, err)
			}

			// check if nodeID has been assigned correctly
			if nodeID != test.expectedNodeID {
				t.Errorf("AddNode(%v): expected nodeID = %v, got %v", test.PubKey, test.expectedNodeID, nodeID)
			}

			// check if DB internals have been changed correctly
			if DB != nil {
				if DB.NextNodeID != test.expectedNextNodeID {
					t.Errorf("AddNode(%v): expected NextNodeID = %v, got %v", test.PubKey, test.expectedNextNodeID, DB.NextNodeID)
				}

				if _, exist := DB.KeyIndex[test.PubKey]; !exist {
					t.Errorf("AddNode(%v): node was not added to the KeyIndex", test.PubKey)
				}
			}

			// check if data was added correctly
			if nodeID != math.MaxUint32 {
				node, err := DB.Node(nodeID)
				if err != nil {
					t.Fatalf("Node(%d): expected nil, got %v", nodeID, err)
				}

				if !reflect.DeepEqual(node, test.expectedNode) {
					t.Errorf("AddNode(%v): expected node %v \n got %v", test.PubKey, test.expectedNode, node)
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
			expectedNode:  &models.Node{Successors: []uint32{1}, Timestamp: 0},
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
				t.Fatalf("All(): expected %v, got %v", test.expectedError, err)
			}

			// if provided, check that the expected result is equal to the result
			if test.expectedSlice != nil {

				if nodeIDs == nil {
					t.Errorf("All(): expected %v, got nil", test.expectedSlice)
				}

				if !reflect.DeepEqual(nodeIDs, test.expectedSlice) {
					t.Errorf("All(): expected %v, got %v", test.expectedSlice, nodeIDs)
				}
			}
		})
	}
}

func TestNodeCount(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		expectedCount int
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedCount: 0,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedCount: 0,
		},
		{
			name:          "DB with node 0",
			DBType:        "one-node0",
			expectedCount: 1,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)
			count := DB.Size()

			if count != test.expectedCount {
				t.Errorf("Size(): expected %v, got %v", test.expectedCount, count)
			}
		})
	}
}

func TestInterface(t *testing.T) {
	var _ models.Database = &Database{}
}
