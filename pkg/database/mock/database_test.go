package mock

import (
	"context"
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
			expectedError: models.ErrNilDB,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: nil,
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

			contains := DB.ContainsNode(context.Background(), 1)
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
		pubkey             string
		expectedNodeID     uint32
		expectedLastNodeID int
		expectedError      error
	}{
		{
			name:               "nil DB",
			DBType:             "nil",
			expectedNodeID:     math.MaxUint32,
			expectedLastNodeID: -1,
			expectedError:      models.ErrNilDB,
		},
		{
			name:               "node already in the DB",
			DBType:             "simple",
			pubkey:             "0",
			expectedNodeID:     math.MaxUint32,
			expectedLastNodeID: 2,
			expectedError:      models.ErrNodeAlreadyInDB,
		},
		{
			name:               "valid",
			DBType:             "simple",
			pubkey:             "4",
			expectedNodeID:     3,
			expectedLastNodeID: 3,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			nodeID, err := DB.AddNode(context.Background(), test.pubkey)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("AddNode(%v): expected %v, got %v", test.pubkey, test.expectedError, err)
			}

			// check if nodeID has been assigned correctly
			if nodeID != test.expectedNodeID {
				t.Errorf("AddNode(%v): expected nodeID = %v, got %v", test.pubkey, test.expectedNodeID, nodeID)
			}

			// check if DB internals have been changed correctly
			if DB != nil {
				if DB.LastNodeID != test.expectedLastNodeID {
					t.Errorf("AddNode(%v): expected LastNodeID = %v, got %v", test.pubkey, test.expectedLastNodeID, DB.LastNodeID)
				}

				if _, exist := DB.KeyIndex[test.pubkey]; !exist {
					t.Errorf("AddNode(%v): node was not added to the KeyIndex", test.pubkey)
				}
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		testCases := []struct {
			name          string
			DBType        string
			delta         *models.Delta
			expectedNode  *models.Node
			expectedError error
		}{
			{
				name:          "nil DB",
				DBType:        "nil",
				expectedError: models.ErrNilDB,
			},
			{
				name:          "nil delta",
				DBType:        "one-node0",
				delta:         nil,
				expectedError: models.ErrNilDelta,
			},
			{
				name:          "node not found",
				DBType:        "one-node0",
				delta:         &models.Delta{NodeID: 1},
				expectedError: models.ErrNodeNotFoundDB,
			},
			{
				name:   "valid promotion",
				DBType: "simple",
				delta: &models.Delta{
					NodeID: 0,
					Record: models.Record{Timestamp: 111, Type: models.Promotion},
				},

				expectedNode: &models.Node{
					ID:      0,
					Pubkey:  "0",
					Status:  models.StatusActive,
					Records: []models.Record{{Timestamp: 111, Type: models.Promotion}},
				},
			},
			{
				name:   "valid demotion",
				DBType: "simple",
				delta: &models.Delta{
					NodeID: 1,
					Record: models.Record{Timestamp: 111, Type: models.Demotion},
				},

				expectedNode: &models.Node{
					ID:      1,
					Pubkey:  "1",
					Status:  models.StatusInactive,
					Records: []models.Record{{Timestamp: 111, Type: models.Demotion}},
				},
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := SetupDB(test.DBType)

				err := DB.Update(context.Background(), test.delta)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("Update(%v): expected %v, got %v", test.delta, test.expectedError, err)
				}

				// check if node was updated correctly
				if err == nil {
					node := DB.NodeIndex[test.delta.NodeID]

					if !reflect.DeepEqual(node, test.expectedNode) {
						t.Errorf("UpdateNode(%v): expected node %v \n got %v", test.delta.NodeID, test.expectedNode, node)
					}
				}
			})
		}
	})

	t.Run("valid follows", func(t *testing.T) {
		DB := SetupDB("simple")
		delta := &models.Delta{
			NodeID:  0,
			Record:  models.Record{ID: "xxx", Timestamp: 111, Type: models.Follow},
			Removed: []uint32{1},
			Added:   []uint32{2},
		}

		if err := DB.Update(context.Background(), delta); err != nil {
			t.Fatalf("Update(%d): expected nil got %v", delta.NodeID, err)
		}

		if !reflect.DeepEqual(DB.NodeIndex[delta.NodeID].Records, []models.Record{delta.Record}) {
			t.Errorf("expected records %v, got %v", delta.Record, DB.NodeIndex[delta.NodeID].Records)
		}

		if !reflect.DeepEqual(DB.Follow[delta.NodeID].ToSlice(), []uint32{2}) {
			t.Errorf("expected follows %v, got %v", []uint32{2}, DB.Follow[delta.NodeID])
		}

		if DB.Follower[1].Cardinality() != 0 {
			t.Errorf("expected no followers of 1, got %v", DB.Follower[1])
		}
	})
}

func TestNodeByKey(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		pubkey        string
		expectedError error
		expectedNode  *models.Node
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			pubkey:        "zero",
			expectedError: models.ErrNilDB,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			pubkey:        "zero",
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "node not found",
			DBType:        "simple",
			pubkey:        "three",
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:   "valid",
			DBType: "simple",
			pubkey: "0",
			expectedNode: &models.Node{
				ID:     0,
				Pubkey: "0",
				Status: models.StatusInactive,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)
			node, err := DB.NodeByKey(context.Background(), test.pubkey)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeByKey(1): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(test.expectedNode, node) {
				t.Errorf("NodeByKey(1): expected %v, got %v", test.expectedNode, node)
			}
		})
	}
}

func TestNodeByID(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		nodeID        uint32
		expectedError error
		expectedNode  *models.Node
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			nodeID:        0,
			expectedError: models.ErrNilDB,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			nodeID:        0,
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "node not found",
			DBType:        "simple",
			nodeID:        3,
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:   "node found",
			DBType: "simple",
			nodeID: 0,
			expectedNode: &models.Node{
				ID:     0,
				Pubkey: "0",
				Status: models.StatusInactive,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)
			node, err := DB.NodeByID(context.Background(), test.nodeID)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeByID(1): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(test.expectedNode, node) {
				t.Errorf("NodeByID(1): expected %v, got %v", test.expectedNode, node)
			}
		})
	}
}

func TestFollows(t *testing.T) {
	testCases := []struct {
		name            string
		DBType          string
		nodeIDs         []uint32
		expectedError   error
		expectedFollows [][]uint32
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			nodeIDs:       []uint32{0},
			expectedError: models.ErrNilDB,
		},
		{
			name:            "empty nodeIDs",
			DBType:          "one-node0",
			nodeIDs:         []uint32{0},
			expectedFollows: [][]uint32{{}},
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			nodeIDs:       []uint32{0},
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "node not found",
			DBType:        "one-node0",
			nodeIDs:       []uint32{1},
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:            "valid",
			DBType:          "triangle",
			nodeIDs:         []uint32{0, 1},
			expectedFollows: [][]uint32{{1}, {2}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)
			follows, err := DB.Follows(context.Background(), test.nodeIDs...)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Follows(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(follows, test.expectedFollows) {
				t.Errorf("Follows(): expected %v, got %v", test.expectedFollows, follows)
			}
		})
	}
}

func TestFollowers(t *testing.T) {
	testCases := []struct {
		name            string
		DBType          string
		nodeIDs         []uint32
		expectedError   error
		expectedFollows [][]uint32
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			nodeIDs:       []uint32{0},
			expectedError: models.ErrNilDB,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			nodeIDs:       []uint32{0},
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "node not found",
			DBType:        "one-node0",
			nodeIDs:       []uint32{1},
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:            "valid",
			DBType:          "triangle",
			nodeIDs:         []uint32{0, 1},
			expectedFollows: [][]uint32{{2}, {0}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)
			follows, err := DB.Followers(context.Background(), test.nodeIDs...)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Followers(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(follows, test.expectedFollows) {
				t.Errorf("Followers(): expected %v, got %v", test.expectedFollows, follows)
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
		expectedNodeIDs []*uint32
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: models.ErrNilDB,
		},
		{
			name:            "one pubkey not found DB",
			DBType:          "simple",
			pubkeys:         []string{"four"},
			expectedNodeIDs: []*uint32{nil},
		},
		{
			name:            "one pubkey found DB",
			DBType:          "simple",
			pubkeys:         []string{"0"},
			expectedNodeIDs: []*uint32{new(uint32)},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			nodeIDs, err := DB.NodeIDs(context.Background(), test.pubkeys...)
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
		expectedPubkeys []*string
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: models.ErrNilDB,
		},
		{
			name:            "one nodeID not found DB",
			DBType:          "simple",
			nodeIDs:         []uint32{4},
			expectedPubkeys: []*string{nil},
		},
		{
			name:            "one pubkey found DB",
			DBType:          "simple",
			nodeIDs:         []uint32{1},
			expectedPubkeys: []*string{&[]string{"1"}[0]}, // did this trick to have a pointer to "one" inline
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := SetupDB(test.DBType)

			pubkeys, err := DB.Pubkeys(context.Background(), test.nodeIDs...)
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
			expectedError: models.ErrNilDB,
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
			nodeIDs, err := DB.AllNodes(context.Background())

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
			size := DB.Size(context.Background())

			if size != test.expectedSize {
				t.Errorf("Size(): expected %v, got %v", test.expectedSize, size)
			}
		})
	}
}

func TestInterface(t *testing.T) {
	var _ models.Database = &Database{}
}
