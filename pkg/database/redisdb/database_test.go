package redisdb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

func TestParseNode(t *testing.T) {
	testCases := []struct {
		name          string
		nodeMap       map[string]string
		expectedNode  *models.Node
		expectedError error
	}{
		{
			name:    "nil map",
			nodeMap: nil,
		},
		{
			name: "valid without follow record",
			nodeMap: map[string]string{
				NodeID:     "19",
				NodePubkey: "nineteen",
				NodeStatus: models.StatusActive,
			},
			expectedNode: &models.Node{
				ID:     19,
				Pubkey: "nineteen",
				Status: models.StatusActive,
			},
		},
		{
			name: "valid with follow record",
			nodeMap: map[string]string{
				NodeID:            "19",
				NodePubkey:        "nineteen",
				NodeStatus:        models.StatusActive,
				NodeFollowEventID: "dsaudsaiudsa",
				NodeFollowEventTS: "11",
			},
			expectedNode: &models.Node{
				ID:      19,
				Pubkey:  "nineteen",
				Status:  models.StatusActive,
				Records: []models.Record{{ID: "dsaudsaiudsa", Timestamp: 11, Type: models.Follow}},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			node, err := ParseNode(test.nodeMap)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("ParseNode(): expected %v got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(node, test.expectedNode) {
				t.Fatalf("ParseNode(): expected node %v got %v", test.expectedNode, node)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

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
			name:          "nil client",
			DBType:        "nil-client",
			expectedError: ErrNilClient,
		},
		{
			name:          "DB with node 0",
			DBType:        "one-node0",
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			err = DB.Validate()
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Validate(): expected %v, got %v", test.expectedError, err)
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
			name:          "pubkey not found",
			DBType:        "one-node0",
			nodeID:        1,
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:   "valid",
			DBType: "one-node0",
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
			ctx := context.Background()
			cl := redisutils.SetupTestClient()
			defer redisutils.CleanupRedis(cl)

			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			nodeMeta, err := DB.NodeByID(ctx, test.nodeID)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeByID(%v): expected %v, got %v", test.nodeID, test.expectedError, err)
			}

			if !reflect.DeepEqual(nodeMeta, test.expectedNode) {
				t.Errorf("NodeByID(%v): expected %v, got %v", test.nodeID, test.expectedNode, nodeMeta)
			}
		})
	}
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
			expectedError: redis.Nil,
		},
		{
			name:          "pubkey not found",
			DBType:        "one-node0",
			pubkey:        "one",
			expectedError: redis.Nil,
		},
		{
			name:   "valid",
			DBType: "one-node0",
			pubkey: "0",
			expectedNode: &models.Node{
				ID:     0,
				Pubkey: "0",
				Status: models.StatusInactive,
			},
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			cl := redisutils.SetupTestClient()
			defer redisutils.CleanupRedis(cl)

			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			nodeMeta, err := DB.NodeByKey(ctx, test.pubkey)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeByKey(%v): expected %v, got %v", test.pubkey, test.expectedError, err)
			}

			if !reflect.DeepEqual(nodeMeta, test.expectedNode) {
				t.Errorf("NodeByKey(%v): expected %v, got %v", test.pubkey, test.expectedNode, nodeMeta)
			}
		})
	}
}

func TestAddNode(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name           string
			DBType         string
			pubkey         string
			expectedNodeID uint32
			expectedError  error
		}{
			{
				name:           "nil DB",
				DBType:         "nil",
				pubkey:         "0",
				expectedNodeID: math.MaxUint32,
				expectedError:  models.ErrNilDB,
			},
			{
				name:           "node already in the DB",
				DBType:         "one-node0",
				pubkey:         "0",
				expectedNodeID: math.MaxUint32,
				expectedError:  models.ErrNodeAlreadyInDB,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				cl := redisutils.SetupTestClient()
				defer redisutils.CleanupRedis(cl)

				DB, err := SetupDB(cl, test.DBType)
				if err != nil {
					t.Fatalf("SetupDB(): expected nil, got %v", err)
				}

				nodeID, err := DB.AddNode(context.Background(), test.pubkey)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("AddNode(%v): expected %v, got %v", test.pubkey, test.expectedError, err)
				}

				// check if nodeID has been assigned correctly
				if nodeID != test.expectedNodeID {
					t.Errorf("AddNode(%v): expected nodeID = %v, got %v", test.pubkey, test.expectedNodeID, nodeID)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		ctx := context.Background()
		cl := redisutils.SetupTestClient()
		defer redisutils.CleanupRedis(cl)

		pubkey := "1"
		expectedNode := &models.Node{
			ID:     1,
			Pubkey: pubkey,
			Status: models.StatusInactive,
		}

		DB, err := SetupDB(cl, "one-node0")
		if err != nil {
			t.Fatalf("SetupDB(): expected nil, got %v", err)
		}

		nodeID, err := DB.AddNode(ctx, pubkey)
		if err != nil {
			t.Fatalf("AddNode(%s): expected nil, got %v", pubkey, err)
		}

		// check if nodeID has been assigned correctly
		if nodeID != expectedNode.ID {
			t.Errorf("AddNode(%s): expected nodeID = %v, got %v", pubkey, expectedNode.ID, nodeID)
		}

		// check if database HASH was updated correctly
		cmdReturnDB := cl.HMGet(ctx, KeyDatabase, KeyLastNodeID)
		if cmdReturnDB.Err() != nil {
			t.Errorf("HMGet(): expected nil, got %v", err)
		}
		var fields DatabaseFields
		if err := cmdReturnDB.Scan(&fields); err != nil {
			t.Errorf("Scan(): expected nil, got %v", err)
		}
		if fields.LastNodeID != int(expectedNode.ID) {
			t.Errorf("AddNode(%v): expected LastNodeID = %v, got %v", pubkey, expectedNode.ID, fields.LastNodeID)
		}

		// check if the node was added to the keyIndex correctly
		strNodeID, err := cl.HGet(ctx, KeyKeyIndex, pubkey).Result()
		if err != nil {
			t.Errorf("HGet(): expected nil, got %v", err)
		}
		LoadedNodeID, err := redisutils.ParseID(strNodeID)
		if err != nil {
			t.Errorf("ParseID(%v): expected nil, got %v", strNodeID, err)
		}
		if LoadedNodeID != expectedNode.ID {
			t.Errorf("AddNode(%s): expected nodeID = %v, got %v", pubkey, expectedNode.ID, nodeID)
		}

		node, err := DB.NodeByKey(ctx, pubkey)
		if err != nil {
			t.Fatalf("NodeByKey(%s): expected nil, got %v", pubkey, err)
		}

		if !reflect.DeepEqual(node, expectedNode) {
			t.Errorf("AddNode(): expected node %v \n got %v", expectedNode, node)
		}
	})
}

func TestUpdate(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		testCases := []struct {
			name          string
			DBType        string
			nodeID        uint32
			delta         *models.Delta
			expectedNode  *models.Node
			expectedError error
		}{
			{
				name:          "nil DB",
				DBType:        "nil",
				nodeID:        0,
				expectedError: models.ErrNilDB,
			},
			{
				name:          "node not found",
				DBType:        "one-node0",
				nodeID:        1,
				expectedError: models.ErrNodeNotFoundDB,
			},
			{
				name:   "valid promotion",
				DBType: "simple",
				nodeID: 0,
				delta: &models.Delta{
					Record: models.Record{Type: models.Promotion, Timestamp: 123},
				},
				expectedNode: &models.Node{
					ID:     0,
					Pubkey: "0",
					Status: models.StatusActive,
				},
			},
			{
				name:   "valid demotion",
				DBType: "simple",
				nodeID: 0,
				delta: &models.Delta{
					Record: models.Record{Type: models.Promotion, Timestamp: 123},
				},
				expectedNode: &models.Node{
					ID:     0,
					Pubkey: "0",
					Status: models.StatusActive,
				},
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				cl := redisutils.SetupTestClient()
				defer redisutils.CleanupRedis(cl)

				DB, err := SetupDB(cl, test.DBType)
				if err != nil {
					t.Fatalf("SetupDB(): expected nil, got %v", err)
				}

				err = DB.Update(ctx, test.nodeID, test.delta)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("Update(%v): expected %v, got %v", test.nodeID, test.expectedError, err)
				}

				// check if node was updated correctly
				if err == nil {
					node, err := DB.NodeByID(ctx, test.nodeID)
					if err != nil {
						t.Fatalf("NodeByID(%d): expected nil, got %v", test.nodeID, err)
					}

					if !reflect.DeepEqual(node, test.expectedNode) {
						t.Errorf("Update(): expected node %v \n got %v", test.expectedNode, &node)
					}
				}
			})
		}
	})

	t.Run("valid follows", func(t *testing.T) {
		ctx := context.Background()
		cl := redisutils.SetupTestClient()
		defer redisutils.CleanupRedis(cl)

		DB, err := SetupDB(cl, "simple")
		if err != nil {
			t.Fatalf("SetupDB(): expected nil, got %v", err)
		}

		var nodeID uint32
		delta := &models.Delta{
			Record:  models.Record{Type: models.Follow, Timestamp: 123, ID: "abc"},
			Removed: []uint32{1},
			Added:   []uint32{2},
		}

		if err := DB.Update(ctx, nodeID, delta); err != nil {
			t.Fatalf("Update(%d): expected nil, got %v", nodeID, err)
		}

		// check that the follow record changed
		expectedNode := &models.Node{
			ID:      0,
			Pubkey:  "0",
			Status:  models.StatusInactive,
			Records: []models.Record{delta.Record},
		}

		node, err := DB.NodeByID(ctx, nodeID)
		if err != nil {
			t.Fatalf("NodeByID(%d) expected nil, got %v", nodeID, err)
		}

		if !reflect.DeepEqual(node, expectedNode) {
			t.Fatalf("expected node %v, got %v", expectedNode, node)
		}

		// check the follows of nodeID
		follows, err := DB.client.SMembers(ctx, KeyFollows(nodeID)).Result()
		if err != nil {
			t.Fatalf("SMembers(%s) expected nil got %v", KeyFollows(nodeID), err)
		}
		if !reflect.DeepEqual(follows, []string{"2"}) {
			t.Fatalf("Expected follows %v, got %v", []string{"2"}, follows)
		}

		// check the followers of 1
		followers, err := DB.client.SMembers(ctx, KeyFollowers(1)).Result()
		if err != nil {
			t.Fatalf("SMembers(%s) expected nil got %v", KeyFollowers(1), err)
		}
		if !reflect.DeepEqual(followers, []string{}) {
			t.Fatalf("Expected follows %v, got %v", []string{"2"}, follows)
		}

		// check the followers of 2
		followers, err = DB.client.SMembers(ctx, KeyFollowers(2)).Result()
		if err != nil {
			t.Fatalf("SMembers(%s) expected nil got %v", KeyFollowers(2), err)
		}
		if !reflect.DeepEqual(followers, []string{"0"}) {
			t.Fatalf("Expected follows %v, got %v", []string{"0"}, follows)
		}
	})
}

func TestContainsNode(t *testing.T) {
	testCases := []struct {
		name             string
		DBType           string
		nodeID           uint32
		expectedContains bool
	}{
		{
			name:             "nil DB",
			DBType:           "nil",
			nodeID:           0,
			expectedContains: false,
		},
		{
			name:             "empty DB",
			DBType:           "empty",
			nodeID:           0,
			expectedContains: false,
		},
		{
			name:             "node not found",
			DBType:           "one-node0",
			nodeID:           1,
			expectedContains: false,
		},
		{
			name:             "node found",
			DBType:           "one-node0",
			nodeID:           0,
			expectedContains: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			cl := redisutils.SetupTestClient()
			defer redisutils.CleanupRedis(cl)

			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			contains := DB.ContainsNode(ctx, test.nodeID)
			if contains != test.expectedContains {
				t.Errorf("ContainsNode(%d): expected %v, got %v", test.nodeID, test.expectedContains, contains)
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
			name:          "empty DB",
			DBType:        "empty",
			nodeIDs:       []uint32{0},
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "node not found",
			DBType:        "one-node0",
			nodeIDs:       []uint32{69, 0},
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:            "dandling node",
			DBType:          "one-node0",
			nodeIDs:         []uint32{0},
			expectedFollows: [][]uint32{{}},
		},
		{
			name:            "valid",
			DBType:          "simple",
			nodeIDs:         []uint32{0, 1},
			expectedFollows: [][]uint32{{1}, {}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			cl := redisutils.SetupTestClient()
			defer redisutils.CleanupRedis(cl)

			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			follows, err := DB.Follows(context.Background(), test.nodeIDs...)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Follows(%d): expected %v, got %v", test.nodeIDs, test.expectedError, err)
			}

			if !reflect.DeepEqual(follows, test.expectedFollows) {
				t.Errorf("Follows(%d): expected %v, got %v", test.nodeIDs, test.expectedFollows, follows)
			}
		})
	}
}

func TestFollowers(t *testing.T) {
	testCases := []struct {
		name              string
		DBType            string
		nodeIDs           []uint32
		expectedError     error
		expectedFollowers [][]uint32
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
			nodeIDs:       []uint32{69, 0},
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:              "dandling node",
			DBType:            "one-node0",
			nodeIDs:           []uint32{0},
			expectedFollowers: [][]uint32{{}},
		},
		{
			name:              "valid",
			DBType:            "simple",
			nodeIDs:           []uint32{0, 1},
			expectedFollowers: [][]uint32{{}, {0}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			cl := redisutils.SetupTestClient()
			defer redisutils.CleanupRedis(cl)

			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			followers, err := DB.Followers(context.Background(), test.nodeIDs...)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Follows(%d): expected %v, got %v", test.nodeIDs, test.expectedError, err)
			}

			if !reflect.DeepEqual(followers, test.expectedFollowers) {
				t.Errorf("Follows(%d): expected %v, got %v", test.nodeIDs, test.expectedFollowers, followers)
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
			DBType:          "one-node0",
			pubkeys:         []string{"69"},
			expectedNodeIDs: []*uint32{nil},
		},
		{
			name:            "one pubkey found DB",
			DBType:          "one-node0",
			pubkeys:         []string{"0"},
			expectedNodeIDs: []*uint32{new(uint32)},
		},
		{
			name:            "valid",
			DBType:          "simple",
			pubkeys:         []string{"0", "1"},
			expectedNodeIDs: []*uint32{&[]uint32{0}[0], &[]uint32{1}[0]},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			cl := redisutils.SetupTestClient()
			defer redisutils.CleanupRedis(cl)

			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			nodeIDs, err := DB.NodeIDs(ctx, test.pubkeys...)
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
			name:            "one pubkey not found DB",
			DBType:          "one-node0",
			nodeIDs:         []uint32{4},
			expectedPubkeys: []*string{nil},
		},
		{
			name:            "one pubkey found DB",
			DBType:          "one-node0",
			nodeIDs:         []uint32{0},
			expectedError:   nil,
			expectedPubkeys: []*string{&[]string{"0"}[0]}, // a trick to add a pointer to "zero" inline
		},
		{
			name:            "valid",
			DBType:          "simple",
			nodeIDs:         []uint32{0, 1},
			expectedError:   nil,
			expectedPubkeys: []*string{&[]string{"0"}[0], &[]string{"1"}[0]}, // a trick to add a pointer to "zero" inline
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			cl := redisutils.SetupTestClient()
			defer redisutils.CleanupRedis(cl)

			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			pubkeys, err := DB.Pubkeys(ctx, test.nodeIDs...)
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
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

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
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			nodeIDs, err := DB.AllNodes(context.Background())
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("AllNodes(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(nodeIDs, test.expectedSlice) {
				t.Errorf("AllNodes(): expected %v, got %v", test.expectedSlice, nodeIDs)
			}
		})
	}
}

func TestScanNodes(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	type testCase struct {
		name            string
		DBType          string
		limit           int
		expectedError   error
		expectedNodeIDs []uint32
	}

	t.Run("simple errors", func(t *testing.T) {
		testCases := []testCase{
			{
				name:          "nil DB",
				DBType:        "nil",
				limit:         100,
				expectedError: models.ErrNilDB,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB, err := SetupDB(cl, test.DBType)
				if err != nil {
					t.Fatalf("SetupDB(): expected nil, got %v", err)
				}

				_, _, err = DB.ScanNodes(context.Background(), 0, test.limit)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("ScanNodes(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {

		testCases := []testCase{
			{
				name:            "one batch",
				DBType:          "one-node0",
				limit:           100,
				expectedNodeIDs: []uint32{0},
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB, err := SetupDB(cl, test.DBType)
				if err != nil {
					t.Fatalf("SetupDB(): expected nil, got %v", err)
				}

				var cursor uint64 = 0
				nodeIDs := []uint32{}
				for {

					res, cursor, err := DB.ScanNodes(context.Background(), cursor, test.limit)
					if err != nil {
						t.Fatalf("ScanNodes(): expected nil, got %v", err)
					}
					nodeIDs = append(nodeIDs, res...)

					if cursor == 0 {
						break
					}
				}

				if !reflect.DeepEqual(nodeIDs, test.expectedNodeIDs) {
					t.Errorf("ScanNodes(): expected %v, got %v", test.expectedNodeIDs, nodeIDs)
				}
			})
		}
	})
}

func TestSize(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

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
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

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

// ------------------------------------BENCHMARKS------------------------------

func BenchmarkNodeByKey(b *testing.B) {
	ctx := context.Background()
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	DB, err := SetupDB(cl, "one-node0")
	if err != nil {
		b.Fatalf("SetupDB(): expected nil, got %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := DB.NodeByKey(ctx, "zero")
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}

func BenchmarkNodeByID(b *testing.B) {
	ctx := context.Background()
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	DB, err := SetupDB(cl, "one-node0")
	if err != nil {
		b.Fatalf("SetupDB(): expected nil, got %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := DB.NodeByID(ctx, 0)
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}

func BenchmarkAllNodes(b *testing.B) {
	ctx := context.Background()
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))

	// Different DB sizes
	for _, nodesSize := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("DBSize=%d", nodesSize), func(b *testing.B) {
			cl := redisutils.SetupTestClient()
			defer redisutils.CleanupRedis(cl)

			DB, err := GenerateDB(cl, nodesSize, edgesPerNode, rng)
			if err != nil {
				b.Fatalf("GenerateDB(): expected nil, got %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := DB.AllNodes(ctx); err != nil {
					b.Fatalf("benchmark failed: %v", err)
				}
			}
		})
	}
}
