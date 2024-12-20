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
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "nil client",
			DBType:        "nil-client",
			expectedError: models.ErrNilClientPointer,
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

func TestAddFollows(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	nodeID := uint32(0)
	addedFollows := []uint32{1, 2, 3}

	pipe := cl.TxPipeline()
	ctx := context.Background()

	AddFollows(ctx, pipe, nodeID, addedFollows)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec(): expected nil, got %v", err)
	}

	// check the follows of nodeID are correctly added
	follows, err := cl.SMembers(ctx, KeyFollows(nodeID)).Result()
	if err != nil {
		t.Errorf("SMembers(): expected nil, got %v", err)
	}
	// parse the follows
	followIDs := make([]uint32, 0, len(follows))
	for _, follow := range follows {
		followID, err := redisutils.ParseID(follow)
		if err != nil {
			t.Fatalf("ParseID(%v): expected nil, got %v", follow, err)
		}
		followIDs = append(followIDs, followID)
	}

	if !reflect.DeepEqual(followIDs, addedFollows) {
		t.Fatalf("AddFollows(): expected %v, got %v", addedFollows, followIDs)
	}

	// check the follows have nodeID as a follower
	for _, ID := range addedFollows {

		isMember, err := cl.SIsMember(ctx, KeyFollowers(ID), redisutils.FormatID(nodeID)).Result()
		if err != nil {
			t.Errorf("IsMember(): expected nil, got %v", err)
		}

		if !isMember {
			t.Fatalf("AddFollows(): expected nodeID = %d part of followers:%d", nodeID, ID)
		}
	}
}

func TestRemoveFollows(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	nodeID := uint32(0)
	succ := []uint32{0, 1, 2}
	removedSucc := []uint32{1, 2, 3}
	remainingSucc := []uint32{0} // {0,1,2} - {1,2,3} = {0}

	// add succ to Redis
	pipe := cl.TxPipeline()
	ctx := context.Background()
	AddFollows(ctx, pipe, nodeID, succ)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec(): expected nil, got %v", err)
	}

	// remove succ from Redis
	pipe = cl.TxPipeline()
	ctx = context.Background()
	RemoveFollows(ctx, pipe, nodeID, removedSucc)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec(): expected nil, got %v", err)
	}

	// check the follows of nodeID are correctly added
	follows, err := cl.SMembers(ctx, KeyFollows(nodeID)).Result()
	if err != nil {
		t.Errorf("SMembers(): expected nil, got %v", err)
	}
	// parse the follows
	followIDs := make([]uint32, 0, len(follows))
	for _, follow := range follows {
		followID, err := redisutils.ParseID(follow)
		if err != nil {
			t.Fatalf("ParseID(%v): expected nil, got %v", follow, err)
		}
		followIDs = append(followIDs, followID)
	}

	if !reflect.DeepEqual(followIDs, remainingSucc) {
		t.Fatalf("RemoveFollows(): expected %v, got %v", remainingSucc, followIDs)
	}

	// check the remainingSucc have nodeID as a follower
	for _, succ := range remainingSucc {
		isMember, err := cl.SIsMember(ctx, KeyFollowers(succ), redisutils.FormatID(nodeID)).Result()
		if err != nil {
			t.Errorf("IsMember(): expected nil, got %v", err)
		}

		if !isMember {
			t.Fatalf("RemoveFollows(): expected nodeID = %d part of followers:%d", nodeID, succ)
		}
	}

	// check the removedSucc DON'T have nodeID as a follower
	for _, succ := range removedSucc {
		isMember, err := cl.SIsMember(ctx, KeyFollowers(succ), redisutils.FormatID(nodeID)).Result()
		if err != nil {
			t.Errorf("IsMember(): expected nil, got %v", err)
		}

		if isMember {
			t.Fatalf("RemoveFollows(): expected nodeID = %d NOT part of followers:%d", nodeID, succ)
		}
	}
}

func TestAddFollowers(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	nodeID := uint32(0)
	followers := []uint32{1, 2, 3}

	pipe := cl.TxPipeline()
	ctx := context.Background()

	AddFollowers(ctx, pipe, nodeID, followers)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec(): expected nil, got %v", err)
	}

	// check the followers of nodeID are correctly added
	strIDs, err := cl.SMembers(ctx, KeyFollowers(nodeID)).Result()
	if err != nil {
		t.Errorf("SMembers(): expected nil, got %v", err)
	}
	// parse the follows
	f := make([]uint32, 0, len(strIDs))
	for _, strID := range strIDs {
		ID, err := redisutils.ParseID(strID)
		if err != nil {
			t.Fatalf("ParseID(%v): expected nil, got %v", strID, err)
		}
		f = append(f, ID)
	}

	if !reflect.DeepEqual(f, followers) {
		t.Fatalf("AddFollows(): expected %v, got %v", followers, f)
	}

	// check the pred have nodeID as a follows
	for _, pred := range followers {

		isMember, err := cl.SIsMember(ctx, KeyFollows(pred), redisutils.FormatID(nodeID)).Result()
		if err != nil {
			t.Errorf("IsMember(): expected nil, got %v", err)
		}

		if !isMember {
			t.Fatalf("AddFollows(): expected nodeID = %d part of follows:%d", nodeID, pred)
		}
	}
}

func TestNodeByID(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

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
			expectedNodeMeta: &models.NodeMeta{},
			expectedError:    models.ErrNilDBPointer,
		},
		{
			name:             "empty DB",
			DBType:           "empty",
			nodeID:           0,
			expectedNodeMeta: &models.NodeMeta{},
			expectedError:    redis.Nil,
		},
		{
			name:             "pubkey not found",
			DBType:           "one-node0",
			nodeID:           1,
			expectedNodeMeta: &models.NodeMeta{},
			expectedError:    redis.Nil,
		},
		{
			name:   "valid",
			DBType: "one-node0",
			nodeID: 0,
			expectedNodeMeta: &models.NodeMeta{
				ID:       0,
				Pubkey:   "zero",
				EventTS:  1731685733,
				Status:   "idk",
				Pagerank: 1.0,
			},
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			nodeMeta, err := DB.NodeByID(context.Background(), test.nodeID)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeByID(%v): expected %v, got %v", test.nodeID, test.expectedError, err)
			}

			if !reflect.DeepEqual(nodeMeta, test.expectedNodeMeta) {
				t.Errorf("NodeByID(%v): expected %v, got %v", test.nodeID, test.expectedNodeMeta, nodeMeta)
			}
		})
	}
}

func TestNodeByKey(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

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
			expectedNodeMeta: &models.NodeMeta{},
			expectedError:    models.ErrNilDBPointer,
		},
		{
			name:             "empty DB",
			DBType:           "empty",
			pubkey:           "zero",
			expectedNodeMeta: &models.NodeMeta{},
			expectedError:    redis.Nil,
		},
		{
			name:             "pubkey not found",
			DBType:           "one-node0",
			pubkey:           "one",
			expectedNodeMeta: &models.NodeMeta{},
			expectedError:    redis.Nil,
		},
		{
			name:   "valid",
			DBType: "one-node0",
			pubkey: "zero",
			expectedNodeMeta: &models.NodeMeta{
				ID:       0,
				Pubkey:   "zero",
				EventTS:  1731685733,
				Status:   "idk",
				Pagerank: 1.0,
			},
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			nodeMeta, err := DB.NodeByKey(context.Background(), test.pubkey)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeByKey(%v): expected %v, got %v", test.pubkey, test.expectedError, err)
			}

			if !reflect.DeepEqual(nodeMeta, test.expectedNodeMeta) {
				t.Errorf("NodeByKey(%v): expected %v, got %v", test.pubkey, test.expectedNodeMeta, nodeMeta)
			}
		})
	}
}

func TestAddNode(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name           string
			DBType         string
			expectedNodeID uint32
			expectedError  error
			Node           *models.Node
		}{
			{
				name:           "nil DB",
				DBType:         "nil",
				expectedNodeID: math.MaxUint32,
				expectedError:  models.ErrNilDBPointer,
				Node:           &models.Node{},
			},
			{
				name:           "node already in the DB",
				DBType:         "one-node0",
				expectedNodeID: math.MaxUint32,
				expectedError:  models.ErrNodeAlreadyInDB,
				Node:           &models.Node{Metadata: models.NodeMeta{Pubkey: "zero"}},
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

				nodeID, err := DB.AddNode(context.Background(), test.Node)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("AddNode(%v): expected %v, got %v", test.Node, test.expectedError, err)
				}

				// check if nodeID has been assigned correctly
				if nodeID != test.expectedNodeID {
					t.Errorf("AddNode(%v): expected nodeID = %v, got %v", test.Node, test.expectedNodeID, nodeID)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		testCases := []struct {
			name               string
			DBType             string
			Node               *models.Node
			expectedNodeID     uint32
			expectedLastNodeID int
			expectedError      error
		}{
			{
				name:               "just Pubkey",
				DBType:             "one-node0",
				Node:               &models.Node{Metadata: models.NodeMeta{Pubkey: "one"}},
				expectedNodeID:     1,
				expectedLastNodeID: 1,
			},
			{
				name:               "all meta fields",
				DBType:             "one-node0",
				expectedNodeID:     1,
				expectedLastNodeID: 1,
				Node: &models.Node{
					Metadata: models.NodeMeta{
						Pubkey:   "one",
						EventTS:  0,
						Status:   "not-crawled",
						Pagerank: 0.0,
					},
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

				nodeID, err := DB.AddNode(ctx, test.Node)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("AddNode(%v): expected %v, got %v", test.Node, test.expectedError, err)
				}

				// check if nodeID has been assigned correctly
				if nodeID != test.expectedNodeID {
					t.Errorf("AddNode(%v): expected nodeID = %v, got %v", test.Node, test.expectedNodeID, nodeID)
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
				if fields.LastNodeID != test.expectedLastNodeID {
					t.Errorf("AddNode(%v): expected LastNodeID = %v, got %v", test.Node, test.expectedLastNodeID, fields.LastNodeID)
				}

				// check if the node was added to the keyIndex correctly
				strNodeID, err := cl.HGet(ctx, KeyKeyIndex, test.Node.Metadata.Pubkey).Result()
				if err != nil {
					t.Errorf("HGet(): expected nil, got %v", err)
				}
				LoadedNodeID, err := redisutils.ParseID(strNodeID)
				if err != nil {
					t.Errorf("ParseID(%v): expected nil, got %v", strNodeID, err)
				}
				if LoadedNodeID != test.expectedNodeID {
					t.Errorf("AddNode(%v): expected nodeID = %v, got %v", test.Node, test.expectedNodeID, nodeID)
				}

				// check if node HASH was added correctly
				cmdReturnNode := cl.HGetAll(ctx, KeyNode(test.expectedNodeID))
				if cmdReturnNode.Err() != nil {
					t.Errorf("HGetAll(): expected nil, got %v", err)
				}
				var nodeMeta models.NodeMeta
				if err := cmdReturnNode.Scan(&nodeMeta); err != nil {
					t.Errorf("Scan(): expected nil, got %v", err)
				}
				if !reflect.DeepEqual(nodeMeta, test.Node.Metadata) {
					t.Errorf("AddNode(): expected node %v \n got %v", test.Node.Metadata, nodeMeta)
				}
			})
		}
	})
}

func TestUpdateNode(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name             string
		DBType           string
		nodeID           uint32
		nodeDiff         *models.NodeDiff
		expectedNodeMeta *models.NodeMeta
		expectedError    error
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
			DBType: "one-node0",
			nodeID: 0,
			nodeDiff: &models.NodeDiff{
				Metadata: models.NodeMeta{EventTS: 11}},
			expectedNodeMeta: &models.NodeMeta{
				Pubkey:   "zero",
				EventTS:  11, // the only field that changes
				Status:   "idk",
				Pagerank: 1.0,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			err = DB.UpdateNode(ctx, test.nodeID, test.nodeDiff)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("UpdateNode(%v): expected %v, got %v", test.nodeDiff, test.expectedError, err)
			}

			// check if node was updated correctly
			if err == nil {
				cmdReturn := cl.HGetAll(ctx, KeyNode(test.nodeID))
				if cmdReturn.Err() != nil {
					t.Errorf("HGetAll(): expected nil, got %v", err)
				}

				var nodeMeta models.NodeMeta
				if err := cmdReturn.Scan(&nodeMeta); err != nil {
					t.Errorf("Scan(): expected nil, got %v", err)
				}

				if !reflect.DeepEqual(&nodeMeta, test.expectedNodeMeta) {
					t.Errorf("UpdateNode(): expected node %v \n got %v", test.expectedNodeMeta, &nodeMeta)
				}
			}
		})
	}
}

func TestContainsNode(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

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
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			contains := DB.ContainsNode(context.Background(), test.nodeID)
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
			name:            "nil DB",
			DBType:          "nil",
			nodeIDs:         []uint32{0},
			expectedFollows: nil,
			expectedError:   models.ErrNilDBPointer,
		},
		{
			name:            "empty DB",
			DBType:          "empty",
			nodeIDs:         []uint32{0},
			expectedFollows: nil,
			expectedError:   models.ErrNodeNotFoundDB,
		},
		{
			name:            "node not found",
			DBType:          "triangle",
			nodeIDs:         []uint32{69, 2},
			expectedFollows: nil,
			expectedError:   models.ErrNodeNotFoundDB,
		},
		{
			name:            "dandling node",
			DBType:          "dandling",
			nodeIDs:         []uint32{0},
			expectedFollows: [][]uint32{{}},
			expectedError:   nil,
		},
		{
			name:            "valid",
			DBType:          "triangle",
			nodeIDs:         []uint32{0, 1},
			expectedFollows: [][]uint32{{1}, {2}},
			expectedError:   nil,
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
			name:              "nil DB",
			DBType:            "nil",
			nodeIDs:           []uint32{0},
			expectedFollowers: nil,
			expectedError:     models.ErrNilDBPointer,
		},
		{
			name:              "empty DB",
			DBType:            "empty",
			nodeIDs:           []uint32{0},
			expectedFollowers: nil,
			expectedError:     models.ErrNodeNotFoundDB,
		},
		{
			name:              "node not found",
			DBType:            "triangle",
			nodeIDs:           []uint32{69, 2},
			expectedFollowers: nil,
			expectedError:     models.ErrNodeNotFoundDB,
		},
		{
			name:              "dandling node",
			DBType:            "dandling",
			nodeIDs:           []uint32{0},
			expectedFollowers: [][]uint32{{}},
			expectedError:     nil,
		},
		{
			name:              "valid",
			DBType:            "triangle",
			nodeIDs:           []uint32{0, 1},
			expectedFollowers: [][]uint32{{2}, {0}},
			expectedError:     nil,
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
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

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
			name:            "one pubkey not found DB",
			DBType:          "one-node0",
			pubkeys:         []string{"four"},
			expectedError:   nil,
			expectedNodeIDs: []interface{}{nil},
		},
		{
			name:            "one pubkey found DB",
			DBType:          "one-node0",
			pubkeys:         []string{"zero"},
			expectedError:   nil,
			expectedNodeIDs: []interface{}{uint32(0)},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

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
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

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
			name:            "one pubkey not found DB",
			DBType:          "one-node0",
			nodeIDs:         []uint32{4},
			expectedError:   nil,
			expectedPubkeys: []interface{}{nil},
		},
		{
			name:            "one pubkey found DB",
			DBType:          "one-node0",
			nodeIDs:         []uint32{0},
			expectedError:   nil,
			expectedPubkeys: []interface{}{"zero"},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

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
				expectedError: models.ErrNilDBPointer,
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

func TestSetPagerank(t *testing.T) {
	cl := redisutils.SetupTestClient()
	defer redisutils.CleanupRedis(cl)

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
				expectedError: models.ErrNodeNotFoundDB,
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
				DB, err := SetupDB(cl, test.DBType)
				if err != nil {
					t.Fatalf("SetupDB(): expected nil, got %v", err)
				}

				err = DB.SetPagerank(context.Background(), test.pagerank)

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
				DBType:        "one-node0",
				pagerank:      models.PagerankMap{0: 11.0}, // random values
				expectedError: nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				DB, err := SetupDB(cl, test.DBType)
				if err != nil {
					t.Fatalf("SetupDB(): expected nil, got %v", err)
				}

				err = DB.SetPagerank(ctx, test.pagerank)

				if !errors.Is(err, test.expectedError) {
					t.Errorf("SetPagerank(): expected %v, got %v", test.expectedError, err)
				}

				for nodeID, rank := range test.pagerank {

					strP, err := DB.client.HGet(ctx, KeyNode(nodeID), "pagerank").Result()
					if err != nil {
						t.Errorf("HGet(): expected nil, got %v", err)
					}

					p, err := redisutils.ParseFloat64(strP)
					if err != nil {
						t.Errorf("ParseFloat64(): expected nil, got %v", err)
					}

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

func BenchmarkSetPagerank(b *testing.B) {
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

			pagerank := make(models.PagerankMap, nodesSize)
			for nodeID := uint32(0); nodeID < uint32(nodesSize); nodeID++ {
				pagerank[nodeID] = rand.Float64()
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {

				if err := DB.SetPagerank(ctx, pagerank); err != nil {
					b.Fatalf("benchmark failed: %v", err)
				}
			}
		})
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
