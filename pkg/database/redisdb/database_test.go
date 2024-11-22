package redisdb

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

func TestValidate(t *testing.T) {
	cl := redisutils.SetupClient()
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

func TestAddSuccessors(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	nodeID := uint32(0)
	addedSucc := []uint32{1, 2, 3}

	pipe := cl.TxPipeline()
	ctx := context.Background()

	AddSuccessors(ctx, pipe, nodeID, addedSucc)
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

	if !reflect.DeepEqual(followIDs, addedSucc) {
		t.Fatalf("AddSuccessors(): expected %v, got %v", addedSucc, followIDs)
	}

	// check the succ have nodeID as a follower
	for _, succ := range addedSucc {

		isMember, err := cl.SIsMember(ctx, KeyFollowers(succ), redisutils.FormatID(nodeID)).Result()
		if err != nil {
			t.Errorf("IsMember(): expected nil, got %v", err)
		}

		if !isMember {
			t.Fatalf("AddSuccessors(): expected nodeID = %d part of followers:%d", nodeID, succ)
		}
	}
}

func TestRemoveSuccessors(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	nodeID := uint32(0)
	succ := []uint32{0, 1, 2}
	removedSucc := []uint32{1, 2, 3}
	remainingSucc := []uint32{0} // {0,1,2} - {1,2,3} = {0}

	// add succ to Redis
	pipe := cl.TxPipeline()
	ctx := context.Background()
	AddSuccessors(ctx, pipe, nodeID, succ)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec(): expected nil, got %v", err)
	}

	// remove succ from Redis
	pipe = cl.TxPipeline()
	ctx = context.Background()
	RemoveSuccessors(ctx, pipe, nodeID, removedSucc)
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
		t.Fatalf("RemoveSuccessors(): expected %v, got %v", remainingSucc, followIDs)
	}

	// check the remainingSucc have nodeID as a follower
	for _, succ := range remainingSucc {
		isMember, err := cl.SIsMember(ctx, KeyFollowers(succ), redisutils.FormatID(nodeID)).Result()
		if err != nil {
			t.Errorf("IsMember(): expected nil, got %v", err)
		}

		if !isMember {
			t.Fatalf("RemoveSuccessors(): expected nodeID = %d part of followers:%d", nodeID, succ)
		}
	}

	// check the removedSucc DON'T have nodeID as a follower
	for _, succ := range removedSucc {
		isMember, err := cl.SIsMember(ctx, KeyFollowers(succ), redisutils.FormatID(nodeID)).Result()
		if err != nil {
			t.Errorf("IsMember(): expected nil, got %v", err)
		}

		if isMember {
			t.Fatalf("RemoveSuccessors(): expected nodeID = %d NOT part of followers:%d", nodeID, succ)
		}
	}
}

func TestAddPredecessors(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	nodeID := uint32(0)
	predecessors := []uint32{1, 2, 3}

	pipe := cl.TxPipeline()
	ctx := context.Background()

	AddPredecessors(ctx, pipe, nodeID, predecessors)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec(): expected nil, got %v", err)
	}

	// check the followers of nodeID are correctly added
	followers, err := cl.SMembers(ctx, KeyFollowers(nodeID)).Result()
	if err != nil {
		t.Errorf("SMembers(): expected nil, got %v", err)
	}
	// parse the follows
	followerIDs := make([]uint32, 0, len(followers))
	for _, follower := range followers {
		followerID, err := redisutils.ParseID(follower)
		if err != nil {
			t.Fatalf("ParseID(%v): expected nil, got %v", follower, err)
		}
		followerIDs = append(followerIDs, followerID)
	}

	if !reflect.DeepEqual(followerIDs, predecessors) {
		t.Fatalf("AddSuccessors(): expected %v, got %v", predecessors, followerIDs)
	}

	// check the pred have nodeID as a follows
	for _, pred := range predecessors {

		isMember, err := cl.SIsMember(ctx, KeyFollows(pred), redisutils.FormatID(nodeID)).Result()
		if err != nil {
			t.Errorf("IsMember(): expected nil, got %v", err)
		}

		if !isMember {
			t.Fatalf("AddSuccessors(): expected nodeID = %d part of follows:%d", nodeID, pred)
		}
	}
}

func TestAddNode(t *testing.T) {
	cl := redisutils.SetupClient()

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
				Node:           &models.Node{Metadata: models.NodeMeta{PubKey: "zero"}},
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB, err := SetupDB(cl, test.DBType)
				defer redisutils.CleanupRedis(cl)

				if err != nil {
					t.Fatalf("SetupDB(): expected nil, got %v", err)
				}

				nodeID, err := DB.AddNode(test.Node)
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
				Node:               &models.Node{Metadata: models.NodeMeta{PubKey: "one"}},
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
						PubKey:    "one",
						Timestamp: 0,
						Status:    "not-crawled",
						Pagerank:  0.0,
					},
				},
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				defer redisutils.CleanupRedis(cl)

				DB, err := SetupDB(cl, test.DBType)
				if err != nil {
					t.Fatalf("SetupDB(): expected nil, got %v", err)
				}

				nodeID, err := DB.AddNode(test.Node)
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
				strNodeID, err := cl.HGet(ctx, KeyKeyIndex, test.Node.Metadata.PubKey).Result()
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
	cl := redisutils.SetupClient()
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
				Metadata: models.NodeMeta{Timestamp: 11}},
			expectedNodeMeta: &models.NodeMeta{
				PubKey:    "zero",
				Timestamp: 11, // the only field that changes
				Status:    "idk",
				Pagerank:  1.0,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			err = DB.UpdateNode(test.nodeID, test.nodeDiff)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("UpdateNode(%v): expected %v, got %v", test.nodeDiff, test.expectedError, err)
			}

			// check if node was updated correctly
			if err == nil {
				cmdReturn := cl.HGetAll(DB.ctx, KeyNode(test.nodeID))
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
	cl := redisutils.SetupClient()
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

			contains := DB.ContainsNode(test.nodeID)
			if contains != test.expectedContains {
				t.Errorf("ContainsNode(%d): expected %v, got %v", test.nodeID, test.expectedContains, contains)
			}
		})
	}
}

func TestIsDandling(t *testing.T) {
	cl := redisutils.SetupClient()

	testCases := []struct {
		name             string
		DBType           string
		nodeID           uint32
		expectedDandling bool
	}{
		{
			name:             "nil DB",
			DBType:           "nil",
			nodeID:           0,
			expectedDandling: false,
		},
		{
			name:             "empty DB",
			DBType:           "empty",
			nodeID:           0,
			expectedDandling: false,
		},
		{
			name:             "node not found",
			DBType:           "one-node0",
			nodeID:           1,
			expectedDandling: false,
		},
		{
			name:             "node found, has succ",
			DBType:           "one-node0",
			nodeID:           0,
			expectedDandling: false,
		},
		{
			name:             "node found, dandling",
			DBType:           "dandling",
			nodeID:           0,
			expectedDandling: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			defer redisutils.CleanupRedis(cl)
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			dandling := DB.IsDandling(test.nodeID)
			if dandling != test.expectedDandling {
				t.Errorf("IsDandling(%d): expected %v, got %v", test.nodeID, test.expectedDandling, dandling)
			}
		})
	}
}

func TestSuccessors(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name          string
		DBType        string
		nodeID        uint32
		expectedError error
		expectedSlice []uint32
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			nodeID:        0,
			expectedSlice: nil,
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			nodeID:        0,
			expectedSlice: []uint32{},
			expectedError: nil,
		},
		{
			name:          "node not found",
			DBType:        "one-node0",
			nodeID:        1,
			expectedSlice: []uint32{},
			expectedError: nil,
		},
		{
			name:          "valid",
			DBType:        "one-node0",
			nodeID:        0,
			expectedSlice: []uint32{0},
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDb(): expected nil, got %v", err)
			}

			succ, err := DB.Successors(test.nodeID)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Successors(%d): expected %v, got %v", test.nodeID, test.expectedError, err)
			}

			if !reflect.DeepEqual(succ, test.expectedSlice) {
				t.Errorf("Successors(%d): expected %v, got %v", test.nodeID, test.expectedSlice, succ)
			}
		})
	}
}

func TestNodeIDs(t *testing.T) {
	cl := redisutils.SetupClient()
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
	cl := redisutils.SetupClient()
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

			nodeIDs, err := DB.AllNodes()
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("AllNodes(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(nodeIDs, test.expectedSlice) {
				t.Errorf("AllNodes(): expected %v, got %v", test.expectedSlice, nodeIDs)
			}
		})
	}
}

func TestSize(t *testing.T) {
	cl := redisutils.SetupClient()
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

			size := DB.Size()
			if size != test.expectedSize {
				t.Errorf("Size(): expected %v, got %v", test.expectedSize, size)
			}
		})
	}
}

func TestNodeCache(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name              string
		DBType            string
		expectedError     error
		expectedNodeCache map[string]models.NodeFilterAttributes
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
			DBType:        "one-node0",
			expectedError: nil,
			expectedNodeCache: map[string]models.NodeFilterAttributes{
				"zero": {ID: 0, Timestamp: 1731685733, Pagerank: 1.0},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			NC, err := DB.NodeCache()
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("NodeCache(): expected %v, got %v", test.expectedError, err)
			}

			NCMap := models.ToMap(NC)
			if !reflect.DeepEqual(NCMap, test.expectedNodeCache) {
				t.Errorf("NodeCache(): expected %v, got %v", test.expectedNodeCache, NCMap)
			}
		})
	}
}

func TestInterface(t *testing.T) {
	var _ models.Database = &Database{}
}
