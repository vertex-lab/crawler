package redisdb

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	"github.com/pippellia-btc/Nostrcrawler/pkg/utils/redisutils"
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

func TestAddNode(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name               string
		DBType             string
		PubKey             string
		expectedNodeID     uint32
		expectedLastNodeID int
		expectedError      error
		expectedNode       *models.Node
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
			DBType:             "one-node0",
			PubKey:             "zero",
			expectedNodeID:     math.MaxUint32,
			expectedLastNodeID: 0,
			expectedError:      models.ErrNodeAlreadyInDB,
		},
		{
			name:               "valid",
			DBType:             "one-node0",
			PubKey:             "one",
			expectedNodeID:     1,
			expectedLastNodeID: 1,
			expectedNode:       &models.Node{PubKey: "one"},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			node := &models.Node{
				PubKey:    test.PubKey,
				Timestamp: 0,
			}

			nodeID, err := DB.AddNode(node)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("AddNode(%v): expected %v, got %v", test.PubKey, test.expectedError, err)
			}

			// check if nodeID has been assigned correctly
			if nodeID != test.expectedNodeID {
				t.Errorf("AddNode(%v): expected nodeID = %v, got %v", test.PubKey, test.expectedNodeID, nodeID)
			}

			if DB != nil {
				// check if database HASH was updated correctly
				cmdReturn := cl.HMGet(ctx, KeyDatabase, KeyLastNodeID)
				if cmdReturn.Err() != nil {
					t.Errorf("HMGet(): expected nil, got %v", err)
				}

				var fields DatabaseFields
				if err := cmdReturn.Scan(&fields); err != nil {
					t.Errorf("Scan(): expected nil, got %v", err)
				}

				if fields.LastNodeID != test.expectedLastNodeID {
					t.Errorf("AddNode(%v): expected LastNodeID = %v, got %v", test.PubKey, test.expectedLastNodeID, fields.LastNodeID)
				}
			}

			// check if node data was added correctly
			if test.expectedNodeID != math.MaxUint32 {

				// check if the node was added to the keyIndex correctly
				strNodeID, err := cl.HGet(ctx, KeyKeyIndex, test.PubKey).Result()
				if err != nil {
					t.Errorf("HGet(): expected nil, got %v", err)
				}

				nodeID, err := redisutils.ParseID(strNodeID)
				if err != nil {
					t.Errorf("ParseID(%v): expected nil, got %v", strNodeID, err)
				}

				if nodeID != test.expectedNodeID {
					t.Errorf("AddNode(%v): expected nodeID = %v, got %v", test.PubKey, test.expectedNodeID, nodeID)
				}

				// check if node HASH was added correctly
				cmdReturn := cl.HGetAll(ctx, KeyNode(test.expectedNodeID))
				if cmdReturn.Err() != nil {
					t.Errorf("HGetAll(): expected nil, got %v", err)
				}

				var node models.Node
				if err := cmdReturn.Scan(&node); err != nil {
					t.Errorf("Scan(): expected nil, got %v", err)
				}

				if !reflect.DeepEqual(&node, test.expectedNode) {
					t.Errorf("AddNode(%v): expected node %v \n got %v", test.PubKey, test.expectedNode, node)
				}
			}
		})
	}
}
