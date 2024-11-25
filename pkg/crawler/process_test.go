package crawler

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/walks"
)

const odell = "04c915daefee38317fa734444acee390a8269fe5810b2241e5e6dd343dfbecc9"
const calle = "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"
const pip = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
const gigi = "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93"

// A list of fake events used for testing.
var fakeEvents = []nostr.Event{
	{
		PubKey:    odell,
		Kind:      3,
		CreatedAt: nostr.Timestamp(1713083262),
		Tags: nostr.Tags{
			nostr.Tag{"p", gigi},
			nostr.Tag{"e", calle},       // not a p tag
			nostr.Tag{"p", pip + "xxx"}, // pubkey not valid
		},
	},
	{
		PubKey:    odell,
		Kind:      3,
		CreatedAt: nostr.Timestamp(11),
		Tags: nostr.Tags{
			nostr.Tag{"p", pip}},
	},
}

func TestParsePubKeys(t *testing.T) {
	testCases := []struct {
		name            string
		tags            nostr.Tags
		expectedPubkeys []string
	}{
		{
			name:            "nil tags",
			tags:            nil,
			expectedPubkeys: []string{},
		},
		{
			name:            "empty tags",
			tags:            nostr.Tags{},
			expectedPubkeys: []string{},
		},
		{
			name:            "one valid tag",
			tags:            fakeEvents[0].Tags,
			expectedPubkeys: []string{gigi},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pubkeys := ParsePubKeys(test.tags)
			if !reflect.DeepEqual(pubkeys, test.expectedPubkeys) {
				t.Fatalf("ParseFollowList(): expected %v, got %v", test.expectedPubkeys, pubkeys)
			}
		})
	}
}

func TestProcessNodeIDs(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		RWMType       string
		author        models.NodeMetaWithID
		pubkeys       []string
		expectedError error
		expectedIDs   []uint32
		expectedQueue []string
	}{
		{
			name:    "nil pubkeys",
			DBType:  "simple-with-mock-pks",
			RWMType: "one-node0",
			pubkeys: nil,
			author: models.NodeMetaWithID{
				NodeMeta: &models.NodeMeta{
					Pagerank: 1.0,
				},
			},
			expectedError: nil,
			expectedIDs:   []uint32{},
			expectedQueue: []string{},
		},
		{
			name:    "empty pubkeys",
			DBType:  "simple-with-mock-pks",
			RWMType: "one-node0",
			author: models.NodeMetaWithID{
				NodeMeta: &models.NodeMeta{
					Pagerank: 1.0,
				},
			},
			pubkeys:       []string{},
			expectedError: nil,
			expectedIDs:   []uint32{},
			expectedQueue: []string{},
		},
		{
			name:    "existing pubkey",
			DBType:  "simple-with-mock-pks",
			RWMType: "one-node0",
			author: models.NodeMetaWithID{
				NodeMeta: &models.NodeMeta{
					Pagerank: 1.0,
				},
			},
			pubkeys:       []string{"zero", "one"},
			expectedError: nil,
			expectedIDs:   []uint32{0, 1},
			expectedQueue: []string{},
		},
		{
			name:    "existing and new pubkey",
			DBType:  "simple-with-mock-pks",
			RWMType: "one-node0",
			author: models.NodeMetaWithID{
				NodeMeta: &models.NodeMeta{
					Pagerank: 1.0,
				},
			},
			pubkeys:       []string{"zero", "one", "three"},
			expectedError: nil,
			expectedIDs:   []uint32{0, 1, 3},
			expectedQueue: []string{"three"},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := mock.SetupDB(test.DBType)
			RWM := walks.SetupRWM(test.RWMType)
			queuePubkeys := []string{}

			followIDs, err := ProcessNodeIDs(context.Background(), DB, RWM, test.author, test.pubkeys, func(pk string) error {
				queuePubkeys = append(queuePubkeys, pk)
				return nil
			})

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("ProcessNodeIDs(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(followIDs, test.expectedIDs) {
				t.Errorf("ProcessNodeIDs(): expected %v, got %v", test.expectedIDs, followIDs)
			}

			if !reflect.DeepEqual(queuePubkeys, test.expectedQueue) {
				t.Errorf("ProcessNodeIDs(): expected %v, got %v", test.expectedQueue, queuePubkeys)
			}
		})
	}
}

// func TestProcessFollowListEvent(t *testing.T) {
// 	t.Run("simple errors", func(t *testing.T) {
// 		testCases := []struct {
// 			name          string
// 			DBType        string
// 			RWSType       string
// 			expectedError error
// 		}{
// 			{
// 				name:          "nil DB",
// 				DBType:        "nil",
// 				RWSType:       "one-node0",
// 				expectedError: models.ErrNilDBPointer,
// 			},
// 			{
// 				name:          "nil RWS",
// 				DBType:        "one-node0",
// 				RWSType:       "nil",
// 				expectedError: models.ErrNilRWSPointer,
// 			},
// 			{
// 				name:          "event.PubKey not found",
// 				DBType:        "one-node0",
// 				RWSType:       "one-node0",
// 				expectedError: models.ErrNodeNotFoundNC,
// 			},
// 		}

// 		for _, test := range testCases {
// 			t.Run(test.name, func(t *testing.T) {
// 				DB := mockdb.SetupDB(test.DBType)
// 				RWM := walks.SetupRWM(test.RWSType)
// 				NC := models.NewNodeCache()

// 				err := ProcessFollowListEvent(DB, RWM, NC, &fakeEvents[0])
// 				if !errors.Is(err, test.expectedError) {
// 					t.Fatalf("ProcessFollowListEvent(): expected %v, got %v", test.expectedError, err)
// 				}
// 			})
// 		}
// 	})

// 	t.Run("valid", func(t *testing.T) {
// 		DB := mockdb.SetupDB("simple-with-pks")
// 		RWM := walks.SetupRWM("simple")
// 		NC, err := DB.NodeCache()
// 		if err != nil {
// 			t.Fatalf("NodeCache(): expected nil, got %v", err)
// 		}

// 		err = ProcessFollowListEvent(DB, RWM, NC, &fakeEvents[1])
// 		if err != nil {
// 			t.Fatalf("ProcessFollowListEvent(): expected nil, got %v", err)
// 		}

// 		expectedNodes := map[uint32]models.Node{
// 			0: {
// 				Metadata: models.NodeMeta{
// 					PubKey:    odell,
// 					Status:    models.StatusCrawled,
// 					Timestamp: fakeEvents[1].CreatedAt.Time().Unix(),
// 					Pagerank:  0.26,
// 				},
// 				Successors:   []uint32{2},
// 				Predecessors: []uint32{},
// 			},

// 			1: {
// 				Metadata: models.NodeMeta{
// 					PubKey:    calle,
// 					Status:    models.StatusNotCrawled,
// 					Timestamp: 0,
// 					Pagerank:  0.26,
// 				},
// 				Successors:   []uint32{},
// 				Predecessors: []uint32{},
// 			},

// 			2: {
// 				Metadata: models.NodeMeta{
// 					PubKey:    pip,
// 					Status:    models.StatusNotCrawled,
// 					Timestamp: 0,
// 					Pagerank:  0.48,
// 				},
// 				Successors:   []uint32{},
// 				Predecessors: []uint32{0},
// 			},
// 		}

// 		for nodeID, expectedNode := range expectedNodes {
// 			node := DB.NodeIndex[nodeID]
// 			if !reflect.DeepEqual(node, &expectedNode) {
// 				t.Errorf("ProcessFollowListEvent(): expected node %v, got %v", &expectedNode, node)
// 			}
// 		}

// 	})
// }
