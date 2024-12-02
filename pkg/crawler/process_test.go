package crawler

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/logger"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/walks"
)

const odell = "04c915daefee38317fa734444acee390a8269fe5810b2241e5e6dd343dfbecc9"
const calle = "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"
const pip = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
const gigi = "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93"

// A list of fake events used for testing.
var badlyFormattedEvent = nostr.Event{
	PubKey:    odell,
	Kind:      3,
	CreatedAt: nostr.Timestamp(1713083262),
	Tags: nostr.Tags{
		nostr.Tag{"p", gigi},
		nostr.Tag{"e", calle},       // not a p tag
		nostr.Tag{"p", pip + "xxx"}, // pubkey not valid
	},
}
var multipleFollowEvent = nostr.Event{
	PubKey:    odell,
	Kind:      3,
	CreatedAt: nostr.Timestamp(11),
	Tags: nostr.Tags{
		nostr.Tag{"p", pip},
		nostr.Tag{"p", pip}}, // added two times
}
var autoFollowEvent = nostr.Event{
	PubKey:    odell,
	Kind:      3,
	CreatedAt: nostr.Timestamp(11),
	Tags: nostr.Tags{
		nostr.Tag{"p", odell}, // autofollow event
		nostr.Tag{"p", pip}},
}
var validEvent = nostr.Event{
	PubKey:    calle,
	Kind:      3,
	CreatedAt: nostr.Timestamp(11),
	Tags: nostr.Tags{
		nostr.Tag{"p", gigi},
		nostr.Tag{"p", odell}},
}

func TestParsePubkeys(t *testing.T) {
	testCases := []struct {
		name            string
		event           *nostr.Event
		expectedPubkeys []string
	}{
		{
			name:            "nil tags",
			event:           nil,
			expectedPubkeys: []string{},
		},
		{
			name:            "nil event",
			event:           nil,
			expectedPubkeys: []string{},
		},
		{
			name:            "empty tags",
			event:           &nostr.Event{Tags: nostr.Tags{}},
			expectedPubkeys: []string{},
		},
		{
			name:            "badly formatted tags",
			event:           &badlyFormattedEvent,
			expectedPubkeys: []string{gigi},
		},
		{
			name:            "multiple follow tags",
			event:           &multipleFollowEvent,
			expectedPubkeys: []string{pip},
		},
		{
			name:            "auto follow tag",
			event:           &autoFollowEvent,
			expectedPubkeys: []string{pip},
		},
		{
			name:            "valid",
			event:           &validEvent,
			expectedPubkeys: []string{gigi, odell},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pubkeys := ParsePubkeys(test.event)
			if !reflect.DeepEqual(pubkeys, test.expectedPubkeys) {
				t.Fatalf("ParseFollowList(): expected %v, got %v", test.expectedPubkeys, pubkeys)
			}
		})
	}
}

func TestHandleMissingPubkey(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name           string
			DBType         string
			RWSType        string
			pubkey         string
			expectedError  error
			expectedNodeID uint32
		}{
			{
				name:           "nil DB",
				DBType:         "nil",
				RWSType:        "one-node0",
				pubkey:         "zero",
				expectedError:  models.ErrNilDBPointer,
				expectedNodeID: math.MaxUint32,
			},
			{
				name:           "nil RWM",
				DBType:         "empty",
				RWSType:        "nil",
				pubkey:         "zero",
				expectedError:  models.ErrNilRWSPointer,
				expectedNodeID: math.MaxUint32,
			},
			{
				name:           "node already in DB",
				DBType:         "simple-with-mock-pks",
				RWSType:        "simple",
				pubkey:         "zero",
				expectedError:  models.ErrNodeAlreadyInDB,
				expectedNodeID: math.MaxUint32,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mockdb.SetupDB(test.DBType)
				RWM := walks.SetupRWM(test.RWSType)

				queue := []string{}
				nodeID, err := HandleMissingPubkey(context.Background(), DB, RWM, test.pubkey, 1.0, func(pk string) error {
					queue = append(queue, pk)
					return nil
				})

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("HandleMissingPubkey: expected %v, got %v", test.expectedError, err)
				}

				if nodeID != test.expectedNodeID {
					t.Errorf("HandleMissingPubkey: expected %v, got %v", test.expectedNodeID, nodeID)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		DB := mockdb.SetupDB("pip")
		RWS := mockstore.SetupRWS("one-node0")
		RWM := &walks.RandomWalkManager{
			Store: RWS,
		}

		queue := []string{}
		nodeID, err := HandleMissingPubkey(context.Background(), DB, RWM, calle, 1.0, func(pk string) error {
			queue = append(queue, pk)
			return nil
		})

		if err != nil {
			t.Fatalf("HandleMissingPubkey(): expected nil, got %v", err)
		}

		if nodeID != 1 {
			t.Errorf("expected nodeID %v, got %v", 1, nodeID)
		}

		for walkID, walk := range RWS.WalkIndex {
			expectedWalk := models.RandomWalk{walkID}
			if !reflect.DeepEqual(walk, expectedWalk) {
				t.Errorf("expected walk %v, got %v", expectedWalk, walk)
			}
		}
	})
}

func TestProcessNodeIDs(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		RWMType       string
		author        *models.NodeMeta
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
			author: &models.NodeMeta{
				Pagerank: 1.0,
			},
			expectedError: nil,
			expectedIDs:   []uint32{},
			expectedQueue: []string{},
		},
		{
			name:    "empty pubkeys",
			DBType:  "simple-with-mock-pks",
			RWMType: "one-node0",
			author: &models.NodeMeta{
				Pagerank: 1.0,
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
			author: &models.NodeMeta{
				Pagerank: 1.0,
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
			author: &models.NodeMeta{
				Pagerank: 1.0,
			},
			pubkeys:       []string{"zero", "one", "three"},
			expectedError: nil,
			expectedIDs:   []uint32{0, 1, 3},
			expectedQueue: []string{"three"},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := mockdb.SetupDB(test.DBType)
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

func TestProcessFollowListEvent(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			DBType        string
			RWSType       string
			expectedError error
		}{
			{
				name:          "nil DB",
				DBType:        "nil",
				RWSType:       "one-node0",
				expectedError: models.ErrNilDBPointer,
			},
			{
				name:          "event.PubKey not found",
				DBType:        "one-node0",
				RWSType:       "one-node0",
				expectedError: models.ErrNodeNotFoundDB,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mockdb.SetupDB(test.DBType)
				RWM := walks.SetupRWM(test.RWSType)

				err := ProcessFollowListEvent(context.Background(), &validEvent, DB, RWM, func(pk string) error {
					return nil
				})

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("ProcessFollowListEvent(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		DB := mockdb.SetupDB("pip")
		RWS := mockstore.SetupRWS("empty")
		RWM := &walks.RandomWalkManager{
			Store: RWS,
		}

		if err := RWM.GenerateAll(DB); err != nil {
			t.Fatalf("GenerateAll(): expected nil, got %v", err)
		}

		// one after the other, the graph will be built: pip --> calle --> odell --> pip
		events := []*nostr.Event{
			{
				PubKey:    pip,
				CreatedAt: nostr.Timestamp(1),
				Tags: nostr.Tags{
					nostr.Tag{"p", calle},
				},
			},
			{
				PubKey:    calle,
				CreatedAt: nostr.Timestamp(2),
				Tags: nostr.Tags{
					nostr.Tag{"p", odell},
				},
			},
			{
				PubKey:    odell,
				CreatedAt: nostr.Timestamp(3),
				Tags: nostr.Tags{
					nostr.Tag{"p", pip},
				},
			},
		}

		expectedQueue := map[int][]string{
			0: {calle},
			1: {calle, odell},
			2: {calle, odell},
		}

		expectedNodeIDs := map[int][]uint32{
			0: {0, 1},
			1: {0, 1, 2},
			2: {0, 1, 2},
		}

		expectedWalks := map[int]map[uint32]models.RandomWalk{
			0: { // walks at iteration 0
				0: {0, 1},
				1: {1},
			},
			1: { // walks at iteration 1
				0: {0, 1, 2},
				1: {1, 2},
				2: {2},
			},
			2: { // walks at iteration 2
				0: {0, 1, 2},
				1: {1, 2, 0},
				2: {2, 0, 1},
			},
		}

		_ = expectedWalks

		queue := []string{}
		for i, event := range events {

			err := ProcessFollowListEvent(context.Background(), event, DB, RWM, func(pk string) error {
				queue = append(queue, pk)
				return nil
			})

			if err != nil {
				t.Fatalf("ProcessFollowListEvent(event%d): expected nil, got %v", i, err)
			}

			if !reflect.DeepEqual(queue, expectedQueue[i]) {
				t.Fatalf("expected queue %v, got %v", expectedQueue[i], queue)
			}

			nodeIDs, err := DB.AllNodes()
			if err != nil {
				t.Fatalf("AllNodes(): expected nil, got %v", err)
			}
			slices.Sort(nodeIDs) // sort nodeIDs before comparing them.

			if !reflect.DeepEqual(nodeIDs, expectedNodeIDs[i]) {
				t.Fatalf("expected nodeIDs %v, got %v", expectedNodeIDs[i], nodeIDs)
			}

			// The following test fails 55% of the times, due to the random nature of the walks,
			// for walkID, walk := range RWS.WalkIndex {
			// 	expectedWalk := expectedWalks[i][walkID]
			// 	if !reflect.DeepEqual(walk, expectedWalk) {
			// 		t.Errorf("Iteration %d: expected walk %v, got %v", i, expectedWalk, walk)
			// 	}
			// }
		}
	})
}

func TestNodeArbiter(t *testing.T) {

	logger := logger.New(os.Stdout)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go HandleSignals(cancel, logger)

	DB := mockdb.SetupDB("one-node0")
	RWM := walks.SetupRWM("one-node0")
	NodeArbiter(ctx, logger, DB, RWM, func(pk string) error {
		return nil
	})
}

// ---------------------------------BENCHMARKS----------------------------------

func BenchmarkParsePubkeys(b *testing.B) {
	event := nostr.Event{
		Tags: nostr.Tags{},
	}

	// creating a followList with 10k
	pubkeys := []string{pip, calle, gigi, odell}
	for i := 0; i < 10000; i++ {
		pk := pubkeys[rand.IntN(4)]
		event.Tags = append(event.Tags, nostr.Tag{"p", pk})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParsePubkeys(&event)
	}
}
