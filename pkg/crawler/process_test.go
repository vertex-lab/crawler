package crawler

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"reflect"
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

func TestAssignNodeIDs(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		pubkeys       []string
		expectedError error
		expectedIDs   []uint32
	}{
		{
			name:          "nil pubkeys",
			DBType:        "simple-with-mock-pks",
			pubkeys:       nil,
			expectedError: nil,
			expectedIDs:   []uint32{},
		},
		{
			name:          "empty pubkeys",
			DBType:        "simple-with-mock-pks",
			pubkeys:       []string{},
			expectedError: nil,
			expectedIDs:   []uint32{},
		},
		{
			name:          "existing pubkey",
			DBType:        "simple-with-mock-pks",
			pubkeys:       []string{"zero", "one"},
			expectedError: nil,
			expectedIDs:   []uint32{0, 1},
		},
		{
			name:          "existing and new pubkey",
			DBType:        "simple-with-mock-pks",
			pubkeys:       []string{"zero", "one", "three"},
			expectedError: nil,
			expectedIDs:   []uint32{0, 1, 3},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := mockdb.SetupDB(test.DBType)
			followIDs, err := AssignNodeIDs(context.Background(), DB, test.pubkeys)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("ProcessNodeIDs(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(followIDs, test.expectedIDs) {
				t.Errorf("ProcessNodeIDs(): expected %v, got %v", test.expectedIDs, followIDs)
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

				err := ProcessFollowListEvent(context.Background(), &validEvent, DB, RWM)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("ProcessFollowListEvent(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		maxDist := 0.01

		DB := mockdb.SetupDB("pip")
		RWS, err := mockstore.NewRWS(0.85, 1000)
		if err != nil {
			t.Fatalf("NewRWS(): expected nil, got %v", err)
		}

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

		expectedPagerank := map[int]models.PagerankMap{
			0: {0: 0.54, 1: 0.46},
			1: {0: 0.389, 1: 0.33, 2: 0.2809},
			2: {0: 0.389, 1: 0.33, 2: 0.2809},
		}

		for i, event := range events {

			err := ProcessFollowListEvent(context.Background(), event, DB, RWM)
			if err != nil {
				t.Fatalf("ProcessFollowListEvent(event%d): expected nil, got %v", i, err)
			}

			pagerank := models.PagerankMap{}
			for nodeID, node := range DB.NodeIndex {
				pagerank[nodeID] = node.Metadata.Pagerank
			}

			distance := models.Distance(pagerank, expectedPagerank[i])
			if distance > maxDist {
				t.Errorf("Expected distance %v, got %v", maxDist, distance)
				t.Errorf("Expected pagerank %v, got %v", expectedPagerank[i], pagerank)
			}
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
