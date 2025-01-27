package crawler

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/pagerank"
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
			DBType:        "simple",
			pubkeys:       nil,
			expectedError: nil,
			expectedIDs:   []uint32{},
		},
		{
			name:          "empty pubkeys",
			DBType:        "simple",
			pubkeys:       []string{},
			expectedError: nil,
			expectedIDs:   []uint32{},
		},
		{
			name:          "existing pubkey",
			DBType:        "simple",
			pubkeys:       []string{"0", "1"},
			expectedError: nil,
			expectedIDs:   []uint32{0, 1},
		},
		{
			name:          "existing and new pubkey",
			DBType:        "simple",
			pubkeys:       []string{"0", "1", "3"},
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

func TestProcessFollowList(t *testing.T) {
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
				expectedError: models.ErrNilDB,
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
				RWM := walks.SetupMockRWM(test.RWSType)
				walksCounter := atomic.Uint32{}

				err := ProcessFollowList(DB, RWM, &validEvent, &walksCounter)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("ProcessFollowList(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		var maxDist float64 = 0.01
		var walksPerNode uint16 = 5000
		var alpha float32 = 0.85
		var maxWalkDiff float64 = 3 * maxDist * float64(walksPerNode) // the walks counter seems to converge slower than the pagerank
		ctx := context.Background()

		DB := mockdb.SetupDB("pip")
		RWS, err := mockstore.NewRWS(alpha, walksPerNode)
		if err != nil {
			t.Fatalf("NewRWS(): expected nil, got %v", err)
		}

		RWM := &walks.RandomWalkManager{
			Store: RWS,
		}
		if err := RWM.GenerateAll(ctx, DB); err != nil {
			t.Fatalf("GenerateAll(): expected nil, got %v", err)
		}

		// one after the other, the graph will be built: pip --> gigi; calle --> odell --> pip
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
			0: {0: 0.54, 1: 0.46, 2: 0},
			1: {0: 0.3887, 1: 0.3304, 2: 0.2809},
			2: {0: 0.3887, 1: 0.3304, 2: 0.2809},
		}

		expectedWalksCounter := map[int]float64{
			0: float64(walksPerNode),
			1: float64(walksPerNode) + float64(walksPerNode)*float64(alpha),
			2: float64(walksPerNode) + float64(walksPerNode)*float64(alpha) + float64(walksPerNode)*float64(alpha)*float64(alpha),
		}

		walksCounter := atomic.Uint32{}
		for i, event := range events {
			if err := ProcessFollowList(DB, RWM, event, &walksCounter); err != nil {
				t.Fatalf("ProcessFollowList(event%d): expected nil, got %v", i, err)
			}

			walks := walksCounter.Load()
			expectedWalks := expectedWalksCounter[i]
			diff := math.Abs(float64(expectedWalks) - float64(walks))

			if diff > maxWalkDiff {
				t.Errorf("Expected distance %v, got abs(%v)", maxWalkDiff, diff)
			}

			ranks, err := pagerank.Global(ctx, RWS, 0, 1, 2)
			if err != nil {
				t.Errorf("pagerank Global failed(): %v", err)
			}

			distance := pagerank.Distance(ranks, expectedPagerank[i])
			if distance > maxDist {
				t.Errorf("Expected distance %v, got %v", maxDist, distance)
				t.Errorf("Expected pagerank %v, got %v", expectedPagerank[i], ranks)
			}
		}
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
