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
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/utils/counter"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
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
				RWM := walks.SetupMockRWM(test.RWSType)
				pagerankTotal := counter.NewFloatCounter()

				err := ProcessFollowList(context.Background(), DB, RWM, &validEvent, pagerankTotal)

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("ProcessFollowList(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		ctx := context.Background()
		maxDist := 0.01
		DB := mockdb.SetupDB("pip")
		RWS, err := mockstore.NewRWS(0.85, 1000)
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
			0: {1: 0.46},
			1: {2: 0.2809},
			2: {0: 0.389},
		}

		expectedTotal := map[int]float64{
			0: 1.0,
			1: 1.46,
			2: 1.74,
		}

		pagerankTotal := counter.NewFloatCounter()
		for i, event := range events {
			err := ProcessFollowList(ctx, DB, RWM, event, pagerankTotal)
			if err != nil {
				t.Fatalf("ProcessFollowList(event%d): expected nil, got %v", i, err)
			}

			total := pagerankTotal.Load()
			if math.Abs(expectedTotal[i]-total) > maxDist {
				t.Errorf("Expected distance %v, got %v", maxDist, math.Abs(expectedTotal[i]-total))
			}

			rank := make(models.PagerankMap, 1)
			for ID := range expectedPagerank[i] {
				rank[ID] = DB.NodeIndex[ID].Metadata.Pagerank
			}

			distance := pagerank.Distance(rank, expectedPagerank[i])
			if distance > maxDist {
				t.Errorf("Expected distance %v, got %v", maxDist, distance)
				t.Errorf("Expected pagerank %v, got %v", expectedPagerank[i], rank)
			}
		}
	})
}

func TestArbiterScan(t *testing.T) {
	type testCases struct {
		name          string
		DBType        string
		RWMType       string
		expectedError error
	}

	t.Run("simple errors", func(t *testing.T) {
		testCases := []testCases{
			{
				name:          "nil DB",
				DBType:        "nil",
				RWMType:       "one-node0",
				expectedError: models.ErrNilDBPointer,
			},
			{
				name:          "valid",
				DBType:        "one-node0",
				RWMType:       "one-node0",
				expectedError: nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mockdb.SetupDB(test.DBType)
				RWM := walks.SetupMockRWM(test.RWMType)
				err := ArbiterScan(context.Background(), DB, RWM, 0, 0, func(pk string) error {
					return nil
				})

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("ArbiterScan(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		t.Run("demotion", func(t *testing.T) {
			// calle will be demoted to inactive, because the demotion threshold is 1.0 * 1/1  = 1
			ctx := context.Background()
			DB := mockdb.SetupDB("promotion-demotion")
			RWM := walks.SetupMockRWM("one-node1")

			err := ArbiterScan(ctx, DB, RWM, 1.0, 1.0, func(pk string) error {
				return nil
			})
			if err != nil {
				t.Fatalf("ArbiterScan(): expected nil, got %v", err)
			}

			// check that calle's status changed
			node, exists := DB.NodeIndex[1]
			if !exists {
				t.Fatalf("nodeID %d doesn't exists in the DB", 1)
			}

			if node.Metadata.Status != models.StatusInactive {
				t.Errorf("expected status of nodeID %d %v, got %v", 1, models.StatusInactive, node.Metadata.Status)
			}

			// check the only walk (from calle) has been removed
			walkIDs, err := RWM.Store.WalksVisiting(ctx, -1, 1)
			if err != nil {
				t.Errorf("WalksVisiting(): expected nil, got %v", err)
			}

			if len(walkIDs) > 0 {
				t.Errorf("expected no walks, got %v", walkIDs)
			}

		})
		t.Run("promotion", func(t *testing.T) {
			// pip and odell will be promoted from inactive to active, because the promotion threshold is 0 * 1/1  = 0
			ctx := context.Background()
			DB := mockdb.SetupDB("promotion-demotion")
			RWM := walks.SetupMockRWM("one-node1")
			queue := []string{}

			err := ArbiterScan(ctx, DB, RWM, 0, 0, func(pk string) error {
				queue = append(queue, pk)
				return nil
			})

			if err != nil {
				t.Fatalf("ArbiterScan(): expected nil, got %v", err)
			}

			// compare queues when sorted in lexicographic order
			expectedQueue := []string{odell, pip}
			slices.Sort(queue)
			if !reflect.DeepEqual(queue, expectedQueue) {
				t.Errorf("ArbiterScan(): expected queue %v, got %v", expectedQueue, queue)
			}

			// check that the status changed
			for nodeID := uint32(0); nodeID < 3; nodeID++ {
				node, exists := DB.NodeIndex[nodeID]
				if !exists {
					t.Fatalf("nodeID %d doesn't exists in the DB", nodeID)
				}

				if node.Metadata.Status != models.StatusActive {
					t.Errorf("expected status of nodeID %d %v, got %v", nodeID, models.StatusActive, node.Metadata.Status)
				}
			}

			// check that walks for pip and odell have been generated.
			for _, nodeID := range []uint32{0, 2} {
				walkIDs, err := RWM.Store.WalksVisiting(ctx, -1, nodeID)
				if err != nil {
					t.Fatalf("Walks(%d): expected nil, got %v", 0, err)
				}

				// check it contains exactly one walk (the one generated)
				if len(walkIDs) != 1 {
					t.Errorf("walkIDs: %v", walkIDs)
				}
			}
		})
	})
}

func TestNodeArbiter(t *testing.T) {
	logger := logger.New(os.Stdout)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go HandleSignals(cancel, logger)

	DB := mockdb.SetupDB("one-node0")
	RWM := walks.SetupMockRWM("one-node0")
	pagerankTotal := counter.NewFloatCounter()
	NodeArbiter(ctx, logger, DB, RWM, pagerankTotal, 0, 0, 0, func(pk string) error {
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
