package crawler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
)

// Manually change pip's follow list and see if the events gets printed. Works only with `go test`
func TestFirehose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	DB := mockdb.SetupDB("pip")
	config := FirehoseConfig{
		Log:    logger.New(os.Stdout),
		Relays: Relays,
	}

	go HandleSignals(cancel, config.Log)
	Firehose(ctx, config, DB, PrintEvent)
}

func TestQueryPubkeys(t *testing.T) {
	// These two tests should print the same 4 events, as the only difference is the triggering factor.
	t.Run("BatchSize", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		config := QueryPubkeysConfig{
			Log:       logger.New(os.Stdout),
			Relays:    Relays,
			BatchSize: 4,
			Interval:  30 * time.Second,
		}

		config.Log.Info("---------------------BatchSize---------------------")
		go HandleSignals(cancel, config.Log)

		// the queue contains enough pubkeys (4), so it should query immediately and then print.
		pubkeyChan := make(chan string, 10)
		pubkeys := []string{pip, calle, gigi, odell}
		for _, pk := range pubkeys {
			pubkeyChan <- pk
		}

		QueryPubkeys(ctx, config, pubkeyChan, PrintEvent)
	})

	t.Run("timer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		config := QueryPubkeysConfig{
			Log:       logger.New(os.Stdout),
			Relays:    Relays,
			BatchSize: 5,
			Interval:  3 * time.Second,
		}

		config.Log.Info("---------------------timer---------------------")
		go HandleSignals(cancel, config.Log)

		// there aren't enough pubkeys, but the timer will kick in, so it should query and then print.
		pubkeyChan := make(chan string, 10)
		pubkeys := []string{pip, calle, gigi, odell}
		for _, pk := range pubkeys {
			pubkeyChan <- pk
		}

		QueryPubkeys(ctx, config, pubkeyChan, PrintEvent)
	})
}

func TestIsEventOutdated(t *testing.T) {
	testCases := []struct {
		name             string
		node             *models.Node
		event            *nostr.Event
		expectedOutdated bool
	}{
		{
			name:  "nil node",
			event: &nostr.Event{Kind: nostr.KindFollowList},
		},
		{
			name:  "nil records",
			node:  &models.Node{},
			event: &nostr.Event{Kind: nostr.KindFollowList},
		},
		{
			name:             "outdated event",
			node:             &models.Node{Records: []models.Record{{Kind: nostr.KindFollowList, Timestamp: 1}}},
			event:            &nostr.Event{Kind: nostr.KindFollowList, CreatedAt: 0},
			expectedOutdated: true,
		},
		{
			name:             "newer event",
			node:             &models.Node{Records: []models.Record{{Kind: nostr.KindFollowList, Timestamp: 1}}},
			event:            &nostr.Event{Kind: nostr.KindFollowList, CreatedAt: 5},
			expectedOutdated: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			outdated := IsEventOutdated(test.node, test.event)
			if outdated != test.expectedOutdated {
				t.Fatalf("IsEventOutdated(): expected %v, got %v", test.expectedOutdated, outdated)
			}
		})
	}
}
