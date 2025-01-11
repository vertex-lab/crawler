package crawler

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
)

var (
	RelevantKinds = []int{
		nostr.KindFollowList,
	}

	Relays = []string{
		"wss://purplepag.es",
		"wss://njump.me",
		"wss://relay.snort.social",
		"wss://relay.damus.io",
		"wss://relay.primal.net",
		"wss://relay.nostr.band",
		"wss://nostr-pub.wellorder.net",
		"wss://relay.nostr.net",
		"wss://nostr.lu.ke",
		"wss://nostr.at",
		"wss://e.nos.lol",
		"wss://nostr.lopp.social",
		"wss://nostr.vulpem.com",
		"wss://relay.nostr.bg",
		"wss://wot.utxo.one",
		"wss://nostrelites.org",
		"wss://wot.nostr.party",
		"wss://wot.sovbit.host",
		"wss://wot.girino.org",
		"wss://relay.lnau.net",
		"wss://wot.siamstr.com",
		"wss://wot.sudocarlos.com",
		"wss://relay.otherstuff.fyi",
		"wss://relay.lexingtonbitcoin.org",
		"wss://wot.azzamo.net",
		"wss://wot.swarmstr.com",
		"wss://zap.watch",
		"wss://satsage.xyz",
	}
)

/*
Firehose connects to a list of relays and pulls kind:3 events that are newer than the current time.
It efficiently filters events based on the pubkey "spamminess", determined by our
own pagerank-based reputation system.

Finally, it uses the specified queueHandler function to send the events to the
queue for further processing and/or to be written to the database.
*/
func Firehose(
	ctx context.Context,
	logger *logger.Aggregate,
	DB models.Database,
	relays []string,
	queueHandler func(event *nostr.Event) error) {

	pool := nostr.NewSimplePool(ctx)
	defer close(logger, pool, "Firehose")

	ts := nostr.Now()
	filters := nostr.Filters{{
		Kinds: RelevantKinds,
		Since: &ts,
	}}

	for event := range pool.SubMany(ctx, relays, filters) {
		if event.Event == nil {
			continue
		}

		if match, err := event.CheckSignature(); err != nil || !match {
			continue
		}

		node, err := DB.NodeByKey(ctx, event.PubKey)
		if err != nil {
			continue // If the node is not found (err != nil), skip
		}

		if event.CreatedAt.Time().Unix() < node.EventTS {
			continue
		}

		// if the author is an inactive node, skip
		if node.Status == models.StatusInactive {
			continue
		}

		if err := queueHandler(event.Event); err != nil {
			logger.Error("Firehose queue handler: %v", err)
			return
		}
	}
}

// QueryPubkeys() extracts pubkeys from the pubkeyChan channel, and queries for their events in batches of batchSize.
func QueryPubkeys(
	ctx context.Context,
	logger *logger.Aggregate,
	relays []string,
	pubkeyChan <-chan string,
	batchSize int,
	queueHandler func(event *nostr.Event) error) {

	var firstQuery bool = true // the first query runs right away to speed up initialization

	batch := make([]string, 0, batchSize)
	pool := nostr.NewSimplePool(ctx)
	defer close(logger, pool, "QueryPubkeys")

	for {
		select {
		case <-ctx.Done():
			return

		case pubkey, ok := <-pubkeyChan:
			if !ok {
				fmt.Println("\n  > Pubkey channel closed, stopping processing.")
				logger.Warn("Pubkey channel closed, stopping processing.")
				return
			}

			batch = append(batch, pubkey)
			if len(batch) >= batchSize || firstQuery {

				if err := QueryPubkeyBatch(ctx, pool, relays, batch, queueHandler); err != nil {
					logger.Error("QueryPubkeys queue handler: %v", err)
					continue
				}

				// reset only if successful
				batch = make([]string, 0, batchSize)
				//firstQuery = false
			}
		}
	}
}

// QueryPubkeyBatch() queries the follow lists of the specified pubkeys.
// It sends the newest events for each pubkey to the queue using the provided queueHandler.
func QueryPubkeyBatch(
	ctx context.Context,
	pool *nostr.SimplePool,
	relays []string,
	pubkeys []string,
	queueHandler func(event *nostr.Event) error) error {

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	filters := nostr.Filters{{
		Kinds:   RelevantKinds,
		Authors: pubkeys,
	}}

	// a map that associates each pubkey with the newest follow list
	newest := make(map[string]*nostr.Event, len(pubkeys))
	for event := range pool.SubManyEose(ctx, relays, filters) {
		if event.Event == nil {
			continue
		}

		if match, err := event.CheckSignature(); err != nil || !match {
			continue
		}

		newestEvent, exists := newest[event.PubKey]
		if !exists {
			newest[event.PubKey] = event.Event
			continue
		}

		if event.CreatedAt.Time().Unix() > newestEvent.CreatedAt.Time().Unix() {
			newest[event.PubKey] = event.Event
		}
	}

	// send only the newest events to the queue.
	for _, event := range newest {
		if err := queueHandler(event); err != nil {
			return err
		}
	}

	return nil
}

// Close() iterates over the relays in the pool and closes all connections.
func close(logger *logger.Aggregate, pool *nostr.SimplePool, funcName string) {
	logger.Info("  > " + funcName + ": closing relay connections... ")
	pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
		relay.Close()
		return true
	})
}

// PrintEvent() is a simple function that prints the event ID, PubKey and Timestamp.
func PrintEvent(event *nostr.Event) error {
	fmt.Printf("\nevent ID: %v", event.ID)
	fmt.Printf("\nevent pubkey: %v", event.PubKey)
	fmt.Printf("\nevent timestamp: %d\n", event.CreatedAt.Time().Unix())
	return nil
}

// HandleSignals() listens for OS signals and triggers context cancellation.
func HandleSignals(cancel context.CancelFunc, l *logger.Aggregate) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-signalChan // Block until a signal is received
	l.Info(" Signal received. Shutting down...")
	cancel()
}
