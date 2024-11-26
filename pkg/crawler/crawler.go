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
Firehose connects to a list of relays and pulls kind:3 events that are newer than
current time - timeLimit.
It efficiently filters events based on the pubkey "spamminess", determined by our
own pagerank-based reputation system.

Finally, it uses the specified queueHandler function to send the events to the
queue for further processing and/or to be written to the database.
*/
func Firehose(
	ctx context.Context,
	relays []string,
	DB models.Database,
	timeLimit int64,
	queueHandler func(event nostr.RelayEvent) error) {

	pool := nostr.NewSimplePool(ctx)
	defer close("Firehose", pool)

	ts := nostr.Timestamp(time.Now().Unix() - timeLimit)
	filters := nostr.Filters{{
		Kinds: RelevantKinds,
		Since: &ts,
	}}

	for event := range pool.SubMany(ctx, relays, filters) {
		if match, err := event.CheckSignature(); err != nil || !match {
			continue
		}

		// query from the DB the node associated with the pubkey; If it's not found, skip (err != nil)
		node, err := DB.NodeMetaWithID(event.PubKey)
		if err != nil {
			continue
		}

		// if this event is older than what we have, skip
		if event.CreatedAt.Time().Unix() < node.Timestamp {
			continue
		}

		// if the author has not been crawled AND has low pagerank, skip.
		if node.Status != models.StatusCrawled && node.Pagerank < pagerankThreshold(DB.Size()) {
			continue
		}

		// send the event to the queue
		if err := queueHandler(event); err != nil {
			return
		}
	}
}

// QueryNewPubkeys() extracts pubkeys from the pubkeyChan channel, and queries
// for their events in batches of batchSize.
func QueryNewPubkeys(
	ctx context.Context,
	relays []string,
	pubkeyChan <-chan string,
	batchSize int,
	queueHandler func(event nostr.RelayEvent) error) {

	batch := make([]string, 0, batchSize)
	pool := nostr.NewSimplePool(ctx)
	defer close("QueryNewPubkeys", pool)

	for {
		select {
		case <-ctx.Done():
			return

		case pubkey, ok := <-pubkeyChan:
			if !ok {
				fmt.Println("\n  > Pubkey channel closed, stopping processing.")
				return
			}

			// add to the batch, until the size is big enough to query
			batch = append(batch, pubkey)
			if len(batch) >= batchSize {

				err := QueryPubkeyBatch(ctx, pool, relays, batch, queueHandler)
				if err != nil {
					fmt.Printf("\nError querying pubkeys: %v", err)
				} else {
					// reset the batch
					batch = make([]string, 0, batchSize)
				}
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
	queueHandler func(event nostr.RelayEvent) error) error {

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	filters := nostr.Filters{{
		Kinds:   RelevantKinds,
		Authors: pubkeys,
	}}

	// a map that associates each pubkey with the newest follow list
	newestEvents := make(map[string]nostr.RelayEvent, len(pubkeys))
	for event := range pool.SubManyEose(ctx, relays, filters) {

		newestEvent, exists := newestEvents[event.PubKey]
		if !exists {
			newestEvents[event.PubKey] = event
			continue
		}

		if event.CreatedAt.Time().Unix() > newestEvent.CreatedAt.Time().Unix() {
			newestEvents[event.PubKey] = event
		}
	}

	// send only the newest events to the queue.
	for _, event := range newestEvents {
		if err := queueHandler(event); err != nil {
			return err
		}
	}

	return nil
}

// close iterates over the relays in the pool and closes all connections.
func close(funcName string, pool *nostr.SimplePool) {
	fmt.Printf("\n  > %v: closing relay connections... ", funcName)
	pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
		relay.Close()
		return true
	})
	fmt.Printf("All closed!")
}

// pagerankThreshold returns the minimum pagerank a pubkey needs to have for its
// events to be processed.
func pagerankThreshold(graphSize int) float64 {
	_ = graphSize
	return 0.0
}

// PrintEvent is a simple function that prints the event ID, PubKey and Timestamp.
func PrintEvent(event nostr.RelayEvent) error {
	fmt.Printf("\nevent ID: %v", event.ID)
	fmt.Printf("\nevent pubkey: %v", event.PubKey)
	fmt.Printf("\nevent timestamp: %d\n", event.CreatedAt.Time().Unix())
	return nil
}

// HandleSignals listens for OS signals and triggers context cancellation.
func HandleSignals(cancel context.CancelFunc) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-signalChan // Block until a signal is received
	fmt.Printf("\nSignal received. Shutting down...")
	cancel()
}
