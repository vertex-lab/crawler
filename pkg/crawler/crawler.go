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
func Firehose(ctx context.Context, relays []string, DB models.Database,
	timeLimit int64, queueHandler func(event nostr.RelayEvent) error) {

	pool := nostr.NewSimplePool(ctx)
	defer close(pool)

	ts := nostr.Timestamp(time.Now().Unix() - timeLimit)
	filters := nostr.Filters{{
		Kinds: RelevantKinds,
		Since: &ts,
	}}

	// iterate over the events
	for event := range pool.SubMany(ctx, relays, filters) {

		// if the signature doesn't match, skip
		if match, err := event.CheckSignature(); err != nil || !match {
			continue
		}

		// query from the DB the node associated with that pubkey
		nodeMeta, err := DB.NodeMeta(event.PubKey)
		if err != nil {
			continue
		}

		// if this event is older than what we have, skip
		if event.CreatedAt.Time().Unix() < nodeMeta.Timestamp {
			continue
		}

		// if the author has not enough pagerank, skip
		if nodeMeta.Pagerank < pagerankThreshold(DB.Size()) {
			continue
		}

		// send the event to the queue
		if err := queueHandler(event); err != nil {
			return
		}
	}
}

// QueryAuthors() queries the follow lists of the specified pubkeys.
// It sends the newest events for each pubkey to the queue using the provided queueHandler.
func QueryAuthors(ctx context.Context, relays, pubkeys []string,
	queueHandler func(event nostr.RelayEvent) error) error {

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	pool := nostr.NewSimplePool(ctx)
	defer close(pool)

	filters := nostr.Filters{{
		Kinds:   RelevantKinds,
		Authors: pubkeys,
	}}

	// a map that associates each pubkey with the newest follow list
	eventMap := make(map[string]nostr.RelayEvent, len(pubkeys))
	for event := range pool.SubManyEose(ctx, relays, filters) {

		newestEvent, exists := eventMap[event.PubKey]
		if !exists {
			eventMap[event.PubKey] = event
			continue
		}

		if event.CreatedAt.Time().Unix() > newestEvent.CreatedAt.Time().Unix() {
			eventMap[event.PubKey] = event
		}
	}

	// send only the newest events to the queue.
	for _, event := range eventMap {
		if err := queueHandler(event); err != nil {
			return err
		}
	}

	return nil
}

// close iterates over the relays in the pool and closes all connections.
func close(pool *nostr.SimplePool) {
	fmt.Printf("\n  > Closing relay connections... ")
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

// handleSignals listens for OS signals and triggers context cancellation.
func handleSignals(cancel context.CancelFunc) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-signalChan // Block until a signal is received
	fmt.Printf("\nSignal received. Shutting down...")
	cancel()
}
