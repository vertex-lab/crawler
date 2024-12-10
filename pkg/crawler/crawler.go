package crawler

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/logger"
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
	logger *logger.Aggregate,
	relays []string,
	DB models.Database,
	timeLimit int64, // use a value <= 1000, or you will get rate-limited
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

		// If the node is not found (err != nil), skip
		node, err := DB.NodeByKey(event.PubKey)
		if err != nil {
			continue
		}

		if event.CreatedAt.Time().Unix() < node.EventTS {
			continue
		}

		// if the author has low pagerank, skip.
		if node.Pagerank < pagerankThreshold(DB.Size(), bottom) {
			continue
		}

		if err := queueHandler(event); err != nil {
			logger.Error("Firehose queue handler: %v", err)
			return
		}
	}
}

// QueryPubkeys() extracts pubkeys from the pubkeyChan channel, and queries
// for their events in batches of batchSize.
func QueryPubkeys(
	ctx context.Context,
	logger *logger.Aggregate,
	relays []string,
	pubkeyChan <-chan string,
	batchSize int,
	queueHandler func(event nostr.RelayEvent) error) {

	batch := make([]string, 0, batchSize)
	pool := nostr.NewSimplePool(ctx)
	defer close("QueryPubkeys", pool)

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
			if len(batch) >= batchSize {

				err := QueryPubkeyBatch(ctx, pool, relays, batch, queueHandler)
				if err != nil {
					logger.Error("QueryPubkeys queue handler: %v", err)
				} else {
					// reset the batch only if successful, otherwise retry
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

		if match, err := event.CheckSignature(); err != nil || !match {
			continue
		}

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

// Close() iterates over the relays in the pool and closes all connections.
func close(funcName string, pool *nostr.SimplePool) {
	fmt.Printf("\n  > %v: closing relay connections... ", funcName)
	pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
		relay.Close()
		return true
	})
	fmt.Printf("All closed!")
}

// constants for pagerankThreshold. bottom and top respectively represents
// the bottom 5%, and top the top 0.1% of the pagerank distribution.
const (
	bottom float64 = 0.95
	top    float64 = 0.001
)

/*
PagerankThreshold returns the pagerank satisfied by the `percentageCut` of the nodes
according to the following (approximated) exponential distribution:

	p_j ~ (1-b) / N^(1-b) * j^(-b)

Where p_j is the j-th highest pagerank, b is the exponent, N is the size of the graph.
For example, to determine a threshold satisfied by 95% of all the nodes, we do:

	p_(0.95 * N) = (1-b) / N^(1-b) * (0.95 * N)^(-b) = (1-b) * 0.95^(-b) / N

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func pagerankThreshold(graphSize int, percentageCut float64) float64 {
	// the exponent
	const b float64 = 0.76
	return (1 - b) * math.Pow(percentageCut, -b) / float64(graphSize)
}

// PrintEvent() is a simple function that prints the event ID, PubKey and Timestamp.
func PrintEvent(event nostr.RelayEvent) error {
	fmt.Printf("\nevent ID: %v", event.ID)
	fmt.Printf("\nevent pubkey: %v", event.PubKey)
	fmt.Printf("\nevent timestamp: %d\n", event.CreatedAt.Time().Unix())
	return nil
}

// HandleSignals() listens for OS signals and triggers context cancellation.
func HandleSignals(cancel context.CancelFunc, logger *logger.Aggregate) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-signalChan // Block until a signal is received
	fmt.Printf("\nSignal received. Shutting down...")
	logger.Info("Signal received. Shutting down...")
	cancel()
}
