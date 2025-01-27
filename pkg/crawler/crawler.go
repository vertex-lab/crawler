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

		if !event.CheckID() {
			continue
		}

		if match, err := event.CheckSignature(); err != nil || !match {
			continue
		}

		node, err := DB.NodeByKey(ctx, event.PubKey)
		if err != nil || node == nil {
			continue // If the node is not found (err != nil), skip
		}

		// if the author is an inactive node, skip
		if node.Status == models.StatusInactive {
			continue
		}

		if event.CreatedAt.Time().Unix() < LatestEventTimestamp(node, event.Kind) {
			continue
		}

		if err := queueHandler(event.Event); err != nil {
			logger.Error("Firehose queue handler: %v", err)
			return
		}
	}
}

// QueryPubkeys() extracts pubkeys from the pubkeyChan channel, and queries for
// their events when the batch is bigger than batchSize, OR after queryInterval since the last query.
func QueryPubkeys(
	ctx context.Context,
	logger *logger.Aggregate,
	relays []string,
	pubkeyChan <-chan string,
	batchSize int,
	queryInterval time.Duration,
	queueHandler func(event *nostr.Event) error) {

	batch := make([]string, 0, batchSize)
	timer := time.After(queryInterval)

	pool := nostr.NewSimplePool(ctx)
	defer close(logger, pool, "QueryPubkeys")

	for {
		select {
		case <-ctx.Done():
			return

		case pubkey, ok := <-pubkeyChan:
			if !ok {
				logger.Warn("Pubkey channel closed, stopping processing.")
				return
			}

			batch = append(batch, pubkey)
			if len(batch) < batchSize {
				continue
			}

			if err := QueryPubkeyBatch(ctx, pool, relays, batch, queueHandler); err != nil {
				logger.Error("QueryPubkeys queue handler: %v", err)
				continue
			}

			// reset batch and timer only if successful
			batch = make([]string, 0, batchSize)
			timer = time.After(queryInterval)

		case <-timer:

			if err := QueryPubkeyBatch(ctx, pool, relays, batch, queueHandler); err != nil {
				logger.Error("QueryPubkeys(): %w", err)
				continue
			}

			// reset batch and timer only if successful
			batch = make([]string, 0, batchSize)
			timer = time.After(queryInterval)
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

	if len(pubkeys) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	filter := nostr.Filter{
		Kinds:   RelevantKinds,
		Authors: pubkeys,
	}

	// a map that associates each pair (pubkey,kind) with the latest event of that kind.
	latest := make(map[string]*nostr.Event, len(pubkeys)*len(filter.Kinds))
	for event := range pool.SubManyEose(ctx, relays, nostr.Filters{filter}) {

		if event.Event == nil {
			continue
		}

		if !event.CheckID() {
			continue
		}

		if match, err := event.CheckSignature(); err != nil || !match {
			continue
		}

		key := KeyPubkeyKind(event.PubKey, event.Kind)
		e, exists := latest[key]
		if !exists {
			latest[key] = event.Event
			continue
		}

		if event.CreatedAt.Time().Unix() > e.CreatedAt.Time().Unix() {
			latest[key] = event.Event
		}
	}

	// send only the newest events to the queue.
	for _, event := range latest {
		if err := queueHandler(event); err != nil {
			return fmt.Errorf("queueHandler(): %w", err)
		}
	}

	return nil
}

// ------------------------------------HELPERS----------------------------------

// LatestEventTimestamp() returns the timestamp of the latest event of node for the specified kind.
// For example, it returns the timestamp of the latest follow-list of a node.
func LatestEventTimestamp(node *models.Node, kind int) int64 {
	if node == nil || node.Records == nil {
		return 0
	}

	var filterType = KindToRecordType(kind)
	var newest int64
	for _, rec := range node.Records {
		if rec.Type != filterType {
			continue
		}

		if rec.Timestamp > newest {
			newest = rec.Timestamp
		}
	}

	return newest
}

// KindToRecordType() returns the appropriate record type for the specified event kind.
func KindToRecordType(kind int) string {
	switch kind {
	case nostr.KindFollowList:
		return models.Follow

	default:
		return ""
	}
}

// Close() iterates over the relays in the pool and closes all connections.
func close(logger *logger.Aggregate, pool *nostr.SimplePool, funcName string) {
	logger.Info("  > " + funcName + ": closing relay connections... ")
	pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
		relay.Close()
		return true
	})
}

// KeyPubkeyKind() returns the string "<pubkey>:<kind>", useful as a key in maps that need to associates one value to the pair (pubkey, kind).
func KeyPubkeyKind(pubkey string, kind int) string {
	return fmt.Sprintf("%s:%d", pubkey, kind)
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
