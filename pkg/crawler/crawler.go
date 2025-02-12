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

type FirehoseConfig struct {
	Log    *logger.Aggregate
	Relays []string
}

func NewFirehoseConfig() FirehoseConfig {
	return FirehoseConfig{
		Log:    logger.New(os.Stdout),
		Relays: Relays,
	}
}

func (c FirehoseConfig) Print() {
	fmt.Printf("Firehose\n")
	fmt.Printf("  Relays: %v\n", c.Relays)
}

/*
Firehose connects to a list of relays and pulls kind:3 events that are newer than the current time.
It efficiently filters events based on the pubkey "spamminess", determined by our own pagerank-based reputation system.

Finally, it uses the specified queueHandler function to send the events to the
queue for further processing and/or to be written to the database.
*/
func Firehose(
	ctx context.Context,
	config FirehoseConfig,
	DB models.Database,
	queueHandler func(event *nostr.Event) error) {

	pool := nostr.NewSimplePool(ctx)
	defer close(config.Log, pool, "Firehose")

	ts := nostr.Now()
	filters := nostr.Filters{{
		Kinds: RelevantKinds,
		Since: &ts,
	}}

	for event := range pool.SubMany(ctx, config.Relays, filters) {
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

		if IsEventOutdated(node, event.Event) {
			continue
		}

		if err := queueHandler(event.Event); err != nil {
			config.Log.Error("Firehose queue handler: %v", err)
		}
	}
}

type QueryPubkeysConfig struct {
	Log       *logger.Aggregate
	Relays    []string
	BatchSize int
	Interval  time.Duration
}

func NewQueryPubkeysConfig() QueryPubkeysConfig {
	return QueryPubkeysConfig{
		Log:       logger.New(os.Stdout),
		Relays:    Relays,
		BatchSize: 50,
		Interval:  time.Minute,
	}
}

func (c QueryPubkeysConfig) Print() {
	fmt.Printf("Query\n")
	fmt.Printf("  Relays: %v\n", c.Relays)
	fmt.Printf("  BatchSize: %d\n", c.BatchSize)
	fmt.Printf("  Interval: %v\n", c.Interval)
}

// QueryPubkeys() extracts pubkeys from the pubkeyChan channel, and queries for
// their events when the batch is bigger than config.batchSize, OR after config.Interval since the last query.
func QueryPubkeys(
	ctx context.Context,
	config QueryPubkeysConfig,
	pubkeyChan <-chan string,
	queueHandler func(event *nostr.Event) error) {

	batch := make([]string, 0, config.BatchSize)
	timer := time.After(config.Interval)

	pool := nostr.NewSimplePool(ctx)
	defer close(config.Log, pool, "QueryPubkeys")

	for {
		select {
		case <-ctx.Done():
			return

		case pubkey, ok := <-pubkeyChan:
			if !ok {
				config.Log.Warn("Pubkey queue closed, stopped processing.")
				return
			}

			batch = append(batch, pubkey)
			if len(batch) < config.BatchSize {
				continue
			}

			if err := QueryPubkeyBatch(ctx, pool, config.Relays, batch, queueHandler); err != nil {
				config.Log.Error("QueryPubkeys(): %v", err)
				continue
			}

			// reset batch and timer only if successful
			batch = make([]string, 0, config.BatchSize)
			timer = time.After(config.Interval)

		case <-timer:

			if err := QueryPubkeyBatch(ctx, pool, config.Relays, batch, queueHandler); err != nil {
				config.Log.Error("QueryPubkeys(): %v", err)
				continue
			}

			// reset batch and timer only if successful
			batch = make([]string, 0, config.BatchSize)
			timer = time.After(config.Interval)
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

	// a map that associates each pair (pubkey,kind) with the latest event from that authors for that kind.
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

		key := fmt.Sprintf("%s:%d", event.PubKey, event.Kind) // "<pubkey>:<kind>"" represent the pair (pubkey, kind)
		e, exists := latest[key]

		if !exists || event.CreatedAt > e.CreatedAt {
			latest[key] = event.Event
		}
	}

	// send only the latest events to the queue.
	for _, event := range latest {
		if err := queueHandler(event); err != nil {
			return fmt.Errorf("queueHandler(): %w", err)
		}
	}

	return nil
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

// ------------------------------------HELPERS----------------------------------

// IsEventOutdated() returns whether it exists a record of node that is newer than the specified event.
// e.g. `event` is a follow-list that is OLDER than the latest follow-list we processed for that node, and should therefore be ignored.
func IsEventOutdated(node *models.Node, event *nostr.Event) bool {
	if node == nil || node.Records == nil {
		return false
	}

	var filterType = KindToRecordType(event.Kind)
	for _, rec := range node.Records {
		if rec.Type != filterType {
			continue
		}

		if rec.Timestamp >= event.CreatedAt.Time().Unix() {
			return true
		}
	}

	return false
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
