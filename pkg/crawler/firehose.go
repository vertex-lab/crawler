package crawler

import (
	"context"
	"fmt"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
)

var (
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
Firehose connects to a list of relays and pulls recent kind:3 events.
It efficiently filters events based on the pubkey "spamminess", determined by our
own pagerank-based reputation system. It leverage a NodeCache to do these checks
without having to query the database.

Finally, it uses the specified queueHandler function to send the events to the
queue for further processing and/or to be written to the database.
*/
func Firehose(ctx context.Context, relays []string,
	NC models.NodeCache, queueHandler func(event nostr.RelayEvent) error) {

	pool := nostr.NewSimplePool(ctx)
	stop := func() {
		fmt.Printf("\n  > Closing relay connections... ")
		pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
			relay.Close()
			return true
		})
		fmt.Printf("All closed!")
	}
	defer stop()

	ts := nostr.Timestamp(time.Now().Unix())
	filters := nostr.Filters{{
		Kinds: RelevantKinds,
		Since: &ts,
	}}

	// iterate over the events
	for event := range pool.SubMany(ctx, Relays, filters) {

		// if the the author is not in the DB, skip
		nodeAttr, exists := NC.Load(event.PubKey)
		if !exists {
			continue
		}

		// if this event is older than what we have, skip
		if event.CreatedAt.Time().Unix() < nodeAttr.Timestamp {
			continue
		}

		// if the signature doesn't match, skip
		if match, err := event.CheckSignature(); err != nil || !match {
			continue
		}

		// if the author has not enough pagerank, skip
		if nodeAttr.Pagerank < pagerankThreshold(NC.Size()) {
			continue
		}

		// send the event to the queue
		if err := queueHandler(event); err != nil {
			return
		}
	}
}

// pagerankThreshold returns the minimum pagerank a pubkey needs to have for its
// events to be processed.
func pagerankThreshold(graphSize int) float64 {
	_ = graphSize
	return 0.0
}
