package main

import (
	"context"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
)

// pagerankThreshold returns the minimum pagerank a pubkey needs to have for its
// events to be processed.
func pagerankThreshold(graphSize int) float64 {
	_ = graphSize
	return 0.0
}

/*
Firehose connects to a list of relays and pulls recent kind:3 events.
It efficiently filters events based on the pubkey "spamminess", determined by our
own pagerank-based reputation system. It leverage a NodeCache to do these checks
without having to query the database.

Finally, it uses the specified queueHandler function to send the events to the
queue for further processing and/or to be written to the database.
*/
func Firehose(ctx context.Context, cancel context.CancelFunc, relays []string,
	NC models.NodeCache, queueHandler func(event nostr.RelayEvent) error) {

	pool := nostr.NewSimplePool(ctx)
	stop := func() {
		pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
			relay.Close()
			return true
		})
		cancel()
	}
	defer stop()

	ts := nostr.Timestamp(time.Now().Unix())
	filters := nostr.Filters{{
		Kinds: []int{nostr.KindFollowList},
		Since: &ts,
	}}

	// iterate over the events
	for event := range pool.SubMany(ctx, Relays, filters) {

		// if the the author is NOT "reputable enough" to be in the DB, skip
		nodeAttr, exists := NC.Load(event.PubKey)
		if !exists {
			continue
		}

		// if this event is older than what we have, skip
		if event.CreatedAt.Time().Unix() < nodeAttr.Timestamp {
			continue
		}

		// if the author has NOT enough pagerank, skip
		if nodeAttr.Pagerank < pagerankThreshold(NC.Size()) {
			continue
		}

		// if the signature doesn't match, skip
		if match, err := event.CheckSignature(); err != nil || !match {
			continue
		}

		// send the event to the queue
		if err := queueHandler(event); err != nil {
			return
		}
	}
}
