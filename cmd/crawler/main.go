package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

var Relays = []string{
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

const pagerankThreshold float64 = 0.0

func main() {
	fmt.Println("------------------------")
	fmt.Println("Nostr crawler is running")
	fmt.Println("------------------------\n")

	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	DB, err := redisdb.SetupDB(cl, "pip")
	if err != nil {
		panic(err)
	}

	RWS, err := redistore.NewRWS(context.Background(), cl, 0.85, 10)
	if err != nil {
		panic(err)
	}

	NC, err := DB.NodeCache()
	if err != nil {
		panic(err)
	}

	_ = DB
	_ = RWS
	_ = NC

	// initialize nostr relay pool and filters
	pool := nostr.NewSimplePool(context.Background())
	ts := nostr.Timestamp(time.Now().Unix() - 10000000)
	filters := nostr.Filters{{
		Kinds: []int{nostr.KindFollowList},
		Since: &ts,
	}}

	// iterate over the events
	eventCounter := xsync.NewCounter()
	for event := range pool.SubMany(context.Background(), Relays, filters) {

		eventCounter.Inc()
		fmt.Printf("\rEvents processed: %d", eventCounter.Value())

		// if the the author is NOT "reputable enough" to be in the DB, skip
		nodeAttr, exists := NC.Load(event.PubKey)
		if !exists {
			continue
		}

		// if the author has NOT enough pagerank, skip
		if nodeAttr.Pagerank < pagerankThreshold {
			continue
		}

		// if this event is older than what we have, skip
		if event.CreatedAt.Time().Unix() < nodeAttr.Timestamp {
			continue
		}

		// if the ID doesn't match, skip
		if !event.CheckID() {
			continue
		}

		// if the signature doesn't match, skip
		if match, err := event.CheckSignature(); err != nil || !match {
			continue
		}
	}
}
