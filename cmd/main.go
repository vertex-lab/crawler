package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/crawler"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

func main() {
	PrintTitle()

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
	RWM := &walks.RandomWalkManager{Store: RWS}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go crawler.HandleSignals(cancel)

	eventChan := make(chan nostr.RelayEvent, 1000)
	pubkeyChan := make(chan string, 1000)

	wg.Add(2)
	go func() {
		defer wg.Done()
		crawler.Firehose(ctx, crawler.Relays, DB, 10000, func(event nostr.RelayEvent) error {
			eventChan <- event
			return nil
		})
	}()

	go func() {
		defer wg.Done()
		crawler.QueryNewPubkeys(ctx, crawler.Relays, pubkeyChan, 100, func(event nostr.RelayEvent) error {
			eventChan <- event
			return nil
		})
	}()

	crawler.ProcessFollowListEvents(ctx, eventChan, DB, RWM, func(pubkey string) error {
		pubkeyChan <- pubkey
		return nil
	})

	wg.Wait()
	fmt.Printf("\nExiting\n")
}

// PrintTitle() prints a title.
func PrintTitle() {
	fmt.Println("------------------------")
	fmt.Println("Nostr crawler is running")
	fmt.Println("------------------------")
}
