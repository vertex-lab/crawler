package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/crawler"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

func main() {
	fmt.Println("------------------------")
	fmt.Println("Nostr crawler is running")
	fmt.Println("------------------------")

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

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleSignals(cancel)

	eventChan := make(chan nostr.RelayEvent, 1000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		crawler.Firehose(ctx, crawler.Relays, NC, func(event nostr.RelayEvent) error {
			eventChan <- event
			return nil
		})
	}()

	if err := ProcessEvents(ctx, eventChan); err != nil {
		panic(err)
	}
	wg.Wait()
	fmt.Printf("\nExiting\n")
}

// ProcessEvents processes events from the event channel until the context is canceled.
func ProcessEvents(ctx context.Context, eventChan <-chan nostr.RelayEvent) error {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\n  > Finishing processing the event... ")
			return nil

		case event, ok := <-eventChan:
			if !ok {
				fmt.Println("Event channel closed, exiting...")
				return nil
			}

			if err := crawler.PrintEvent(event); err != nil {
				// Log the error but continue processing other events
				fmt.Printf("Error processing event %s: %v\n", event.ID, err)
			}
		}
	}
}

// handleSignals listens for OS signals and triggers context cancellation.
func handleSignals(cancel context.CancelFunc) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-signalChan // Block until a signal is received
	fmt.Printf("\nSignal received. Shutting down...")
	cancel()
}
