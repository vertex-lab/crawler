package main

import (
	"fmt"

	"github.com/vertex-lab/crawler/pkg/models"
)

func main() {
	// fmt.Println("------------------------")
	// fmt.Println("Nostr crawler is running")
	// fmt.Println("------------------------")

	// cl := redisutils.SetupClient()
	// defer redisutils.CleanupRedis(cl)

	// DB, err := redisdb.SetupDB(cl, "pip")
	// if err != nil {
	// 	panic(err)
	// }

	// RWS, err := redistore.NewRWS(context.Background(), cl, 0.85, 10)
	// if err != nil {
	// 	panic(err)
	// }

	// NC, err := DB.NodeCache()
	// if err != nil {
	// 	panic(err)
	// }

	// _ = DB
	// _ = RWS
	// _ = NC

	// var wg sync.WaitGroup
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	// go handleSignals(cancel)

	// eventChan := make(chan nostr.RelayEvent, 1000)
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	crawler.Firehose(ctx, crawler.Relays, NC, func(event nostr.RelayEvent) error {
	// 		eventChan <- event
	// 		return nil
	// 	})
	// }()

	// if err := ProcessEvents(ctx, eventChan); err != nil {
	// 	panic(err)
	// }
	// wg.Wait()
	// fmt.Printf("\nExiting\n")

	nodeMeta := models.NodeMeta{}
	fmt.Printf(".timestamp = %v", nodeMeta.Timestamp)

}

// // ProcessEvents processes events from the event channel until the context is canceled.
// func ProcessEvents(ctx context.Context, eventChan <-chan nostr.RelayEvent) error {
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			fmt.Printf("\n  > Finishing processing the event... ")
// 			return nil

// 		case event, ok := <-eventChan:
// 			if !ok {
// 				fmt.Println("Event channel closed, exiting...")
// 				return nil
// 			}

// 			if err := crawler.PrintEvent(event); err != nil {
// 				// Log the error but continue processing other events
// 				fmt.Printf("Error processing event %s: %v\n", event.ID, err)
// 			}
// 		}
// 	}
// }

// // handleSignals listens for OS signals and triggers context cancellation.
// func handleSignals(cancel context.CancelFunc) {
// 	signalChan := make(chan os.Signal, 1)
// 	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

// 	<-signalChan // Block until a signal is received
// 	fmt.Printf("\nSignal received. Shutting down...")
// 	cancel()
// }

// pk := "503f9927838af9ae5701d96bc3eade86bb776582922b0394766a41a2ccee1c7a"
// met := "ee9ba8b3dd0b2e8507d4ac6dfcd3fb2e7f0cc20f220f410a9ce3eccaac79fecd"

// _ = pk
// _ = met

// pool := nostr.NewSimplePool(context.Background())
// stop := func() {
// 	fmt.Printf("\n  > Closing relay connections... ")
// 	pool.Relays.Range(func(_ string, relay *nostr.Relay) bool {
// 		relay.Close()
// 		return true
// 	})
// 	fmt.Printf("All closed!")
// }
// defer stop()

// filters := nostr.Filters{{
// 	Kinds:   []int{3},
// 	Authors: []string{met},
// }}

// // iterate over the events
// for event := range pool.SubMany(context.Background(), crawler.Relays, filters) {

// 	fmt.Printf("\nevent ID: %v", event.ID)
// 	fmt.Printf("\nevent pubkey: %v", event.PubKey)
// 	fmt.Printf("\nevent kind: %v", event.Kind)
// 	fmt.Printf("\nevent createdAt: %v", event.CreatedAt)
// 	fmt.Printf("\n\nevent tags: %v", event.Tags)
// 	fmt.Printf("\n\nevent Content: %v", event.Content)
// 	fmt.Printf("\nevent Signature: %v", event.Sig)
// 	break
// }
