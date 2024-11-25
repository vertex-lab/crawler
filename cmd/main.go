package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/vertex-lab/crawler/pkg/crawler"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

const fiatjaf = "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d"

func main() {
	PrintTitle()

	cl := redisutils.SetupClient()
	DB, err := redisdb.NewDatabase(context.Background(), cl)
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

	eventChan := make(chan nostr.RelayEvent, 1000)
	pubkeyChan := make(chan string, 1000)
	pubkeyChan <- fiatjaf

	eventCounter := xsync.NewCounter()

	go crawler.HandleSignals(cancel)
	go DisplayStats(ctx, DB, RWM, eventCounter, eventChan, pubkeyChan)

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

	crawler.ProcessFollowListEvents(ctx, eventChan, DB, RWM, eventCounter, func(pubkey string) error {
		pubkeyChan <- pubkey
		return nil
	})

	wg.Wait()
	fmt.Printf("\nExiting\n")
}

func DisplayStats(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	eventCounter *xsync.Counter,
	eventChan <-chan nostr.RelayEvent,
	pubkeyChan <-chan string,
) {
	ticker := time.NewTicker(5 * time.Second) // Update stats every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n  > Stopping stats display...")
			return

		case <-ticker.C:
			// Fetch stats
			eventChanLen := len(eventChan)
			eventChanCap := cap(eventChan)
			pubkeyChanLen := len(pubkeyChan)
			pubkeyChanCap := cap(pubkeyChan)
			goroutines := runtime.NumGoroutine()
			memStats := new(runtime.MemStats)
			runtime.ReadMemStats(memStats)

			fmt.Printf("\n--- System Stats ---\n")
			fmt.Printf("Database Size: %d nodes\n", DB.Size())
			fmt.Printf("Event Channel: %d/%d (used/total)\n", eventChanLen, eventChanCap)
			fmt.Printf("Pubkey Channel: %d/%d (used/total)\n", pubkeyChanLen, pubkeyChanCap)
			fmt.Printf("Processed Events: %d\n", eventCounter.Value())
			fmt.Printf("Goroutines: %d\n", goroutines)
			fmt.Printf("Memory Usage: %.2f MB\n", float64(memStats.Alloc)/(1024*1024))
			fmt.Println("---------------------")
		}
	}
}

// PrintTitle() prints a title.
func PrintTitle() {
	fmt.Println("------------------------")
	fmt.Println("Nostr crawler is running")
	fmt.Println("------------------------")
}
