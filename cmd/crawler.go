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
	"github.com/vertex-lab/crawler/pkg/utils/logger"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

const logFilePath = "crawler.log"

func main() {
	logger, logFile := logger.Init(logFilePath)
	defer logFile.Close()
	PrintTitle(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cl := redisutils.SetupProdClient()
	DB, err := redisdb.NewDatabaseConnection(ctx, cl)
	if err != nil {
		panic(err)
	}
	RWS, err := redistore.NewRWSConnection(ctx, cl)
	if err != nil {
		panic(err)
	}
	RWM := &walks.RandomWalkManager{Store: RWS}

	eventChan := make(chan nostr.RelayEvent, 10000)
	pubkeyChan := make(chan string, 1000000)
	eventCounter := xsync.NewCounter()

	go crawler.HandleSignals(cancel, logger)
	go DisplayStats(ctx, logger, DB, RWM, eventCounter, eventChan, pubkeyChan)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		crawler.Firehose(ctx, logger, crawler.Relays, DB, 0, func(event nostr.RelayEvent) error {
			select {
			case eventChan <- event:
			default:
				logger.Warn("Firehose: Channel is full, dropping eventID: %v by %v", event.ID, event.PubKey)
			}
			return nil
		})
	}()

	go func() {
		defer wg.Done()
		crawler.QueryPubkeys(ctx, logger, crawler.Relays, pubkeyChan, 100, func(event nostr.RelayEvent) error {
			select {
			case eventChan <- event:
			default:
				logger.Warn("QueryPubkeys: Channel is full, dropping eventID: %v by %v", event.ID, event.PubKey)
			}
			return nil
		})
	}()

	go func() {
		defer wg.Done()
		crawler.NodeArbiter(ctx, logger, DB, RWM, 30, func(pubkey string) error {
			select {
			case pubkeyChan <- pubkey:
			default:
				logger.Warn("NodeArbiter: Channel is full, dropping pubkey: %v", pubkey)
			}
			return nil
		})
	}()

	crawler.ProcessEvents(ctx, logger, eventChan, DB, RWM, eventCounter)

	wg.Wait()
	fmt.Printf("\nExiting\n")
	logger.Info("Exiting")
	logger.Info("------------------------------------------------------")
}

func DisplayStats(
	ctx context.Context,
	logger *logger.Aggregate,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	eventCounter *xsync.Counter,
	eventChan <-chan nostr.RelayEvent,
	pubkeyChan <-chan string,
) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	const statsLines = 9
	firstDisplay := true
	clearStats := func() {
		if !firstDisplay {
			// Move the cursor up by `statsLines` and clear those lines
			fmt.Printf("\033[%dA", statsLines)
			fmt.Print("\033[J")
		}
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\n  > Stopping stats display...")
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

			clearStats()

			fmt.Printf("\n--- System Stats ---\n")
			fmt.Printf("Database Size: %d nodes\n", DB.Size(ctx))
			fmt.Printf("Event Channel: %d/%d\n", eventChanLen, eventChanCap)
			fmt.Printf("Pubkey Channel: %d/%d\n", pubkeyChanLen, pubkeyChanCap)
			fmt.Printf("Processed Events: %d\n", eventCounter.Value())
			fmt.Printf("Goroutines: %d\n", goroutines)
			fmt.Printf("Memory Usage: %.2f MB\n", float64(memStats.Alloc)/(1024*1024))
			fmt.Println("---------------------")

			firstDisplay = false
		}
	}
}

// PrintTitle() prints a title.
func PrintTitle(l *logger.Aggregate) {
	fmt.Println("------------------------")
	fmt.Println("Nostr crawler is running")
	fmt.Println("------------------------")

	l.Info("------------------------------------------------------")
	l.Info("Nostr crawler is starting up")
}
