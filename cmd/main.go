package main

import (
	"context"
	"fmt"
	"log"
	"os"
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

const logFile = "app.log"

func main() {
	Start()

	ctx := context.Background()
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	DB, err := redisdb.SetupDB(cl, "pip")
	if err != nil {
		panic(err)
	}

	RWS, err := redistore.SetupRWS(cl, "one-node0")
	if err != nil {
		panic(err)
	}
	RWM := &walks.RandomWalkManager{Store: RWS}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan := make(chan nostr.RelayEvent, 1000)
	pubkeyChan := make(chan string, 100000)

	eventCounter := xsync.NewCounter()

	go crawler.HandleSignals(cancel)
	go DisplayStats(ctx, DB, RWM, eventCounter, eventChan, pubkeyChan)

	wg.Add(2)
	go func() {
		defer wg.Done()
		crawler.Firehose(ctx, crawler.Relays, DB, 0, func(event nostr.RelayEvent) error {
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
	log.Printf("INFO: Exiting")
}

func DisplayStats(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	eventCounter *xsync.Counter,
	eventChan <-chan nostr.RelayEvent,
	pubkeyChan <-chan string,
) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	firstDisplay := true
	const statsLines = 9
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
			fmt.Printf("Database Size: %d nodes\n", DB.Size())
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

func Start() {
	InitLogger(logFile)
	PrintTitle()
}

// InitLogger() returns a logger
func InitLogger(filePath string) {
	// Open the log file or create it if it doesn't exist
	logFile, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.Println("INFO: Nostr crawler is starting up")
}

// PrintTitle() prints a title.
func PrintTitle() {
	fmt.Println("------------------------")
	fmt.Println("Nostr crawler is running")
	fmt.Println("------------------------")
}
