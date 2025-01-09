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
	"github.com/vertex-lab/crawler/pkg/utils/counter"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
	"github.com/vertex-lab/crawler/pkg/walks"
)

var DB models.Database
var RWM *walks.RandomWalkManager

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := LoadConfig()
	if err != nil {
		panic(err)
	}

	logger := logger.New(config.LogWriter)
	defer config.CloseLogs()

	switch config.Mode {
	case "prod":
		DB, err = redisdb.NewDatabaseConnection(ctx, config.RedisClient)
		if err != nil {
			panic(err)
		}

		RWM, err = walks.NewRWMConnection(ctx, config.RedisClient)
		if err != nil {
			panic(err)
		}

	case "init":
		// make sure the DB is empty before initializing
		size, err := config.RedisClient.DBSize(context.Background()).Result()
		if err != nil {
			panic(err)
		}
		if size != 0 {
			panic(models.ErrNonEmptyDB)
		}

		DB, err = redisdb.SetupDB(config.RedisClient, "pip")
		if err != nil {
			panic(err)
		}

		RWM, err = walks.NewRWM(config.RedisClient, 0.85, 100)
		if err != nil {
			panic(err)
		}

		if err := RWM.GenerateAll(context.Background(), DB); err != nil {
			panic(err)
		}
	}

	eventChan := make(chan *nostr.Event, config.EventChanCapacity)
	pubkeyChan := make(chan string, config.PubkeyChanCapacity)
	eventCounter := xsync.NewCounter()
	pagerankTotal := counter.NewFloatCounter() // tracks the pagerank mass accumulated since the last full recomputation.

	PrintStartup(logger)
	defer PrintShutdown(logger)
	go crawler.HandleSignals(cancel, logger)
	if config.DisplayStats {
		go DisplayStats(ctx, DB, RWM, eventChan, pubkeyChan, eventCounter, pagerankTotal)
	}

	// spawn the Firehose, the QueryPubkeys and NodeArbiter as three goroutines.
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		crawler.Firehose(ctx, logger, DB, RWM.Store, crawler.Relays, config.PagerankMultiplier, func(event *nostr.Event) error {
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
		crawler.QueryPubkeys(ctx, logger, crawler.Relays, pubkeyChan, config.QueryPubkeysBatchSize, func(event *nostr.Event) error {
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
		crawler.NodeArbiter(ctx, logger, DB, RWM, pagerankTotal, config.NodeArbiterActivationThreshold, config.PagerankMultiplier, func(pubkey string) error {
			select {
			case pubkeyChan <- pubkey:
			default:
				logger.Warn("NodeArbiter: Channel is full, dropping pubkey: %v", pubkey)
			}
			return nil
		})
	}()

	crawler.ProcessEvents(ctx, logger, DB, RWM, eventChan, eventCounter, pagerankTotal)
	wg.Wait()
}

// -----------------------------------HELPERS----------------------------------

func DisplayStats(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	eventChan <-chan *nostr.Event,
	pubkeyChan <-chan string,
	eventCounter *xsync.Counter,
	pagerankTotal *counter.Float) {

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	const statsLines = 10
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
			fmt.Println(" Stopped stats display.")
			return

		case <-ticker.C:
			eventChanLen := len(eventChan)
			eventChanCap := cap(eventChan)
			pubkeyChanLen := len(pubkeyChan)
			pubkeyChanCap := cap(pubkeyChan)
			goroutines := runtime.NumGoroutine()
			memStats := new(runtime.MemStats)
			runtime.ReadMemStats(memStats)

			clearStats()
			fmt.Printf("\n------------ System Stats -------------\n")
			fmt.Printf("Database Size: %d nodes\n", DB.Size(ctx))
			fmt.Printf("Event Channel: %d/%d\n", eventChanLen, eventChanCap)
			fmt.Printf("Pubkey Channel: %d/%d\n", pubkeyChanLen, pubkeyChanCap)
			fmt.Printf("Processed Events: %d\n", eventCounter.Value())
			fmt.Printf("Pagerank since last recomputation: %v\n", pagerankTotal.Load())
			fmt.Printf("Goroutines: %d\n", goroutines)
			fmt.Printf("Memory Usage: %.2f MB\n", float64(memStats.Alloc)/(1024*1024))
			fmt.Println("---------------------------------------")
			firstDisplay = false
		}
	}
}

// PrintStartup() prints a simple start up message.
func PrintStartup(l *logger.Aggregate) {
	l.Info("---------------------------------------")
	l.Info("Nostr crawler is starting up")
}

// PrintShutdown() prints a simple shutdown message.
func PrintShutdown(l *logger.Aggregate) {
	l.Info("Shutdown")
	l.Info("---------------------------------------")
}
