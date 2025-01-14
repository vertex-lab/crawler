package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/crawler"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/counter"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
	"github.com/vertex-lab/crawler/pkg/walks"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redis := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	size, err := redis.DBSize(ctx).Result()
	if err != nil {
		panic(err)
	}

	var config *Config
	var DB models.Database
	var RWM *walks.RandomWalkManager

	switch size {
	case 0:
		// if redis is empty, initialize a new database with the INIT_PUBKEYS specified in the init.env file
		config, err = LoadConfig("init.env")
		if err != nil {
			panic(err)
		}

		DB, err = redisdb.NewDatabaseFromPubkeys(ctx, redis, config.InitPubkeys)
		if err != nil {
			panic(err)
		}

		RWM, err = walks.NewRWM(ctx, redis, 0.85, 100)
		if err != nil {
			panic(err)
		}

		if err = RWM.GenerateAll(ctx, DB); err != nil {
			panic(err)
		}

	default:
		config, err = LoadConfig("prod.env")
		if err != nil {
			panic(err)
		}

		DB, err = redisdb.NewDatabaseConnection(ctx, redis)
		if err != nil {
			panic(err)
		}

		RWM, err = walks.NewRWMConnection(ctx, redis)
		if err != nil {
			panic(err)
		}
	}

	config.Print()
	return

	// configuring all the logs to write to the same place
	logger := logger.New(config.LogWriter)
	nostr.InfoLogger.SetOutput(config.LogWriter)
	nostr.DebugLogger.SetOutput(config.LogWriter)
	defer config.CloseLogs()

	PrintStartup(logger)
	defer PrintShutdown(logger)
	go crawler.HandleSignals(cancel, logger)

	eventCounter := xsync.NewCounter()         // tracks the number of events processed
	pagerankTotal := counter.NewFloatCounter() // tracks the pagerank mass accumulated since the last scan of NodeArbiter.

	eventChan := make(chan *nostr.Event, config.EventChanCapacity)
	pubkeyChan := make(chan string, config.PubkeyChanCapacity)
	for _, pk := range config.InitPubkeys { // send the initialization pubkeys to the queue (if any)
		pubkeyChan <- pk
	}

	// spawn the Firehose, the QueryPubkeys and NodeArbiter as three goroutines.
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		crawler.Firehose(ctx, logger, DB, crawler.Relays, func(event *nostr.Event) error {
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
		crawler.QueryPubkeys(ctx, logger, crawler.Relays, pubkeyChan, config.QueryBatchSize, config.QueryInterval, func(event *nostr.Event) error {
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
		crawler.NodeArbiter(ctx, logger, DB, RWM, pagerankTotal, config.NodeArbiterActivationThreshold, config.PromotionMultiplier, config.DemotionMultiplier, func(pubkey string) error {
			select {
			case pubkeyChan <- pubkey:
			default:
				logger.Warn("NodeArbiter: Channel is full, dropping pubkey: %v", pubkey)
			}
			return nil
		})
	}()

	if config.DisplayStats {
		go DisplayStats(ctx, DB, RWM, eventChan, pubkeyChan, eventCounter, pagerankTotal)
	}

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
