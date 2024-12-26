package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/vertex-lab/crawler/pkg/utils/counter"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

const configFilePath string = "config.json"

// The configuration parameters for the crawler.
type Config struct {
	LogFilePath           string  `json:"log_file_path"`
	EventChanCapacity     int     `json:"event_chan_capacity"`
	PubkeyChanCapacity    int     `json:"pubkey_chan_capacity"`
	FirehoseTimeLimit     int64   `json:"firehose_time_limit"`
	QueryPubkeysBatchSize int     `json:"query_pubkeys_batch_size"`
	NodeArbiterThreshold  float64 `json:"node_arbiter_threshold"`
}

// LoadConfig() returns an initialized config.
func LoadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &Config{}
	if err := json.NewDecoder(file).Decode(config); err != nil {
		return nil, err
	}

	return config, nil
}

func main() {

	config, err := LoadConfig(configFilePath)
	if err != nil {
		panic(err)
	}

	logger, logFile := logger.Init(config.LogFilePath)
	defer logFile.Close()

	nostr.InfoLogger = logger.InfoLogger
	nostr.DebugLogger = logger.WarnLogger

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

	eventChan := make(chan *nostr.Event, config.EventChanCapacity)
	pubkeyChan := make(chan string, config.PubkeyChanCapacity)
	eventCounter := xsync.NewCounter()
	pagerankTotal := counter.NewFloatCounter()

	PrintTitle(logger)

	go crawler.HandleSignals(cancel, logger)
	go DisplayStats(ctx, logger, DB, RWM, eventChan, pubkeyChan, eventCounter, pagerankTotal)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		crawler.Firehose(ctx, logger, DB, RWS, crawler.Relays, config.FirehoseTimeLimit, func(event *nostr.Event) error {
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
		crawler.NodeArbiter(ctx, logger, DB, RWM, config.NodeArbiterThreshold, pagerankTotal, func(pubkey string) error {
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
	fmt.Printf("\nExiting\n")
	logger.Info("Exiting")
	logger.Info("------------------------------------------------------")
}

func DisplayStats(
	ctx context.Context,
	logger *logger.Aggregate,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	eventChan <-chan *nostr.Event,
	pubkeyChan <-chan string,
	eventCounter *xsync.Counter,
	pagerankTotal *counter.Float) {

	ticker := time.NewTicker(2 * time.Second)
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
			fmt.Printf("\n  > Stopping stats display...")
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
			fmt.Printf("\n--- System Stats ---\n")
			fmt.Printf("Database Size: %d nodes\n", DB.Size(ctx))
			fmt.Printf("Event Channel: %d/%d\n", eventChanLen, eventChanCap)
			fmt.Printf("Pubkey Channel: %d/%d\n", pubkeyChanLen, pubkeyChanCap)
			fmt.Printf("Processed Events: %d\n", eventCounter.Value())
			fmt.Printf("Pagerank since last recomputation: %v\n", pagerankTotal.Load())
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
