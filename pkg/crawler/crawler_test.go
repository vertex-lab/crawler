package crawler

import (
	"context"
	"os"
	"testing"
	"time"

	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
)

// Manually change pip's follow list and see if the events gets printed. Works only with `go test`
func TestFirehose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	DB := mockdb.SetupDB("pip")
	config := FirehoseConfig{
		log:    logger.New(os.Stdout),
		relays: Relays,
	}

	go HandleSignals(cancel, config.log)
	Firehose(ctx, DB, PrintEvent, config)
}

func TestQueryPubkeys(t *testing.T) {
	// These two tests should print the same 4 events, as the only difference is the triggering factor.
	t.Run("batchSize", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		config := QueryPubkeysConfig{
			log:           logger.New(os.Stdout),
			relays:        Relays,
			batchSize:     4,
			queryInterval: 30 * time.Second,
		}

		config.log.Info("---------------------batchSize---------------------")
		go HandleSignals(cancel, config.log)

		// the queue contains enough pubkeys (4), so it should query immediately and then print.
		pubkeyChan := make(chan string, 10)
		pubkeys := []string{pip, calle, gigi, odell}
		for _, pk := range pubkeys {
			pubkeyChan <- pk
		}

		QueryPubkeys(ctx, pubkeyChan, PrintEvent, config)
	})

	t.Run("timer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		config := QueryPubkeysConfig{
			log:           logger.New(os.Stdout),
			relays:        Relays,
			batchSize:     5,
			queryInterval: 3 * time.Second,
		}

		config.log.Info("---------------------timer---------------------")
		go HandleSignals(cancel, config.log)

		// there aren't enough pubkeys, but the timer will kick in, so it should query and then print.
		pubkeyChan := make(chan string, 10)
		pubkeys := []string{pip, calle, gigi, odell}
		for _, pk := range pubkeys {
			pubkeyChan <- pk
		}

		QueryPubkeys(ctx, pubkeyChan, PrintEvent, config)
	})
}
