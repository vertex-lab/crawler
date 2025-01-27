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
	DB := mockdb.SetupDB("pip")
	logger := logger.New(os.Stdout)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	go HandleSignals(cancel, logger)
	Firehose(ctx, logger, DB, Relays, PrintEvent)
}

func TestQueryPubkeys(t *testing.T) {
	// These two tests should print the same 4 events, as the only difference is the triggering factor.
	t.Run("batchSize", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		logger := logger.New(os.Stdout)
		go HandleSignals(cancel, logger)

		pubkeyChan := make(chan string, 10)
		pubkeys := []string{pip, calle, gigi, odell}
		for _, pk := range pubkeys { // send the pubkeys to the queue
			pubkeyChan <- pk
		}

		// the channel contains enough pubkeys, so it should query immediately and then print.
		QueryPubkeys(ctx, logger, Relays, pubkeyChan, 4, 30*time.Second, PrintEvent)
	})

	t.Run("timer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		logger := logger.New(os.Stdout)
		go HandleSignals(cancel, logger)

		pubkeyChan := make(chan string, 10)
		pubkeys := []string{pip, calle, gigi, odell}
		for _, pk := range pubkeys { // send the pubkeys to the queue
			pubkeyChan <- pk
		}

		// there aren't enough pubkeys, but the timer will kick in, so it should query and then print.
		QueryPubkeys(ctx, logger, Relays, pubkeyChan, 11, 2*time.Second, PrintEvent)
	})
}
