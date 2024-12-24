package crawler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
)

func TestFirehose(t *testing.T) {
	// I will manually change the follow list and see if the events gets printed.
	// Works only with `go test`
	DB := mock.SetupDB("pip")
	logger := logger.New(os.Stdout)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	go HandleSignals(cancel, logger)
	Firehose(ctx, logger, Relays, DB, 2000, PrintEvent)
}

func TestQueryPubkeyBatch(t *testing.T) {
	pool := nostr.NewSimplePool(context.Background())
	defer close("QueryPubkeyBatch", pool)

	logger := logger.New(os.Stdout)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go HandleSignals(cancel, logger)

	pubkeys := []string{pip, calle, gigi, odell}
	QueryPubkeyBatch(ctx, logger, pool, Relays, pubkeys, PrintEvent)
}
