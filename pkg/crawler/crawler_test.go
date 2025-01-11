package crawler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
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

func TestQueryPubkeyBatch(t *testing.T) {
	logger := logger.New(os.Stdout)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go HandleSignals(cancel, logger)

	pool := nostr.NewSimplePool(context.Background())
	defer close(logger, pool, "QueryPubkeyBatch")

	pubkeys := []string{pip, calle, gigi, odell}
	QueryPubkeyBatch(ctx, pool, Relays, pubkeys, PrintEvent)
}
