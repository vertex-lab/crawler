package crawler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
)

// Manually change pip's follow list and see if the events gets printed. Works only with `go test`
func TestFirehose(t *testing.T) {
	DB := mockdb.SetupDB("pip")
	RWS := mockstore.SetupRWS("one-node0")
	logger := logger.New(os.Stdout)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	go HandleSignals(cancel, logger)
	Firehose(ctx, logger, DB, RWS, Relays, 500, PrintEvent)
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
