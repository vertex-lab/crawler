package crawler

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/database/mock"
)

func TestFirehose(t *testing.T) {
	// I will manually change the follow list and see if the events gets printed. Works only with `go test`
	DB := mock.SetupDB("pip")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT)

	go func() {
		select {
		case <-signalChan:
			cancel() // Cancel the context when SIGINT is received (Ctrl + C)
		case <-time.After(time.Second * 20):
			cancel() // Cancel after 20 seconds
		}
	}()

	Firehose(ctx, Relays, DB, 2000, PrintEvent)
}

func TestQueryPubkeyBatch(t *testing.T) {

	pool := nostr.NewSimplePool(context.Background())
	defer close(pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go HandleSignals(cancel)

	pubkeys := []string{pip, calle, gigi, odell}
	QueryPubkeyBatch(ctx, pool, Relays, pubkeys, PrintEvent)
}
