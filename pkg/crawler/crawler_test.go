package crawler

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/vertex-lab/crawler/pkg/models"
)

func TestFirehose(t *testing.T) {
	// I will manually change the follow list and see if the events get printed
	pip := "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
	nodeAttr := models.NodeFilterAttributes{
		ID:        0,
		Timestamp: 0,
		Pagerank:  1.0,
	}
	NC := models.NewNodeCache()
	NC.Store(pip, nodeAttr)

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

	Firehose(ctx, Relays, NC, PrintEvent)
}
