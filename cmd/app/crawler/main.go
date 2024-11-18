package main

import (
	"context"
	"fmt"
	"log"

	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

func main() {
	fmt.Println("Nostr crawler is running")
	cl := redisutils.SetupClient()

	DB, err := redisdb.NewDatabase(context.Background(), cl)
	if err != nil {
		log.Fatalf("NewDatabase() failed: %v", err)
	}

	RWS, err := redistore.LoadRWS(context.Background(), cl)
	if err != nil {
		log.Fatalf("LoadRWS() failed: %v", err)
	}

	_ = DB
	_ = RWS
}
