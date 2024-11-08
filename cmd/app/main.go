// cmd/app/main.go
package main

import (
	"context"
	"fmt"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	"github.com/pippellia-btc/Nostrcrawler/pkg/store/redistore"
	"github.com/redis/go-redis/v9"
)

func main() {

	fmt.Println("Nostrcrawler is running")
	cl := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // No password set
		DB:       0,  // Use default DB
		Protocol: 2,  // Connection protocol
	})

	ctx := context.Background()

	RWS, err := redistore.NewRWS(ctx, cl, 0.85, 10)
	if err != nil {
		panic(err)
	}

	err = RWS.AddWalk(models.RandomWalk{0, 1, 2, 3})
	if err != nil {
		panic(err)
	}
}
