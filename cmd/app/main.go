// cmd/app/main.go
package main

import (
	"context"
	"fmt"
	"reflect"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	"github.com/pippellia-btc/Nostrcrawler/pkg/store/redistore"
	redis "github.com/redis/go-redis/v9"
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
	expectedWalk := models.RandomWalk{0, 1, 2, 3, 4, 5, 6, 111}

	walk, err := redistore.GetStringAndParse(ctx, cl, "walk:0", "RandomWalk")
	if err != nil {
		panic(err)
	}

	if !reflect.DeepEqual(expectedWalk, walk) {
		fmt.Printf("expected %v, got %v", expectedWalk, walk)
	}
}
