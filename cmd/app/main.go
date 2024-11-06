// cmd/app/main.go
package main

import (
	"context"
	"fmt"

	redis "github.com/redis/go-redis/v9"
)

func main() {
	fmt.Println("Nostrcrawler is running")

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // No password set
		DB:       0,  // Use default DB
		Protocol: 2,  // Connection protocol
	})

	ctx := context.Background()

	if err := client.Set(ctx, "TestWalk", "[0,1,2,3]", 0).Err(); err != nil {
		panic(err)
	}

	val, err := client.Get(ctx, "TestWalk").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("TestWalk", val)
}
