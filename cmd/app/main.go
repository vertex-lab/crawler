// cmd/app/main.go
package main

import (
	"fmt"

	"github.com/pippellia-btc/Nostrcrawler/pkg/database/redisdb"
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

	_, err := redisdb.SetupDB(cl, "one-node0")
	if err != nil {
		panic(err)
	}
}
