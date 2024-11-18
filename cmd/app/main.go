// cmd/app/main.go
package main

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

func main() {

	fmt.Println("Nostrcrawler is running")
	cl := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	_ = cl
}
