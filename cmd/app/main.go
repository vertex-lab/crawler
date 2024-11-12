// cmd/app/main.go
package main

import (
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

	RWS, err := redistore.SetupRWS(cl, "one-walk0")
	if err != nil {
		panic(err)
	}

	walkSegment := models.RandomWalk{}
	if err := RWS.PruneGraftWalk(0, 1, walkSegment); err != nil {
		panic(err)
	}

	// walkID := uint32(0)
	// cutIndex := 1
	// newWalkSegment := models.RandomWalk{2, 3, 4}

	// if err := RWS.PruneGraftWalk(walkID, cutIndex, newWalkSegment); err != nil {
	// 	panic(err)
	// }
}
