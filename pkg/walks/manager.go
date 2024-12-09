// The walks package encapsulates all the logic around generating and updating random walks.
package walks

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/models"
	mock "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
)

// RandomWalkManager wraps the RandomWalkStore and extends its functionalities
// with complex methods like Update() and Generate().
type RandomWalkManager struct {
	Store models.RandomWalkStore
}

// NewRWM() returns a RWM using the Redis RWS.
func NewRWM(cl *redis.Client, alpha float32, walksPerNode uint16) (*RandomWalkManager, error) {
	RWS, err := redistore.NewRWS(context.Background(), cl, alpha, walksPerNode)
	if err != nil {
		return nil, err
	}
	return &RandomWalkManager{Store: RWS}, nil
}

// NewMockRWM() returns a RWM using the mock RWS
func NewMockRWM(alpha float32, walksPerNode uint16) (*RandomWalkManager, error) {
	RWS, err := mock.NewRWS(alpha, walksPerNode)
	if err != nil {
		return nil, err
	}
	return &RandomWalkManager{Store: RWS}, nil
}

// function that returns a RWM setup based on the RWMType.
func SetupMockRWM(RWMType string) *RandomWalkManager {
	return &RandomWalkManager{Store: mock.SetupRWS(RWMType)}
}
