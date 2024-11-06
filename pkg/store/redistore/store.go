package redistore

import (
	"context"
	"fmt"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	"github.com/redis/go-redis/v9"
)

// RandomWalkStore fullfills the RandomWalkStore interface defined in models
type RandomWalkStore struct {
	client       *redis.Client
	ctx          context.Context
	alpha        float32
	walksPerNode uint16
}

// Alpha() returns the dampening factor used for the RandomWalks
func (RWS *RandomWalkStore) Alpha() float32 {
	return RWS.alpha
}

// WalkPerNode() returns the number of walks to be generated for each node in the DB
func (RWS *RandomWalkStore) WalksPerNode() uint16 {
	return RWS.walksPerNode
}

// NewRWS creates a new instance of RandomWalkStore using the provided Redis client,
// and overwrites alpha and walksPerNode in Redis.
func NewRWS(ctx context.Context, cl *redis.Client,
	alpha float32, walksPerNode uint16) (*RandomWalkStore, error) {

	if alpha <= 0 || alpha >= 1 {
		return nil, models.ErrInvalidAlpha
	}

	if walksPerNode <= 0 {
		return nil, models.ErrInvalidWalksPerNode
	}

	// Overwrites alpha and walksPerNode in Redis
	err := cl.Set(context.Background(), "alpha", alpha, 0).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to set alpha: %v", err)
	}

	err = cl.Set(context.Background(), "walksPerNode", walksPerNode, 0).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to set walksPerNode: %v", err)
	}

	RWS := &RandomWalkStore{
		client:       cl,
		ctx:          context.Background(),
		alpha:        alpha,
		walksPerNode: walksPerNode,
	}
	return RWS, nil
}

// LoadRWS() loads the instance of RandomWalkStore using the provided Redis client
func LoadRWS(ctx context.Context, cl *redis.Client) (*RandomWalkStore, error) {

	alpha, err := GetAndParse(ctx, cl, "alpha", "float32")
	if err != nil {
		return nil, err
	}

	if alpha.(float32) <= 0 || alpha.(float32) >= 1 {
		return nil, models.ErrInvalidAlpha
	}

	walksPerNode, err := GetAndParse(ctx, cl, "walksPerNode", "uint16")
	if err != nil {
		return nil, err
	}

	if walksPerNode.(uint16) <= 0 {
		return nil, models.ErrInvalidWalksPerNode
	}

	RWS := &RandomWalkStore{
		client:       cl,
		ctx:          context.Background(),
		alpha:        alpha.(float32),
		walksPerNode: walksPerNode.(uint16),
	}
	return RWS, nil
}
