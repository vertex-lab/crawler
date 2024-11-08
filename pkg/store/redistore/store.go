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

// RWSFields are the fields of the RandomWalkStore in Redis. This struct is used for serialize and deserialize.
type RWSFields struct {
	Alpha        float32 `redis:"alpha"`
	WalksPerNode uint16  `redis:"walksPerNode"`
	NextNodeID   uint32  `redis:"nextNodeID"`
	NextWalkID   uint32  `redis:"nextWalkID"`
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
// and overwrites alpha, walksPerNode, nextNodeID, and nextWalkID in a Redis hash named "rws".
func NewRWS(ctx context.Context, cl *redis.Client, alpha float32, walksPerNode uint16) (*RandomWalkStore, error) {

	// Validate input parameters
	if alpha <= 0 || alpha >= 1 {
		return nil, models.ErrInvalidAlpha
	}
	if walksPerNode <= 0 {
		return nil, models.ErrInvalidWalksPerNode
	}

	fields := RWSFields{
		Alpha:        alpha,
		WalksPerNode: walksPerNode,
		NextNodeID:   0,
		NextWalkID:   0,
	}

	if err := cl.HSet(ctx, "RWS", fields).Err(); err != nil {
		return nil, err
	}

	RWS := &RandomWalkStore{
		client:       cl,
		ctx:          ctx,
		alpha:        alpha,
		walksPerNode: walksPerNode,
	}
	return RWS, nil
}

// LoadRWS() loads the instance of RandomWalkStore using the provided Redis client
func LoadRWS(ctx context.Context, cl *redis.Client) (*RandomWalkStore, error) {

	cmdReturn := cl.HMGet(ctx, "RWS", "alpha", "walksPerNode")
	if cmdReturn.Err() != nil {
		return nil, cmdReturn.Err()
	}

	var fields RWSFields
	if err := cmdReturn.Scan(&fields); err != nil {
		return nil, err
	}

	if fields.Alpha <= 0 || fields.Alpha >= 1 {
		return nil, models.ErrInvalidAlpha
	}

	if fields.WalksPerNode <= 0 {
		return nil, models.ErrInvalidWalksPerNode
	}

	RWS := &RandomWalkStore{
		client:       cl,
		ctx:          ctx,
		alpha:        fields.Alpha,
		walksPerNode: fields.WalksPerNode,
	}
	return RWS, nil
}

// IsEmpty() returns false if "walk:0" is found in Redis, otherwise true.
func (RWS *RandomWalkStore) IsEmpty() bool {
	if RWS == nil {
		return true
	}

	_, err := RWS.client.Get(context.Background(), KeyRedis(KeyWalk, 0)).Result()
	return err != nil
}

// Validate() checks the fields alpha, walksPerNode and whether the RWS is nil, empty or
// non-empty and returns an appropriate error based on the requirement.
func (RWS *RandomWalkStore) Validate(expectEmptyRWS bool) error {

	if RWS == nil {
		return models.ErrNilRWSPointer
	}

	if RWS.alpha <= 0.0 || RWS.alpha >= 1.0 {
		return models.ErrInvalidAlpha
	}

	if RWS.walksPerNode <= 0 {
		return models.ErrInvalidWalksPerNode
	}

	empty := RWS.IsEmpty()
	if empty && !expectEmptyRWS {
		return models.ErrEmptyRWS
	}

	if !empty && expectEmptyRWS {
		return models.ErrNonEmptyRWS
	}

	return nil
}

// AddWalk() adds the walk to the WalkIndex. It also adds the walkID to the
// WalkIDSet of each node the walk visited. This means that for each node
// visited by the walk, the walkID will be added to its WalkSet.
func (RWS *RandomWalkStore) AddWalk(walk models.RandomWalk) error {

	if RWS == nil {
		return models.ErrNilRWSPointer
	}

	if err := models.Validate(walk); err != nil {
		return err
	}

	// Begins the transaction
	pipe := RWS.client.TxPipeline()

	// assign a new walkID to this walk
	walkID, err := pipe.Incr(RWS.ctx, "RWS:nextWalkID").Uint64()
	if err != nil {
		return err
	}

	// add the walk to the WalkIndex
	walkKey := KeyRedis(KeyWalk, uint32(walkID))
	strWalk := FormatWalk(walk)
	pipe.Set(RWS.ctx, walkKey, strWalk, 0)

	// add the walkID to each node
	for _, nodeID := range walk {
		nodeKey := fmt.Sprintf("nodeWalkIDs:%d", nodeID)
		pipe.SAdd(RWS.ctx, nodeKey, walkID)
	}

	// Execute the transaction
	_, err = pipe.Exec(RWS.ctx)
	if err != nil {
		return fmt.Errorf("AddWalk(%v) failed to execute: %v", walk, err)
	}

	return nil
}
