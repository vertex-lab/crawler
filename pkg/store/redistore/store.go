// The redistore package defines a Redis store that fulfills the RandomWalkStore
// interface in models.
package redistore

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
)

const (
	KeyRWS                 string = "RWS"
	KeyAlpha               string = "alpha"
	KeyWalksPerNode        string = "walksPerNode"
	KeyLastWalkID          string = "lastWalkID"
	KeyTotalVisits         string = "totalVisits"
	KeyWalks               string = "walks"
	KeyWalksVisitingPrefix string = "walksVisiting:"
)

// KeyWalksVisiting() returns the Redis key for the nodeWalkIDs with specified nodeID
func KeyWalksVisiting(nodeID uint32) string {
	return fmt.Sprintf("%v%d", KeyWalksVisitingPrefix, nodeID)
}

// RandomWalkStore implements the omonimus interface defined in models.
type RandomWalkStore struct {
	client       *redis.Client
	alpha        float32
	walksPerNode uint16
}

// RWSFields are the fields of the RWS in Redis. This struct is used for serialize and deserialize.
type RWSFields struct {
	Alpha        float32 `redis:"alpha"`
	WalksPerNode uint16  `redis:"walksPerNode"`
	LastWalkID   int     `redis:"lastWalkID"`
	TotalVisits  int     `redis:"totalVisits"`
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
		TotalVisits:  0,
		LastWalkID:   -1, // the first ID will be 0, since we increment and return with HIncrBy
	}

	if err := cl.HSet(ctx, KeyRWS, fields).Err(); err != nil {
		return nil, err
	}

	RWS := &RandomWalkStore{
		client:       cl,
		alpha:        alpha,
		walksPerNode: walksPerNode,
	}
	return RWS, nil
}

// NewRWSConnection() loads the instance of RandomWalkStore using the provided Redis client
func NewRWSConnection(ctx context.Context, cl *redis.Client) (*RandomWalkStore, error) {

	if cl == nil {
		return nil, models.ErrNilClientPointer
	}

	cmdReturn := cl.HMGet(ctx, KeyRWS, KeyAlpha, KeyWalksPerNode)
	if cmdReturn.Err() != nil {
		return nil, cmdReturn.Err()
	}

	// Handle the empty RWS case
	vals := cmdReturn.Val()
	if vals[0] == nil || vals[1] == nil {
		return nil, models.ErrEmptyRWS
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
		alpha:        fields.Alpha,
		walksPerNode: fields.WalksPerNode,
	}
	return RWS, nil
}

// Alpha() returns the dampening factor used for the RandomWalks
func (RWS *RandomWalkStore) Alpha(ctx context.Context) float32 {
	_ = ctx
	return RWS.alpha
}

// WalkPerNode() returns the number of walks to be generated for each node in the DB.
func (RWS *RandomWalkStore) WalksPerNode(ctx context.Context) uint16 {
	_ = ctx
	return RWS.walksPerNode
}

// TotalVisits() returns the total number of visits.
// In case of any error, the default value 0 is returned.
func (RWS *RandomWalkStore) TotalVisits(ctx context.Context) int {
	if err := RWS.Validate(); err != nil {
		return 0
	}

	strVisits, err := RWS.client.HGet(ctx, KeyRWS, KeyTotalVisits).Result()
	if err != nil {
		return 0
	}

	visits, err := redisutils.ParseInt64(strVisits)
	if err != nil {
		return 0
	}

	return int(visits)
}

// Validate() checks the fields of the RWS struct and returns the appropriate error.
func (RWS *RandomWalkStore) Validate() error {

	if RWS == nil {
		return models.ErrNilRWSPointer
	}

	if RWS.client == nil {
		return models.ErrNilClientPointer
	}

	if RWS.alpha <= 0.0 || RWS.alpha >= 1.0 {
		return models.ErrInvalidAlpha
	}

	if RWS.walksPerNode <= 0 {
		return models.ErrInvalidWalksPerNode
	}

	return nil
}

// VisitCounts() returns a slice containing the number of times each node was visited by a walk.
func (RWS *RandomWalkStore) VisitCounts(ctx context.Context, nodeIDs ...uint32) ([]int, error) {

	if err := RWS.Validate(); err != nil {
		return []int{}, err
	}

	if len(nodeIDs) == 0 {
		return []int{}, nil
	}

	pipe := RWS.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(nodeIDs))
	for i, ID := range nodeIDs {
		cmds[i] = pipe.SCard(ctx, KeyWalksVisiting(ID))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return []int{}, err
	}

	visits := make([]int, len(nodeIDs))
	for i := range nodeIDs {
		visits[i] = int(cmds[i].Val())
	}

	return visits, nil
}

// Walks() returns the walks associated with the walkIDs.
func (RWS *RandomWalkStore) Walks(ctx context.Context, walkIDs ...uint32) ([]models.RandomWalk, error) {

	if err := RWS.Validate(); err != nil {
		return nil, err
	}

	if len(walkIDs) == 0 {
		return nil, nil
	}

	const batchSize int = 500000 // asking for more walks in one go can cause problems with Redis
	strIDs := redisutils.FormatIDs(walkIDs)
	batchedIDs := sliceutils.SplitSlice(strIDs, batchSize)

	res := make([]interface{}, 0, len(strIDs))
	for _, IDs := range batchedIDs {
		cmd := RWS.client.HMGet(ctx, KeyWalks, IDs...)
		if cmd.Err() != nil {
			return nil, cmd.Err()
		}

		res = append(res, cmd.Val()...)
	}

	strWalks := make([]string, 0, len(res))
	for i, r := range res {
		strWalk, ok := r.(string)
		if !ok {
			// it means r is nil, which happens when the walk was not found
			return nil, fmt.Errorf("%w: walkID %v", models.ErrWalkNotFound, walkIDs[i])
		}

		strWalks = append(strWalks, strWalk)
	}

	return redisutils.ParseWalks(strWalks)
}

/*
WalksVisiting() returns up to limit UNIQUE walkIDs evenly distributed among the specified nodeIDs.
In other words, it returns up to limit/len(nodeIDs) walkIDs for each of the nodes.

Note:
- If 0 < limit < nodeIDs, no walk is returned
- If limit <= 0, all walks for all nodes are returned (use signalling value limit = -1)
*/
func (RWS *RandomWalkStore) WalksVisiting(ctx context.Context, limit int, nodeIDs ...uint32) ([]uint32, error) {

	if err := RWS.Validate(); err != nil {
		return nil, err
	}

	var limitPerNode int64
	if limit <= 0 {
		limitPerNode = 1000000000 // a very big number to return all
		limit = 100000
	} else {
		limitPerNode = int64(limit) / int64(len(nodeIDs))
	}

	pipe := RWS.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(nodeIDs))
	for i, ID := range nodeIDs {
		cmds[i] = pipe.SRandMemberN(ctx, KeyWalksVisiting(ID), limitPerNode)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	strIDs := make([]string, 0, limit)
	for i, cmd := range cmds {
		IDs := cmd.Val()
		if len(IDs) == 0 {
			return nil, fmt.Errorf("%w: nodeID %v", models.ErrNodeNotFoundRWS, nodeIDs[i])
		}

		strIDs = append(strIDs, IDs...)
	}

	if len(nodeIDs) > 1 {
		// multiple nodes might have walks in common, hence we remove duplicates.
		return redisutils.ParseUniqueIDs(strIDs)
	} else {
		return redisutils.ParseIDs(strIDs)
	}

}

// WalksVisitingAll() returns all the IDs of the walk that visit ALL specified nodes.
func (RWS *RandomWalkStore) WalksVisitingAll(ctx context.Context, nodeIDs ...uint32) ([]uint32, error) {
	if err := RWS.Validate(); err != nil {
		return nil, err
	}

	keys := make([]string, len(nodeIDs))
	for i, ID := range nodeIDs {
		keys[i] = KeyWalksVisiting(ID)
	}

	// check first if all the keys exist, otherwise the intersection will be empty
	pipe := RWS.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.Exists(ctx, key)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("pipeline failed: %w", err)
	}

	for i, cmd := range cmds {
		if cmd.Val() != 1 {
			return nil, fmt.Errorf("%w: nodeID %v", models.ErrNodeNotFoundRWS, nodeIDs[i])
		}
	}

	strIDs, err := RWS.client.SInter(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	return redisutils.ParseIDs(strIDs)
}

// AddWalks() adds all the specified walks to the RWS. If at least one of the walks
// is invalid, no walk gets added.
func (RWS *RandomWalkStore) AddWalks(ctx context.Context, walks ...models.RandomWalk) error {

	if err := RWS.Validate(); err != nil {
		return err
	}

	if len(walks) == 0 {
		return nil
	}

	for _, walk := range walks {
		if err := models.Validate(walk); err != nil {
			return err
		}
	}

	// assign a new walkID to the last walk. It's not possible to do it inside the tx,
	// which means there might be "holes" in the walkIndex, meaning walkIDs NOT associated to any walk.
	lastID, err := RWS.client.HIncrBy(ctx, KeyRWS, KeyLastWalkID, int64(len(walks))).Result()
	if err != nil {
		return err
	}

	pipe := RWS.client.TxPipeline()
	var newVisits int64 = 0
	for i, walk := range walks {
		walkID := uint32(int(lastID) - len(walks) + i + 1) // assigning IDs in the same order

		pipe.HSet(ctx, KeyWalks, redisutils.FormatID(walkID), redisutils.FormatWalk(walk))

		// add the walkID to each node
		for _, nodeID := range walk {
			pipe.SAdd(ctx, KeyWalksVisiting(nodeID), walkID)
		}

		newVisits += int64(len(walk))
	}
	pipe.HIncrBy(ctx, KeyRWS, KeyTotalVisits, newVisits)

	if _, err = pipe.Exec(ctx); err != nil {
		return fmt.Errorf("AddWalk(%v) failed to execute: %v", walks, err)
	}

	return nil
}

// RemoveWalks() removes all the specified walks from the RWS. If one walkID
// is not found, no walk gets removed.
func (RWS *RandomWalkStore) RemoveWalks(ctx context.Context, walkIDs ...uint32) error {

	if err := RWS.Validate(); err != nil {
		return err
	}

	if len(walkIDs) == 0 {
		return nil
	}

	strIDs := redisutils.FormatIDs(walkIDs)
	res, err := RWS.client.HMGet(ctx, KeyWalks, strIDs...).Result()
	if err != nil {
		return err
	}

	walks := make([]models.RandomWalk, 0, len(walkIDs))
	for _, r := range res {
		strWalk, ok := r.(string)
		if !ok {
			return fmt.Errorf("%w: unexpected return type: %v", models.ErrWalkNotFound, res)
		}

		walk, err := redisutils.ParseWalk(strWalk)
		if err != nil {
			return fmt.Errorf("unexpected return type: %v", strWalk)
		}

		walks = append(walks, walk)
	}

	var removedVisits int64 = 0.0
	pipe := RWS.client.TxPipeline()

	for i, strID := range strIDs {
		pipe.HDel(ctx, KeyWalks, strID)

		for _, nodeID := range walks[i] {
			pipe.SRem(ctx, KeyWalksVisiting(nodeID), strID)
		}

		removedVisits += int64(len(walks[i]))
	}
	pipe.HIncrBy(ctx, KeyRWS, KeyTotalVisits, -removedVisits)

	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}

// PruneGraftWalk() encapsulates the functions of pruning and grafting ( = appending to) a walk.
// These functions need to be coupled together to leverage the atomicity of
// Redis transactions.
func (RWS *RandomWalkStore) PruneGraftWalk(ctx context.Context, walkID uint32, cutIndex int, walkSegment models.RandomWalk) error {

	if err := RWS.Validate(); err != nil {
		return err
	}

	if cutIndex < 0 {
		return models.ErrInvalidWalkIndex
	}

	// fetch and parse the walk by the walkID
	walkIDKey := redisutils.FormatID(walkID)
	strWalk, err := RWS.client.HGet(ctx, KeyWalks, walkIDKey).Result()
	if err != nil {
		return err
	}
	walk, err := redisutils.ParseWalk(strWalk)
	if err != nil {
		return err
	}

	if cutIndex > len(walk) {
		return models.ErrInvalidWalkIndex
	}

	// remove the walkID form each pruned node
	pipe := RWS.client.TxPipeline()
	for _, prunedNodeID := range walk[cutIndex:] {
		pipe.SRem(ctx, KeyWalksVisiting(prunedNodeID), walkID)
	}

	// add the walkID to each grafted node
	for _, graftedNodeID := range walkSegment {
		pipe.SAdd(ctx, KeyWalksVisiting(graftedNodeID), walkID)
	}

	// update the totalVisits
	diff := len(walkSegment) - (len(walk) - cutIndex)
	pipe.HIncrBy(ctx, KeyRWS, KeyTotalVisits, int64(diff))

	// prune and graft operation on the walk
	newWalk := append(walk[:cutIndex], walkSegment...)
	pipe.HSet(ctx, KeyWalks, walkIDKey, redisutils.FormatWalk(newWalk))

	if _, err = pipe.Exec(ctx); err != nil {
		return fmt.Errorf("PruneGraftWalk(%v) failed to execute: %v", walk, err)
	}

	return nil
}

// SetupRWS returns a RandomWalkStore ready to be used in tests.
func SetupRWS(cl *redis.Client, RWSType string) (*RandomWalkStore, error) {
	if cl == nil {
		return nil, models.ErrNilClientPointer
	}

	switch RWSType {
	case "nil":
		return nil, nil

	case "empty":
		RWS, err := NewRWS(context.Background(), cl, 0.85, 1)
		if err != nil {
			return nil, err
		}
		return RWS, nil

	case "one-node0":
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walk := models.RandomWalk{0}
		if err := cl.HSet(ctx, KeyWalks, "0", redisutils.FormatWalk(walk)).Err(); err != nil {
			return nil, err
		}

		if err := cl.SAdd(ctx, KeyWalksVisiting(0), 0).Err(); err != nil {
			return nil, err
		}

		if err := RWS.client.HIncrBy(ctx, KeyRWS, KeyLastWalkID, 1).Err(); err != nil {
			return nil, err
		}

		if err := RWS.client.HIncrBy(ctx, KeyRWS, KeyTotalVisits, 1).Err(); err != nil {
			return nil, err
		}

		return RWS, nil

	case "one-walk0":
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walk := models.RandomWalk{0, 1, 2, 3}
		if err := cl.HSet(ctx, KeyWalks, "0", redisutils.FormatWalk(walk)).Err(); err != nil {
			return nil, err
		}

		for _, nodeID := range walk {
			if err := cl.SAdd(ctx, KeyWalksVisiting(nodeID), 0).Err(); err != nil {
				return nil, err
			}
		}

		if err := RWS.client.HIncrBy(ctx, KeyRWS, KeyLastWalkID, 1).Err(); err != nil {
			return nil, err
		}

		if err := RWS.client.HIncrBy(ctx, KeyRWS, KeyTotalVisits, int64(len(walk))).Err(); err != nil {
			return nil, err
		}

		return RWS, nil

	case "triangle":
		// 0 --> 1 --> 2 --> 0
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walks := []models.RandomWalk{{0, 1, 2}, {1, 2, 0}, {2, 0, 1}}
		if err := RWS.AddWalks(context.Background(), walks...); err != nil {
			return nil, err
		}
		return RWS, nil

	case "complex":
		// 0 --> 1 --> 2
		// 0 --> 3
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walks := []models.RandomWalk{{0, 1, 2}, {0, 3}, {1, 2}}
		if err := RWS.AddWalks(context.Background(), walks...); err != nil {
			return nil, err
		}

		return RWS, nil

	case "pip":
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walk := []models.RandomWalk{{0}}
		if err := RWS.AddWalks(context.Background(), walk...); err != nil {
			return nil, err
		}
		return RWS, nil

	default:
		return nil, nil
	}
}

// GenerateRWS() randomly generates walks and adds them to the RWS.
func GenerateRWS(cl *redis.Client, nodesNum, walksNum int) (*RandomWalkStore, error) {
	RWS, err := NewRWS(context.Background(), cl, 0.85, 1)
	if err != nil {
		return nil, err
	}

	walks := make([]models.RandomWalk, 0, walksNum)
	for i := 0; i < walksNum; i++ {
		walk := make(models.RandomWalk, 0, 7)
		for j := 0; j < 7; j++ {
			nodeID := uint32(rand.IntN(nodesNum))
			walk = append(walk, nodeID)
		}

		walks = append(walks, walk)
	}

	if err := RWS.AddWalks(context.Background(), walks...); err != nil {
		return nil, err
	}

	return RWS, nil
}
