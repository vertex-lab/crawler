// The redistore package defines a Redis store that fulfills the RandomWalkStore
// interface in models.
package redistore

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
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
	ctx          context.Context
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
		ctx:          ctx,
		alpha:        alpha,
		walksPerNode: walksPerNode,
	}
	return RWS, nil
}

// LoadRWS() loads the instance of RandomWalkStore using the provided Redis client
func LoadRWS(ctx context.Context, cl *redis.Client) (*RandomWalkStore, error) {

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
		ctx:          ctx,
		alpha:        fields.Alpha,
		walksPerNode: fields.WalksPerNode,
	}
	return RWS, nil
}

// Alpha() returns the dampening factor used for the RandomWalks
func (RWS *RandomWalkStore) Alpha() float32 {
	return RWS.alpha
}

// WalkPerNode() returns the number of walks to be generated for each node in the DB.
func (RWS *RandomWalkStore) WalksPerNode() uint16 {
	return RWS.walksPerNode
}

// TotalVisits() returns the total number of visits.
// In case of any error, the default value 0 is returned.
func (RWS *RandomWalkStore) TotalVisits() int {
	if err := RWS.validateFields(); err != nil {
		return 0
	}

	strVisits, err := RWS.client.HGet(RWS.ctx, KeyRWS, KeyTotalVisits).Result()
	if err != nil {
		return 0
	}

	visits, err := redisutils.ParseInt64(strVisits)
	if err != nil {
		return 0
	}

	return int(visits)
}

// IsEmpty() returns false if the size of the hash key "walks" is > 0, otherwise true.
func (RWS *RandomWalkStore) IsEmpty() bool {
	if RWS == nil {
		return true
	}

	len, err := RWS.client.HLen(RWS.ctx, KeyWalks).Result()
	return err != nil || len == 0
}

// Validate() checks the fields alpha, walksPerNode and whether the RWS is nil, empty or
// non-empty and returns an appropriate error based on the requirement.
func (RWS *RandomWalkStore) Validate(expectEmptyRWS bool) error {

	if err := RWS.validateFields(); err != nil {
		return err
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

// validateFields() checks the fields of the RWS struct and returns the appropriate error.
func (RWS *RandomWalkStore) validateFields() error {

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

// VisitCounts() returns a map that associates each nodeID with the number of
// times it was visited by a walk.
func (RWS *RandomWalkStore) VisitCounts(nodeIDs []uint32) (map[uint32]int, error) {

	if RWS == nil || RWS.client == nil {
		return map[uint32]int{}, models.ErrNilRWSPointer
	}

	if len(nodeIDs) == 0 {
		return map[uint32]int{}, nil
	}

	pipe := RWS.client.Pipeline()

	cmdMap := make(map[uint32]*redis.IntCmd, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		cmdMap[nodeID] = pipe.SCard(RWS.ctx, KeyWalksVisiting(nodeID))
	}

	if _, err := pipe.Exec(RWS.ctx); err != nil {
		return map[uint32]int{}, err
	}

	visitMap := make(map[uint32]int, len(nodeIDs))
	for nodeID, cmd := range cmdMap {
		visitMap[nodeID] = int(cmd.Val())
	}

	return visitMap, nil
}

// VisitCounts() returns a map that associates each nodeID with the number of
// times it was visited by a walk.
func (RWS *RandomWalkStore) VisitCountsLUA(nodeIDs []uint32) (map[uint32]int, error) {

	if RWS == nil || RWS.client == nil {
		return map[uint32]int{}, models.ErrNilRWSPointer
	}

	if len(nodeIDs) == 0 {
		return map[uint32]int{}, nil
	}

	// Prepare keys for the Lua script
	keys := make([]string, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		keys[i] = KeyWalksVisiting(nodeID)
	}

	// Lua script for batching SCARD commands
	luaScript := `
		local results = {}
		for i, key in ipairs(KEYS) do
			results[i] = redis.call('SCARD', key)
		end
		return results
	`

	// execute the Lua script
	result, err := RWS.client.Eval(RWS.ctx, luaScript, keys, KeyWalks).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to execute Lua script: %v", err)
	}

	// Parse the results into the visitMap
	visitMap := make(map[uint32]int, len(nodeIDs))
	counts, ok := result.([]interface{})
	if !ok {
		return map[uint32]int{}, fmt.Errorf("unexpected result type: %T", result)
	}

	for i, count := range counts {
		nodeID := nodeIDs[i]
		visitMap[nodeID] = int(count.(int64)) // Redis results are returned as int64
	}

	return visitMap, nil
}

// Walks() returns a map of walks by walksID that visit nodeID.
func (RWS *RandomWalkStore) Walks(nodeID uint32, limit int) (map[uint32]models.RandomWalk, error) {

	if err := RWS.validateFields(); err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 1000000000 // a very large number, such that SRANDMEMBER returns everything
	}

	strIDs, err := RWS.client.SRandMemberN(RWS.ctx, KeyWalksVisiting(nodeID), int64(limit)).Result()
	if err != nil {
		return map[uint32]models.RandomWalk{}, err
	}
	if len(strIDs) == 0 {
		return map[uint32]models.RandomWalk{}, models.ErrNodeNotFoundRWS
	}

	walkIDs, err := redisutils.ParseIDs(strIDs)
	if err != nil {
		return map[uint32]models.RandomWalk{}, err
	}

	res, err := RWS.client.HMGet(RWS.ctx, KeyWalks, strIDs...).Result()
	if err != nil {
		return map[uint32]models.RandomWalk{}, err
	}

	strWalks := make([]string, 0, len(res))
	for _, r := range res {
		strWalk, ok := r.(string)
		if !ok {
			return map[uint32]models.RandomWalk{}, fmt.Errorf("unexpected type: %v", res)
		}

		strWalks = append(strWalks, strWalk)
	}

	walks, err := redisutils.ParseWalks(strWalks)
	if err != nil {
		return map[uint32]models.RandomWalk{}, err
	}

	walkMap := make(map[uint32]models.RandomWalk, len(walkIDs))
	for i, walkID := range walkIDs {
		walkMap[walkID] = walks[i]
	}

	return walkMap, nil
}

// AddWalks() adds all the specified walks to the RWS. If at least one of the walks
// is invalid, no one gets added.
func (RWS *RandomWalkStore) AddWalks(walks []models.RandomWalk) error {

	if err := RWS.validateFields(); err != nil {
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
	lastID, err := RWS.client.HIncrBy(RWS.ctx, KeyRWS, KeyLastWalkID, int64(len(walks))).Result()
	if err != nil {
		return err
	}

	pipe := RWS.client.TxPipeline()
	var newVisits int64 = 0
	for i, walk := range walks {
		walkID := uint32(int(lastID) - len(walks) + i + 1) // assigning IDs in the same order

		pipe.HSet(RWS.ctx, KeyWalks, redisutils.FormatID(walkID), redisutils.FormatWalk(walk))

		// add the walkID to each node
		for _, nodeID := range walk {
			pipe.SAdd(RWS.ctx, KeyWalksVisiting(nodeID), walkID)
		}

		newVisits += int64(len(walk))
	}
	pipe.HIncrBy(RWS.ctx, KeyRWS, KeyTotalVisits, newVisits)

	if _, err = pipe.Exec(RWS.ctx); err != nil {
		return fmt.Errorf("AddWalk(%v) failed to execute: %v", walks, err)
	}

	return nil
}

// RemoveWalks() removes the all the specified walks from the RWS. If one walkID
// is not found, no walk gets removed.
func (RWS *RandomWalkStore) RemoveWalks(walkIDs []uint32) error {

	if err := RWS.validateFields(); err != nil {
		return err
	}

	if len(walkIDs) == 0 {
		return nil
	}

	strIDs := redisutils.FormatIDs(walkIDs)
	res, err := RWS.client.HMGet(RWS.ctx, KeyWalks, strIDs...).Result()
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
		pipe.HDel(RWS.ctx, KeyWalks, strID)

		for _, nodeID := range walks[i] {
			pipe.SRem(RWS.ctx, KeyWalksVisiting(nodeID), strID)
		}

		removedVisits += int64(len(walks[i]))
	}
	pipe.HIncrBy(RWS.ctx, KeyRWS, KeyTotalVisits, -removedVisits)

	if _, err := pipe.Exec(RWS.ctx); err != nil {
		return err
	}

	return nil
}

// PruneGraftWalk() encapsulates the functions of Pruning and
// Grafting ( = appending to) a walk.
// These functions need to be coupled together to leverage the atomicity of
// Redis transactions. This ensures that a walk is either uneffected or is both
// pruned and grafted successfully.
func (RWS *RandomWalkStore) PruneGraftWalk(walkID uint32, cutIndex int, walkSegment models.RandomWalk) error {

	if err := RWS.validateFields(); err != nil {
		return err
	}

	if cutIndex < 0 {
		return models.ErrInvalidWalkIndex
	}

	// fetch and parse the walk by the walkID
	walkIDKey := redisutils.FormatID(walkID)
	strWalk, err := RWS.client.HGet(RWS.ctx, KeyWalks, walkIDKey).Result()
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
		pipe.SRem(RWS.ctx, KeyWalksVisiting(prunedNodeID), walkID)
	}

	// add the walkID to each grafted node
	for _, graftedNodeID := range walkSegment {
		pipe.SAdd(RWS.ctx, KeyWalksVisiting(graftedNodeID), walkID)
	}

	// update the totalVisits
	diff := len(walkSegment) - (len(walk) - cutIndex)
	pipe.HIncrBy(RWS.ctx, KeyRWS, KeyTotalVisits, int64(diff))

	// prune and graft operation on the walk
	newWalk := append(walk[:cutIndex], walkSegment...)
	pipe.HSet(RWS.ctx, KeyWalks, walkIDKey, redisutils.FormatWalk(newWalk))

	if _, err = pipe.Exec(RWS.ctx); err != nil {
		return fmt.Errorf("PruneGraftWalk(%v) failed to execute: %v", walk, err)
	}

	return nil
}

// SetupRWS returns a RandomWalkStore ready to be used in tests
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

		if err := RWS.client.HIncrBy(RWS.ctx, KeyRWS, KeyLastWalkID, 1).Err(); err != nil {
			return nil, err
		}

		if err := RWS.client.HIncrBy(RWS.ctx, KeyRWS, KeyTotalVisits, 1).Err(); err != nil {
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

		if err := RWS.client.HIncrBy(RWS.ctx, KeyRWS, KeyLastWalkID, 1).Err(); err != nil {
			return nil, err
		}

		if err := RWS.client.HIncrBy(RWS.ctx, KeyRWS, KeyTotalVisits, int64(len(walk))).Err(); err != nil {
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
		if err := RWS.AddWalks(walks); err != nil {
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
		if err := RWS.AddWalks(walks); err != nil {
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
		if err := RWS.AddWalks(walk); err != nil {
			return nil, err
		}
		return RWS, nil

	default:
		return nil, nil
	}
}
