// The redistore package defines a Redis store that fulfills the RandomWalkStore
// interface in models.
package redistore

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

// RandomWalkStore fulfills the RandomWalkStore interface defined in models
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
		return nil, ErrNilClientPointer
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

// WalkPerNode() returns the number of walks to be generated for each node in the DB
func (RWS *RandomWalkStore) WalksPerNode() uint16 {
	return RWS.walksPerNode
}

// IsEmpty() returns false if the size of the hash key "walks" is > 0, otherwise true.
func (RWS *RandomWalkStore) IsEmpty() bool {
	if RWS == nil {
		return true
	}

	len, err := RWS.client.HLen(RWS.ctx, KeyWalks).Result()
	return err != nil || len == 0
}

// ContainsNode() returns whether RWS contains a given nodeID, meaning if
// nodeWalkIDs:nodeID exist in Redis. It ignores errors intentionally,
// returning false in case of any Redis or context issues.
func (RWS *RandomWalkStore) ContainsNode(nodeID uint32) bool {

	if RWS == nil || RWS.client == nil {
		return false
	}

	exist, err := RWS.client.Exists(RWS.ctx, KeyWalksVisiting(nodeID)).Result()
	if err != nil {
		return false
	}

	return exist > 0
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
		return ErrNilClientPointer
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

	if err := RWS.Validate(false); err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 1000000000 // a very large number, such that SRANDMEMBER returns everything
	}

	luaScript := `
        local nodeID = KEYS[1]
		local limit = ARGV[1]
        local KeyWalks = ARGV[2]

        -- Get some walkIDs from the walks visiting nodeID
        local walkIDs = redis.call("SRANDMEMBER", nodeID, limit)
        
		if not walkIDs or #walkIDs == 0 then
			return {{}, {}}
		end
		
		-- Get the walks associated with the walkIDs
		local walks = redis.call("HMGET", KeyWalks, unpack(walkIDs))

        -- Return both walkIDs and walks as two parallel arrays
        return {walkIDs, walks}
    `
	// execute the Lua script
	keys := []string{KeyWalksVisiting(nodeID)}
	result, err := RWS.client.Eval(RWS.ctx, luaScript, keys, limit, KeyWalks).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to execute Lua script: %v", err)
	}

	return redisutils.ParseWalkMap(result)
}

/*
WalksRand() returns a map of walks by walkID chosen at random from the walks
that visit nodeID, according to a specified probability of selection.
*/
/*
WalksRand() returns a map of walks by walkID chosen at random from the walks
that visit nodeID, according to a specified probability of selection.
*/
func (RWS *RandomWalkStore) WalksRand(nodeID uint32,
	probabilityOfSelection float32) (map[uint32]models.RandomWalk, error) {

	if err := RWS.validateFields(); err != nil {
		return nil, err
	}

	luaScript := `
		local nodeID = KEYS[1]
		local probability = tonumber(ARGV[1])
		local KeyWalks = ARGV[2]
		local cardinality = redis.call("SCARD", nodeID)

		-- Round up to the nearest integer
		local expectedSize = math.floor(cardinality * probability + 0.5) 
		
		-- Get some walkIDs from the walks visiting nodeID
		local walkIDs = redis.call("SRANDMEMBER", nodeID, expectedSize)
		
		if not walkIDs or #walkIDs == 0 then
			return {{}, {}}
		end
		
		-- Get the walks associated with the walkIDs
		local walks = redis.call("HMGET", KeyWalks, unpack(walkIDs))

        -- Return both walkIDs and walks as two parallel arrays
        return {walkIDs, walks}
    `
	// execute the Lua script
	keys := []string{KeyWalksVisiting(nodeID)}
	args := []interface{}{probabilityOfSelection, KeyWalks}
	result, err := RWS.client.Eval(RWS.ctx, luaScript, keys, args...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to execute Lua script: %v", err)
	}

	return redisutils.ParseWalkMap(result)
}

// CommonWalks returns a map of walks by walkID that contain both nodeID and at
// least one of the removedNode in removedNodes.
func (RWS *RandomWalkStore) CommonWalks(nodeID uint32,
	removedNodes []uint32) (map[uint32]models.RandomWalk, error) {

	if err := RWS.validateFields(); err != nil {
		return nil, err
	}

	luaScript := `
		local nodeID = KEYS[1]
		local removedNodes = {}		
		for i = 2, #KEYS do
			table.insert(removedNodes, KEYS[i])
		end

		local KeyWalks = ARGV[1]
		
		-- Get walk IDs associated with the main nodeID
		local nodeWalkIDs = redis.call("SMEMBERS", nodeID)
		
		-- Build a union set of walk IDs for all removed nodes
		local unionRemovedNodesWalkIDs = {}
		for _, removedNode in ipairs(removedNodes) do
			local removedNodeWalkIDs = redis.call("SMEMBERS", removedNode)
			for _, id in ipairs(removedNodeWalkIDs) do
				unionRemovedNodesWalkIDs[id] = true  -- Using a table as a set for union
			end
		end
		
		-- Find the intersection of nodeWalkIDs and unionRemovedNodesWalkIDs
		local walkIDs = {}
		for _, walkID in ipairs(nodeWalkIDs) do
			if unionRemovedNodesWalkIDs[walkID] then
				table.insert(walkIDs, walkID)
			end
		end

		if not walkIDs or #walkIDs == 0 then
			return {{}, {}}
		end
		
		-- Get the walks associated with the walkIDs
		local walks = redis.call("HMGET", KeyWalks, unpack(walkIDs))

		-- Return both walkIDs and walks as two parallel arrays
		return {walkIDs, walks}
	`

	// format the keys
	keys := []string{KeyWalksVisiting(nodeID)}
	for _, removedNode := range removedNodes {
		keys = append(keys, KeyWalksVisiting(removedNode))
	}

	// execute the Lua script
	result, err := RWS.client.Eval(RWS.ctx, luaScript, keys, KeyWalks).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to execute Lua script: %v", err)
	}

	return redisutils.ParseWalkMap(result)
}

// AddWalk() adds the walk to the WalkIndex. It also adds the walkID to the
// WalkIDSet of each node the walk visited. This means that for each node
// visited by the walk, the walkID will be added to its WalkSet.
func (RWS *RandomWalkStore) AddWalk(walk models.RandomWalk) error {

	if err := RWS.validateFields(); err != nil {
		return err
	}

	if err := models.Validate(walk); err != nil {
		return err
	}

	// assign a new walkID to this walk. It's not possible to do it inside the tx,
	// which means there might be "holes" in the walkIndex, meaning walkIDs NOT associated to any walk.
	walkID, err := RWS.client.HIncrBy(RWS.ctx, KeyRWS, KeyLastWalkID, 1).Result()
	if err != nil {
		return err
	}

	// begin the transaction
	pipe := RWS.client.TxPipeline()

	// add the walk to the walks HASH
	pipe.HSet(RWS.ctx, KeyWalks, redisutils.FormatID(uint32(walkID)), redisutils.FormatWalk(walk))

	// add the walkID to each node
	for _, nodeID := range walk {
		pipe.SAdd(RWS.ctx, KeyWalksVisiting(nodeID), walkID)
	}

	// execute the transaction
	if _, err = pipe.Exec(RWS.ctx); err != nil {
		return fmt.Errorf("AddWalk(%v) failed to execute: %v", walk, err)
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

	// Begins the transaction
	pipe := RWS.client.TxPipeline()

	// remove the walkID form each pruned node
	for _, prunedNodeID := range walk[cutIndex:] {
		pipe.SRem(RWS.ctx, KeyWalksVisiting(prunedNodeID), walkID)
	}

	// add the walkID to each grafted node
	for _, graftedNodeID := range walkSegment {
		pipe.SAdd(RWS.ctx, KeyWalksVisiting(graftedNodeID), walkID)
	}

	// prune and graft operation on the walk
	newWalk := append(walk[:cutIndex], walkSegment...)
	pipe.HSet(RWS.ctx, KeyWalks, walkIDKey, redisutils.FormatWalk(newWalk))

	// Execute the transaction
	if _, err = pipe.Exec(RWS.ctx); err != nil {
		return fmt.Errorf("PruneGraftWalk(%v) failed to execute: %v", walk, err)
	}

	return nil
}

// SetupRWS returns a RandomWalkStore ready to be used in tests
func SetupRWS(cl *redis.Client, RWSType string) (*RandomWalkStore, error) {
	if cl == nil {
		return nil, ErrNilClientPointer
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

		return RWS, nil

	case "triangle":
		// 0 --> 1 --> 2 --> 0
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walks := []models.RandomWalk{{0, 1, 2}, {1, 2, 0}, {2, 0, 1}}
		for _, walk := range walks {
			if err := RWS.AddWalk(walk); err != nil {
				return nil, err
			}
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
		for _, walk := range walks {
			if err := RWS.AddWalk(walk); err != nil {
				return nil, err
			}
		}
		return RWS, nil

	case "pip":
		ctx := context.Background()
		RWS, err := NewRWS(ctx, cl, 0.85, 1)
		if err != nil {
			return nil, err
		}

		walk := models.RandomWalk{0}
		if err := RWS.AddWalk(walk); err != nil {
			return nil, err
		}
		return RWS, nil

	default:
		return nil, nil
	}
}

//----------------------------------REDIS-KEYS----------------------------------

const KeyRWS string = "RWS"
const KeyAlpha string = "alpha"
const KeyWalksPerNode string = "walksPerNode"
const KeyLastWalkID string = "lastWalkID"
const KeyWalks string = "walks"
const KeyWalksVisitingPrefix string = "walksVisiting:"

// // KeyWalk() returns the Redis key for the walk with specified walkID
// func KeyWalk(walkID uint32) string {
// 	return fmt.Sprintf("%v%d", KeyWalks, walkID)
// }

// KeyWalksVisiting() returns the Redis key for the nodeWalkIDs with specified nodeID
func KeyWalksVisiting(nodeID uint32) string {
	return fmt.Sprintf("%v%d", KeyWalksVisitingPrefix, nodeID)
}

//---------------------------------ERROR-CODES---------------------------------

var ErrNilClientPointer = errors.New("nil redis client pointer")
