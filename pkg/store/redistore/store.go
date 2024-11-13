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

// IsEmpty() returns false if "walk:0" is found in Redis, otherwise true.
func (RWS *RandomWalkStore) IsEmpty() bool {
	if RWS == nil {
		return true
	}

	_, err := RWS.client.Get(RWS.ctx, KeyWalk(0)).Result()
	return err != nil
}

// ContainsNode() returns whether RWS contains a given nodeID, meaning if
// nodeWalkIDs:nodeID exist in Redis. It ignores errors intentionally,
// returning false in case of any Redis or context issues.
func (RWS *RandomWalkStore) ContainsNode(nodeID uint32) bool {

	if RWS == nil || RWS.client == nil {
		return false
	}

	exist, err := RWS.client.Exists(RWS.ctx, KeyNodeWalkIDs(nodeID)).Result()
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

// VisitCount() returns the number of times nodeID has been visited by a walk.
// intentionally, returning 0 in case of any Redis or context issues.
func (RWS *RandomWalkStore) VisitCount(nodeID uint32) int {

	if RWS == nil || RWS.client == nil {
		return 0
	}

	visits, err := RWS.client.SCard(RWS.ctx, KeyNodeWalkIDs(nodeID)).Result()
	if err != nil {
		return 0
	}

	return int(visits)
}

// Walks() returns a map of walks by walksID that visit nodeID.
func (RWS *RandomWalkStore) Walks(nodeID uint32) (map[uint32]models.RandomWalk, error) {

	if err := RWS.Validate(false); err != nil {
		return nil, err
	}

	luaScript := `
        local nodeID = KEYS[1]
        local KeyWalkPrefix = ARGV[1]

        -- Get all walkIDs for the node
        local walkIDs = redis.call("SMEMBERS", nodeID)
        local walks = {}

        -- For each walkID, retrieve the corresponding walk
        for _, walkID in ipairs(walkIDs) do
            local walk = redis.call("GET", KeyWalkPrefix .. walkID)
            table.insert(walks, walk)
        end

        -- Return both walkIDs and walks as two parallel arrays
        return {walkIDs, walks}
    `
	// execute the Lua script
	keys := []string{KeyNodeWalkIDs(nodeID)}
	result, err := RWS.client.Eval(RWS.ctx, luaScript, keys, KeyWalkPrefix).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to execute Lua script: %v", err)
	}

	return ParseWalkMap(result)
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

	// add the walk to the WalkIndex
	pipe.Set(RWS.ctx, KeyWalk(uint32(walkID)), FormatWalk(walk), 0)

	// add the walkID to each node
	for _, nodeID := range walk {
		pipe.SAdd(RWS.ctx, KeyNodeWalkIDs(nodeID), walkID)
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
	strWalk, err := RWS.client.Get(RWS.ctx, KeyWalk(walkID)).Result()
	if err != nil {
		return err
	}
	walk, err := ParseWalk(strWalk)
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
		pipe.SRem(RWS.ctx, KeyNodeWalkIDs(prunedNodeID), walkID)
	}

	// add the walkID to each grafted node
	for _, graftedNodeID := range walkSegment {
		pipe.SAdd(RWS.ctx, KeyNodeWalkIDs(graftedNodeID), walkID)
	}

	// prune and graft operation on the walk
	newWalk := append(walk[:cutIndex], walkSegment...)
	pipe.Set(RWS.ctx, KeyWalk(walkID), FormatWalk(newWalk), 0)

	// Execute the transaction
	if _, err = pipe.Exec(RWS.ctx); err != nil {
		return fmt.Errorf("PruneGraftWalk(%v) failed to execute: %v", walk, err)
	}

	return nil
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

		local KeyWalkPrefix = ARGV[1]
		
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
		local candidateWalkIDs = {}
		for _, walkID in ipairs(nodeWalkIDs) do
			if unionRemovedNodesWalkIDs[walkID] then
				table.insert(candidateWalkIDs, walkID)
			end
		end
		
		-- Fetch the walk for each candidate walkID
		local candidateWalks = {}
		for _, walkID in ipairs(candidateWalkIDs) do
			local walk = redis.call("GET", KeyWalkPrefix .. walkID)
			table.insert(candidateWalks, walk)
		end
		
		-- Return both candidateWalkIDs and candidateWalks as two parallel arrays
		return {candidateWalkIDs, candidateWalks}
	`

	// format the keys
	keys := []string{KeyNodeWalkIDs(nodeID)}
	for _, removedNode := range removedNodes {
		keys = append(keys, KeyNodeWalkIDs(removedNode))
	}

	// execute the Lua script
	result, err := RWS.client.Eval(RWS.ctx, luaScript, keys, KeyWalkPrefix).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to execute Lua script: %v", err)
	}

	return ParseWalkMap(result)
}

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
		local KeyWalkPrefix = ARGV[1]
		local cardinality = redis.call("SCARD", nodeID)
		local probability = tonumber(ARGV[2])

		-- Round up to the nearest integer
		local expectedSize = math.floor(cardinality * probability + 0.5) 
		
		-- Get random members from the set based on the expected size
		local walkIDs = redis.call("SRANDMEMBER", nodeID, expectedSize)
		
		local walks = {}
		
		-- For each walkID, retrieve the corresponding walk
		for _, walkID in ipairs(walkIDs) do
			local walk = redis.call("GET", KeyWalkPrefix .. walkID)
			table.insert(walks, walk)
		end
		
		-- Return both walkIDs and walks as two parallel arrays
		return {walkIDs, walks}
    `
	// execute the Lua script
	keys := []string{KeyNodeWalkIDs(nodeID)}
	args := []interface{}{KeyWalkPrefix, probabilityOfSelection}
	result, err := RWS.client.Eval(RWS.ctx, luaScript, keys, args...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to execute Lua script: %v", err)
	}

	return ParseWalkMap(result)
}
