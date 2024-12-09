// The redisdb package defines a Redis database that fulfills the Database interface in models.
package redisdb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

const (
	KeyDatabase        string = "database"
	KeyLastNodeID      string = "lastNodeID"
	KeyKeyIndex        string = "keyIndex"
	KeyNodePrefix      string = "node:"
	KeyFollowsPrefix   string = "follows:"
	KeyFollowersPrefix string = "followers:"
)

// Database fulfills the Database interface defined in models
type Database struct {
	client *redis.Client
	ctx    context.Context
}

// DatabaseFields are the fields of the Database in Redis. This struct is used for serialize and deserialize.
type DatabaseFields struct {
	LastNodeID int `redis:"lastNodeID"`
	// more fields coming in the future
}

// NewDatabase() creates and returns a new Database instance.
func NewDatabase(ctx context.Context, cl *redis.Client) (*Database, error) {
	if cl == nil {
		return nil, ErrNilClientPointer
	}

	fields := DatabaseFields{
		LastNodeID: -1, // the first ID will be 0, since we increment and return with HIncrBy
	}

	if err := cl.HSet(ctx, KeyDatabase, fields).Err(); err != nil {
		return nil, err
	}

	DB := &Database{
		client: cl,
		ctx:    ctx,
	}
	return DB, nil
}

// Validate() check if DB and client are nil and returns the appropriare error
func (DB *Database) Validate() error {
	if DB == nil {
		return models.ErrNilDBPointer
	}

	if DB.client == nil {
		return models.ErrNilClientPointer
	}

	return nil
}

// NodeByID() retrieves a node by its nodeID.
func (DB *Database) NodeByID(nodeID uint32) (*models.NodeMeta, error) {

	if err := DB.Validate(); err != nil {
		return &models.NodeMeta{}, err
	}

	cmd := DB.client.HGetAll(DB.ctx, KeyNode(nodeID))
	if cmd.Err() != nil {
		return &models.NodeMeta{}, cmd.Err()
	}

	// if an empty map is returned, it means the node was not found
	if len(cmd.Val()) == 0 {
		return &models.NodeMeta{}, redis.Nil
	}

	var node models.NodeMeta
	if err := cmd.Scan(&node); err != nil {
		return &models.NodeMeta{}, err
	}

	return &node, nil
}

// NodeByKey() retrieves a node by its pubkey.
func (DB *Database) NodeByKey(pubkey string) (*models.NodeMeta, error) {

	if err := DB.Validate(); err != nil {
		return &models.NodeMeta{}, err
	}

	// get the nodeID associated with the pubkey
	strNodeID, err := DB.client.HGet(DB.ctx, KeyKeyIndex, pubkey).Result()
	if err != nil {
		return &models.NodeMeta{}, err
	}
	nodeID, err := redisutils.ParseID(strNodeID)
	if err != nil {
		return &models.NodeMeta{}, err
	}

	cmd := DB.client.HGetAll(DB.ctx, KeyNode(nodeID))
	if cmd.Err() != nil {
		return &models.NodeMeta{}, cmd.Err()
	}
	var node models.NodeMeta
	if err := cmd.Scan(&node); err != nil {
		return &models.NodeMeta{}, err
	}

	return &node, nil
}

// AddNode() adds a node to the database and returns its assigned nodeID.
func (DB *Database) AddNode(node *models.Node) (uint32, error) {

	if err := DB.Validate(); err != nil {
		return math.MaxUint32, err
	}

	// check if pubkey already exists in the DB
	exist, err := DB.client.HExists(DB.ctx, KeyKeyIndex, node.Metadata.Pubkey).Result()
	if err != nil {
		return math.MaxUint32, err
	}
	if exist {
		return math.MaxUint32, models.ErrNodeAlreadyInDB
	}

	// get the nodeID outside the transaction. This implies that there might
	// be "holes", meaning IDs not associated with any node
	nodeID, err := DB.client.HIncrBy(DB.ctx, KeyDatabase, KeyLastNodeID, 1).Result()
	if err != nil {
		return math.MaxUint32, err
	}
	node.Metadata.ID = uint32(nodeID)

	// add pubkey to the KeyIndex, and node
	pipe := DB.client.TxPipeline()
	pipe.HSetNX(DB.ctx, KeyKeyIndex, node.Metadata.Pubkey, nodeID)
	pipe.HSet(DB.ctx, KeyNode(nodeID), node.Metadata)

	AddFollows(DB.ctx, pipe, uint32(nodeID), node.Follows)
	AddFollowers(DB.ctx, pipe, uint32(nodeID), node.Followers)

	if _, err := pipe.Exec(DB.ctx); err != nil {
		return math.MaxUint32, err
	}

	return uint32(nodeID), nil
}

// AddFollows() adds the successors of nodeID to the database
func AddFollows(ctx context.Context, pipe redis.Pipeliner, nodeID uint32, addedFollows []uint32) {
	strFollows := redisutils.FormatIDs(addedFollows)
	if len(strFollows) == 0 {
		return
	}

	// add successors to the follows set of nodeID
	pipe.SAdd(ctx, KeyFollows(nodeID), strFollows)

	// add nodeID to the followers of the other nodes
	for _, added := range addedFollows {
		pipe.SAdd(ctx, KeyFollowers(added), nodeID)
	}
}

// RemoveFollows() adds the successors of nodeID to the database
func RemoveFollows(ctx context.Context, pipe redis.Pipeliner, nodeID uint32, removedFollows []uint32) {
	strFollows := redisutils.FormatIDs(removedFollows)
	if len(strFollows) == 0 {
		return
	}

	// remove successors from the follows set of nodeID
	pipe.SRem(ctx, KeyFollows(nodeID), strFollows)

	// remove nodeID from the followers of the removedFollows
	for _, removed := range removedFollows {
		pipe.SRem(ctx, KeyFollowers(removed), nodeID)
	}
}

// AddFollowers() adds the predecessors of nodeID to the database
func AddFollowers(ctx context.Context, pipe redis.Pipeliner, nodeID uint32, addedPred []uint32) {
	strPred := redisutils.FormatIDs(addedPred)
	if len(strPred) == 0 {
		return
	}

	// add predecessors to the followers set of nodeID
	pipe.SAdd(ctx, KeyFollowers(nodeID), strPred)

	// add nodeID to the follows of the other nodes
	for _, added := range addedPred {
		pipe.SAdd(ctx, KeyFollows(added), nodeID)
	}
}

// UpdateNode() updates the nodeID using the new values inside the nodeDiff.
func (DB *Database) UpdateNode(nodeID uint32, nodeDiff *models.NodeDiff) error {

	if err := DB.Validate(); err != nil {
		return err
	}

	// check if nodeID exists
	exists, err := DB.client.Exists(DB.ctx, KeyNode(nodeID)).Result()
	if err != nil {
		return err
	}
	if exists <= 0 {
		return models.ErrNodeNotFoundDB
	}

	// update the node HASH. Only the non empty fields will be updated, thanks to "omitempty"
	pipe := DB.client.TxPipeline()
	pipe.HSet(DB.ctx, KeyNode(nodeID), nodeDiff.Metadata).Err()

	AddFollows(DB.ctx, pipe, nodeID, nodeDiff.AddedFollows)
	RemoveFollows(DB.ctx, pipe, nodeID, nodeDiff.RemovedFollows)

	_, err = pipe.Exec(DB.ctx)
	return err
}

// ContainsNode() returns wheter the DB contains nodeID. In case of errors returns false.
func (DB *Database) ContainsNode(nodeID uint32) bool {

	if err := DB.Validate(); err != nil {
		return false
	}

	exists, err := DB.client.Exists(DB.ctx, KeyNode(nodeID)).Result()
	if err != nil {
		return false
	}

	return exists == 1
}

// Follows() returns a slice that contains the IDs of all successors of a node
func (DB *Database) Follows(nodeID uint32) ([]uint32, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	strFollows, err := DB.client.SMembers(DB.ctx, KeyFollows(nodeID)).Result()
	if err != nil {
		return nil, err
	}

	successors := make([]uint32, 0, len(strFollows))
	for _, ID := range strFollows {
		succ, err := redisutils.ParseID(ID)
		if err != nil {
			return nil, err
		}

		successors = append(successors, succ)
	}
	return successors, nil
}

// NodeIDs() returns a slice of nodeIDs that correspond with the given slice of pubkeys.
// If a pubkey is not found, nil is returned
func (DB *Database) NodeIDs(pubkeys []string) ([]interface{}, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if len(pubkeys) == 0 {
		return []interface{}{}, nil
	}

	nodeIDs, err := DB.client.HMGet(DB.ctx, KeyKeyIndex, pubkeys...).Result()
	if err != nil {
		return nil, err
	}

	for i, strNodeID := range nodeIDs {
		// whatever is not nil, parse it to uint32
		if strNodeID != nil {
			nodeID, err := redisutils.ParseID(strNodeID.(string))
			if err != nil {
				return nil, err
			}
			nodeIDs[i] = nodeID
		}
	}

	return nodeIDs, err
}

// Pubkeys() returns a slice of pubkeys that correspond with the given slice of nodeIDs.
// If a nodeID is not found, nil is returned
func (DB *Database) Pubkeys(nodeIDs []uint32) ([]interface{}, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return []interface{}{}, nil
	}

	pipe := DB.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		cmds[i] = pipe.HGet(DB.ctx, KeyNode(nodeID), models.KeyPubkey)
	}

	// if the error is redis.Nil, deal with it later
	if _, err := pipe.Exec(DB.ctx); err != nil && err != redis.Nil {
		return []interface{}{}, err
	}

	pubkeys := make([]interface{}, 0, len(nodeIDs))
	for _, cmd := range cmds {

		if cmd.Err() == redis.Nil {
			pubkeys = append(pubkeys, nil)
			continue
		}
		pubkeys = append(pubkeys, cmd.Val())
	}

	return pubkeys, nil
}

// ScanNodes() scans over the nodes and returns a batch of nodeIDs of size roughly equal to limit.
// Limit controls how much "work" is invested in fetching the batch, hence it's not precise
// in determining the number of nodes returned.
func (DB *Database) ScanNodes(cursor uint64, limit int) ([]uint32, uint64, error) {

	if err := DB.Validate(); err != nil {
		return []uint32{}, 0, err
	}

	lenPrefix := len(KeyNodePrefix)
	match := KeyNodePrefix + "*"

	strIDs, newCursor, err := DB.client.Scan(DB.ctx, cursor, match, int64(limit)).Result()
	if err != nil {
		return []uint32{}, 0, err
	}

	nodeIDs := make([]uint32, 0, len(strIDs))
	for _, strNodeID := range strIDs {

		nodeID, err := redisutils.ParseID(strNodeID[lenPrefix:])
		if err != nil {
			return []uint32{}, 0, err
		}

		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, newCursor, nil
}

// AllNodes() returns a slice with the IDs of all nodes in the DB
func (DB *Database) AllNodes() ([]uint32, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	strIDs, err := DB.client.HVals(DB.ctx, KeyKeyIndex).Result()
	if err != nil {
		return nil, err
	}

	if len(strIDs) == 0 {
		return nil, models.ErrEmptyDB
	}

	nodeIDs := make([]uint32, 0, len(strIDs))
	for _, ID := range strIDs {

		nodeID, err := redisutils.ParseID(ID)
		if err != nil {
			return nil, err
		}

		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, nil
}

// Size() returns the number of nodes in the DB. In case of errors, it returns 0.
func (DB *Database) Size() int {

	if err := DB.Validate(); err != nil {
		return 0
	}

	size, err := DB.client.HLen(DB.ctx, KeyKeyIndex).Result()
	if err != nil {
		return 0
	}

	return int(size)
}

// SetPagerank() writes the pagerank values on the specified nodeIDs.
// Before writing, it ensures that all keys exists. If that's not the case
// no writes occur and an error is returned.
func (DB *Database) SetPagerank(pagerankMap models.PagerankMap) error {

	if err := DB.Validate(); err != nil {
		return err
	}

	if len(pagerankMap) == 0 {
		return nil
	}

	// prepare keys and values
	keys := make([]string, 0, len(pagerankMap))
	vals := make([]float64, 0, len(pagerankMap))
	for nodeID, rank := range pagerankMap {
		keys = append(keys, KeyNode(nodeID))
		vals = append(vals, rank)
	}

	// validate the existence of all the keys before writing.
	existsPipe := DB.client.Pipeline()
	for _, key := range keys {
		existsPipe.Exists(DB.ctx, key)
	}
	cmds, err := existsPipe.Exec(DB.ctx)
	if err != nil {
		return err
	}

	for i, cmd := range cmds {
		if cmd.(*redis.IntCmd).Val() <= 0 { // Key does not exist
			return fmt.Errorf("%w: %v", models.ErrNodeNotFoundDB, keys[i])
		}
	}

	// write the new pagerank scores
	writePipe := DB.client.TxPipeline()
	for i, val := range vals {
		writePipe.HSet(DB.ctx, keys[i], models.KeyPagerank, val)
	}
	if _, err := writePipe.Exec(DB.ctx); err != nil {
		return err
	}

	return nil
}

// function that returns a DB setup based on the DBType
func SetupDB(cl *redis.Client, DBType string) (*Database, error) {
	ctx := context.Background()
	if cl == nil {
		return nil, ErrNilClientPointer
	}

	switch DBType {
	case "nil":
		return nil, nil

	case "nil-client":
		DB, err := NewDatabase(ctx, cl)
		if err != nil {
			return nil, err
		}

		DB.client = nil
		return DB, nil

	case "empty":
		DB, err := NewDatabase(ctx, cl)
		if err != nil {
			return nil, err
		}
		return DB, nil

	case "one-node0":
		DB, err := NewDatabase(ctx, cl)
		if err != nil {
			return nil, err
		}

		// add node0 to the KeyIndex
		if _, err = cl.HSet(DB.ctx, KeyKeyIndex, "zero", "0").Result(); err != nil {
			return nil, err
		}

		// change the LastNodeID
		if err := cl.HSet(ctx, KeyDatabase, KeyLastNodeID, 0).Err(); err != nil {
			return nil, err
		}

		// add node0  metadata
		fields := models.NodeMeta{
			Pubkey:   "zero",
			EventTS:  1731685733,
			Status:   "idk",
			Pagerank: 1.0,
		}
		if err = cl.HSet(DB.ctx, KeyNode(0), fields).Err(); err != nil {
			return nil, err
		}

		if err := cl.SAdd(ctx, KeyFollows(0), 0).Err(); err != nil {
			return nil, err
		}

		return DB, nil

	case "dandling":
		DB, err := NewDatabase(ctx, cl)
		if err != nil {
			return nil, err
		}

		// add node0 to the KeyIndex
		if _, err = cl.HSet(DB.ctx, KeyKeyIndex, "zero", "0").Result(); err != nil {
			return nil, err
		}

		// change the LastNodeID
		if err := cl.HSet(ctx, KeyDatabase, KeyLastNodeID, 0).Err(); err != nil {
			return nil, err
		}

		// add node0  metadata
		fields := models.NodeMeta{
			Pubkey:   "zero",
			EventTS:  1731685733,
			Status:   "idk",
			Pagerank: 0.0,
		}
		if err = cl.HSet(DB.ctx, KeyNode(0), fields).Err(); err != nil {
			return nil, err
		}
		return DB, nil

	case "fiatjaf":
		const fiatjaf = "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d"

		DB, err := NewDatabase(ctx, cl)
		if err != nil {
			return nil, err
		}

		node := models.Node{
			Metadata: models.NodeMeta{
				Pubkey:   fiatjaf,
				EventTS:  0,
				Status:   models.StatusActive,
				Pagerank: 1.0,
			},
			Follows:   []uint32{},
			Followers: []uint32{},
		}

		if _, err := DB.AddNode(&node); err != nil {
			return nil, err
		}
		return DB, nil

	case "pip":
		const pip = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"

		DB, err := NewDatabase(ctx, cl)
		if err != nil {
			return nil, err
		}

		node := models.Node{
			Metadata: models.NodeMeta{
				Pubkey:   pip,
				EventTS:  0,
				Status:   models.StatusActive,
				Pagerank: 1.0,
			},
			Follows:   []uint32{},
			Followers: []uint32{},
		}

		if _, err := DB.AddNode(&node); err != nil {
			return nil, err
		}
		return DB, nil

	case "fran-pip":
		const pip = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
		const fran = "726a1e261cc6474674e8285e3951b3bb139be9a773d1acf49dc868db861a1c11"
		pks := []string{fran, pip}

		DB, err := NewDatabase(ctx, cl)
		if err != nil {
			return nil, err
		}

		initialPagerank := 1.0 / float64(len(pks))
		for _, pk := range pks {

			node := models.Node{
				Metadata: models.NodeMeta{
					Pubkey:   pk,
					EventTS:  0,
					Status:   models.StatusInactive,
					Pagerank: initialPagerank,
				},
				Follows:   []uint32{},
				Followers: []uint32{},
			}

			if _, err := DB.AddNode(&node); err != nil {
				return nil, err
			}
		}

		return DB, nil

	case "triangle":
		DB, err := NewDatabase(context.Background(), cl)
		if err != nil {
			return nil, err
		}

		nodes := []*models.Node{
			{
				Metadata: models.NodeMeta{
					Pubkey: "zero",
				},
				Follows: []uint32{1},
			},
			{
				Metadata: models.NodeMeta{
					Pubkey: "one",
				},
				Follows: []uint32{2},
			},
			{
				Metadata: models.NodeMeta{
					Pubkey: "two",
				},
				Follows: []uint32{0},
			},
		}

		for _, node := range nodes {
			if _, err := DB.AddNode(node); err != nil {
				return nil, err
			}
		}
		return DB, nil

	default:
		return nil, nil // default to nil
	}
}

// generates a random mock database of a specified number of nodes and successors per node
// the successor of a node won't include itself, and won't have repetitions
func GenerateDB(cl *redis.Client, nodesNum, successorsPerNode int, rng *rand.Rand) (*Database, error) {

	DB, err := NewDatabase(context.Background(), cl)
	if err != nil {
		return nil, err
	}

	if successorsPerNode > nodesNum {
		return nil, fmt.Errorf("successorsPerNode must be lower than nodesNum")
	}

	for i := 0; i < nodesNum; i++ {
		// create random successors for each node
		randomFollows := make([]uint32, 0, successorsPerNode)
		for len(randomFollows) != successorsPerNode {

			succ := uint32(rng.Intn(nodesNum))
			if slices.Contains(randomFollows, succ) {
				continue
			}

			randomFollows = append(randomFollows, succ)
		}

		node := &models.Node{
			Metadata: models.NodeMeta{
				Pubkey:  redisutils.FormatID(uint32(i)),
				EventTS: 0},
			Follows: randomFollows}

		if _, err := DB.AddNode(node); err != nil {
			return nil, err
		}
	}
	return DB, nil
}

// KeyNode() returns the Redis key for the node with specified nodeID
func KeyNode(nodeID interface{}) string {
	return fmt.Sprintf("%v%d", KeyNodePrefix, nodeID)
}

// KeyFollows() returns the Redis key for the follows of the specified nodeID
func KeyFollows(nodeID interface{}) string {
	return fmt.Sprintf("%v%d", KeyFollowsPrefix, nodeID)
}

// KeyFollowers() returns the Redis key for the followers of the specified nodeID
func KeyFollowers(nodeID interface{}) string {
	return fmt.Sprintf("%v%d", KeyFollowersPrefix, nodeID)
}

//---------------------------------ERROR-CODES---------------------------------

var ErrNilClientPointer = errors.New("nil redis client pointer")
