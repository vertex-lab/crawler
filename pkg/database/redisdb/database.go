// The redisdb package defines a Redis database that fulfills the Database interface in models.
package redisdb

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
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

// validateFields() check if DB and client are nil and returns the appropriare error
func (DB *Database) validateFields() error {

	if DB == nil {
		return models.ErrNilDBPointer
	}

	if DB.client == nil {
		return ErrNilClientPointer
	}

	return nil
}

// Validate() returns an error if the DB is nil or has no nodes
func (DB *Database) Validate() error {

	if err := DB.validateFields(); err != nil {
		return err
	}

	len, err := DB.client.HLen(DB.ctx, KeyKeyIndex).Result()
	if err != nil {
		return err
	}

	if len <= 0 {
		return models.ErrEmptyDB
	}

	return nil
}

// AddNode() adds a node to the database and returns its assigned nodeID.
func (DB *Database) AddNode(node *models.Node) (uint32, error) {

	if err := DB.validateFields(); err != nil {
		return math.MaxUint32, err
	}

	// check if pubkey already exists in the DB
	exist, err := DB.client.HExists(DB.ctx, KeyKeyIndex, node.Metadata.PubKey).Result()
	if err != nil {
		return math.MaxUint32, err
	}
	if exist {
		return math.MaxUint32, models.ErrNodeAlreadyInDB
	}

	// get the nodeID outside the transaction. This implies that there might be "holes",
	// meaning IDs not associated with any node
	nodeID, err := DB.client.HIncrBy(DB.ctx, KeyDatabase, KeyLastNodeID, 1).Result()
	if err != nil {
		return math.MaxUint32, err
	}

	// begin the transaction
	pipe := DB.client.TxPipeline()

	// add pubkey to the KeyIndex
	pipe.HSetNX(DB.ctx, KeyKeyIndex, node.Metadata.PubKey, nodeID)

	// add the node metadata in a node HASH
	pipe.HSet(DB.ctx, KeyNode(nodeID), node.Metadata)

	// add successors and predecessors
	SetSuccessors(DB.ctx, pipe, uint32(nodeID), node.Successors)
	SetPredecessors(DB.ctx, pipe, uint32(nodeID), node.Predecessors)

	// execute the transaction
	if _, err := pipe.Exec(DB.ctx); err != nil {
		return math.MaxUint32, err
	}

	return uint32(nodeID), nil
}

// SetSuccessors() adds the successors of nodeID to the database
func SetSuccessors(ctx context.Context, pipe redis.Pipeliner, nodeID uint32, succ []uint32) {

	if len(succ) == 0 {
		return // early return to avoid errors
	}

	// format successors
	strSucc := make([]string, 0, len(succ))
	for _, s := range succ {
		strSucc = append(strSucc, redisutils.FormatID(s))
	}

	// add successors to the follows set of nodeID
	pipe.SAdd(ctx, KeyFollows(nodeID), strSucc)

	// add nodeID to the followers of the other nodes
	for _, followedNodeID := range succ {
		pipe.SAdd(ctx, KeyFollowers(followedNodeID), nodeID)
	}
}

// SetPredecessors() adds the predecessors of nodeID to the database
func SetPredecessors(ctx context.Context, pipe redis.Pipeliner, nodeID uint32, pred []uint32) {

	if len(pred) == 0 {
		return // early return to avoid errors
	}

	// format predecessors
	strPred := make([]string, 0, len(pred))
	for _, p := range pred {
		strPred = append(strPred, redisutils.FormatID(p))
	}

	// add predecessors to the followers set of nodeID
	pipe.SAdd(ctx, KeyFollowers(nodeID), strPred)

	// add nodeID to the follows of the other nodes
	for _, followersNodeID := range pred {
		pipe.SAdd(ctx, KeyFollows(followersNodeID), nodeID)
	}
}

// UpdateNode() updates the nodeID using the new values inside node.
func (DB *Database) UpdateNode(nodeID uint32, node *models.Node) error {

	if err := DB.validateFields(); err != nil {
		return err
	}

	// check if the nodeID exists
	exists, err := DB.client.Exists(DB.ctx, KeyNode(nodeID)).Result()
	if err != nil {
		return err
	}
	if exists <= 0 {
		return models.ErrNodeNotFoundDB
	}

	// begin the transaction
	pipe := DB.client.TxPipeline()

	// update the node HASH
	pipe.HSet(DB.ctx, KeyNode(nodeID), node.Metadata).Err()

	// update successors and predecessors
	SetSuccessors(DB.ctx, pipe, nodeID, node.Successors)
	SetPredecessors(DB.ctx, pipe, nodeID, node.Predecessors)

	// execute the transaction
	_, err = pipe.Exec(DB.ctx)
	return err
}

// ContainsNode() returns wheter a specified nodeID is found in the DB. In case of
// errors, it returns the default false.
func (DB *Database) ContainsNode(nodeID uint32) bool {

	if err := DB.validateFields(); err != nil {
		return false
	}

	exists, err := DB.client.Exists(DB.ctx, KeyNode(nodeID)).Result()
	if err != nil {
		return false
	}

	return exists == 1
}

// IsDandling() returns whether a node has any successor. In case of errors,
// it returns the default false.
func (DB *Database) IsDandling(nodeID uint32) bool {

	if err := DB.validateFields(); err != nil {
		return false
	}

	exists, err := DB.client.Exists(DB.ctx, KeyNode(nodeID)).Result()
	if err != nil || exists <= 0 {
		return false
	}

	card, err := DB.client.SCard(DB.ctx, KeyFollows(nodeID)).Result()
	if err != nil {
		return false
	}

	return card == 0
}

// Successors() returns a slice that contains the IDs of all successors of a node
func (DB *Database) Successors(nodeID uint32) ([]uint32, error) {

	if err := DB.validateFields(); err != nil {
		return nil, err
	}

	strSucc, err := DB.client.SMembers(DB.ctx, KeyFollows(nodeID)).Result()
	if err != nil {
		return nil, err
	}

	successors := make([]uint32, 0, len(strSucc))
	for _, ID := range strSucc {
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

	if err := DB.validateFields(); err != nil {
		return nil, err
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

// AllNodes() returns a slice with the IDs of all nodes in the DB
func (DB *Database) AllNodes() ([]uint32, error) {

	if err := DB.validateFields(); err != nil {
		return nil, err
	}

	strNodeIDs, err := DB.client.HVals(DB.ctx, KeyKeyIndex).Result()
	if err != nil {
		return nil, err
	}

	if len(strNodeIDs) == 0 {
		return nil, models.ErrEmptyDB
	}

	nodeIDs := make([]uint32, 0, len(strNodeIDs))
	for _, ID := range strNodeIDs {

		nodeID, err := redisutils.ParseID(ID)
		if err != nil {
			return nil, err
		}

		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, nil
}

// Size() returns the number of nodes in the DB. In case of errors, it returns
// the default value of 0.
func (DB *Database) Size() int {

	if err := DB.validateFields(); err != nil {
		return 0
	}

	size, err := DB.client.HLen(DB.ctx, KeyKeyIndex).Result()
	if err != nil {
		return 0
	}

	return int(size)
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
			PubKey:    "zero",
			Timestamp: 1731685733,
			Status:    "idk",
			Pagerank:  0.0,
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
			PubKey:    "zero",
			Timestamp: 1731685733,
			Status:    "idk",
			Pagerank:  0.0,
		}
		if err = cl.HSet(DB.ctx, KeyNode(0), fields).Err(); err != nil {
			return nil, err
		}

		return DB, nil

	default:
		return nil, nil // default to nil
	}
}

//----------------------------------REDIS-KEYS----------------------------------

const KeyDatabase string = "database"
const KeyLastNodeID string = "lastNodeID"
const KeyKeyIndex string = "keyIndex"
const KeyNodePrefix string = "node:"
const KeyFollowsPrefix string = "follows:"
const KeyFollowersPrefix string = "followers:"

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
