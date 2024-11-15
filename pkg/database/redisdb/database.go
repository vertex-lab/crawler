package redisdb

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	"github.com/redis/go-redis/v9"
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

// Validate() returns an error if the DB is nil or has no nodes
func (DB *Database) Validate() error {

	if DB == nil {
		return models.ErrNilDBPointer
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

// AddNode() adds a node to the database and returns its assigned nodeID
func (DB *Database) AddNode(node *models.Node) (uint32, error) {
	if DB == nil {
		return math.MaxUint32, models.ErrNilDBPointer
	}

	if DB.client == nil {
		return math.MaxUint32, ErrNilClientPointer
	}

	// check if pubkey already exists in the DB
	exist, err := DB.client.HExists(DB.ctx, KeyKeyIndex, node.PubKey).Result()
	if err != nil {
		return math.MaxUint32, err
	}
	if exist {
		return math.MaxUint32, models.ErrNodeAlreadyInDB
	}

	// get the next nodeID
	nextNodeID, err := DB.client.HIncrBy(DB.ctx, KeyDatabase, KeyLastNodeID, 1).Result()
	if err != nil {
		return math.MaxUint32, err
	}

	// begin the transaction
	pipe := DB.client.TxPipeline()

	// add pubkey to the KeyIndex
	pipe.HSetNX(DB.ctx, KeyKeyIndex, node.PubKey, nextNodeID)

	// add the node data in a new hash
	pipe.HSet(DB.ctx, KeyNode(uint32(nextNodeID)), node)

	if _, err := pipe.Exec(DB.ctx); err != nil {
		return math.MaxUint32, err
	}

	return uint32(nextNodeID), nil
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

		// add node0 related data
		fields := models.Node{
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

// KeyWalk() returns the Redis key for the walk with specified walkID
func KeyNode(nodeID uint32) string {
	return fmt.Sprintf("%v%d", KeyNodePrefix, nodeID)
}

//---------------------------------ERROR-CODES---------------------------------

var ErrNilClientPointer = errors.New("nil redis client pointer")
