package redisdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	"github.com/redis/go-redis/v9"
)

// Database fulfills the Database interface defined in models
type Database struct {
	client *redis.Client
	ctx    context.Context
}

// NewDatabase() creates and returns a new Database instance.
func NewDatabase(ctx context.Context, cl *redis.Client) (*Database, error) {
	if cl == nil {
		return nil, ErrNilClientPointer
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

	return nil
}

// function that returns a DB setup based on the DBType
func SetupDB(cl *redis.Client, DBType string) (*Database, error) {

	if cl == nil {
		return nil, ErrNilClientPointer
	}

	switch DBType {
	case "nil":
		return nil, nil

	case "empty":
		DB, err := NewDatabase(context.Background(), cl)
		if err != nil {
			return nil, err
		}
		return DB, nil

	default:
		return nil, nil // default to nil
	}
}

//----------------------------------REDIS-KEYS----------------------------------

const KeyNodePrefix string = "node:"
const KeyKeyIndexPrefix string = "pubkeys:"

// KeyWalk() returns the Redis key for the walk with specified walkID
func KeyNode(nodeID uint32) string {
	return fmt.Sprintf("%v%d", KeyNodePrefix, nodeID)
}

//---------------------------------ERROR-CODES---------------------------------

var ErrNilClientPointer = errors.New("nil redis client pointer")
