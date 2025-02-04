// The redisdb package defines a Redis database that fulfills the Database interface in models.
package redisdb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
)

const (
	// redis variable names
	KeyDatabase        string = "database"
	KeyLastNodeID      string = "lastNodeID"
	KeyKeyIndex        string = "keyIndex"
	KeyNodePrefix      string = "node:"
	KeyFollowsPrefix   string = "follows:"
	KeyFollowersPrefix string = "followers:"

	// redis node HASH fields
	NodeID            string = "id"
	NodePubkey        string = "pubkey"
	NodeStatus        string = "status"
	NodeFollowEventID string = "follows_event_ID"
	NodeFollowEventTS string = "follows_event_TS"
	NodePromotionTS   string = "promotion_TS"
	NodeDemotionTS    string = "demotion_TS"
	NodeAddedTS       string = "added_TS"
)

// Database fulfills the Database interface defined in models
type Database struct {
	client *redis.Client
}

// DatabaseFields are the fields of the Database in Redis. This struct is used for serialize and deserialize.
type DatabaseFields struct {
	LastNodeID int `redis:"lastNodeID"`
	// more fields coming in the future
}

// NewDatabaseConnection() returns an initialized Database
func NewDatabaseConnection(ctx context.Context, cl *redis.Client) (*Database, error) {
	if cl == nil {
		return nil, ErrNilClient
	}
	return &Database{client: cl}, nil
}

// NewDatabase() creates and returns a new Database instance.
func NewDatabase(ctx context.Context, cl *redis.Client) (*Database, error) {
	if cl == nil {
		return nil, ErrNilClient
	}

	fields := DatabaseFields{
		LastNodeID: -1, // the first ID will be 0, since we increment and return with HIncrBy
	}

	if err := cl.HSet(ctx, KeyDatabase, fields).Err(); err != nil {
		return nil, err
	}

	return &Database{client: cl}, nil
}

// Validate() check if DB and client are nil and returns the appropriare error
func (DB *Database) Validate() error {
	if DB == nil {
		return models.ErrNilDB
	}

	if DB.client == nil {
		return ErrNilClient
	}

	return nil
}

// ParseNode() parses the map into a node structure, with only one Record of type Follow.
func ParseNode(nodeMap map[string]string) (*models.Node, error) {
	if len(nodeMap) == 0 {
		return nil, nil
	}

	node := models.Node{}
	FollowRecord := models.Record{Type: models.Follow}

	for key, val := range nodeMap {
		switch key {
		case NodeID:
			ID, err := redisutils.ParseID(val)
			if err != nil {
				return nil, err
			}
			node.ID = ID

		case NodePubkey:
			node.Pubkey = val

		case NodeStatus:
			node.Status = val

		case NodeFollowEventID:
			FollowRecord.ID = val

		case NodeFollowEventTS:
			ts, err := redisutils.ParseInt64(val)
			if err != nil {
				return nil, err
			}
			FollowRecord.Timestamp = ts
		}
	}

	if FollowRecord.Timestamp > 0 {
		node.Records = []models.Record{FollowRecord}
	}

	return &node, nil
}

// NodeByID() retrieves a node by its nodeID.
func (DB *Database) NodeByID(ctx context.Context, nodeID uint32) (*models.Node, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	nodeMap, err := DB.client.HGetAll(ctx, KeyNode(nodeID)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %v: %w", KeyNode(nodeID), err)
	}

	if len(nodeMap) == 0 {
		return nil, fmt.Errorf("%w with ID %d", models.ErrNodeNotFoundDB, nodeID)
	}

	return ParseNode(nodeMap)
}

// NodeByKey() retrieves a node by its pubkey.
func (DB *Database) NodeByKey(ctx context.Context, pubkey string) (*models.Node, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	// get the nodeID associated with the pubkey
	strID, err := DB.client.HGet(ctx, KeyKeyIndex, pubkey).Result()
	if errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("%w with pubkey %s", models.ErrNodeNotFoundDB, pubkey)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to fetch node with pubkey %s: %w", pubkey, err)
	}

	nodeMap, err := DB.client.HGetAll(ctx, KeyNodePrefix+strID).Result()
	if err != nil || len(nodeMap) == 0 {
		return nil, fmt.Errorf("failed to fetch node with pubkey %s: %w", pubkey, err)
	}

	return ParseNode(nodeMap)
}

// AddNode() adds a node to the database and returns its assigned nodeID.
func (DB *Database) AddNode(ctx context.Context, pubkey string) (uint32, error) {

	if err := DB.Validate(); err != nil {
		return math.MaxUint32, err
	}

	// check if pubkey already exists in the DB
	exist, err := DB.client.HExists(ctx, KeyKeyIndex, pubkey).Result()
	if err != nil {
		return math.MaxUint32, fmt.Errorf("failed to check for existance of pubkey %v: %w", pubkey, err)
	}
	if exist {
		return math.MaxUint32, fmt.Errorf("%w with pubkey %v", models.ErrNodeAlreadyInDB, pubkey)
	}

	// get the nodeID outside the transaction. This implies that there might
	// be "holes", meaning IDs not associated with any node
	nodeID, err := DB.client.HIncrBy(ctx, KeyDatabase, KeyLastNodeID, 1).Result()
	if err != nil {
		return math.MaxUint32, err
	}

	// add pubkey to the KeyIndex, and node
	pipe := DB.client.TxPipeline()
	pipe.HSetNX(ctx, KeyKeyIndex, pubkey, nodeID)
	pipe.HSet(ctx, KeyNode(nodeID), NodeID, nodeID, NodePubkey, pubkey, NodeStatus, models.StatusInactive, NodeAddedTS, time.Now().Unix())

	if _, err := pipe.Exec(ctx); err != nil {
		return math.MaxUint32, fmt.Errorf("failed to add %v: %w", pubkey, err)
	}

	return uint32(nodeID), nil
}

// UpdateNode() updates the nodeID using the new values inside the nodeDiff.
func (DB *Database) Update(ctx context.Context, delta *models.Delta) error {
	if err := DB.Validate(); err != nil {
		return err
	}

	if delta == nil {
		return models.ErrNilDelta
	}

	// check if nodeID exists
	exists, err := DB.client.Exists(ctx, KeyNode(delta.NodeID)).Result()
	if err != nil {
		return fmt.Errorf("failed to check for the existance of nodeID %v: %w", delta.NodeID, err)
	}
	if exists <= 0 {
		return fmt.Errorf("%w with ID %d", models.ErrNodeNotFoundDB, delta.NodeID)
	}

	switch delta.Type {
	case models.Promotion, models.Demotion:
		err = DB.updateStatus(ctx, delta.NodeID, delta.Record)

	case models.Follow:
		err = DB.updateFollows(ctx, delta)
	}

	if err != nil {
		return fmt.Errorf("failed to update with delta %v: %w", delta, err)
	}

	return nil
}

// updateStatus updates the status of nodeID
func (DB *Database) updateStatus(ctx context.Context, nodeID uint32, record models.Record) error {
	switch record.Type {
	case models.Promotion:
		return DB.client.HSet(ctx, KeyNode(nodeID), NodeStatus, models.StatusActive, NodePromotionTS, record.Timestamp).Err()

	case models.Demotion:
		return DB.client.HSet(ctx, KeyNode(nodeID), NodeStatus, models.StatusInactive, NodeDemotionTS, record.Timestamp).Err()

	default:
		return fmt.Errorf("invalid record type: %v", record.Type)
	}
}

// updateFollows adds and removed follow relationships
func (DB *Database) updateFollows(ctx context.Context, delta *models.Delta) error {
	pipe := DB.client.TxPipeline()

	if len(delta.Added) > 0 {
		// add all to the follows of nodeID
		pipe.SAdd(ctx, KeyFollows(delta.NodeID), redisutils.FormatIDs(delta.Added))

		// add nodeID to the followers of all
		for _, ID := range delta.Added {
			pipe.SAdd(ctx, KeyFollowers(ID), delta.NodeID)
		}
	}

	if len(delta.Removed) > 0 {
		// remove all from the follows of nodeID
		pipe.SRem(ctx, KeyFollows(delta.NodeID), redisutils.FormatIDs(delta.Removed))

		// remove nodeID from the followers of all
		for _, ID := range delta.Removed {
			pipe.SRem(ctx, KeyFollowers(ID), delta.NodeID)
		}
	}

	// updating FollowRecord
	pipe.HSet(ctx, KeyNode(delta.NodeID), NodeFollowEventID, delta.ID, NodeFollowEventTS, delta.Timestamp)
	_, err := pipe.Exec(ctx)
	return err
}

// ContainsNode() returns wheter the DB contains nodeID. In case of errors returns false.
func (DB *Database) ContainsNode(ctx context.Context, nodeID uint32) bool {
	if err := DB.Validate(); err != nil {
		return false
	}

	exists, err := DB.client.Exists(ctx, KeyNode(nodeID)).Result()
	if err != nil {
		return false
	}

	return exists == 1
}

// Followers() returns a slice containing the follows of each of the specified nodeIDs.
func (DB *Database) Followers(ctx context.Context, nodeIDs ...uint32) ([][]uint32, error) {
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	return pipelineSMembers(ctx, DB, KeyFollowers, nodeIDs...)
}

// Follows() returns a slice containing the follows of each of the specified nodeIDs.
func (DB *Database) Follows(ctx context.Context, nodeIDs ...uint32) ([][]uint32, error) {
	if err := DB.Validate(); err != nil {
		return nil, err
	}

	return pipelineSMembers(ctx, DB, KeyFollows, nodeIDs...)
}

// The method pipelineSMembers() fetches the SMembers of the specified keys, which are:
// KeyFunc(nodeID). If some commands return empty arrays, it checks the existence
// of KeyNode(nodeID) and returns an error if a node was not found.
func pipelineSMembers[ID uint32 | int64 | int](
	ctx context.Context,
	DB *Database,
	KeyFunc func(ID) string,
	nodeIDs ...ID) ([][]uint32, error) {

	pipe := DB.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(nodeIDs))
	for i, ID := range nodeIDs {
		cmds[i] = pipe.SMembers(ctx, KeyFunc(ID))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	var potentialMissing []string
	members := make([][]uint32, 0, len(nodeIDs))
	for i, cmd := range cmds {

		strMembers := cmd.Val()
		if len(strMembers) == 0 { // empty slice might mean node not found.
			potentialMissing = append(potentialMissing, KeyNode(nodeIDs[i]))
			members = append(members, []uint32{})
			continue
		}

		m, err := redisutils.ParseIDs(strMembers)
		if err != nil {
			return nil, err
		}

		members = append(members, m)
	}

	// check if some of the dandling nodes where in reality not found in the DB
	if len(potentialMissing) > 0 {
		countExists, err := DB.client.Exists(ctx, potentialMissing...).Result()
		if err != nil {
			return nil, err
		}

		if int(countExists) < len(potentialMissing) {
			return nil, fmt.Errorf("%w: some of these nodeIDs :%v", models.ErrNodeNotFoundDB, potentialMissing)
		}
	}

	return members, nil
}

// NodeIDs() returns a slice of nodeIDs that correspond with the given slice of pubkeys.
// If a pubkey is not found, nil is returned
func (DB *Database) NodeIDs(ctx context.Context, pubkeys ...string) ([]*uint32, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if len(pubkeys) == 0 {
		return nil, nil
	}

	IDs, err := DB.client.HMGet(ctx, KeyKeyIndex, pubkeys...).Result()
	if err != nil {
		return nil, err
	}

	// parse to *uint32, unless it's nil (which means node not found).
	nodeIDs := make([]*uint32, len(IDs))
	for i, ID := range IDs {
		if ID == nil {
			nodeIDs[i] = nil
			continue
		}

		nodeID, err := redisutils.ParseID(ID.(string))
		if err != nil {
			return nil, err
		}
		nodeIDs[i] = &nodeID
	}

	return nodeIDs, err
}

// Pubkeys() returns a slice of pubkeys that correspond with the given slice of nodeIDs.
// If a nodeID is not found, nil is returned
func (DB *Database) Pubkeys(ctx context.Context, nodeIDs ...uint32) ([]*string, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	pipe := DB.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		cmds[i] = pipe.HGet(ctx, KeyNode(nodeID), NodePubkey)
	}

	// if the error is redis.Nil, deal with it later
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	pubkeys := make([]*string, len(nodeIDs))
	for i, cmd := range cmds {
		// add nil where the key was not found
		if cmd.Err() == redis.Nil {
			pubkeys[i] = nil
			continue
		}

		key := cmd.Val()
		pubkeys[i] = &key
	}

	return pubkeys, nil
}

// ScanNodes() scans over the nodes and returns a batch of nodeIDs of size roughly equal to limit.
// Limit controls how much "work" is invested in fetching the batch, hence it's not precise
// in determining the number of nodes returned.
func (DB *Database) ScanNodes(ctx context.Context, cursor uint64, limit int) ([]uint32, uint64, error) {

	if err := DB.Validate(); err != nil {
		return []uint32{}, 0, err
	}

	lenPrefix := len(KeyNodePrefix)
	match := KeyNodePrefix + "*"

	strIDs, newCursor, err := DB.client.Scan(ctx, cursor, match, int64(limit)).Result()
	if err != nil {
		return []uint32{}, 0, err
	}

	nodeIDs := make([]uint32, 0, len(strIDs))
	for _, ID := range strIDs {

		nodeID, err := redisutils.ParseID(ID[lenPrefix:])
		if err != nil {
			return []uint32{}, 0, err
		}

		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, newCursor, nil
}

// AllNodes() returns a slice with the IDs of all nodes in the DB
func (DB *Database) AllNodes(ctx context.Context) ([]uint32, error) {

	if err := DB.Validate(); err != nil {
		return nil, err
	}

	size := DB.Size(ctx)
	if size == 0 {
		return nil, fmt.Errorf("AllNodes(): %w", models.ErrEmptyDB)
	}

	nodeIDs := make([]uint32, 0, DB.Size(ctx))
	var cursor uint64
	var IDs []uint32
	var err error

	for {
		select {
		case <-ctx.Done():
			return nil, nil
		default:
			// proceed with the scan
		}

		IDs, cursor, err = DB.ScanNodes(ctx, cursor, 10000)
		if err != nil {
			return nil, fmt.Errorf("ScanNodes(): %w", err)
		}

		nodeIDs = append(nodeIDs, IDs...)

		// If the cursor returns to 0, the scan is complete
		if cursor == 0 {
			break
		}
	}

	return sliceutils.Unique(nodeIDs), nil
}

// Size() returns the number of nodes in the DB. In case of errors, it returns 0.
func (DB *Database) Size(ctx context.Context) int {
	if err := DB.Validate(); err != nil {
		return 0
	}

	size, err := DB.client.HLen(ctx, KeyKeyIndex).Result()
	if err != nil {
		return 0
	}

	return int(size)
}

// --------------------------------------HELPERS--------------------------------

// NewDatabaseFromPubkeys() returns an initialized database storing the specified pubkeys.
func NewDatabaseFromPubkeys(ctx context.Context, cl *redis.Client, pubkeys []string) (*Database, error) {
	DB, err := NewDatabase(ctx, cl)
	if err != nil {
		return nil, err
	}

	for _, pk := range pubkeys {
		if _, err := DB.AddNode(ctx, pk); err != nil {
			return nil, err
		}
	}

	return DB, nil
}

// function that returns a DB setup based on the DBType
func SetupDB(cl *redis.Client, DBType string) (*Database, error) {
	ctx := context.Background()
	if cl == nil {
		return nil, ErrNilClient
	}

	switch DBType {
	case "nil":
		return nil, nil

	case "nil-client":
		return &Database{client: nil}, nil

	case "empty":
		return NewDatabase(ctx, cl)

	case "one-node0":
		DB, err := NewDatabase(ctx, cl)
		if err != nil {
			return nil, err
		}

		if _, err := DB.AddNode(ctx, "0"); err != nil {
			return nil, err
		}

		return DB, nil

	case "simple":
		DB, err := NewDatabase(ctx, cl)
		if err != nil {
			return nil, err
		}

		// adding the pubkeys
		pubkeys := []string{"0", "1", "2"}
		for _, pk := range pubkeys {
			if _, err := DB.AddNode(ctx, pk); err != nil {
				return nil, err
			}
		}

		// promoting node1
		if err := DB.client.HSet(ctx, KeyNode(1), NodeStatus, models.StatusActive).Err(); err != nil {
			return nil, err
		}

		// adding 0 --follows--> 1
		if err := DB.client.SAdd(ctx, KeyFollows(0), 1).Err(); err != nil {
			return nil, err
		}

		if err := DB.client.SAdd(ctx, KeyFollowers(1), 0).Err(); err != nil {
			return nil, err
		}

		return DB, nil

	default:
		return nil, nil
	}
}

// generates a random mock database of a specified number of nodes and successors per node
// the successor of a node won't include itself, and won't have repetitions
func GenerateDB(cl *redis.Client, nodesNum, successorsPerNode int, rng *rand.Rand) (*Database, error) {
	ctx := context.Background()
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

		nodeID := uint32(i)
		pubkey := redisutils.FormatID(nodeID)
		if _, err := DB.AddNode(ctx, pubkey); err != nil {
			return nil, err
		}

		delta := &models.Delta{
			NodeID: nodeID,
			Record: models.Record{Type: models.Follow},
			Added:  randomFollows,
		}

		if err := DB.Update(ctx, delta); err != nil {
			return nil, err
		}
	}
	return DB, nil
}

// KeyNode() returns the Redis key for the node with specified nodeID.
func KeyNode[ID uint32 | int64 | int](nodeID ID) string {
	return fmt.Sprintf("%v%d", KeyNodePrefix, nodeID)
}

// KeyFollows() returns the Redis key for the follows of the specified nodeID
func KeyFollows[ID uint32 | int64 | int](nodeID ID) string {
	return fmt.Sprintf("%v%d", KeyFollowsPrefix, nodeID)
}

// KeyFollowers() returns the Redis key for the followers of the specified nodeID
func KeyFollowers[ID uint32 | int64 | int](nodeID ID) string {
	return fmt.Sprintf("%v%d", KeyFollowersPrefix, nodeID)
}

//---------------------------------ERROR-CODES---------------------------------

var ErrNilClient = errors.New("nil redis client pointer")
