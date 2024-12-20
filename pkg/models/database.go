/*
The models package defines the fundamental structures and interfaces used in this project.
Interfaces:

Database:
The Database interface abstracts the basic database functionalities, allowing for
multiple implementations.

RandomWalkStore:
The RandomWalkStore interface abstracts atomic operations to create, update, and
remove RandomWalks. These walks are then utilized in the pagerank package.
*/
package models

import (
	"context"
	"errors"
)

const (
	KeyID       string = "id"
	KeyPubkey   string = "pubkey"
	KeyEventTS  string = "event_timestamp"
	KeyStatus   string = "status"
	KeyPagerank string = "pagerank"

	StatusActive   string = "active" // we generate random walks for this node
	StatusInactive string = "inactive"
)

// NodeMeta contains the metadata about a node, meaning everything that is not a relationship
type NodeMeta struct {
	ID       uint32  `redis:"id,omitempty"`
	Pubkey   string  `redis:"pubkey,omitempty"`
	EventTS  int64   `redis:"event_timestamp,omitempty"`
	Status   string  `redis:"status,omitempty"`
	Pagerank float64 `redis:"pagerank,omitempty"`
}

// Node represent the basic structure of a node in the graph
type Node struct {
	Metadata  NodeMeta
	Follows   []uint32
	Followers []uint32
}

// NodeDiff represent the updates to do for a specified node.
type NodeDiff struct {

	// Only the specified metadata fields will be changed; the others will
	// mantain the old value thanks to "omitempty".
	Metadata NodeMeta

	// The slice of nodeIDs to be added to the node's successors
	AddedFollows []uint32

	// The slice of nodeIDs to be removed from the node's successors
	RemovedFollows []uint32
}

// The Database interface abstracts the DB basic functions
type Database interface {
	// Size() returns the number of nodes in the DB (ignores errors).
	Size(ctx context.Context) int

	// ContainsNode() returns wheter a specified nodeID is found in the DB
	ContainsNode(ctx context.Context, nodeID uint32) bool

	// Validate() returns the appropriate error if the DB is nil or empty
	Validate() error

	// NodeByID() retrieves a node by its nodeID.
	NodeByID(ctx context.Context, nodeID uint32) (*NodeMeta, error)

	// NodeByKey() retrieves a node by its pubkey.
	NodeByKey(ctx context.Context, pubkey string) (*NodeMeta, error)

	// AddNode() adds a node to the database and returns its assigned nodeID
	AddNode(ctx context.Context, node *Node) (uint32, error)

	// UpdateNode() updates the nodeID using the new values inside node.
	UpdateNode(ctx context.Context, nodeID uint32, nodeDiff *NodeDiff) error

	// Followers() returns a slice that contains the followers of each nodeID.
	Followers(ctx context.Context, nodeIDs ...uint32) ([][]uint32, error)

	// Follows() returns a slice that contains the follows of each nodeID.
	Follows(ctx context.Context, nodeIDs ...uint32) ([][]uint32, error)

	// NodeIDs() returns a slice of nodeIDs that correspond with the given slice of pubkeys.
	// If a pubkey is not found, nil is returned
	NodeIDs(ctx context.Context, pubkeys ...string) ([]interface{}, error)

	// Pubkeys() returns a slice of pubkeys that correspond with the given slice of nodeIDs.
	// If a nodeID is not found, nil is returned
	Pubkeys(ctx context.Context, nodeIDs ...uint32) ([]interface{}, error)

	// ScanNodes() scans over the nodes and returns a batch of nodeIDs of size roughly equal to limit.
	// Limit controls how much "work" is invested in fetching the batch, hence it is not precise.
	ScanNodes(ctx context.Context, cursor uint64, limit int) ([]uint32, uint64, error)

	// AllNodes() returns a slice with the IDs of all nodes in the DB.
	// This is a blocking operation, so ScanNodes should be prefered when running in prod.
	AllNodes(ctx context.Context) ([]uint32, error)

	// SetPagerank() set the pagerank in the database according to the pagerankMap
	SetPagerank(ctx context.Context, p PagerankMap) error
}

// a map that associates each nodeID with its corrisponding pagerank value
type PagerankMap map[uint32]float64

//--------------------------ERROR-CODES--------------------------

var ErrNilDBPointer = errors.New("database pointer is nil")
var ErrEmptyDB = errors.New("database is empty")
var ErrNodeNotFoundDB = errors.New("node not found in the database")
var ErrNodeAlreadyInDB = errors.New("node already in the database")

var ErrNilClientPointer = errors.New("nil client pointer")
