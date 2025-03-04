/*
The models package defines the fundamental interfaces used in this project.
Interfaces:

Database:
The Database interface abstracts the basic CRUD database functionalities.

RandomWalkStore:
The RandomWalkStore interface abstracts atomic operations to create, update, and
remove RandomWalks. These walks are then utilized in the pagerank package to
efficiently compute global and personalized pageranks.
*/
package models

import (
	"context"
	"errors"
)

const (
	// types of status
	StatusActive   string = "active" // meaning, we generate random walks for this node
	StatusInactive string = "inactive"

	// internal record kinds
	Added     int = -3
	Promotion int = -2
	Demotion  int = -1
)

// Node contains the metadata about a node, including a collection of Records.
type Node struct {
	ID      uint32
	Pubkey  string
	Status  string   // either active or inactive
	Records []Record // not all Records will be necessarily returned from the DB
}

// Record encapsulates data around an update that involved a Node. For example an update to its follow-list, or its promotion/demotion.
type Record struct {
	ID        string // e.g. the event.ID
	Timestamp int64  // e.g. the event.CreatedAt
	Kind      int    // e.g. the event.Kind or "Promotion" / "Demotion"
}

// Delta represent the updates to do for a specified NodeID. Added and Removed represent respectively the
// added and removed relationship (e.g. a Node added 0,11 and removed 12 from its follow-list)
type Delta struct {
	Record
	NodeID  uint32
	Removed []uint32
	Added   []uint32
}

type Database interface {
	// Size() returns the number of nodes in the DB (ignores errors).
	Size(ctx context.Context) int

	// ContainsNode() returns wheter a specified nodeID is found in the DB
	ContainsNode(ctx context.Context, nodeID uint32) bool

	// Validate() returns the appropriate error if the DB is nil or empty
	Validate() error

	// NodeByID() retrieves a node by its nodeID.
	NodeByID(ctx context.Context, nodeID uint32) (*Node, error)

	// NodeByKey() retrieves a node by its pubkey.
	NodeByKey(ctx context.Context, pubkey string) (*Node, error)

	// AddNode() adds a node to the database and returns its assigned nodeID
	AddNode(ctx context.Context, pubkey string) (uint32, error)

	// Update() applies the delta to the database.
	Update(ctx context.Context, delta *Delta) error

	// Followers() returns a slice that contains the followers of each nodeID.
	Followers(ctx context.Context, nodeIDs ...uint32) ([][]uint32, error)

	// Follows() returns a slice that contains the follows of each nodeID.
	Follows(ctx context.Context, nodeIDs ...uint32) ([][]uint32, error)

	// NodeIDs() returns a slice of nodeIDs that correspond with the given slice of pubkeys.
	// If a pubkey is not found, nil is returned
	NodeIDs(ctx context.Context, pubkeys ...string) ([]*uint32, error)

	// Pubkeys() returns a slice of pubkeys that correspond with the given slice of nodeIDs.
	// If a nodeID is not found, nil is returned
	Pubkeys(ctx context.Context, nodeIDs ...uint32) ([]*string, error)

	// ScanNodes() scans over the nodes and returns a batch of nodeIDs of size roughly equal to limit.
	// Limit controls how much "work" is invested in fetching the batch, hence it is not precise.
	// See the guarantees of scan: https://redis.io/docs/latest/commands/scan/
	ScanNodes(ctx context.Context, cursor uint64, limit int) ([]uint32, uint64, error)

	// AllNodes() returns a slice with the IDs of all nodes in the DB.
	AllNodes(ctx context.Context) ([]uint32, error)
}

// a map that associates each nodeID with its corrisponding pagerank value
type PagerankMap map[uint32]float64

//--------------------------------ERROR-CODES-----------------------------------

var (
	ErrNilDB           error = errors.New("database pointer is nil")
	ErrNilDelta        error = errors.New("nil delta pointer")
	ErrEmptyDB         error = errors.New("database is empty")
	ErrNonEmptyDB      error = errors.New("database is NOT empty")
	ErrNodeNotFoundDB  error = errors.New("node not found in the database")
	ErrNodeAlreadyInDB error = errors.New("node already in the database")
)
