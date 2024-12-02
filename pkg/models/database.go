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
	"errors"
)

const StatusActive = "active" // we generate random walks from this node.
const StatusInactive = "inactive"

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
	Metadata     NodeMeta
	Successors   []uint32
	Predecessors []uint32
}

// NodeDiff represent the updates to do for a specified node.
type NodeDiff struct {

	// Only the specified metadata fields will be changed; the others will
	// mantain the old value thanks to "omitempty".
	Metadata NodeMeta

	// The slice of nodeIDs to be added to the node's successors
	AddedSucc []uint32

	// The slice of nodeIDs to be removed from the node's successors
	RemovedSucc []uint32
}

// The Database interface abstracts the DB basic functions
type Database interface {

	// Validate() returns the appropriate error if the DB is nil or empty
	Validate() error

	// NodeByID() retrieves a node by its nodeID.
	NodeByID(nodeID uint32) (*NodeMeta, error)

	// NodeByKey() retrieves a node by its pubkey.
	NodeByKey(pubkey string) (*NodeMeta, error)

	// AddNode() adds a node to the database and returns its assigned nodeID
	AddNode(node *Node) (uint32, error)

	// UpdateNode() updates the nodeID using the new values inside node.
	UpdateNode(nodeID uint32, nodeDiff *NodeDiff) error

	// ContainsNode() returns wheter a specified nodeID is found in the DB
	ContainsNode(nodeID uint32) bool

	// IsDandling() returns whether a node has any successor
	IsDandling(nodeID uint32) bool

	// Successors() returns a slice that contains the IDs of all successors of a node
	Successors(nodeID uint32) ([]uint32, error)

	// NodeIDs() returns a slice of nodeIDs that correspond with the given slice of pubkeys.
	// If a pubkey is not found, nil is returned
	NodeIDs(pubkeys []string) ([]interface{}, error)

	// Pubkeys() returns a slice of pubkeys that correspond with the given slice of nodeIDs.
	// If a nodeID is not found, nil is returned
	Pubkeys(nodeIDs []uint32) ([]interface{}, error)

	// ScanNodes() scans over the nodes and returns a batch of nodeIDs of size roughly equal to limit.
	// Limit controls how much "work" is invested in fetching the batch, hence it is not precise.
	ScanNodes(cursor uint64, limit int) ([]uint32, uint64, error)

	// AllNodes() returns a slice with the IDs of all nodes in the DB
	AllNodes() ([]uint32, error)

	// Size() returns the number of nodes in the DB (ignores errors).
	Size() int

	// SetPagerank() set the pagerank in the database according to the pagerankMap
	SetPagerank(PagerankMap) error
}

//--------------------------ERROR-CODES--------------------------

var ErrNilDBPointer = errors.New("database pointer is nil")
var ErrEmptyDB = errors.New("database is empty")
var ErrNodeNotFoundDB = errors.New("node not found in the database")
var ErrNodeAlreadyInDB = errors.New("node already in the database")
