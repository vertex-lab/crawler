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

import "errors"

// NodeMeta contains the metadata about a node in the graph, meaning everything that is not a relationship
type NodeMeta struct {
	PubKey    string  `redis:"pubkey"`
	Timestamp int64   `redis:"timestamp"`
	Status    string  `redis:"status"`
	Pagerank  float64 `redis:"pagerank"`
}

// the basic structure of a node in the graph
type Node struct {
	Metadata     NodeMeta
	Successors   []uint32
	Predecessors []uint32
}

// The Database interface abstracts the DB basic functions
type Database interface {

	// Validate() returns the appropriate error if the DB is nil or empty
	Validate() error

	// AddNode() adds a node to the database and returns its assigned nodeID
	AddNode(*Node) (uint32, error)

	// UpdateNode() updates the nodeID using the new values inside node.
	UpdateNode(nodeID uint32, node *Node) error

	// ContainsNode() returns wheter a specified nodeID is found in the DB
	ContainsNode(nodeID uint32) bool

	// IsDandling() returns whether a node has any successor
	IsDandling(nodeID uint32) bool

	// Successors() returns a slice that contains the IDs of all successors of a node
	Successors(nodeID uint32) ([]uint32, error)

	// NodeIDs() returns a slice of nodeIDs that correspond with the given slice of pubkeys.
	// If a pubkey is not found, nil is returned
	NodeIDs(pubkeys []string) ([]interface{}, error)

	// AllNodes() returns a slice with the IDs of all nodes in the DB
	AllNodes() ([]uint32, error)

	// Size() returns the number of nodes in the DB (ignores errors).
	Size() int

	// NodeCache() returns a NodeCache struct, used in the main crawler for
	// efficiently filtering events without calling the database
	NodeCache() (NodeCache, error)
}

// NodeFilterAttributes contains attributes of a node used to filter a Nostr
// event without querying the Database.
type NodeFilterAttributes struct {
	ID        uint32
	Timestamp int64
	Pagerank  float64
}

// NodeCache maps a pubkey to the filter attributes of the corrisponding node.
type NodeCache map[string]NodeFilterAttributes

//--------------------------ERROR-CODES--------------------------

var ErrNilDBPointer = errors.New("database pointer is nil")
var ErrEmptyDB = errors.New("database is empty")
var ErrNodeNotFoundDB = errors.New("node not found in the database")
var ErrNodeAlreadyInDB = errors.New("node already in the database")
