package models

import "errors"

// the basic structure of a node in the graph
type Node struct {
	Npub         string
	Timestamp    int64
	Successors   []uint32
	Predecessors []uint32
}

// The Database interface abstracts the DB basic functions
type Database interface {

	// Validate() returns the appropriate error if the DB is nil or empty
	Validate() error

	// AddNode() adds a node to the database and returns its assigned nodeID
	AddNode(Npub string) (uint32, error)

	// ContainsNode() returns wheter a specified nodeID is found in the DB
	ContainsNode(nodeID uint32) bool

	// IsDandling() returns whether a node has any successor
	IsDandling(nodeID uint32) bool

	// Successors() returns a slice that contains the IDs of all successors of a node
	Successors(nodeID uint32) ([]uint32, error)

	// AllNodes() returns a slice with the IDs of all nodes in the DB
	AllNodes() ([]uint32, error)

	// NodeCount() returns the number of nodes in the DB (ignores errors).
	NodeCount() int
}

//--------------------------ERROR-CODES--------------------------

var ErrNilDBPointer = errors.New("database pointer is nil")
var ErrEmptyDB = errors.New("database is empty")
var ErrNodeNotFoundDB = errors.New("node not found in the database")
