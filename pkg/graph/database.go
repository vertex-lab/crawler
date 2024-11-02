package graph

import "errors"

// the basic structure of a node in the graph
type Node struct {
	ID           uint32
	Successors   []uint32
	Predecessors []uint32
}

// The Database interface abstracts the DB basic functions
type Database interface {

	// returns the appropriate error if the DB is nil or has no nodes
	CheckEmpty() error

	// retrieves a node by its ID from the DB
	Node(ID uint32) (*Node, error)

	IsDandling(ID uint32) bool

	// returns a slice that contains the IDs of all successors of a node
	Successors(ID uint32) ([]uint32, error)

	// returns a slice with the IDs of all nodes in the DB
	AllIDs() ([]uint32, error)
}

//--------------------------ERROR-CODES--------------------------

var ErrNilGraphDBPointer = errors.New("GraphDB pointer is nil")
var ErrGraphDBIsEmpty = errors.New("GraphDB is empty")
var ErrNodeNotFoundDB = errors.New("node not found in the GraphDB")
