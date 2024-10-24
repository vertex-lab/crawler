package graph

import "errors"

// defines the basic structure of a node in the graph
type Node struct {
	ID           uint32
	SuccessorIDs []uint32
}

/*
The Database interface abstracts the DB basic functions
*/
type Database interface {

	// returns the appropriate error if the DB is nil or has no nodes
	CheckEmpty() error

	// retrieves a node by its ID from the DB
	NodeByID(ID uint32) (*Node, error)

	IsDandling(ID uint32) bool

	// returns a slice that contains the IDs of all successors of a node
	NodeSuccessorIDs(ID uint32) ([]uint32, error)

	// returns a slice with the IDs of all nodes in the DB
	AllNodeIDs() ([]uint32, error)
}

//--------------------------ERROR-CODES--------------------------

var ErrNilDatabasePointer = errors.New("database pointer is nil")
var ErrDatabaseIsEmpty = errors.New("database is empty")
var ErrNodeNotFoundDB = errors.New("node not found in the DB")
