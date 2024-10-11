package graph

import "errors"

/*
The Database interface abstracts the db basic functions, so I can develop
the analytics engine without relaying on a specific database
*/
type Database interface {

	// CheckEmpty returns the appropriate error if the db is nil or has no nodes
	CheckEmpty() error

	// FetchNodeByID retrieves a node by ID from the db
	NodeByID(ID uint32) (*Node, error)

	// GetNodesuccessorIDs returns a slice that contains the ids of all successors of a node
	NodeSuccessorIDs(ID uint32) ([]uint32, error)

	// GetAllNodeIDs returns a slice with the ids of all nodes in the db
	AllNodeIDs() ([]uint32, error)
}

//--------------------------ERROR-CODES--------------------------

var ErrNilDatabasePointer = errors.New("database pointer is nil")
var ErrDatabaseIsEmpty = errors.New("database is empty")
var ErrNodeNotFoundDB = errors.New("node not found in the DB")
