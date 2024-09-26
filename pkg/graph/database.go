package graph

import "errors"

// this abstracts the Database basic functions, so I can develop
// the analytics engine without relaying on a specific database
type Database interface {
	CheckEmpty() error
	FetchNodeByID(ID uint32) (*Node, error)
	GetNodeSuccessorIDs(ID uint32) ([]uint32, error)
	GetAllNodeIDs() ([]uint32, error)
}

//--------------------------ERROR-CODES--------------------------

var ErrNilDatabasePointer = errors.New("database pointer is nil")
var ErrDatabaseIsEmpty = errors.New("database is empty")
var ErrNodeNotFound = errors.New("node not found")
