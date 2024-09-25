package graph

// this abstracts the Database basic functions, so I can develop
// the analytics engine without relaying on a specific database
type Database interface {
	FetchNodeByID(ID uint32) (*Node, error)
	GetNodeSuccessorsID(ID uint32) ([]uint32, error)
}
