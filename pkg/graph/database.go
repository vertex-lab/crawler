package graph

// this abstracts the Database basic functions, so I can develop
// the analytics engine without relaying on a specific database
type Database interface {
	FetchNodeByID(id string) (*Node, error)
	GetNodeSuccessorsID(ID string) ([]string, error)
}
