package graph

type Node struct {
	ID           string   // unique identifier
	SuccessorsID []string // List of nodes it follows
}
