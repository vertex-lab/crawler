package graph

// defines the basic structure of a node in the graph
type Node struct {
	ID           uint32   // unique identifier
	SuccessorIDs []uint32 // List of nodes it follows
}
