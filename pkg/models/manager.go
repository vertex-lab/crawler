package models

// RandomWalkManager handles high-level operations like generating and updating random walks
type RandomWalkManager interface {
	// IsEmpty returns whether RWM is empty (ignores errors).
	IsEmpty() bool

	// Generates and stores random walks for nodeID.
	Generate(DB Database, nodeID uint32) error

	// Generates and stores random walks for ALL nodes in the database.
	GenerateAll(DB Database) error

	// Updates the random walks of nodeID who has updated its successors from OldSucc to currentSucc
	Update(DB Database, nodeID uint32, oldSucc []uint32, currentSucc []uint32) error
}
