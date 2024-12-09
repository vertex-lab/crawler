package models

import (
	"errors"
)

// RandomWalk represent the slice of nodeIDs visited during the walk (e.g. {1,2,77,5})
type RandomWalk []uint32

// Validate returns the appropriate error if the RandomWalk is empty or nil.
func Validate(walk RandomWalk) error {
	if walk == nil {
		return ErrNilWalkPointer
	}
	if len(walk) == 0 {
		return ErrEmptyWalk
	}
	return nil
}

// RandomWalkStore handles atomic operations to create, update, and remove RandomWalks.
type RandomWalkStore interface {

	// Alpha() returns the dampening factor used for the RandomWalks
	Alpha() float32

	// WalkPerNode() returns the number of walks to be generated for each node in the DB
	WalksPerNode() uint16

	// TotalVisits() returns the total number of visits. In case of any error, the default value 0 is returned.
	TotalVisits() int

	// Validate() checks whether RWS is nil, and it's fields.
	Validate() error

	// VisitCounts() returns a map that associates each nodeID with the number of times it was visited by a walk.
	VisitCounts(nodeIDs []uint32) (map[uint32]int, error)

	// Walks() returns a map of walks by walksID that visit nodeID.
	// - if limit > 0, the map contains up to that many key-value pairs.
	// - if limit <= 0, all walks are returned
	Walks(nodeID uint32, limit int) (map[uint32]RandomWalk, error)

	// AddWalks() adds all the walks to the RandomWalkStore.
	AddWalks(walks []RandomWalk) error

	// RemoveWalks() removes all the walks associated with the walkIDs.
	RemoveWalks(walkIDs []uint32) error

	// PruneGraftWalk() encapsulates the functions of pruning and grafting ( = appending to) a walk.
	// These functions need to be coupled together to leverage the atomicity of Redis transactions.
	// Example:
	// 1. Pruning: walk = {0,1,2,3} gets pruned with cutIndex = 1, becoming walk[:cutIndex] = {0,1}
	// 2. Grafting: walkSegment = {4,5} is added to the walk, resulting in walk = {0,1,4,5}
	PruneGraftWalk(walkID uint32, cutIndex int, walkSegment RandomWalk) error
}

//---------------------------------ERROR-CODES---------------------------------

// RandomWalk errors
var ErrNilWalkPointer = errors.New("nil RandomWalk pointer")
var ErrEmptyWalk = errors.New("RandomWalk is empty")
var ErrWalkNotFound = errors.New("RandomWalk not found in RWS")
var ErrInvalidWalkIndex = errors.New("the index is bigger than the lenght of the walk")

// RWS errors
var ErrInvalidAlpha = errors.New("alpha should be a number between 0 and 1 (excluded)")
var ErrInvalidWalksPerNode = errors.New("walksPerNode should be greater than zero")
var ErrNilRWSPointer = errors.New("nil RWS pointer")
var ErrEmptyRWS = errors.New("RWS is empty")
var ErrNonEmptyRWS = errors.New("the RWS is NOT empty")
var ErrNodeNotFoundRWS = errors.New("nodeID not found in the RWS")
