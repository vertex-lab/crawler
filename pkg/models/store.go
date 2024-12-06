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

	// TotalVisits() returns the total number of visits.
	// In case of any error, the default value 0 is returned.
	TotalVisits() int

	// IsEmpty() returns whether RWS is empty (ignores errors).
	IsEmpty() bool

	// Validate() checks whether the RWS is nil, empty or non-empty and returns
	// an appropriate error based on the requirement.
	Validate(expectEmptyRWS bool) error

	// VisitCounts() returns a map that associates each nodeID with the number of times it was visited by a walk.
	VisitCounts(nodeIDs []uint32) (map[uint32]int, error)

	// Walks() returns a map of walks by walksID that visit nodeID.
	// - if limit > 0, the map contains up to that many key-value pairs.
	// - if limit <= 0, all walks are returned
	Walks(nodeID uint32, limit int) (map[uint32]RandomWalk, error)

	// WalksRand() returns a map of walks by walkID chosen at random from the walks
	// that visit nodeID, according to a specified probability of selection.
	WalksRand(nodeID uint32, probabilityOfSelection float32) (map[uint32]RandomWalk, error)

	// CommonWalks() returns a map of walks by walkID that contain both nodeID
	// and at least one of the removedNode in removedNodes.
	CommonWalks(nodeID uint32, removedNodes []uint32) (map[uint32]RandomWalk, error)

	// AddWalk() adds a walk to the RandomWalkStore.
	AddWalk(walk RandomWalk) error

	// RemoveWalks() removes all the walks associated with the specified walkIDs

	// PruneGraftWalk() encapsulates the functions of Pruning and
	// Grafting ( = appending to) a walk.
	// These functions need to be coupled together to leverage the atomicity of
	// Redis transactions.
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
var ErrInvalidTotalVisits = errors.New("totalVisits should be greater than zero")
var ErrNilRWSPointer = errors.New("nil RWS pointer")
var ErrEmptyRWS = errors.New("RWS is empty")
var ErrNonEmptyRWS = errors.New("the RWS is NOT empty")
var ErrNodeNotFoundRWS = errors.New("nodeID not found in the RWS")
