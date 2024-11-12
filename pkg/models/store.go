package models

import (
	"errors"
	"math/rand"

	mapset "github.com/deckarep/golang-set/v2"
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

// WalkIDSet is a set of IDs of RandomWalks. Each node in the RandomWalkStore
// is associated with the set of IDs of walks that visit that node.
type WalkIDSet mapset.Set[uint32]

// RandomWalkStore handles atomic operations to create, update, and remove RandomWalks.
type RandomWalkStore interface {

	// Alpha() returns the dampening factor used for the RandomWalks
	Alpha() float32

	// WalkPerNode() returns the number of walks to be generated for each node in the DB
	WalksPerNode() uint16

	// IsEmpty() returns whether RWS is empty (ignores errors).
	IsEmpty() bool

	// Validate() checks whether the RWS is nil, empty or non-empty and returns
	// an appropriate error based on the requirement.
	Validate(expectEmptyRWS bool) error

	// ContainsNode() returns whether RWS contains a given nodeID (ignores errors)
	ContainsNode(nodeID uint32) bool

	// NodeCount() returns the number of nodes in the RWS (ignores errors).
	NodeCount() int

	// All() returns a slice with all the nodeIDs in the RWS.
	AllNodes() []uint32

	// VisitCount() returns the number of times nodeID has been visited by a walk
	VisitCount(nodeID uint32) int

	// NodeWalks() returns a map of walks by walksID that visit nodeID.
	NodeWalks(nodeID uint32) (map[uint32]RandomWalk, error)

	// AddWalk() adds a walk to the RandomWalkStore.
	AddWalk(walk RandomWalk) error

	// PruneGraftWalk() encapsulates the functions of Pruning and
	// Grafting ( = appending to) a walk.
	// These functions need to be coupled together to leverage the atomicity of
	// Redis transactions. This ensures that a walk is either uneffected or is both
	// pruned and grafted successfully.
	PruneGraftWalk(walkID uint32, cutIndex int, walkSegment RandomWalk) error

	// WalksForUpdateRemoved returns a map of candidate walks by walkID that MIGHT
	// be updated inside the method RWM.updateRemovedNodes().
	// These candidate walks are the one that contain both nodeID and at least one
	// of the removed node in removedNodes.
	WalksForUpdateRemoved(nodeID uint32, removedNodes []uint32) (map[uint32]RandomWalk, error)

	// WalksForUpdateAdded returns a slice of random walks that WILL be updated
	// inside the method RWM.updateAddedNodes().
	// These walks will be chosen at random from the walks that visit nodeID, according to
	// a specified probability of selection.
	WalksForUpdateAdded(nodeID uint32, probabilityOfSelection float32, rng *rand.Rand) (map[uint32]RandomWalk, error)
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
