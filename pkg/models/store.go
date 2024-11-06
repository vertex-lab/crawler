package models

import (
	"errors"

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
	// Validate() checks whether the RWS is nil, empty or non-empty and returns
	// an appropriate error based on the requirement.
	Validate(expectEmptyRWS bool) error

	// Alpha() returns the dampening factor used for the RandomWalks
	Alpha() float32

	// WalkPerNode() returns the number of walks to be generated for each node in the DB
	WalksPerNode() uint16

	// IsEmpty() returns whether RWS is empty (ignores errors).
	IsEmpty() bool

	// ContainsNode() returns whether RWS contains a given nodeID (ignores errors)
	ContainsNode(nodeID uint32) bool

	// NodeCount() returns the number of nodes in the RWS (ignores errors).
	NodeCount() int

	// All() returns a slice with all the nodeIDs in the RWS.
	AllNodes() []uint32

	// VisitCount() returns the number of times nodeID has been visited by a walk
	VisitCount(nodeID uint32) int

	// Walks() returns a map of walks by walksID that visit nodeID.
	NodeWalks(nodeID uint32) (map[uint32]RandomWalk, error)

	// AddWalk() adds the walk to the WalkIndex. It also adds the walkID to the
	// WalkIDSet of each node the walk visited. This means that for each node
	// visited by the walk, the walk ID will be added to its WalkSet.
	AddWalk(walk RandomWalk) error

	// PruneWalk() removes the walkID from each node in the walk after cutIndex.
	// This means that for each prunedNode in walk[cutIndex:], the walk ID will
	// be removed from its WalkSet.
	PruneWalk(walkID uint32, cutIndex int) error

	// GraftWalk() grafts (extends) the walk with the walkSegment, and adds
	// the walkID to the WalkSet of each node in the new walkSegment.
	GraftWalk(walkID uint32, walkSegment []uint32) error
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
