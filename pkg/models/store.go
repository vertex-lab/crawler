package models

import (
	"context"
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

	// Validate() checks whether RWS is nil, and it's fields.
	Validate() error

	// Alpha() returns the dampening factor used for the RandomWalks
	Alpha(ctx context.Context) float32

	// WalkPerNode() returns the number of walks to be generated for each node in the DB
	WalksPerNode(ctx context.Context) uint16

	// TotalVisits() returns the total number of visits. In case of any error, the default value 0 is returned.
	TotalVisits(ctx context.Context) int

	// VisitCounts() returns a map that associates each nodeID with the number of times it was visited by a walk.
	VisitCounts(ctx context.Context, nodeIDs []uint32) ([]int, error)

	/*WalksVisiting() returns up to limit UNIQUE walkIDs evenly distributed among the specified nodeIDs.
	In other words, it returns up to limit/len(nodeIDs) walkIDs for each of the nodes.

	Note:
	- If 0 < limit < nodeIDs, no walk is returned
	- If limit <= 0, all walks for all nodes are returned (use signalling value limit = -1)
	*/
	WalksVisiting(ctx context.Context, limit int, nodeIDs ...uint32) ([]uint32, error)

	// WalksVisitingAll() returns all the IDs of the walk that visit ALL specified nodes.
	WalksVisitingAll(ctx context.Context, nodeIDs ...uint32) ([]uint32, error)

	// Walks() returns the walks associated with the walkIDs.
	Walks(ctx context.Context, walkIDs ...uint32) ([]RandomWalk, error)

	// AddWalks() adds all the walks to the RandomWalkStore.
	AddWalks(ctx context.Context, walks []RandomWalk) error

	// RemoveWalks() removes all the walks associated with the walkIDs.
	RemoveWalks(ctx context.Context, walkIDs []uint32) error

	/*PruneGraftWalk() encapsulates the functions of pruning and grafting ( = appending to) a walk.
	These functions need to be coupled together to leverage the atomicity of Redis transactions.

	# Example:
	1. Pruning: walk = {0,1,2,3} gets pruned with cutIndex = 1, becoming walk[:cutIndex] = {0,1}
	2. Grafting: walkSegment = {4,5} is added to the walk, resulting in walk = {0,1,4,5}*/
	PruneGraftWalk(ctx context.Context, walkID uint32, cutIndex int, walkSegment RandomWalk) error
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
