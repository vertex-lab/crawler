package pagerank

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

// PersonalizedWalk struct encapsulates data around the personalized walk, which is
// a type of random walk that resets to a specified node.
type PersonalizedWalk struct {
	// The ID of the node where the personalized walk starts and resets to
	startID uint32

	// The ID of the node that was last visited by the walk
	currentID uint32

	// The slice of nodeIDs that have been visited in the current walk, needed for cycle checks.
	current models.RandomWalk

	// The slice containing all node IDs of the personalized walk. It's the sum of all current walks.
	all models.RandomWalk
}

// initialize a new personalized walk with a specified targetLenght
func NewPersonalizedWalk(startID uint32, targetLength int) *PersonalizedWalk {
	return &PersonalizedWalk{
		startID:   startID,
		currentID: startID,
		current:   models.RandomWalk{startID},
		all:       make(models.RandomWalk, 0, targetLength),
	}
}

// Reached() returns whether the personalized walk is long enough
func (p *PersonalizedWalk) Reached(targetLength int) bool {
	return len(p.all) >= targetLength
}

// Reset() appends the current walk and goes back to the starting node
func (p *PersonalizedWalk) Reset() {
	p.all = append(p.all, p.current...)
	p.currentID = p.startID
	p.current = models.RandomWalk{p.startID}
}

// Move() appends nextID and moves there
func (p *PersonalizedWalk) Move(nextID uint32) {
	p.current = append(p.current, nextID)
	p.currentID = nextID
}

// Append() removed potential cycles from the walk, appends it to the personalized walks and resets
func (p *PersonalizedWalk) Append(walk models.RandomWalk) {
	walk = sliceutils.TrimCycles(p.current, walk)
	p.current = append(p.current, walk...)
	p.Reset()
}

/*
Personalized() computes the personalized pagerank of nodeID by simulating a
long random walk starting at and resetting to itself. This long walk is generated
from the random walks stored in the RandomWalkStore.

# INPUTS

	> DB models.Database
	The interface of the graph database

	> RWS models.RandomWalkStore
	The interface of the store where random walks are stored.

	> nodeID uint32
	The ID of the node we are going to compute the personalized pagerank

	> topK uint16
	The number of nodes with the highest personalized pagerank that the
	algorithm aims to identify and converge on. Increasing this parameter
	improves the precision for all nodes but increases the computational cost.

# REFERENCES

[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
URL: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func Personalized(
	ctx context.Context,
	DB models.Database,
	RWS models.RandomWalkStore,
	nodeID uint32,
	topK uint16) (models.PagerankMap, error) {

	if err := checkInputs(DB, RWS, nodeID, topK); err != nil {
		return nil, err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return personalized(ctx, DB, RWS, nodeID, topK, rng)
}

// The personalized() function implements the internal logic of the Personalized Pagerank algorithm
func personalized(
	ctx context.Context,
	DB models.Database,
	RWS models.RandomWalkStore,
	nodeID uint32,
	topK uint16,
	rng *rand.Rand) (models.PagerankMap, error) {

	followSlice, err := DB.Follows(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	follows := followSlice[0]

	// if it's a dandling node, return this special case distribution
	if len(follows) == 0 {
		return models.PagerankMap{nodeID: 1.0}, nil
	}

	FC := NewFollowCache(DB, len(follows)+1)
	FC.follows[nodeID] = follows
	// if err := FC.Load(ctx, follows...); err != nil {
	// 	return nil, err
	// }

	lenght := requiredLenght(topK)
	alpha := RWS.Alpha(ctx)
	// WC := NewWalkCache(walksNeeded(lenght, alpha))
	// if err := WC.Load(ctx, RWS, follows...); err != nil {
	// 	return nil, err
	// }
	WC := NewWalkCache(1)

	walk, err := personalizedWalk(ctx, FC, WC, nodeID, lenght, alpha, rng)
	if err != nil {
		return nil, err
	}

	return countAndNormalize(walk), nil
}

// The personalizedWalk() function simulates a long personalized random walk
// starting from nodeID with reset to itself. Whenever possible, walks from the
// WalkCache are used to speed up the computation.
func personalizedWalk(
	ctx context.Context,
	FC *FollowCache,
	WC *WalkCache,
	nodeID uint32,
	targetLength int,
	alpha float32,
	rng *rand.Rand) (models.RandomWalk, error) {

	walk := NewPersonalizedWalk(nodeID, targetLength)

	for {
		if walk.Reached(targetLength) {
			break
		}

		if rng.Float32() > alpha {
			walk.Reset()
			continue
		}

		nextWalk, exists := WC.Next(walk.currentID)
		if !exists {
			// perform a walk step
			follows, err := FC.Follows(ctx, walk.currentID)
			if err != nil {
				return nil, err
			}

			nextID, shouldStop := walks.WalkStep(follows, walk.current, rng)
			if shouldStop {
				walk.Reset()
				continue
			}

			walk.Move(nextID)
			continue
		}

		nextWalk, err := CropWalk(nextWalk, walk.currentID)
		if err != nil {
			return nil, err
		}
		walk.Append(nextWalk)
	}

	return walk.all, nil
}

// count the number of times each node is visited in the walk and computes their frequencies.
// Returns an empty map if walk is nil or empty.
func countAndNormalize(walk models.RandomWalk) models.PagerankMap {
	lenght := len(walk)
	if lenght == 0 {
		return models.PagerankMap{}
	}

	freq := 1.0 / float64(lenght)
	pp := make(models.PagerankMap, lenght/100)
	for _, node := range walk {
		pp[node] += freq
	}

	return pp
}

// returns the walk from nodeID onward (excluded). If nodeID is not found, returns an error
func CropWalk(walk models.RandomWalk, nodeID uint32) (models.RandomWalk, error) {
	for i, ID := range walk {
		if ID == nodeID {
			return walk[i+1:], nil
		}
	}
	return nil, ErrNodeNotInWalk
}

// The function walksNeeded() estimates the number of walks needed to reach the
// target lenght. It uses the fact that, on average, walks are 1/(1-alpha) long.
func walksNeeded(lenght int, alpha float32) int {
	return int(float32(lenght) / (1 - alpha))
}

// The function requiredLenght() returns the lenght that the personalized walk
// has to reach for the Personalized Pagerank to achieve the specified precision.
func requiredLenght(topK uint16) int {
	_ = topK
	return 300000
}

// function that checks the inputs of Personalized Pagerank;
func checkInputs(DB models.Database, RWS models.RandomWalkStore,
	nodeID uint32, topK uint16) error {

	if err := DB.Validate(); err != nil {
		return err
	}

	if err := RWS.Validate(); err != nil {
		return err
	}

	if !DB.ContainsNode(context.Background(), nodeID) { // ARE U SURE? THINK
		return models.ErrNodeNotFoundDB
	}

	if topK <= 0 {
		return ErrInvalidTopN
	}

	return nil
}

// function that set up a PersonalizedWalk based on the provided type and required lenght
func SetupPWalk(pWalkType string, targetLenght int) *PersonalizedWalk {
	switch pWalkType {

	case "one-node0":
		return NewPersonalizedWalk(0, targetLenght)

	case "triangle":
		walk := NewPersonalizedWalk(0, targetLenght)
		walk.currentID = 2
		walk.current = []uint32{0, 1, 2}
		return walk

	default:
		return NewPersonalizedWalk(0, targetLenght)
	}
}

// ---------------------------------ERROR-CODES--------------------------------

var ErrInvalidTopN = errors.New("topK shoud be greater than 0")
