package pagerank

import (
	"errors"
	"math/rand"
	"time"

	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

/*
computes the personalized pagerank of nodeID by simulating a long random walk starting at
and resetting to itself. This long walk is generated from the
random walks stored in the RandomWalkStore.

# INPUTS

	> DB models.Database
	The interface of the graph database

	> RWS models.RandomWalkStore
	The interface of the store.

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
func Personalized(DB models.Database, RWS models.RandomWalkStore,
	nodeID uint32, topK uint16) (models.PagerankMap, error) {

	if err := checkInputs(DB, RWS, nodeID, topK); err != nil {
		return nil, err
	}

	// for reproducibility in tests
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return personalized(DB, RWS, nodeID, topK, rng)
}

// personalized() implements the internal logic of the Personalized Pagerank function
func personalized(DB models.Database, RWS models.RandomWalkStore,
	nodeID uint32, topK uint16, rng *rand.Rand) (models.PagerankMap, error) {

	succ, err := DB.Follows(nodeID)
	if err != nil {
		return nil, err
	}

	// if it's a dandling node
	if len(succ) == 0 {
		return models.PagerankMap{nodeID: 1.0}, nil
	}

	pWalk, err := personalizedWalk(DB, RWS, nodeID, requiredLenght(topK), rng)
	if err != nil {
		return nil, err
	}

	return countAndNormalize(pWalk), nil
	// TODO: SEND ONLY TARGET NODES
}

/*
encapsulates the data around the personalized walk.

# FIELDS

	> startingNodeID: uint32
	The ID of the node where the personalized walk starts and resets to

	> currentNodeID: uint32
	The ID of the node that was last visited by the walk

	> currentWalk: []uint32
	The slice of node IDs that have been visited in the current walk. The current walk
	is needed to check for cycles.

	> walk: []uint32
	The slice containing all node IDs of the personalized walk.
	It's the sum of all current walks.
*/
type PersonalizedWalk struct {
	startingNodeID uint32
	currentNodeID  uint32
	currentWalk    models.RandomWalk
	walk           models.RandomWalk
}

// initialize a new personalized walk with a specified targetLenght
func NewPersonalizedWalk(nodeID uint32, targetLength int) *PersonalizedWalk {
	return &PersonalizedWalk{
		startingNodeID: nodeID,
		currentNodeID:  nodeID,
		currentWalk:    models.RandomWalk{nodeID},
		walk:           make(models.RandomWalk, 0, targetLength),
	}
}

// returns whether the personalized walk is long enough
func (p *PersonalizedWalk) Reached(targetLength int) bool {
	return len(p.walk) >= targetLength
}

// appends the current walk and goes back to the starting node
func (p *PersonalizedWalk) Reset() {
	p.walk = append(p.walk, p.currentWalk...)
	p.currentNodeID = p.startingNodeID
	p.currentWalk = models.RandomWalk{p.startingNodeID}
}

// appends nextNodeID and moves there
func (p *PersonalizedWalk) AppendNode(nextNodeID uint32) {
	p.currentWalk = append(p.currentWalk, nextNodeID)
	p.currentNodeID = nextNodeID
}

// removed potential cycles from the walkSegment, appends it to the personalized walks and resets
func (p *PersonalizedWalk) AppendWalk(walkSegment models.RandomWalk) {

	// remove potential cycles
	walkSegment = sliceutils.TrimCycles(p.currentWalk, walkSegment)

	// append
	p.currentWalk = append(p.currentWalk, walkSegment...)
	p.walk = append(p.walk, p.currentWalk...)

	// reset
	p.currentNodeID = p.startingNodeID
	p.currentWalk = models.RandomWalk{p.startingNodeID}
}

/*
simulates a long personalized random walkSegment starting from nodeID with reset to itself.
This personalized walkSegment is generated using the random walks stored in the in the RandomWalkManager.

To avoid the overhead of continually fetching walks from the RWS, the requests
are batched and the walks are stored in the WalkCache struct.
*/
func personalizedWalk(DB models.Database, RWS models.RandomWalkStore,
	nodeID uint32, targetLength int, rng *rand.Rand) (models.RandomWalk, error) {

	WC := NewWalkCache()
	pWalk := NewPersonalizedWalk(nodeID, targetLength)
	alpha := RWS.Alpha()
	estimateWalksToBeLoaded := estimateWalksNum(targetLength, alpha)

	// load walks for nodeID
	if err := WC.Load(RWS, nodeID, estimateWalksToBeLoaded); err != nil {
		return nil, err
	}

	for {
		// the exit condition
		if pWalk.Reached(targetLength) {
			return pWalk.walk, nil
		}

		if rng.Float32() > alpha {
			pWalk.Reset()
			continue
		}

		// if there are no walks, load them
		if !WC.ContainsNode(pWalk.currentNodeID) {
			if err := WC.Load(RWS, pWalk.currentNodeID, 1000); err != nil {
				return nil, err
			}
		}

		// if all walks have been used, do a walk step
		if WC.FullyUsed(pWalk.currentNodeID) {

			successorIDs, err := DB.Follows(pWalk.currentNodeID)
			if err != nil {
				return nil, err
			}

			// perform a walk step
			nextNodeID, shouldStop := walks.WalkStep(successorIDs, pWalk.currentWalk, rng)
			if shouldStop {
				pWalk.Reset()
				continue
			}

			pWalk.AppendNode(nextNodeID)
			continue
		}

		// else, get the next walk
		walkSegment, err := WC.NextWalk(pWalk.currentNodeID)
		if err != nil {
			return nil, err
		}

		pWalk.AppendWalk(walkSegment)
		continue
	}
}

// count the number of times each node is visited in the pWalk and computes their frequencies.
// Returns an empty map if pWalk is nil or empty.
func countAndNormalize(pWalk models.RandomWalk) models.PagerankMap {

	// count the frequency of each nodeID
	pp := make(models.PagerankMap, (len(pWalk)))
	for _, node := range pWalk {
		pp[node]++
	}

	// normalize
	totalVisits := float64(len(pWalk))
	for node, visits := range pp {
		pp[node] = visits / totalVisits
	}

	return pp
}

func estimateWalksNum(lenght int, alpha float32) int {
	return int(float32(lenght) / (1 - alpha))
}

// returns the required lenght of the walkSegment for the Personalized pagerank.
// the result has to be strictly positive
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

	if !DB.ContainsNode(nodeID) {
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
		pWalk := NewPersonalizedWalk(0, targetLenght)
		pWalk.currentNodeID = 2
		pWalk.currentWalk = []uint32{0, 1, 2}
		return pWalk

	default:
		return NewPersonalizedWalk(0, targetLenght)
	}
}

// ---------------------------------ERROR-CODES--------------------------------

var ErrInvalidTopN = errors.New("topK shoud be greater than 0")
