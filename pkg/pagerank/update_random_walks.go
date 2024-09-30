package pagerank

import (
	"math/rand"
	"time"

	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

/*
UpdateRandomWalks updates the RandomWalksMap when a node's successors change from
succOld to succNew.

INPUTS
------

	> db: graph.Database
	The database where nodes are stored

	>

OUTPUT
------

	> error: look at checkInputs() to read all the errors

NOTE
----

	This function is computationally expensive and should be called only when
	the RandomWalksMap is empty. During the normal execution of the program,
	there should always be random walks, so we should not re-do them from scratch,
	but just update them when necessary (e.g. when there is a graph update).
	checkInputs checks if the RandomWalksMap is empty.

REFERENCES
----------

	[1] B. Bahmani, A. Chowdhury, A. Goel; "Fast Incremental and Personalized PageRank"
	link: http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf
*/
func (rwm *RandomWalksMap) UpdateRandomWalks(db graph.Database) error {

	const expectEmptyRWM = false

	// checking all the inputs
	err := checkInputs(rwm, db, expectEmptyRWM)
	if err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return rwm.updateRandomWalks(db, rng)
}

func (rwm *RandomWalksMap) updateRandomWalks(db graph.Database, rng *rand.Rand) error {

	return nil
}
