package pagerank

import (
	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

func (rwm *RandomWalksMap) GenerateRandomWalks(db graph.Database,
	alpha float32, walks_per_node uint16) error {

	// handle empty database
	is_empty, _ := db.IsEmpty()
	if is_empty {
		return graph.ErrDatabaseIsEmpty
	}

	return nil
}
