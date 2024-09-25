package pagerank

import (
	"github.com/pippellia-btc/analytic_engine/pkg/graph"
)

func (rwm *RandomWalksMap) GenerateRandomWalks(db graph.Database,
	alpha float32, walks_per_node uint16) error {

	err := CheckInputs(rwm, db)
	if err != nil {
		return err
	}

	// TODO implementing random walk logic
	return nil
}

func CheckInputs(rwm *RandomWalksMap, db graph.Database) error {

	// handle nil database pointer
	if db == nil {
		return graph.ErrNilDatabasePointer
	}

	// handle empty database
	is_empty, _ := db.IsEmpty()
	if is_empty {
		return graph.ErrDatabaseIsEmpty
	}

	// handle nil rwm pointer
	if rwm == nil {
		return ErrNilRWMPointer
	}

	return nil
}
