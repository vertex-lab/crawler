package pagerank

import (
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestPagerank(t *testing.T) {

	t.Run("Pagerank, all dandling nodes", func(t *testing.T) {

		size := uint32(5)
		DB := mock.NewMockDatabase()

		// all dandling node DB
		for i := uint32(0); i < size; i++ {
			DB.Nodes[i] = &graph.Node{ID: i, SuccessorIDs: []uint32{}}
		}

		// generate walks
		RWM, _ := walks.NewRWM(0.85, 1000)
		RWM.GenerateAll(DB)

		// the expected pagerank
		pr := float32(1) / float32(size)
		got := Pagerank(RWM)

		for i := uint32(0); i < size; i++ {

			if got[i] != pr {
				t.Errorf("Pagerank(): expected %v, got %v", pr, got[i])
			}
		}

	})
}
