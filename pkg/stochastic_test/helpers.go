package stochastictest

import (
	"math"

	"github.com/pippellia-btc/Nostrcrawler/pkg/pagerank"
)

// computes the L1 distance between two maps who are supposed to have the same keys
func distance(map1, map2 pagerank.PagerankMap) float64 {

	distance := 0.0

	for key, val1 := range map1 {
		distance += math.Abs(val1 - map2[key])
	}

	return distance
}
