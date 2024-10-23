package pagerank

import "math"

// computes the L1 distance between two maps who are supposed to have the same keys
func Distance(map1, map2 PagerankMap) float64 {

	distance := 0.0

	for key, val1 := range map1 {
		distance += math.Abs(val1 - map2[key])
	}

	return distance
}
