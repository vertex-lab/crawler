package models

import "math"

// a map that associates each nodeID with its corrisponding pagerank value
type PagerankMap map[uint32]float64

// computes the L1 distance between two maps who are supposed to have the same keys.
// if map 1 is nil or empty, it returns 0.0
func Distance(map1, map2 PagerankMap) float64 {
	distance := 0.0
	for key := range map1 {
		distance += math.Abs(map1[key] - map2[key])
	}
	return distance
}
