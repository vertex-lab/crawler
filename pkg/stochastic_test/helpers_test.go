package stochastictest

import (
	"math"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/pagerank"
)

func TestDistance(t *testing.T) {

	map1 := pagerank.PagerankMap{
		0: 1.0,
		1: 2.0,
	}

	map2 := pagerank.PagerankMap{
		0: 1.1,
		1: 2.2,
	}

	// exected distance
	expected := 0.3

	got := distance(map1, map2)
	if math.Abs(got-expected) > 1e-12 {
		t.Errorf("Distance(): expected %v, got %v", expected, got)
	}

}
