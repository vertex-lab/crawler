package pagerank

import (
	"errors"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
)

func TestWalksToRemoveByNode(t *testing.T) {

	t.Run("positive NewWalksToRemoveByNode, empty set", func(t *testing.T) {

		WTR := NewWalksToRemoveByNode(mapset.NewSet[uint32]())

		if len(WTR.removals) != 0 {
			t.Errorf("NewWalksToRemovebyNode(): expected 0, got %d", len(WTR.removals))
		}
	})

	t.Run("positive NewWalksToRemoveByNode, non-empty set", func(t *testing.T) {

		nodesSet := mapset.NewSet[uint32](1, 2)
		WTR := NewWalksToRemoveByNode(nodesSet)

		if len(WTR.removals) != 2 {
			t.Errorf("NewWalksToRemovebyNode(): expected 2, got %d", len(WTR.removals))
		}
	})

	t.Run("negative recordWalkRemoval, nil WTR", func(t *testing.T) {

		var WTR *WalksToRemoveByNode // nil WTR
		walk := &RandomWalk{NodeIDs: []uint32{0}}

		err := WTR.recordWalkRemoval(walk, 0)
		if !errors.Is(err, ErrNilWTRPointer) {
			t.Errorf("recordWalkRemoval(): expected %v, got %v", ErrNilWTRPointer, err)
		}
	})

	t.Run("negative recordWalkRemoval, nil walk", func(t *testing.T) {

		WTR := NewWalksToRemoveByNode(mapset.NewSet[uint32]())
		var walk *RandomWalk // nil walk

		err := WTR.recordWalkRemoval(walk, 0)
		if !errors.Is(err, ErrNilWalkPointer) {
			t.Errorf("recordWalkRemoval(): expected %v, got %v", ErrNilWalkPointer, err)
		}
	})

	t.Run("negative recordWalkRemoval, empty walk", func(t *testing.T) {

		WTR := NewWalksToRemoveByNode(mapset.NewSet[uint32]())
		walk := &RandomWalk{NodeIDs: []uint32{}} // empty walk

		err := WTR.recordWalkRemoval(walk, 0)
		if !errors.Is(err, ErrEmptyWalk) {
			t.Errorf("recordWalkRemoval(): expected %v, got %v", ErrEmptyWalk, err)
		}
	})

	t.Run("negative recordWalkRemoval, invalid indexes", func(t *testing.T) {

		WTR := NewWalksToRemoveByNode(mapset.NewSet[uint32]())
		walk := &RandomWalk{NodeIDs: []uint32{1}}

		invalidIndexes := []int{99, 11, -123}
		for _, index := range invalidIndexes {

			err := WTR.recordWalkRemoval(walk, index)
			if !errors.Is(err, ErrInvalidWalkIndex) {
				t.Errorf("recordWalkRemoval(): expected %v, got %v", ErrInvalidWalkIndex, err)
			}
		}
	})

	t.Run("positive recordWalkRemoval", func(t *testing.T) {

		WTR := NewWalksToRemoveByNode(mapset.NewSet[uint32]())
		walk := &RandomWalk{NodeIDs: []uint32{0, 2}}

		err := WTR.recordWalkRemoval(walk, 1)
		if err != nil {
			t.Fatalf("recordWalkRemoval(): expected nil, got %v", err)
		}

		if WTR.removals[2][walk] != 1 {
			t.Errorf("recordWalkRemoval(): expected %v, got %v", 1, WTR.removals[2][walk])
		}

	})

}
