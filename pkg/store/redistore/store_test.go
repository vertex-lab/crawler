package redistore

import (
	"context"
	"errors"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

func TestNewRWS(t *testing.T) {

	cl := SetupClient()
	defer CleanupRedis(cl)

	testCases := []struct {
		name          string
		alphas        []float32
		walksPerNode  uint16
		expectedError error
		// ADD CONTEXT TESTS
	}{
		{
			name:          "invalid alphas",
			alphas:        []float32{1.01, 1.0, -0.1, -2},
			walksPerNode:  1,
			expectedError: models.ErrInvalidAlpha,
		},
		{
			name:          "invalid walksPerNode",
			alphas:        []float32{0.99, 0.11, 0.57, 0.0001},
			walksPerNode:  0,
			expectedError: models.ErrInvalidWalksPerNode,
		},
		{
			name:          "both valid",
			alphas:        []float32{0.99, 0.11, 0.57, 0.0001},
			walksPerNode:  1,
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			// iterate over the alphas
			for _, alpha := range test.alphas {

				RWS, err := NewRWS(context.Background(), cl, alpha, test.walksPerNode)
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("NewRWS(): expected %v, got %v", test.expectedError, err)
				}

				// check if the parameters have been added correctly
				if RWS != nil {
					if RWS.Alpha() != alpha {
						t.Errorf("NewRWS(): expected %v, got %v", alpha, RWS.Alpha())
					}

					if RWS.WalksPerNode() != test.walksPerNode {
						t.Errorf("NewRWS(): expected %v, got %v", test.walksPerNode, RWS.WalksPerNode())
					}
				}
			}
		})
	}
}

func TestLoadRWS(t *testing.T) {
	cl := SetupClient()
	defer CleanupRedis(cl)

	testCases := []struct {
		name          string
		expectedError error
		// ADD CONTEXT TESTS
	}{
		{
			name:          "normal",
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			SetupRWS(cl, "empty")
			RWS, err := LoadRWS(context.Background(), cl)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("LoadRWS(): expected %v, got %v", test.expectedError, err)
			}

			// check if the parameters have been added correctly
			if RWS != nil {
				if RWS.Alpha() != float32(0.85) {
					t.Errorf("LoadRWS(): expected %v, got %v", 0.85, RWS.Alpha())
				}

				if RWS.WalksPerNode() != uint16(1) {
					t.Errorf("LoadRWS(): expected %v, got %v", 1, RWS.WalksPerNode())
				}
			}
		})
	}
}

// func TestIsEmpty(t *testing.T) {
// 	cl := SetupClient()
// 	defer CleanupRedis(cl)

// 	testCases := []struct {
// 		name          string
// 		RWSType       string
// 		expectedEmpty bool
// 	}{
// 		{
// 			name:          "nil RWS",
// 			RWSType:       "nil",
// 			expectedEmpty: true,
// 		},
// 		{
// 			name:          "empty RWS",
// 			RWSType:       "empty",
// 			expectedEmpty: true,
// 		},
// 		{
// 			name:          "non-empty RWS",
// 			RWSType:       "one-node0",
// 			expectedEmpty: false,
// 		},
// 	}

// 	for _, test := range testCases {
// 		t.Run(test.name, func(t *testing.T) {

// 			RWS := SetupRWS(cl, test.RWSType)
// 			empty := RWS.IsEmpty()

// 			if empty != test.expectedEmpty {
// 				t.Errorf("IsEmpty(): expected %v, got %v", test.expectedEmpty, empty)
// 			}
// 		})
// 	}
// }
