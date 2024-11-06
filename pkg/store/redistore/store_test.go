package redistore

import (
	"context"
	"errors"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

func TestNewRandomWalksStore(t *testing.T) {

	cl := SetupRedis()
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

func TestLoadRandomWalksStore(t *testing.T) {

	cl := SetupRedis()
	defer CleanupRedis(cl)

	var alpha float32 = 0.85
	var walksPerNode uint16 = 10
	if err := cl.Set(context.Background(), "alpha", alpha, 0).Err(); err != nil {
		t.Fatalf("failed to set: %v", err)
	}
	if err := cl.Set(context.Background(), "walksPerNode", walksPerNode, 0).Err(); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

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

			RWS, err := LoadRWS(context.Background(), cl)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("LoadRWS(): expected %v, got %v", test.expectedError, err)
			}

			// check if the parameters have been added correctly
			if RWS != nil {
				if RWS.Alpha() != alpha {
					t.Errorf("LoadRWS(): expected %v, got %v", alpha, RWS.Alpha())
				}

				if RWS.WalksPerNode() != walksPerNode {
					t.Errorf("LoadRWS(): expected %v, got %v", walksPerNode, RWS.WalksPerNode())
				}
			}
		})
	}
}
