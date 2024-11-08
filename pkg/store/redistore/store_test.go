package redistore

import (
	"context"
	"errors"
	"reflect"
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

func TestIsEmpty(t *testing.T) {
	cl := SetupClient()
	defer CleanupRedis(cl)

	testCases := []struct {
		name          string
		RWSType       string
		expectedEmpty bool
	}{
		{
			name:          "nil RWS",
			RWSType:       "nil",
			expectedEmpty: true,
		},
		{
			name:          "empty RWS",
			RWSType:       "empty",
			expectedEmpty: true,
		},
		{
			name:          "non-empty RWS",
			RWSType:       "one-walk0",
			expectedEmpty: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RWS := SetupRWS(cl, test.RWSType)
			empty := RWS.IsEmpty()

			if empty != test.expectedEmpty {
				t.Errorf("IsEmpty(): expected %v, got %v", test.expectedEmpty, empty)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	cl := SetupClient()
	defer CleanupRedis(cl)

	testCases := []struct {
		name          string
		RWSType       string
		expectedEmpty bool
		expectedError error
	}{
		{
			name:          "nil RWS, expected empty",
			RWSType:       "nil",
			expectedEmpty: true,
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "nil RWS, expected non-empty",
			RWSType:       "nil",
			expectedEmpty: false,
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "empty RWS, expected empty",
			RWSType:       "empty",
			expectedEmpty: true,
			expectedError: nil,
		},
		{
			name:          "empty RWS, expected non-empty",
			RWSType:       "empty",
			expectedEmpty: false,
			expectedError: models.ErrEmptyRWS,
		},
		{
			name:          "non-empty RWS, expected empty",
			RWSType:       "one-walk0",
			expectedEmpty: true,
			expectedError: models.ErrNonEmptyRWS,
		},
		{
			name:          "non-empty RWS, expected non-empty",
			RWSType:       "one-walk0",
			expectedEmpty: false,
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RWS := SetupRWS(cl, test.RWSType)
			err := RWS.Validate(test.expectedEmpty)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("Validate(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestAddWalk(t *testing.T) {
	cl := SetupClient()
	defer CleanupRedis(cl)

	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			RWSType       string
			walk          models.RandomWalk
			expectedError error
		}{
			{
				name:          "nil RWS",
				RWSType:       "nil",
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "nil walk",
				RWSType:       "empty",
				walk:          nil,
				expectedError: models.ErrNilWalkPointer,
			},
			{
				name:          "empty walk",
				RWSType:       "empty",
				walk:          models.RandomWalk{},
				expectedError: models.ErrEmptyWalk,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				RWS := SetupRWS(cl, test.RWSType)

				err := RWS.AddWalk(test.walk)
				if !errors.Is(err, test.expectedError) {
					t.Errorf("AddWalk(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {

		RWS := SetupRWS(cl, "empty")
		walk := models.RandomWalk{1, 2, 3}

		if err := RWS.AddWalk(walk); err != nil {
			t.Fatalf("AddWalk(): expected nil, got %v", err)
		}

		// Load the walk from Redis
		strWalk, err := RWS.client.Get(RWS.ctx, KeyRedis(KeyWalk, 0)).Result()
		if err != nil {
			t.Fatalf("Get(): expected nil, got %v", err)
		}

		loadedWalk, err := ParseWalk(strWalk)
		if err != nil {
			t.Fatalf("ParseWalk(): expected nil, got %v", err)
		}

		// check if the two walks match
		if !reflect.DeepEqual(walk, loadedWalk) {
			t.Errorf("AddWalk(): expected %v, got %v", walk, loadedWalk)
		}

		// check that each node is associated with the walkID = 0
		for _, nodeID := range walk {

			// Get the only walkID associated with nodeID
			key := KeyRedis(KeyNodeWalkIDs, nodeID)
			strWalkID, err := RWS.client.SRandMember(RWS.ctx, key).Result()
			if err != nil {
				t.Fatalf("GetStringAndParse(): expected nil, got %v", err)
			}

			// Parse it to Float32
			loadedWalkID, err := ParseID(strWalkID)
			if err != nil {
				t.Fatalf("ParseFromString(): expected nil, got %v", err)
			}

			// check it matches the intended walkID
			if loadedWalkID != 0 {
				t.Errorf("AddWalk(): expected %v, got %v", 0, loadedWalkID)
			}
		}
	})
}
