package crawler

import (
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"

	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
)

func TestArbiterScan(t *testing.T) {
	type testCases struct {
		name          string
		DBType        string
		RWSType       string
		expectedError error
	}

	t.Run("simple errors", func(t *testing.T) {
		testCases := []testCases{
			{
				name:          "nil DB",
				DBType:        "nil",
				RWSType:       "one-node0",
				expectedError: models.ErrNilDB,
			},
			{
				name:          "valid",
				DBType:        "one-node0",
				RWSType:       "one-node0",
				expectedError: nil,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				DB := mockdb.SetupDB(test.DBType)
				RWS := mockstore.SetupRWS(test.RWSType)

				config := NodeArbiterConfig{
					promotionMultiplier: 0.0,
					demotionMultiplier:  0.0,
				}

				_, _, err := ArbiterScan(ctx, config, DB, RWS, func(pk string) error {
					return nil
				})

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("ArbiterScan(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {
		t.Run("demotion", func(t *testing.T) {
			// calle will be demoted to inactive, because the demotion threshold is unattainable.
			ctx := context.Background()
			DB := mockdb.SetupDB("simple-with-pks")
			RWS := mockstore.SetupRWS("one-node1")

			config := NodeArbiterConfig{
				promotionMultiplier: 2.0,
				demotionMultiplier:  2.0,
			}

			_, _, err := ArbiterScan(ctx, config, DB, RWS, func(pk string) error {
				return nil
			})

			if err != nil {
				t.Fatalf("ArbiterScan(): expected nil, got %v", err)
			}

			// check that calle's status changed
			node, exists := DB.NodeIndex[1]
			if !exists {
				t.Fatalf("nodeID %d doesn't exists in the DB", 1)
			}

			if node.Status != models.StatusInactive {
				t.Errorf("expected status of nodeID %d %v, got %v", 1, models.StatusInactive, node.Status)
			}

			// check the only walk (from calle) has been removed
			walkIDs, err := RWS.WalksVisiting(ctx, -1, 1)
			if err != nil {
				t.Errorf("WalksVisiting(): expected nil, got %v", err)
			}

			if len(walkIDs) > 0 {
				t.Errorf("expected no walks, got %v", walkIDs)
			}

		})
		t.Run("promotion", func(t *testing.T) {
			// pip and odell will be promoted from inactive to active, because the promotion threshold is 0 * 1/1  = 0
			ctx := context.Background()
			DB := mockdb.SetupDB("simple-with-pks")
			RWS := mockstore.SetupRWS("one-node1")
			queue := []string{}

			config := NodeArbiterConfig{
				promotionMultiplier: 0.0,
				demotionMultiplier:  0.0,
			}

			_, _, err := ArbiterScan(ctx, config, DB, RWS, func(pk string) error {
				queue = append(queue, pk)
				return nil
			})

			if err != nil {
				t.Fatalf("ArbiterScan(): expected nil, got %v", err)
			}

			// compare queues when sorted in lexicographic order
			expectedQueue := []string{odell, pip}
			slices.Sort(queue)
			if !reflect.DeepEqual(queue, expectedQueue) {
				t.Errorf("ArbiterScan(): expected queue %v, got %v", expectedQueue, queue)
			}

			// check that the status changed
			for nodeID := uint32(0); nodeID < 3; nodeID++ {
				node, exists := DB.NodeIndex[nodeID]
				if !exists {
					t.Fatalf("nodeID %d doesn't exists in the DB", nodeID)
				}

				if node.Status != models.StatusActive {
					t.Errorf("expected status of nodeID %d %v, got %v", nodeID, models.StatusActive, node.Status)
				}
			}

			// check that walks for pip and odell have been generated.
			for _, nodeID := range []uint32{0, 2} {
				walkIDs, err := RWS.WalksVisiting(ctx, -1, nodeID)
				if err != nil {
					t.Fatalf("Walks(%d): expected nil, got %v", 0, err)
				}

				// check it contains exactly one walk (the one generated)
				if len(walkIDs) != 1 {
					t.Errorf("walkIDs: %v", walkIDs)
				}
			}
		})
	})
}

func TestNodeArbiter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	DB := mockdb.SetupDB("one-node0")
	RWS := mockstore.SetupRWS("one-node0")
	walksChanged := &atomic.Uint32{}

	config := NodeArbiterConfig{
		log:                 logger.New(os.Stdout),
		startThreshold:      0,
		promotionMultiplier: 0,
		demotionMultiplier:  0,
	}

	go HandleSignals(cancel, config.log)
	NodeArbiter(ctx, config, DB, RWS, walksChanged, func(pk string) error {
		return nil
	})
}
