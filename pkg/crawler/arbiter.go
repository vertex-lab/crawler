package crawler

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
	"github.com/vertex-lab/crawler/pkg/walks"
)

type NodeArbiterConfig struct {
	log                 *logger.Aggregate
	startThreshold      float64
	promotionMultiplier float64
	demotionMultiplier  float64
}

// NodeArbiter() activates when pagerankTotal > threshold. When that happens it:
// - scans through all the nodes in the database
// - promotes or demotes them based on their pagerank and promotion/demotion multipliers
func NodeArbiter(
	ctx context.Context,
	config NodeArbiterConfig,
	DB models.Database,
	RWS models.RandomWalkStore,
	walksChanged *atomic.Uint32,
	queueHandler func(pk string) error) {

	var totalWalks float64
	var changeRatio float64

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			config.log.Info("  > Stopping the Node Arbiter... ")
			return

		case <-ticker.C:
			totalWalks = float64(RWS.TotalVisits(ctx)) * float64(1-RWS.Alpha(ctx)) // on average a walk is 1/(1-alpha) steps long
			changeRatio = float64(walksChanged.Load()) / totalWalks

			if changeRatio >= config.startThreshold {
				promoted, demoted, err := ArbiterScan(ctx, config, DB, RWS, queueHandler)
				if err != nil {
					config.log.Error("%v", err)
					continue
				}

				// resetting the walksChanged since the last successful recomputation
				walksChanged.Store(0)
				config.log.Info("NodeArbiter scan completed: promoted %d, demoted %d", promoted, demoted)
			}
		}
	}
}

// ArbiterScan() performs one entire database scan, promoting or demoting nodes based on their pagerank.
func ArbiterScan(
	ctx context.Context,
	config NodeArbiterConfig,
	DB models.Database,
	RWS models.RandomWalkStore,
	queueHandler func(pk string) error) (promoted, demoted int, err error) {

	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	var cursor uint64
	var nodeIDs []uint32

	for {
		select {
		case <-ctx.Done():
			return promoted, demoted, nil
		default:
			// proceed with the scan
		}

		nodeIDs, cursor, err = DB.ScanNodes(ctx, cursor, 10000)
		if err != nil {
			return promoted, demoted, fmt.Errorf("ArbiterScan(): ScanNodes: %w", err)
		}

		visits, err := RWS.VisitCounts(ctx, nodeIDs...)
		if err != nil {
			return promoted, demoted, fmt.Errorf("ArbiterScan(): visits: %w", err)
		}

		walksPerNode := float64(RWS.WalksPerNode(ctx))
		promotionThreshold := int(config.promotionMultiplier*walksPerNode + 0.5)
		demotionThreshold := int(config.demotionMultiplier*walksPerNode + 0.5)

		for i, ID := range nodeIDs {
			// use a new context for the operation to avoid it being interrupted,
			// which might result in an inconsistent state of the database. Expected time <100ms
			err = func() error {
				opCtx, opCancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer opCancel()

				node, err := DB.NodeByID(opCtx, ID)
				if err != nil {
					return fmt.Errorf("failed to retrieve node by ID %d: %w", ID, err)
				}

				switch {
				// Active --> Inactive
				case node.Status == models.StatusActive && visits[i] < demotionThreshold:
					if err := DemoteNode(opCtx, DB, RWS, ID); err != nil {
						return fmt.Errorf("failed to demote node %d: %w", ID, err)
					}

					demoted++

				// Inactive --> Active
				case node.Status == models.StatusInactive && visits[i] >= promotionThreshold:
					if err := PromoteNode(opCtx, DB, RWS, ID); err != nil {
						return fmt.Errorf("failed to promote node %d: %w", ID, err)
					}

					if err := queueHandler(node.Pubkey); err != nil {
						return fmt.Errorf("failed to queue pubkey %s: %w", node.Pubkey, err)
					}

					promoted++
				}

				return nil
			}()

			if err != nil {
				return promoted, demoted, fmt.Errorf("ArbiterScan(): %w", err)
			}
		}

		// If the cursor returns to 0, the scan is complete
		if cursor == 0 {
			break
		}
	}

	return promoted, demoted, nil
}

// PromoteNode() makes a node active, which means it generates random walks for it and updates the status to active.
func PromoteNode(
	ctx context.Context,
	DB models.Database,
	RWS models.RandomWalkStore,
	nodeID uint32) error {

	if err := walks.Generate(ctx, DB, RWS, nodeID); err != nil {
		return fmt.Errorf("PromoteNode(): %w", err)
	}

	delta := &models.Delta{
		Record: models.Record{Timestamp: time.Now().Unix(), Type: models.Promotion},
		NodeID: nodeID,
	}

	if err := DB.Update(ctx, delta); err != nil {
		return fmt.Errorf("PromoteNode(): %w", err)
	}

	return nil
}

// DemoteNode() makes a node inactive, which means removes random walks that
// start from it, and updates the status to inactive.
func DemoteNode(
	ctx context.Context,
	DB models.Database,
	RWS models.RandomWalkStore,
	nodeID uint32) error {

	if err := walks.Remove(ctx, RWS, nodeID); err != nil {
		return fmt.Errorf("DemoteNode(): %w", err)
	}

	delta := &models.Delta{
		Record: models.Record{Timestamp: time.Now().Unix(), Type: models.Demotion},
		NodeID: nodeID,
	}

	if err := DB.Update(ctx, delta); err != nil {
		return fmt.Errorf("DemoteNode(): %w", err)
	}

	return nil
}
