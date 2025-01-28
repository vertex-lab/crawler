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

// NodeArbiter() activates when pagerankTotal > threshold. When that happens it:
// - scans through all the nodes in the database
// - promotes or demotes them based on their pagerank and promotion/demotion multipliers
func NodeArbiter(
	ctx context.Context,
	logger *logger.Aggregate,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	walksChanged *atomic.Uint32,
	startThreshold, promotionMultiplier, demotionMultiplier float64,
	queueHandler func(pk string) error) {

	var totalWalks float64
	var changeRatio float64

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("  > Stopping the Node Arbiter... ")
			return

		case <-ticker.C:
			totalWalks = float64(RWM.Store.TotalVisits(ctx)) * float64(1-RWM.Store.Alpha(ctx)) // on average a walk is 1/(1-alpha) steps long (roughly)
			changeRatio = float64(walksChanged.Load()) / totalWalks

			if changeRatio >= startThreshold {
				promoted, demoted, err := ArbiterScan(ctx, DB, RWM, promotionMultiplier, demotionMultiplier, queueHandler)
				if err != nil {
					logger.Error("%v", err)
					continue
				}

				// resetting the walksChanged since the last recomputation
				walksChanged.Store(0)
				logger.Info("NodeArbiter scan completed: promoted %d, demoted %d", promoted, demoted)
			}
		}
	}
}

// ArbiterScan() performs one entire database scan, promoting or demoting nodes based on their pagerank.
func ArbiterScan(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	promotionMultiplier, demotionMultiplier float64,
	queueHandler func(pk string) error) (promoted, demoted int, err error) {

	ctx, cancel := context.WithTimeout(ctx, 600*time.Second)
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

		visits, err := RWM.Store.VisitCounts(ctx, nodeIDs...)
		if err != nil {
			return promoted, demoted, fmt.Errorf("ArbiterScan(): visits: %w", err)
		}

		walksPerNode := float64(RWM.Store.WalksPerNode(ctx))
		promotionThreshold := int(promotionMultiplier*walksPerNode + 0.5)
		demotionThreshold := int(demotionMultiplier*walksPerNode + 0.5)

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
					if err := DemoteNode(opCtx, DB, RWM, ID); err != nil {
						return fmt.Errorf("failed to demote node %d: %w", ID, err)
					}

					demoted++

				// Inactive --> Active
				case node.Status == models.StatusInactive && visits[i] >= promotionThreshold:
					if err := PromoteNode(opCtx, DB, RWM, ID); err != nil {
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

// PromoteNode() makes a node active, which means it generates random walks
// for it and updates the status to active.
func PromoteNode(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	nodeID uint32) error {

	if err := RWM.Generate(ctx, DB, nodeID); err != nil {
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
	RWM *walks.RandomWalkManager,
	nodeID uint32) error {

	if err := RWM.Remove(ctx, nodeID); err != nil {
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
