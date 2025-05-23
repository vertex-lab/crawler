package crawler

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
	"github.com/vertex-lab/crawler/pkg/walks"
)

type NodeArbiterConfig struct {
	Log                 *logger.Aggregate
	ActivationThreshold float64
	PromotionMultiplier float64
	DemotionMultiplier  float64
	PromotionWaitPeriod time.Duration
}

func NewNodeArbiterConfig() NodeArbiterConfig {
	return NodeArbiterConfig{
		Log:                 logger.New(os.Stdout),
		ActivationThreshold: 0.01,
		PromotionMultiplier: 0.1,
		DemotionMultiplier:  1.05,
		PromotionWaitPeriod: time.Hour,
	}
}

func (c NodeArbiterConfig) Print() {
	fmt.Printf("Arbiter\n")
	fmt.Printf("  Activation: %f\n", c.ActivationThreshold)
	fmt.Printf("  Promotion: %f\n", c.PromotionMultiplier)
	fmt.Printf("  Demotion: %f\n", c.DemotionMultiplier)
	fmt.Printf("  WaitPeriod: %v\n", c.PromotionWaitPeriod)
}

// NodeArbiter() activates when pagerankTotal > threshold. When that happens it:
// - scans through all the nodes in the database
// - promotes or demotes nodes
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
			config.Log.Info("  > Stopping the Node Arbiter... ")
			return

		case <-ticker.C:
			totalWalks = float64(RWS.TotalVisits(ctx)) * float64(1-RWS.Alpha(ctx)) // on average a walk is 1/(1-alpha) steps long
			changeRatio = float64(walksChanged.Load()) / totalWalks

			if changeRatio >= config.ActivationThreshold {
				promoted, demoted, err := ArbiterScan(ctx, config, DB, RWS, queueHandler)
				if err != nil {
					config.Log.Error("%v", err)
					continue
				}

				// resetting the walksChanged since the last successful recomputation
				walksChanged.Store(0)
				config.Log.Info("NodeArbiter scan completed: promoted %d, demoted %d", promoted, demoted)
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

		walksPerNode := RWS.WalksPerNode(ctx)
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
				case shouldDemote(node, visits[i], walksPerNode, config):
					if err := DemoteNode(opCtx, DB, RWS, ID); err != nil {
						return fmt.Errorf("failed to demote node %d: %w", ID, err)
					}

					demoted++

				case shouldPromote(node, visits[i], walksPerNode, config):
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

// Returns whether the given node should be demoted.
func shouldDemote(node *models.Node, visits int, walksPerNode uint16, config NodeArbiterConfig) bool {
	if node.Status != models.StatusActive {
		return false
	}

	threshold := int(config.DemotionMultiplier*float64(walksPerNode) + 0.5)
	return visits < threshold
}

// Returns whether the given node should be promoted.
func shouldPromote(node *models.Node, visits int, walksPerNode uint16, config NodeArbiterConfig) bool {
	if node.Status != models.StatusInactive {
		return false
	}

	ts := node.Added()
	if ts == nil || time.Since(*ts) < config.PromotionWaitPeriod {
		// node is too new to be eligible for promotion
		return false
	}

	threshold := int(config.PromotionMultiplier*float64(walksPerNode) + 0.5)
	return visits >= threshold
}

// PromoteNode() makes a node active, which means it generates random walks for it and updates the status to active.
func PromoteNode(
	ctx context.Context,
	DB models.Database,
	RWS models.RandomWalkStore,
	nodeID uint32) error {

	if err := walks.Generate(ctx, DB, RWS, nodeID); err != nil {
		return fmt.Errorf("failed to generate walks: %w", err)
	}

	delta := &models.Delta{Kind: models.Promotion, NodeID: nodeID}
	if err := DB.Update(ctx, delta); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
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
		return fmt.Errorf("failed to remove walks: %w", err)
	}

	delta := &models.Delta{Kind: models.Demotion, NodeID: nodeID}
	if err := DB.Update(ctx, delta); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
	}

	return nil
}
