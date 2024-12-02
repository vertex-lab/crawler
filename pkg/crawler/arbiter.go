package crawler

import (
	"context"
	"fmt"

	"github.com/vertex-lab/crawler/pkg/logger"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/walks"
)

// NodeArbiter() scans through all the nodes in the database, and promotes or
// demotes them based on their pagerank.
func NodeArbiter(
	ctx context.Context,
	logger logger.Aggregate,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	queueHandler func(pk string) error) {

	var threshold float64 = pagerankThreshold(DB.Size())
	var cursor uint64 = 0

	for {
		// check if the context is done before a scan
		select {
		case <-ctx.Done():
			fmt.Printf("\n  > Stopping the Node Arbiter... ")
			return
		default:
		}

		nodeIDs, cursor, err := DB.ScanNodes(cursor, 1000)
		if err != nil {
			logger.Error("Error scanning nodes: %v", err)
			break
		}

		// If the cursor returns to 0, the scan is complete
		if cursor == 0 {
			break
		}

		for _, nodeID := range nodeIDs {
			node, err := DB.NodeByID(nodeID)
			if err != nil {
				logger.Error("Error querying nodeID %d: %v", nodeID, err)
				break
			}

			// Active --> Inactive
			if node.Status == models.StatusActive && node.Pagerank < threshold {
				if err := DemoteNode(ctx, DB, RWM, nodeID); err != nil {
					logger.Error("Error demoting nodeID %d: %v", nodeID, err)
					break
				}
			}

			// Inactive --> Active
			if node.Status == models.StatusInactive && node.Pagerank > threshold {
				if err := PromoteNode(ctx, DB, RWM, nodeID); err != nil {
					logger.Error("Error promoting nodeID %d: %v", nodeID, err)
					break
				}

				if err := queueHandler(node.Pubkey); err != nil {
					logger.Error("Error sending to queue: %v", err)
				}
			}
		}
	}
}

// PromoteNode() generates random walks for the specified node and sets it to "active".
func PromoteNode(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	nodeID uint32) error {

	if err := RWM.Generate(DB, nodeID); err != nil {
		return err
	}

	nodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			Status: "active",
		},
	}

	if err := DB.UpdateNode(nodeID, &nodeDiff); err != nil {
		return err
	}

	return nil
}

// DemoteNode() sets a node to "inactive". In the future this function will also
// remove the walks that starts from that node.
func DemoteNode(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	nodeID uint32) error {

	// if err := RWM.Remove(DB, nodeID); err != nil {
	// 	return err
	// }

	nodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			Status: "inactive",
		},
	}

	if err := DB.UpdateNode(nodeID, &nodeDiff); err != nil {
		return err
	}

	return nil
}
