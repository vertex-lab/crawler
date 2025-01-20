package crawler

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

// ProcessEvents() process one event at the time from the eventChannel, based on their kind.
func ProcessEvents(
	ctx context.Context,
	logger *logger.Aggregate,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	eventChan <-chan *nostr.Event,
	eventCounter, walksChanged *atomic.Uint32) {

	for {
		select {
		case <-ctx.Done():
			logger.Info("  > Finishing processing the event... ")
			return

		case event, ok := <-eventChan:
			if !ok {
				logger.Warn("Event channel closed, stopping processing.")
				return
			}

			if event == nil {
				logger.Warn("ProcessEvents: event is nil")
				continue
			}

			switch event.Kind {
			case nostr.KindFollowList:
				if err := ProcessFollowList(DB, RWM, event, walksChanged); err != nil {
					logger.Error("Error processing the eventID %v: %v", event.ID, err)
				}
			default:
				logger.Warn("event of unwanted kind: %v", event.Kind)
			}

			eventCounter.Add(1)
			if eventCounter.Load()%1000 == 0 {
				logger.Info("processed %d events", eventCounter.Load())
			}
		}
	}
}

// ProcessFollowList() adds the author and its follows to the database.
// It updates the node metadata of the author, and updates the random walks.
func ProcessFollowList(
	DB models.Database,
	RWM *walks.RandomWalkManager,
	event *nostr.Event,
	walksChanged *atomic.Uint32) error {

	// use a new context for the operation to avoid it being interrupted,
	// which might result in an inconsistent state of the database. Expected time <1000ms
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	author, err := DB.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return fmt.Errorf("failed to fetch node by key %v: %w", event.PubKey, err)
	}

	if event.CreatedAt.Time().Unix() <= author.EventTS {
		return nil
	}

	pubkeys := ParsePubkeys(event)
	newFollows, err := AssignNodeIDs(ctx, DB, pubkeys)
	if err != nil {
		return fmt.Errorf("failed to assign node IDs to the follows of %v: %w", event.PubKey, err)
	}

	followsByNode, err := DB.Follows(ctx, author.ID)
	if err != nil {
		return fmt.Errorf("failed to fetch the old follows of %v: %w", author.ID, err)
	}
	oldFollows := followsByNode[0]

	removed, common, added := sliceutils.Partition(oldFollows, newFollows)
	nodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			EventTS: event.CreatedAt.Time().Unix(),
		},
		AddedFollows:   added,
		RemovedFollows: removed,
	}

	if err := DB.UpdateNode(ctx, author.ID, &nodeDiff); err != nil {
		return fmt.Errorf("failed to update nodeID %d: %w", author.ID, err)
	}

	updated, err := RWM.Update(ctx, DB, author.ID, removed, common, added)
	if err != nil {
		return err
	}

	walksChanged.Add(uint32(updated)) // this counter triggers the activation of NodeArbiter
	return nil
}

// AssignNodeIDs() returns the nodeIDs of the specified pubkeys. If a pubkey isn't found in the database, it gets added.
func AssignNodeIDs(
	ctx context.Context,
	DB models.Database,
	pubkeys []string) ([]uint32, error) {

	IDs, err := DB.NodeIDs(ctx, pubkeys...)
	if err != nil {
		return nil, err
	}

	nodeIDs := make([]uint32, len(IDs))
	for i, ID := range IDs {
		if ID == nil {
			// if it's nil, the pubkey wasn't found in the database
			// add a new node to the database, and assign it an ID
			node := models.Node{
				Metadata: models.NodeMeta{
					Pubkey:  pubkeys[i],
					EventTS: 0,
					Status:  models.StatusInactive,
				},
			}
			nodeID, err := DB.AddNode(ctx, &node)
			if err != nil {
				return nil, err
			}

			nodeIDs[i] = nodeID
			continue
		}

		nodeIDs[i] = *ID
	}

	return nodeIDs, nil
}

// ParsePubkeys() returns the slice of pubkeys that are correctly listed in the nostr.Tags.
// - Badly formatted tags are ignored.
// - Pubkeys will be uniquely added (no repetitions).
// - The author of the event will be removed from the followed pubkeys if present.
func ParsePubkeys(event *nostr.Event) []string {
	const followPrefix = "p"

	// if it's empty or very big, skip
	if event == nil || len(event.Tags) == 0 || len(event.Tags) > 100000 {
		return []string{}
	}

	pubkeys := make([]string, 0, len(event.Tags))
	for _, tag := range event.Tags {
		if len(tag) < 2 {
			continue
		}

		prefix, pubkey := tag[0], tag[1]
		if prefix != followPrefix {
			continue
		}

		// remove the author from the followed pubkeys, as that is no signal
		if pubkey == event.PubKey {
			continue
		}

		// pubkeys should be unique in the follow list; TODO, this is inefficient.
		if slices.Contains(pubkeys, pubkey) {
			continue
		}

		if !nostr.IsValidPublicKey(pubkey) {
			continue
		}

		pubkeys = append(pubkeys, pubkey)
	}

	return pubkeys
}

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

	var totalVisits float64
	var changeRatio float64

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("  > Stopping the Node Arbiter... ")
			return

		case <-ticker.C:
			totalVisits = float64(RWM.Store.TotalVisits(ctx))
			changeRatio = float64(walksChanged.Load()) / totalVisits

			if changeRatio >= startThreshold {
				promoted, demoted, err := ArbiterScan(ctx, DB, RWM, promotionMultiplier, demotionMultiplier, queueHandler)
				logger.Info("promoted %d, demoted %d", promoted, demoted)

				if err != nil {
					logger.Error("%v", err)
					continue
				}

				// resetting the walksChanged since the last recomputation
				walksChanged.Store(0)
				logger.Info("NodeArbiter: scan completed")
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

// The minPagerank() returns the minimum pagerank for an active node, which is
// walksPerNode / TotalVisits in the extreme case that a node is visited only by its own walks.
// func minPagerank(ctx context.Context, RWS models.RandomWalkStore) float64 {
// 	walksPerNode := float64(RWS.WalksPerNode(ctx))
// 	totalVisits := float64(RWS.TotalVisits(ctx))
// 	return walksPerNode / totalVisits
// }

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

	nodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			Status: models.StatusActive,
		},
	}

	if err := DB.UpdateNode(ctx, nodeID, &nodeDiff); err != nil {
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

	nodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			Status: models.StatusInactive,
		},
	}

	if err := DB.UpdateNode(ctx, nodeID, &nodeDiff); err != nil {
		return fmt.Errorf("DemoteNode(): %w", err)
	}

	return nil
}
