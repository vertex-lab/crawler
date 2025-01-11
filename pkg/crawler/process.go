package crawler

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	"github.com/vertex-lab/crawler/pkg/utils/counter"
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
	eventCounter *xsync.Counter,
	pagerankTotal *counter.Float) {

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
				if err := ProcessFollowList(ctx, DB, RWM, event, pagerankTotal); err != nil {
					logger.Error("Error processing the eventID %v: %v", event.ID, err)
				}
			default:
				logger.Warn("event of unwanted kind: %v", event.Kind)
			}

			eventCounter.Inc()
			if eventCounter.Value()%1000 == 0 {
				logger.Info("processed %d events", eventCounter.Value())
			}
		}
	}
}

// ProcessFollowList() adds the author and its follows to the database.
// It updates the node metadata of the author, and updates the random walks.
func ProcessFollowList(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	event *nostr.Event,
	pagerankTotal *counter.Float) error {

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	author, err := DB.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return err
	}

	if event.CreatedAt.Time().Unix() <= author.EventTS {
		return nil
	}

	pubkeys := ParsePubkeys(event)
	newFollows, err := AssignNodeIDs(ctx, DB, pubkeys)
	if err != nil {
		return err
	}

	followsByNode, err := DB.Follows(ctx, author.ID)
	if err != nil {
		return err
	}
	oldFollows := followsByNode[0]

	removed, common, added := sliceutils.Partition(oldFollows, newFollows)
	if len(removed)+len(added) == 0 { // the follow list is a rebrodcast of one we alreay have
		return nil
	}

	nodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			EventTS: event.CreatedAt.Time().Unix(),
		},
		AddedFollows:   added,
		RemovedFollows: removed,
	}

	if err := DB.UpdateNode(ctx, author.ID, &nodeDiff); err != nil {
		return err
	}

	if err := RWM.Update(ctx, DB, author.ID, removed, common, added); err != nil {
		return err
	}

	// lazy recomputation of pagerank. Update the scores of the most impacted nodes only
	pagerank, err := pagerank.Global(ctx, RWM.Store, newFollows...)
	if err != nil {
		return err
	}

	if err := DB.SetPagerank(ctx, pagerank); err != nil {
		return err
	}

	pagerankTotal.Add(author.Pagerank)
	return nil
}

// AssignNodeIDs() returns the nodeIDs of the specified pubkeys.
// If a pubkey isn't found in the database, it gets added.
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
					Pubkey:   pubkeys[i],
					EventTS:  0,
					Status:   models.StatusInactive,
					Pagerank: 0.0,
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

		// pubkeys should be unique in the follow list
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
// - recomputes the pagerank of all nodes
func NodeArbiter(
	ctx context.Context,
	logger *logger.Aggregate,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	pagerankTotal *counter.Float,
	startThreshold, promotionMultiplier, demotionMultiplier float64,
	queueHandler func(pk string) error) {

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("  > Stopping the Node Arbiter... ")
			return

		case <-ticker.C:
			if pagerankTotal.Load() >= startThreshold {

				if err := ArbiterScan(ctx, DB, RWM, promotionMultiplier, demotionMultiplier, queueHandler); err != nil {
					logger.Error("%v", err)
					continue
				}

				if err := FullPagerankUpdate(ctx, DB, RWM.Store); err != nil {
					logger.Error("%v", err)
					continue
				}

				// resetting the pagerank since the last recomputation
				pagerankTotal.Store(0)
				logger.Info("NodeArbiter: scan completed")
			}
		}
	}
}

// ArbiterScan() performs one entire database scan, promoting or demoting nodes
// based on their pagerank.
func ArbiterScan(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	promotionMultiplier, demotionMultiplier float64,
	queueHandler func(pk string) error) error {

	ctx, cancel := context.WithTimeout(ctx, 600*time.Second)
	defer cancel()

	var cursor uint64
	var nodeIDs []uint32
	var err error

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// proceed with the scan
		}

		nodeIDs, cursor, err = DB.ScanNodes(ctx, cursor, 1000)
		if err != nil {
			return fmt.Errorf("ArbiterScan(): %w", err)
		}

		minPagerank := minPagerank(ctx, RWM.Store)
		promotionThreshold := minPagerank * promotionMultiplier
		demotionThreshold := minPagerank * demotionMultiplier

		for _, ID := range nodeIDs {
			node, err := DB.NodeByID(ctx, ID)
			if err != nil {
				continue
			}

			// Active --> Inactive
			if node.Status == models.StatusActive && node.Pagerank < demotionThreshold {
				if err := DemoteNode(ctx, DB, RWM, ID); err != nil {
					return fmt.Errorf("ArbiterScan(): %w", err)
				}
			}

			// Inactive --> Active
			if node.Status == models.StatusInactive && node.Pagerank >= promotionThreshold {
				if err := PromoteNode(ctx, DB, RWM, ID); err != nil {
					return fmt.Errorf("ArbiterScan(): %w", err)
				}

				if err := queueHandler(node.Pubkey); err != nil {
					return fmt.Errorf("ArbiterScan(): error sending pubkey %v to the queue: %w", node.Pubkey, err)
				}
			}
		}

		// If the cursor returns to 0, the scan is complete
		if cursor == 0 {
			break
		}
	}

	return nil
}

// The minPagerank() returns the minimum pagerank for an active node, which is
// walksPerNode / TotalVisits in the extreme case that a node is visited only by its own walks.
func minPagerank(ctx context.Context, RWS models.RandomWalkStore) float64 {
	walksPerNode := float64(RWS.WalksPerNode(ctx))
	totalVisits := float64(RWS.TotalVisits(ctx))
	return walksPerNode / totalVisits
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

// FullPagerankUpdate() performs a full pagerank recomputation, which is computationally expensive.
func FullPagerankUpdate(ctx context.Context, DB models.Database, RWS models.RandomWalkStore) error {

	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	nodeIDs, err := DB.AllNodes(ctx)
	if err != nil {
		return fmt.Errorf("FullPagerankUpdate(): %w", err)
	}

	rank, err := pagerank.Global(ctx, RWS, nodeIDs...)
	if err != nil {
		return fmt.Errorf("FullPagerankUpdate(): %w", err)
	}

	if err := DB.SetPagerank(ctx, rank); err != nil {
		return fmt.Errorf("FullPagerankUpdate(): %w", err)
	}

	return nil
}
