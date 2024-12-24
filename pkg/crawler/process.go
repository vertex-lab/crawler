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
			fmt.Printf("\n  > Finishing processing the event... ")
			return

		case event, ok := <-eventChan:
			if !ok {
				fmt.Printf("\n  > Event channel closed, stopping processing.")
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

/*
ProcessFollowList() adds the author and its follows to the database.
It updates the node metadata of the author, and updates the random walks.
*/
func ProcessFollowList(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	event *nostr.Event,
	pagerankTotal *counter.Float) error {

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	author, err := DB.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return err
	}

	if event.CreatedAt.Time().Unix() < author.EventTS {
		return nil
	}

	pubkeys := ParsePubkeys(event)
	newFollows, err := AssignNodeIDs(ctx, DB, pubkeys)
	if err != nil {
		return err
	}

	oldFollows, err := DB.Follows(ctx, author.ID)
	if err != nil {
		return err
	}

	removed, common, added := sliceutils.Partition(oldFollows[0], newFollows)
	if len(removed)+len(added) == 0 {
		return nil
	}

	authorNodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			EventTS: event.CreatedAt.Time().Unix(),
		},
		AddedFollows:   added,
		RemovedFollows: removed,
	}

	if err := DB.UpdateNode(ctx, author.ID, &authorNodeDiff); err != nil {
		return err
	}

	if err := RWM.Update(ctx, DB, author.ID, removed, common, added); err != nil {
		return err
	}

	// lazy recomputation of pagerank. Update the scores of the most impacted nodes only
	impacted := append(added, removed...)
	pagerank, err := pagerank.Global(ctx, RWM.Store, impacted...)
	if err != nil {
		return err
	}

	pagerankTotal.Add(author.Pagerank)
	return DB.SetPagerank(ctx, pagerank)
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

// ParsePubKeys returns the slice of pubkeys that are correctly listed in the nostr.Tags.
// - Badly formatted tags are ignored.
// - Pubkeys will be uniquely added (no repetitions).
// - The author of the event will be removed from the followed pubkeys if present.
func ParsePubkeys(event *nostr.Event) []string {
	const followPrefix = "p"

	if event == nil || len(event.Tags) == 0 {
		return []string{}
	}

	// if it's very big, skip
	if len(event.Tags) > 100000 {
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
// - promotes or demotes them based on their pagerank
// - recomputes the pagerank of all nodes
func NodeArbiter(
	ctx context.Context,
	logger *logger.Aggregate,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	startThreshold float64,
	pagerankTotal *counter.Float,
	queueHandler func(pk string) error) {

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\n  > Stopping the Node Arbiter... ")
			return

		case <-ticker.C:
			if pagerankTotal.Load() >= startThreshold {

				if err := ArbiterScan(ctx, DB, RWM, 0.0, queueHandler); err != nil {
					logger.Error("Arbiter Scan error: %v", err)
					continue
				}

				if err := FullPagerankUpdate(ctx, DB, RWM.Store); err != nil {
					logger.Error("Full Pagerank Update error: %v", err)
					continue
				}

				pagerankTotal.Store(0)
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
	threshold float64,
	queueHandler func(pk string) error) error {

	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	var cursor uint64
	var nodeIDs []uint32
	var err error

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		nodeIDs, cursor, err = DB.ScanNodes(ctx, cursor, 1000)
		if err != nil {
			return fmt.Errorf("error scanning: %w", err)
		}

		for _, nodeID := range nodeIDs {
			node, err := DB.NodeByID(ctx, nodeID)
			if err != nil {
				continue
			}

			// Active --> Inactive
			if node.Status == models.StatusActive && node.Pagerank < threshold {
				if err := DemoteNode(ctx, DB, RWM, nodeID); err != nil {
					return fmt.Errorf("error demoting nodeID %d: %w", nodeID, err)
				}
			}

			// Inactive --> Active
			if node.Status == models.StatusInactive && node.Pagerank >= threshold {
				if err := PromoteNode(ctx, DB, RWM, nodeID); err != nil {
					return fmt.Errorf("error promoting nodeID %d: %w", nodeID, err)
				}

				if err := queueHandler(node.Pubkey); err != nil {
					return fmt.Errorf("error sending pubkey %v to the queue: %w", node.Pubkey, err)
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

// PromoteNode() makes a node active, which means it generates random walks
// for it and updates the status to active.
func PromoteNode(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	nodeID uint32) error {

	if err := RWM.Generate(ctx, DB, nodeID); err != nil {
		return fmt.Errorf("Generate(): %v", err)
	}

	nodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			Status: models.StatusActive,
		},
	}

	if err := DB.UpdateNode(ctx, nodeID, &nodeDiff); err != nil {
		return fmt.Errorf("UpdateNode(): %v", err)
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
		return fmt.Errorf("Remove(): %v", err)
	}

	nodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			Status: models.StatusInactive,
		},
	}

	if err := DB.UpdateNode(ctx, nodeID, &nodeDiff); err != nil {
		return fmt.Errorf("UpdateNode(): %v", err)
	}

	return nil
}

// FullPagerankUpdate() performs a full pagerank recomputation, which is computationally expensive.
func FullPagerankUpdate(ctx context.Context, DB models.Database, RWS models.RandomWalkStore) error {

	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	nodeIDs, err := DB.AllNodes(ctx)
	if err != nil {
		return err
	}

	rank, err := pagerank.Global(ctx, RWS, nodeIDs...)
	if err != nil {
		return err
	}

	return DB.SetPagerank(ctx, rank)
}
