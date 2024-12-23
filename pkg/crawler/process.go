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
	"github.com/vertex-lab/crawler/pkg/utils/logger"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

// ProcessEvents() process one event at the time from the eventChannel, based on their kind.
func ProcessEvents(
	ctx context.Context,
	logger *logger.Aggregate,
	eventChan chan nostr.RelayEvent,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	eventCounter *xsync.Counter) {

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

			if event.Event == nil {
				logger.Warn("event is nil")
				continue
			}

			eventCounter.Inc()
			if eventCounter.Value()%1000 == 0 {
				logger.Info("processed %d events", eventCounter.Value())
			}

			// process event based on its kind
			switch event.Kind {
			case nostr.KindFollowList:

				if err := ProcessFollowListEvent(ctx, DB, RWM, event.Event); err != nil {
					logger.Error("Error processing the eventID %v: %v", event.ID, err)

					// re-add event to the queue
					select {
					case eventChan <- event:
					default:
						logger.Warn("Channel is full, dropping eventID: %v by %v", event.ID, event.PubKey)
					}
				}
			default:
				logger.Warn("event of unwanted kind: %v", event.Kind)
			}
		}
	}
}

// The accumulated pagerank mass since the last full recomputation of pagerank
var mass float64

/*
ProcessFollowListEvent() adds the author and its follows to the database.
It updates the node metadata of the author, and updates the random walks.
*/
func ProcessFollowListEvent(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	event *nostr.Event) error {

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	author, err := DB.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return err
	}

	if event.CreatedAt.Time().Unix() < author.EventTS {
		return nil
	}

	// parse pubkeys and fetch/assign nodeIDs
	followPubkeys := ParsePubkeys(event)
	newFollows, err := AssignNodeIDs(ctx, DB, followPubkeys)
	if err != nil {
		return err
	}

	oldFollows, err := DB.Follows(ctx, author.ID)
	if err != nil {
		return err
	}

	removed, common, added := sliceutils.Partition(oldFollows[0], newFollows)

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

	var pagerankMap models.PagerankMap
	mass += author.Pagerank

	if mass > 0.001 {
		// full recomputation of pagerank
		nodeIDs, err := DB.AllNodes(ctx)
		if err != nil {
			return err
		}

		pagerankMap, err = pagerank.Global(ctx, RWM.Store, nodeIDs...)
		if err != nil {
			return err
		}
		mass = 0

	} else {
		// lazy recomputation of pagerank. Update the scores of the most impacted nodes only
		impactedNodes := append(added, removed...)
		pagerankMap, err = pagerank.Global(ctx, RWM.Store, impactedNodes...)
		if err != nil {
			return err
		}
	}

	if err := DB.SetPagerank(ctx, pagerankMap); err != nil {
		return err
	}

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
		// if it's nil, the pubkey wasn't found in the database
		if ID == nil {
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

// NodeArbiter() periodically scans through all the nodes in the database,
// promoting or demoting them based on their pagerank.
func NodeArbiter(
	ctx context.Context,
	logger *logger.Aggregate,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	sleepTime int,
	queueHandler func(pk string) error) {

	counter := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\n  > Stopping the Node Arbiter... ")
			return
		default:
			// If the node is not found (err != nil), skip
			time.Sleep(time.Duration(sleepTime) * time.Second)

			//threshold := pagerankThreshold(DB.Size(), bottom)
			threshold := 0.0
			if err := ArbiterScan(ctx, DB, RWM, threshold, queueHandler); err != nil {
				logger.Error("NodeArbiter error: %v", err)
			}

			counter++
			logger.Info("NodeArbiter completed scan: %d", counter)
		}
	}
}

// ArbiterScan() performs one entire database scan, promoting or demoting nodes
// based on their pagerank.
func ArbiterScan(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	threshold float64,
	queueHandler func(pk string) error) error {

	ctx, cancel := context.WithTimeout(ctx, time.Second*100)
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

// PromoteNode() makes a node active, which means it generates random walks for it and updates the DB.
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
			Status: "active",
		},
	}

	if err := DB.UpdateNode(ctx, nodeID, &nodeDiff); err != nil {
		return fmt.Errorf("UpdateNode(): %v", err)
	}

	return nil
}

// DemoteNode() sets a node to "inactive". In the future this function will also
// remove the walks that starts from that node, thus removing its influence on the pagerank.
func DemoteNode(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	nodeID uint32) error {

	if err := RWM.Remove(ctx, nodeID); err != nil {
		return err
	}

	nodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			Status: "inactive",
		},
	}

	if err := DB.UpdateNode(ctx, nodeID, &nodeDiff); err != nil {
		return err
	}

	return nil
}
