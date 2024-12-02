package crawler

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/vertex-lab/crawler/pkg/logger"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

// ProcessEvents() process one event at the time from the eventChannel.
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
				if err := ProcessFollowListEvent(ctx, event.Event, DB, RWM); err != nil {
					logger.Error("Error processing the eventID %v: %v", event.ID, err)

					// re-add event to the queue
					select {
					case eventChan <- event:
					default:
						logger.Warn("Channel is full, dropping eventID: %v by %v", event.ID, event.PubKey)
					}
				}
			}
		}
	}
}

/*
ProcessFollowListEvent() adds the author and its follows to the database.
It updates the node metadata of the author, and updates the random walks.
*/
func ProcessFollowListEvent(
	ctx context.Context,
	event *nostr.Event,
	DB models.Database,
	RWM *walks.RandomWalkManager) error {

	author, err := DB.NodeByKey(event.PubKey)
	if err != nil {
		return err
	}

	// if the event is older than what we have, stop
	if event.CreatedAt.Time().Unix() < author.EventTS {
		return nil
	}

	// parse pubkeys and fetch/assign nodeIDs
	followPubkeys := ParsePubkeys(event)
	newSucc, err := AssignNodeIDs(ctx, DB, followPubkeys)
	if err != nil {
		return err
	}

	oldSucc, err := DB.Successors(author.ID)
	if err != nil {
		return err
	}

	removedSucc, _, addedSucc := sliceutils.Partition(oldSucc, newSucc)

	// update the author's node in the database
	authorNodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			EventTS: event.CreatedAt.Time().Unix(),
		},
		AddedSucc:   addedSucc,
		RemovedSucc: removedSucc,
	}
	if err := DB.UpdateNode(author.ID, &authorNodeDiff); err != nil {
		return err
	}

	// update the random walks
	if err := RWM.Update(DB, author.ID, oldSucc, newSucc); err != nil {
		return err
	}

	// recompute pagerank and update the DB
	pagerank, err := pagerank.Pagerank(DB, RWM.Store)
	if err != nil {
		return err
	}
	if err := DB.SetPagerank(pagerank); err != nil {
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

	IDs, err := DB.NodeIDs(pubkeys)
	if err != nil {
		return []uint32{}, err
	}

	nodeIDs := make([]uint32, len(IDs))
	for i, ID := range IDs {

		nodeID, ok := ID.(uint32)
		// if it's not uin32, it means the pubkey wasn't found in the database
		if !ok {

			// add a new node to the database, and assign it an ID
			node := models.Node{
				Metadata: models.NodeMeta{
					Pubkey:   pubkeys[i],
					EventTS:  0,
					Status:   models.StatusInactive,
					Pagerank: 0.0,
				},
			}
			nodeID, err = DB.AddNode(&node)
			if err != nil {
				return []uint32{}, err
			}
		}
		nodeIDs[i] = nodeID
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

// NodeArbiter() scans through all the nodes in the database, and promotes or
// demotes them based on their pagerank.
func NodeArbiter(
	ctx context.Context,
	logger *logger.Aggregate,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	queueHandler func(pk string) error) {

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\n  > Stopping the Node Arbiter... ")
			return
		default:
			// 30s delay before the next complete scan
			time.Sleep(time.Second * 30)

			if err := ScanNodes(ctx, DB, RWM, queueHandler); err != nil {
				logger.Error("Error in NodeArbiter: %v", err)
			}
		}
	}
}

func ScanNodes(ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	queueHandler func(pk string) error) error {

	var threshold float64 = pagerankThreshold(DB.Size())
	var cursor uint64 = 0

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		nodeIDs, cursor, err := DB.ScanNodes(cursor, 1000)
		if err != nil {
			return fmt.Errorf("error scanning: %v", err)
		}

		// If the cursor returns to 0, the scan is complete
		if cursor == 0 {
			break
		}

		for _, nodeID := range nodeIDs {
			node, err := DB.NodeByID(nodeID)
			if err != nil {
				continue
			}

			// Active --> Inactive
			if node.Status == models.StatusActive && node.Pagerank < threshold {
				if err := DemoteNode(ctx, DB, RWM, nodeID); err != nil {
					return fmt.Errorf("error demoting nodeID %d: %v", nodeID, err)
				}
			}

			// Inactive --> Active
			if node.Status == models.StatusInactive && node.Pagerank > threshold {
				if err := PromoteNode(ctx, DB, RWM, nodeID); err != nil {
					return fmt.Errorf("error promoting nodeID %d: %v", nodeID, err)
				}

				if err := queueHandler(node.Pubkey); err != nil {
					return fmt.Errorf("error sending pubkey %v to the queue: %v", node.Pubkey, err)
				}
			}
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

	_ = RWM
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
