package crawler

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/nbd-wtf/go-nostr"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/vertex-lab/crawler/pkg/logger"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

// ProcessFollowListEvents() process one event at the time from the eventChannel.
func ProcessFollowListEvents(
	ctx context.Context,
	logger *logger.Aggregate,
	eventChan chan nostr.RelayEvent,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	eventCounter *xsync.Counter,
	newPubkeyHandler func(pk string) error) {

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

			if event.Kind != nostr.KindFollowList {
				logger.Warn("event kind is not %d", nostr.KindFollowList)
				continue
			}

			eventCounter.Inc()
			if eventCounter.Value()%1000 == 0 {
				logger.Info("processed %d events", eventCounter.Value())
			}

			if err := ProcessFollowListEvent(ctx, event.Event, DB, RWM, newPubkeyHandler); err != nil {
				logger.Error("Error processing the eventID %v: %v", event.ID, err)

				// re add event to the queue
				select {
				case eventChan <- event:
				default:
					logger.Warn("Channel is full, dropping eventID: %v by %v", event.ID, event.PubKey)
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
	RWM *walks.RandomWalkManager,
	newPubkeyHandler func(pk string) error) error {

	// Fetch node metadata and successors of the author
	author, err := DB.NodeMetaWithID(event.PubKey)
	if err != nil {
		return err
	}
	oldSucc, err := DB.Successors(author.ID)
	if err != nil {
		return err
	}

	followPubkeys := ParsePubkeys(event)
	newSucc, err := ProcessNodeIDs(ctx, DB, RWM, author, followPubkeys, newPubkeyHandler)
	if err != nil {
		return err
	}

	removedSucc, _, addedSucc := sliceutils.Partition(oldSucc, newSucc)

	// update the author's node in the database
	authorNodeDiff := models.NodeDiff{
		Metadata: models.NodeMeta{
			Timestamp: event.CreatedAt.Time().Unix(),
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

// ProcessNodeIDs() returns the nodeIDs of the specified pubkeys.
// If a pubkey isn't found in the database:
// - the corrisponding node is added to the DB
// - the pubkey is sent to the newPubkeysQueue
func ProcessNodeIDs(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	author models.NodeMetaWithID,
	pubkeys []string,
	newPubkeyHandler func(pk string) error) ([]uint32, error) {

	// Each pubkey will inherit this pagerank from the event's author.
	pr := InheritedPagerank(RWM.Store.Alpha(), author.Pagerank, len(pubkeys))

	IDs, err := DB.NodeIDs(pubkeys)
	if err != nil {
		return nil, err
	}

	nodeIDs := make([]uint32, len(IDs))
	for i, ID := range IDs {

		nodeID, ok := ID.(uint32)
		// if it's not uin32, it means the pubkey wasn't found in the database
		if !ok {
			nodeID, err = HandleMissingPubkey(ctx, DB, RWM, pubkeys[i], pr, newPubkeyHandler)
			if err != nil {
				return nil, err
			}
		}
		nodeIDs[i] = nodeID
	}

	return nodeIDs, nil
}

// HandleMissingPubkey() adds a new node to the database,
// If the expected pagerank is higher than the threshold:
// - generates walks for it,
// - sends the pubkey to the queue
func HandleMissingPubkey(
	ctx context.Context,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	pubkey string,
	pagerank float64,
	queueHandler func(pk string) error) (uint32, error) {

	// add a new node to the database, and assign it an ID
	node := models.Node{
		Metadata: models.NodeMeta{
			PubKey:    pubkey,
			Timestamp: 0,
			Status:    models.StatusInactive,
			Pagerank:  0.0,
		},
	}

	nodeID, err := DB.AddNode(&node)
	if err != nil {
		return math.MaxUint32, err
	}

	// if the iherited pagerank is higher than the threshold, generate walks and send to the queue.
	if pagerank > pagerankThreshold(DB.Size()) {

		if err := RWM.Generate(DB, nodeID); err != nil {
			return math.MaxUint32, err
		}

		if err := queueHandler(pubkey); err != nil {
			return math.MaxUint32, err
		}
	}

	return nodeID, nil
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

	// if it's massively big, skip
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

// InheritedPagerank() returns the expected pagerank that flows from a node with
// specified pagerank to its follows.
func InheritedPagerank(alpha float32, pagerank float64, outDegree int) float64 {
	return float64(alpha) * pagerank / float64(outDegree)
}

// NodeArbiter() scans through all the nodes in the database, and promotes or
// demotes them based on their pagerank.
func NodeArbiter(
	ctx context.Context,
	logger logger.Aggregate,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	queueHandler func(pk string) error) {

	var threshold float64 = pagerankThreshold(DB.Size())

	// Scan all nodes
	cursor := uint64(0)
	for {
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

				if err := queueHandler(node.PubKey); err != nil {
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
