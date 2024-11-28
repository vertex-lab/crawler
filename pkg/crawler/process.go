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
			Status:    models.StatusCrawled,
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

	IDs, err := DB.NodeIDs(pubkeys) // IDs can be uint32 or nil, if the pubkey is not found in the database
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

// HandleMissingPubkey() adds a new node to the database, generates walks for it,
// and sends the pubkey to the queue if the expected pagerank is higher than the threshold.
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
			Status:    models.StatusNotCrawled,
			Pagerank:  0.0,
		},
	}

	nodeID, err := DB.AddNode(&node)
	if err != nil {
		return math.MaxUint32, err
	}

	// if the pagerank is higher than the threshold, generate walks and send to the queue.
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
