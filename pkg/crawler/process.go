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

			switch KindToRecordType(event.Kind) {
			case models.Follow:
				if err := ProcessFollowList(DB, RWM, event, walksChanged); err != nil {
					logger.Error("Error processing follow list with eventID %v: %v", event.ID, err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	author, err := DB.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return fmt.Errorf("failed to fetch node by key %v: %w", event.PubKey, err)
	}

	if event.CreatedAt.Time().Unix() <= LatestEventTimestamp(author, event.Kind) {
		return nil
	}

	pubkeys := ParsePubkeys(event)
	newFollows, err := AssignNodeIDs(ctx, DB, pubkeys)
	if err != nil {
		return fmt.Errorf("failed to assign node IDs to the follows of %s: %w", event.PubKey, err)
	}

	followsByNode, err := DB.Follows(ctx, author.ID)
	if err != nil {
		return fmt.Errorf("failed to fetch the old follows of %d: %w", author.ID, err)
	}
	oldFollows := followsByNode[0]

	removed, common, added := sliceutils.Partition(oldFollows, newFollows)
	delta := &models.Delta{
		Record:  models.Record{ID: event.ID, Timestamp: event.CreatedAt.Time().Unix(), Type: models.Follow},
		NodeID:  author.ID,
		Added:   added,
		Removed: removed,
	}

	if err := DB.Update(ctx, delta); err != nil {
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
			nodeID, err := DB.AddNode(ctx, pubkeys[i])
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
