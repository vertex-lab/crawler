package crawler

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

type ProcessEventsConfig struct {
	Log        *logger.Aggregate
	PrintEvery uint32
}

func NewProcessEventsConfig() ProcessEventsConfig {
	return ProcessEventsConfig{
		Log:        logger.New(os.Stdout),
		PrintEvery: 5000,
	}
}

func (c ProcessEventsConfig) Print() {
	fmt.Printf("Process\n")
	fmt.Printf("  PrintEvery: %d\n", c.PrintEvery)
}

// ProcessEvents() process one event at the time from the eventChannel, based on their kind.
func ProcessEvents(
	ctx context.Context,
	config ProcessEventsConfig,
	DB models.Database,
	RWS models.RandomWalkStore,
	eventChan <-chan *nostr.Event,
	eventCounter, walksChanged *atomic.Uint32) {

	var err error

	for {
		select {
		case <-ctx.Done():
			config.Log.Info("  > Finishing processing the event... ")
			return

		case event, ok := <-eventChan:
			if !ok {
				config.Log.Warn("Event queue closed, stopped processing.")
				return
			}

			if event == nil {
				config.Log.Warn("ProcessEvents: event is nil")
				continue
			}

			switch KindToRecordType(event.Kind) {
			case models.Follow:
				err = ProcessFollowList(DB, RWS, event, walksChanged)

			default:
				err = fmt.Errorf("unsupported event kind")
			}

			if err != nil {
				config.Log.Error("ProcessEvents kind %d: %v; eventID %s by %s", event.Kind, err, event.ID, event.PubKey)
			}

			count := eventCounter.Add(1)
			if count%config.PrintEvery == 0 {
				config.Log.Info("processed %d events", count)
			}
		}
	}
}

// ProcessFollowList() updates the follow relationships for the event's author in the database.
// If the author is active, new follows are added to the database as inactive nodes.
func ProcessFollowList(
	DB models.Database,
	RWS models.RandomWalkStore,
	event *nostr.Event,
	walksChanged *atomic.Uint32) error {

	// use a new context for the operation to avoid it being interrupted
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	author, err := DB.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return fmt.Errorf("failed to fetch node by key %v: %w", event.PubKey, err)
	}

	if IsEventOutdated(author, event) {
		return nil
	}

	pubkeys := ParsePubkeys(event)
	newFollows, err := resolveIDs(ctx, DB, pubkeys, author.Status)
	if err != nil {
		return fmt.Errorf("resolveIDs: %w", err)
	}

	follows, err := DB.Follows(ctx, author.ID)
	if err != nil {
		return fmt.Errorf("failed to fetch the old follows of %d: %w", author.ID, err)
	}

	removed, common, added := sliceutils.Partition(follows[0], newFollows)
	delta := &models.Delta{
		Record:  models.Record{ID: event.ID, Timestamp: event.CreatedAt.Time().Unix(), Type: models.Follow},
		NodeID:  author.ID,
		Added:   added,
		Removed: removed,
	}

	if err := DB.Update(ctx, delta); err != nil {
		return fmt.Errorf("failed to update nodeID %d: %w", author.ID, err)
	}

	updated, err := walks.Update(ctx, DB, RWS, author.ID, removed, common, added)
	if err != nil {
		return fmt.Errorf("failed to update the walks of nodeID %d: %w", author.ID, err)
	}

	walksChanged.Add(uint32(updated)) // this counter triggers the activation of NodeArbiter
	return nil
}

// resolveIDs() returns an ID for each pubkey. If the authorStatus is active and
// a pubkey is not found (ID = nil), a new node is added with that pubkey.
func resolveIDs(
	ctx context.Context,
	DB models.Database,
	pubkeys []string,
	authorStatus string) ([]uint32, error) {

	if len(pubkeys) == 0 {
		return nil, nil
	}

	IDs, err := DB.NodeIDs(ctx, pubkeys...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the IDs: %w", err)
	}

	newFollows := make([]uint32, 0, len(IDs))
	switch authorStatus {
	case models.StatusActive:
		for i, ID := range IDs {
			// if the pubkey is not in the DB (ID=nil), it gets added as a new node
			if ID == nil {
				if !nostr.IsValidPublicKey(pubkeys[i]) {
					continue
				}

				newID, err := DB.AddNode(ctx, pubkeys[i])
				if err != nil {
					return nil, fmt.Errorf("failed to add %s: %w", pubkeys[i], err)
				}

				ID = &newID
			}

			newFollows = append(newFollows, *ID)
		}

	case models.StatusInactive:
		// if the pubkey is not in the DB (ID=nil), it DOESN'T get added as a new node
		for _, ID := range IDs {
			if ID != nil {
				newFollows = append(newFollows, *ID)
			}
		}

	default:
		return nil, fmt.Errorf("unknown status: %s", authorStatus)
	}

	return newFollows, nil
}

// ParsePubkeys() returns the slice of pubkeys that are correctly listed in the nostr.Tags.
// - Badly formatted tags are ignored.
// - Pubkeys will be uniquely added (no repetitions).
// - The author of the event will be removed from the followed pubkeys if present.
// - NO CHECKING the validity of the pubkeys
func ParsePubkeys(event *nostr.Event) []string {
	const followPrefix = "p"

	// if it's empty or very big, skip
	if event == nil || len(event.Tags) == 0 || len(event.Tags) > 100000 {
		return nil
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

		pubkeys = append(pubkeys, pubkey)
	}

	return sliceutils.Unique(pubkeys)
}
