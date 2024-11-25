package crawler

import (
	"context"
	"fmt"
	"math"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

// ProcessFollowListEvents() process one event at the time from the eventChannel.
func ProcessFollowListEvents(
	ctx context.Context,
	eventChan chan nostr.RelayEvent,
	DB models.Database,
	RWM *walks.RandomWalkManager,
	newPubkeyHandler func(pk string) error) {

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\n  > Finishing processing the event... ")
			return

		case event, ok := <-eventChan:
			if !ok {
				fmt.Println("\n  > Event channel closed, stopping processing.")
				return
			}

			if err := ProcessFollowListEvent(ctx, event.Event, DB, RWM, newPubkeyHandler); err != nil {
				fmt.Printf("\nError processing the event: %v", err)

				// re add event to the queue
				select {
				case eventChan <- event:
				default:
					fmt.Printf("Channel is full, dropping eventID: %v\n", event.ID)
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

	if event == nil {
		return fmt.Errorf("event is nil")
	}

	// Fetch node metadata and successors of the author
	author, err := DB.NodeMetaWithID(event.PubKey)
	if err != nil {
		return err
	}
	oldSucc, err := DB.Successors(author.ID)
	if err != nil {
		return err
	}

	followPubkeys := ParsePubKeys(event.Tags)
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

	// TO DO, RECOMPUTE PAGERANK

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

	if err := RWM.Generate(DB, nodeID); err != nil {
		return math.MaxUint32, err
	}

	// if the pagerank is higher than the threshold, send to queue
	if pagerank > pagerankThreshold(DB.Size()) {
		if err := queueHandler(pubkey); err != nil {
			return math.MaxUint32, err
		}
	}

	return nodeID, nil
}

// ParsePubKeys returns the slice of pubkeys that are correctly listed in the nostr.Tags.
// Badly formatted tags are ignored.
func ParsePubKeys(tags nostr.Tags) []string {
	const followPrefix = "p"

	pubkeys := make([]string, 0, len(tags))
	for _, tag := range tags {

		if len(tag) < 2 {
			continue
		}

		if tag[0] != followPrefix {
			continue
		}

		if !nostr.IsValidPublicKey(tag[1]) {
			continue
		}

		pubkeys = append(pubkeys, tag[1])
	}

	return pubkeys
}

// InheritedPagerank() returns the expected pagerank that flows from a node with
// specified pagerank to its follows.
func InheritedPagerank(alpha float32, pagerank float64, outDegree int) float64 {
	return float64(alpha) * pagerank / float64(outDegree)
}
