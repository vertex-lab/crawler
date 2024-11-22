package crawler

import (
	"fmt"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
)

var (
	followPrefix  = "p"
	RelevantKinds = []int{nostr.KindFollowList}
)

/*
ProcessFollowListEvent is responsible for:
- writing to the Database
- updating the RandomWalkStore
- re-computing pagerank and updating the NodeCache
*/
func ProcessFollowListEvent(DB models.Database, RWS models.RandomWalkStore,
	NC models.NodeCache, event *nostr.Event) error {

	author, exists := NC.Load(event.PubKey)
	if !exists {
		return models.ErrNodeNotFoundDB
	}

	followPubkeys := ParseFollowList(event.Tags)
	newSucc, err := ProcessFollows(DB, followPubkeys, author.ID)
	if err != nil {
		return err
	}

	oldSucc, err := DB.Successors(author.ID)
	if err != nil {
		return err
	}

	// if the successors are actually different, modify the DB
	if !sliceutils.EqualElements(newSucc, oldSucc) {

		authorNode := models.Node{
			Metadata: models.NodeMeta{
				PubKey:    event.PubKey,
				Timestamp: event.CreatedAt.Time().Unix(),
				Status:    models.StatusCrawled,
			},
		}

		_ = authorNode
	}

	return nil
}

// ProcessFollows() returns the nodeIDs of the specified followPubkeys.
// If a pubkey isn't found in the database, the corrisponding node is added.
func ProcessFollows(DB models.Database, followPubkeys []string, nodeID uint32) ([]uint32, error) {

	if len(followPubkeys) == 0 {
		return []uint32{}, nil
	}

	// get the nodeIDs of the followPubkeys
	newSucc, err := DB.NodeIDs(followPubkeys)
	if err != nil {
		return nil, err
	}

	succIDs := make([]uint32, len(newSucc))
	for i, succ := range newSucc {

		succID, ok := succ.(uint32)
		// if it's not uin32, it means the pubkey wasn't found in the database
		// so we add a new node to the database
		if !ok {
			node := models.Node{
				Metadata: models.NodeMeta{
					PubKey:    followPubkeys[i],
					Timestamp: 0,
					Status:    models.StatusNotCrawled,
					Pagerank:  0.0,
				},
				Successors:   nil,
				Predecessors: []uint32{nodeID},
			}

			// add the node to the database, and assign it an ID
			succID, err = DB.AddNode(&node)
			if err != nil {
				return nil, err
			}
		}

		succIDs[i] = succID
	}

	return succIDs, nil
}

// ParseFollowList returns the slice of followPubkeys that are correctly listed in the nostr.Tags.
// Badly formatted tags are ignored.
func ParseFollowList(tags nostr.Tags) []string {
	followPubkeys := make([]string, 0, len(tags))
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

		followPubkeys = append(followPubkeys, tag[1])
	}

	return followPubkeys
}

// PrintEvent is a simple function that gets passed to the Firehose for testing and debugging.
// It prints the event ID and PubKey.
func PrintEvent(event nostr.RelayEvent) error {
	fmt.Printf("\nevent ID: %v", event.ID)
	fmt.Printf("\nevent pubkey: %v\n", event.PubKey)
	return nil
}
