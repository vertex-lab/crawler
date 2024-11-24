package crawler

import (
	"fmt"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

var (
	RelevantKinds = []int{
		nostr.KindFollowList,
	}
)

/*
ProcessFollowListEvent is responsible for:
- writing to the Database
- updating the RandomWalkStore
- re-computing pagerank and updating the NodeCache
*/
func ProcessFollowListEvent(DB models.Database, RWM *walks.RandomWalkManager,
	NC models.NodeCache, event *nostr.Event) error {

	if err := checkInputs(DB, RWM, NC); err != nil {
		return err
	}

	author, exists := NC.Load(event.PubKey)
	if !exists {
		return models.ErrNodeNotFoundNC
	}

	followPubkeys := ParsePubkeys(event.Tags)
	newSucc, err := ProcessNodeIDs(DB, followPubkeys)
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

	return nil
}

// ProcessNodeIDs() returns the nodeIDs of the specified pubkeys.
// If a pubkey isn't found in the database, the corrisponding node is added.
func ProcessNodeIDs(DB models.Database, pubkeys []string) ([]uint32, error) {

	InterfaceNodeIDs, err := DB.NodeIDs(pubkeys)
	if err != nil {
		return nil, err
	}

	nodeIDs := make([]uint32, len(InterfaceNodeIDs))
	for i, InterfaceNodeID := range InterfaceNodeIDs {

		nodeID, ok := InterfaceNodeID.(uint32)
		// if it's not uin32, it means the pubkey wasn't found in the database
		// so we add a new node to the database
		if !ok {
			node := models.Node{
				Metadata: models.NodeMeta{
					PubKey:    pubkeys[i],
					Timestamp: 0,
					Status:    models.StatusNotCrawled,
					Pagerank:  0.0,
				},
			}

			// add the node to the database, and assign it an ID
			nodeID, err = DB.AddNode(&node)
			if err != nil {
				return nil, err
			}
		}

		nodeIDs[i] = nodeID
	}

	return nodeIDs, nil
}

// ParsePubkeys returns the slice of pubkeys that are correctly listed in the nostr.Tags.
// Badly formatted tags are ignored.
func ParsePubkeys(tags nostr.Tags) []string {
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

// checkInputs() checks the DB, RWM and NC, returning the appropriate error.
func checkInputs(DB models.Database, RWM *walks.RandomWalkManager,
	NC models.NodeCache) error {

	if err := DB.Validate(); err != nil {
		return err
	}

	if err := RWM.Store.Validate(false); err != nil {
		return err
	}

	if NC == nil {
		return models.ErrNilNCPointer
	}

	return nil
}

// PrintEvent is a simple function that prints the event ID and PubKey.
func PrintEvent(event nostr.RelayEvent) error {
	fmt.Printf("\nevent ID: %v", event.ID)
	fmt.Printf("\nevent pubkey: %v\n", event.PubKey)
	return nil
}
