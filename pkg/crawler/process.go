package crawler

import (
	"fmt"
	"math"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

/*
ProcessFollowListEvent() adds to the database the author and its follows to the database.
It updates the node metadata of the author, and updates the random walks.
*/
func ProcessFollowListEvent(DB models.Database, RWM *walks.RandomWalkManager,
	event *nostr.Event, newPubkeyHandler func(pk string) error) error {

	if event == nil {
		return fmt.Errorf("nostr event is nil")
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
	pagerank := expectedPagerank(RWM.Store.Alpha(), author.Pagerank, len(followPubkeys))
	newSucc, err := ProcessNodeIDs(DB, followPubkeys, pagerank, newPubkeyHandler)
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
// If a pubkey isn't found in the database:
// - the corrisponding node is added to the DB
// - the pubkey is sent to the newPubkeysQueue
func ProcessNodeIDs(DB models.Database, pubkeys []string, pagerank float64,
	newPubkeyHandler func(pk string) error) ([]uint32, error) {

	interfaceNodeIDs, err := DB.NodeIDs(pubkeys)
	if err != nil {
		return nil, err
	}

	nodeIDs := make([]uint32, len(interfaceNodeIDs))
	for i, interfaceNodeID := range interfaceNodeIDs {

		nodeID, ok := interfaceNodeID.(uint32)
		// if it's not uin32, it means the pubkey wasn't found in the database
		if !ok {
			nodeID, err = HandleMissingPubkey(DB, pubkeys[i], pagerank, newPubkeyHandler)
			if err != nil {
				return nil, err
			}
		}
		nodeIDs[i] = nodeID
	}

	return nodeIDs, nil
}

// HandleMissingPubkey() adds a new node to the database, and sends
// the pubkey to the queue if the expected pagerank is higher than the threshold.
func HandleMissingPubkey(DB models.Database, pubkey string,
	pagerank float64, queueHandler func(pk string) error) (uint32, error) {

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

// expectedPagerank() returns the expected pagerank that flows from a node with
// specified pagerank to its follows.
func expectedPagerank(alpha float32, pagerank float64, outDegree int) float64 {
	return float64(alpha) * pagerank / float64(outDegree)
}
