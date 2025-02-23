package crawler

import (
	"context"
	"encoding/json"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
	"github.com/vertex-lab/relay/pkg/eventstore"
	"golang.org/x/image/draw"
)

type ProcessEventsConfig struct {
	Log        *logger.Aggregate
	PrintEvery uint32

	ImagesURL string // the path to the directory where kind:0 'picture' and 'banner' are stored
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
	fmt.Printf("  ImagesURL: %s\n", c.ImagesURL)
}

// ProcessEvents() process one event at the time from the eventChannel, based on their kind.
func ProcessEvents(
	ctx context.Context,
	config ProcessEventsConfig,
	DB models.Database,
	RWS models.RandomWalkStore,
	eventStore *eventstore.Store,
	eventChan <-chan *nostr.Event,
	eventCounter, walksTracker *atomic.Uint32) {

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

			switch event.Kind {
			case nostr.KindFollowList:
				err = HandleFollowList(DB, RWS, eventStore, event, walksTracker)

			case nostr.KindProfileMetadata:
				err = HandleProfileMetadata(eventStore, event, config.ImagesURL)

			default:
				err = fmt.Errorf("unsupported event kind")
			}

			if err != nil {
				config.Log.Error("ProcessEvents: eventID %s, kind %d by %s: %v", event.ID, event.Kind, event.PubKey, err)
			}

			count := eventCounter.Add(1)
			if count%config.PrintEvery == 0 {
				config.Log.Info("processed %d events", count)
			}
		}
	}
}

func HandleProfileMetadata(eventStore *eventstore.Store, event *nostr.Event, imagesDir string) error {
	// use a new context for the operation to avoid it being interrupted
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var oldContent string
	row := eventStore.DB.QueryRowContext(ctx, "SELECT content FROM events WHERE kind = ? pubkey = ?", event.Kind, event.PubKey)
	err := row.Scan(&oldContent)
	if err != nil {
		return fmt.Errorf("failed to query for the content of the old event: %w", err)
	}

	saved, err := eventStore.Replace(ctx, event)
	if err != nil {
		return err
	}

	if !saved {
		return nil
	}

	oldURL := extractPictureURL(oldContent)
	newURL := extractPictureURL(event.Content)

	if newURL != "" && newURL != oldURL {
		img, _, err := downloadImage(newURL)
		if err != nil {
			return err
		}

		img1 := scaleImage(img, draw.BiLinear, 300)
		path1 := imagesDir + "picture_300_" + event.PubKey + "_" + strconv.FormatInt(event.CreatedAt.Time().Unix(), 10) + ".jpeg"
		if err := saveImage(img1, path1); err != nil {
			return err
		}

		img2 := scaleImage(img, draw.BiLinear, 30)
		path2 := imagesDir + "picture_30_" + event.PubKey + "_" + strconv.FormatInt(event.CreatedAt.Time().Unix(), 10) + ".jpeg"
		if err := saveImage(img2, path2); err != nil {
			return err
		}
	}

	return nil
}

// This function extracts the URL specified in the 'picture' field. In case of errors or missing field, returns the empty string "".
func extractPictureURL(content string) string {
	if len(content) == 0 {
		return ""
	}

	var img map[string]string
	if err := json.Unmarshal([]byte(content), &img); err != nil {
		return ""
	}

	return img["picture"]
}

func downloadImage(URL string) (img image.Image, format string, err error) {
	res, err := http.Get(URL)
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch image %s: %w", URL, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("failed to download image %s, status: %s", URL, res.Status)
	}

	img, format, err = image.Decode(res.Body)
	if err != nil {
		return nil, "", fmt.Errorf("failed to decode image: %w", err)
	}

	return img, format, nil
}

// ScaleImage() returns a rescaled image with the same aspect ratio using the specified scaler.
func scaleImage(img image.Image, scaler draw.Scaler, width int) image.Image {
	scaleFactor := float64(width) / float64(img.Bounds().Dx())
	height := int(float64(img.Bounds().Dy())*scaleFactor + 0.5)
	scaled := image.NewRGBA(image.Rect(0, 0, width, height))
	scaler.Scale(scaled, scaled.Bounds(), img, img.Bounds(), draw.Over, nil)
	return scaled
}

func saveImage(img image.Image, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", path, err)
	}
	defer file.Close()

	if err := jpeg.Encode(file, img, nil); err != nil {
		return fmt.Errorf("failed to save image to %s: %w", path, err)
	}

	return nil
}

// HandleFollowList() saves the event to the eventStore, replacing an older event
// if present, and then process the follow-list.
func HandleFollowList(
	DB models.Database,
	RWS models.RandomWalkStore,
	eventStore *eventstore.Store,
	event *nostr.Event,
	walksTracker *atomic.Uint32) error {

	// use a new context for the operation to avoid it being interrupted
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stored, err := eventStore.Replace(ctx, event)
	if err != nil {
		return err
	}

	if stored {
		walksChanged, err := processFollowList(ctx, DB, RWS, event)
		if err != nil {
			return fmt.Errorf("failed to process follow-list: %w", err)
		}

		walksTracker.Add(uint32(walksChanged))
	}

	return nil
}

// processFollowList() updates the follow relationships for the event's author in the database, as well as the random walks.
// Only if the author is active, new follows are added to the database as inactive nodes.
// It returns the number of walks that have been updated.
func processFollowList(
	ctx context.Context,
	DB models.Database,
	RWS models.RandomWalkStore,
	event *nostr.Event) (int, error) {

	author, err := DB.NodeByKey(ctx, event.PubKey)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch node by key %v: %w", event.PubKey, err)
	}

	pubkeys := ParsePubkeys(event)
	newFollows, err := resolveIDs(ctx, DB, pubkeys, author.Status)
	if err != nil {
		return 0, fmt.Errorf("resolveIDs: %w", err)
	}

	follows, err := DB.Follows(ctx, author.ID)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch the old follows of %d: %w", author.ID, err)
	}

	removed, common, added := sliceutils.Partition(follows[0], newFollows)
	delta := &models.Delta{
		Record:  models.Record{ID: event.ID, Timestamp: event.CreatedAt.Time().Unix(), Kind: event.Kind},
		NodeID:  author.ID,
		Added:   added,
		Removed: removed,
	}

	if err := DB.Update(ctx, delta); err != nil {
		return 0, fmt.Errorf("failed to update nodeID %d: %w", author.ID, err)
	}

	return walks.Update(ctx, DB, RWS, author.ID, removed, common, added)
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
