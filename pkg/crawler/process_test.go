package crawler

import (
	"context"
	"errors"
	"fmt"
	"image/jpeg"
	"os"
	"reflect"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"golang.org/x/image/draw"
)

const odell = "04c915daefee38317fa734444acee390a8269fe5810b2241e5e6dd343dfbecc9"
const calle = "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"
const pip = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
const gigi = "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93"

func TestParsePubkeys(t *testing.T) {
	testCases := []struct {
		name            string
		event           *nostr.Event
		expectedPubkeys []string
	}{
		{
			name:  "nil tags",
			event: nil,
		},
		{
			name:  "nil event",
			event: nil,
		},
		{
			name:  "empty tags",
			event: &nostr.Event{Tags: nostr.Tags{}},
		},
		{
			name: "badly formatted tags",
			event: &nostr.Event{
				PubKey:    odell,
				Kind:      3,
				CreatedAt: nostr.Timestamp(1713083262),
				Tags: nostr.Tags{
					nostr.Tag{"p", gigi},
					nostr.Tag{"e", calle}, // not a p tag
				}},
			expectedPubkeys: []string{gigi},
		},
		{
			name: "multiple follow tags",
			event: &nostr.Event{
				PubKey:    odell,
				Kind:      3,
				CreatedAt: nostr.Timestamp(11),
				Tags: nostr.Tags{
					nostr.Tag{"p", pip},
					nostr.Tag{"p", pip}}, // added two times
			},
			expectedPubkeys: []string{pip},
		},
		{
			name: "auto follow tag",
			event: &nostr.Event{
				PubKey:    odell,
				Kind:      3,
				CreatedAt: nostr.Timestamp(11),
				Tags: nostr.Tags{
					nostr.Tag{"p", odell}, // autofollow event
					nostr.Tag{"p", pip}},
			},
			expectedPubkeys: []string{pip},
		},
		{
			name: "valid",
			event: &nostr.Event{
				PubKey:    calle,
				Kind:      3,
				CreatedAt: nostr.Timestamp(11),
				Tags: nostr.Tags{
					nostr.Tag{"p", gigi},
					nostr.Tag{"p", odell}},
			},
			expectedPubkeys: []string{odell, gigi},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pubkeys := ParsePubkeys(test.event)
			if !reflect.DeepEqual(pubkeys, test.expectedPubkeys) {
				t.Fatalf("ParsePubkeys(): expected %v, got %v", test.expectedPubkeys, pubkeys)
			}
		})
	}
}

func TestResolveIDs(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		status        string
		pubkeys       []string
		expectedError error
		expectedIDs   []uint32
	}{
		{
			name:   "nil pks",
			DBType: "simple-with-pks",
			status: models.StatusActive,
		},
		{
			name:    "empty pks",
			DBType:  "simple-with-pks",
			status:  models.StatusActive,
			pubkeys: []string{},
		},
		{
			name:        "existing pks (active)",
			DBType:      "simple-with-pks",
			status:      models.StatusActive,
			pubkeys:     []string{odell, calle},
			expectedIDs: []uint32{0, 1},
		},
		{
			name:        "existing pks (inactive)",
			DBType:      "simple-with-pks",
			status:      models.StatusInactive,
			pubkeys:     []string{odell, calle},
			expectedIDs: []uint32{0, 1},
		},
		{
			name:        "existing and new pks (active)",
			DBType:      "simple-with-pks",
			status:      models.StatusActive,
			pubkeys:     []string{odell, calle, gigi},
			expectedIDs: []uint32{0, 1, 3},
		},
		{
			name:        "existing and new INVALID pks (active)",
			DBType:      "simple-with-pks",
			status:      models.StatusActive,
			pubkeys:     []string{odell, calle, "6e468422dfb74a5738702a8823b9b28168abab8655faacb6953cd0ee15deee93"},
			expectedIDs: []uint32{0, 1},
		},
		{
			name:        "existing and new pks (inactive)",
			DBType:      "simple-with-pks",
			status:      models.StatusInactive,
			pubkeys:     []string{odell, calle, gigi},
			expectedIDs: []uint32{0, 1},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			DB := mockdb.SetupDB(test.DBType)
			IDs, err := resolveIDs(ctx, DB, test.pubkeys, test.status)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("resolveIDs(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(IDs, test.expectedIDs) {
				t.Errorf("resolveIDs(): expected %v, got %v", test.expectedIDs, IDs)
			}
		})
	}
}

func TestProcessFollowList(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		RWSType       string
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			RWSType:       "one-node0",
			expectedError: models.ErrNilDB,
		},
		{
			name:          "event.PubKey not found",
			DBType:        "one-node0",
			RWSType:       "one-node0",
			expectedError: models.ErrNodeNotFoundDB,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			DB := mockdb.SetupDB(test.DBType)
			RWS := mockstore.SetupRWS(test.RWSType)
			event := &nostr.Event{
				PubKey:    calle,
				Kind:      3,
				CreatedAt: nostr.Timestamp(11),
				Tags: nostr.Tags{
					nostr.Tag{"p", gigi},
					nostr.Tag{"p", odell}},
			}

			_, err := processFollowList(ctx, DB, RWS, event)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("ProcessFollowList(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

var (
	picturePip string = "https://m.primal.net/IfSZ.jpg"
	bannerPip  string = "https://m.primal.net/IfSc.png"
)

func TestResizeSaveImage(t *testing.T) {
	file, err := os.Open("test.jpeg")
	if err != nil {
		t.Fatal(err)
	}

	img, err := jpeg.Decode(file)
	if err != nil {
		t.Fatal(err)
	}

	img1 := scaleImage(img, draw.BiLinear, 300)
	if err := saveImage(img1, "test_300.jpeg"); err != nil {
		t.Fatal(err)
	}

	img2 := scaleImage(img, draw.BiLinear, 30)
	if err := saveImage(img2, "test_30.jpeg"); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------BENCHMARKS----------------------------------

func BenchmarkIsValidPubkey(b *testing.B) {
	sk := nostr.GeneratePrivateKey()
	pk, err := nostr.GetPublicKey(sk)
	if err != nil {
		b.Fatalf("failed to get public key")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nostr.IsValidPublicKey(pk)
	}
}

func BenchmarkParsePubkeys(b *testing.B) {
	tagSizes := []int{10, 100, 1000, 10000}

	for _, size := range tagSizes {
		b.Run(fmt.Sprintf("size %d", size), func(b *testing.B) {
			event := nostr.Event{
				Tags: nostr.Tags{},
			}

			for i := 0; i < size; i++ {
				sk := nostr.GeneratePrivateKey()
				pk, err := nostr.GetPublicKey(sk)
				if err != nil {
					b.Fatalf("failed to get public key: %v", err)
				}

				event.Tags = append(event.Tags, nostr.Tag{"p", pk})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ParsePubkeys(&event)
			}
		})
	}
}

func BenchmarkResizeImage(b *testing.B) {
	file, err := os.Open("test.jpeg")
	if err != nil {
		b.Fatal(err)
	}

	img, err := jpeg.Decode(file)
	if err != nil {
		b.Fatal(err)
	}

	benchs := []struct {
		name   string
		scaler draw.Scaler
	}{
		{name: "nearest neighbor", scaler: draw.NearestNeighbor},
		{name: "approx bilinear", scaler: draw.ApproxBiLinear},
		{name: "bilinear", scaler: draw.BiLinear},
		{name: "catmullrom", scaler: draw.CatmullRom},
	}

	for _, bench := range benchs {
		b.Run(bench.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scaleImage(img, bench.scaler, 300)
			}
		})
	}
}
