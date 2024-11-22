package crawler

import (
	"errors"
	"reflect"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
)

// A list of fake events used for testing.
var fakeEvents = []nostr.Event{
	{
		ID:        "xxx",
		PubKey:    "503f9927838af9ae5701d96bc3eade86bb776582922b0394766a41a2ccee1c7a",
		Kind:      3,
		CreatedAt: nostr.Timestamp(1713083262),
		Tags: nostr.Tags{
			nostr.Tag{"p", "503f9927838af9ae5701d96bc3eade86bb776582922b0394766a41a2ccee1c7a"},
			nostr.Tag{"e", "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"},       // not a p tag
			nostr.Tag{"p", "xxxee9ba8b3dd0b2e8507d4ac6dfcd3fb2e7f0cc20f220f410a9ce3eccaac79fecdxxx"}, // pubkey badly formatted
		},
	},
}

func TestParseFollowList(t *testing.T) {
	testCases := []struct {
		name            string
		tags            nostr.Tags
		expectedPubkeys []string
	}{
		{
			name:            "nil tags",
			tags:            nil,
			expectedPubkeys: []string{},
		},
		{
			name:            "empty tags",
			tags:            nostr.Tags{},
			expectedPubkeys: []string{},
		},
		{
			name:            "one valid tag",
			tags:            fakeEvents[0].Tags,
			expectedPubkeys: []string{"503f9927838af9ae5701d96bc3eade86bb776582922b0394766a41a2ccee1c7a"},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			pubkeys := ParseFollowList(test.tags)
			if !reflect.DeepEqual(pubkeys, test.expectedPubkeys) {
				t.Fatalf("ParseFollowList(): expected %v, got %v", test.expectedPubkeys, pubkeys)
			}
		})
	}
}

func TestProcessFollows(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		pubkeys       []string
		expectedError error
		expectedIDs   []uint32
	}{
		{
			name:          "nil pubkeys",
			DBType:        "simple-with-mock-pks",
			pubkeys:       nil,
			expectedError: nil,
			expectedIDs:   []uint32{},
		},
		{
			name:          "empty pubkeys",
			DBType:        "simple-with-mock-pks",
			pubkeys:       []string{},
			expectedIDs:   []uint32{},
			expectedError: nil,
		},
		{
			name:          "existing pubkey",
			DBType:        "simple-with-mock-pks",
			pubkeys:       []string{"zero", "one"},
			expectedError: nil,
			expectedIDs:   []uint32{0, 1},
		},
		{
			name:          "existing and new pubkey",
			DBType:        "simple-with-mock-pks",
			pubkeys:       []string{"zero", "one", "three"},
			expectedError: nil,
			expectedIDs:   []uint32{0, 1, 3},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := mockdb.SetupDB(test.DBType)
			followIDs, err := ProcessFollows(DB, test.pubkeys, 0)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("ProcessFollows(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(followIDs, test.expectedIDs) {
				t.Errorf("ProcessFollows(): expected %v, got %v", test.expectedIDs, followIDs)
			}
		})
	}
}

func TestProcessFollowListEvent(t *testing.T) {
	t.Run("simple errors", func(t *testing.T) {
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
				expectedError: models.ErrNilDBPointer,
			},
			{
				name:          "nil RWS",
				DBType:        "one-node0",
				RWSType:       "nil",
				expectedError: models.ErrNilRWSPointer,
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
				DB := mockdb.SetupDB(test.DBType)
				RWS := mockstore.SetupRWS(test.RWSType)
				NC := models.NewNodeCache()

				err := ProcessFollowListEvent(DB, RWS, NC, &fakeEvents[0])
				if !errors.Is(err, test.expectedError) {
					t.Fatalf("ProcessFollowListEvent(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})
}
