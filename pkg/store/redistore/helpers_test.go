package redistore

import (
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
)

func TestFormatWalk(t *testing.T) {
	testCases := []struct {
		name            string
		walk            models.RandomWalk
		expectedStrWalk string
	}{
		{
			name:            "nil walk",
			walk:            nil,
			expectedStrWalk: "",
		},
		{
			name:            "empty walk",
			walk:            models.RandomWalk{},
			expectedStrWalk: "",
		},
		{
			name:            "normal walk",
			walk:            models.RandomWalk{0, 1, 2, 5, 7},
			expectedStrWalk: "0,1,2,5,7",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			strWalk := FormatWalk(test.walk)

			if strWalk != test.expectedStrWalk {
				t.Errorf("FormatWalk(): expected %v, got %v", test.expectedStrWalk, strWalk)
			}
		})
	}
}

func TestParseWalk(t *testing.T) {
	testCases := []struct {
		name          string
		strWalk       string
		expectedWalk  models.RandomWalk
		expectedError error
	}{
		{
			name:          "empty strWalk",
			strWalk:       "",
			expectedWalk:  models.RandomWalk{},
			expectedError: nil,
		},
		{
			name:          "invalid strWalk",
			strWalk:       "0.33,11.0,1",
			expectedWalk:  nil,
			expectedError: strconv.ErrSyntax,
		},
		{
			name:          "valid strWalk",
			strWalk:       "0,1,2,3,5",
			expectedWalk:  models.RandomWalk{0, 1, 2, 3, 5},
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			walk, err := ParseWalk(test.strWalk)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("ParseWalk(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walk, test.expectedWalk) {
				t.Errorf("FormatWalk(): expected %v, got %v", test.expectedWalk, walk)
			}
		})
	}
}

// ----------------------------------BENCHMARK----------------------------------

func BenchmarkFormattingWalk(b *testing.B) {
	walk := []uint32{0, 1, 2, 3, 4, 5, 6, 7}
	for i := 0; i < b.N; i++ {
		FormatWalk(walk)
	}
}

func BenchmarkParsingWalk(b *testing.B) {
	strWalk := "0,1,2,3,4,5,6"
	for i := 0; i < b.N; i++ {
		ParseWalk(strWalk)
	}
}
