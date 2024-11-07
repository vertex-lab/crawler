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

func TestParseFromString(t *testing.T) {
	testCases := []struct {
		name          string
		val           string
		datatype      string
		expectedVal   interface{}
		expectedError error
	}{
		{
			name:          "empty walk",
			val:           "",
			datatype:      "RandomWalk",
			expectedVal:   models.RandomWalk{},
			expectedError: nil,
		},
		{
			name:          "normal walk",
			val:           "0,1,2,3,4",
			datatype:      "RandomWalk",
			expectedVal:   models.RandomWalk{0, 1, 2, 3, 4},
			expectedError: nil,
		},
		{
			name:          "invalid walk",
			val:           "0.33,11.0,1",
			datatype:      "RandomWalk",
			expectedVal:   nil,
			expectedError: strconv.ErrSyntax,
		},
		{
			name:          "float32",
			val:           "0.33",
			datatype:      "float32",
			expectedVal:   float32(0.33),
			expectedError: nil,
		},
		{
			name:          "uint32",
			val:           "11",
			datatype:      "uint32",
			expectedVal:   uint32(11),
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			walk, err := ParseFromString(test.val, test.datatype)

			if !errors.Is(err, test.expectedError) {
				t.Fatalf("ParseWalk(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walk, test.expectedVal) {
				t.Errorf("FormatWalk(): expected %v, got %v", test.expectedVal, walk)
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
