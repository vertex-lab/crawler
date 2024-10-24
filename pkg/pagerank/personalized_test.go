package pagerank

import (
	"errors"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/graph"
	"github.com/pippellia-btc/Nostrcrawler/pkg/mock"
	"github.com/pippellia-btc/Nostrcrawler/pkg/walks"
)

func TestCheckInputs(t *testing.T) {

	testCases := []struct {
		name          string
		DBType        string
		RWMType       string
		topN          uint16
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			RWMType:       "one-node0",
			topN:          5,
			expectedError: graph.ErrNilDatabasePointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			RWMType:       "one-node0",
			topN:          5,
			expectedError: graph.ErrDatabaseIsEmpty,
		},
		{
			name:          "nil RWM",
			DBType:        "one-node0",
			RWMType:       "nil",
			topN:          5,
			expectedError: walks.ErrNilRWMPointer,
		},
		{
			name:          "empty RWM",
			RWMType:       "empty",
			DBType:        "one-node0",
			topN:          5,
			expectedError: walks.ErrEmptyRWM,
		},
		{
			name:          "invalid topN",
			DBType:        "one-node0",
			RWMType:       "one-node0",
			topN:          0,
			expectedError: ErrInvalidTopN,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			DB := mock.SetupDB(test.DBType)
			RWM := walks.SetupRWM(test.RWMType)

			err := checkInputs(DB, RWM, test.topN)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("Pagerank(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestPersonalizedPagerank(t *testing.T) {

	t.Run("simple errors", func(t *testing.T) {

		testCases := []struct {
			name          string
			DBType        string
			RWMType       string
			nodeID        uint32
			topN          uint16
			expectedError error
		}{
			{
				name:          "nil DB",
				DBType:        "nil",
				RWMType:       "one-node0",
				nodeID:        0,
				topN:          5,
				expectedError: graph.ErrNilDatabasePointer,
			},
			{
				name:          "empty DB",
				DBType:        "empty",
				RWMType:       "one-node0",
				nodeID:        0,
				topN:          5,
				expectedError: graph.ErrDatabaseIsEmpty,
			},
			{
				name:          "nil RWM",
				DBType:        "one-node0",
				RWMType:       "nil",
				nodeID:        0,
				topN:          5,
				expectedError: walks.ErrNilRWMPointer,
			},
			{
				name:          "empty RWM",
				RWMType:       "empty",
				DBType:        "one-node0",
				nodeID:        0,
				topN:          5,
				expectedError: walks.ErrEmptyRWM,
			},
			{
				name:          "node not in the RWM",
				DBType:        "triangle",
				RWMType:       "one-node0",
				nodeID:        1,
				topN:          5,
				expectedError: walks.ErrNodeNotFoundRWM,
			},
			{
				name:          "invalid topN",
				DBType:        "one-node0",
				RWMType:       "one-node0",
				nodeID:        0,
				topN:          0,
				expectedError: ErrInvalidTopN,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {

				DB := mock.SetupDB(test.DBType)
				RWM := walks.SetupRWM(test.RWMType)

				_, err := Personalized(DB, RWM, test.nodeID, test.topN)

				if !errors.Is(err, test.expectedError) {
					t.Errorf("Pagerank(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("valid", func(t *testing.T) {

	})

}
