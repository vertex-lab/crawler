package redisdb

import (
	"errors"
	"testing"

	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	"github.com/pippellia-btc/Nostrcrawler/pkg/utils/redisutils"
)

func TestValidate(t *testing.T) {
	cl := redisutils.SetupClient()
	defer redisutils.CleanupRedis(cl)

	testCases := []struct {
		name          string
		DBType        string
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "empty DB",
			DBType:        "empty",
			expectedError: models.ErrEmptyDB,
		},
		{
			name:          "DB with node 0",
			DBType:        "one-node0",
			expectedError: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB, err := SetupDB(cl, test.DBType)
			if err != nil {
				t.Fatalf("SetupDB(): expected nil, got %v", err)
			}

			err = DB.Validate()
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("Validate(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}
