package redistore

import (
	"context"
	"testing"
)

func TestGetAndParse(t *testing.T) {

	cl := SetupRedis()
	defer CleanupRedis(cl)

	// Set a key-value
	var key string = "testKey"
	var val int = 10
	if err := cl.Set(context.Background(), key, val, 0).Err(); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	testCases := []struct {
		datatype    string
		expectedVal interface{}
	}{
		{
			datatype:    "uint16",
			expectedVal: uint16(val),
		},
		{
			datatype:    "float32",
			expectedVal: float32(val),
		},
		{
			datatype:    "float64",
			expectedVal: float64(val),
		},
	}

	for _, test := range testCases {
		t.Run(test.datatype, func(t *testing.T) {

			got, err := GetAndParse(context.Background(), cl, key, test.datatype)
			if err != nil {
				t.Fatalf("GetAndParse(): expected nil, got %v", err)
			}

			if got != test.expectedVal {
				t.Errorf("GetAndParse(): expected %v (type %T), got %v (type %T)", test.expectedVal, test.expectedVal, got, got)
			}
		})
	}
}
