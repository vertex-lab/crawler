// The walks package encapsulates all the logic around generating and updating random walks.
package walks

import (
	"github.com/pippellia-btc/Nostrcrawler/pkg/models"
	mock "github.com/pippellia-btc/Nostrcrawler/pkg/store/mock"
)

// RandomWalkManager wraps the RandomWalkStore and extends its functionalities
// with complex methods like Update() and Generate().
type RandomWalkManager struct {
	Store models.RandomWalkStore
}

// NewRandomWalkManager initialise RandomWalkManager based on the specified type
func NewRWM(storeType string,
	alpha float32, walksPerNode uint16) (*RandomWalkManager, error) {

	switch storeType {
	case "redis":
		return nil, nil

	default:
		// defaults to the mock RandomWalkStore
		RWS, err := mock.NewRWS(alpha, walksPerNode)
		if err != nil {
			return nil, err
		}
		return &RandomWalkManager{Store: RWS}, nil
	}
}

// function that returns a RWM setup based on the RWMType.
func SetupRWM(RWMType string) *RandomWalkManager {
	return &RandomWalkManager{Store: mock.SetupRWS(RWMType)}
}
