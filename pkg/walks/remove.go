package walks

import "github.com/vertex-lab/crawler/pkg/models"

func (RWM *RandomWalkManager) Remove(nodeID uint32) error {
	return nil
}

func startsWith(walk models.RandomWalk, nodeID uint32) (bool, error) {

	// if err := models.Validate(walk); err != nil {
	// 	return -1, err
	// }

	// for i := 0; i < len(walk)-1; i++ {
	// 	// if it contains a hop (nodeID --> removedNode)
	// 	if walk[i] == nodeID && slices.Contains(removedNodes, walk[i+1]) {
	// 		// it needs to be updated from (i+1)th element (included) onwards
	// 		cutIndex := i + 1
	// 		return cutIndex, nil
	// 	}
	// }
	// return -1, nil
	return true, nil
}
