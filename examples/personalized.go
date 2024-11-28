package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

func main() {
	cl := redisutils.SetupClient()

	// DO THIS ONLY ONCE, THEN COMMENT OUT.
	// Setup the DB and RWS with just the pip key (fucking narcissist).
	_, err := redisdb.SetupDB(cl, "pip")
	if err != nil {
		panic(err)
	}
	_, err = redistore.SetupRWS(cl, "pip")
	if err != nil {
		panic(err)
	}

	const pip = "f683e87035f7ad4f44e0b98cfbd9537e16455a92cd38cefc4cb31db7557f5ef2"
	pp, err := PersonalizedPagerank(
		context.Background(),
		cl,
		pip,
		100,
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("personalized pagerank of pip: %v\n", pp)
}

func PersonalizedPagerank(
	ctx context.Context,
	cl *redis.Client,
	pubkey string, // this is the hex
	topK uint16) (map[string]float64, error) {

	_ = ctx // we'll use the ctx in the future, after I (pip) will use it more consistently

	// Create new DB and RWS connections; Names are bad, I know... I will change them
	DB, err := redisdb.NewDatabase(context.Background(), cl)
	if err != nil {
		return map[string]float64{}, err
	}
	RWS, err := redistore.LoadRWS(context.Background(), cl)
	if err != nil {
		return map[string]float64{}, err
	}

	// the result is a slice of empty interfaces, which is an uint32 (nodeID) if the pubkey was found in the DB, nil otherwise
	node, err := DB.NodeIDs([]string{pubkey})
	if err != nil {
		return map[string]float64{}, err
	}

	// type assert
	nodeID, ok := node[0].(uint32)
	if !ok {
		return map[string]float64{}, fmt.Errorf("unexpected format: %v (type %T)", node, node)
	}

	// pp is a map nodeID --> rank; we need pubkey --> rank.
	pp, err := pagerank.Personalized(DB, RWS, nodeID, topK)
	if err != nil {
		return map[string]float64{}, err
	}

	// extract nodeIDs and ranks
	nodeIDs := make([]uint32, 0, len(pp))
	ranks := make([]float64, 0, len(pp))
	for nodeID, rank := range pp {
		nodeIDs = append(nodeIDs, nodeID)
		ranks = append(ranks, rank)
	}

	// get the pubkeys that correspond to the nodeIDs. This operation preserve order
	pubkeys, err := DB.Pubkeys(nodeIDs)
	if err != nil {
		return map[string]float64{}, err
	}

	// build the map pubkey --> rank.
	personalizedPagerank := make(map[string]float64, len(pubkeys))
	for i, pubkey := range pubkeys {

		pk, ok := pubkey.(string)
		if !ok {
			return map[string]float64{}, fmt.Errorf("unexpected format: %v (type %T)", pubkey, pubkey)
		}

		personalizedPagerank[pk] = ranks[i]
	}

	return personalizedPagerank, nil
}
