package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
)

func PersonalizedPagerank(
	ctx context.Context,
	cl *redis.Client,
	pubkey string, // this is the hex
	topK uint16) (map[string]float64, error) {

	_ = ctx // we'll use the ctx in the future, after I (pip) will use it more consistently

	// The names are bad, should be ConnectDatabase/RWS. Will change them
	DB, err := redisdb.NewDatabase(context.Background(), cl)
	if err != nil {
		return map[string]float64{}, err
	}
	RWS, err := redistore.LoadRWS(context.Background(), cl)
	if err != nil {
		return map[string]float64{}, err
	}

	node, err := DB.NodeIDs([]string{pubkey}) // the result is a slice of empty interfaces, which is an uint32 (nodeID) if the pubkey was found in the DB, nil otherwise
	if err != nil {
		return map[string]float64{}, err
	}

	// type assert
	nodeID, ok := node[0].(uint32)
	if !ok {
		return map[string]float64{}, fmt.Errorf("unexpected format: %v (type %T)", node, node)
	}

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

	pubkeys, err := DB.Pubkeys(nodeIDs)
	if err != nil {
		return map[string]float64{}, err
	}

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
