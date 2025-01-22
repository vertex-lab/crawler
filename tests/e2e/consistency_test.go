package e2e

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

/*
TestWalks() will:
- fetch walk batches of batchSize
- verify the consistency of walks (meaning each node visited by a walk contains it's walkID)
- repeate for iterationNum (iterating over the whole DB can take minutes)

Therefore, the number of walks checked is (roughly) iterationNum * batchSize.
*/
func TestWalks(t *testing.T) {
	cl := redisutils.SetupProdClient()
	ctx := context.Background()

	var counter int
	var iterationNum int = 20
	var batchSize int64 = 100000

	var res []string
	var cursor uint64
	var err error

	for {
		counter++
		res, cursor, err = cl.HScan(ctx, redistore.KeyWalks, cursor, "", batchSize).Result()
		if err != nil {
			t.Fatalf("HScan(): expected nil, got %v", err)
		}

		if counter >= iterationNum || cursor == 0 {
			break
		}

		strIDs := make([]string, 0, len(res)/2)
		strWalks := make([]string, 0, len(res)/2)
		for i := 0; i < len(res); i += 2 {
			strIDs = append(strIDs, res[i])
			strWalks = append(strWalks, res[i+1])
		}

		walks, err := redisutils.ParseWalks(strWalks)
		if err != nil {
			t.Fatalf("ParseWalks(): expected nil, got %v", err)
		}

		pipe := cl.Pipeline()
		cmds := make(map[string]*redis.BoolCmd)
		for i, ID := range strIDs {
			for _, nodeID := range walks[i] {
				// the key is the string "<nodeID>:<walkID>". It's unique because a node can be visited by a walk only once
				key := redisutils.FormatID(nodeID) + ":" + ID
				cmd := pipe.SIsMember(ctx, redistore.KeyWalksVisiting(nodeID), ID)
				cmds[key] = cmd
			}
		}

		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Pipeline failed: %v", err)
		}

		// check if all are true.
		for key, cmd := range cmds {
			if !cmd.Val() {
				t.Errorf("expected true, got %v: %v", cmd.Val(), key)
			}
		}
	}
}

// ------------------------------------BENCHMARKS-------------------------------

func BenchmarkPersonalizedPagerank(b *testing.B) {
	cl := redisutils.SetupProdClient()
	ctx := context.Background()

	var nodeID uint32 = 0
	var topk uint16 = 100

	DB, err := redisdb.NewDatabaseConnection(ctx, cl)
	if err != nil {
		b.Fatalf("NewDatabase(): benchmark failed: %v", err)
	}
	RWS, err := redistore.NewRWSConnection(ctx, cl)
	if err != nil {
		b.Fatalf("NewRWSConnection(): benchmark failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pagerank.Personalized(ctx, DB, RWS, nodeID, topk)
		if err != nil {
			b.Fatalf("Personalized(): benchmark failed: %v", err)
		}
	}
}

func BenchmarkPagerank(b *testing.B) {
	cl := redisutils.SetupProdClient()
	ctx := context.Background()

	RWS, err := redistore.NewRWSConnection(ctx, cl)
	if err != nil {
		b.Fatalf("NewRWSConnection(): benchmark failed: %v", err)
	}

	const size = 1000
	nodeIDs := make([]uint32, size)
	for i := 0; i < size; i++ {
		nodeIDs[i] = uint32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := pagerank.Global(ctx, RWS, nodeIDs...); err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}
