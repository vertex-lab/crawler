package e2e

import (
	"context"
	"math"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

// GetPagerankDB() fetched the pagerank scores from the DB for each of the specified nodes.
func GetPagerankDB(ctx context.Context, cl *redis.Client, nodeIDs []uint32) ([]float64, error) {
	pipe := cl.Pipeline()
	cmds := make([]*redis.StringCmd, len(nodeIDs))
	for i, ID := range nodeIDs {
		cmds[i] = pipe.HGet(ctx, redisdb.KeyNode(ID), models.KeyPagerank)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	pagerank := make([]float64, len(nodeIDs))
	for i, cmd := range cmds {
		strRank := cmd.Val()
		rank, err := redisutils.ParseFloat64(strRank)
		if err != nil {
			return nil, err
		}

		pagerank[i] = rank
	}

	return pagerank, nil
}

// TestPagerankSum() tests if the L1 norm of the pagerank vector is equal to 1, as it should be.
func TestPagerankSum(t *testing.T) {
	cl := redisutils.SetupProdClient()
	ctx := context.Background()

	DB, err := redisdb.NewDatabaseConnection(ctx, cl)
	if err != nil {
		t.Fatalf("NewDatabaseConnection(): expected nil, got %v", err)
	}

	nodeIDs, err := DB.AllNodes(ctx)
	if err != nil {
		t.Fatalf("AllNodes(): expected nil, got %v", err)
	}

	pagerank, err := GetPagerankDB(ctx, cl, nodeIDs)
	if err != nil {
		t.Fatalf("GetPagerank(): expected nil, got %v", err)
	}

	sum := 0.0
	for _, rank := range pagerank {
		sum += rank
	}

	if math.Abs(sum-1) > 0.001 {
		t.Errorf("the L1 norm of the pagerank is: %v", sum)
	}
}

// TestVisits() check if:
// - the totalVisits = sum of the visits
// - the ratio visit/totalVisits = pagerank
func TestVisits(t *testing.T) {
	cl := redisutils.SetupProdClient()
	ctx := context.Background()

	DB, err := redisdb.NewDatabaseConnection(ctx, cl)
	if err != nil {
		t.Fatalf("NewDatabaseConnection(): expected nil, got %v", err)
	}

	RWS, err := redistore.NewRWSConnection(ctx, cl)
	if err != nil {
		t.Fatalf("NewRWSConnection(): expected nil, got %v", err)
	}

	nodeIDs, err := DB.AllNodes(ctx)
	if err != nil {
		t.Fatalf("AllNodes(): expected nil, got %v", err)
	}

	visits, err := RWS.VisitCounts(ctx, nodeIDs...)
	if err != nil {
		t.Fatalf("VisitCounts(): expected nil, got %v", err)
	}
	totalVisits := RWS.TotalVisits(ctx)

	// check if the sum of the visits is  = totalVisits
	var sumVisits int
	for _, v := range visits {
		sumVisits += v
	}

	if sumVisits != totalVisits {
		t.Errorf("totalVisits: expected %v, got %v", totalVisits, sumVisits)
	}

	// check if the pagerank is indeed visit/totalVisits
	pagerank := make([]float64, len(nodeIDs))
	for i, v := range visits {
		pagerank[i] = float64(v) / float64(totalVisits)
	}

	loadedPagerank, err := GetPagerankDB(ctx, cl, nodeIDs)
	if err != nil {
		t.Fatalf("GetPagerank(): expected nil, got %v", err)
	}

	for i, ID := range nodeIDs {
		pr := pagerank[i]
		expected := loadedPagerank[i]
		if math.Abs(pr-expected) > 0.0001 {
			t.Errorf("pagerank of nodeID %d: expected %v, got %v", ID, expected, pr)
		}
	}
}

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
