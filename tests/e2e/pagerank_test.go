package e2e

import (
	"context"
	"math"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
)

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

	pipe := cl.Pipeline()
	cmds := make([]*redis.StringCmd, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		cmd := pipe.HGet(ctx, redisdb.KeyNode(nodeID), "pagerank")
		cmds = append(cmds, cmd)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Pipeline failed: %v", err)
	}

	// now we want to test that the L1 norm of the pagerank vector is 1.
	sumPagerank := 0.0
	for _, cmd := range cmds {
		strRank := cmd.Val()
		rank, err := redisutils.ParseFloat64(strRank)
		if err != nil {
			t.Errorf("unexpected result type: %v", strRank)
		}

		sumPagerank += rank
	}

	if math.Abs(sumPagerank-1) > 0.001 {
		t.Errorf("the L1 norm of the pagerank is: %v", sumPagerank)
	}
}

// TestTotalVisits() tests if the totalVisits field in the RWS is indeed equal to
// the sum of all the visits for each node.
func TestTotalVisits(t *testing.T) {
	cl := redisutils.SetupProdClient()
	ctx := context.Background()

	// get the field totalVisits
	strTotalVisits, err := cl.HGet(ctx, redistore.KeyRWS, redistore.KeyTotalVisits).Result()
	if err != nil {
		t.Fatalf("error in getting totalVisits: %v", err)
	}
	totalVisits, err := redisutils.ParseInt64(strTotalVisits)
	if err != nil {
		t.Fatalf("unexpected return type: %v", strTotalVisits)
	}

	// compute the sum of the visits for each node
	DB, err := redisdb.NewDatabaseConnection(ctx, cl)
	if err != nil {
		t.Fatalf("NewDatabase(): expected nil, got %v", err)
	}
	nodeIDs, err := DB.AllNodes(ctx)
	if err != nil {
		t.Fatalf("AllNodes(): expected nil, got %v", err)
	}

	pipe := cl.Pipeline()
	cmds := make([]*redis.IntCmd, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		cmd := pipe.SCard(ctx, redistore.KeyWalksVisiting(nodeID))
		cmds = append(cmds, cmd)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Pipeline failed: %v", err)
	}

	var sumVisits int64 = 0
	for _, cmd := range cmds {
		sumVisits += cmd.Val()
	}

	if sumVisits != totalVisits {
		t.Errorf("totalVisits: expected %v, got %v", sumVisits, totalVisits)
	}
}

// TestWalks() tests for each walk, if its walkID is present in the walksVisiting each of the node it visits.
func TestWalks(t *testing.T) {
	cl := redisutils.SetupProdClient()
	ctx := context.Background()

	// get the walkIndex walkID --> random walk
	strWalkIndex, err := cl.HGetAll(ctx, redistore.KeyWalks).Result()
	if err != nil {
		t.Fatalf("HGetAll(): expected nil, got %v", err)
	}

	pipe := cl.Pipeline()
	cmds := make(map[string]*redis.BoolCmd)
	for walkID, strWalk := range strWalkIndex {

		walk, err := redisutils.ParseWalk(strWalk)
		if err != nil {
			t.Fatalf("unexpected ID type: %v", strWalk)
		}

		// check that the each nodeID in the walk contains that walkID
		for _, nodeID := range walk {
			// the key is the string "<walkID>:<nodeID>". It's unique because a node can be visited by a walk only once
			key := redisutils.FormatID(nodeID) + ":" + walkID
			cmd := pipe.SIsMember(ctx, redistore.KeyWalksVisiting(nodeID), walkID)
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

// ------------------------------------BENCHMARKS-------------------------------

func BenchmarkPersonalizedPagerank(b *testing.B) {
	cl := redisutils.SetupTestClient()
	ctx := context.Background()

	// Create new DB and RWS connections; Names are bad, I know... I will change them
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
		_, err := pagerank.Personalized(ctx, DB, RWS, 0, 100)
		if err != nil {
			b.Fatalf("Personalized(): benchmark failed: %v", err)
		}
	}
}
