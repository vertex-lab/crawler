package e2e

import (
	"context"
	"fmt"
	"math"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/redis/go-redis/v9"
	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/models"
	"github.com/vertex-lab/crawler/pkg/pagerank"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
	"github.com/vertex-lab/crawler/pkg/utils/sliceutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

/*
TestWalks() will:
- fetch walk batches of batchSize
- verify the consistency of walks (meaning each node visited by a walk contains it's walkID)
- repeate for iterations (iterating over the whole DB can take minutes)

Therefore, the number of walks checked is (roughly) iterations * batchSize.
*/
func TestWalks(t *testing.T) {
	cl := redisutils.SetupProdClient()
	ctx := context.Background()

	fmt.Println("----------------------------")
	fmt.Println("Testing the walks consistency")
	fmt.Printf("----------------------------\n\n")

	var counter int
	var batchSize int64 = 10000

	var res []string
	var cursor uint64
	var err error

	for {
		counter++
		fmt.Printf("\033[1A")
		fmt.Print("\033[J")
		fmt.Printf("iteration %d...\n", counter)

		res, cursor, err = cl.HScan(ctx, redistore.KeyWalks, cursor, "", batchSize).Result()
		if err != nil {
			t.Fatalf("HScan(): expected nil, got %v", err)
		}

		if cursor == 0 {
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
			t.Fatalf("ParseWalks(): %v", err)
		}

		pipe := cl.Pipeline()
		cmds := make(map[string]*redis.BoolCmd)
		for i, ID := range strIDs {
			for _, nodeID := range walks[i] {
				// the key is the string "<nodeID>:<walkID>". It's unique because a node can be visited by a walk only once
				key := redisutils.FormatID(nodeID) + ":" + ID
				cmds[key] = pipe.SIsMember(ctx, redistore.KeyWalksVisiting(nodeID), ID)
			}
		}

		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Pipeline failed: %v", err)
		}

		for key, cmd := range cmds {
			if !cmd.Val() {
				t.Errorf("expected true, got %v: %v", cmd.Val(), key)
			}
		}
	}
}

func TestPagerank(t *testing.T) {
	cl := redisutils.SetupProdClient()
	ctx := context.Background()

	fmt.Println("---------------------------------")
	fmt.Println("Testing the pagerank distribution")

	DB, err := redisdb.NewDatabaseConnection(ctx, cl)
	if err != nil {
		t.Fatalf("NewDatabaseConnection(): %v", err)
	}

	RWS, err := redistore.NewRWSConnection(ctx, cl)
	if err != nil {
		t.Fatalf("NewRWSConnection(): %v", err)
	}

	nodeIDs, err := DB.AllNodes(ctx)
	if err != nil {
		t.Fatalf("AllNodes(): %v", err)
	}

	pg, err := pagerank.Global(ctx, RWS, nodeIDs...)
	if err != nil {
		t.Fatalf("Global(): %v", err)
	}
	fmt.Println(" > original pagerank successfully computed")

	// copy the DB into an in-memory mock to speed up random walks generation
	DB_memory, err := copy(DB, nodeIDs...)
	if err != nil {
		t.Fatalf("CopyFollows(): %v", err)
	}
	fmt.Println(" > DB successfully copied")

	// generate the walks only for the active nodes, and then compare
	// the resulting pagerank distribution with the one in redis.
	alpha := RWS.Alpha(ctx)
	walksPerNode := RWS.WalksPerNode(ctx)
	RWS_memory, err := mockstore.NewRWS(alpha, walksPerNode)
	if err != nil {
		t.Fatalf("NewRWS(): %v", err)
	}

	fmt.Printf(" > generating walks for active nodes\n")
	fmt.Printf("---------------------------------\n\n")

	var actives int
	for i, ID := range nodeIDs {
		fmt.Printf("\033[1A")
		fmt.Print("\033[J")
		fmt.Printf("progress %d/%d...\n", i+1, len(nodeIDs))

		node, err := DB.NodeByID(ctx, ID)
		if err != nil {
			t.Fatalf("failed to fetch node by ID %d: %v", ID, err)
		}

		if node.Status == models.StatusActive {
			if err := walks.Generate(ctx, DB_memory, RWS_memory, ID); err != nil {
				t.Fatalf("failed to generate walks for nodeID %d: %v", ID, err)
			}

			actives++
		}
	}

	pg_memory, err := pagerank.Global(ctx, RWS_memory, nodeIDs...)
	if err != nil {
		t.Fatalf("Global(): %v", err)
	}

	expected := expectedDistance(actives, len(nodeIDs), int(walksPerNode))
	distance := pagerank.Distance(pg, pg_memory)
	fmt.Printf("expected distance %f, got %f\n", expected, distance)

	if distance > expected {
		t.Fatalf("distance is higher than expected!")
	}
}

// copy copies the follow relationship of DB into an in-memory Database. Useful for speeding up testing.
func copy(DB models.Database, nodeIDs ...uint32) (*mockdb.Database, error) {
	ctx := context.Background()
	db := mockdb.NewDatabase()

	const batchSize = 1000
	batches := sliceutils.SplitSlice(nodeIDs, batchSize)

	for _, batch := range batches {
		follows, err := DB.Follows(ctx, batch...)
		if err != nil {
			return nil, fmt.Errorf("Follows(): %v", err)
		}

		for i, ID := range batch {
			db.NodeIndex[ID] = &models.Node{ID: ID}
			db.Follow[ID] = mapset.NewSet(follows[i]...)
		}
	}

	return db, nil
}

/*
ExpectedDistance() returns the expected distance between the real pagerank and
the Monte-Carlo pagerank, as a function of how many active nodes (and therefore number of walks).
Such distance goes as ~1/sqrt(R), where R is the number of walks.
# REFERENCES:
[1] K. Avrachenkov, N. Litvak, D. Nemirovsky, N. Osipova; "Monte Carlo methods in PageRank computation"
URL: https://www-sop.inria.fr/members/Konstantin.Avratchenkov/pubs/mc.pdf
*/
func expectedDistance(activeNodes, totalNodes, walksPerNode int) float64 {
	// empirically-derived distance of the Monte-Carlo pagerank algo, with 2M walks
	const defaultWalks = 2000000
	const defaultDistance = 0.05

	nodeRatio := float64(totalNodes) / float64(activeNodes)
	walks := float64(activeNodes * walksPerNode)
	return defaultDistance / math.Sqrt(walks/defaultWalks) * nodeRatio
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
