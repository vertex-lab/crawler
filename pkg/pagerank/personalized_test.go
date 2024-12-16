package pagerank

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	mockdb "github.com/vertex-lab/crawler/pkg/database/mock"
	"github.com/vertex-lab/crawler/pkg/database/redisdb"
	"github.com/vertex-lab/crawler/pkg/models"
	mockstore "github.com/vertex-lab/crawler/pkg/store/mock"
	"github.com/vertex-lab/crawler/pkg/store/redistore"
	"github.com/vertex-lab/crawler/pkg/utils/redisutils"
	"github.com/vertex-lab/crawler/pkg/walks"
)

func TestCheckInputs(t *testing.T) {
	testCases := []struct {
		name          string
		DBType        string
		RWSType       string
		nodeID        uint32
		topK          uint16
		expectedError error
	}{
		{
			name:          "nil DB",
			DBType:        "nil",
			RWSType:       "one-node0",
			nodeID:        0,
			topK:          5,
			expectedError: models.ErrNilDBPointer,
		},
		{
			name:          "nil RWS",
			DBType:        "one-node0",
			RWSType:       "nil",
			nodeID:        0,
			topK:          5,
			expectedError: models.ErrNilRWSPointer,
		},
		{
			name:          "node not in DB",
			DBType:        "one-node0",
			RWSType:       "one-node1",
			nodeID:        1,
			topK:          5,
			expectedError: models.ErrNodeNotFoundDB,
		},
		{
			name:          "invalid topK",
			DBType:        "one-node0",
			RWSType:       "one-node0",
			topK:          0,
			expectedError: ErrInvalidTopN,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			DB := mockdb.SetupDB(test.DBType)
			RWS := mockstore.SetupRWS(test.RWSType)
			err := checkInputs(DB, RWS, test.nodeID, test.topK)

			if !errors.Is(err, test.expectedError) {
				t.Errorf("Pagerank(): expected %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestCountAndNormalize(t *testing.T) {
	testCases := []struct {
		name       string
		longWalk   []uint32
		expectedPP models.PagerankMap
	}{
		{
			name:       "nil walk",
			longWalk:   nil,
			expectedPP: models.PagerankMap{},
		},
		{
			name:       "empty walk",
			longWalk:   []uint32{},
			expectedPP: models.PagerankMap{},
		},
		{
			name:     "normal walk",
			longWalk: []uint32{0, 1, 2, 0, 1},
			expectedPP: models.PagerankMap{
				0: 2.0 / 5.0,
				1: 2.0 / 5.0,
				2: 1.0 / 5.0,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			pp := countAndNormalize(test.longWalk)
			if Distance(test.expectedPP, pp) > 1e-10 {
				t.Fatalf("countAndNormalize(): expected %v, got %v", test.expectedPP, pp)
			}
		})
	}
}

func TestReached(t *testing.T) {
	testCases := []struct {
		name            string
		targetLength    int
		expectedReached bool
	}{
		{
			name:            "target Lenght reached",
			targetLength:    0,
			expectedReached: true,
		},
		{
			name:            "target Lenght not reached",
			targetLength:    10,
			expectedReached: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			walk := NewPersonalizedWalk(0, test.targetLength)
			reached := walk.Reached(test.targetLength)

			if reached != test.expectedReached {
				t.Errorf("Reached(): expected %v, got %v", test.expectedReached, reached)
			}
		})
	}
}

func TestReset(t *testing.T) {
	testCases := []struct {
		name            string
		pWalkType       string
		expectedNodeIDs models.RandomWalk
	}{
		{
			name:            "one-node0",
			pWalkType:       "one-node0",
			expectedNodeIDs: models.RandomWalk{0},
		},
		{
			name:            "triangle",
			pWalkType:       "triangle",
			expectedNodeIDs: models.RandomWalk{0, 1, 2},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			walk := SetupPWalk(test.pWalkType, 10)
			walk.Reset()

			if walk.currentID != walk.startID {
				t.Errorf("Reset(): expected %v, got %v", walk.startID, walk.currentID)
			}

			if !reflect.DeepEqual(walk.current, models.RandomWalk{walk.startID}) {
				t.Errorf("Reset(): expected %v, got %v", models.RandomWalk{walk.startID}, walk.current)
			}

			if !reflect.DeepEqual(walk.all, test.expectedNodeIDs) {
				t.Errorf("Reset(): expected %v, got %v", test.expectedNodeIDs, walk.all)
			}
		})
	}
}

func TestAppendNode(t *testing.T) {
	testCases := []struct {
		name                string
		pWalkType           string
		nextNodeID          uint32
		expectedCurrentWalk models.RandomWalk
	}{
		{
			name:                "one-node0",
			pWalkType:           "one-node0",
			nextNodeID:          1,
			expectedCurrentWalk: models.RandomWalk{0, 1},
		},
		{
			name:                "triangle",
			pWalkType:           "triangle",
			nextNodeID:          3,
			expectedCurrentWalk: models.RandomWalk{0, 1, 2, 3},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			walk := SetupPWalk(test.pWalkType, 10)
			walk.Move(test.nextNodeID)

			if walk.currentID != test.nextNodeID {
				t.Errorf("AppendNode(): expected %v, got %v", test.nextNodeID, walk.currentID)
			}

			if !reflect.DeepEqual(walk.current, test.expectedCurrentWalk) {
				t.Errorf("AppendNode(): expected %v, got %v", test.expectedCurrentWalk, walk.current)
			}
		})
	}
}

func TestAppendWalk(t *testing.T) {
	testCases := []struct {
		name            string
		pWalkType       string
		walkSegment     models.RandomWalk
		expectedNodeIDs models.RandomWalk
	}{
		{
			name:            "one-node0",
			pWalkType:       "one-node0",
			walkSegment:     models.RandomWalk{1, 2},
			expectedNodeIDs: models.RandomWalk{0, 1, 2},
		},
		{
			name:            "triangle",
			pWalkType:       "triangle",
			walkSegment:     models.RandomWalk{3, 1},
			expectedNodeIDs: models.RandomWalk{0, 1, 2, 3},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			walk := SetupPWalk(test.pWalkType, 10)
			walk.Append(test.walkSegment)

			if !reflect.DeepEqual(walk.all, test.expectedNodeIDs) {
				t.Errorf("AppendNode(): expected %v, got %v", test.expectedNodeIDs, walk.current)
			}
		})
	}
}

func TestPersonalizedWalk(t *testing.T) {
	testCases := []struct {
		name           string
		DBType         string
		WCType         string
		startID        uint32
		requiredLenght int
		expectedWalk   models.RandomWalk
		expectedError  error
	}{
		{
			name:           "empty WC",
			DBType:         "triangle",
			WCType:         "triangle",
			startID:        0,
			requiredLenght: 10,
			expectedWalk:   models.RandomWalk{0, 1, 2, 0, 0, 1, 0, 1, 2, 0, 1, 2},
			expectedError:  nil,
		},
		{
			name:           "non-empty WC",
			DBType:         "triangle",
			WCType:         "triangle",
			startID:        0,
			requiredLenght: 10,
			expectedWalk:   models.RandomWalk{0, 1, 2, 0, 0, 1, 0, 1, 2, 0, 1, 2},
			expectedError:  nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			DB := mockdb.SetupDB(test.DBType)
			FC := SetupFC(DB, "empty")
			WC := SetupWC(test.WCType)
			rng := rand.New(rand.NewSource(42))

			walk, err := personalizedWalk(ctx, FC, WC, test.startID, test.requiredLenght, 0.85, rng)
			if !errors.Is(err, test.expectedError) {
				t.Fatalf("personalizedWalk(): expected %v, got %v", test.expectedError, err)
			}

			if !reflect.DeepEqual(walk, test.expectedWalk) {
				t.Errorf("personalizedWalk(): expected %v, got %v", test.expectedWalk, walk)
			}
		})
	}
}

func TestPersonalizedPagerank(t *testing.T) {
	ctx := context.Background()
	t.Run("simple errors", func(t *testing.T) {
		testCases := []struct {
			name          string
			DBType        string
			RWSType       string
			nodeID        uint32
			topK          uint16
			expectedError error
		}{
			{
				name:          "nil DB",
				DBType:        "nil",
				RWSType:       "one-node0",
				nodeID:        0,
				topK:          5,
				expectedError: models.ErrNilDBPointer,
			},
			{
				name:          "empty DB",
				DBType:        "empty",
				RWSType:       "one-node0",
				nodeID:        0,
				topK:          5,
				expectedError: models.ErrNodeNotFoundDB,
			},
			{
				name:          "nil RWS",
				DBType:        "one-node0",
				RWSType:       "nil",
				nodeID:        0,
				topK:          5,
				expectedError: models.ErrNilRWSPointer,
			},
			{
				name:          "empty RWS",
				RWSType:       "empty",
				DBType:        "one-node0",
				nodeID:        0,
				topK:          5,
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "node not in the RWS",
				DBType:        "triangle",
				RWSType:       "one-node0",
				nodeID:        1,
				topK:          5,
				expectedError: models.ErrNodeNotFoundRWS,
			},
			{
				name:          "invalid topK",
				DBType:        "one-node0",
				RWSType:       "one-node0",
				nodeID:        0,
				topK:          0,
				expectedError: ErrInvalidTopN,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				DB := mockdb.SetupDB(test.DBType)
				RWS := mockstore.SetupRWS(test.RWSType)
				_, err := Personalized(ctx, DB, RWS, test.nodeID, test.topK)

				if !errors.Is(err, test.expectedError) {
					t.Errorf("Personalized(): expected %v, got %v", test.expectedError, err)
				}
			})
		}
	})

	t.Run("fuzzy test", func(t *testing.T) {
		nodesNum := 200
		edgesPerNode := 20
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))

		DB := mockdb.GenerateDB(nodesNum, edgesPerNode, rng)
		RWM, _ := walks.NewMockRWM(0.85, 10)
		RWM.GenerateAll(ctx, DB)

		if _, err := Personalized(ctx, DB, RWM.Store, 0, 5); err != nil {
			t.Fatalf("Personalized() expected nil, got %v", err)
		}

		// doing it two times to check that it donesn't change the DB or RWS
		if _, err := Personalized(ctx, DB, RWM.Store, 0, 5); err != nil {
			t.Errorf("Personalized() expected nil, got %v", err)
		}
	})
}

// ----------------------------------BENCHMARKS--------------------------------

func BenchmarkCountAndNormalize(b *testing.B) {
	const walkSize = 300000

	walk := make(models.RandomWalk, 0, walkSize)
	for i := 0; i < walkSize; i++ {
		nodeID := uint32(rand.Intn(walkSize / 100))
		walk = append(walk, nodeID)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		countAndNormalize(walk)
	}
}

// func BenchmarkWalkCacheLoad(b *testing.B) {
// 	cl := redisutils.SetupClient()
// 	ctx := context.Background()

// 	RWS, err := redistore.NewRWSConnection(ctx, cl)
// 	if err != nil {
// 		b.Fatalf("NewRWSConnection(): benchmark failed: %v", err)
// 	}

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		WC := NewWalkCache()
// 		if err := WC.Load(ctx, RWS, 2, -1); err != nil {
// 			b.Fatalf("Personalized(): benchmark failed: %v", err)
// 		}
// 	}
// }

func BenchmarkWalks(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	RWS, err := redistore.NewRWSConnection(ctx, cl)
	if err != nil {
		b.Fatalf("NewRWSConnection(): benchmark failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := RWS.Walks(ctx, 2, -1); err != nil {
			b.Fatalf("Personalized(): benchmark failed: %v", err)
		}
	}
}

func BenchmarkSMembersPipe(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	size := 1000
	keys := make([]string, 0, size)
	for i := 0; i < size; i++ {
		//keys = append(keys, redistore.KeyWalksVisiting(uint32(i)))
		keys = append(keys, redisdb.KeyFollows(uint32(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipe := cl.Pipeline()
		strIDsByNode := make([][]string, 0, size)
		cmds := make([]*redis.StringSliceCmd, 0, size)
		for _, key := range keys {
			cmds = append(cmds, pipe.SMembers(ctx, key))
		}

		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatalf("pipelin failed: expected nil, got %v", err)
		}

		for _, cmd := range cmds {
			strIDs := cmd.Val()
			strIDsByNode = append(strIDsByNode, strIDs)
		}
	}
}

func BenchmarkFollowUnion(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	size := 34000
	keys := make([]string, 0, size)
	for i := 0; i < size; i++ {
		//keys = append(keys, redistore.KeyWalksVisiting(uint32(i)))
		keys = append(keys, redisdb.KeyFollows(uint32(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeIDs, err := cl.SUnion(ctx, keys...).Result()
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}

		_ = nodeIDs
		b.Errorf("nodes: %v", len(nodeIDs))
	}
}

func BenchmarkFollowsPipe(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	size := 10000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipe := cl.Pipeline()
		cmds := make([]*redis.StringSliceCmd, 0, size)
		for i := 0; i < size; i++ {
			nodeID := uint32(i)
			cmds = append(cmds, pipe.SMembers(ctx, redisdb.KeyFollows(nodeID)))
		}

		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatalf("pipe fialed: %v", err)
		}

		followByNode := make([][]string, 0, size)
		for _, cmd := range cmds {
			followByNode = append(followByNode, cmd.Val())
		}
	}
}

func BenchmarkHMGETPipe(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	keys := make([]string, 0, 10000)
	for i := 0; i < 10000; i++ {
		keys = append(keys, redistore.KeyWalksVisiting(uint32(i)))
	}

	pipe := cl.Pipeline()
	strIDsByNode := make([][]string, 0, 10000)
	cmds := make([]*redis.StringSliceCmd, 0, 10000)
	for _, key := range keys {
		cmds = append(cmds, pipe.SMembers(ctx, key))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		b.Fatalf("pipelin failed: expected nil, got %v", err)
	}

	for _, cmd := range cmds {
		strIDs := cmd.Val()
		strIDsByNode = append(strIDsByNode, strIDs)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipeH := cl.Pipeline()
		walksByNode := make([][]interface{}, 0, 10000)
		cmdsH := make([]*redis.SliceCmd, 0, 10000)
		for _, strIDs := range strIDsByNode {
			cmdsH = append(cmdsH, pipe.HMGet(ctx, redistore.KeyWalks, strIDs...))
		}

		if _, err := pipeH.Exec(ctx); err != nil {
			b.Fatalf("second pipeline failed: expected nil, got %v", err)
		}

		for _, cmd := range cmdsH {
			walks := cmd.Val()
			walksByNode = append(walksByNode, walks)
		}
	}
}

func BenchmarkCompletePipe(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	keys := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		keys = append(keys, redistore.KeyWalksVisiting(uint32(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipe := cl.Pipeline()
		strIDsByNode := make([][]string, 0, 100)
		cmds := make([]*redis.StringSliceCmd, 0, 100)
		for _, key := range keys {
			cmds = append(cmds, pipe.SMembers(ctx, key))
		}

		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatalf("pipelin failed: expected nil, got %v", err)
		}

		for _, cmd := range cmds {
			strIDs := cmd.Val()
			strIDsByNode = append(strIDsByNode, strIDs)
		}

		pipeH := cl.Pipeline()
		walksByNode := make([][]interface{}, 0, 100)
		cmdsH := make([]*redis.SliceCmd, 0, 100)
		for _, strIDs := range strIDsByNode {
			cmdsH = append(cmdsH, pipe.HMGet(ctx, redistore.KeyWalks, strIDs...))
		}

		if _, err := pipeH.Exec(ctx); err != nil {
			b.Fatalf("second pipeline failed: expected nil, got %v", err)
		}

		for _, cmd := range cmdsH {
			walks := cmd.Val()
			walksByNode = append(walksByNode, walks)
		}
	}
}

func BenchmarkHMGET(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	keys := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		keys = append(keys, redistore.KeyWalksVisiting(uint32(i)))
	}

	strIDs, err := cl.SUnion(ctx, keys...).Result()
	if err != nil {
		b.Fatalf("SMembers failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cl.HMGet(ctx, redistore.KeyWalks, strIDs...).Result(); err != nil {
			b.Fatalf("HMGET(): benchmark failed: %v", err)
		}
	}
}

func BenchmarkSUNION(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	keys := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		keys = append(keys, redistore.KeyWalksVisiting(uint32(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cl.SUnion(ctx, keys...).Result(); err != nil {
			b.Fatalf("SUnion(): benchmark failed: %v", err)
		}
	}
}

func BenchmarkSUNION2(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()

	follows, err := cl.SMembers(ctx, redisdb.KeyFollows(0)).Result()
	if err != nil {
		b.Fatalf("SMembers() benchmark failed: %v", err)
	}

	keys := make([]string, 0, len(follows))
	for _, ID := range follows {
		keys = append(keys, fmt.Sprintf("walksVisiting:%v", ID))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cl.SUnion(ctx, keys...).Result(); err != nil {
			b.Fatalf("SUnion(): benchmark failed: %v", err)
		}
	}
}

func BenchmarkSteps(b *testing.B) {
	cl := redisutils.SetupClient()
	ctx := context.Background()
	DB, err := redisdb.NewDatabase(ctx, cl)
	if err != nil {
		b.Fatalf("NewDatabase(): benchmark failed: %v", err)
	}

	for i := 0; i < b.N; i++ {
		if _, err := DB.Follows(ctx, 0); err != nil {
			b.Fatalf("Follows(): benchmark failed: %v", err)
		}
	}
}

func BenchmarkPersonalized(b *testing.B) {
	ctx := context.Background()
	nodesNum := 2000
	edgesPerNode := 100
	rng := rand.New(rand.NewSource(69))
	DB := mockdb.GenerateDB(nodesNum, edgesPerNode, rng)

	for _, walksPerNode := range []uint16{1, 10, 100, 1000} {
		RWM, _ := walks.NewMockRWM(0.85, walksPerNode)
		RWM.GenerateAll(ctx, DB)

		b.Run(fmt.Sprintf("walksPerNode: %d", walksPerNode), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Personalized(ctx, DB, RWM.Store, 0, 100); err != nil {
					b.Fatalf("Benchmark failed: %v", err)
				}
			}
		})
	}
}
