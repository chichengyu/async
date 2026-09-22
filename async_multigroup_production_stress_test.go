package async

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// MultiGroup / Group.Shard 千万级生产压力测试 + Race 检测
//
// 运行方式：
//   go test -run "TestProduction_MultiGroup" -v -count=1 -timeout 30m .
//   go test -run "TestProduction_MultiGroup" -v -count=1 -timeout 60m -race .
// ============================================================================

func multiGroupMemStats(tag string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("[%s] G=%d Heap=%dMB Obj=%d\n",
		tag, runtime.NumGoroutine(), m.HeapAlloc/1024/1024, m.HeapObjects)
}

// ============================================================================
// 千万级核心正确性
// ============================================================================

func TestProduction_MultiGroup_Shard_10M_Go(t *testing.T) {
	multiGroupMemStats("10M-MG-Go-start")
	n := 10_000_000

	mg := NewGroup[int](200).Shard(8)
	defer mg.Close()

	ctx := context.Background()

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		mg.Go(ctx, func(ctx context.Context) (int, error) {
			return idx * 2, nil
		})
	}

	results := mg.Wait()
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	failures := 0
	for _, r := range results {
		if !r.Ok() {
			failures++
		}
	}
	if failures > 0 {
		t.Fatalf("%d tasks failed", failures)
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] MultiGroup Shard(8) Go: shards=%d concurrency=%d tasks=%d in %v (%.0f ops/s) failures=%d",
		mg.ShardCount(), mg.TotalConcurrency(), n, elapsed, opsPerSec, failures)
	multiGroupMemStats("10M-MG-Go-end")
}

func TestProduction_MultiGroup_Shard_10M_GoKeyed(t *testing.T) {
	multiGroupMemStats("10M-MG-Keyed-start")
	n := 10_000_000

	mg := NewGroup[int](200).Shard(8)
	defer mg.Close()

	ctx := context.Background()

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		mg.GoKeyed(uint64(idx%1000), ctx, func(ctx context.Context) (int, error) {
			return idx % 1000, nil
		})
	}

	results := mg.Wait()
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] MultiGroup Shard(8) GoKeyed: tasks=%d in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	multiGroupMemStats("10M-MG-Keyed-end")
}

// ============================================================================
// 并发竞态安全
// ============================================================================

func TestProduction_MultiGroup_Race_10KConcurrent(t *testing.T) {
	mg := NewGroup[int](200).Shard(16)
	defer mg.Close()

	ctx := context.Background()
	concurrency := 100
	tasksPerGoroutine := 500

	var submitWg sync.WaitGroup
	submitWg.Add(concurrency)
	for gid := 0; gid < concurrency; gid++ {
		go func(gid int) {
			defer submitWg.Done()
			for tid := 0; tid < tasksPerGoroutine; tid++ {
				idx := gid*tasksPerGoroutine + tid
				switch gid % 3 {
				case 0:
					mg.Go(ctx, func(ctx context.Context) (int, error) { return idx, nil })
				case 1:
					mg.GoKeyed(uint64(gid), ctx, func(ctx context.Context) (int, error) { return idx, nil })
				default:
					mg.Go(ctx, func(ctx context.Context) (int, error) { return idx, nil })
				}
			}
		}(gid)
	}
	submitWg.Wait()

	results := mg.Wait()
	total := concurrency * tasksPerGoroutine
	if len(results) != total {
		t.Fatalf("mismatch: expected %d results, got %d", total, len(results))
	}

	failures := 0
	successes := 0
	for _, r := range results {
		if r.Ok() {
			successes++
		} else {
			failures++
		}
	}

	t.Logf("[Race] MultiGroup Shard(16): %d goroutines × %d tasks, results=%d successes=%d failures=%d",
		concurrency, tasksPerGoroutine, len(results), successes, failures)
}

func TestProduction_MultiGroup_Race_GoGoWait(t *testing.T) {
	for round := 0; round < 100; round++ {
		mg := NewGroup[int](50).Shard(4)
		ctx := context.Background()

		var wg sync.WaitGroup
		wg.Add(10)
		for g := 0; g < 10; g++ {
			go func() {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					mg.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
				}
			}()
		}

		wg.Wait()
		results := mg.Wait()
		mg.Close()

		if len(results) != 10*500 {
			t.Fatalf("round %d: expected %d results, got %d", round, 10*500, len(results))
		}
	}

	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	g := runtime.NumGoroutine()
	if g > 200 {
		t.Fatalf("goroutine leak: %d goroutines after 100 rounds of Go+Go+Wait race", g)
	}
	t.Logf("[Race] MultiGroup Go+Go+Wait race: 100 rounds, goroutines=%d", g)
}

func TestProduction_MultiGroup_Race_GetShard(t *testing.T) {
	for round := 0; round < 50; round++ {
		mg := NewGroup[int](20).Shard(4)
		ctx := context.Background()

		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 200; i++ {
					sp := mg.GetShard(gid % 4)
					if sp != nil {
						sp.Go(ctx, func(ctx context.Context) (int, error) { return gid, nil })
					}
				}
			}(g)
		}

		wg.Wait()
		results := mg.Wait()
		mg.Close()

		if len(results) != 4*200 {
			t.Fatalf("round %d: expected %d results, got %d", round, 4*200, len(results))
		}
	}

	runtime.GC()
	time.Sleep(300 * time.Millisecond)
	t.Logf("[Race] MultiGroup GetShard race: 50 rounds passed")
}

// ============================================================================
// 性能对比
// ============================================================================

func TestProduction_MultiGroup_Shard_vs_NoShard_1M(t *testing.T) {
	n := 1_000_000

	// ── 单 Group ──
	t.Logf("[1M] NoShard Group start (1 group × 200 concurrency)...")
	g := NewGroup[int](200)
	ctx := context.Background()

	startNoShard := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		g.Go(ctx, func(ctx context.Context) (int, error) { return idx, nil })
	}
	resultsNoShard := g.Wait()
	elapsedNoShard := time.Since(startNoShard)

	failuresNoShard := 0
	for _, r := range resultsNoShard {
		if !r.Ok() {
			failuresNoShard++
		}
	}
	opsNoShard := float64(n) / elapsedNoShard.Seconds()

	runtime.GC()
	time.Sleep(200 * time.Millisecond)

	// ── Shard(8) ──
	t.Logf("[1M] Shard(8) Group start (8 groups × 200 concurrency)...")
	mg := NewGroup[int](200).Shard(8)
	startShard := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		mg.Go(ctx, func(ctx context.Context) (int, error) { return idx, nil })
	}
	resultsShard := mg.Wait()
	elapsedShard := time.Since(startShard)
	mg.Close()

	failuresShard := 0
	for _, r := range resultsShard {
		if !r.Ok() {
			failuresShard++
		}
	}
	opsShard := float64(n) / elapsedShard.Seconds()

	speedup := opsShard / opsNoShard

	t.Logf("[1M] NoShard Group: 1 group × 200 = %d results, %v (%.0f ops/s), failures=%d",
		len(resultsNoShard), elapsedNoShard, opsNoShard, failuresNoShard)
	t.Logf("[1M] Shard(8) Group: 8 × 200 = %d concurrency = %d results, %v (%.0f ops/s), failures=%d",
		mg.TotalConcurrency(), len(resultsShard), elapsedShard, opsShard, failuresShard)
	t.Logf("[1M] Speedup: %.2f× | Shard(8) is %.0f%% faster than single Group",
		speedup, (speedup-1)*100)
}

// ============================================================================
// Goroutine 泄漏检测
// ============================================================================

func TestProduction_MultiGroup_Shard_Leak(t *testing.T) {
	before := runtime.NumGoroutine()
	multiGroupMemStats("10M-MG-Leak-before")

	for round := 0; round < 10; round++ {
		mg := NewGroup[int](100).Shard(8)
		ctx := context.Background()

		for i := 0; i < 500000; i++ {
			idx := i
			mg.Go(ctx, func(ctx context.Context) (int, error) { return idx, nil })
		}
		results := mg.Wait()
		mg.Close()

		failures := 0
		for _, r := range results {
			if !r.Ok() {
				failures++
			}
		}
		if failures > 0 {
			t.Fatalf("round %d: %d failures", round, failures)
		}

		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}

	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	after := runtime.NumGoroutine()

	multiGroupMemStats("10M-MG-Leak-after")
	t.Logf("[Leak] MultiGroup Shard(8): goroutines before=%d after=%d diff=%d", before, after, after-before)

	if after-before > 50 {
		t.Fatalf("goroutine leak detected: %d goroutines leaked after 10 rounds", after-before)
	}
}

// ============================================================================
// 统计聚合验证
// ============================================================================

func TestProduction_MultiGroup_Stats(t *testing.T) {
	mg := NewGroup[int](50).Shard(4)
	ctx := context.Background()

	for i := 0; i < 10000; i++ {
		idx := i
		mg.Go(ctx, func(ctx context.Context) (int, error) { return idx, nil })
	}
	mg.Wait()

	t.Logf("[Stats] MultiGroup Shard(4): shards=%d concurrency=%d active=%d busy=%d total=%d success=%d fail=%d",
		mg.ShardCount(),
		mg.TotalConcurrency(),
		mg.TotalActive(),
		mg.TotalBusy(),
		mg.TotalTaskCount(),
		mg.TotalSuccessCount(),
		mg.TotalFailCount(),
	)
}

// ============================================================================
// DefaultShard 自动分片
// ============================================================================

func TestProduction_MultiGroup_DefaultShard(t *testing.T) {
	g := NewGroup[int](50)
	mg := g.DefaultShard()
	defer mg.Close()

	expectedShards := runtime.GOMAXPROCS(0)
	if expectedShards < 2 {
		expectedShards = 2
	}

	if mg.ShardCount() != expectedShards {
		t.Fatalf("DefaultShard: expected %d shards, got %d", expectedShards, mg.ShardCount())
	}

	ctx := context.Background()
	for i := 0; i < 5000; i++ {
		idx := i
		mg.Go(ctx, func(ctx context.Context) (int, error) { return idx, nil })
	}
	results := mg.Wait()
	if len(results) != 5000 {
		t.Fatalf("expected 5000 results, got %d", len(results))
	}

	t.Logf("[DefaultShard] MultiGroup: GOMAXPROCS=%d → %d shards, each 50 concurrency, total=%d",
		runtime.GOMAXPROCS(0), mg.ShardCount(), mg.TotalConcurrency())
}

func TestProduction_MultiGroup_ShardGroup_Func(t *testing.T) {
	g := NewGroup[int](100)
	mg := ShardGroup(g, 16)
	defer mg.Close()

	if mg.ShardCount() != 16 {
		t.Fatalf("ShardGroup: expected 16 shards, got %d", mg.ShardCount())
	}

	ctx := context.Background()
	for i := 0; i < 5000; i++ {
		idx := i
		mg.Go(ctx, func(ctx context.Context) (int, error) { return idx, nil })
	}
	results := mg.Wait()
	if len(results) != 5000 {
		t.Fatalf("expected 5000 results, got %d", len(results))
	}

	t.Logf("[ShardGroup] %d shards, all good", mg.ShardCount())
}

func TestProduction_MultiGroup_DefaultShardGroup_Func(t *testing.T) {
	g := NewGroup[int](100)
	mg := DefaultShardGroup(g)
	defer mg.Close()

	expectedShards := runtime.GOMAXPROCS(0)
	if expectedShards < 2 {
		expectedShards = 2
	}
	if mg.ShardCount() != expectedShards {
		t.Fatalf("DefaultShardGroup: expected %d shards, got %d", expectedShards, mg.ShardCount())
	}

	ctx := context.Background()
	for i := 0; i < 5000; i++ {
		idx := i
		mg.Go(ctx, func(ctx context.Context) (int, error) { return idx, nil })
	}
	results := mg.Wait()
	if len(results) != 5000 {
		t.Fatalf("expected 5000 results, got %d", len(results))
	}

	t.Logf("[DefaultShardGroup] %d shards, all good", mg.ShardCount())
}
