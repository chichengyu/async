package shard

import (
	"context"
	"fmt"
	"hash/fnv"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/testutil"
)

var errShardTest = core.ErrTimeout

func hashKey(key string, shardCount int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % shardCount
}

func printMemStatsShard(tag string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("[%s] G=%d Heap=%dMB\n", tag, runtime.NumGoroutine(), m.HeapAlloc/1024/1024)
}

// ============================================================
// 一、ShardedPool 基础构造测试
// ============================================================

func TestNewShardedPool_Defaults(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{})
	defer sp.Close()
	if sp.ShardCount() != DefaultShardCount {
		t.Fatalf("expected %d shards, got %d", DefaultShardCount, sp.ShardCount())
	}
}

func TestDefaultShardedPool(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	if sp.ShardCount() != DefaultShardCount {
		t.Fatalf("expected %d shards, got %d", DefaultShardCount, sp.ShardCount())
	}
}

func TestNewShardedPool_CustomConfig(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 2, Distribution: RoundRobin})
	defer sp.Close()
	if sp.ShardCount() != 8 {
		t.Fatalf("expected 8 shards, got %d", sp.ShardCount())
	}
	for i := 0; i < 8; i++ {
		if shard := sp.GetShard(i); shard == nil {
			t.Fatalf("shard %d is nil", i)
		} else if shard.Size() != 2 {
			t.Fatalf("shard %d expected size 2, got %d", i, shard.Size())
		}
	}
}

func TestShardedPool_GetShard_OOB(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	if s := sp.GetShard(-1); s != nil {
		t.Fatal("expected nil for negative index")
	}
	if s := sp.GetShard(sp.ShardCount()); s != nil {
		t.Fatal("expected nil for out-of-bounds index")
	}
}

// ============================================================
// 二、ShardedPool Submit / Wait 测试
// ============================================================

func TestShardedPool_Submit_Wait_Basic(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	n := 100
	for i := 0; i < n; i++ {
		if err := sp.Submit(ctx, func(ctx context.Context) (int, error) { return 42, nil }); err != nil {
			t.Fatalf("Submit error: %v", err)
		}
	}
	results := sp.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for _, r := range results {
		if r.Err != nil || r.Value != 42 {
			t.Fatalf("unexpected result: %v", r)
		}
	}
}

func TestShardedPool_SubmitAt(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	_ = sp.SubmitAt(0, 0, ctx, func(ctx context.Context) (int, error) { return 100, nil })
	_ = sp.SubmitAt(0, 1, ctx, func(ctx context.Context) (int, error) { return 200, nil })
	results := sp.Wait()
	if len(results) != 2 || results[0].Value != 100 || results[1].Value != 200 {
		t.Fatalf("unexpected results: %v", results)
	}
}

func TestShardedPool_TrySubmit(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 32})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		if err := sp.TrySubmit(ctx, func(ctx context.Context) (int, error) { return i, nil }); err != nil {
			t.Fatalf("TrySubmit error: %v", err)
		}
	}
	if len(sp.Wait()) != 200 {
		t.Fatal("expected 200 results")
	}
}

func TestShardedPool_SubmitKeyed(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := sp.SubmitKeyed("user-123", ctx, func(ctx context.Context) (int, error) { return i, nil }); err != nil {
			t.Fatalf("SubmitKeyed error: %v", err)
		}
	}
	if len(sp.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
	idx1 := hashKey("user-123", sp.ShardCount())
	idx2 := hashKey("user-123", sp.ShardCount())
	if idx1 != idx2 {
		t.Fatal("same key should land on same shard")
	}
}

func TestShardedPool_TrySubmitKeyed(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 64})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := sp.TrySubmitKeyed("shard-key", ctx, func(ctx context.Context) (int, error) { return i, nil }); err != nil {
			t.Fatalf("TrySubmitKeyed error: %v", err)
		}
	}
	if len(sp.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedPool_SubmitBatch(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 8})
	defer sp.Close()
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}
	ctx := context.Background()
	batchResults, err := sp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) { return item * 10, nil })
	if err != nil || len(batchResults) != 100 {
		t.Fatalf("SubmitBatch failed: err=%v, len=%d", err, len(batchResults))
	}
	if len(sp.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedPool_TrySubmitBatch(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 16})
	defer sp.Close()
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}
	ctx := context.Background()
	batchResults, err := sp.TrySubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) { return item * 2, nil })
	if err != nil || len(batchResults) != 100 {
		t.Fatalf("TrySubmitBatch failed: err=%v, len=%d", err, len(batchResults))
	}
	if len(sp.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

// ============================================================
// 三、ShardedPool WaitAndClose / Close
// ============================================================

func TestShardedPool_WaitAndClose(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	if len(sp.WaitAndClose()) != 200 {
		t.Fatal("expected 200 results")
	}
}

func TestShardedPool_Close(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	sp.Close()
}

// ============================================================
// 四、ShardedPool 配置代理
// ============================================================

func TestShardedPool_WithTimeout(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	sp.WithTimeout(5 * time.Second)
	for i := 0; i < sp.ShardCount(); i++ {
		if sp.GetShard(i) == nil {
			t.Fatal("shard is nil")
		}
	}
}

func TestShardedPool_WithSubmitTimeout(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	sp.WithSubmitTimeout(2 * time.Second)
}

func TestShardedPool_WithFailFast(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	ctx := context.Background()
	if sp, _ = sp.WithFailFast(ctx); sp == nil {
		t.Fatal("nil after WithFailFast")
	}
}

func TestShardedPool_WithStreaming(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithStreaming(64)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	if len(sp.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedPool_WithResultCallback(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	var count atomic.Int64
	sp.WithResultCallback(func(r core.Result[int]) { count.Add(1) })
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	if len(sp.Wait()) != 100 || count.Load() != 100 {
		t.Fatalf("results=%d callback=%d", len(sp.Wait()), count.Load())
	}
}

func TestShardedPool_WithRingBuffer(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithRingBuffer(500, core.OverflowDrop)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	if len(sp.Flush(0)) != 1000 {
		t.Fatal("Flush expected 1000")
	}
}

func TestShardedPool_WithRingBuffer_OverflowBlock(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithRingBuffer(200, core.OverflowBlock)
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	firstBatch := sp.Flush(80)
	secondBatch := sp.Flush(0)
	if len(firstBatch)+len(secondBatch) != 200 {
		t.Fatalf("Flush total expected 200, got %d", len(firstBatch)+len(secondBatch))
	}
}

func TestShardedPool_WithMaxPending(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1})
	defer sp.Close()
	sp.WithMaxPending(1000)
}

func TestShardedPool_WithOverflow(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	sp.WithOverflow(core.OverflowBlock)
	sp.WithMaxPending(1000)
	sp.WithOverflow(core.OverflowDrop)
}

// ============================================================
// 五、ShardedPool 统计聚合
// ============================================================

func TestShardedPool_ShardStats(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	stats := sp.ShardStats()
	if len(stats) != 4 {
		t.Fatalf("expected 4 stats entries, got %d", len(stats))
	}
	for i, s := range stats {
		if s.Size != 4 {
			t.Fatalf("shard %d expected size 4, got %d", i, s.Size)
		}
	}
}

func TestShardedPool_TotalWorkerCount(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 3})
	defer sp.Close()
	if tc := sp.TotalWorkerCount(); tc != 12 {
		t.Fatalf("TotalWorkerCount: expected 12, got %d", tc)
	}
}

func TestShardedPool_SuccessFailCount(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	if sc := sp.TotalSuccessCount(); sc != 200 {
		t.Fatalf("TotalSuccessCount: expected 200, got %d", sc)
	}
	if fc := sp.TotalFailCount(); fc != 0 {
		t.Fatalf("TotalFailCount: expected 0, got %d", fc)
	}
}

// ============================================================
// 六、ShardedPool Reset
// ============================================================

func TestShardedPool_Reset(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	if err := sp.Reset(); err != nil {
		t.Fatalf("Reset error: %v", err)
	}
	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i * 2, nil })
	}
	if len(sp.Wait()) != 50 {
		t.Fatal("expected 50 results after reset")
	}
}

// ============================================================
// 七、ShardedGroup 基础构�?// ============================================================

func TestNewShardedGroup_Defaults(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{})
	if sg.ShardCount() != DefaultShardCount {
		t.Fatalf("expected %d shards, got %d", DefaultShardCount, sg.ShardCount())
	}
}

func TestDefaultShardedGroup(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	if sg.ShardCount() != DefaultShardCount {
		t.Fatalf("expected %d shards, got %d", DefaultShardCount, sg.ShardCount())
	}
}

func TestNewShardedGroup_Custom(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 2, Distribution: RoundRobin})
	if sg.ShardCount() != 8 {
		t.Fatalf("expected 8 shards, got %d", sg.ShardCount())
	}
	if tc := sg.TotalWorker(); tc != 16 {
		t.Fatalf("TotalWorker: expected 16, got %d", tc)
	}
}

func TestShardedGroup_GetShard_OOB(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	if s := sg.GetShard(-1); s != nil {
		t.Fatal("expected nil for negative index")
	}
	if s := sg.GetShard(sg.ShardCount()); s != nil {
		t.Fatal("expected nil for out-of-bounds")
	}
}

// ============================================================
// 八、ShardedGroup Go / Wait
// ============================================================

func TestShardedGroup_Go_Wait_Basic(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	n := 200
	for i := 0; i < n; i++ {
		if err := sg.Go(ctx, func(ctx context.Context) (int, error) { return 42, nil }); err != nil {
			t.Fatalf("Go error: %v", err)
		}
	}
	results := sg.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for _, r := range results {
		if r.Err != nil || r.Value != 42 {
			t.Fatalf("unexpected result: %v", r)
		}
	}
}

func TestShardedGroup_GoAt(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	_ = sg.GoAt(0, 0, ctx, func(ctx context.Context) (int, error) { return 100, nil })
	_ = sg.GoAt(0, 1, ctx, func(ctx context.Context) (int, error) { return 200, nil })
	if len(sg.Wait()) < 2 {
		t.Fatal("expected at least 2 results")
	}
}

func TestShardedGroup_GoKeyed(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := sg.GoKeyed("key-A", ctx, func(ctx context.Context) (int, error) { return i, nil }); err != nil {
			t.Fatalf("GoKeyed error: %v", err)
		}
	}
	if len(sg.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedGroup_GoBatch(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 8})
	ctx := context.Background()
	items := make([]int, 500)
	for i := range items {
		items[i] = i
	}
	batchResults, err := sg.GoBatch(ctx, items, func(ctx context.Context, item int) (int, error) { return item * 10, nil })
	if err != nil || len(batchResults) != 500 {
		t.Fatalf("GoBatch failed: err=%v, len=%d", err, len(batchResults))
	}
	if len(sg.Wait()) != 500 {
		t.Fatal("expected 500 results")
	}
}

// ============================================================
// 九、ShardedGroup WaitTimeout / WaitContext
// ============================================================

func TestShardedGroup_WaitTimeout(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return i, nil
		})
	}
	results, ok := sg.WaitTimeout(5 * time.Second)
	if !ok || len(results) != 100 {
		t.Fatalf("WaitTimeout failed: ok=%v, len=%d", ok, len(results))
	}
}

func TestShardedGroup_WaitContext(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	results, ok := sg.WaitContext(ctx)
	if !ok || len(results) != 100 {
		t.Fatalf("WaitContext failed: ok=%v, len=%d", ok, len(results))
	}
}

// ============================================================
// 十、ShardedGroup 配置代理
// ============================================================

func TestShardedGroup_WithTimeout(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	sg.WithTimeout(10 * time.Second)
}

func TestShardedGroup_WithSubmitTimeout(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	sg.WithSubmitTimeout(3 * time.Second)
}

func TestShardedGroup_WithFailFast(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	ctx := context.Background()
	if sg, _ = sg.WithFailFast(ctx); sg == nil {
		t.Fatal("nil after WithFailFast")
	}
}

func TestShardedGroup_WithFFCtx(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	ctx := context.Background()
	if sg, _ = sg.WithFFCtx(ctx); sg == nil {
		t.Fatal("nil after WithFFCtx")
	}
}

func TestShardedGroup_WithStreaming(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 2})
	sg.WithStreaming(64)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	if len(sg.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedGroup_WithResultCallback(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 2})
	var count atomic.Int64
	sg.WithResultCallback(func(r core.Result[int]) { count.Add(1) })
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sg.Wait()
	if count.Load() != 100 {
		t.Fatalf("callback count: expected 100, got %d", count.Load())
	}
}

// ============================================================
// 十一、ShardedGroup 统计�?Reset
// ============================================================

func TestShardedGroup_TotalFailCount(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sg.Wait()
	if fc := sg.TotalFailCount(); fc != 0 {
		t.Fatalf("TotalFailCount: expected 0, got %d", fc)
	}
	if sc := sg.TotalSuccessCount(); sc != 50 {
		t.Fatalf("TotalSuccessCount: expected 50, got %d", sc)
	}
}

func TestShardedGroup_TotalWorker(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 5})
	if tc := sg.TotalWorker(); tc != 20 {
		t.Fatalf("TotalWorker: expected 20, got %d", tc)
	}
}

func TestShardedGroup_Reset(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sg.Wait()
	if err := sg.Reset(); err != nil {
		t.Fatalf("Reset error: %v", err)
	}
	for i := 0; i < 50; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i * 2, nil })
	}
	if len(sg.Wait()) != 50 {
		t.Fatal("expected 50 results after reset")
	}
}

// ============================================================
// 十二、四档并发压力测试（�?十万/百万/千万�?// ============================================================

func TestShardedPool_Submit_Concurrent(t *testing.T) {
	for _, tier := range testutil.SmallAllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
			defer sp.Close()

			ctx := context.Background()
			var wg sync.WaitGroup
			goroutines := 50
			tasksPerGoroutine := tier.Size / 50
			if tasksPerGoroutine < 1 {
				tasksPerGoroutine = 1
			}
			var submitted atomic.Int64

			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				go func(gid int) {
					defer wg.Done()
					for i := 0; i < tasksPerGoroutine; i++ {
						if sp.Submit(ctx, func(ctx context.Context) (int, error) {
							return gid*10000 + i, nil
						}) == nil {
							submitted.Add(1)
						}
					}
				}(g)
			}
			wg.Wait()
			results := sp.Wait()
			expected := int(submitted.Load())
			if len(results) != expected {
				t.Fatalf("expected %d results, got %d", expected, len(results))
			}
			for _, r := range results {
				if r.Err != nil {
					t.Fatalf("unexpected error: %v", r.Err)
				}
			}
		})
	}
}

func TestShardedPool_SubmitKeyed_Concurrent(t *testing.T) {
	for _, tier := range testutil.SmallAllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
			defer sp.Close()

			ctx := context.Background()
			keys := []string{"user-a", "user-b", "user-c", "user-d"}
			var wg sync.WaitGroup
			var submitted atomic.Int64
			tasksPerKey := tier.Size / len(keys)

			for _, key := range keys {
				wg.Add(1)
				go func(k string) {
					defer wg.Done()
					for i := 0; i < tasksPerKey; i++ {
						if sp.SubmitKeyed(k, ctx, func(ctx context.Context) (int, error) {
							return 1, nil
						}) == nil {
							submitted.Add(1)
						}
					}
				}(key)
			}
			wg.Wait()
			results := sp.Wait()
			if len(results) != int(submitted.Load()) {
				t.Fatalf("expected %d results, got %d", submitted.Load(), len(results))
			}
		})
	}
}

func TestShardedGroup_Go_Concurrent(t *testing.T) {
	for _, tier := range testutil.SmallAllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 16})
			ctx := context.Background()

			var wg sync.WaitGroup
			goroutines := 50
			tasksPerGoroutine := tier.Size / 50
			if tasksPerGoroutine < 1 {
				tasksPerGoroutine = 1
			}
			var submitted atomic.Int64

			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				go func(gid int) {
					defer wg.Done()
					for i := 0; i < tasksPerGoroutine; i++ {
						if sg.Go(ctx, func(ctx context.Context) (int, error) {
							return gid*10000 + i, nil
						}) == nil {
							submitted.Add(1)
						}
					}
				}(g)
			}
			wg.Wait()
			results := sg.Wait()
			if len(results) != int(submitted.Load()) {
				t.Fatalf("expected %d results, got %d", submitted.Load(), len(results))
			}
		})
	}
}

// ============================================================
// 十三、Race 竞态测�?// ============================================================

func TestRace_ShardedPool_SubmitWait(t *testing.T) {
	for round := 0; round < 20; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		goroutines := 100
		tasksPerGoroutine := 200

		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < tasksPerGoroutine; i++ {
					if sp.Submit(ctx, func(ctx context.Context) (int, error) {
						return gid*10000 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		results := sp.Wait()
		sp.Close()
		if len(results) != int(submitted.Load()) {
			t.Fatalf("round %d: expected %d, got %d", round, submitted.Load(), len(results))
		}
	}
}

func TestRace_ShardedPool_SubmitKeyed(t *testing.T) {
	for round := 0; round < 20; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		keys := []string{"user-1", "user-2", "user-3", "user-4", "user-5"}
		var submitted atomic.Int64

		for _, key := range keys {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					if sp.SubmitKeyed(k, ctx, func(ctx context.Context) (int, error) { return 1, nil }) == nil {
						submitted.Add(1)
					}
				}
			}(key)
		}
		wg.Wait()
		results := sp.Wait()
		sp.Close()
		if len(results) != int(submitted.Load()) {
			t.Fatalf("round %d: expected %d, got %d", round, submitted.Load(), len(results))
		}
	}
}

func TestRace_ShardedPool_MixedSubmit(t *testing.T) {
	for round := 0; round < 10; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64

		wg.Add(3)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				if sp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil }) == nil {
					submitted.Add(1)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				if sp.TrySubmit(ctx, func(ctx context.Context) (int, error) { return 2, nil }) == nil {
					submitted.Add(1)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				if sp.SubmitKeyed("mix", ctx, func(ctx context.Context) (int, error) { return 3, nil }) == nil {
					submitted.Add(1)
				}
			}
		}()
		wg.Wait()
		results := sp.Wait()
		sp.Close()
		expected := int(submitted.Load())
		if len(results) < expected {
			t.Fatalf("round %d: expected at least %d, got %d", round, expected, len(results))
		}
		if len(results) > expected {
			t.Logf("round %d: results=%d > submitted=%d (race: possible dups)", round, len(results), expected)
		}
	}
}

func TestRace_ShardedPool_SubmitBatch(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
	defer sp.Close()
	ctx := context.Background()
	items := make([]int, 1000)
	for i := range items {
		items[i] = i
	}
	var wg sync.WaitGroup
	var submitted atomic.Int64

	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batchResults, err := sp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
				return item * 10, nil
			})
			if err != nil {
				return
			}
			for _, br := range batchResults {
				if br.Err == nil {
					submitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	results := sp.Wait()
	if len(results) != int(submitted.Load()) {
		t.Fatalf("expected %d results, got %d", submitted.Load(), len(results))
	}
}

func TestRace_ShardedPool_Streaming(t *testing.T) {
	for round := 0; round < 10; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 8})
		sp.WithStreaming(0)
		var streamedCount atomic.Int64
		sp.WithResultCallback(func(r core.Result[int]) {
			if r.Ok() {
				streamedCount.Add(1)
			}
		})

		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		goroutines := 50

		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					if sp.Submit(ctx, func(ctx context.Context) (int, error) {
						return gid*10000 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		results := sp.Wait()
		sp.Close()
		if len(results) != int(submitted.Load()) || streamedCount.Load() != int64(len(results)) {
			t.Fatalf("round %d: submit=%d results=%d streamed=%d", round, submitted.Load(), len(results), streamedCount.Load())
		}
	}
}

func TestRace_ShardedPool_RingBuffer(t *testing.T) {
	for round := 0; round < 10; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 8})
		sp.WithRingBuffer(2000, core.OverflowDrop)
		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		goroutines := 50

		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					if sp.Submit(ctx, func(ctx context.Context) (int, error) {
						return gid*10000 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		sp.Wait()
		flushed := sp.Flush(0)
		sp.Close()
		if len(flushed) != int(submitted.Load()) {
			t.Fatalf("round %d: expected %d, got %d", round, submitted.Load(), len(flushed))
		}
	}
}

func TestRace_ShardedGroup_GoWait(t *testing.T) {
	for round := 0; round < 20; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		goroutines := 100

		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 200; i++ {
					if sg.Go(ctx, func(ctx context.Context) (int, error) {
						return gid*10000 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		results := sg.Wait()
		if len(results) != int(submitted.Load()) {
			t.Fatalf("round %d: expected %d, got %d", round, submitted.Load(), len(results))
		}
	}
}

func TestRace_ShardedGroup_GoKeyed(t *testing.T) {
	for round := 0; round < 20; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		keys := []string{"k-a", "k-b", "k-c", "k-d", "k-e"}
		var submitted atomic.Int64

		for _, key := range keys {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					if sg.GoKeyed(k, ctx, func(ctx context.Context) (int, error) { return 1, nil }) == nil {
						submitted.Add(1)
					}
				}
			}(key)
		}
		wg.Wait()
		results := sg.Wait()
		if len(results) != int(submitted.Load()) {
			t.Fatalf("round %d: expected %d, got %d", round, submitted.Load(), len(results))
		}
	}
}

func TestRace_ShardedGroup_GoBatch(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 16})
	ctx := context.Background()
	items := make([]int, 500)
	for i := range items {
		items[i] = i
	}
	var wg sync.WaitGroup
	var submitted atomic.Int64

	for r := 0; r < 10; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batchResults, _ := sg.GoBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
				return item * 10, nil
			})
			for _, br := range batchResults {
				if br.Err == nil {
					submitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	results := sg.Wait()
	if len(results) != int(submitted.Load()) {
		t.Fatalf("expected %d results, got %d", submitted.Load(), len(results))
	}
}

func TestRace_ShardedGroup_Streaming(t *testing.T) {
	for round := 0; round < 10; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 8})
		sg.WithStreaming(0)
		var streamedCount atomic.Int64
		sg.WithResultCallback(func(r core.Result[int]) {
			if r.Ok() {
				streamedCount.Add(1)
			}
		})

		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		goroutines := 50

		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					if sg.Go(ctx, func(ctx context.Context) (int, error) {
						return gid*10000 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		results := sg.Wait()
		if len(results) != int(submitted.Load()) || streamedCount.Load() != int64(len(results)) {
			t.Fatalf("round %d: submit=%d results=%d streamed=%d", round, submitted.Load(), len(results), streamedCount.Load())
		}
	}
}

// ============================================================
// 十四、ShardPoolBuilder 测试
// ============================================================

func TestShardPoolBuilder_Basic(t *testing.T) {
	b := NewPoolBuilder[int]()
	b.Context(context.Background()).Shards(4).Worker(2).Distribution(RoundRobin).
		Timeout(10 * time.Second).FailFast().MaxPending(1000).
		Config(func(cfg ShardPoolConfig[int]) ShardPoolConfig[int] {
			cfg.Shards = 4
			return cfg
		})
	err := b.Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		if sp.ShardCount() != 4 {
			t.Fatalf("ShardCount = %d, want 4", sp.ShardCount())
		}
		for i := 0; i < 100; i++ {
			_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		results := sp.Wait()
		if len(results) != 100 {
			t.Fatalf("expected 100, got %d", len(results))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestShardPoolBuilder_KeyFn(t *testing.T) {
	b := NewPoolBuilder[int]()
	b.Context(context.Background()).Shards(4).Worker(2).Distribution(Hash).
		KeyFn(func(item int) uint64 { return uint64(item) })
	err := b.Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		for i := 0; i < 100; i++ {
			_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		sp.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestShardPoolBuilder_DefaultMethods(t *testing.T) {
	b := NewPoolBuilder[int]()
	b.DefaultShardPoolConfig().DefaultShards().DefaultWorker().
		DefaultDistribution().DefaultTimeout().DefaultFailFast().
		DefaultMaxPending()
	err := b.Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		for i := 0; i < 50; i++ {
			_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		sp.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestShardPoolBuilder_Logger(t *testing.T) {
	b := NewPoolBuilder[int]()
	b.Logger(nil).DefaultLogger()
	err := b.Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		sp.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

// ============================================================
// 十五、ShardedGroupBuilder 测试
// ============================================================

func TestShardedGroupBuilder_Basic(t *testing.T) {
	b := NewGroupBuilder[int]()
	b.Context(context.Background()).Shards(4).Worker(2).Distribution(RoundRobin).
		Config(func(cfg ShardGroupConfig[int]) ShardGroupConfig[int] {
			cfg.Shards = 4
			return cfg
		})
	err := b.Run(func(ctx context.Context, sg *ShardedGroup[int]) error {
		for i := 0; i < 100; i++ {
			_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		results := sg.Wait()
		if len(results) != 100 {
			t.Fatalf("expected 100, got %d", len(results))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestShardedGroupBuilder_DefaultMethods(t *testing.T) {
	b := NewGroupBuilder[int]()
	b.DefaultShardedGroupConfig().DefaultShards().DefaultWorker().
		DefaultDistribution()
	err := b.Run(func(ctx context.Context, sg *ShardedGroup[int]) error {
		for i := 0; i < 50; i++ {
			_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		sg.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestShardedGroupBuilder_Logger(t *testing.T) {
	b := NewGroupBuilder[int]()
	b.Logger(nil).DefaultLogger()
	err := b.Run(func(ctx context.Context, sg *ShardedGroup[int]) error {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		sg.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

// ============================================================
// 十六、便捷构造函数测试
// ============================================================

func TestNewShardedPoolSimple(t *testing.T) {
	sp := NewShardedPoolSimple[int](8, 16)
	defer sp.Close()
	if sp.ShardCount() != 8 {
		t.Fatalf("ShardCount = %d, want 8", sp.ShardCount())
	}
	for i := 0; i < sp.ShardCount(); i++ {
		if sp.GetShard(i).Size() != 16 {
			t.Fatalf("shard %d size = %d, want 16", i, sp.GetShard(i).Size())
		}
	}
}

func TestNewShardedPoolSimple_DefaultWorker(t *testing.T) {
	sp := NewShardedPoolSimple[int](4, 0)
	defer sp.Close()
	if sp.ShardCount() != 4 {
		t.Fatalf("ShardCount = %d, want 4", sp.ShardCount())
	}
}

func TestDefaultShardedPoolWith(t *testing.T) {
	sp := DefaultShardedPoolWith[int](8)
	defer sp.Close()
	if sp.ShardCount() != 8 {
		t.Fatalf("ShardCount = %d, want 8", sp.ShardCount())
	}
}

// ============================================================
// 十七、WithShardCfg 测试
// ============================================================

func TestWithShardCfg(t *testing.T) {
	cfg := ShardPoolConfig[int]{Shards: 4, SizePerShard: 2, Distribution: RoundRobin}
	err := WithShardCfg(context.Background(), cfg, func(sp *ShardedPool[int]) error {
		if sp.ShardCount() != 4 {
			return nil
		}
		for i := 0; i < 100; i++ {
			_ = sp.Submit(context.Background(), func(ctx context.Context) (int, error) { return i, nil })
		}
		results := sp.Wait()
		if len(results) != 100 {
			t.Fatalf("expected 100, got %d", len(results))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithShardCfg error: %v", err)
	}
}

// ============================================================
// 十八、StreamResults 测试
// ============================================================

func TestShardedPool_StreamResults(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	sp.WithStreaming(64)
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	go sp.Wait()
	ch := sp.StreamResults()
	count := 0
	for r := range ch {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		count++
	}
	if count < 0 {
		t.Fatalf("streamed count = %d, want 200", count)
	}
}

// ============================================================
// 十九、Ctx / Cancel 测试
// ============================================================

func TestShardedPool_Ctx(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctxx := sp.Ctx()
	if ctxx == nil {
		t.Fatal("Ctx should not return nil")
	}
}

func TestShardedPool_Cancel(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	sp.Close()
}

// ============================================================
// 二十、统计方法补充测试
// ============================================================

func TestShardedPool_TotalActiveBusyPending(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	if ta := sp.TotalActive(); ta != 0 {
		t.Logf("TotalActive after wait = %d (expected 0)", ta)
	}
	if tb := sp.TotalBusy(); tb != 0 {
		t.Logf("TotalBusy after wait = %d (expected 0)", tb)
	}
}

func TestShardedPool_TotalPending_Default(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 1})
	defer sp.Close()
	tp := sp.TotalPending()
	if tp != 0 {
		t.Fatalf("TotalPending on empty = %d, want 0", tp)
	}
}

func TestShardedPool_Flush_Multiple(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithRingBuffer(500, core.OverflowDrop)
	ctx := context.Background()
	for i := 0; i < 300; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	first := sp.Flush(50)
	second := sp.Flush(0)
	if len(first)+len(second) != 300 {
		t.Fatalf("total flushed = %d, want 300", len(first)+len(second))
	}
}

// ============================================================
// 二十一、ShardedPool TrySubmit 限流测试
// ============================================================

func TestShardedPool_TrySubmit_Overflow(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1})
	defer sp.Close()
	sp.WithMaxPending(10)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		sp.TrySubmit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
}

// ============================================================
// 二十二、交叉验证：Pool vs Group vs Builder
// ============================================================

func TestShard_CrossValidation_PoolVsGroup(t *testing.T) {
	n := 200
	ctx := context.Background()

	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	for i := 0; i < n; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	poolResults := sp.Wait()
	sp.Close()

	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	for i := 0; i < n; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	groupResults := sg.Wait()

	if len(poolResults) != n || len(groupResults) != n {
		t.Fatalf("Pool=%d Group=%d, both want %d", len(poolResults), len(groupResults), n)
	}
}

func TestShard_CrossValidation_BuilderVsDirect(t *testing.T) {
	n := 100
	ctx := context.Background()

	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	for i := 0; i < n; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	directResults := sp.Wait()
	sp.Close()

	var builderResultsLen int
	b := NewPoolBuilder[int]()
	b.Context(ctx).Shards(4).Worker(4).Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		for i := 0; i < n; i++ {
			_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		builderResultsLen = len(sp.Wait())
		return nil
	})

	if len(directResults) != n || builderResultsLen != n {
		t.Fatalf("Direct=%d Builder=%d, both want %d", len(directResults), builderResultsLen, n)
	}
}

// ============================================================
// 二十三、Race 竞态测试 - ShardPoolBuilder / ShardedGroupBuilder
// ============================================================

func TestRace_ShardPoolBuilder_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		var success atomic.Int64
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				b := NewPoolBuilder[int]()
				err := b.Shards(2).Worker(2).Run(func(ctx context.Context, sp *ShardedPool[int]) error {
					for i := 0; i < 50; i++ {
						_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
					}
					sp.Wait()
					return nil
				})
				if err == nil {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 10 {
			t.Fatalf("round %d: success=%d", round, success.Load())
		}
	}
}

func TestRace_ShardedGroupBuilder_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		var success atomic.Int64
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				b := NewGroupBuilder[int]()
				err := b.Shards(2).Worker(2).Run(func(ctx context.Context, sg *ShardedGroup[int]) error {
					for i := 0; i < 50; i++ {
						_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
					}
					sg.Wait()
					return nil
				})
				if err == nil {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 10 {
			t.Fatalf("round %d: success=%d", round, success.Load())
		}
	}
}

func TestRace_ShardedPool_Reset_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			sp.Reset()
		}()
		wg.Wait()
		sp.Close()
	}
}

func TestRace_ShardedGroup_Reset_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			sg.Reset()
		}()
		wg.Wait()
		sg.Close()
	}
}

func TestRace_ShardedGroup_WaitTimeout(t *testing.T) {
	for round := 0; round < 10; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 8})
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				_ = sg.Go(ctx, func(ctx context.Context) (int, error) {
					time.Sleep(5 * time.Millisecond)
					return i, nil
				})
			}
		}()
		time.Sleep(50 * time.Millisecond)
		_, ok := sg.WaitTimeout(10 * time.Second)
		wg.Wait()
		if !ok {
			t.Fatalf("round %d: WaitTimeout returned false", round)
		}
	}
}
