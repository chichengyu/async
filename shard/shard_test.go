package shard

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
)

var errShardTest = core.ErrTimeout

// helper: hash a key to a shard index
func hashKey(key string, shardCount int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % shardCount
}

// ==================== ShardedPool 基本功能测试 ====================

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
	sp := NewShardedPool(ShardPoolConfig[int]{
		Shards:       8,
		SizePerShard: 2,
		Distribution: RoundRobin,
	})
	defer sp.Close()

	if sp.ShardCount() != 8 {
		t.Fatalf("expected 8 shards, got %d", sp.ShardCount())
	}
	for i := 0; i < 8; i++ {
		shard := sp.GetShard(i)
		if shard == nil {
			t.Fatalf("shard %d is nil", i)
		}
		if shard.Size() != 2 {
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

// ==================== ShardedPool Submit / Wait ====================

func TestShardedPool_Submit_Wait_Basic(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()

	ctx := context.Background()
	n := 100
	for i := 0; i < n; i++ {
		err := sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return 42, nil
		})
		if err != nil {
			t.Fatalf("Submit error: %v", err)
		}
	}
	results := sp.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		if r.Value != 42 {
			t.Fatalf("expected 42, got %d", r.Value)
		}
	}
}

func TestShardedPool_SubmitAt(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()

	ctx := context.Background()
	err := sp.SubmitAt(0, 0, ctx, func(ctx context.Context) (int, error) {
		return 100, nil
	})
	if err != nil {
		t.Fatalf("SubmitAt error: %v", err)
	}
	_ = sp.SubmitAt(0, 1, ctx, func(ctx context.Context) (int, error) {
		return 200, nil
	})

	results := sp.Wait()
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Value != 100 {
		t.Fatalf("results[0]=%d", results[0].Value)
	}
	if results[1].Value != 200 {
		t.Fatalf("results[1]=%d", results[1].Value)
	}
}

func TestShardedPool_TrySubmit(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 32})
	defer sp.Close()

	ctx := context.Background()
	for i := 0; i < 200; i++ {
		err := sp.TrySubmit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
		if err != nil {
			t.Fatalf("TrySubmit error: %v", err)
		}
	}
	results := sp.Wait()
	if len(results) != 200 {
		t.Fatalf("expected 200 results, got %d", len(results))
	}
}

func TestShardedPool_SubmitKeyed(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err := sp.SubmitKeyed("user-123", ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
		if err != nil {
			t.Fatalf("SubmitKeyed error: %v", err)
		}
	}
	results := sp.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}

	key1Idx := hashKey("user-123", sp.ShardCount())
	key2Idx := hashKey("user-456", sp.ShardCount())
	if key1Idx != key2Idx {
		t.Log("different keys may land on different shards: ok")
	}
	key1Again := hashKey("user-123", sp.ShardCount())
	if key1Idx != key1Again {
		t.Fatalf("same key should land on same shard: %d vs %d", key1Idx, key1Again)
	}
}

func TestShardedPool_TrySubmitKeyed(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 64})
	defer sp.Close()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err := sp.TrySubmitKeyed("shard-key", ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
		if err != nil {
			t.Fatalf("TrySubmitKeyed error: %v", err)
		}
	}
	results := sp.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
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
	batchResults, err := sp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 10, nil
	})
	if err != nil {
		t.Fatalf("SubmitBatch error: %v", err)
	}
	if len(batchResults) != 100 {
		t.Fatalf("expected 100 batch results, got %d", len(batchResults))
	}

	results := sp.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
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
	batchResults, err := sp.TrySubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 2, nil
	})
	if err != nil {
		t.Fatalf("TrySubmitBatch error: %v", err)
	}
	if len(batchResults) != 100 {
		t.Fatalf("expected 100 results, got %d", len(batchResults))
	}

	results := sp.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

// ==================== ShardedPool WaitAndClose / Close ====================

func TestShardedPool_WaitAndClose(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})

	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	results := sp.WaitAndClose()
	if len(results) != 200 {
		t.Fatalf("expected 200 results, got %d", len(results))
	}
}

func TestShardedPool_Close(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	sp.Wait()
	sp.Close()
}

// ==================== ShardedPool 配置代理方法 ====================

func TestShardedPool_WithTimeout(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	sp.WithTimeout(5 * time.Second)

	for i := 0; i < sp.ShardCount(); i++ {
		shard := sp.GetShard(i)
		if shard == nil {
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
	sp, _ = sp.WithFailFast(ctx)
	if sp == nil {
		t.Fatal("sharded pool is nil after WithFailFast")
	}
}

func TestShardedPool_WithStreaming(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithStreaming(64)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	results := sp.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

func TestShardedPool_WithResultCallback(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()

	var count atomic.Int64
	sp.WithResultCallback(func(r core.Result[int]) {
		count.Add(1)
	})

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	results := sp.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
	if c := count.Load(); c != 100 {
		t.Fatalf("callback count: expected 100, got %d", c)
	}
}

func TestShardedPool_WithRingBuffer(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithRingBuffer(500, core.OverflowDrop)

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	sp.Wait()
	flushed := sp.Flush(0)
	if len(flushed) != 1000 {
		t.Fatalf("Flush: expected 1000 results, got %d", len(flushed))
	}
}

func TestShardedPool_WithRingBuffer_OverflowBlock(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithRingBuffer(200, core.OverflowBlock)

	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	sp.Wait()
	firstBatch := sp.Flush(80)
	if len(firstBatch) < 80 {
		t.Fatalf("Flush first batch expected >=80, got %d", len(firstBatch))
	}
	secondBatch := sp.Flush(0)
	total := len(firstBatch) + len(secondBatch)
	if total != 200 {
		t.Fatalf("Flush total: expected 200, got %d", total)
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

// ==================== ShardedPool 统计聚合 ====================

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
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	sp.Wait()

	if sc := sp.TotalSuccessCount(); sc != 200 {
		t.Fatalf("TotalSuccessCount: expected 200, got %d", sc)
	}
	if fc := sp.TotalFailCount(); fc != 0 {
		t.Fatalf("TotalFailCount: expected 0, got %d", fc)
	}
}

// ==================== ShardedPool Reset ====================

func TestShardedPool_Reset(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	sp.Wait()

	err := sp.Reset()
	if err != nil {
		t.Fatalf("Reset error: %v", err)
	}

	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i * 2, nil
		})
	}
	results := sp.Wait()
	if len(results) != 50 {
		t.Fatalf("expected 50 results after reset, got %d", len(results))
	}
}

// ==================== ShardedGroup 基本功能测试 ====================

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
	sg := NewShardedGroup(ShardGroupConfig[int]{
		Shards:              8,
		ConcurrencyPerShard: 2,
		Distribution:        RoundRobin,
	})
	if sg.ShardCount() != 8 {
		t.Fatalf("expected 8 shards, got %d", sg.ShardCount())
	}
	if tc := sg.TotalConcurrency(); tc != 16 {
		t.Fatalf("TotalConcurrency: expected 16, got %d", tc)
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

// ==================== ShardedGroup Go / Wait ====================

func TestShardedGroup_Go_Wait_Basic(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	n := 200
	for i := 0; i < n; i++ {
		err := sg.Go(ctx, func(ctx context.Context) (int, error) {
			return 42, nil
		})
		if err != nil {
			t.Fatalf("Go error: %v", err)
		}
	}
	results := sg.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		if r.Value != 42 {
			t.Fatalf("expected 42, got %d", r.Value)
		}
	}
}

func TestShardedGroup_GoAt(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()

	_ = sg.GoAt(0, 0, ctx, func(ctx context.Context) (int, error) {
		return 100, nil
	})
	_ = sg.GoAt(0, 1, ctx, func(ctx context.Context) (int, error) {
		return 200, nil
	})

	results := sg.Wait()
	if len(results) < 2 {
		t.Fatalf("expected at least 2 results, got %d", len(results))
	}
}

func TestShardedGroup_GoKeyed(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		err := sg.GoKeyed("key-A", ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
		if err != nil {
			t.Fatalf("GoKeyed error: %v", err)
		}
	}
	results := sg.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

func TestShardedGroup_GoBatch(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 8})
	ctx := context.Background()

	items := make([]int, 500)
	for i := range items {
		items[i] = i
	}
	batchResults, err := sg.GoBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 10, nil
	})
	if err != nil {
		t.Fatalf("GoBatch error: %v", err)
	}
	if len(batchResults) != 500 {
		t.Fatalf("expected 500 batch results, got %d", len(batchResults))
	}

	results := sg.Wait()
	if len(results) != 500 {
		t.Fatalf("expected 500 results, got %d", len(results))
	}
}

// ==================== ShardedGroup WaitTimeout / WaitContext ====================

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
	if !ok {
		t.Fatal("WaitTimeout returned false")
	}
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

func TestShardedGroup_WaitContext(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	results, ok := sg.WaitContext(ctx)
	if !ok {
		t.Fatal("WaitContext returned false")
	}
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

// ==================== ShardedGroup 配置代理 ====================

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
	sg, _ = sg.WithFailFast(ctx)
	if sg == nil {
		t.Fatal("nil after WithFailFast")
	}
}

func TestShardedGroup_WithFFCtx(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	ctx := context.Background()
	sg, _ = sg.WithFFCtx(ctx)
	if sg == nil {
		t.Fatal("nil after WithFFCtx")
	}
}

func TestShardedGroup_WithStreaming(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 2})
	sg.WithStreaming(64)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	results := sg.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

func TestShardedGroup_WithResultCallback(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 2})

	var count atomic.Int64
	sg.WithResultCallback(func(r core.Result[int]) {
		count.Add(1)
	})

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	sg.Wait()

	if c := count.Load(); c != 100 {
		t.Fatalf("callback count: expected 100, got %d", c)
	}
}

// ==================== ShardedGroup 统计聚合 ====================

func TestShardedGroup_TotalFailCount(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	sg.Wait()

	if fc := sg.TotalFailCount(); fc != 0 {
		t.Fatalf("TotalFailCount: expected 0, got %d", fc)
	}
	if sc := sg.TotalSuccessCount(); sc != 50 {
		t.Fatalf("TotalSuccessCount: expected 50, got %d", sc)
	}
}

func TestShardedGroup_TotalConcurrency(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 5})
	if tc := sg.TotalConcurrency(); tc != 20 {
		t.Fatalf("TotalConcurrency: expected 20, got %d", tc)
	}
}

// ==================== ShardedGroup Reset ====================

func TestShardedGroup_Reset(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	sg.Wait()

	err := sg.Reset()
	if err != nil {
		t.Fatalf("Reset error: %v", err)
	}

	for i := 0; i < 50; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) {
			return i * 2, nil
		})
	}
	results := sg.Wait()
	if len(results) != 50 {
		t.Fatalf("expected 50 results after reset, got %d", len(results))
	}
}

// ==================== 并发安全测试 ====================

func TestShardedPool_ConcurrentSubmit(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 8})
	defer sp.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	goroutines := 50
	tasksPerGoroutine := 100
	var submitted atomic.Int64

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < tasksPerGoroutine; i++ {
				err := sp.Submit(ctx, func(ctx context.Context) (int, error) {
					return gid*10000 + i, nil
				})
				if err == nil {
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
}

func TestShardedGroup_ConcurrentGo(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 8})
	ctx := context.Background()

	var wg sync.WaitGroup
	goroutines := 50
	tasksPerGoroutine := 100
	var submitted atomic.Int64

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < tasksPerGoroutine; i++ {
				err := sg.Go(ctx, func(ctx context.Context) (int, error) {
					return gid*10000 + i, nil
				})
				if err == nil {
					submitted.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	results := sg.Wait()
	expected := int(submitted.Load())
	if len(results) != expected {
		t.Fatalf("expected %d results, got %d", expected, len(results))
	}
}

func TestShardedPool_ConcurrentSubmitKeyed(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 8})
	defer sp.Close()

	ctx := context.Background()
	keys := []string{"user-a", "user-b", "user-c", "user-d"}
	var wg sync.WaitGroup
	var submitted atomic.Int64

	for _, key := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				err := sp.SubmitKeyed(k, ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				})
				if err == nil {
					submitted.Add(1)
				}
			}
		}(key)
	}
	wg.Wait()

	results := sp.Wait()
	expected := int(submitted.Load())
	if len(results) != expected {
		t.Fatalf("expected %d results, got %d", expected, len(results))
	}
}
