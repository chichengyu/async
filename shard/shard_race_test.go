package shard

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
)

func printMemStatsShard(tag string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("[%s] G=%d Heap=%dMB\n", tag, runtime.NumGoroutine(), m.HeapAlloc/1024/1024)
}

// ==================== ShardedPool 高并发压力测试 ====================

func TestRace_ShardedPool_ConcurrentSubmitWait(t *testing.T) {
	for round := 0; round < 20; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
		ctx := context.Background()

		var wg sync.WaitGroup
		goroutines := 100
		tasksPerGoroutine := 200
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
		sp.Close()

		expected := int(submitted.Load())
		if len(results) != expected {
			t.Fatalf("round %d: expected %d results, got %d", round, expected, len(results))
		}
		for _, r := range results {
			if r.Err != nil {
				t.Fatalf("round %d: unexpected error: %v", round, r.Err)
			}
		}
	}
}

func TestRace_ShardedPool_SubmitKeyed_Concurrent(t *testing.T) {
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
		sp.Close()

		expected := int(submitted.Load())
		if len(results) != expected {
			t.Fatalf("round %d: expected %d results, got %d", round, expected, len(results))
		}
	}
}

func TestRace_ShardedPool_MixedSubmitTypes(t *testing.T) {
	for round := 0; round < 10; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64

		wg.Add(3)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				err := sp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
				if err == nil {
					submitted.Add(1)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				err := sp.TrySubmit(ctx, func(ctx context.Context) (int, error) { return 2, nil })
				if err == nil {
					submitted.Add(1)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				err := sp.SubmitKeyed("mix", ctx, func(ctx context.Context) (int, error) { return 3, nil })
				if err == nil {
					submitted.Add(1)
				}
			}
		}()
		wg.Wait()
		results := sp.Wait()
		sp.Close()

		expected := int(submitted.Load())
		okCount := 0
		for _, r := range results {
			if r.Err == nil {
				okCount++
			}
		}
		if okCount != expected {
			t.Fatalf("round %d: expected %d ok results, got %d", round, expected, okCount)
		}
	}
}

func TestRace_ShardedPool_SubmitBatch_Concurrent(t *testing.T) {
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
				t.Errorf("SubmitBatch error: %v", err)
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
	expected := int(submitted.Load())
	if len(results) != expected {
		t.Fatalf("expected %d results, got %d", expected, len(results))
	}
}

func TestRace_ShardedPool_WithStreaming_Concurrent(t *testing.T) {
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
		sp.Close()

		expected := int(submitted.Load())
		if len(results) != expected {
			t.Fatalf("round %d: expected %d results, got %d", round, expected, len(results))
		}
		if sc := streamedCount.Load(); sc != int64(expected) {
			t.Fatalf("round %d: streamed count %d != %d", round, sc, expected)
		}
	}
}

func TestRace_ShardedPool_WithRingBuffer_Concurrent(t *testing.T) {
	for round := 0; round < 10; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 8})
		sp.WithRingBuffer(2000, core.OverflowDrop)

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
		sp.Wait()
		flushed := sp.Flush(0)
		sp.Close()

		expected := int(submitted.Load())
		if len(flushed) != expected {
			t.Fatalf("round %d: Flush expected %d results, got %d", round, expected, len(flushed))
		}
	}
}

// ==================== ShardedGroup 高并发压力测试 ====================

func TestRace_ShardedGroup_ConcurrentGoWait(t *testing.T) {
	for round := 0; round < 20; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 16})
		ctx := context.Background()

		var wg sync.WaitGroup
		goroutines := 100
		tasksPerGoroutine := 200
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
			t.Fatalf("round %d: expected %d results, got %d", round, expected, len(results))
		}
		for _, r := range results {
			if r.Err != nil {
				t.Fatalf("round %d: unexpected error: %v", round, r.Err)
			}
		}
	}
}

func TestRace_ShardedGroup_GoKeyed_Concurrent(t *testing.T) {
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
					err := sg.GoKeyed(k, ctx, func(ctx context.Context) (int, error) {
						return 1, nil
					})
					if err == nil {
						submitted.Add(1)
					}
				}
			}(key)
		}
		wg.Wait()
		results := sg.Wait()

		expected := int(submitted.Load())
		if len(results) != expected {
			t.Fatalf("round %d: expected %d results, got %d", round, expected, len(results))
		}
	}
}

func TestRace_ShardedGroup_GoBatch_Concurrent(t *testing.T) {
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

	expected := int(submitted.Load())
	if len(results) != expected {
		t.Fatalf("expected %d results, got %d", expected, len(results))
	}
}

func TestRace_ShardedGroup_WithStreaming_Concurrent(t *testing.T) {
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
			t.Fatalf("round %d: expected %d results, got %d", round, expected, len(results))
		}
		if sc := streamedCount.Load(); sc != int64(expected) {
			t.Fatalf("round %d: streamed count %d != %d", round, sc, expected)
		}
	}
}

func TestRace_ShardedGroup_WaitTimeout_Race(t *testing.T) {
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

// ==================== 极限高并发 1M 级别测试 ====================

func TestStress_ShardedPool_1M_Tasks(t *testing.T) {
	printMemStatsShard("1M-SPool-start")
	n := 1_000_000
	batchSize := 100_000
	batches := n / batchSize
	var grandTotal atomic.Int64

	for batch := 0; batch < batches; batch++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 16, SizePerShard: 16})
		ctx := context.Background()
		var submitted atomic.Int64

		for i := 0; i < batchSize; i++ {
			idx := i
			err := sp.Submit(ctx, func(ctx context.Context) (int, error) {
				return idx * 2, nil
			})
			if err == nil {
				submitted.Add(1)
			}
		}
		results := sp.Wait()
		sp.Close()

		expected := int(submitted.Load())
		if len(results) != expected {
			t.Fatalf("batch %d: expected %d results, got %d", batch+1, expected, len(results))
		}
		for _, r := range results {
			if !r.Ok() {
				t.Fatalf("batch %d: task failed: %v", batch+1, r.Err)
			}
		}
		grandTotal.Add(int64(expected))
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("1M ShardedPool: %d tasks total", grandTotal.Load())
	printMemStatsShard("1M-SPool-end")
}

func TestStress_ShardedGroup_1M_Tasks(t *testing.T) {
	printMemStatsShard("1M-SGroup-start")
	n := 1_000_000
	batchSize := 100_000
	batches := n / batchSize
	var grandTotal atomic.Int64

	for batch := 0; batch < batches; batch++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 16, ConcurrencyPerShard: 16})
		ctx := context.Background()
		var submitted atomic.Int64

		for i := 0; i < batchSize; i++ {
			idx := i
			err := sg.Go(ctx, func(ctx context.Context) (int, error) {
				return idx * 2, nil
			})
			if err == nil {
				submitted.Add(1)
			}
		}
		results := sg.Wait()

		expected := int(submitted.Load())
		if len(results) != expected {
			t.Fatalf("batch %d: expected %d results, got %d", batch+1, expected, len(results))
		}
		grandTotal.Add(int64(expected))
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("1M ShardedGroup: %d tasks total", grandTotal.Load())
	printMemStatsShard("1M-SGroup-end")
}

// ==================== 溢出策略真实测试 ====================

func TestStress_ShardedPool_OverflowDrop(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 2})
	sp.WithRingBuffer(500, core.OverflowDrop)
	defer sp.Close()

	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	sp.Wait()
	flushed := sp.Flush(5000)
	if len(flushed) < 1 {
		t.Fatal("Flush returned empty")
	}
	t.Logf("OverflowDrop: flushed %d results (capacity=500, submitted=10000)", len(flushed))
}

func TestStress_ShardedPool_OverflowBlock_Flush(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 8})
	sp.WithRingBuffer(2000, core.OverflowBlock)
	defer sp.Close()

	ctx := context.Background()
	for i := 0; i < 5000; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	sp.Wait()
	first := sp.Flush(1000)
	second := sp.Flush(5000)
	total := len(first) + len(second)
	if total != 5000 {
		t.Fatalf("OverflowBlock: expected 5000 total flushed, got %d", total)
	}
	t.Logf("OverflowBlock: batch1=%d batch2=%d total=%d", len(first), len(second), total)
}
