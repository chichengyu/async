package async

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// ========== BoundedRunner 正确性 + 竞态 ==========

func TestBoundedRunner_Basic(t *testing.T) {
	runner := NewBoundedRunner(10)
	ctx := context.Background()

	var results []*AsyncResult[int]
	for i := 0; i < 1000; i++ {
		idx := i
		results = append(results, BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
			return idx * 2, nil
		}))
	}

	success, fail := 0, 0
	for _, ar := range results {
		val, err := ar.Wait()
		if err != nil {
			fail++
		} else if val%2 == 0 {
			success++
		}
	}

	if fail > 0 {
		t.Fatalf("failures=%d", fail)
	}
	if success != 1000 {
		t.Fatalf("expected 1000 successes, got %d", success)
	}

	t.Logf("[BoundedRunner] Basic: max=%d successes=%d busy=%d", runner.Max(), success, runner.Busy())
}

func TestBoundedRunner_Race(t *testing.T) {
	runner := NewBoundedRunner(50)
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				idx := gid*100 + i
				ar := BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
					return idx, nil
				})
				val, err := ar.Wait()
				if err != nil || val != idx {
					panic(fmt.Sprintf("mismatch: expected %d, got %d err=%v", idx, val, err))
				}
			}
		}(g)
	}

	wg.Wait()
	t.Logf("[BoundedRunner] Race: 100 goroutines × 100 tasks, max=%d busy=%d", runner.Max(), runner.Busy())
}

// ========== ParallelPipeline 正确性 ==========

func TestParallelPipeline_Basic(t *testing.T) {
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
		{Name: "add_one", Concurrency: 2},
	}

	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// 无分片
	p := NewParallelPipeline(stages)
	results, err := p.Execute(context.Background(), items, func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "double":
			return item * 2, nil
		case "add_one":
			return item + 1, nil
		}
		return item, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}

	sum := 0
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("unexpected error in result: %v", r.Err)
		}
		sum += r.Value
	}
	// each: (n*2)+1 → 3,5,7,9,11,13,15,17,19,21 = 120
	if sum != 120 {
		t.Fatalf("expected sum=120, got %d", sum)
	}
	t.Logf("[ParallelPipeline] NoShard: sum=%d ✓", sum)
}

func TestParallelPipeline_Shard(t *testing.T) {
	stages := []Stage[int]{
		{Name: "square", Concurrency: 8},
	}

	items := make([]int, 10000)
	for i := range items {
		items[i] = i
	}

	p := NewParallelPipeline(stages).Shard(4)

	if p.ShardCount() != 4 {
		t.Fatalf("expected shardCount=4, got %d", p.ShardCount())
	}

	results, err := p.Execute(context.Background(), items, func(ctx context.Context, stage string, item int) (int, error) {
		return item * item, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 10000 {
		t.Fatalf("expected 10000 results, got %d", len(results))
	}

	fail := 0
	for _, r := range results {
		if r.Err != nil {
			fail++
		}
	}
	if fail > 0 {
		t.Fatalf("failures=%d", fail)
	}

	t.Logf("[ParallelPipeline] Shard(4): items=%d results=%d ✓", len(items), len(results))
}

func TestParallelPipeline_DefaultShard(t *testing.T) {
	stages := []Stage[int]{
		{Name: "noop", Concurrency: 2},
	}

	p := NewParallelPipeline(stages).DefaultShard()

	expected := runtime.GOMAXPROCS(0)
	if expected < 2 {
		expected = 2
	}
	if p.ShardCount() != expected {
		t.Fatalf("expected shardCount=%d, got %d", expected, p.ShardCount())
	}

	results, _ := p.Execute(context.Background(), []int{1, 2, 3}, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	t.Logf("[ParallelPipeline] DefaultShard: GOMAXPROCS=%d shards=%d ✓", runtime.GOMAXPROCS(0), p.ShardCount())
}

func TestParallelPipeline_Meta(t *testing.T) {
	stages := []Stage[int]{
		{Name: "a", Concurrency: 2},
		{Name: "b", Concurrency: 2},
	}

	p := NewParallelPipeline(stages).Shard(2)

	meta := p.ExecuteWithMeta(context.Background(), []int{1, 2, 3}, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})

	if len(meta) != 6 {
		t.Fatalf("expected 6 meta results (3×2 stages), got %d", len(meta))
	}

	t.Logf("[ParallelPipeline] ExecuteWithMeta: %d results ✓", len(meta))
}

// ========== BoundedRunner 压力 ==========

func TestBoundedRunner_500K_Stress(t *testing.T) {
	runner := NewBoundedRunner(200)
	ctx := context.Background()

	var results []*AsyncResult[int]
	n := 500_000

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		results = append(results, BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		}))
	}

	for _, ar := range results {
		_, _ = ar.Wait()
	}
	elapsed := time.Since(start)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[BoundedRunner] 500K: max=%d tasks=%d in %v (%.0f ops/s)", runner.Max(), n, elapsed, opsPerSec)
}
