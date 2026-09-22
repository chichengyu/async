package async

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

// ============================================================================
// BoundedRunner / ParallelPipeline 千万级生产压力测试 + Race 检测
//
// 运行方式：
//   go test -run "TestProduction_BR|TestProduction_PP" -v -count=1 -timeout 60m .
//   go test -run "TestProduction_BR|TestProduction_PP" -v -count=1 -timeout 60m -race .
// ============================================================================

func prodMemStats(tag string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("[%s] G=%d Heap=%dMB Obj=%d\n",
		tag, runtime.NumGoroutine(), m.HeapAlloc/1024/1024, m.HeapObjects)
}

// ============================================================================
// BoundedRunner 千万级
// ============================================================================

func TestProduction_BoundedRunner_10M(t *testing.T) {
	prodMemStats("BR-10M-start")
	runner := NewBoundedRunner(500)
	ctx := context.Background()
	n := 10_000_000

	var ars []*AsyncResult[int]
	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		ars = append(ars, BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		}))
	}

	var success, fail int64
	var doneWg sync.WaitGroup
	doneWg.Add(4)
	for w := 0; w < 4; w++ {
		go func(start, end int) {
			defer doneWg.Done()
			for j := start; j < end; j++ {
				_, err := ars[j].Wait()
				if err != nil {
					atomic.AddInt64(&fail, 1)
				} else {
					atomic.AddInt64(&success, 1)
				}
			}
		}(w*n/4, (w+1)*n/4)
	}
	doneWg.Wait()

	elapsed := time.Since(start)
	opsPerSec := float64(n) / elapsed.Seconds()

	if fail > 0 {
		t.Fatalf("[10M] BoundedRunner: %d failures", fail)
	}
	if success != int64(n) {
		t.Fatalf("expected %d successes, got %d", n, success)
	}

	t.Logf("[10M] BoundedRunner(max=500): tasks=%d in %v (%.0f ops/s) success=%d fail=%d",
		n, elapsed, opsPerSec, success, fail)
	prodMemStats("BR-10M-end")
}

func TestProduction_BoundedRunner_500K_Race(t *testing.T) {
	runner := NewBoundedRunner(500)
	ctx := context.Background()
	concurrency := 100
	tasksPerGoroutine := 5_000
	n := concurrency * tasksPerGoroutine

	var ars []*AsyncResult[int]
	ars = make([]*AsyncResult[int], n)

	var submitWg sync.WaitGroup
	submitWg.Add(concurrency)
	for g := 0; g < concurrency; g++ {
		go func(gid int) {
			defer submitWg.Done()
			start := gid * tasksPerGoroutine
			for tid := 0; tid < tasksPerGoroutine; tid++ {
				idx := start + tid
				ars[idx] = BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
					return idx, nil
				})
			}
		}(g)
	}
	submitWg.Wait()

	var success, fail int64
	var doneWg sync.WaitGroup
	doneWg.Add(8)
	for w := 0; w < 8; w++ {
		go func(start, end int) {
			defer doneWg.Done()
			for j := start; j < end; j++ {
				_, err := ars[j].Wait()
				if err != nil {
					atomic.AddInt64(&fail, 1)
				} else {
					atomic.AddInt64(&success, 1)
				}
			}
		}(w*n/8, (w+1)*n/8)
	}
	doneWg.Wait()

	if fail > 0 {
		t.Fatalf("[Race 500K] BoundedRunner: %d failures", fail)
	}
	if success != int64(n) {
		t.Fatalf("expected %d successes, got %d", n, success)
	}

	t.Logf("[Race 500K] BoundedRunner: %d goroutines × %d tasks, max=%d success=%d",
		concurrency, tasksPerGoroutine, runner.Max(), success)
}

func TestProduction_BoundedRunner_10M_Race(t *testing.T) {
	runner := NewBoundedRunner(500)
	ctx := context.Background()
	concurrency := 100
	tasksPerGoroutine := 100_000
	n := concurrency * tasksPerGoroutine

	var ars []*AsyncResult[int]
	ars = make([]*AsyncResult[int], n)

	var submitWg sync.WaitGroup
	submitWg.Add(concurrency)
	for g := 0; g < concurrency; g++ {
		go func(gid int) {
			defer submitWg.Done()
			start := gid * tasksPerGoroutine
			for tid := 0; tid < tasksPerGoroutine; tid++ {
				idx := start + tid
				ars[idx] = BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
					return idx, nil
				})
			}
		}(g)
	}
	submitWg.Wait()

	var success, fail int64
	var doneWg sync.WaitGroup
	doneWg.Add(8)
	for w := 0; w < 8; w++ {
		go func(start, end int) {
			defer doneWg.Done()
			for j := start; j < end; j++ {
				_, err := ars[j].Wait()
				if err != nil {
					atomic.AddInt64(&fail, 1)
				} else {
					atomic.AddInt64(&success, 1)
				}
			}
		}(w*n/8, (w+1)*n/8)
	}
	doneWg.Wait()

	if fail > 0 {
		t.Fatalf("[Race] BoundedRunner: %d failures", fail)
	}
	if success != int64(n) {
		t.Fatalf("expected %d successes, got %d", n, success)
	}

	t.Logf("[Race] BoundedRunner: %d goroutines × %d tasks, max=%d success=%d fail=%d",
		concurrency, tasksPerGoroutine, runner.Max(), success, fail)
}

// ============================================================================
// BoundedRunner Goroutine 泄漏
// ============================================================================

func TestProduction_BoundedRunner_Leak(t *testing.T) {
	before := runtime.NumGoroutine()
	prodMemStats("BR-Leak-before")

	for round := 0; round < 10; round++ {
		runner := NewBoundedRunner(200)
		ctx := context.Background()

		for i := 0; i < 500000; i++ {
			idx := i
			BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
				return idx, nil
			})
		}

		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}

	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	after := runtime.NumGoroutine()

	prodMemStats("BR-Leak-after")
	t.Logf("[Leak] BoundedRunner: goroutines before=%d after=%d diff=%d", before, after, after-before)

	if after-before > 100 {
		t.Fatalf("goroutine leak: %d goroutines leaked after 10 rounds", after-before)
	}
}

// ============================================================================
// ParallelPipeline 千万级
// ============================================================================

func TestProduction_ParallelPipeline_10M_Shard(t *testing.T) {
	prodMemStats("PP-10M-start")
	stages := []Stage[int]{
		{Name: "double", Concurrency: 200},
		{Name: "square", Concurrency: 200},
	}
	n := 10_000_000
	items := make([]int, n)
	for i := range items {
		items[i] = i % 1000
	}

	p := NewParallelPipeline(stages).Shard(8)
	ctx := context.Background()

	start := time.Now()
	results, err := p.Execute(ctx, items, func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "double":
			return item * 2, nil
		case "square":
			return item * item, nil
		}
		return item, nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	var fail int64
	for _, r := range results {
		if r.Err != nil {
			fail++
		}
	}
	if fail > 0 {
		t.Fatalf("failures=%d", fail)
	}

	opsPerSec := float64(n*2) / elapsed.Seconds()
	t.Logf("[10M] ParallelPipeline Shard(8): stages=2 items=%d in %v (%.0f ops/s) fail=%d",
		n, elapsed, opsPerSec, fail)
	prodMemStats("PP-10M-end")
}

func TestProduction_ParallelPipeline_5M_MultiStage(t *testing.T) {
	prodMemStats("PP-5M-Multi-start")
	stages := []Stage[int]{
		{Name: "add", Concurrency: 100},
		{Name: "multiply", Concurrency: 100},
		{Name: "mod", Concurrency: 100},
		{Name: "identity", Concurrency: 100},
	}
	n := 5_000_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	p := NewParallelPipeline(stages).Shard(8)
	ctx := context.Background()

	start := time.Now()
	results, err := p.Execute(ctx, items, func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "add":
			return item + 1, nil
		case "multiply":
			return item * 2, nil
		case "mod":
			return item % 100, nil
		case "identity":
			return item, nil
		}
		return item, nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	var fail int64
	for _, r := range results {
		if r.Err != nil {
			fail++
		}
	}
	if fail > 0 {
		t.Fatalf("failures=%d", fail)
	}

	opsPerSec := float64(n*4) / elapsed.Seconds()
	t.Logf("[5M] ParallelPipeline Shard(8): stages=4 items=%d in %v (%.0f ops/s) fail=%d",
		n, elapsed, opsPerSec, fail)
	prodMemStats("PP-5M-Multi-end")
}

// ============================================================================
// Pipeline 竞态安全
// ============================================================================

func TestProduction_ParallelPipeline_Race_MultiPipeline(t *testing.T) {
	for round := 0; round < 30; round++ {
		stages := []Stage[int]{
			{Name: "noop", Concurrency: 20},
		}
		n := 5000
		items := make([]int, n)
		for i := range items {
			items[i] = i
		}

		var wg sync.WaitGroup
		var mu sync.Mutex
		var allResults [][]core.Result[int]

		wg.Add(4)
		for g := 0; g < 4; g++ {
			go func() {
				defer wg.Done()
				p := NewParallelPipeline(stages).Shard(4)
				results, _ := p.Execute(context.Background(), items, func(ctx context.Context, stage string, item int) (int, error) {
					return item, nil
				})
				mu.Lock()
				allResults = append(allResults, results)
				mu.Unlock()
			}()
		}
		wg.Wait()

		for _, r := range allResults {
			if len(r) != n {
				t.Fatalf("round %d: expected %d results, got %d", round, n, len(r))
			}
		}
	}

	runtime.GC()
	time.Sleep(300 * time.Millisecond)
	t.Logf("[Race] ParallelPipeline: 30 rounds × 4 parallel pipelines passed")
}

func TestProduction_ParallelPipeline_Race_ShardThenExecute(t *testing.T) {
	for round := 0; round < 50; round++ {
		stages := []Stage[int]{
			{Name: "round", Concurrency: 10},
		}
		items := make([]int, 1000)
		for i := range items {
			items[i] = round
		}

		p := NewParallelPipeline(stages).Shard(4)

		var wg sync.WaitGroup
		var mu sync.Mutex
		var allResults [][]core.Result[int]

		wg.Add(4)
		for g := 0; g < 4; g++ {
			go func() {
				defer wg.Done()
				results, _ := p.Execute(context.Background(), items, func(ctx context.Context, stage string, item int) (int, error) {
					return item + 1, nil
				})
				mu.Lock()
				allResults = append(allResults, results)
				mu.Unlock()
			}()
		}
		wg.Wait()

		for _, r := range allResults {
			if len(r) != 1000 {
				t.Fatalf("round %d: expected 1000 results, got %d", round, len(r))
			}
		}
	}

	runtime.GC()
	time.Sleep(300 * time.Millisecond)
	t.Logf("[Race] ParallelPipeline: 50 rounds × 4 concurrent Execute")
}

// ============================================================================
// 便捷函数
// ============================================================================

func TestProduction_Convenience_ShardParallelPipeline(t *testing.T) {
	stages := []Stage[int]{{Name: "x", Concurrency: 4}}
	p := NewParallelPipeline(stages)
	p = ShardParallelPipeline(p, 16)

	if p.ShardCount() != 16 {
		t.Fatalf("expected 16 shards, got %d", p.ShardCount())
	}

	results, _ := p.Execute(context.Background(), []int{1, 2, 3}, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	t.Logf("[Conv] ShardParallelPipeline: %d shards ✓", p.ShardCount())
}

func TestProduction_Convenience_DefaultShardParallelPipeline(t *testing.T) {
	stages := []Stage[int]{{Name: "x", Concurrency: 4}}
	p := NewParallelPipeline(stages)
	p = DefaultShardParallelPipeline(p)

	expected := runtime.GOMAXPROCS(0)
	if expected < 2 {
		expected = 2
	}
	if p.ShardCount() != expected {
		t.Fatalf("expected %d shards, got %d", expected, p.ShardCount())
	}

	results, _ := p.Execute(context.Background(), []int{1, 2}, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	t.Logf("[Conv] DefaultShardParallelPipeline: GOMAXPROCS=%d shards=%d ✓",
		runtime.GOMAXPROCS(0), p.ShardCount())
}

func TestProduction_Convenience_NewDefaultBoundedRunner(t *testing.T) {
	runner := NewDefaultBoundedRunner()
	if runner.Max() <= 0 {
		t.Fatalf("expected positive max, got %d", runner.Max())
	}

	ctx := context.Background()
	ar := BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	})
	val, err := ar.Wait()
	if err != nil || val != 42 {
		t.Fatalf("expected 42, got %d err=%v", val, err)
	}

	t.Logf("[Conv] NewDefaultBoundedRunner: max=%d ✓", runner.Max())
}

// ============================================================================
// ParallelPipeline Goroutine 泄漏
// ============================================================================

func TestProduction_ParallelPipeline_Leak(t *testing.T) {
	before := runtime.NumGoroutine()
	prodMemStats("PP-Leak-before")

	for round := 0; round < 10; round++ {
		stages := []Stage[int]{{Name: "x", Concurrency: 50}}
		p := NewParallelPipeline(stages).Shard(4)
		items := make([]int, 10000)
		for i := range items {
			items[i] = i
		}
		results, _ := p.Execute(context.Background(), items, func(ctx context.Context, stage string, item int) (int, error) {
			return item, nil
		})
		_ = results

		runtime.GC()
		time.Sleep(30 * time.Millisecond)
	}

	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	after := runtime.NumGoroutine()

	prodMemStats("PP-Leak-after")
	t.Logf("[Leak] ParallelPipeline: goroutines before=%d after=%d diff=%d", before, after, after-before)

	if after-before > 50 {
		t.Fatalf("goroutine leak: %d goroutines leaked after 10 rounds", after-before)
	}
}

// ============================================================================
// 性能对比：Shard vs NoShard Pipeline
// ============================================================================

func TestProduction_ParallelPipeline_1M_Compare(t *testing.T) {
	n := 1_000_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	stages := []Stage[int]{{Name: "double", Concurrency: 200}}

	ctx := context.Background()

	// ── NoShard ──
	t.Logf("[1M] Pipeline NoShard start...")
	pNoShard := NewParallelPipeline(stages)
	startNoShard := time.Now()
	resultsNoShard, _ := pNoShard.Execute(ctx, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item * 2, nil
	})
	elapsedNoShard := time.Since(startNoShard)

	failNoShard := 0
	for _, r := range resultsNoShard {
		if r.Err != nil {
			failNoShard++
		}
	}
	opsNoShard := float64(n) / elapsedNoShard.Seconds()

	runtime.GC()
	time.Sleep(200 * time.Millisecond)

	// ── Shard(8) ──
	t.Logf("[1M] Pipeline Shard(8) start...")
	pShard := NewParallelPipeline(stages).Shard(8)
	startShard := time.Now()
	resultsShard, _ := pShard.Execute(ctx, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item * 2, nil
	})
	elapsedShard := time.Since(startShard)

	failShard := 0
	for _, r := range resultsShard {
		if r.Err != nil {
			failShard++
		}
	}
	opsShard := float64(n) / elapsedShard.Seconds()
	speedup := opsShard / opsNoShard

	t.Logf("[1M] Pipeline NoShard: %v (%.0f ops/s) fail=%d", elapsedNoShard, opsNoShard, failNoShard)
	t.Logf("[1M] Pipeline Shard(8): %v (%.0f ops/s) fail=%d", elapsedShard, opsShard, failShard)
	t.Logf("[1M] Speedup: %.2f× | Shard(8) is %.0f%% faster", speedup, (speedup-1)*100)
}
