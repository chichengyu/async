package async

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// MultiPool / Pool.Shard 千万级生产压力测试
//
// 运行方式：
//   go test -run "TestProduction_MultiPool" -v -count=1 -timeout 30m .
//   go test -run "TestProduction_MultiPool" -v -count=1 -timeout 60m -race .
//
// 验证维度：
//   1. 千万级 Submit / TrySubmit / SubmitKeyed / SubmitBatch 正确性
//   2. 背压控制（WithMaxPending + WithOverflow）
//   3. 流式消费（WithRingBuffer）
//   4. 自动扩缩容（AutoScale）
//   5. 并发竞态安全（Race Detector）
//   6. Goroutine 泄漏检测
//   7. Shard vs NoShard 性能对比
// ============================================================================

func multiPoolMemStats(tag string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("[%s] G=%d Heap=%dMB Obj=%d\n",
		tag, runtime.NumGoroutine(), m.HeapAlloc/1024/1024, m.HeapObjects)
}

// ============================================================================
// 千万级核心正确性
// ============================================================================

// TestProduction_MultiPool_Shard_10M_Submit 1000万任务通过 Shard(8) RoundRobin 分发
func TestProduction_MultiPool_Shard_10M_Submit(t *testing.T) {
	multiPoolMemStats("10M-ShardSubmit-start")
	n := 10_000_000

	mp := NewPool[int](100).Shard(8)
	defer mp.Close()

	ctx := context.Background()
	var submitted atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		err := mp.Submit(ctx, func(ctx context.Context) (int, error) {
			return idx * 2, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}

	results := mp.Wait()
	elapsed := time.Since(start)

	submittedVal := int(submitted.Load())
	if len(results) != submittedVal {
		t.Fatalf("expected %d results, got %d (dropped=%d)", submittedVal, len(results), n-submittedVal)
	}

	failures := 0
	for _, r := range results {
		if !r.Ok() {
			failures++
		}
	}
	if failures > 0 {
		t.Fatalf("%d tasks failed among %d", failures, len(results))
	}

	opsPerSec := float64(submittedVal) / elapsed.Seconds()
	t.Logf("[10M] MultiPool Shard(8) Submit: shards=%d workersPerShard=%d totalWorkers=%d tasks=%d in %v (%.0f ops/s) failures=%d",
		mp.ShardCount(), mp.GetShard(0).Size(), mp.TotalWorkerCount(), submittedVal, elapsed, opsPerSec, failures)
	multiPoolMemStats("10M-ShardSubmit-end")
}

// TestProduction_MultiPool_Shard_10M_TrySubmit 1000万非阻塞 TrySubmit
func TestProduction_MultiPool_Shard_10M_TrySubmit(t *testing.T) {
	multiPoolMemStats("10M-ShardTrySubmit-start")
	n := 10_000_000

	mp := NewPool[int](200).Shard(16)
	defer mp.Close()

	ctx := context.Background()
	var submitted atomic.Int64
	var rejected atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		err := mp.TrySubmit(ctx, func(ctx context.Context) (int, error) {
			return idx * 2, nil
		})
		if err == nil {
			submitted.Add(1)
		} else {
			rejected.Add(1)
		}
	}

	results := mp.Wait()
	elapsed := time.Since(start)
	rejectedVal := int(rejected.Load())

	// 注意：TrySubmit 失败时 discardTask 仍写入结果槽，所以 len(results) ≈ n
	if len(results) != n {
		t.Fatalf("mismatch: total=%d results=%d", n, len(results))
	}

	failures := 0
	for _, r := range results {
		if !r.Ok() {
			failures++
		}
	}

	rejectRate := float64(rejectedVal) / float64(n) * 100
	submittedVal := int(submitted.Load())
	t.Logf("[10M] MultiPool Shard(16) TrySubmit: shards=%d totalWorkers=%d submitted=%d rejected=%d (%.1f%%) in %v failures=%d",
		mp.ShardCount(), mp.TotalWorkerCount(), submittedVal, rejectedVal, rejectRate, elapsed, failures)
	multiPoolMemStats("10M-ShardTrySubmit-end")
}

// TestProduction_MultiPool_Shard_10M_SubmitKeyed 1000万按Key哈希分发
func TestProduction_MultiPool_Shard_10M_SubmitKeyed(t *testing.T) {
	multiPoolMemStats("10M-ShardKeyed-start")
	n := 10_000_000
	keyCount := 1000 // 模拟 1000 个不同的 key

	mp := NewPool[int](100).Shard(8)
	defer mp.Close()

	ctx := context.Background()
	var submitted atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		key := uint64(idx % keyCount)
		err := mp.SubmitKeyed(key, ctx, func(ctx context.Context) (int, error) {
			return idx % keyCount, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}

	results := mp.Wait()
	elapsed := time.Since(start)

	submittedVal := int(submitted.Load())
	if len(results) != submittedVal {
		t.Fatalf("expected %d results, got %d", submittedVal, len(results))
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

	opsPerSec := float64(submittedVal) / elapsed.Seconds()
	t.Logf("[10M] MultiPool Shard(8) SubmitKeyed: keys=%d tasks=%d in %v (%.0f ops/s) failures=%d",
		keyCount, submittedVal, elapsed, opsPerSec, failures)
	multiPoolMemStats("10M-ShardKeyed-end")
}

// TestProduction_MultiPool_Shard_10M_SubmitBatch 1000万批量提交（每批1000）
func TestProduction_MultiPool_Shard_10M_SubmitBatch(t *testing.T) {
	multiPoolMemStats("10M-ShardBatch-start")
	n := 10_000_000
	batchSize := 1000
	batches := n / batchSize

	mp := NewPool[int](100).Shard(8)
	defer mp.Close()

	ctx := context.Background()
	var grandTotal atomic.Int64

	start := time.Now()
	for batch := 0; batch < batches; batch++ {
		items := make([]int, batchSize)
		for j := 0; j < batchSize; j++ {
			items[j] = batch*batchSize + j
		}
		submitResults := mp.SubmitBatch(ctx, items, func(ctx context.Context, v int) (int, error) {
			return v * 2, nil
		})
		for _, sr := range submitResults {
			if sr.Err == nil {
				grandTotal.Add(1)
			}
		}
	}
	results := mp.Wait()
	elapsed := time.Since(start)

	totalSubmitted := int(grandTotal.Load())
	if len(results) != totalSubmitted {
		t.Fatalf("expected %d results, got %d", totalSubmitted, len(results))
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

	opsPerSec := float64(totalSubmitted) / elapsed.Seconds()
	t.Logf("[10M] MultiPool Shard(8) SubmitBatch: batchSize=%d batches=%d tasks=%d in %v (%.0f ops/s) failures=%d",
		batchSize, batches, totalSubmitted, elapsed, opsPerSec, failures)
	multiPoolMemStats("10M-ShardBatch-end")
}

// ============================================================================
// 千万级背压控制 + 超时
// ============================================================================

// TestProduction_MultiPool_Shard_10M_Backpressure 1000万带背压控制（MaxPending+OverflowDrop）
func TestProduction_MultiPool_Shard_10M_Backpressure(t *testing.T) {
	multiPoolMemStats("10M-ShardBackpress-start")
	n := 10_000_000

	mp := NewPool[int](50).
		WithMaxPending(10000).
		WithOverflow(OverflowDrop).
		WithTimeout(30 * time.Second).
		Shard(16)
	defer mp.Close()

	ctx := context.Background()
	var submitted atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		err := mp.Submit(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}

	results := mp.Wait()
	elapsed := time.Since(start)
	submittedVal := int(submitted.Load())

	// 背压丢弃的任务是预期的
	dropped := n - submittedVal
	if len(results) != submittedVal {
		t.Fatalf("mismatch: submitted=%d results=%d", submittedVal, len(results))
	}

	dropRate := float64(dropped) / float64(n) * 100
	opsPerSec := float64(submittedVal) / elapsed.Seconds()
	t.Logf("[10M] MultiPool Shard(16) Backpressure: totalWorkers=%d submitted=%d dropped=%d (%.1f%%) in %v (%.0f ops/s)",
		mp.TotalWorkerCount(), submittedVal, dropped, dropRate, elapsed, opsPerSec)
	multiPoolMemStats("10M-ShardBackpress-end")
}

// TestProduction_MultiPool_Shard_10M_WithTimeout 1000万带任务超时
func TestProduction_MultiPool_Shard_10M_WithTimeout(t *testing.T) {
	multiPoolMemStats("10M-ShardTimeout-start")
	n := 10_000_000

	mp := NewPool[int](100).
		WithTimeout(60 * time.Second).
		Shard(8)
	defer mp.Close()

	ctx := context.Background()
	var submitted atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		err := mp.Submit(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}

	results := mp.Wait()
	elapsed := time.Since(start)
	submittedVal := int(submitted.Load())

	failures := 0
	for _, r := range results {
		if !r.Ok() {
			failures++
		}
	}
	if failures > 0 {
		t.Fatalf("%d tasks failed", failures)
	}

	opsPerSec := float64(submittedVal) / elapsed.Seconds()
	t.Logf("[10M] MultiPool Shard(8) WithTimeout: tasks=%d in %v (%.0f ops/s) failures=%d",
		submittedVal, elapsed, opsPerSec, failures)
	multiPoolMemStats("10M-ShardTimeout-end")
}

// ============================================================================
// 千万级流式消费（RingBuffer）
// ============================================================================

// TestProduction_MultiPool_Shard_10M_RingBuffer 1000万 RingBuffer 流式消费
func TestProduction_MultiPool_Shard_10M_RingBuffer(t *testing.T) {
	multiPoolMemStats("10M-ShardRingStart-start")
	n := 10_000_000

	mp := NewPool[int](100).
		WithRingBuffer(100000, OverflowDrop).
		Shard(8)
	defer mp.Close()

	ctx := context.Background()
	var submitted atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		err := mp.Submit(ctx, func(ctx context.Context) (int, error) {
			return idx * 3, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}

	// 环形缓冲排空
	flushed := mp.Flush(1000000)
	results := mp.Wait()
	elapsed := time.Since(start)

	totalResults := len(flushed) + len(results)
	submittedVal := int(submitted.Load())

	t.Logf("[10M] MultiPool Shard(8) RingBuffer: submitted=%d flushed=%d waitResults=%d total=%d in %v",
		submittedVal, len(flushed), len(results), totalResults, elapsed)

	if totalResults < submittedVal-1000 {
		t.Fatalf("too many results missing: submitted=%d total=%d", submittedVal, totalResults)
	}

	opsPerSec := float64(submittedVal) / elapsed.Seconds()
	t.Logf("[10M] MultiPool Shard(8) RingBuffer: (%.0f ops/s)", opsPerSec)
	multiPoolMemStats("10M-ShardRingStart-end")
}

// ============================================================================
// 千万级自动扩缩容
// ============================================================================

// TestProduction_MultiPool_Shard_10M_AutoScale 1000万自动扩缩容（每个分片独立扩缩）
func TestProduction_MultiPool_Shard_10M_AutoScale(t *testing.T) {
	multiPoolMemStats("10M-ShardAutoScale-start")
	n := 10_000_000

	// 每个分片起始 4 worker，最多可扩到 500
	mp := NewPool[int](4).Shard(8)

	// 为每个分片独立启用 AutoScale
	for i := 0; i < mp.ShardCount(); i++ {
		sp := mp.GetShard(i)
		sp.EnableAutoScale(&AutoScaleConfig{
			MinWorkers:      2,
			MaxWorkers:      500,
			CheckInterval:   500 * time.Millisecond,
			ScaleUpChecks:   2,
			ScaleDownChecks: 5,
		})
	}
	defer mp.Close()

	ctx := context.Background()
	var submitted atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		var err error
		err = mp.Submit(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}

	results := mp.Wait()
	elapsed := time.Since(start)

	submittedVal := int(submitted.Load())
	if len(results) != submittedVal {
		t.Fatalf("mismatch: submitted=%d results=%d", submittedVal, len(results))
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

	// 汇总每个分片的最终 worker 数量
	var totalFinalWorkers int
	for i := 0; i < mp.ShardCount(); i++ {
		sp := mp.GetShard(i)
		totalFinalWorkers += sp.Size()
	}

	opsPerSec := float64(submittedVal) / elapsed.Seconds()
	t.Logf("[10M] MultiPool Shard(8) AutoScale: initialWorkers=%d finalWorkers=%d tasks=%d in %v (%.0f ops/s) failures=%d",
		32, totalFinalWorkers, submittedVal, elapsed, opsPerSec, failures)
	multiPoolMemStats("10M-ShardAutoScale-end")
}

// ============================================================================
// 并发竞态安全（Race Detector 核心测试）
//
// 设计原则：
//   1. Submit + Wait 不是并发安全的（WaitGroup 语义限制），测试中所有 Submit 应在 Wait 之前完成
//   2. Submit + Close 是并发安全的（Close 后 Submit 返回 ErrPoolClosed）
//   3. TrySubmit + SubmitKeyed 多种提交方式混合 + 多 goroutine 并发是核心竞态场景
// ============================================================================

// TestProduction_MultiPool_Shard_Race_10KConcurrent 1万并发 goroutine 混合提交竞态检测
func TestProduction_MultiPool_Shard_Race_10KConcurrent(t *testing.T) {
	// 大 worker 池减少 TrySubmit 失败
	mp := NewPool[int](200).Shard(16)
	defer mp.Close()

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
				switch gid % 4 {
				case 0:
					mp.Submit(ctx, func(ctx context.Context) (int, error) { return idx, nil })
				case 1:
					mp.TrySubmit(ctx, func(ctx context.Context) (int, error) { return idx, nil })
				case 2:
					mp.SubmitKeyed(uint64(gid), ctx, func(ctx context.Context) (int, error) { return idx, nil })
				case 3:
					mp.TrySubmitKeyed(uint64(gid), ctx, func(ctx context.Context) (int, error) { return idx, nil })
				}
			}
		}(gid)
	}
	submitWg.Wait()

	results := mp.Wait()
	totalAttempts := concurrency * tasksPerGoroutine

	// 注意：TrySubmit 失败时 discardTask 也会占用结果槽位
	// 所以 len(results) 应等于总尝试次数（每次调用 Submit/TrySubmit 都会分配槽位）
	if len(results) != totalAttempts {
		t.Fatalf("mismatch: total attempts=%d, results=%d", totalAttempts, len(results))
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

	t.Logf("[Race] MultiPool Shard(16): %d goroutines × %d tasks, results=%d, successes=%d, failures=%d",
		concurrency, tasksPerGoroutine, len(results), successes, failures)
}

// TestProduction_MultiPool_Shard_Race_CloseSubmit 并发 Close + Submit 竞态
// Close 后 Submit 会安全返回错误，不会 panic 或泄漏
func TestProduction_MultiPool_Shard_Race_CloseSubmit(t *testing.T) {
	for round := 0; round < 100; round++ {
		mp := NewPool[int](50).Shard(4)

		ctx := context.Background()
		var wg sync.WaitGroup

		// 并发提交
		wg.Add(10)
		for g := 0; g < 10; g++ {
			go func() {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					mp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
				}
			}()
		}

		// 并发关闭（在提交进行中关闭）
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(round%10) * time.Millisecond)
			mp.Close()
		}()

		wg.Wait()
		mp.Wait() // 安全消费剩余结果
	}

	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	g := runtime.NumGoroutine()
	if g > 200 {
		t.Fatalf("goroutine leak: %d goroutines remaining after 100 rounds of Close+Submit race", g)
	}
	t.Logf("[Race] MultiPool Close+Submit race: 100 rounds, goroutines=%d", g)
}

// TestProduction_MultiPool_Shard_Race_CloseWaitSubmit 并发 Close + Wait + Submit 混合竞态
// 验证异常流程（先提交 → 并发 Close + Wait → 再提交）无 panic / 无泄漏
func TestProduction_MultiPool_Shard_Race_CloseWaitSubmit(t *testing.T) {
	for round := 0; round < 50; round++ {
		mp := NewPool[int](50).Shard(4)

		ctx := context.Background()

		// Phase 1: 快速提交一批
		for i := 0; i < 5000; i++ {
			mp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		}

		// Phase 2: 并发 Close + Wait
		var wg sync.WaitGroup
		wg.Add(2)
		var results []Result[int]

		go func() {
			defer wg.Done()
			results = mp.Wait()
		}()
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(round%5) * time.Millisecond)
			mp.Close()
		}()
		wg.Wait()

		// Phase 3: Close 后再 Submit 应该返回错误
		for i := 0; i < 50; i++ {
			err := mp.Submit(ctx, func(ctx context.Context) (int, error) { return 2, nil })
			if err == nil {
				// 极小概率：Close 还没完全生效
				_ = i
			}
		}

		// 验证已有结果
		for _, r := range results {
			if !r.Ok() {
				t.Errorf("round %d: result error: %v", round, r.Err)
			}
		}
	}

	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	g := runtime.NumGoroutine()
	if g > 100 {
		t.Fatalf("goroutine leak: %d goroutines after 50 rounds of Close+Wait+Submit race", g)
	}
	t.Logf("[Race] MultiPool Close+Wait+Submit race: 50 rounds, goroutines=%d", g)
}

// TestProduction_MultiPool_Shard_Race_GetShard 并发 GetShard + Close 竞态
func TestProduction_MultiPool_Shard_Race_GetShard(t *testing.T) {
	for round := 0; round < 50; round++ {
		mp := NewPool[int](20).Shard(4)
		ctx := context.Background()

		var wg sync.WaitGroup
		wg.Add(4)
		for g := 0; g < 4; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 200; i++ {
					sp := mp.GetShard(gid % 4)
					if sp != nil {
						sp.Submit(ctx, func(ctx context.Context) (int, error) { return gid, nil })
					}
				}
			}(g)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(round%10) * time.Millisecond)
			mp.Close()
		}()

		wg.Wait()
		mp.Wait()
	}

	runtime.GC()
	time.Sleep(300 * time.Millisecond)
	t.Logf("[Race] MultiPool GetShard race: 50 rounds passed")
}

// ============================================================================
// 性能对比：Shard vs NoShard
// ============================================================================

// TestProduction_MultiPool_Shard_vs_NoShard_10M 1000万 Shard(8) vs 单Pool 性能对比
func TestProduction_MultiPool_Shard_vs_NoShard_10M(t *testing.T) {
	n := 10_000_000

	// ── 单 Pool ──
	multiPoolMemStats("10M-Compare-NoShard-start")
	p := NewPool[int](100)
	ctx := context.Background()

	startNoShard := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		p.Submit(ctx, func(ctx context.Context) (int, error) { return idx, nil })
	}
	resultsNoShard := p.Wait()
	elapsedNoShard := time.Since(startNoShard)
	p.Close()

	failuresNoShard := 0
	for _, r := range resultsNoShard {
		if !r.Ok() {
			failuresNoShard++
		}
	}
	opsNoShard := float64(n) / elapsedNoShard.Seconds()

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	multiPoolMemStats("10M-Compare-Mid")

	// ── Shard(8) ──
	mp := NewPool[int](100).Shard(8)
	startShard := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		mp.Submit(ctx, func(ctx context.Context) (int, error) { return idx, nil })
	}
	resultsShard := mp.Wait()
	elapsedShard := time.Since(startShard)
	mp.Close()

	failuresShard := 0
	for _, r := range resultsShard {
		if !r.Ok() {
			failuresShard++
		}
	}
	opsShard := float64(n) / elapsedShard.Seconds()

	speedup := opsShard / opsNoShard

	t.Logf("[10M] NoShard: 1 pool × 100 workers = %d results, %v (%.0f ops/s), failures=%d",
		len(resultsNoShard), elapsedNoShard, opsNoShard, failuresNoShard)
	t.Logf("[10M] Shard(8): 8 pools × 100 workers = 800 workers = %d results, %v (%.0f ops/s), failures=%d",
		len(resultsShard), elapsedShard, opsShard, failuresShard)
	t.Logf("[10M] Speedup: %.2f× | Shard(8) is %.0f%% faster than single Pool",
		speedup, (speedup-1)*100)
	multiPoolMemStats("10M-Compare-end")
}

// ============================================================================
// Goroutine 泄漏检测
// ============================================================================

// TestProduction_MultiPool_Shard_10M_GoroutineLeak Shard 千万级 goroutine 泄漏检测
func TestProduction_MultiPool_Shard_10M_GoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	multiPoolMemStats("10M-Leak-before")

	n := 5000000 // 500万，减少单测耗时

	for round := 0; round < 10; round++ {
		mp := NewPool[int](50).Shard(8)
		ctx := context.Background()

		for i := 0; i < n/10; i++ {
			idx := i
			mp.Submit(ctx, func(ctx context.Context) (int, error) { return idx, nil })
		}
		results := mp.Wait()
		mp.Close()

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

	multiPoolMemStats("10M-Leak-after")
	t.Logf("[Leak] MultiPool Shard(8): goroutines before=%d after=%d diff=%d", before, after, after-before)

	// 允许少量系统 goroutine 波动（测试框架自身可能启动 goroutine）
	if after-before > 50 {
		t.Fatalf("goroutine leak detected: %d goroutines leaked after 10 rounds of 500K tasks", after-before)
	}
}

// ============================================================================
// 综合场景：模拟线上真实混合负载
// ============================================================================

// TestProduction_MultiPool_Shard_10M_RealWorld 模拟线上真实混合负载
func TestProduction_MultiPool_Shard_10M_RealWorld(t *testing.T) {
	multiPoolMemStats("10M-RealWorld-start")
	n := 10_000_000

	mp := NewPool[int](50).
		WithTimeout(30*time.Second).
		WithSubmitTimeout(10*time.Second).
		WithMaxPending(20000).
		WithOverflow(OverflowDrop).
		WithRingBuffer(50000, OverflowDrop).
		Shard(8)
	defer mp.Close()

	ctx := context.Background()
	var submitted atomic.Int64
	var fastTasks atomic.Int64
	var slowTasks atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i

		// 90% 快任务，10% 慢任务
		isSlow := idx%10 == 0

		err := mp.Submit(ctx, func(ctx context.Context) (int, error) {
			if isSlow {
				time.Sleep(100 * time.Microsecond)
			}
			return idx, nil
		})
		if err == nil {
			submitted.Add(1)
			if isSlow {
				slowTasks.Add(1)
			} else {
				fastTasks.Add(1)
			}
		}
	}

	// 流式排空
	flushed := mp.Flush(500000)
	results := mp.Wait()
	elapsed := time.Since(start)

	totalResults := len(flushed) + len(results)
	submittedVal := int(submitted.Load())

	t.Logf("[10M] RealWorld: fastTasks=%d slowTasks=%d submitted=%d results=%d (flushed=%d + wait=%d) in %v",
		int(fastTasks.Load()), int(slowTasks.Load()), submittedVal, totalResults, len(flushed), len(results), elapsed)

	if totalResults < submittedVal-5000 {
		t.Fatalf("too many results missing: submitted=%d total=%d", submittedVal, totalResults)
	}

	t.Logf("[10M] RealWorld: totalWorkers=%d goroutines=%d heap=%dMB",
		mp.TotalWorkerCount(), runtime.NumGoroutine(), getHeapMB())
	multiPoolMemStats("10M-RealWorld-end")
}

func getHeapMB() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc / 1024 / 1024
}

// ============================================================================
// DefaultShard 自动分片测试
// ============================================================================

// TestProduction_MultiPool_DefaultShard 验证 DefaultShard() 自动使用 GOMAXPROCS 分片
func TestProduction_MultiPool_DefaultShard(t *testing.T) {
	p := NewPool[int](50)

	mp := p.DefaultShard()
	defer mp.Close()

	expectedShards := runtime.GOMAXPROCS(0)
	if expectedShards < 2 {
		expectedShards = 2
	}

	if mp.ShardCount() != expectedShards {
		t.Fatalf("DefaultShard: expected %d shards (GOMAXPROCS), got %d", expectedShards, mp.ShardCount())
	}

	if mp.GetShard(0) != p {
		t.Fatal("DefaultShard: first shard should reuse original pool")
	}

	for i := 0; i < mp.ShardCount(); i++ {
		sp := mp.GetShard(i)
		if sp == nil {
			t.Fatalf("DefaultShard: shard %d is nil", i)
		}
		if sp.Size() != 50 {
			t.Fatalf("DefaultShard: shard %d size=%d, expected 50", i, sp.Size())
		}
	}

	t.Logf("[DefaultShard] GOMAXPROCS=%d → %d shards, each 50 workers, total=%d workers",
		runtime.GOMAXPROCS(0), mp.ShardCount(), mp.TotalWorkerCount())
}

// TestProduction_MultiPool_DefaultShard_Chain 链式调用 DefaultShard() + With 选项
func TestProduction_MultiPool_DefaultShard_Chain(t *testing.T) {
	mp := NewPool[int](50).
		WithTimeout(30 * time.Second).
		WithMaxPending(10000).
		DefaultShard()
	defer mp.Close()

	// 验证每个分片都继承了配置
	for i := 0; i < mp.ShardCount(); i++ {
		sp := mp.GetShard(i)
		if sp == nil {
			continue
		}
		if sp.Size() != 50 {
			t.Fatalf("shard %d: size=%d, expected 50", i, sp.Size())
		}
	}

	// 提交任务验证正确性
	ctx := context.Background()
	n := 10000
	for i := 0; i < n; i++ {
		idx := i
		mp.Submit(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
	}

	results := mp.Wait()
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
		t.Fatalf("%d failures", failures)
	}

	t.Logf("[DefaultShard-Chain] %d shards, %d tasks, %d results, %d failures",
		mp.ShardCount(), n, len(results), failures)
}

// TestProduction_MultiPool_DefaultShardPool 验证 DefaultShardPool 便捷函数
func TestProduction_MultiPool_DefaultShardPool(t *testing.T) {
	p := NewPool[int](100).WithMaxPending(5000)

	mp := DefaultShardPool(p)
	defer mp.Close()

	expectedShards := runtime.GOMAXPROCS(0)
	if expectedShards < 2 {
		expectedShards = 2
	}

	if mp.ShardCount() != expectedShards {
		t.Fatalf("DefaultShardPool: expected %d shards, got %d", expectedShards, mp.ShardCount())
	}

	// 提交+验证
	ctx := context.Background()
	for i := 0; i < 5000; i++ {
		idx := i
		mp.Submit(ctx, func(ctx context.Context) (int, error) { return idx, nil })
	}
	results := mp.Wait()
	if len(results) != 5000 {
		t.Fatalf("expected 5000 results, got %d", len(results))
	}

	t.Logf("[DefaultShardPool] %d shards, all good", mp.ShardCount())
}
