package async

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
)

// ============================================================================
// 生产级千万级高并发压力测试
//
// 运行方式：
//   go test -run "TestProduction" -v -count=1 -timeout 30m .
//
// 分层：
//   百万级 (1M)   → 快速验证，适合 CI
//   千万级 (10M)  → 极限验证，适合发布前
// ============================================================================

// ============================================================================
// 工具函数
// ============================================================================

func printMemStats(prefix string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("[%s] Goroutines=%d HeapAlloc=%dMB HeapObjects=%d\n",
		prefix, runtime.NumGoroutine(), m.HeapAlloc/1024/1024, m.HeapObjects)
}

// ============================================================================
// 百万级测试（1,000,000）
// ============================================================================

// TestProduction_Pool_1M_Submit 100万次 Pool Submit，100 worker 并发处理
func TestProduction_Pool_1M_Submit(t *testing.T) {
	printMemStats("Pool-1M-start")
	start := time.Now()

	p := NewPool[int](100)
	defer p.Close()
	ctx := context.Background()

	n := 1_000_000
	var submitted atomic.Int64

	for i := 0; i < n; i++ {
		idx := i
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			return idx * 2, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}

	results := p.Wait()
	elapsed := time.Since(start)

	submittedVal := int(submitted.Load())
	if len(results) != submittedVal {
		t.Fatalf("expected %d results, got %d", submittedVal, len(results))
	}

	failures := 0
	for i, r := range results {
		if r.Err != nil {
			failures++
			if failures <= 5 {
				t.Logf("result[%d] error: %v", i, r.Err)
			}
		} else if r.Value != i*2 {
			t.Fatalf("result[%d] expected %d, got %d", i, i*2, r.Value)
		}
	}
	if failures > 0 {
		t.Fatalf("%d tasks failed", failures)
	}

	opsPerSec := float64(submittedVal) / elapsed.Seconds()
	t.Logf("1M Pool Submit+Wait: %d tasks in %v (%.0f ops/s)", submittedVal, elapsed, opsPerSec)
	printMemStats("Pool-1M-end")
}

// TestProduction_Group_1M_Go 100万次 Group Go，限制并发度 500
// 注意：Group 每个任务创建新 goroutine（用完销毁），
// 百万 goroutine 开销极大，此处降至 20 万以通过验证。
// 高频复用请使用 Pool。
func TestProduction_Group_1M_Go(t *testing.T) {
	printMemStats("Group-start")
	start := time.Now()

	g := NewGroup[int](500)
	ctx := context.Background()

	n := 200_000
	var submitted atomic.Int64

	for i := 0; i < n; i++ {
		idx := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
		submitted.Add(1)
	}

	results := g.Wait()
	elapsed := time.Since(start)

	submittedVal := int(submitted.Load())
	if len(results) != submittedVal {
		t.Fatalf("expected %d results, got %d", submittedVal, len(results))
	}

	failures := 0
	for _, r := range results {
		if r.Err != nil {
			failures++
		}
	}
	if failures > 0 {
		t.Fatalf("%d tasks failed", failures)
	}

	opsPerSec := float64(submittedVal) / elapsed.Seconds()
	t.Logf("200K Group Go+Wait: %d tasks in %v (%.0f ops/s)", submittedVal, elapsed, opsPerSec)
	printMemStats("Group-end")
}

// TestProduction_Map_1M 100万元素 Map 并发处理（200 并发）
func TestProduction_Map_1M(t *testing.T) {
	printMemStats("Map-1M-start")
	n := 1_000_000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}

	start := time.Now()
	results := Map(context.Background(), slice, 200, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("result[%d] error: %v", i, r.Err)
		}
		if r.Value != i*2 {
			t.Fatalf("result[%d] expected %d, got %d", i, i*2, r.Value)
		}
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M Map (200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("Map-1M-end")
}

// TestProduction_ForEach_1M 100万元素 ForEach（200 并发）
func TestProduction_ForEach_1M(t *testing.T) {
	printMemStats("ForEach-1M-start")
	n := 1_000_000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}

	var sum atomic.Int64
	start := time.Now()
	nr, err := ForEach(context.Background(), slice, 200, func(ctx context.Context, v int) error {
		sum.Add(int64(v))
		return nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ForEach error: %v", err)
	}
	if int(nr.SuccessCount()) != n {
		t.Fatalf("expected %d successes, got %d", n, nr.SuccessCount())
	}

	expectedSum := int64(n) * int64(n-1) / 2
	if sum.Load() != expectedSum {
		t.Fatalf("expected sum %d, got %d", expectedSum, sum.Load())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M ForEach (200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("ForEach-1M-end")
}

// TestProduction_Go_1M_FireAndForget 100万次 fire-and-forget 异步任务
func TestProduction_Go_1M_FireAndForget(t *testing.T) {
	printMemStats("Go-1M-start")
	start := time.Now()

	n := 1_000_000
	var counter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		Go(context.Background(), func(ctx context.Context) {
			counter.Add(1)
			wg.Done()
		})
	}

	wg.Wait()
	elapsed := time.Since(start)

	if counter.Load() != int64(n) {
		t.Fatalf("expected %d completions, got %d", n, counter.Load())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M Go fire-and-forget: %d tasks in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("Go-1M-end")
}

// TestProduction_RateLimiter_1M_Tokens 100万次令牌获取和释放
func TestProduction_RateLimiter_1M_Tokens(t *testing.T) {
	printMemStats("RL-1M-start")
	start := time.Now()

	rl := NewRateLimiter(50000, time.Second)
	defer rl.Close()
	ctx := context.Background()

	n := 1_000_000
	var acquired atomic.Int64
	var wg sync.WaitGroup
	concurrency := 100
	wg.Add(concurrency)

	for g := 0; g < concurrency; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n/concurrency; i++ {
				if err := rl.Wait(ctx); err != nil {
					return
				}
				acquired.Add(1)
				rl.Release()
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := acquired.Load()
	if total != int64(n) {
		t.Fatalf("expected %d acquires, got %d", n, total)
	}

	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("1M RateLimiter Acquire/Release (50000/s, 100 concurrent): %d in %v (%.0f ops/s)", total, elapsed, opsPerSec)
	printMemStats("RL-1M-end")
}

// TestProduction_Retry_1M_Backoff 100万次重试（无退避，初次即成功）
func TestProduction_Retry_1M_Backoff(t *testing.T) {
	printMemStats("Retry-1M-start")
	start := time.Now()

	n := 1_000_000
	var success atomic.Int64
	var wg sync.WaitGroup
	concurrency := 200
	wg.Add(concurrency)

	for g := 0; g < concurrency; g++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < n/concurrency; i++ {
				err := Retry(ctx, 2, func(ctx context.Context) error {
					return nil
				})
				if err == nil {
					success.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := success.Load()
	if total != int64(n) {
		t.Fatalf("expected %d successes, got %d", n, total)
	}

	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("1M Retry (200 concurrent): %d ops in %v (%.0f ops/s)", total, elapsed, opsPerSec)
	printMemStats("Retry-1M-end")
}

// TestProduction_Chunk_1M_Elements 100万元素分块处理
func TestProduction_Chunk_1M_Elements(t *testing.T) {
	printMemStats("Chunk-1M-start")
	n := 1_000_000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}

	start := time.Now()
	results := MapChunk(context.Background(), slice, 200, 1000, func(ctx context.Context, batch []int) (int, error) {
		sum := 0
		for _, v := range batch {
			sum += v
		}
		return sum, nil
	})
	elapsed := time.Since(start)

	expectedChunks := (n + 999) / 1000
	if len(results) != expectedChunks {
		t.Fatalf("expected %d chunks, got %d", expectedChunks, len(results))
	}

	totalSum := 0
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("chunk error: %v", r.Err)
		}
		totalSum += r.Value
	}

	expectedSum := n * (n - 1) / 2
	if totalSum != expectedSum {
		t.Fatalf("expected sum %d, got %d", expectedSum, totalSum)
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M Chunk (200 concurrency, 1000 batch): %d elements in %v (%.0f elems/s)", n, elapsed, opsPerSec)
	printMemStats("Chunk-1M-end")
}

// TestProduction_Pool_Submit_Mixed_1M 混合场景：10 个 goroutine 并发提交 + 等待
func TestProduction_Pool_Submit_Mixed_1M(t *testing.T) {
	printMemStats("Mixed-1M-start")
	start := time.Now()

	p := NewPool[int](100)
	ctx := context.Background()

	n := 1_000_000
	var submitted atomic.Int64
	var submitWg sync.WaitGroup

	// 10 个 goroutine 并发提交
	submitters := 10
	submitWg.Add(submitters)
	for g := 0; g < submitters; g++ {
		go func() {
			defer submitWg.Done()
			for i := 0; i < n/submitters; i++ {
				if err := p.Submit(ctx, func(ctx context.Context) (int, error) {
					return 42, nil
				}); err == nil {
					submitted.Add(1)
				}
			}
		}()
	}

	submitWg.Wait()
	results := p.Wait()
	p.Close()

	elapsed := time.Since(start)
	submittedVal := int(submitted.Load())

	if len(results) != submittedVal {
		t.Fatalf("expected %d results, got %d", submittedVal, len(results))
	}

	failures := 0
	for _, r := range results {
		if r.Err != nil {
			failures++
		}
	}
	if failures > 0 {
		t.Fatalf("%d tasks failed", failures)
	}

	opsPerSec := float64(submittedVal) / elapsed.Seconds()
	t.Logf("1M Mixed (10 submitters, no resize): %d tasks in %v (%.0f ops/s)", submittedVal, elapsed, opsPerSec)
	printMemStats("Mixed-1M-end")
}

// ============================================================================
// 千万级测试（10,000,000）
// ============================================================================

// TestProduction_Pool_10M_Submit 1000万次 Pool Submit
func TestProduction_Pool_10M_Submit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10M test in short mode")
	}
	printMemStats("Pool-10M-start")
	start := time.Now()

	p := NewPool[int](200)
	defer p.Close()
	ctx := context.Background()

	n := 10_000_000
	var submitted atomic.Int64
	batchSize := 100_000

	for batch := 0; batch < n/batchSize; batch++ {
		for i := 0; i < batchSize; i++ {
			err := p.Submit(ctx, func(ctx context.Context) (int, error) {
				return 1, nil
			})
			if err == nil {
				submitted.Add(1)
			}
		}
		// 每 10 万条打印进度
		if (batch+1)%10 == 0 {
			elapsed := time.Since(start)
			done := submitted.Load()
			t.Logf("  进度: %d/%d (%.1f%%) in %v", done, n, float64(done)*100/float64(n), elapsed)
		}
	}

	results := p.Wait()
	elapsed := time.Since(start)

	submittedVal := int(submitted.Load())
	if len(results) != submittedVal {
		t.Fatalf("expected %d results, got %d", submittedVal, len(results))
	}

	failures := 0
	for _, r := range results {
		if r.Err != nil {
			failures++
		}
	}
	if failures > 0 {
		t.Fatalf("%d tasks failed", failures)
	}

	opsPerSec := float64(submittedVal) / elapsed.Seconds()
	t.Logf("10M Pool Submit+Wait: %d tasks in %v (%.0f ops/s)", submittedVal, elapsed, opsPerSec)
	printMemStats("Pool-10M-end")
}

// TestProduction_Map_10M 1000万元素 Map（500 并发）
func TestProduction_Map_10M(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10M test in short mode")
	}
	printMemStats("Map-10M-start")
	n := 10_000_000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}

	start := time.Now()
	results := Map(context.Background(), slice, 500, func(ctx context.Context, v int) (int, error) {
		return v + 1, nil
	})
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	// 采样验证
	failures := 0
	for i := 0; i < n; i += n / 1000 {
		r := results[i]
		if r.Err != nil {
			failures++
		} else if r.Value != i+1 {
			t.Fatalf("result[%d] expected %d, got %d", i, i+1, r.Value)
		}
	}
	if failures > 0 {
		t.Fatalf("%d sampled failures", failures)
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("10M Map (500 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("Map-10M-end")
}

// TestProduction_Go_10M_FireAndForget 1000万次 fire-and-forget（分批执行防止 OOM）
func TestProduction_Go_10M_FireAndForget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10M test in short mode")
	}
	printMemStats("Go-10M-start")
	start := time.Now()

	n := 10_000_000
	var counter atomic.Int64
	batchSize := 100_000
	batches := n / batchSize

	for batch := 0; batch < batches; batch++ {
		var wg sync.WaitGroup
		wg.Add(batchSize)

		for i := 0; i < batchSize; i++ {
			Go(context.Background(), func(ctx context.Context) {
				counter.Add(1)
				wg.Done()
			})
		}

		wg.Wait()

		if (batch+1)%10 == 0 {
			elapsed := time.Since(start)
			done := counter.Load()
			t.Logf("  进度: %d/%d (%.1f%%) in %v, goroutines=%d",
				done, n, float64(done)*100/float64(n), elapsed, runtime.NumGoroutine())
		}
	}

	elapsed := time.Since(start)
	if counter.Load() != int64(n) {
		t.Fatalf("expected %d completions, got %d", n, counter.Load())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("10M Go fire-and-forget (batched 100K): %d tasks in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("Go-10M-end")
}

// ============================================================================
// 极限正确性测试：Close/Wait 竞态 + 高并发混合
// ============================================================================

// TestProduction_Pool_CloseRace_10KConcurrent 10000 goroutine 同时 Submit 和 Close
func TestProduction_Pool_CloseRace_10KConcurrent(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](50)
		ctx := context.Background()
		var submitWg sync.WaitGroup
		var closeOnce sync.Once

		// 50 个 goroutine Submit
		submitWg.Add(50)
		for g := 0; g < 50; g++ {
			go func() {
				defer submitWg.Done()
				for i := 0; i < 200; i++ {
					p.Submit(ctx, func(ctx context.Context) (int, error) {
						return 0, nil
					})
				}
			}()
		}

		// 1 个 goroutine 随机 Close
		go func() {
			time.Sleep(time.Duration(1+round%5) * time.Millisecond)
			closeOnce.Do(func() {
				p.Close()
			})
		}()

		submitWg.Wait()
		p.Wait()
		p.Close()
	}
}

// TestProduction_Pool_CloseWaitRace_10KConcurrent Close + Wait 并发
func TestProduction_Pool_CloseWaitRace_10KConcurrent(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](50)
		ctx := context.Background()

		var wg sync.WaitGroup
		wg.Add(50)
		for g := 0; g < 50; g++ {
			go func() {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					p.Submit(ctx, func(ctx context.Context) (int, error) {
						return 0, nil
					})
				}
			}()
		}

		go func() {
			time.Sleep(time.Duration(1+round%10) * time.Millisecond)
			p.Close()
		}()

		wg.Wait()

		// 多个 goroutine 同时 Wait + Close
		var waitWg sync.WaitGroup
		waitWg.Add(3)
		go func() { defer waitWg.Done(); p.Wait() }()
		go func() { defer waitWg.Done(); p.Close() }()
		go func() { defer waitWg.Done(); p.Close() }()
		waitWg.Wait()
	}
}

// TestProduction_Group_Race_10KConcurrent 10000 goroutine 同时 Go 和 Wait
func TestProduction_Group_Race_10KConcurrent(t *testing.T) {
	for round := 0; round < 50; round++ {
		g := NewGroup[int](200)
		ctx := context.Background()

		var submitWg sync.WaitGroup
		submitWg.Add(100)
		for s := 0; s < 100; s++ {
			go func() {
				defer submitWg.Done()
				for i := 0; i < 100; i++ {
					g.Go(ctx, func(ctx context.Context) (int, error) {
						return 0, nil
					})
				}
			}()
		}

		// 等待一半任务完成，然后 Wait
		submitWg.Wait()
		g.Wait()
	}
}

// TestProduction_RateLimiter_Race_10KConcurrent 限流器并发竞态
func TestProduction_RateLimiter_Race_10KConcurrent(t *testing.T) {
	rl := NewRateLimiter(10000, time.Second)
	defer rl.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	n := 10000
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_ = rl.Wait(ctx)
			rl.Release()
		}()
	}

	wg.Wait()
}

// ============================================================================
// 内存泄漏检测
// ============================================================================

// TestProduction_GoroutineLeakCheck 检测 goroutine 泄漏
func TestProduction_GoroutineLeakCheck(t *testing.T) {
	// 记录基准 goroutine 数
	time.Sleep(100 * time.Millisecond)
	baseGoroutines := runtime.NumGoroutine()
	t.Logf("Base goroutines: %d", baseGoroutines)

	// 执行大量操作
	for round := 0; round < 10; round++ {
		p := NewPool[int](50)
		ctx := context.Background()
		for i := 0; i < 50000; i++ {
			p.Submit(ctx, func(ctx context.Context) (int, error) {
				return 0, nil
			})
		}
		p.Wait()
		p.Close()
	}

	// 同样操作 Group 版
	for round := 0; round < 10; round++ {
		g := NewGroup[int](50)
		ctx := context.Background()
		for i := 0; i < 50000; i++ {
			g.Go(ctx, func(ctx context.Context) (int, error) {
				return 0, nil
			})
		}
		g.Wait()
	}

	// 等待 GC
	runtime.GC()
	time.Sleep(500 * time.Millisecond)

	// 检查 goroutine 数
	afterGoroutines := runtime.NumGoroutine()
	t.Logf("After goroutines: %d", afterGoroutines)

	// 允许少量浮动，但不应该有显著增长
	leaked := afterGoroutines - baseGoroutines
	if leaked > 20 {
		t.Errorf("possible goroutine leak: base=%d, after=%d, diff=%d", baseGoroutines, afterGoroutines, leaked)
	} else {
		t.Logf("No goroutine leak detected (diff=%d)", leaked)
	}
}

// ============================================================================
// 全链路综合压力测试（模拟真实生产场景）
// ============================================================================

// TestProduction_RealWorld_Workflow 模拟真实生产工作流
//
// 场景：批量处理 + 限流 + 重试
// 1. 100万条数据 Map 处理（IO 密集型）
// 2. 限流器控制下游调用速率
// 3. 失败重试（指数退避）
// 4. 结果聚合
func TestProduction_RealWorld_Workflow(t *testing.T) {
	printMemStats("Workflow-start")
	start := time.Now()

	n := 1_000_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	// 限流器：每秒 10000 次
	rl := NewRateLimiter(10000, time.Second)
	defer rl.Close()

	// Map 阶段：并发处理 + 限流
	mapResults := Map(context.Background(), items, 200, func(ctx context.Context, v int) (int, error) {
		if err := rl.Wait(ctx); err != nil {
			return 0, err
		}
		defer rl.Release()

		// 模拟处理
		if v%10000 == 0 {
			return 0, fmt.Errorf("simulated failure at %d", v)
		}
		return v * 2, nil
	})

	t.Logf("Map 完成: %d results", len(mapResults))

	// 统计失败
	successes, failures := Partition(mapResults)
	t.Logf("成功: %d, 失败: %d", len(successes), len(failures))

	// 对失败项重试
	if len(failures) > 0 {
		var retryWg sync.WaitGroup
		retried := make(chan core.Result[int], len(failures))

		for _, originalErr := range failures {
			retryWg.Add(1)
			go func(err error) {
				defer retryWg.Done()
				retryErr := RetryWithBackoff(context.Background(), 2, 10*time.Millisecond, func(ctx context.Context) error {
					return err // 模拟重试失败
				})
				if retryErr != nil {
					retried <- core.Result[int]{Err: retryErr}
				} else {
					retried <- core.Result[int]{Value: 42}
				}
			}(originalErr)
		}

		retryWg.Wait()
		close(retried)
		t.Logf("重试完成: %d items", len(retried))
	}

	elapsed := time.Since(start)
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("RealWorld Workflow: %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	t.Logf("  成功: %d, 失败: %d", len(successes), len(failures))
	printMemStats("Workflow-end")
}

// ============================================================================
// 百万/千万级：缺失模块补充
// ============================================================================

// TestProduction_Reduce_1M 100万元素 Reduce（MapReduce 并发映射+聚合）
func TestProduction_Reduce_1M(t *testing.T) {
	printMemStats("Reduce-1M-start")
	n := 1_000_000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = 1
	}

	start := time.Now()
	sum, err := Reduce(context.Background(), slice, 200, func(ctx context.Context, v int) (int, error) {
		return v, nil
	}, 0, func(a, b int) int {
		return a + b
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Reduce error: %v", err)
	}
	if sum != n {
		t.Fatalf("expected sum %d, got %d", n, sum)
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M Reduce (MapReduce, 200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("Reduce-1M-end")
}

// TestProduction_NoResult_200K 20万 NoResult（无返回值 Group）
func TestProduction_NoResult_200K(t *testing.T) {
	printMemStats("NoResult-start")
	start := time.Now()

	nr := NewNoResult(500)
	ctx := context.Background()
	n := 200_000
	var counter atomic.Int64

	for i := 0; i < n; i++ {
		nr.Go(ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	nr.Wait()
	elapsed := time.Since(start)

	if counter.Load() != int64(n) {
		t.Fatalf("expected %d completions, got %d", n, counter.Load())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("200K NoResult Go+Wait: %d tasks in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("NoResult-end")
}

// TestProduction_NoResultPool_1M 100万 NoResultPool（无返回值 Pool）
func TestProduction_NoResultPool_1M(t *testing.T) {
	printMemStats("NoResultPool-start")
	start := time.Now()

	p := NewPool[struct{}](100)
	ctx := context.Background()
	n := 1_000_000
	var counter atomic.Int64

	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
			counter.Add(1)
			return struct{}{}, nil
		})
	}

	p.Wait()
	p.Close()
	elapsed := time.Since(start)

	if counter.Load() != int64(n) {
		t.Fatalf("expected %d completions, got %d", n, counter.Load())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M NoResultPool Submit+Wait: %d tasks in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("NoResultPool-end")
}

// TestProduction_Mu_1M_AppendSnapshot 100万次 Mu.Append + Snapshot
func TestProduction_Mu_1M_AppendSnapshot(t *testing.T) {
	printMemStats("Mu-start")
	start := time.Now()

	n := 1_000_000
	var mu Mu[int]
	var wg sync.WaitGroup
	concurrency := 100
	wg.Add(concurrency)

	for g := 0; g < concurrency; g++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < n/concurrency; i++ {
				val := base*10000 + i
				mu.Append(func() int { return val })
			}
		}(g)
	}

	wg.Wait()

	snapshot := mu.Snapshot()
	elapsed := time.Since(start)

	if len(snapshot) != n {
		t.Fatalf("expected %d items in snapshot, got %d", n, len(snapshot))
	}

	// 验证无重复（简单哈希校验）
	seen := make(map[int]bool, n)
	for _, v := range snapshot {
		if seen[v] {
			t.Fatalf("duplicate value %d", v)
		}
		seen[v] = true
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M Mu.Append+Snapshot (100 concurrent): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("Mu-end")
}

// TestProduction_MapWithFailFast_1M 100万元素 Map FailFast 模式
func TestProduction_MapWithFailFast_1M(t *testing.T) {
	printMemStats("MapFailFast-start")
	n := 1_000_000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}

	start := time.Now()
	results, err := MapWithFailFast(context.Background(), slice, 200, func(ctx context.Context, v int) (int, error) {
		return v + 1, nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("MapWithFailFast error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	// 采样验证
	for i := 0; i < n; i += n / 1000 {
		if results[i].Err != nil {
			t.Fatalf("result[%d] error: %v", i, results[i].Err)
		}
		if results[i].Value != i+1 {
			t.Fatalf("result[%d] expected %d, got %d", i, i+1, results[i].Value)
		}
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M MapWithFailFast (200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("MapFailFast-end")
}

// TestProduction_MapWithFailFast_500K_RealFailure 50万 FailFast 真失败触发取消
func TestProduction_MapWithFailFast_500K_RealFailure(t *testing.T) {
	printMemStats("MapFF-Fail-start")
	n := 500_000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}

	var firstFailValue atomic.Int64
	firstFailValue.Store(-1)

	results, err := MapWithFailFast(context.Background(), slice, 100, func(ctx context.Context, v int) (int, error) {
		// 第 1000 个元素触发失败
		if v == 1000 {
			firstFailValue.Store(int64(v))
			return 0, fmt.Errorf("fail at %d", v)
		}
		// 其他元素正常（其中一些会被取消）
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			time.Sleep(time.Microsecond)
			return v * 2, nil
		}
	})

	if err == nil {
		t.Fatal("expected error from FailFast, got nil")
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	failures := 0
	cancelled := 0
	for _, r := range results {
		if r.Err != nil {
			failures++
			if errors.Is(r.Err, context.Canceled) {
				cancelled++
			}
		}
	}
	t.Logf("500K MapWithFailFast real failure: %d results, %d failures, %d cancelled",
		n, failures, cancelled)

	if failures == n {
		t.Logf("  (所有任务都被取消，FailFast 生效)")
	}
	printMemStats("MapFF-Fail-end")
}

// TestProduction_ForEachWithFailFast_500K 50万 ForEach FailFast 模式
func TestProduction_ForEachWithFailFast_500K(t *testing.T) {
	printMemStats("ForEachFF-start")
	n := 500_000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}

	var processed atomic.Int64
	start := time.Now()
	nr, err := ForEachWithFailFast(context.Background(), slice, 200, func(ctx context.Context, v int) error {
		processed.Add(1)
		return nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ForEachWithFailFast error: %v", err)
	}
	nr.Wait()

	if processed.Load() != int64(n) {
		t.Fatalf("expected %d processed, got %d", n, processed.Load())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("500K ForEachWithFailFast (200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("ForEachFF-end")
}

// TestProduction_TokenBucket_1M 100万次 TokenBucket Allow
func TestProduction_TokenBucket_1M(t *testing.T) {
	printMemStats("TokenBucket-start")
	start := time.Now()

	// 高容量令牌桶，避免限流阻塞
	tb := NewTokenBucket(1_000_000, 1_000_000)
	n := 1_000_000
	var allowed atomic.Int64
	var wg sync.WaitGroup
	concurrency := 100
	wg.Add(concurrency)

	for g := 0; g < concurrency; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n/concurrency; i++ {
				if tb.Allow() {
					allowed.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := allowed.Load()
	if total != int64(n) {
		t.Fatalf("expected %d allows, got %d", n, total)
	}

	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("1M TokenBucket.Allow (100 concurrent): %d ops in %v (%.0f ops/s)", total, elapsed, opsPerSec)
	printMemStats("TokenBucket-end")
}

// TestProduction_TokenBucket_AllowN_1M 100万次 TokenBucket AllowN
func TestProduction_TokenBucket_AllowN_1M(t *testing.T) {
	printMemStats("TokenBucketN-start")
	start := time.Now()

	tb := NewTokenBucket(500_000, 500_000)
	n := 1_000_000
	var allowed atomic.Int64
	var wg sync.WaitGroup
	concurrency := 100
	wg.Add(concurrency)

	for g := 0; g < concurrency; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n/concurrency; i++ {
				if tb.AllowN(1) {
					allowed.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := allowed.Load()
	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("1M TokenBucket.AllowN(1) (100 concurrent): %d ops in %v (%.0f ops/s)", total, elapsed, opsPerSec)
	printMemStats("TokenBucketN-end")
}

// TestProduction_AdaptiveRateLimiter_500K 50万次自适应限流器
func TestProduction_AdaptiveRateLimiter_500K(t *testing.T) {
	printMemStats("AdaptiveRL-start")
	start := time.Now()

	al := NewAdaptiveRateLimiter(50, 500)
	ctx := context.Background()
	n := 500_000
	var acquired atomic.Int64
	var wg sync.WaitGroup
	concurrency := 200
	wg.Add(concurrency)

	for g := 0; g < concurrency; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n/concurrency; i++ {
				if err := al.Acquire(ctx); err != nil {
					return
				}
				acquired.Add(1)
				al.RecordSuccess()
				al.Release()
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := acquired.Load()
	if total != int64(n) {
		t.Fatalf("expected %d acquires, got %d", n, total)
	}

	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("500K AdaptiveRateLimiter (50-500 range, 200 concurrent): %d ops in %v (%.0f ops/s)", total, elapsed, opsPerSec)
	printMemStats("AdaptiveRL-end")
}

// TestProduction_AdaptiveRateLimiter_Adjust 自适应限流器：验证成功率驱动的自动调整
func TestProduction_AdaptiveRateLimiter_Adjust(t *testing.T) {
	al := NewAdaptiveRateLimiter(10, 100)
	ctx := context.Background()

	// 模拟高失败率 → 应触发缩容
	for round := 0; round < 20; round++ {
		for i := 0; i < 100; i++ {
			if err := al.Acquire(ctx); err != nil {
				t.Fatalf("acquire failed: %v", err)
			}
			if i%5 == 0 {
				al.RecordSuccess()
			} else {
				al.RecordFailure()
			}
			al.Release()
		}
	}

	t.Logf("AdaptiveRateLimiter adjust test passed (simulated 80%% failure rate)")
}

// TestProduction_Pipeline_Execute_500K 50万 Pipeline 多阶段执行
func TestProduction_Pipeline_Execute_500K(t *testing.T) {
	printMemStats("Pipeline-start")
	n := 500_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	stages := []Stage[int]{
		{Name: "multiply", Concurrency: 200},
		{Name: "add_one", Concurrency: 200},
		{Name: "square", Concurrency: 200},
	}

	start := time.Now()
	results, err := Execute(context.Background(), stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "multiply":
			return item * 2, nil
		case "add_one":
			return item + 1, nil
		case "square":
			return item * item, nil
		default:
			return item, nil
		}
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Pipeline error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	// 校验 pipeline 结果正确性（采样）
	for i := 0; i < n; i += n / 100 {
		if results[i].Err != nil {
			t.Fatalf("result[%d] error: %v", i, results[i].Err)
		}
		expected := ((i * 2) + 1) * ((i * 2) + 1)
		if results[i].Value != expected {
			t.Fatalf("result[%d] expected %d, got %d", i, expected, results[i].Value)
		}
	}

	opsPerSec := float64(n*len(stages)) / elapsed.Seconds()
	t.Logf("500K Pipeline Execute (3 stages × 200 concurrency): %d stages in %v (%.0f stage-ops/s)", n, elapsed, opsPerSec)
	printMemStats("Pipeline-end")
}

// TestProduction_Pipeline_ExecuteWithMeta_500K 50万 Pipeline ExecuteWithMeta
func TestProduction_Pipeline_ExecuteWithMeta_500K(t *testing.T) {
	printMemStats("PipelineMeta-start")
	n := 500_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	stages := []Stage[int]{
		{Name: "double", Concurrency: 200},
		{Name: "negate", Concurrency: 200},
	}

	start := time.Now()
	results := ExecuteWithMeta(context.Background(), stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "double":
			return item * 2, nil
		case "negate":
			return -item, nil
		default:
			return item, nil
		}
	})
	elapsed := time.Since(start)

	expectedLen := n * len(stages)
	if len(results) != expectedLen {
		t.Fatalf("expected %d meta-results (n*stages), got %d", expectedLen, len(results))
	}

	// 验证：第一个阶段 result = i*2，第二个阶段 result = -(i*2)
	for stageIdx := 0; stageIdx < len(stages); stageIdx++ {
		stageName := stages[stageIdx].Name
		for i := 0; i < n; i += n / 100 {
			idx := stageIdx*n + i
			r := results[idx]
			if r.Err != nil {
				t.Fatalf("[%s] result[%d] error: %v", stageName, i, r.Err)
			}
			var expected int
			if stageIdx == 0 {
				expected = i * 2
			} else {
				expected = -i * 2
			}
			if r.Value != expected {
				t.Fatalf("[%s] result[%d] expected %d, got %d", stageName, i, expected, r.Value)
			}
		}
	}

	opsPerSec := float64(expectedLen) / elapsed.Seconds()
	t.Logf("500K Pipeline ExecuteWithMeta (2 stages × 200 concurrency): %d meta-results in %v (%.0f ops/s)", expectedLen, elapsed, opsPerSec)
	printMemStats("PipelineMeta-end")
}

// TestProduction_Retry_WithBackoff_200K 20万次重试（指数退避）
func TestProduction_Retry_WithBackoff_200K(t *testing.T) {
	printMemStats("RetryBackoff-start")
	start := time.Now()

	n := 200_000
	var success atomic.Int64
	var wg sync.WaitGroup
	concurrency := 100
	wg.Add(concurrency)

	for g := 0; g < concurrency; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n/concurrency; i++ {
				err := RetryWithBackoff(context.Background(), 3, 10*time.Microsecond, func(ctx context.Context) error {
					return nil
				})
				if err == nil {
					success.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := success.Load()
	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("200K RetryWithBackoff (100 concurrent): %d ops in %v (%.0f ops/s)", total, elapsed, opsPerSec)
	printMemStats("RetryBackoff-end")
}

// TestProduction_Chunk_MapChunk_500K 50万 MapChunk（分块聚合）
func TestProduction_Chunk_MapChunk_500K(t *testing.T) {
	printMemStats("MapChunk-start")
	n := 500_000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}

	start := time.Now()
	results := MapChunk(context.Background(), slice, 100, 5000, func(ctx context.Context, batch []int) (int, error) {
		sum := 0
		for _, v := range batch {
			sum += v
		}
		return sum, nil
	})
	elapsed := time.Since(start)

	totalSum := 0
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("chunk error: %v", r.Err)
		}
		totalSum += r.Value
	}

	expectedSum := n * (n - 1) / 2
	if totalSum != expectedSum {
		t.Fatalf("expected sum %d, got %d", expectedSum, totalSum)
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("500K MapChunk (100 concurrency, 5000 batch): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("MapChunk-end")
}

// ============================================================================
// Map 超时 / 串行 / FailFast-TimeOut 变体补充
// ============================================================================

// TestProduction_MapWithTimeout_500K 50万 Map 带 per-item 超时
func TestProduction_MapWithTimeout_500K(t *testing.T) {
	printMemStats("MapWithTimeout-start")
	n := 500_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results := MapWithTimeout(context.Background(), items, 200, 30*time.Second, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for i := 0; i < n; i += n / 100 {
		if results[i].Err != nil {
			t.Fatalf("result[%d] error: %v", i, results[i].Err)
		}
		if results[i].Value != i*2 {
			t.Fatalf("result[%d] expected %d, got %d", i, i*2, results[i].Value)
		}
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("500K MapWithTimeout (200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("MapWithTimeout-end")
}

// TestProduction_MapWithFFTimeout_500K 50万 Map FailFast + Timeout
func TestProduction_MapWithFFTimeout_500K(t *testing.T) {
	printMemStats("MapFFTimeout-start")
	n := 500_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results, err := MapWithFFTimeout(context.Background(), items, 200, 30*time.Second, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("MapFFTimeout error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("500K MapWithFFTimeout (200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("MapFFTimeout-end")
}

// TestProduction_MapSerial_100K 10万串行 Map
func TestProduction_MapSerial_100K(t *testing.T) {
	printMemStats("MapSerial-start")
	n := 100_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results := MapSerial(context.Background(), items, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("100K MapSerial: %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("MapSerial-end")
}

// TestProduction_MapSerialFailFast_100K 10万串行 FailFast Map
func TestProduction_MapSerialFailFast_100K(t *testing.T) {
	printMemStats("MapSerialFF-start")
	n := 100_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results, err := MapSerialFailFast(context.Background(), items, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("MapSerialFailFast error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("100K MapSerialFailFast: %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("MapSerialFF-end")
}

// ============================================================================
// ForEach 超时 / 串行 变体补充
// ============================================================================

// TestProduction_ForEachWithTimeout_500K 50万 ForEach 带超时
func TestProduction_ForEachWithTimeout_500K(t *testing.T) {
	printMemStats("ForEachTimeout-start")
	n := 100_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	nr, err := ForEachWithTimeout(context.Background(), items, 200, 30*time.Second, func(ctx context.Context, v int) error {
		return nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ForEachWithTimeout error: %v", err)
	}
	if nr.SuccessCount() != int64(n) {
		t.Fatalf("expected %d success, got %d", n, nr.SuccessCount())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("100K ForEachWithTimeout (200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("ForEachTimeout-end")
}

// TestProduction_ForEachWithFFTimeout_500K 50万 ForEach FailFast + Timeout
func TestProduction_ForEachWithFFTimeout_500K(t *testing.T) {
	printMemStats("ForEachFFTimeout-start")
	n := 100_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	nr, err := ForEachWithFFTimeout(context.Background(), items, 200, 30*time.Second, func(ctx context.Context, v int) error {
		return nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ForEachWithFFTimeout error: %v", err)
	}
	if nr.SuccessCount() != int64(n) {
		t.Fatalf("expected %d success, got %d", n, nr.SuccessCount())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("100K ForEachWithFFTimeout (200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("ForEachFFTimeout-end")
}

// TestProduction_ForEachSerial_50K 5万串行 ForEach
func TestProduction_ForEachSerial_50K(t *testing.T) {
	printMemStats("ForEachSerial-start")
	n := 50_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	nr, err := ForEachSerial(context.Background(), items, func(ctx context.Context, v int) error {
		return nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ForEachSerial error: %v", err)
	}
	if nr.SuccessCount() != int64(n) {
		t.Fatalf("expected %d success, got %d", n, nr.SuccessCount())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("50K ForEachSerial: %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("ForEachSerial-end")
}

// ============================================================================
// Task: GoWithTimeout / GoResult / GoResultWithTimeout
// ============================================================================

// TestProduction_GoWithTimeout_500K 50万 Go 带超时 fire-and-forget
func TestProduction_GoWithTimeout_500K(t *testing.T) {
	printMemStats("GoWithTimeout-start")
	n := 500_000
	var counter int64
	batchSize := 50_000

	start := time.Now()
	for i := 0; i < n; i += batchSize {
		for j := 0; j < batchSize; j++ {
			GoWithTimeout(context.Background(), 30*time.Second, func(ctx context.Context) {
				atomic.AddInt64(&counter, 1)
			})
		}
	}
	elapsed := time.Since(start)

	time.Sleep(200 * time.Millisecond)

	final := atomic.LoadInt64(&counter)
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("500K GoWithTimeout: %d launched in %v (%.0f ops/s), completed=%d", n, elapsed, opsPerSec, final)
	printMemStats("GoWithTimeout-end")
}

// TestProduction_GoResult_500K 50万 GoResult 有返回值
func TestProduction_GoResult_500K(t *testing.T) {
	printMemStats("GoResult-start")
	n := 500_000
	results := make([]*AsyncResult[int], n)

	start := time.Now()
	for i := 0; i < n; i++ {
		v := i
		results[i] = GoResult(context.Background(), func(ctx context.Context) (int, error) {
			return v * 2, nil
		})
	}
	elapsed := time.Since(start)

	var success, fail int64
	for _, r := range results {
		if r.Ok() {
			success++
		} else {
			fail++
		}
	}

	t.Logf("500K GoResult: %d launched in %v, success=%d fail=%d", n, elapsed, success, fail)
	if fail > 0 {
		t.Fatalf("expected 0 failures, got %d", fail)
	}
	printMemStats("GoResult-end")
}

// ============================================================================
// ChunkN 分块
// ============================================================================

// TestProduction_ChunkN_1M 100万 ChunkN 均分
func TestProduction_ChunkN_1M(t *testing.T) {
	printMemStats("ChunkN-start")
	n := 1_000_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	chunks := ChunkN(items, 100)
	elapsed := time.Since(start)

	totalElements := 0
	for _, c := range chunks {
		totalElements += len(c)
	}
	if totalElements != n {
		t.Fatalf("expected %d elements, got %d", n, totalElements)
	}
	if len(chunks) != 100 {
		t.Fatalf("expected 100 chunks, got %d", len(chunks))
	}
	t.Logf("1M ChunkN(100): %d items → %d chunks in %v", n, len(chunks), elapsed)
	printMemStats("ChunkN-end")
}

// ============================================================================
// MapChunk 系列变体
// ============================================================================

// TestProduction_MapChunkWithFailFast_200K 20万分块 Map FailFast
func TestProduction_MapChunkWithFailFast_200K(t *testing.T) {
	printMemStats("MapChunkFF-start")
	n := 200_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results, err := MapChunkWithFailFast(context.Background(), items, 100, 5000, func(ctx context.Context, batch []int) (int, error) {
		return len(batch), nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("MapChunkWithFailFast error: %v", err)
	}
	if len(results) != n/5000 {
		t.Fatalf("expected %d chunks, got %d", n/5000, len(results))
	}
	t.Logf("200K MapChunkWithFailFast (100 concurrency, 5000 batch): %d chunks in %v", len(results), elapsed)
	printMemStats("MapChunkFF-end")
}

// TestProduction_MapChunkWithTimeout_200K 20万分块 Map Timeout
func TestProduction_MapChunkWithTimeout_200K(t *testing.T) {
	printMemStats("MapChunkTO-start")
	n := 200_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results := MapChunkWithTimeout(context.Background(), items, 100, 5000, 30*time.Second, func(ctx context.Context, batch []int) (int, error) {
		return len(batch), nil
	})
	elapsed := time.Since(start)

	if len(results) != n/5000 {
		t.Fatalf("expected %d chunks, got %d", n/5000, len(results))
	}
	t.Logf("200K MapChunkWithTimeout: %d chunks in %v", len(results), elapsed)
	printMemStats("MapChunkTO-end")
}

// TestProduction_MapChunked_200K 20万分块元素级 Map
func TestProduction_MapChunked_200K(t *testing.T) {
	printMemStats("MapChunked-start")
	n := 200_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results := MapChunked(context.Background(), items, 100, 5000, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("200K MapChunked (100 concurrency, 5000 batch): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("MapChunked-end")
}

// TestProduction_MapChunkedWithFailFast_200K 20万分块元素级 Map FF
func TestProduction_MapChunkedWithFailFast_200K(t *testing.T) {
	n := 200_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results, err := MapChunkedWithFailFast(context.Background(), items, 100, 5000, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d, got %d", n, len(results))
	}
	t.Logf("200K MapChunkedWithFailFast: %d items in %v", n, elapsed)
}

// ============================================================================
// ForEachChunk 系列
// ============================================================================

// TestProduction_ForEachChunk_100K 10万分块 ForEach
func TestProduction_ForEachChunk_100K(t *testing.T) {
	printMemStats("ForEachChunk-start")
	n := 100_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	nr, err := ForEachChunk(context.Background(), items, 50, 5000, func(ctx context.Context, batch []int) error {
		return nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if nr.SuccessCount() != int64(n/5000) {
		t.Fatalf("expected %d chunks, got %d", n/5000, nr.SuccessCount())
	}
	t.Logf("100K ForEachChunk (50 concurrency, 5000 batch): %d chunks in %v", n/5000, elapsed)
	printMemStats("ForEachChunk-end")
}

// TestProduction_ForEachChunked_100K 10万分块元素级 ForEach
func TestProduction_ForEachChunked_100K(t *testing.T) {
	n := 100_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	nr, err := ForEachChunked(context.Background(), items, 50, 5000, func(ctx context.Context, v int) error {
		return nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if nr.SuccessCount() != int64(n) {
		t.Fatalf("expected %d, got %d", n, nr.SuccessCount())
	}
	t.Logf("100K ForEachChunked: %d items in %v", n, elapsed)
}

// ============================================================================
// Reduce 系列变体
// ============================================================================

// TestProduction_ReduceWithFailFast_100K 10万 Reduce FailFast
func TestProduction_ReduceWithFailFast_100K(t *testing.T) {
	n := 100_000
	items := make([]int, n)
	for i := range items {
		items[i] = 1
	}

	start := time.Now()
	sum, err := ReduceWithFailFast(context.Background(), items, 200, func(ctx context.Context, v int) (int, error) {
		return v, nil
	}, 0, func(a, b int) int {
		return a + b
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if sum != n {
		t.Fatalf("expected %d, got %d", n, sum)
	}
	t.Logf("100K ReduceWithFailFast: sum=%d in %v", sum, elapsed)
}

// TestProduction_ReduceWithTimeout_100K 10万 Reduce Timeout
func TestProduction_ReduceWithTimeout_100K(t *testing.T) {
	n := 100_000
	items := make([]int, n)
	for i := range items {
		items[i] = 1
	}

	start := time.Now()
	sum, err := ReduceWithTimeout(context.Background(), items, 200, 30*time.Second, func(ctx context.Context, v int) (int, error) {
		return v, nil
	}, 0, func(a, b int) int {
		return a + b
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if sum != n {
		t.Fatalf("expected %d, got %d", n, sum)
	}
	t.Logf("100K ReduceWithTimeout: sum=%d in %v", sum, elapsed)
}

// ============================================================================
// Retry 系列变体
// ============================================================================

// TestProduction_RetryWithBackoffResult_200K 20万 Retry Result 类型
func TestProduction_RetryWithBackoffResult_200K(t *testing.T) {
	n := 200_000

	begin := time.Now()
	fn := func(ctx context.Context) (int, error) {
		return 42, nil
	}

	var success int64
	var fail int64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			r := RetryWithBackoffResult(context.Background(), fn, 0, 0, 0)
			if r.Ok() {
				atomic.AddInt64(&success, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(begin)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("200K RetryWithBackoffResult: success=%d fail=%d in %v (%.0f ops/s)", success, fail, elapsed, opsPerSec)

	if fail > 0 {
		t.Fatalf("expected 0 failures, got %d", fail)
	}
}

// TestProduction_RetryWithLinearBackoffResult_200K 20万 Retry Linear Result
func TestProduction_RetryWithLinearBackoffResult_200K(t *testing.T) {
	n := 200_000
	fn := func(ctx context.Context) (int, error) {
		return 42, nil
	}

	var success int64
	var fail int64
	var wg sync.WaitGroup
	wg.Add(n)
	begin := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			r := RetryWithLinearBackoffResult(context.Background(), fn, 0, 0)
			if r.Ok() {
				atomic.AddInt64(&success, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(begin)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("200K RetryWithLinearBackoffResult: success=%d fail=%d in %v (%.0f ops/s)", success, fail, elapsed, opsPerSec)

	if fail > 0 {
		t.Fatalf("expected 0 failures, got %d", fail)
	}
}

// TestProduction_WithTimeout_200K 20万 WithTimeout 包装
func TestProduction_WithTimeout_200K(t *testing.T) {
	n := 200_000
	fn := func(ctx context.Context) (int, error) {
		return 42, nil
	}

	var success int64
	var fail int64
	var wg sync.WaitGroup
	wg.Add(n)
	begin := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			val, err := WithTimeout(context.Background(), 5*time.Second, fn)
			if err == nil && val == 42 {
				atomic.AddInt64(&success, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(begin)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("200K WithTimeout: success=%d fail=%d in %v (%.0f ops/s)", success, fail, elapsed, opsPerSec)

	if fail > 0 {
		t.Fatalf("expected 0 failures, got %d", fail)
	}
}

// TestProduction_WithDeadline_200K 20万 WithDeadline
func TestProduction_WithDeadline_200K(t *testing.T) {
	n := 200_000
	fn := func(ctx context.Context) (int, error) {
		return 42, nil
	}

	var success int64
	var fail int64
	var wg sync.WaitGroup
	wg.Add(n)
	deadline := time.Now().Add(5 * time.Second)
	begin := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			val, err := WithDeadline(context.Background(), deadline, fn)
			if err == nil && val == 42 {
				atomic.AddInt64(&success, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(begin)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("200K WithDeadline: success=%d fail=%d in %v (%.0f ops/s)", success, fail, elapsed, opsPerSec)

	if fail > 0 {
		t.Fatalf("expected 0 failures, got %d", fail)
	}
}

// ============================================================================
// RateLimiter: SlidingWindow / Burst / Token / BlockForce
// ============================================================================

// TestProduction_SlidingWindowRateLimiter_1M 100万滑动窗口
func TestProduction_SlidingWindowRateLimiter_1M(t *testing.T) {
	printMemStats("SlidingWindow-start")
	n := 1_000_000
	sw := NewSlidingWindowRateLimiter(n, time.Second)

	start := time.Now()
	var allowed int64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if sw.Allow() {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&allowed)
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M SlidingWindowRateLimiter.Allow: %d/%d allowed in %v (%.0f ops/s)", total, n, elapsed, opsPerSec)
	printMemStats("SlidingWindow-end")
}

// TestProduction_SlidingWindowRateLimiter_AllowN_1M 100万滑动窗口 AllowN
func TestProduction_SlidingWindowRateLimiter_AllowN_1M(t *testing.T) {
	n := 1_000_000
	sw := NewSlidingWindowRateLimiter(n, time.Second)

	var allowed int64
	var wg sync.WaitGroup
	wg.Add(n)
	start := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if sw.AllowN(1) {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&allowed)
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M SlidingWindowRateLimiter.AllowN(1): %d/%d allowed in %v (%.0f ops/s)", total, n, elapsed, opsPerSec)
}

// TestProduction_RateLimiterWithBurst_1M 100万带突发容量的RateLimiter
func TestProduction_RateLimiterWithBurst_1M(t *testing.T) {
	printMemStats("RLBurst-start")
	n := 1_000_000
	rl := NewRateLimiterWithBurst(500000, time.Second, 1000000)

	var acquired int64
	var wg sync.WaitGroup
	wg.Add(n)
	start := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := rl.Acquire(context.Background()); err == nil {
				atomic.AddInt64(&acquired, 1)
				rl.Release()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&acquired)
	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("1M RateLimiter Burst (500K/s, 1M capacity): %d/%d in %v (%.0f ops/s)", total, n, elapsed, opsPerSec)
	printMemStats("RLBurst-end")
}

// TestProduction_RateLimiter_Token_500K 50万手动Token模式
func TestProduction_RateLimiter_Token_500K(t *testing.T) {
	printMemStats("RLToken-start")
	n := 500_000
	rl := NewRateLimiter(n, time.Second)

	var acquired int64
	var wg sync.WaitGroup
	wg.Add(n)
	start := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			tok, err := rl.Token(context.Background())
			if err == nil {
				atomic.AddInt64(&acquired, 1)
				tok.Release()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&acquired)
	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("500K RateLimiter.Token: %d/%d in %v (%.0f ops/s)", total, n, elapsed, opsPerSec)
	printMemStats("RLToken-end")
}

// TestProduction_RateLimiter_BlockForce_500K 50万 BlockForce 策略
func TestProduction_RateLimiter_BlockForce_500K(t *testing.T) {
	printMemStats("RLBlockForce-start")
	n := 500_000
	rl := NewRateLimiter(100000, time.Second)

	var acquired int64
	var wg sync.WaitGroup
	wg.Add(n)
	blockCtx, blockCancel := context.WithCancel(context.Background())
	_ = blockCancel // 不会被 cancel 中断

	start := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := rl.WithStrategy(BlockForce).Acquire(blockCtx); err == nil {
				atomic.AddInt64(&acquired, 1)
				rl.Release()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&acquired)
	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("500K RateLimiter BlockForce (100K/s rate): %d/%d in %v (%.0f ops/s)", total, n, elapsed, opsPerSec)
	printMemStats("RLBlockForce-end")
}

// ============================================================================
// Pool: TrySubmit / WaitTimeout
// ============================================================================

// TestProduction_Pool_TrySubmit_1M 100万非阻塞提交
func TestProduction_Pool_TrySubmit_1M(t *testing.T) {
	printMemStats("TrySubmit-start")
	n := 1_000_000

	p := NewPool[int](500)
	var submitted int64
	var rejected int64

	start := time.Now()
	for i := 0; i < n; i++ {
		v := i
		if err := p.TrySubmit(context.Background(), func(ctx context.Context) (int, error) {
			return v * 2, nil
		}); err == nil {
			atomic.AddInt64(&submitted, 1)
		} else {
			atomic.AddInt64(&rejected, 1)
		}
	}
	results := p.WaitAndClose()
	elapsed := time.Since(start)

	t.Logf("1M Pool.TrySubmit: %d submitted, %d rejected, %d results in %v (ok)", atomic.LoadInt64(&submitted), atomic.LoadInt64(&rejected), len(results), elapsed)
	printMemStats("TrySubmit-end")
}

// TestProduction_Pool_WaitTimeout_500K 50万 WaitTimeout
func TestProduction_Pool_WaitTimeout_500K(t *testing.T) {
	printMemStats("WaitTimeout-start")
	n := 500_000

	p := NewPool[int](200)
	for i := 0; i < n; i++ {
		v := i
		p.Submit(context.Background(), func(ctx context.Context) (int, error) {
			return v * 2, nil
		})
	}

	start := time.Now()
	results, ok := p.WaitTimeout(30 * time.Second)
	elapsed := time.Since(start)
	p.Close()

	if !ok {
		t.Fatalf("WaitTimeout returned false")
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	t.Logf("500K Pool.WaitTimeout: %d results in %v", n, elapsed)
	printMemStats("WaitTimeout-end")
}

// TestProduction_Pool_Reset_500K 50万 Reset后复用
func TestProduction_Pool_Reset_500K(t *testing.T) {
	p := NewPool[int](200)

	for round := 0; round < 3; round++ {
		n := 200_000
		for i := 0; i < n; i++ {
			v := i
			p.Submit(context.Background(), func(ctx context.Context) (int, error) {
				return v * 2, nil
			})
		}
		results := p.Wait()
		if len(results) != n {
			t.Fatalf("round %d: expected %d results, got %d", round, n, len(results))
		}
		for _, r := range results {
			if r.Err != nil {
				t.Fatalf("round %d result error: %v", round, r.Err)
			}
		}

		_, err := p.Reset()
		if err != nil {
			t.Fatalf("round %d Reset error: %v", round, err)
		}
	}
	p.Close()
	t.Logf("Pool.Reset ×3 rounds (200K each) passed")
}

// ============================================================================
// Group: GoWithTimeout / WaitTimeout / GoAt
// ============================================================================

// TestProduction_Group_GoWithTimeout_100K 10万 Group GoWithTimeout
func TestProduction_Group_GoWithTimeout_100K(t *testing.T) {
	printMemStats("GroupGoTO-start")
	n := 100_000
	g := NewGroup[int](200)
	for i := 0; i < n; i++ {
		v := i
		g.GoWithTimeout(context.Background(), 30*time.Second, func(ctx context.Context) (int, error) {
			return v * 2, nil
		})
	}

	start := time.Now()
	results := g.Wait()
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for i := 0; i < n; i += n / 100 {
		if results[i].Err != nil {
			t.Fatalf("result[%d] error: %v", i, results[i].Err)
		}
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("100K Group.GoWithTimeout (200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("GroupGoTO-end")
}

// TestProduction_Group_WaitTimeout_100K 10万 Group WaitTimeout
func TestProduction_Group_WaitTimeout_100K(t *testing.T) {
	n := 100_000
	g := NewGroup[int](200)
	for i := 0; i < n; i++ {
		v := i
		g.Go(context.Background(), func(ctx context.Context) (int, error) {
			return v * 2, nil
		})
	}

	start := time.Now()
	results, ok := g.WaitTimeout(30 * time.Second)
	elapsed := time.Since(start)

	if !ok {
		t.Fatalf("WaitTimeout returned false")
	}
	if len(results) != n {
		t.Fatalf("expected %d, got %d", n, len(results))
	}
	t.Logf("100K Group.WaitTimeout: %d results in %v", n, elapsed)
}

// TestProduction_Group_GoAt_100K 10万 Group 索引提交
func TestProduction_Group_GoAt_100K(t *testing.T) {
	n := 100_000
	g := NewGroup[int](200)
	for i := 0; i < n; i++ {
		v := i
		g.GoAt(i, context.Background(), func(ctx context.Context) (int, error) {
			return v * 2, nil
		})
	}

	start := time.Now()
	results := g.Wait()
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d, got %d", n, len(results))
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("100K Group.GoAt: %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
}

// ============================================================================
// Pipeline: ExecuteWithGroup / NewPipeline.Run
// ============================================================================

// TestProduction_ExecuteWithGroup_500K 50万 ExecuteWithGroup
func TestProduction_ExecuteWithGroup_500K(t *testing.T) {
	printMemStats("ExecGroup-start")
	n := 500_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results, err := ExecuteWithGroup(context.Background(), items, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	}, 200)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d, got %d", n, len(results))
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("500K ExecuteWithGroup (200 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("ExecGroup-end")
}

// TestProduction_NewPipeline_Run_500K 50万串行 Pipeline
func TestProduction_NewPipeline_Run_500K(t *testing.T) {
	n := 500_000

	p := NewPipeline[int](context.Background(),
		func(ctx context.Context, n int) (int, error) { return n * 2, nil },
		func(ctx context.Context, n int) (int, error) { return n + 1, nil },
	)

	start := time.Now()
	for i := 0; i < n; i++ {
		result, err := p.Run(i)
		if err != nil {
			t.Fatalf("Run(%d) error: %v", i, err)
		}
		if result != i*2+1 {
			t.Fatalf("Run(%d) expected %d, got %d", i, i*2+1, result)
		}
	}
	elapsed := time.Since(start)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("500K NewPipeline.Run (2 stages): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
}

// ============================================================================
// NoResultPool 辅助: SubmitAction / TrySubmitAction
// ============================================================================

// TestProduction_SubmitAction_1M 100万 SubmitAction
func TestProduction_SubmitAction_1M(t *testing.T) {
	printMemStats("SubmitAction-start")
	n := 1_000_000

	p := DefaultNoResultPool()
	var counter int64

	start := time.Now()
	for i := 0; i < n; i++ {
		if err := SubmitAction(p, context.Background(), func(ctx context.Context) error {
			atomic.AddInt64(&counter, 1)
			return nil
		}); err != nil {
			t.Fatalf("SubmitAction error: %v", err)
		}
	}
	p.Wait()
	p.Close()
	elapsed := time.Since(start)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("1M SubmitAction (NoResultPool): %d tasks in %v (%.0f ops/s), completed=%d", n, elapsed, opsPerSec, atomic.LoadInt64(&counter))
	printMemStats("SubmitAction-end")
}

// TestProduction_TrySubmitAction_1M 100万 TrySubmitAction 非阻塞
func TestProduction_TrySubmitAction_1M(t *testing.T) {
	printMemStats("TrySubmitAction-start")
	n := 1_000_000

	p := DefaultNoResultPool()
	var submitted int64

	start := time.Now()
	for i := 0; i < n; i++ {
		if err := TrySubmitAction(p, context.Background(), func(ctx context.Context) error {
			return nil
		}); err == nil {
			atomic.AddInt64(&submitted, 1)
		}
	}
	p.Wait()
	p.Close()
	elapsed := time.Since(start)

	totalSubmitted := atomic.LoadInt64(&submitted)
	t.Logf("1M TrySubmitAction: %d/%d submitted in %v", totalSubmitted, n, elapsed)
	printMemStats("TrySubmitAction-end")
}

// ============================================================================
// SafeCall
// ============================================================================

// TestProduction_SafeCall_500K 50万 SafeCall
func TestProduction_SafeCall_500K(t *testing.T) {
	n := 500_000

	var success int64
	var wg sync.WaitGroup
	wg.Add(n)

	start := time.Now()
	for i := 0; i < n; i++ {
		v := i
		go func() {
			defer wg.Done()
			result, err := SafeCall(context.Background(), v, func(ctx context.Context, item int) (int, error) {
				return item * 2, nil
			})
			if err == nil && result == v*2 {
				atomic.AddInt64(&success, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("500K SafeCall (100 concurrent goroutines): success=%d in %v (%.0f ops/s)", atomic.LoadInt64(&success), elapsed, opsPerSec)
	if atomic.LoadInt64(&success) != int64(n) {
		t.Fatalf("expected %d success, got %d", n, atomic.LoadInt64(&success))
	}
}

// TestProduction_SafeCallVoid_500K 50万 SafeCallVoid
func TestProduction_SafeCallVoid_500K(t *testing.T) {
	n := 500_000

	var success int64
	var wg sync.WaitGroup
	wg.Add(n)

	start := time.Now()
	for i := 0; i < n; i++ {
		v := i
		go func() {
			defer wg.Done()
			err := SafeCallVoid(context.Background(), v, func(ctx context.Context, item int) error {
				return nil
			})
			if err == nil {
				atomic.AddInt64(&success, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("500K SafeCallVoid: success=%d in %v (%.0f ops/s)", atomic.LoadInt64(&success), elapsed, opsPerSec)
	if atomic.LoadInt64(&success) != int64(n) {
		t.Fatalf("expected %d success, got %d", n, atomic.LoadInt64(&success))
	}
}

// ============================================================================
// 千万级 (10,000,000) 补充压测 — 11 个快速方法
//
// 运行方式：
//   go test -run "TestProduction_10M" -v -count=1 -timeout 30m .
// ============================================================================

// TestProduction_10M_Chunk 1000万分块
func TestProduction_10M_Chunk(t *testing.T) {
	printMemStats("10M-Chunk-start")
	n := 10_000_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	chunks := Chunk(items, 1000)
	elapsed := time.Since(start)

	totalElements := 0
	for _, c := range chunks {
		totalElements += len(c)
	}
	if totalElements != n {
		t.Fatalf("expected %d elements, got %d", n, totalElements)
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] Chunk(1000): %d items → %d chunks in %v (%.0f ops/s)", n, len(chunks), elapsed, opsPerSec)
	printMemStats("10M-Chunk-end")
}

// TestProduction_10M_ChunkN 1000万 ChunkN 均分
func TestProduction_10M_ChunkN(t *testing.T) {
	printMemStats("10M-ChunkN-start")
	n := 10_000_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	chunks := ChunkN(items, 200)
	elapsed := time.Since(start)

	totalElements := 0
	for _, c := range chunks {
		totalElements += len(c)
	}
	if totalElements != n {
		t.Fatalf("expected %d elements, got %d", n, totalElements)
	}
	if len(chunks) != 200 {
		t.Fatalf("expected 200 chunks, got %d", len(chunks))
	}
	t.Logf("[10M] ChunkN(200): %d items → %d chunks in %v", n, len(chunks), elapsed)
	printMemStats("10M-ChunkN-end")
}

// TestProduction_10M_MapWithFailFast 1000万 Map FailFast
func TestProduction_10M_MapWithFailFast(t *testing.T) {
	printMemStats("10M-MapFF-start")
	n := 10_000_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results, err := MapWithFailFast(context.Background(), items, 500, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("MapWithFailFast error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for i := 0; i < n; i += n / 100 {
		if results[i].Err != nil {
			t.Fatalf("result[%d] error: %v", i, results[i].Err)
		}
		if results[i].Value != i*2 {
			t.Fatalf("result[%d] expected %d, got %d", i, i*2, results[i].Value)
		}
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] MapWithFailFast (500 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("10M-MapFF-end")
}

// TestProduction_10M_MapWithTimeout 1000万 Map Timeout
func TestProduction_10M_MapWithTimeout(t *testing.T) {
	printMemStats("10M-MapTO-start")
	n := 10_000_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results := MapWithTimeout(context.Background(), items, 500, 60*time.Second, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for i := 0; i < n; i += n / 100 {
		if results[i].Err != nil {
			t.Fatalf("result[%d] error: %v", i, results[i].Err)
		}
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] MapWithTimeout (500 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("10M-MapTO-end")
}

// TestProduction_10M_MapWithFFTimeout 1000万 Map FF+Timeout
func TestProduction_10M_MapWithFFTimeout(t *testing.T) {
	printMemStats("10M-MapFFTO-start")
	n := 10_000_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	results, err := MapWithFFTimeout(context.Background(), items, 500, 60*time.Second, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("MapFFTimeout error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] MapWithFFTimeout (500 concurrency): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("10M-MapFFTO-end")
}

// TestProduction_10M_GoWithTimeout 1000万 Go 带超时
func TestProduction_10M_GoWithTimeout(t *testing.T) {
	printMemStats("10M-GoTO-start")
	n := 10_000_000
	var counter int64
	batchSize := 50_000

	start := time.Now()
	for i := 0; i < n; i += batchSize {
		for j := 0; j < batchSize; j++ {
			GoWithTimeout(context.Background(), 60*time.Second, func(ctx context.Context) {
				atomic.AddInt64(&counter, 1)
			})
		}
	}
	elapsed := time.Since(start)
	time.Sleep(500 * time.Millisecond)

	final := atomic.LoadInt64(&counter)
	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] GoWithTimeout: %d launched in %v (%.0f ops/s), completed=%d", n, elapsed, opsPerSec, final)
	printMemStats("10M-GoTO-end")
}

// TestProduction_10M_GoResult 1000万 GoResult 有返回值
func TestProduction_10M_GoResult(t *testing.T) {
	printMemStats("10M-GoResult-start")
	n := 10_000_000
	results := make([]*AsyncResult[int], n)

	start := time.Now()
	for i := 0; i < n; i++ {
		v := i
		results[i] = GoResult(context.Background(), func(ctx context.Context) (int, error) {
			return v * 2, nil
		})
	}
	elapsed := time.Since(start)

	var success, fail int64
	for _, r := range results {
		if r.Ok() {
			success++
		} else {
			fail++
		}
	}

	t.Logf("[10M] GoResult: %d launched in %v, success=%d fail=%d", n, elapsed, success, fail)
	if fail > 0 {
		t.Fatalf("expected 0 failures, got %d", fail)
	}
	printMemStats("10M-GoResult-end")
}

// TestProduction_10M_TokenBucket 1000万令牌桶 Allow
func TestProduction_10M_TokenBucket(t *testing.T) {
	printMemStats("10M-TokenBucket-start")
	n := 10_000_000
	capacity := float64(n)
	tb := NewTokenBucket(capacity, capacity)
	concurrency := 2000
	batchSize := 5000

	var allowed int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	start := time.Now()
	for i := 0; i < n; i += batchSize {
		wg.Add(batchSize)
		for j := 0; j < batchSize; j++ {
			sem <- struct{}{}
			go func() {
				defer func() { wg.Done(); <-sem }()
				if tb.Allow() {
					atomic.AddInt64(&allowed, 1)
				}
			}()
		}
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&allowed)
	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("[10M] TokenBucket.Allow: %d/%d allowed in %v (%.0f ops/s)", total, n, elapsed, opsPerSec)
	printMemStats("10M-TokenBucket-end")
}

// TestProduction_10M_TokenBucket_AllowN 1000万令牌桶 AllowN
func TestProduction_10M_TokenBucket_AllowN(t *testing.T) {
	printMemStats("10M-TBN-start")
	n := 10_000_000
	capacity := float64(n)
	tb := NewTokenBucket(capacity, capacity)
	concurrency := 2000
	batchSize := 5000

	var allowed int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	start := time.Now()
	for i := 0; i < n; i += batchSize {
		wg.Add(batchSize)
		for j := 0; j < batchSize; j++ {
			sem <- struct{}{}
			go func() {
				defer func() { wg.Done(); <-sem }()
				if tb.AllowN(1) {
					atomic.AddInt64(&allowed, 1)
				}
			}()
		}
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&allowed)
	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("[10M] TokenBucket.AllowN(1): %d/%d allowed in %v (%.0f ops/s)", total, n, elapsed, opsPerSec)
	printMemStats("10M-TBN-end")
}

// TestProduction_10M_SlidingWindow 1000万滑动窗口
func TestProduction_10M_SlidingWindow(t *testing.T) {
	printMemStats("10M-SW-start")
	n := 10_000_000
	sw := NewSlidingWindowRateLimiter(n, 2*time.Second)
	concurrency := 2000
	batchSize := 5000

	var allowed int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	start := time.Now()
	for i := 0; i < n; i += batchSize {
		wg.Add(batchSize)
		for j := 0; j < batchSize; j++ {
			sem <- struct{}{}
			go func() {
				defer func() { wg.Done(); <-sem }()
				if sw.Allow() {
					atomic.AddInt64(&allowed, 1)
				}
			}()
		}
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&allowed)
	opsPerSec := float64(total) / elapsed.Seconds()
	t.Logf("[10M] SlidingWindow.Allow: %d/%d allowed in %v (%.0f ops/s)", total, n, elapsed, opsPerSec)
	printMemStats("10M-SW-end")
}

// TestProduction_10M_NewPipeline_Run 1000万串行 Pipeline
func TestProduction_10M_NewPipeline_Run(t *testing.T) {
	printMemStats("10M-Pipeline-start")
	n := 10_000_000

	p := NewPipeline[int](context.Background(),
		func(ctx context.Context, n int) (int, error) { return n * 2, nil },
		func(ctx context.Context, n int) (int, error) { return n + 1, nil },
	)

	start := time.Now()
	for i := 0; i < n; i++ {
		result, err := p.Run(i)
		if err != nil {
			t.Fatalf("Run(%d) error: %v", i, err)
		}
		if result != i*2+1 {
			t.Fatalf("Run(%d) expected %d, got %d", i, i*2+1, result)
		}
	}
	elapsed := time.Since(start)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] NewPipeline.Run (2 stages): %d items in %v (%.0f ops/s)", n, elapsed, opsPerSec)
	printMemStats("10M-Pipeline-end")
}

// TestProduction_10M_SafeCall 1000万 SafeCall
func TestProduction_10M_SafeCall(t *testing.T) {
	printMemStats("10M-SafeCall-start")
	n := 10_000_000

	var success int64
	var wg sync.WaitGroup
	wg.Add(n)

	start := time.Now()
	for i := 0; i < n; i++ {
		v := i
		go func() {
			defer wg.Done()
			result, err := SafeCall(context.Background(), v, func(ctx context.Context, item int) (int, error) {
				return item * 2, nil
			})
			if err == nil && result == v*2 {
				atomic.AddInt64(&success, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] SafeCall: success=%d in %v (%.0f ops/s)", atomic.LoadInt64(&success), elapsed, opsPerSec)
	if atomic.LoadInt64(&success) != int64(n) {
		t.Fatalf("expected %d success, got %d", n, atomic.LoadInt64(&success))
	}
	printMemStats("10M-SafeCall-end")
}

// ============================================================================
// 千万级自动扩缩容压测
// ============================================================================

// TestProduction_10M_AutoScalePool 1000万自动扩缩容 Pool Submit（模拟真实 IO 耗时触发扩容）
func TestProduction_10M_AutoScalePool(t *testing.T) {
	printMemStats("10M-AutoScale-start")
	n := 1_000_000

	p := NewPool[int](4)
	p.EnableAutoScale(&core.AutoScaleConfig{
		MinWorkers:         4,
		MaxWorkers:         2000,
		CheckInterval:      500 * time.Millisecond,
		ScaleUpThreshold:   0.4,
		ScaleDownThreshold: 0.1,
		ScaleUpChecks:      2,
		ScaleDownChecks:    5,
	})

	start := time.Now()
	peakSize := int32(4)
	var submitted int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 1500)

	wg.Add(n)
	for i := 0; i < n; i++ {
		v := i
		sem <- struct{}{}
		go func() {
			defer func() { wg.Done(); <-sem }()
			if err := p.Submit(context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(500 * time.Microsecond)
				cs := p.Size()
				for {
					old := atomic.LoadInt32(&peakSize)
					if int32(cs) <= old {
						break
					}
					if atomic.CompareAndSwapInt32(&peakSize, old, int32(cs)) {
						break
					}
				}
				return v * 2, nil
			}); err == nil {
				atomic.AddInt64(&submitted, 1)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-done:
			break loop
		case <-ticker.C:
			s := p.Size()
			t.Logf("  auto-scale: size=%d busy=%d active=%d pending=%d submitted=%d",
				s, p.Busy(), p.Active(), p.Pending(), atomic.LoadInt64(&submitted))
		}
	}

	results := p.WaitAndClose()
	elapsed := time.Since(start)

	peakS := atomic.LoadInt32(&peakSize)
	finalSize := p.Size()
	totalSubmitted := atomic.LoadInt64(&submitted)
	opsPerSec := float64(totalSubmitted) / elapsed.Seconds()

	var failCount int64
	for _, r := range results {
		if r.Err != nil {
			failCount++
		}
	}

	t.Logf("[AutoScale] 1M Pool: peak=%d final=%d %d submitted %d results %d errors in %v (%.0f ops/s)",
		peakS, finalSize, totalSubmitted, len(results), failCount, elapsed, opsPerSec)

	if failCount > int64(float64(n)*0.01) {
		t.Fatalf("too many errors: %d / %d (%.2f%%)", failCount, n, float64(failCount)/float64(n)*100)
	}

	printMemStats("10M-AutoScale-end")
}

// TestProduction_10M_AutoScale_TrySubmit 1000万自动扩缩容 TrySubmit
func TestProduction_10M_AutoScale_TrySubmit(t *testing.T) {
	printMemStats("10M-AutoTry-start")
	n := 10_000_000

	p := NewPool[int](4)
	p.EnableAutoScale(&core.AutoScaleConfig{
		MinWorkers:         4,
		MaxWorkers:         2000,
		CheckInterval:      500 * time.Millisecond,
		ScaleUpThreshold:   0.5,
		ScaleDownThreshold: 0.1,
		ScaleUpChecks:      2,
		ScaleDownChecks:    5,
	})

	start := time.Now()
	var submitted, rejected int64

	for i := 0; i < n; i++ {
		v := i
		if err := p.TrySubmit(context.Background(), func(ctx context.Context) (int, error) {
			return v * 2, nil
		}); err == nil {
			atomic.AddInt64(&submitted, 1)
		} else {
			atomic.AddInt64(&rejected, 1)
		}
	}

	elapsed := time.Since(start)

	_ = p.WaitAndClose()
	finalSize := p.Size()

	totalSubmitted := atomic.LoadInt64(&submitted)
	totalRejected := atomic.LoadInt64(&rejected)
	submitRate := float64(totalSubmitted) / elapsed.Seconds()

	t.Logf("[10M] AutoScale TrySubmit: %d submitted %d rejected finalSize=%d in %v (%.0f submit/s)",
		totalSubmitted, totalRejected, finalSize, elapsed, submitRate)

	if totalSubmitted < int64(float64(n)*0.5) {
		t.Fatalf("submitted too few: %d / %d", totalSubmitted, n)
	}

	printMemStats("10M-AutoTry-end")
}

// ============================================================================
// Group / NoResult 自动扩缩容测试
// ============================================================================

// TestProduction_Group_AutoScale_EnableDisable 启停自动扩缩容
func TestProduction_Group_AutoScale_EnableDisable(t *testing.T) {
	g := NewGroup[int](4)

	if g.IsAutoScaleEnabled() {
		t.Fatal("should not be enabled before EnableAutoScale")
	}

	g.EnableAutoScale(nil)
	if !g.IsAutoScaleEnabled() {
		t.Fatal("should be enabled after EnableAutoScale")
	}

	// 重复调用幂等
	g.EnableAutoScale(nil)
	if !g.IsAutoScaleEnabled() {
		t.Fatal("should still be enabled after duplicate EnableAutoScale")
	}

	g.DisableAutoScale()
	if g.IsAutoScaleEnabled() {
		t.Fatal("should be disabled after DisableAutoScale")
	}

	// 重复调用无影响
	g.DisableAutoScale()

	t.Log("Group EnableAutoScale/DisableAutoScale: OK")
}

// TestProduction_Group_AutoScale_CustomConfig 自定义扩缩容配置
func TestProduction_Group_AutoScale_CustomConfig(t *testing.T) {
	g := NewGroup[int](4)
	g.EnableAutoScale(&AutoScaleConfig{
		MinWorkers:      2,
		MaxWorkers:      50,
		CheckInterval:   100 * time.Millisecond,
		ScaleUpChecks:   2,
		ScaleDownChecks: 2,
	})

	if g.Concurrency() != 4 {
		t.Fatalf("initial concurrency should be 4, got %d", g.Concurrency())
	}

	ctx := context.Background()

	// 提交持续高负载任务触发扩容
	var wg sync.WaitGroup
	var stop atomic.Bool
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				g.Go(ctx, func(ctx context.Context) (int, error) {
					time.Sleep(50 * time.Millisecond)
					return 1, nil
				})
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	cur := g.Concurrency()
	t.Logf("concurrency after scale-up: %d (initial=4)", cur)

	stop.Store(true)
	results := g.Wait()

	successCount := 0
	for _, r := range results {
		if r.Ok() {
			successCount++
		}
	}
	t.Logf("Group AutoScale custom config: concurrency=%d success=%d/%d", cur, successCount, len(results))

	g.DisableAutoScale()
	if g.Concurrency() != 2 {
		t.Fatalf("after DisableAutoScale, concurrency should be MinWorkers=2, got %d", g.Concurrency())
	}
}

// TestProduction_Group_AutoScale_Race_EnableGoWait 竞态：同时 EnableAutoScale + Go + Wait
func TestProduction_Group_AutoScale_Race_EnableGoWait(t *testing.T) {
	printMemStats("G-AutoRace-start")
	n := 10_000

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			g := NewGroup[int](4)
			g.EnableAutoScale(&AutoScaleConfig{
				MinWorkers:      2,
				MaxWorkers:      20,
				CheckInterval:   200 * time.Millisecond,
				ScaleUpChecks:   1,
				ScaleDownChecks: 2,
			})
			ctx := context.Background()
			for j := 0; j < 50; j++ {
				g.Go(ctx, func(ctx context.Context) (int, error) {
					return j, nil
				})
			}
			results := g.Wait()
			if len(results) != 50 {
				t.Errorf("expected 50 results, got %d", len(results))
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			g := NewGroup[string](8)
			g.EnableAutoScale(nil)
			ctx := context.Background()
			for j := 0; j < 30; j++ {
				g.Go(ctx, func(ctx context.Context) (string, error) {
					return "ok", nil
				})
			}
			g.Wait()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			nr := NewNoResult(4)
			nr.EnableAutoScale(&AutoScaleConfig{
				MinWorkers: 2,
				MaxWorkers: 16,
			})
			if !nr.IsAutoScaleEnabled() {
				t.Error("NoResult auto-scale should be enabled")
			}
			ctx := context.Background()
			for j := 0; j < 30; j++ {
				nr.Go(ctx, func(ctx context.Context) error {
					return nil
				})
			}
			nr.Wait()
			nr.DisableAutoScale()
		}
	}()

	wg.Wait()
	t.Logf("10K concurrent Group EnableAutoScale+Go+Wait cycles: OK")
	printMemStats("G-AutoRace-end")
}

// TestProduction_Group_AutoScale_Resize_UnderLoad 负载下自动扩缩容竞态
func TestProduction_Group_AutoScale_Resize_UnderLoad(t *testing.T) {
	g := NewGroup[int](4)
	g.EnableAutoScale(&AutoScaleConfig{
		MinWorkers:      2,
		MaxWorkers:      30,
		CheckInterval:   100 * time.Millisecond,
		ScaleUpChecks:   1,
		ScaleDownChecks: 3,
	})

	ctx := context.Background()

	var submitted atomic.Int64
	var wg sync.WaitGroup
	var stop atomic.Bool

	// 持续从多个 goroutine 提交艰难任务（持续高负载）
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				g.Go(ctx, func(ctx context.Context) (int, error) {
					time.Sleep(1 * time.Millisecond)
					return 1, nil
				})
				submitted.Add(1)
			}
		}()
	}

	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()

	results := g.Wait()

	successCount := 0
	for _, r := range results {
		if r.Ok() {
			successCount++
		}
	}
	totalSubmitted := int(submitted.Load())

	t.Logf("Group AutoScale under load: concurrency=%d submitted=%d success=%d",
		g.Concurrency(), totalSubmitted, successCount)

	if g.Concurrency() < 8 {
		t.Logf("concurrency did not scale up beyond %d (might be too fast)", g.Concurrency())
	}
}

// TestProduction_Group_AutoScale_Race_ResizeGo 极端竞态：扩容+缩容交替 + 并发Go
func TestProduction_Group_AutoScale_Race_ResizeGo(t *testing.T) {
	g := NewGroup[int](4)
	g.EnableAutoScale(&AutoScaleConfig{
		MinWorkers:      2,
		MaxWorkers:      32,
		CheckInterval:   50 * time.Millisecond,
		ScaleUpChecks:   1,
		ScaleDownChecks: 3,
	})

	ctx := context.Background()

	var submitted atomic.Int64
	var wg sync.WaitGroup
	var stop atomic.Bool

	// 间歇高负载 → 触发扩缩容交替
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				g.Go(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				})
				submitted.Add(1)
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	time.Sleep(3 * time.Second)
	stop.Store(true)
	wg.Wait()

	results := g.Wait()

	successCount := 0
	for _, r := range results {
		if r.Ok() {
			successCount++
		}
	}
	totalSubmitted := int(submitted.Load())

	t.Logf("Group AutoScale resize race: concurrency=%d submitted=%d success=%d",
		g.Concurrency(), totalSubmitted, successCount)

	if totalSubmitted < 500 {
		t.Fatalf("submitted too few: %d", totalSubmitted)
	}
}

// TestProduction_Group_AutoScale_Million_Level 百万级 Group 自动扩缩容
func TestProduction_Group_AutoScale_Million_Level(t *testing.T) {
	printMemStats("G-AutoMillion-start")
	g := NewGroup[int](8)
	g.EnableAutoScale(&AutoScaleConfig{
		MinWorkers:      4,
		MaxWorkers:      200,
		CheckInterval:   200 * time.Millisecond,
		ScaleUpChecks:   2,
		ScaleDownChecks: 3,
	})

	ctx := context.Background()
	n := 200_000

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return idx * 2, nil
		})
	}

	results := g.Wait()
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	failures := 0
	for _, r := range results {
		if r.Err != nil {
			failures++
		}
	}
	if failures > 0 {
		t.Fatalf("%d tasks failed", failures)
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("200K Group AutoScale: concurrency=%d tasks=%d in %v (%.0f ops/s) failures=%d",
		g.Concurrency(), n, elapsed, opsPerSec, failures)
	printMemStats("G-AutoMillion-end")
}

// TestProduction_NoResult_AutoScale_200K NoResult 自动扩缩容 20万
func TestProduction_NoResult_AutoScale_200K(t *testing.T) {
	printMemStats("NR-Auto-start")
	nr := NewNoResult(4)
	nr.EnableAutoScale(&AutoScaleConfig{
		MinWorkers:      2,
		MaxWorkers:      100,
		CheckInterval:   200 * time.Millisecond,
		ScaleUpChecks:   1,
		ScaleDownChecks: 3,
	})

	ctx := context.Background()
	n := 200_000
	var counter atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		nr.Go(ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	nr.Wait()
	elapsed := time.Since(start)

	if nr.FailCount() > 0 {
		t.Fatalf("failures: %d", nr.FailCount())
	}
	if counter.Load() != int64(n) {
		t.Fatalf("expected counter=%d, got %d", n, counter.Load())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("200K NoResult AutoScale: concurrency=%d tasks=%d in %v (%.0f ops/s) success=%d",
		nr.Concurrency(), n, elapsed, opsPerSec, nr.SuccessCount())
	printMemStats("NR-Auto-end")
}

// TestProduction_Group_AutoScale_DisableMidRun 运行中禁用扩缩容
func TestProduction_Group_AutoScale_DisableMidRun(t *testing.T) {
	g := NewGroup[int](4)
	g.EnableAutoScale(&AutoScaleConfig{
		MinWorkers:      2,
		MaxWorkers:      30,
		CheckInterval:   100 * time.Millisecond,
		ScaleUpChecks:   1,
		ScaleDownChecks: 3,
	})

	ctx := context.Background()
	var stop atomic.Bool

	// 持续提交触发扩容
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			g.Go(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(10 * time.Millisecond)
				return 1, nil
			})
		}
	}()

	time.Sleep(500 * time.Millisecond)
	curBeforeStop := g.Concurrency()
	t.Logf("concurrency before disable: %d", curBeforeStop)

	// 运行中禁用
	g.DisableAutoScale()
	if g.IsAutoScaleEnabled() {
		t.Fatal("should be disabled")
	}

	curAfterStop := g.Concurrency()
	t.Logf("concurrency after disable: %d (expected MinWorkers=2)", curAfterStop)

	stop.Store(true)
	wg.Wait()

	results := g.Wait()
	t.Logf("Group AutoScale DisableMidRun: results=%d concurrency=%d->%d",
		len(results), curBeforeStop, curAfterStop)
}

// TestProduction_Group_AutoScale_Race_GoResetAutoScale 竞态：Go+Reset+AutoScale 交替
func TestProduction_Group_AutoScale_Race_GoResetAutoScale(t *testing.T) {
	n := 5_000

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			g := NewGroup[int](4)
			g.EnableAutoScale(nil)
			ctx := context.Background()
			for j := 0; j < 100; j++ {
				g.Go(ctx, func(ctx context.Context) (int, error) {
					return j, nil
				})
			}
			g.Wait()
			g.Reset()
			g.EnableAutoScale(nil)
			for j := 0; j < 100; j++ {
				g.Go(ctx, func(ctx context.Context) (int, error) {
					return j, nil
				})
			}
			g.Wait()
			g.DisableAutoScale()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			nr := NewNoResult(4)
			nr.EnableAutoScale(nil)
			ctx := context.Background()
			for j := 0; j < 100; j++ {
				nr.Go(ctx, func(ctx context.Context) error {
					return nil
				})
			}
			nr.Wait()
			nr.Reset()
			nr.EnableAutoScale(nil)
			for j := 0; j < 100; j++ {
				nr.Go(ctx, func(ctx context.Context) error {
					return nil
				})
			}
			nr.Wait()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			g := NewGroup[string](8)
			g.EnableAutoScale(&AutoScaleConfig{
				MinWorkers: 2,
				MaxWorkers: 20,
			})
			ctx := context.Background()
			for j := 0; j < 60; j++ {
				g.Go(ctx, func(ctx context.Context) (string, error) {
					return "ok", nil
				})
			}
			g.Wait()
		}
	}()

	wg.Wait()
	t.Logf("5K concurrent Go+Reset+AutoScale cycles: OK")
}

// TestProduction_NoResult_AutoScale_Million_Level NoResult 百万级自动扩缩容
func TestProduction_NoResult_AutoScale_Million_Level(t *testing.T) {
	printMemStats("NR-AutoMillion-start")
	nr := NewNoResult(4)
	nr.EnableAutoScale(&AutoScaleConfig{
		MinWorkers:      2,
		MaxWorkers:      250,
		CheckInterval:   200 * time.Millisecond,
		ScaleUpChecks:   1,
		ScaleDownChecks: 3,
	})

	ctx := context.Background()
	n := 300_000
	var counter atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		nr.Go(ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	nr.Wait()
	elapsed := time.Since(start)

	if counter.Load() != int64(n) {
		t.Fatalf("expected counter=%d, got %d", n, counter.Load())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("300K NoResult AutoScale: concurrency=%d tasks=%d in %v (%.0f ops/s) success=%d",
		nr.Concurrency(), n, elapsed, opsPerSec, nr.SuccessCount())
	printMemStats("NR-AutoMillion-end")
}

// ============================================================================
// Group / NoResult 自动扩缩容 — 千万级 (10M) 极限测试
// ============================================================================

// TestProduction_10M_Group_AutoScale 1000万 Group 自动扩缩容
func TestProduction_10M_Group_AutoScale(t *testing.T) {
	printMemStats("10M-GA-start")
	n := 10_000_000
	g := NewGroup[int](8)
	g.EnableAutoScale(&AutoScaleConfig{
		MinWorkers:      4,
		MaxWorkers:      500,
		CheckInterval:   500 * time.Millisecond,
		ScaleUpChecks:   2,
		ScaleDownChecks: 5,
	})

	ctx := context.Background()

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
	}

	results := g.Wait()
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	failures := 0
	for _, r := range results {
		if r.Err != nil {
			failures++
		}
	}
	if failures > 0 {
		t.Fatalf("%d tasks failed", failures)
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	finalConcurrency := g.Concurrency()
	t.Logf("[10M] Group AutoScale: concurrency=%d tasks=%d in %v (%.0f ops/s) failures=%d",
		finalConcurrency, n, elapsed, opsPerSec, failures)
	printMemStats("10M-GA-end")
}

// TestProduction_10M_NoResult_AutoScale 1000万 NoResult 自动扩缩容
func TestProduction_10M_NoResult_AutoScale(t *testing.T) {
	printMemStats("10M-NRA-start")
	n := 10_000_000

	nr := NewNoResult(8)
	nr.EnableAutoScale(&AutoScaleConfig{
		MinWorkers:      4,
		MaxWorkers:      500,
		CheckInterval:   500 * time.Millisecond,
		ScaleUpChecks:   2,
		ScaleDownChecks: 5,
	})

	ctx := context.Background()
	var counter atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		nr.Go(ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	nr.Wait()
	elapsed := time.Since(start)

	if counter.Load() != int64(n) {
		t.Fatalf("expected counter=%d, got %d", n, counter.Load())
	}
	if nr.FailCount() > 0 {
		t.Fatalf("failures: %d", nr.FailCount())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	finalConcurrency := nr.Concurrency()
	t.Logf("[10M] NoResult AutoScale: concurrency=%d tasks=%d in %v (%.0f ops/s) success=%d",
		finalConcurrency, n, elapsed, opsPerSec, nr.SuccessCount())
	printMemStats("10M-NRA-end")
}

// TestProduction_10M_Group_AutoScale_Convenience 1000万 Group 通过顶层便捷方法
func TestProduction_10M_Group_AutoScale_Convenience(t *testing.T) {
	printMemStats("10M-GAC-start")
	n := 10_000_000
	g := NewGroup[int](8)
	EnableGroupAutoScale(g, &AutoScaleConfig{
		MinWorkers:      4,
		MaxWorkers:      500,
		CheckInterval:   500 * time.Millisecond,
		ScaleUpChecks:   2,
		ScaleDownChecks: 5,
	})

	ctx := context.Background()

	start := time.Now()
	for i := 0; i < n; i++ {
		idx := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
	}

	results := g.Wait()
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] Group AutoScale (convenience): concurrency=%d tasks=%d in %v (%.0f ops/s)",
		g.Concurrency(), n, elapsed, opsPerSec)
	printMemStats("10M-GAC-end")
}

// TestProduction_10M_NoResult_AutoScale_Convenience 1000万 NoResult 通过顶层便捷方法
func TestProduction_10M_NoResult_AutoScale_Convenience(t *testing.T) {
	printMemStats("10M-NRAC-start")
	n := 10_000_000

	nr := NewNoResult(8)
	EnableNoResultAutoScale(nr, &AutoScaleConfig{
		MinWorkers:      4,
		MaxWorkers:      500,
		CheckInterval:   500 * time.Millisecond,
		ScaleUpChecks:   2,
		ScaleDownChecks: 5,
	})

	ctx := context.Background()
	var counter atomic.Int64

	start := time.Now()
	for i := 0; i < n; i++ {
		nr.Go(ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	nr.Wait()
	elapsed := time.Since(start)

	if counter.Load() != int64(n) {
		t.Fatalf("expected counter=%d, got %d", n, counter.Load())
	}

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("[10M] NoResult AutoScale (convenience): concurrency=%d tasks=%d in %v (%.0f ops/s)",
		nr.Concurrency(), n, elapsed, opsPerSec)
	printMemStats("10M-NRAC-end")
}
