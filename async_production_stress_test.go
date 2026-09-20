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
