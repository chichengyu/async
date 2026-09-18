package async

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// 极限高并发压力测试（万级 goroutine）
// ============================================================================

// TestHyperStress_Group_10KGoRoutines 10K goroutine 并发 Go + Wait
func TestHyperStress_Group_10KGoRoutines(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](50)
	n := 10000
	var submitted atomic.Int64
	for i := 0; i < n; i++ {
		idx := i
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}
	results := g.Wait()
	submittedVal := int(submitted.Load())
	if len(results) != submittedVal {
		t.Fatalf("expected %d results, got %d", submittedVal, len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
	}
	t.Logf("10K Group Go+Wait: submitted=%d, results=%d", submittedVal, len(results))
}

// TestHyperStress_Pool_10KSubmitWait 10K goroutine Pool Submit + Wait
func TestHyperStress_Pool_10KSubmitWait(t *testing.T) {
	p := NewPool[int](100)
	ctx := context.Background()
	n := 10000
	var submitted atomic.Int64
	for i := 0; i < n; i++ {
		idx := i
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}
	results := p.Wait()
	p.Close()
	submittedVal := int(submitted.Load())
	if len(results) != submittedVal {
		t.Fatalf("expected %d results, got %d", submittedVal, len(results))
	}
	t.Logf("10K Pool Submit+Wait: submitted=%d, results=%d", submittedVal, len(results))
}

// TestHyperStress_Group_ConcurrentGoWait_100GoRoutines 100 个并发 goroutine 同时 Go，每个 Go 100 个任务
func TestHyperStress_Group_ConcurrentGoWait_100GoRoutines(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		g := NewGroup[int](100)
		var submitWg sync.WaitGroup
		goroutines := 100
		tasksPerGoroutine := 100
		var totalSubmitted atomic.Int64
		submitWg.Add(goroutines)
		for gid := 0; gid < goroutines; gid++ {
			go func(gid int) {
				defer submitWg.Done()
				for tid := 0; tid < tasksPerGoroutine; tid++ {
					err := g.Go(ctx, func(ctx context.Context) (int, error) {
						return gid*1000 + tid, nil
					})
					if err == nil {
						totalSubmitted.Add(1)
					}
				}
			}(gid)
		}
		submitWg.Wait()
		results := g.Wait()
		expected := int(totalSubmitted.Load())
		if len(results) != expected {
			t.Fatalf("round %d: expected %d results, got %d", round, expected, len(results))
		}
	}
}

// TestHyperStress_Pool_ConcurrentSubmitWait_100GoRoutines 100 goroutine Pool 并发提交
func TestHyperStress_Pool_ConcurrentSubmitWait_100GoRoutines(t *testing.T) {
	for round := 0; round < 10; round++ {
		p := NewPool[int](100)
		ctx := context.Background()
		var submitWg sync.WaitGroup
		goroutines := 100
		tasksPerGoroutine := 100
		var totalSubmitted atomic.Int64
		submitWg.Add(goroutines)
		for gid := 0; gid < goroutines; gid++ {
			go func(gid int) {
				defer submitWg.Done()
				for tid := 0; tid < tasksPerGoroutine; tid++ {
					err := p.Submit(ctx, func(ctx context.Context) (int, error) {
						return gid*1000 + tid, nil
					})
					if err == nil {
						totalSubmitted.Add(1)
					}
				}
			}(gid)
		}
		submitWg.Wait()
		results := p.Wait()
		p.Close()
		expected := int(totalSubmitted.Load())
		if len(results) != expected {
			t.Fatalf("round %d: expected %d results, got %d", round, expected, len(results))
		}
	}
}

// TestHyperStress_Map_50KSlice 5万元素的 Map 并发处理
func TestHyperStress_Map_50KSlice(t *testing.T) {
	n := 50000
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
			t.Fatalf("result[%d] expected %d, got %v", i, i*2, r.Value)
		}
	}
	t.Logf("50K Map via 200 concurrency: %v", elapsed)
}

// TestHyperStress_ForEach_50KElements 5万元素的 ForEach 并发遍历
func TestHyperStress_ForEach_50KElements(t *testing.T) {
	n := 50000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}
	var counter atomic.Int64
	start := time.Now()
	nr, err := ForEach(context.Background(), slice, 200, func(ctx context.Context, v int) error {
		counter.Add(1)
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ForEach failed: %v", err)
	}
	if nr.FailCount() > 0 {
		t.Fatalf("expected 0 failures, got %d", nr.FailCount())
	}
	if counter.Load() != int64(n) {
		t.Fatalf("expected %d, got %d", n, counter.Load())
	}
	t.Logf("50K ForEach via 200 concurrency: %v", elapsed)
}

// TestHyperStress_GoResult_10KConcurrentWait 10000 个 AsyncResult 并发 Wait
func TestHyperStress_GoResult_10KConcurrentWait(t *testing.T) {
	n := 10000
	results := make([]*AsyncResult[int], n)
	for i := 0; i < n; i++ {
		idx := i
		results[i] = GoResult(context.Background(), func(ctx context.Context) (int, error) {
			return idx * 10, nil
		})
	}
	var wg sync.WaitGroup
	wg.Add(n)
	var success atomic.Int64
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			val, err := results[idx].Wait()
			if err == nil && val == idx*10 {
				success.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if success.Load() != int64(n) {
		t.Fatalf("expected %d success, got %d", n, success.Load())
	}
}

// TestHyperStress_Go_10KFireAndForget 10000 个 fire-and-forget goroutine
func TestHyperStress_Go_10KFireAndForget(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int64
	n := 10000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		Go(ctx, func(ctx context.Context) {
			defer wg.Done()
			counter.Add(1)
		})
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("10K Go fire-and-forget timed out")
	}
	if counter.Load() != int64(n) {
		t.Fatalf("expected %d, got %d", n, counter.Load())
	}
}

// TestHyperStress_Group_FailFast_10K 10K 任务快速失败
func TestHyperStress_Group_FailFast_10K(t *testing.T) {
	for round := 0; round < 10; round++ {
		g, ctx := NewGroup[int](50).WithFFSubmitTO(context.Background(), 2*time.Second)
		n := 10000
		for i := 0; i < n; i++ {
			idx := i
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				if idx == 0 {
					return 0, errors.New("trigger fail fast")
				}
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-time.After(time.Second):
					return idx, nil
				}
			})
		}
		results := g.Wait()
		if len(results) != n {
			t.Fatalf("round %d: expected %d results, got %d", round, n, len(results))
		}
		// 验证第一个任务失败了
		if results[0].Err == nil {
			t.Fatalf("round %d: expected first task to fail", round)
		}
		// 验证大部分后续任务被跳过或取消
		skippedOrCancelled := 0
		for i := 1; i < n; i++ {
			if results[i].Err != nil {
				skippedOrCancelled++
			}
		}
		t.Logf("round %d: %d/%d subsequent tasks failed/skipped/cancelled", round, skippedOrCancelled, n-1)
	}
}

// ============================================================================
// CPU 密集型并发测试
// ============================================================================

// TestHyperStress_Group_CPUBound_Concurrent CPU 密集型并发
func TestHyperStress_Group_CPUBound_Concurrent(t *testing.T) {
	for round := 0; round < 10; round++ {
		g := NewGroup[int](CPU())
		ctx := context.Background()
		n := 500
		for i := 0; i < n; i++ {
			idx := i
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				sum := 0
				for j := 0; j < 10000; j++ {
					sum += j
				}
				return sum + idx, nil
			})
		}
		results := g.Wait()
		if len(results) != n {
			t.Fatalf("round %d: expected %d results, got %d", round, n, len(results))
		}
		for _, r := range results {
			if r.Err != nil {
				t.Fatalf("round %d: unexpected error: %v", round, r.Err)
			}
		}
	}
}

// ============================================================================
// 边界条件：极限值
// ============================================================================

// TestHyperStress_Group_Concurrency1_SingleSlot 并发限制为 1 的高并发提交
func TestHyperStress_Group_Concurrency1_SingleSlot(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](1)
	n := 5000
	for i := 0; i < n; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(time.Microsecond)
			return 1, nil
		})
	}
	results := g.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
}

// TestHyperStress_Pool_Size1_SingleWorker 只有一个 worker 的高并发 Pool
func TestHyperStress_Pool_Size1_SingleWorker(t *testing.T) {
	p := NewPool[int](1)
	ctx := context.Background()
	n := 5000
	for i := 0; i < n; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(time.Microsecond)
			return 1, nil
		})
	}
	results := p.Wait()
	p.Close()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
}

// ============================================================================
// Group + Pool 混合压力测试
// ============================================================================

// TestHyperStress_MixedGroupAndPool 同时使用 Group 和 Pool 压力测试
func TestHyperStress_MixedGroupAndPool(t *testing.T) {
	var wg sync.WaitGroup
	errCh := make(chan error, 100)
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(round int) {
			defer wg.Done()
			ctx := context.Background()
			if round%2 == 0 {
				g := NewGroup[int](20)
				for j := 0; j < 100; j++ {
					_ = g.Go(ctx, func(ctx context.Context) (int, error) { return j, nil })
				}
				results := g.Wait()
				if len(results) != 100 {
					errCh <- fmt.Errorf("Group round %d: expected 100, got %d", round, len(results))
				}
			} else {
				p := NewPool[int](20)
				for j := 0; j < 100; j++ {
					_ = p.Submit(ctx, func(ctx context.Context) (int, error) { return j, nil })
				}
				results := p.Wait()
				p.Close()
				if len(results) != 100 {
					errCh <- fmt.Errorf("Pool round %d: expected 100, got %d", round, len(results))
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// ============================================================================
// NoResult 高并发测试
// ============================================================================

// TestHyperStress_NoResult_10KFireAndForget NoResult 万级并发
func TestHyperStress_NoResult_10KFireAndForget(t *testing.T) {
	nr := NewNoResult(100)
	ctx := context.Background()
	n := 10000
	var counter atomic.Int64
	for i := 0; i < n; i++ {
		_ = nr.Go(ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}
	nr.Wait()
	if counter.Load() != int64(n) {
		t.Fatalf("expected %d, got %d", n, counter.Load())
	}
	if nr.FailCount() != 0 {
		t.Fatalf("expected 0 failures, got %d", nr.FailCount())
	}
}

// ============================================================================
// MapChunked / ForEachChunked 高并发测试
// ============================================================================

// TestHyperStress_MapChunked_10KItems 分块 Map 万级元素
func TestHyperStress_MapChunked_10KItems(t *testing.T) {
	n := 10000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}
	results := MapChunked(context.Background(), slice, 50, 100, func(ctx context.Context, item int) (int, error) {
		return item * 2, nil
	})
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("result[%d] error: %v", i, r.Err)
		}
		if r.Value != i*2 {
			t.Fatalf("result[%d] expected %d, got %v", i, i*2, r.Value)
		}
	}
}

// TestHyperStress_ForEachChunked_10KItems 分块 ForEach 万级元素
func TestHyperStress_ForEachChunked_10KItems(t *testing.T) {
	n := 10000
	slice := make([]int, n)
	for i := range slice {
		slice[i] = i
	}
	var counter atomic.Int64
	nr, err := ForEachChunked(context.Background(), slice, 50, 100, func(ctx context.Context, item int) error {
		counter.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachChunked failed: %v", err)
	}
	if nr.FailCount() != 0 {
		t.Fatalf("expected 0 failures, got %d", nr.FailCount())
	}
	if counter.Load() != int64(n) {
		t.Fatalf("expected %d, got %d", n, counter.Load())
	}
}

// ============================================================================
// Retry 高并发测试
// ============================================================================

// TestHyperStress_Retry_ConcurrentBackoff 并发 RetryWithBackoff
func TestHyperStress_Retry_ConcurrentBackoff(t *testing.T) {
	var wg sync.WaitGroup
	n := 500
	wg.Add(n)
	var success atomic.Int64
	var fail atomic.Int64
	start := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			err := RetryWithBackoff(context.Background(), 5, time.Millisecond, func(ctx context.Context) error {
				time.Sleep(time.Microsecond * 100)
				return nil
			})
			if err != nil {
				fail.Add(1)
			} else {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if fail.Load() > 0 {
		t.Fatalf("expected 0 failures, got %d", fail.Load())
	}
	if success.Load() != int64(n) {
		t.Fatalf("expected %d success, got %d", n, success.Load())
	}
	t.Logf("500 concurrent RetryWithBackoff: %v", elapsed)
}

// ============================================================================
// Pipeline 高并发测试
// ============================================================================

// TestHyperStress_Pipeline_ConcurrentRun 并发多实例 Pipeline.Run
func TestHyperStress_Pipeline_ConcurrentRun(t *testing.T) {
	for round := 0; round < 50; round++ {
		n := 200
		var wg sync.WaitGroup
		wg.Add(n)
		var success atomic.Int64
		for i := 0; i < n; i++ {
			go func(in int) {
				defer wg.Done()
				p := NewPipeline[int](context.Background(),
					func(ctx context.Context, input int) (int, error) {
						return input + 1, nil
					},
					func(ctx context.Context, input int) (int, error) {
						return input * 2, nil
					},
				)
				result, err := p.Run(in)
				if err == nil && result == (in+1)*2 {
					success.Add(1)
				}
			}(i)
		}
		wg.Wait()
		if success.Load() != int64(n) {
			t.Fatalf("round %d: expected %d success, got %d", round, n, success.Load())
		}
	}
}
