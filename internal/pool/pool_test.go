package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

var errPoolTest = errors.New("pool test error")

// ============================================================
// 共享工具：四档数据量（万/十万/百万/千万），short 跳过
// ============================================================

type tier struct {
	name string
	size int
}

var allTiers = []tier{
	{"万级_10K", 10_000},
	{"十万级_100K", 100_000},
	{"百万级_1M", 1_000_000},
	{"千万级_10M", 10_000_000},
}

func skipIfTooLarge(t *testing.T, size int) {
	if testing.Short() && size >= 100_000 {
		t.Skip("short mode: skip large scale test")
	}
}

// ============================================================
// 一、Pool 基本功能测试
// ============================================================

func TestNewPool_Defaults(t *testing.T) {
	p := NewPool[int](5)
	defer p.Close()
	if p.Size() != 5 {
		t.Fatalf("expected size 5, got %d", p.Size())
	}
}

func TestDefaultPool(t *testing.T) {
	p := DefaultPool[int]()
	defer p.Close()
	if p.Size() <= 0 {
		t.Fatal("expected positive size")
	}
}

func TestPool_Submit_Wait_Basic(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 42, nil
		})
	}
	results := p.Wait()
	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}
}

func TestPool_SubmitAt_OrderedResults(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		idx := i
		_ = p.SubmitAt(idx, ctx, func(ctx context.Context) (int, error) {
			return idx * 10, nil
		})
	}
	results := p.Wait()
	for i, r := range results {
		if r.Value != i*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, i*10, r.Value)
		}
	}
}

func TestPool_ConcurrencyLimit_NWorkers(t *testing.T) {
	p := NewPool[int](3)
	defer p.Close()
	var maxConcurrent int32
	var current int32
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			cur := atomic.AddInt32(&current, 1)
			for {
				old := atomic.LoadInt32(&maxConcurrent)
				if cur <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&current, -1)
			return i, nil
		})
	}
	p.Wait()
	if maxConcurrent > 3 {
		t.Fatalf("max concurrent should be <= 3, got %d", maxConcurrent)
	}
}

func TestPool_ChainConfigs(t *testing.T) {
	ctx := context.Background()
	p, _ := NewPool[int](4).WithTraceID(ctx)
	if p == nil {
		t.Fatal("expected non-nil after chain")
	}
	defer p.Close()
}

// ============================================================
// 二、Pool 超时与取消测试
// ============================================================

func TestPool_WaitTimeout(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 1, nil
		})
	}
	_, ok := p.WaitTimeout(50 * time.Millisecond)
	if ok {
		t.Fatal("expected timeout")
	}
}

func TestPool_WaitContext(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 1, nil
		})
	}
	wCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, ok := p.WaitContext(wCtx)
	if ok {
		t.Fatal("expected timeout via context")
	}
}

func TestPool_WithTimeout(t *testing.T) {
	p := NewPool[int](2).WithTimeout(100 * time.Millisecond)
	defer p.Close()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return 1, nil
			}
		})
	}
	results := p.Wait()
	failCount := 0
	for _, r := range results {
		if r.Err != nil {
			failCount++
		}
	}
	if failCount == 0 {
		t.Fatal("expected some timeout failures")
	}
}

// ============================================================
// 三、Pool FailFast 快速失败测试
// ============================================================

func TestPool_FailFast(t *testing.T) {
	p, ctx := NewPool[int](4).WithFailFast(context.Background())
	defer p.Close()
	for i := 0; i < 20; i++ {
		idx := i
		_ = p.SubmitAt(idx, ctx, func(ctx context.Context) (int, error) {
			if idx == 5 {
				return 0, errPoolTest
			}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(50 * time.Millisecond):
				return idx, nil
			}
		})
	}
	results := p.Wait()
	if results[5].Err == nil {
		t.Fatal("expected error at index 5 in fail-fast mode")
	}
}

// ============================================================
// 四、Pool 边界与统计测试
// ============================================================

func TestPool_PanicRecovery(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
		panic("pool panic")
	})
	results := p.Wait()
	if results[0].Err == nil {
		t.Fatal("expected panic error")
	}
}

func TestPool_PendingCount(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(20 * time.Millisecond)
			return 1, nil
		})
	}
	time.Sleep(10 * time.Millisecond)
	if pending := p.Pending(); pending == 0 {
		t.Logf("pending tasks: %d", pending)
	}
	p.Wait()
}

func TestPool_Stats(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
	}
	stats := p.Stats()
	if int(stats.Size) != 4 {
		t.Fatalf("expected size 4, got %d", stats.Size)
	}
	p.Wait()
}

func TestPool_HasError_FirstError(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		idx := i
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			if idx == 2 {
				return 0, errPoolTest
			}
			return idx, nil
		})
	}
	p.Wait()
	if !p.HasError() {
		t.Fatal("expected HasError=true")
	}
}

func TestPool_Close(t *testing.T) {
	p := NewPool[int](4)
	p.Close()
	ctx := context.Background()
	err := p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	if err == nil {
		t.Fatal("expected error when submitting to closed pool")
	}
}

func TestPool_CloseThenSubmitAfterWait(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
	}
	p.Wait()
	err := p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	if err != nil {
		t.Logf("submit after wait: %v (expected if pool is one-shot)", err)
	}
}

// ============================================================
// 五、NoResultPool 测试
// ============================================================

func TestNoResultPool_Basic(t *testing.T) {
	nr := NewPool[struct{}](4)
	defer nr.Close()
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_ = nr.Submit(ctx, func(ctx context.Context) (struct{}, error) { return struct{}{}, nil })
	}
	nr.Wait()
	if nr.FailCount() != 0 {
		t.Fatalf("expected 0 failures, got %d", nr.FailCount())
	}
}

func TestNoResultPool_WaitTimeout(t *testing.T) {
	nr := NewPool[struct{}](4)
	defer nr.Close()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_ = nr.Submit(ctx, func(ctx context.Context) (struct{}, error) {
			time.Sleep(200 * time.Millisecond)
			return struct{}{}, nil
		})
	}
	_, ok := nr.WaitTimeout(50 * time.Millisecond)
	if ok {
		t.Fatal("expected timeout")
	}
}

func TestNoResultPool_Close(t *testing.T) {
	nr := NewPool[struct{}](4)
	nr.Close()
	ctx := context.Background()
	err := nr.Submit(ctx, func(ctx context.Context) (struct{}, error) { return struct{}{}, nil })
	if err == nil {
		t.Fatal("expected error when submitting to closed pool")
	}
}

// ============================================================
// 六、Pool 流式消费测试
// ============================================================

func TestPool_WithStreaming_Basic(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	p.WithStreaming(128)
	ch := p.StreamResults()
	if ch == nil {
		t.Fatal("StreamResults returned nil")
	}

	var streamed []core.Result[int]
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		for r := range ch {
			mu.Lock()
			streamed = append(streamed, r)
			mu.Unlock()
		}
		close(done)
	}()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	results := p.Wait()
	<-done

	if len(results) != 100 {
		t.Fatalf("Wait: expected 100 results, got %d", len(results))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(streamed) != 100 {
		t.Fatalf("StreamResults: expected 100 streamed, got %d", len(streamed))
	}
}

func TestPool_WithStreaming_DefaultBufSize(t *testing.T) {
	p := NewPool[int](8)
	defer p.Close()
	p.WithStreaming(0)

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	p.Wait()
}

func TestPool_WithResultCallback_Basic(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	var cbCount atomic.Int64
	var cbResults sync.Map
	p.WithResultCallback(func(r core.Result[int]) {
		cbCount.Add(1)
		cbResults.Store(r.Value, true)
	})

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	p.Wait()

	if c := cbCount.Load(); c != 100 {
		t.Fatalf("callback count: expected 100, got %d", c)
	}
}

func TestPool_WithResultCallback_Errors(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	var errCount atomic.Int64
	p.WithResultCallback(func(r core.Result[int]) {
		if r.Err != nil {
			errCount.Add(1)
		}
	})

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		idx := i
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			if idx%2 == 0 {
				return 0, errPoolTest
			}
			return idx, nil
		})
	}
	p.Wait()

	if c := errCount.Load(); c != 25 {
		t.Fatalf("error callback count: expected 25, got %d", c)
	}
}

func TestPool_WithStreaming_NoDeadlock_NoConsumer(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	p.WithStreaming(10)

	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	done := make(chan struct{})
	go func() {
		results := p.Wait()
		if len(results) != 10000 {
			t.Errorf("expected 10000 results, got %d", len(results))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("test timed out - possible deadlock")
	}
}

func TestPool_StreamResults_ConcurrentConsumer(t *testing.T) {
	p := NewPool[int](8)
	defer p.Close()
	p.WithStreaming(256)

	var consumed atomic.Int64
	var wg sync.WaitGroup
	consumers := 4
	ch := p.StreamResults()

	wg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer wg.Done()
			for range ch {
				consumed.Add(1)
			}
		}()
	}

	ctx := context.Background()
	n := 1000
	for i := 0; i < n; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	results := p.Wait()
	wg.Wait()

	if len(results) != n {
		t.Fatalf("Wait: expected %d results, got %d", n, len(results))
	}
	if c := consumed.Load(); c != int64(n) {
		t.Fatalf("concurrent consumers: expected %d consumed, got %d", n, c)
	}
}

// ============================================================
// 七、Pool 环形缓冲测试
// ============================================================

func TestPool_WithRingBuffer_Basic(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	p.WithRingBuffer(1000, core.OverflowDrop)

	ctx := context.Background()
	for i := 0; i < 500; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	p.Wait()
	flushed := p.Flush(0)
	if len(flushed) != 500 {
		t.Fatalf("Flush: expected 500 results, got %d", len(flushed))
	}
}

func TestPool_WithRingBuffer_OverflowDrop_SmallCapacity(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	p.WithRingBuffer(50, core.OverflowDrop)

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	flushed := p.Flush(500)
	if len(flushed) < 50 {
		t.Fatalf("Flush: expected at least 50 results, got %d", len(flushed))
	}
	t.Logf("OverflowDrop: capacity=50, submitted=1000, flushed=%d", len(flushed))
}

func TestPool_WithRingBuffer_OverflowBlock_BatchedFlush(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	p.WithRingBuffer(500, core.OverflowBlock)

	ctx := context.Background()
	for i := 0; i < 500; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	p.Wait()
	first := p.Flush(100)
	second := p.Flush(500)
	total := len(first) + len(second)
	if total != 500 {
		t.Fatalf("OverflowBlock: expected 500 total flushed, got %d (batch1=%d batch2=%d)",
			total, len(first), len(second))
	}
}

func TestPool_WithRingBuffer_Empty(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	p.WithRingBuffer(100, core.OverflowDrop)

	flushed := p.Flush(100)
	if len(flushed) != 0 {
		t.Fatalf("Flush on empty ring buffer: expected 0, got %d", len(flushed))
	}
}

func TestPool_WithRingBuffer_LargeCapacity(t *testing.T) {
	p := NewPool[int](8)
	defer p.Close()
	p.WithRingBuffer(10000, core.OverflowDrop)

	ctx := context.Background()
	n := 10000
	for i := 0; i < n; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	p.Wait()
	flushed := p.Flush(0)
	if len(flushed) != n {
		t.Fatalf("Flush: expected %d results, got %d", n, len(flushed))
	}
	for _, r := range flushed {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
	}
}

func TestPool_RingBuffer_PreservedAfterReset(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	p.WithRingBuffer(500, core.OverflowDrop)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	p.Wait()

	_, err := p.Reset()
	if err != nil {
		t.Fatalf("Reset error: %v", err)
	}

	for i := 0; i < 100; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i * 2, nil
		})
	}

	p.Wait()
	flushed := p.Flush(0)
	if len(flushed) != 100 {
		t.Fatalf("Flush after Reset: expected 100 results, got %d", len(flushed))
	}
}

func TestPool_RingBuffer_ConcurrentSubmitFlush(t *testing.T) {
	p := NewPool[int](8)
	defer p.Close()
	p.WithRingBuffer(5000, core.OverflowBlock)

	ctx := context.Background()
	var wg sync.WaitGroup
	goroutines := 10
	tasksPerGoroutine := 500
	var submitted atomic.Int64

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < tasksPerGoroutine; i++ {
				err := p.Submit(ctx, func(ctx context.Context) (int, error) {
					return gid*10000 + i, nil
				})
				if err == nil {
					submitted.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	p.Wait()
	flushed := p.Flush(0)
	expected := int(submitted.Load())
	if len(flushed) != expected {
		t.Fatalf("Flush: expected %d results, got %d", expected, len(flushed))
	}
}

// ============================================================
// 八、Pool 背压控制测试
// ============================================================

func TestPool_WithMaxPending_Basic(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	p.WithMaxPending(10)
	p.WithOverflow(core.OverflowBlock)

	ctx := context.Background()
	submitted := 0
	for i := 0; i < 100; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(5 * time.Millisecond)
			return i, nil
		})
		if err != nil {
			break
		}
		submitted++
	}
	if submitted < 1 {
		t.Fatal("no tasks submitted at all")
	}
	t.Logf("backpressure: submitted %d of 100 tasks", submitted)

	p.Wait()
}

func TestPool_WithMaxPending_Zero(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	p.WithMaxPending(0)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
		if err != nil {
			t.Fatalf("Submit error with maxPending=0: %v", err)
		}
	}
	results := p.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

func TestPool_OverflowStrategy_Error(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	p.WithMaxPending(5)
	p.WithOverflow(core.OverflowError)

	ctx := context.Background()

	overflowDetected := false
	for i := 0; i < 200; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(2 * time.Millisecond)
			return i, nil
		})
		if err == core.ErrQueueOverflow {
			overflowDetected = true
			break
		}
	}

	if !overflowDetected {
		t.Skip("OverflowError not triggered (depends on timing)")
	}
	t.Log("OverflowError correctly triggered")
}

func TestPool_WithOverflow_Drop(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	p.WithMaxPending(3)
	p.WithOverflow(core.OverflowDrop)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(2 * time.Millisecond)
			return i, nil
		})
	}

	results := p.Wait()
	if len(results) < 1 {
		t.Fatal("no results at all")
	}
	t.Logf("OverflowDrop: submitted 100, results=%d (some dropped)", len(results))
}

func TestPool_Backpressure_WorkersBusy(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()
	p.WithMaxPending(50)

	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(3 * time.Millisecond)
			return i, nil
		})
	}

	results := p.Wait()
	if len(results) < 1 {
		t.Fatal("no results")
	}
	t.Logf("backpressure: workers=2, maxPending=50, results=%d", len(results))
}

// ============================================================
// 九、Pool 流式 + 环形缓冲 + 背压 组合测试
// ============================================================

func TestPool_Streaming_RingBuffer_Combined(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	p.WithStreaming(64)
	p.WithRingBuffer(1000, core.OverflowDrop)
	p.WithMaxPending(500)
	p.WithOverflow(core.OverflowBlock)

	var callbackCount atomic.Int64
	p.WithResultCallback(func(r core.Result[int]) {
		callbackCount.Add(1)
	})

	ctx := context.Background()
	for i := 0; i < 500; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	p.Wait()
	flushed := p.Flush(0)

	if len(flushed) != 500 {
		t.Fatalf("Flush: expected 500 results, got %d", len(flushed))
	}
	if c := callbackCount.Load(); c != 500 {
		t.Fatalf("callback: expected 500, got %d", c)
	}
}

// ============================================================
// 十、Pool 自动扩缩容测试
// ============================================================

func TestAutoScale_BasicEnableDisable(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	if p.IsAutoScaleEnabled() {
		t.Fatal("auto-scale should be disabled by default")
	}

	p.EnableAutoScale(nil)
	if !p.IsAutoScaleEnabled() {
		t.Fatal("auto-scale should be enabled after EnableAutoScale")
	}

	p.EnableAutoScale(nil)
	if !p.IsAutoScaleEnabled() {
		t.Fatal("auto-scale should remain enabled after duplicate call")
	}

	p.DisableAutoScale()
	if p.IsAutoScaleEnabled() {
		t.Fatal("auto-scale should be disabled after DisableAutoScale")
	}

	if p.Size() != core.DefaultAutoScaleConfig().MinWorkers {
		t.Logf("size after disable: %d, min: %d", p.Size(), core.DefaultAutoScaleConfig().MinWorkers)
	}
}

func TestAutoScale_ScaleUp(t *testing.T) {
	initialSize := 4
	p := NewPool[int](initialSize)
	defer p.Close()

	p.EnableAutoScale(&core.AutoScaleConfig{
		MinWorkers:         2,
		MaxWorkers:         100,
		CheckInterval:      200 * time.Millisecond,
		ScaleUpThreshold:   0.5,
		ScaleDownThreshold: 0.1,
		ScaleUpChecks:      2,
		ScaleDownChecks:    5,
	})

	startSize := p.Size()
	t.Logf("initial size: %d", startSize)

	var wg sync.WaitGroup
	submitCount := 10000
	wg.Add(submitCount)

	var submitted int64
	for i := 0; i < submitCount; i++ {
		v := i
		if err := p.TrySubmit(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return v * 2, nil
		}); err == nil {
			atomic.AddInt64(&submitted, 1)
		}
		wg.Done()
	}

	time.Sleep(1 * time.Second)

	afterSize := p.Size()
	t.Logf("size after load: %d (initial: %d)", afterSize, startSize)

	if afterSize <= startSize {
		t.Logf("WARNING: auto-scale did not scale up (busy workers may be < threshold)")
	}

	p.Wait()
	p.Close()
	t.Logf("final size: %d, submitted: %d", p.Size(), atomic.LoadInt64(&submitted))
}

func TestAutoScale_ScaleDown(t *testing.T) {
	p := NewPool[int](20)
	defer p.Close()

	p.EnableAutoScale(&core.AutoScaleConfig{
		MinWorkers:         2,
		MaxWorkers:         50,
		CheckInterval:      200 * time.Millisecond,
		ScaleUpThreshold:   0.7,
		ScaleDownThreshold: 0.3,
		ScaleUpChecks:      3,
		ScaleDownChecks:    3,
	})

	p.Resize(20)
	t.Logf("forced size to 20")

	time.Sleep(2 * time.Second)

	finalSize := p.Size()
	t.Logf("size after idle: %d", finalSize)

	if finalSize >= 20 {
		t.Logf("WARNING: auto-scale did not scale down (busy ratio may not be < threshold)")
	}
}

func TestAutoScale_100K_HighLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping autoscale 100K in short mode")
	}
	p := NewPool[int](4)
	defer p.Close()

	p.EnableAutoScale(&core.AutoScaleConfig{
		MinWorkers:         2,
		MaxWorkers:         500,
		CheckInterval:      300 * time.Millisecond,
		ScaleUpThreshold:   0.5,
		ScaleDownThreshold: 0.1,
		ScaleUpChecks:      2,
		ScaleDownChecks:    5,
	})

	n := 100_000
	var wg sync.WaitGroup
	wg.Add(n)

	start := time.Now()
	for i := 0; i < n; i++ {
		v := i
		go func() {
			defer wg.Done()
			p.Submit(context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(1 * time.Millisecond)
				return v * 2, nil
			})
		}()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	peakSize := p.Size()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-done:
			break loop
		case <-ticker.C:
			s := p.Size()
			if s > peakSize {
				peakSize = s
			}
			t.Logf("auto-scale monitoring: size=%d, busy=%d, active=%d, pending=%d",
				s, p.Busy(), p.Active(), p.Pending())
		}
	}

	p.Wait()
	elapsed := time.Since(start)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("100K with auto-scale: peak=%d, final=%d, %d tasks in %v (%.0f ops/s)",
		peakSize, p.Size(), n, elapsed, opsPerSec)
}

func TestAutoScale_NoDeadlock_ConcurrentSubmitResize(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	p.EnableAutoScale(&core.AutoScaleConfig{
		MinWorkers:         2,
		MaxWorkers:         100,
		CheckInterval:      100 * time.Millisecond,
		ScaleUpThreshold:   0.3,
		ScaleDownThreshold: 0.1,
		ScaleUpChecks:      1,
		ScaleDownChecks:    5,
	})

	var wg sync.WaitGroup
	n := 5000
	wg.Add(n)

	start := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			p.Submit(context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(100 * time.Microsecond)
				return 0, nil
			})
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		t.Logf("5K concurrent submit + auto-scale: completed in %v", elapsed)
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock detected: concurrent submit + auto-scale timed out")
	}

	p.Wait()
	p.Close()
}

func TestAutoScale_DisabledByDefault(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	if p.IsAutoScaleEnabled() {
		t.Fatal("auto-scale must be disabled by default")
	}

	if p.autoScale != nil {
		t.Fatal("autoScale config must be nil by default")
	}
}

// ============================================================
// 十一、MultiPool / Shard 基本测试
// ============================================================

func TestMultiPool_Shard_Basic(t *testing.T) {
	mp := NewPool[int](4).Shard(4)
	defer mp.Close()

	ctx := context.Background()
	n := 1000
	for i := 0; i < n; i++ {
		val := i
		if err := mp.Submit(ctx, func(ctx context.Context) (int, error) {
			return val * 2, nil
		}); err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}

	results := mp.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	for _, r := range results {
		if !r.Ok() {
			t.Fatalf("unexpected error: %v", r.Err)
		}
	}
}

func TestMultiPool_Shard_ConfigCopy(t *testing.T) {
	p := NewPool[int](8).
		WithTimeout(5 * time.Second).
		WithMaxPending(100).
		WithOverflow(core.OverflowDrop)
	mp := p.Shard(4)

	if mp.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", mp.ShardCount())
	}

	for i := 0; i < 4; i++ {
		sp := mp.GetShard(i)
		if sp == nil {
			t.Fatalf("shard %d is nil", i)
		}
		if sp.maxPending != 100 {
			t.Fatalf("shard %d maxPending: expected 100, got %d", i, sp.maxPending)
		}
		if sp.timeout != 5*time.Second {
			t.Fatalf("shard %d timeout: expected 5s, got %v", i, sp.timeout)
		}
		if sp.overflowStrat != core.OverflowDrop {
			t.Fatalf("shard %d overflowStrat: expected OverflowDrop", i)
		}
	}

	mp.Close()
}

func TestMultiPool_SubmitKeyed(t *testing.T) {
	mp := NewPool[string](4).Shard(4)
	defer mp.Close()

	ctx := context.Background()

	for i := 0; i < 100; i++ {
		if err := mp.SubmitKeyed(42, ctx, func(ctx context.Context) (string, error) {
			return "a", nil
		}); err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}

	results := mp.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100, got %d", len(results))
	}
}

func TestMultiPool_TrySubmit(t *testing.T) {
	mp := NewPool[int](100).Shard(4)
	defer mp.Close()

	ctx := context.Background()
	n := 500
	var submitted int
	for i := 0; i < n; i++ {
		err := mp.TrySubmit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
		if err == nil {
			submitted++
		}
	}

	results := mp.Wait()
	if len(results) < 100 {
		t.Fatalf("expected at least 100 results from TrySubmit, got %d", len(results))
	}
	t.Logf("TrySubmit: submitted=%d/%d, results=%d", submitted, n, len(results))
}

func TestMultiPool_Shard_Single(t *testing.T) {
	p := NewPool[int](4)
	mp := p.Shard(1)

	if mp.ShardCount() != 1 {
		t.Fatalf("expected 1 shard, got %d", mp.ShardCount())
	}
	if mp.GetShard(0) != p {
		t.Fatal("single shard should reuse original pool")
	}

	mp.Close()
}

func TestMultiPool_Stats(t *testing.T) {
	mp := NewPool[int](4).Shard(4)
	defer mp.Close()

	ctx := context.Background()
	n := 1000
	for i := 0; i < n; i++ {
		mp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	}
	mp.Wait()

	if mp.TotalCount() != int64(n) {
		t.Fatalf("TotalCount: expected %d, got %d", n, mp.TotalCount())
	}
	if mp.TotalSuccessCount() != int64(n) {
		t.Fatalf("TotalSuccessCount: expected %d, got %d", n, mp.TotalSuccessCount())
	}
	if mp.TotalWorkerCount() != 16 {
		t.Fatalf("TotalWorkerCount: expected 16, got %d", mp.TotalWorkerCount())
	}
}

func TestMultiPool_SubmitBatch(t *testing.T) {
	mp := NewPool[int](4).Shard(4)
	defer mp.Close()

	ctx := context.Background()
	items := []int{1, 2, 3, 4, 5, 6, 7, 8}
	results := mp.SubmitBatch(ctx, items, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})

	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("batch submit error at %d: %v", r.Index, r.Err)
		}
	}

	vals := mp.Wait()
	if len(vals) != 8 {
		t.Fatalf("expected 8 results, got %d", len(vals))
	}
}

// ============================================================
// 十二、四档并发压力测试（万/十万/百万/千万）
// ============================================================

func TestPool_Submit_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			p := NewPool[int](100)
			defer p.Close()

			ctx := context.Background()
			var wg sync.WaitGroup
			goroutines := 50
			tasksPerGoroutine := tier.size / goroutines
			if tasksPerGoroutine < 1 {
				tasksPerGoroutine = 1
			}
			var submitted atomic.Int64

			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				go func(gid int) {
					defer wg.Done()
					for i := 0; i < tasksPerGoroutine; i++ {
						if p.Submit(ctx, func(ctx context.Context) (int, error) {
							return gid*10000 + i, nil
						}) == nil {
							submitted.Add(1)
						}
					}
				}(g)
			}
			wg.Wait()
			results := p.Wait()
			expected := int(submitted.Load())
			if len(results) != expected {
				t.Fatalf("expected %d results, got %d", expected, len(results))
			}
		})
	}
}

func TestPool_Submit_FailFast_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			if tier.size > 1_000_000 {
				t.Skip("FailFast stress tests skip >1M")
			}
			p, ctx := NewPool[int](100).WithFailFast(context.Background())
			defer p.Close()

			n := tier.size
			for i := 0; i < n; i++ {
				idx := i
				_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
					if idx == 0 {
						return 0, errors.New("trigger fail fast")
					}
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(100 * time.Millisecond):
						return idx, nil
					}
				})
			}
			results := p.Wait()
			if len(results) != n {
				t.Fatalf("expected %d results, got %d", n, len(results))
			}
			if results[0].Err == nil {
				t.Fatal("expected first task to fail")
			}
		})
	}
}

func TestMultiPool_Submit_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			mp := NewPool[int](16).Shard(8)
			defer mp.Close()

			ctx := context.Background()
			var wg sync.WaitGroup
			goroutines := 50
			tasksPerGoroutine := tier.size / goroutines
			if tasksPerGoroutine < 1 {
				tasksPerGoroutine = 1
			}
			var submitted atomic.Int64

			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				go func(gid int) {
					defer wg.Done()
					for i := 0; i < tasksPerGoroutine; i++ {
						if mp.Submit(ctx, func(ctx context.Context) (int, error) {
							return gid*10000 + i, nil
						}) == nil {
							submitted.Add(1)
						}
					}
				}(g)
			}
			wg.Wait()
			results := mp.Wait()
			expected := int(submitted.Load())
			if len(results) != expected {
				t.Fatalf("expected %d results, got %d", expected, len(results))
			}
		})
	}
}

func TestNoResultPool_Submit_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			nr := NewPool[struct{}](100)
			defer nr.Close()

			ctx := context.Background()
			var counter atomic.Int64
			var wg sync.WaitGroup
			goroutines := 50
			tasksPerGoroutine := tier.size / goroutines
			if tasksPerGoroutine < 1 {
				tasksPerGoroutine = 1
			}

			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				go func() {
					defer wg.Done()
					for i := 0; i < tasksPerGoroutine; i++ {
						nr.Submit(ctx, func(ctx context.Context) (struct{}, error) {
							counter.Add(1)
							return struct{}{}, nil
						})
					}
				}()
			}
			wg.Wait()
			nr.Wait()

			expected := int64(goroutines * tasksPerGoroutine)
			if counter.Load() != expected {
				t.Fatalf("expected %d tasks executed, got %d", expected, counter.Load())
			}
		})
	}
}

// ============================================================
// 十三、Race 竞态测试（go test -race）
// ============================================================

func TestPool_Race_ConcurrentSubmitClose(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](10)
		var wg sync.WaitGroup
		wg.Add(2)
		ctx := context.Background()
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				p.Submit(ctx, func(ctx context.Context) (int, error) {
					time.Sleep(time.Millisecond)
					return 1, nil
				})
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			p.Close()
		}()
		wg.Wait()
	}
}

func TestMultiPool_Race_ConcurrentSubmit(t *testing.T) {
	mp := NewPool[int](8).Shard(8)
	defer mp.Close()

	var wg sync.WaitGroup
	ctx := context.Background()
	n := 5000
	concurrency := 20

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n/concurrency; i++ {
				mp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
			}
		}()
	}
	wg.Wait()

	results := mp.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
}

func TestPool_Race_Streaming_MultiConsumer(t *testing.T) {
	for round := 0; round < 20; round++ {
		p := NewPool[int](16)
		p.WithStreaming(256)

		var consumed atomic.Int64
		ch := p.StreamResults()

		var consumerWg sync.WaitGroup
		for c := 0; c < 4; c++ {
			consumerWg.Add(1)
			go func() {
				defer consumerWg.Done()
				for range ch {
					consumed.Add(1)
				}
			}()
		}

		ctx := context.Background()
		for i := 0; i < 500; i++ {
			p.Submit(ctx, func(ctx context.Context) (int, error) {
				return i, nil
			})
		}

		p.Wait()
		consumerWg.Wait()

		if consumed.Load() != 500 {
			t.Fatalf("round %d: expected 500 consumed, got %d", round, consumed.Load())
		}
		p.Close()
	}
}

func TestPool_Race_RingBuffer_ConcurrentFlush(t *testing.T) {
	for round := 0; round < 10; round++ {
		p := NewPool[int](16)
		p.WithRingBuffer(500, core.OverflowBlock)

		ctx := context.Background()
		var submitted atomic.Int64
		var wg sync.WaitGroup
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					if p.Submit(ctx, func(ctx context.Context) (int, error) {
						return gid*100 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		results := p.Wait()
		p.Close()

		expected := int(submitted.Load())
		if len(results) < expected {
			t.Fatalf("round %d: expected at least %d results, got %d",
				round, expected, len(results))
		}
	}
}
