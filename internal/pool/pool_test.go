package pool

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

func init() {
	core.SetDefaultMaxResults(100000)
}

// ==================== NewPool / DefaultPool ====================

func TestNewPool_Defaults(t *testing.T) {
	p := NewPool[int](5)
	defer p.Close()
	if p.Size() != 5 {
		t.Fatalf("Size = %d, want 5", p.Size())
	}
}

func TestNewPool_ZeroSize(t *testing.T) {
	p := NewPool[int](0)
	defer p.Close()
	if p.Size() != core.IO() {
		t.Fatalf("Size = %d, want %d (core.IO)", p.Size(), core.IO())
	}
}

func TestDefaultPool(t *testing.T) {
	p := DefaultPool[int]()
	defer p.Close()
	if p.Size() != core.IO() {
		t.Fatalf("DefaultPool Size = %d, want %d", p.Size(), core.IO())
	}
}

// ==================== Submit - Basic ====================

func TestSubmit_Normal(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 42, nil
		})
		if err != nil {
			t.Errorf("Submit(%d): %v", i, err)
		}
	}
	results := p.Wait()
	if len(results) != 10 {
		t.Errorf("len = %d, want 10", len(results))
	}
	for _, r := range results {
		if r.Value != 42 || !r.Ok() {
			t.Errorf("unexpected: %+v", r)
		}
	}
}

func TestSubmit_Error(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	testErr := errors.New("pool error")
	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 0, testErr
		})
	}
	p.Wait()
	if p.FailCount() != 10 {
		t.Errorf("FailCount = %d, want 10", p.FailCount())
	}
}

func TestSubmit_PanicRecovery(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		panic("boom")
	})
	results := p.Wait()
	if len(results) != 1 {
		t.Fatalf("len = %d, want 1", len(results))
	}
	if !results[0].IsPanic() {
		t.Error("expected panic result")
	}
}

// ==================== SubmitAt ====================

func TestSubmitAt(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.SubmitAt(0, ctx, func(ctx context.Context) (int, error) { return 100, nil })
	p.SubmitAt(1, ctx, func(ctx context.Context) (int, error) { return 200, nil })
	results := p.Wait()
	if len(results) < 2 {
		t.Fatalf("len = %d, want >= 2", len(results))
	}
}

// ==================== TrySubmit ====================

func TestTrySubmit(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Errorf("TrySubmit: %v", err)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != 1 {
		t.Errorf("results: %+v", results)
	}
}

// ==================== WithPool / WithCfg ====================

func TestWithPool(t *testing.T) {
	err := WithPool[int](4, func(p *Pool[int]) error {
		ctx := context.Background()
		p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		results := p.Wait()
		if len(results) != 1 || results[0].Value != 1 {
			t.Errorf("results: %+v", results)
		}
		return nil
	})
	if err != nil {
		t.Errorf("WithPool: %v", err)
	}
}

func TestWithCfg(t *testing.T) {
	cfg := DefaultConfig().WithSize(4).WithTimeout(10 * time.Second)
	err := WithCfg[int](context.Background(), cfg, func(p *Pool[int]) error {
		p.Submit(p.Ctx(), func(ctx context.Context) (int, error) { return 2, nil })
		results := p.Wait()
		if len(results) != 1 || results[0].Value != 2 {
			t.Errorf("results: %+v", results)
		}
		return nil
	})
	if err != nil {
		t.Errorf("WithCfg: %v", err)
	}
}

// ==================== FailFast ====================

func TestFailFast(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	pCtx, pCancel := context.WithCancel(context.Background())
	defer pCancel()

	p, ctx := p.WithFailFast(pCtx)

	var executed atomic.Int64
	for i := 0; i < 50; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			executed.Add(1)
			return 0, errors.New("fail")
		})
	}
	p.Wait()
	_ = executed.Load()
}

// ==================== WithTimeout ====================

func TestWithTimeout_Pool(t *testing.T) {
	p := NewPool[int](4)
	p.WithTimeout(50 * time.Millisecond)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})
	results := p.Wait()
	if len(results) == 0 {
		t.Fatal("no results")
	}
	if results[0].Err == nil {
		t.Error("expected timeout error")
	}
}

// ==================== Stats ====================

func TestPoolStats(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err") })
	p.Wait()

	stats := p.Stats()
	if stats.TotalTask != 2 {
		t.Errorf("TotalTask = %d, want 2", stats.TotalTask)
	}
	if stats.SuccessTask != 1 {
		t.Errorf("SuccessTask = %d, want 1", stats.SuccessTask)
	}
	if stats.FailTask != 1 {
		t.Errorf("FailTask = %d, want 1", stats.FailTask)
	}
	if stats.Size != 4 {
		t.Errorf("Size = %d, want 4", stats.Size)
	}
	t.Logf("Stats: %+v", stats)
}

// ==================== Results Accessors ====================

func TestPool_Errors_HasError(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	testErr := errors.New("my error")
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 0, testErr })
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	p.Wait()

	errs := p.Errors()
	if len(errs) != 1 {
		t.Errorf("Errors len = %d, want 1", len(errs))
	}
	if !p.HasError() {
		t.Error("HasError should be true")
	}
}

func TestPool_FirstError(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("first") })
	p.Wait()

	err := p.FirstError()
	if err == nil {
		t.Error("FirstError should not be nil")
	}
}

func TestPool_JoinErrors(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	p.Wait()
	if err := p.JoinErrors(); err != nil {
		t.Errorf("JoinErrors nil: %v", err)
	}

	p2 := NewPool[int](4)
	defer p2.Close()
	p2.Submit(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err1") })
	p2.Submit(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err2") })
	p2.Wait()
	if err := p2.JoinErrors(); err == nil {
		t.Error("JoinErrors should have error")
	}
}

func TestPool_Values(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		v := i
		p.Submit(ctx, func(ctx context.Context) (int, error) { return v * 10, nil })
	}
	p.Wait()
	vals := p.Values()
	sort.Ints(vals)
	if len(vals) != 10 {
		t.Errorf("Values len = %d, want 10", len(vals))
	}
	for i, v := range vals {
		if v != i*10 {
			t.Errorf("Values[%d] = %d, want %d", i, v, i*10)
		}
	}
}

// ==================== WaitTimeout / WaitContext ====================

func TestPool_WaitTimeout(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(300 * time.Millisecond)
			return 1, nil
		})
	}

	results, completed := p.WaitTimeout(30 * time.Millisecond)
	if completed {
		t.Error("expected completed=false")
	}
	_ = results
}

func TestPool_WaitContext(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) { return 99, nil })
	results, completed := p.WaitContext(context.Background())
	if !completed {
		t.Error("expected completed=true")
	}
	if len(results) != 1 || results[0].Value != 99 {
		t.Errorf("results: %+v", results)
	}
}

// ==================== Resize ====================

func TestResize(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	added := p.Resize(8)
	if added != 4 {
		t.Errorf("Resize added = %d, want 4", added)
	}
	if p.Size() != 8 {
		t.Errorf("Size after Resize = %d, want 8", p.Size())
	}

	// Resize to smaller
	removed := p.Resize(2)
	if removed != 6 {
		t.Errorf("Resize removed = %d, want 6", removed)
	}
	if p.Size() != 2 {
		t.Errorf("Size after Resize down = %d, want 2", p.Size())
	}
}

func TestResizeAndWaitTimeout(t *testing.T) {
	p := NewPool[int](8)
	defer p.Close()

	p.ResizeAndWaitTimeout(2, 5*time.Second)
	if p.Size() != 2 {
		t.Errorf("Size = %d, want 2", p.Size())
	}
}

// ==================== Pending / QueueDepth / Active ====================

func TestPending_QueueDepth(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()
	ctx := context.Background()

	if p.Pending() != 0 {
		t.Errorf("Pending idle = %d, want 0", p.Pending())
	}
	if p.QueueDepth() != 0 {
		t.Errorf("QueueDepth idle = %d, want 0", p.QueueDepth())
	}

	// Submit tasks and check
	for i := 0; i < 6; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(20 * time.Millisecond)
			return 1, nil
		})
	}
	p.Wait()
}

func TestPool_Active_Busy(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	if p.Size() != 4 {
		t.Errorf("Size = %d, want 4", p.Size())
	}
}

// ==================== StreamResults / ResultCallback ====================

func TestPool_StreamResults(t *testing.T) {
	p := NewPool[int](4)
	p.WithStreaming(200)
	defer p.Close()
	ctx := context.Background()

	n := 50
	for i := 0; i < n; i++ {
		v := i
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return v, nil
		})
	}

	ch := p.StreamResults()
	var collected []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range ch {
			mu.Lock()
			collected = append(collected, r.Value)
			mu.Unlock()
		}
	}()
	p.Wait()
	wg.Wait()

	sort.Ints(collected)
	if len(collected) != n {
		t.Errorf("collected = %d, want %d", len(collected), n)
	}
}

func TestPool_ResultCallback(t *testing.T) {
	var collected []int
	var mu sync.Mutex
	p := NewPool[int](4)
	p.WithResultCallback(func(r core.Result[int]) {
		mu.Lock()
		collected = append(collected, r.Value)
		mu.Unlock()
	})
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		v := i
		p.Submit(ctx, func(ctx context.Context) (int, error) { return v, nil })
	}
	p.Wait()

	sort.Ints(collected)
	if len(collected) != 20 {
		t.Errorf("collected = %d, want 20", len(collected))
	}
}

// ==================== WithSubmitTimeout ====================

func TestPool_WithSubmitTimeout(t *testing.T) {
	p := NewPool[int](1)
	p.WithSubmitTimeout(10 * time.Millisecond)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 1, nil
	})

	// Second submit should time out waiting for a worker
	err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 2, nil
	})
	if err != nil {
		t.Logf("expected submit timeout: %v", err)
	}
	p.Wait()
}

// ==================== RingBuffer / Overflow ====================

func TestRingBuffer(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(100, core.OverflowBlock)
	defer p.Close()
	ctx := context.Background()

	var counter atomic.Int64
	for i := 0; i < 200; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
		if err != nil {
			t.Logf("Submit(%d) error: %v", i, err)
		}
	}
	p.Wait()
	if counter.Load() != 200 {
		t.Errorf("counter = %d, want 200", counter.Load())
	}
}

// ==================== Flush ====================

func TestFlush(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(100, core.OverflowBlock)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(50 * time.Millisecond)
			return 1, nil
		})
	}

	flushed := p.Flush(10)
	_ = flushed // Flush returns results
}

// ==================== RingBufDropped ====================

func TestRingBufDropped(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(10, core.OverflowDrop)
	defer p.Close()

	dropped := p.RingBufDropped()
	_ = dropped
}

// ==================== StreamDropped ====================

func TestStreamDropped(t *testing.T) {
	p := NewPool[int](4)
	p.WithStreaming(5)
	defer p.Close()

	dropped := p.StreamDropped()
	_ = dropped
}

// ==================== MaxResults / MaxPending ====================

func TestMaxResults(t *testing.T) {
	p := NewPool[int](4)
	p.WithMaxResults(100)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	p.Wait()
}

// ==================== PoolBuilder ====================

func TestPoolBuilder(t *testing.T) {
	err := NewBuilder[int]().Worker(4).Timeout(10 * time.Second).FailFast().Run(func(ctx context.Context, p *Pool[int]) error {
		p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		results := p.Wait()
		if len(results) != 1 || results[0].Value != 1 {
			t.Errorf("results: %+v", results)
		}
		return nil
	})
	if err != nil {
		t.Errorf("Builder: %v", err)
	}
}

func TestPoolBuilder_Run(t *testing.T) {
	err := NewBuilder[int]().Worker(4).Timeout(10 * time.Second).Run(func(ctx context.Context, p *Pool[int]) error {
		p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		results := p.Wait()
		if len(results) != 1 || results[0].Value != 1 {
			t.Errorf("results: %+v", results)
		}
		return nil
	})
	if err != nil {
		t.Errorf("Run: %v", err)
	}
}

// ==================== Close / CloseAndWait / ResetWait / WaitAndClose ====================

func TestClose(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	p.Wait()
	p.Close()
}

func TestCloseAndWait(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 1, nil
	})
	p.CloseAndWait()
}

func TestCloseAndWaitTimeout(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(200 * time.Millisecond)
		return 1, nil
	})
	ok, doneCh := p.CloseAndWaitTimeout(300 * time.Millisecond)
	if ok {
		t.Log("workers stopped within timeout")
	}
	select {
	case <-doneCh:
		t.Log("done channel closed")
	case <-time.After(500 * time.Millisecond):
		// may complete
	}
}

func TestCloseByIdle(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	p.Wait()
	p.CloseByIdle(1 * time.Second)
}

func TestResetWait(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	p.Wait()

	p2 := p.ResetWait()
	defer p2.Close()

	p2.Submit(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	results := p2.Wait()
	if len(results) != 1 || results[0].Value != 2 {
		t.Errorf("ResetWait: %+v", results)
	}
}

func TestReset(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	p.Wait()

	p2, err := p.Reset()
	if err != nil {
		t.Fatalf("Reset: %v", err)
	}
	defer p2.Close()

	p2.Submit(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	results := p2.Wait()
	if len(results) != 1 || results[0].Value != 2 {
		t.Errorf("Reset: %+v", results)
	}
}

// ==================== WaitAndClose ====================

func TestWaitAndClose(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	results := p.WaitAndClose()
	if len(results) != 1 || results[0].Value != 1 {
		t.Errorf("WaitAndClose: %+v", results)
	}
}

// ==================== Busy / TotalCount / Active ====================

func TestBusy_TotalCount_Active(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	if p.Size() != 4 {
		t.Errorf("Size = %d, want 4", p.Size())
	}
	if p.TotalCount() != 0 {
		t.Errorf("TotalCount = %d, want 0", p.TotalCount())
	}
	_ = p.Busy()
	_ = p.Active()
}

// ==================== WithFF variants ====================

func TestPool_WithFFCtx(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFCtx(ctx)
	_ = ctx
}

func TestPool_WithFFTraceID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFTraceID(ctx)
	traceID := core.GetTraceID(ctx)
	if traceID == "" {
		t.Error("trace id should not be empty")
	}
}

func TestPool_WithFFSto(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFSto(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithFFStoTID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFStoTID(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithFFTimeout(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFTimeout(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithCtxTraceID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithCtxTraceID(ctx)
	traceID := core.GetTraceID(ctx)
	if traceID == "" {
		t.Error("trace id should not be empty")
	}
}

func TestPool_WithFFTOTID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFTOTID(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithFFTOSto(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFTOSto(ctx, 10*time.Second, 5*time.Second)
	_ = ctx
}

func TestPool_WithFFTOStoTID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFTOStoTID(ctx, 10*time.Second, 5*time.Second)
	_ = ctx
}

func TestPool_WithCtxTimeout(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithCtxTimeout(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithCtxTOTID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithCtxTOTID(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithCtxSto(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithCtxSto(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithCtxStoTID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithCtxStoTID(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithOverflow(t *testing.T) {
	p := NewPool[int](4)
	p.WithOverflow(core.OverflowBlock)
	defer p.Close()
}

// ==================== AutoScale ====================

func TestPool_AutoScale(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	cfg := &core.AutoScaleConfig{
		MinWorkers:    2,
		MaxWorkers:    8,
		CheckInterval: 100 * time.Millisecond,
	}
	p.EnableAutoScale(cfg)
	if !p.IsAutoScaleEnabled() {
		t.Error("should be enabled")
	}
	p.DisableAutoScale()
	if p.IsAutoScaleEnabled() {
		t.Error("should be disabled")
	}
}

// ==================== WithTraceID ====================

func TestPool_WithTraceID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := p.Ctx()
	p, ctx = p.WithTraceID(ctx)

	traceID := core.GetTraceID(ctx)
	if traceID == "" {
		t.Error("trace id should not be empty")
	}
}

// ==================== WithContext ====================

func TestPool_WithContext(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	pCtx := p.Ctx()
	p, ctx := p.WithContext(pCtx)
	_ = ctx

	p.Submit(p.Ctx(), func(ctx context.Context) (int, error) { return 1, nil })
	p.Wait()
}

// ==================== MergeFailFastCancel ====================

func TestMergeFailFastCancel_Pool(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	pCtx, pCancel := context.WithCancel(context.Background())
	p.MergeFailFastCancel(pCancel)
	_ = pCtx
}

// ==================== WithMaxPending ====================

func TestWithMaxPending(t *testing.T) {
	p := NewPool[int](2)
	p.WithMaxPending(5)
	defer p.Close()
	ctx := context.Background()

	var started atomic.Int64
	for i := 0; i < 20; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			started.Add(1)
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
	}
	p.Wait()
	_ = started.Load()
}

// ==================== Production Concurrent Tests ====================

func TestProduction_PoolMixed(t *testing.T) {
	p := NewPool[int](runtime.NumCPU() * 2)
	defer p.Close()
	ctx := context.Background()

	var successCount, errorCount, panicCount atomic.Int64
	n := 1000

	for i := 0; i < n; i++ {
		mod := i % 5
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			switch mod {
			case 0, 1, 2:
				successCount.Add(1)
				return i, nil
			case 3:
				errorCount.Add(1)
				return 0, errors.New("planned error")
			default:
				panicCount.Add(1)
				panic("planned panic")
			}
		})
		if err != nil {
			t.Errorf("Submit(%d) error: %v", i, err)
		}
	}

	results := p.Wait()
	if len(results) != n {
		t.Errorf("len = %d, want %d", len(results), n)
	}
	if p.SuccessCount() != successCount.Load() {
		t.Errorf("SuccessCount = %d, want %d", p.SuccessCount(), successCount.Load())
	}
	if p.FailCount() != errorCount.Load()+panicCount.Load() {
		t.Errorf("FailCount = %d, want %d", p.FailCount(), errorCount.Load()+panicCount.Load())
	}
}

func TestProduction_PoolHighConcurrency(t *testing.T) {
	p := NewPool[int](runtime.NumCPU() * 4)
	defer p.Close()
	ctx := context.Background()

	n := 5000
	var counter atomic.Int64
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
	}
	results := p.Wait()
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
	if len(results) != n {
		t.Errorf("len = %d, want %d", len(results), n)
	}
}

func TestProduction_PoolResizeRace(t *testing.T) {
	p := NewPool[int](16)
	defer p.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			p.Resize(2 + i%8)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	var counter atomic.Int64
	for i := 0; i < 500; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
	}
	p.Wait()
	wg.Wait()
	_ = counter.Load()
}

func TestProduction_PoolFailFastRace(t *testing.T) {
	p := NewPool[int](runtime.NumCPU() * 2)
	defer p.Close()
	pCtx := p.Ctx()
	p, ctx := p.WithFailFast(pCtx)

	n := 500
	var started atomic.Int64
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			started.Add(1)
			return 0, errors.New("fail")
		})
	}
	p.Wait()
	t.Logf("started = %d/%d", started.Load(), n)
}

func TestProduction_PoolSubmitAtRace(t *testing.T) {
	p := NewPool[int](runtime.NumCPU() * 2)
	defer p.Close()
	ctx := context.Background()

	n := 500
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	results := p.Wait()
	if len(results) != n {
		t.Errorf("len = %d, want %d", len(results), n)
	}
}

func TestProduction_PoolRingBufferRace(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(1000, core.OverflowBlock)
	defer p.Close()
	ctx := context.Background()

	n := 2000
	var counter atomic.Int64
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
	}
	p.Wait()
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
}

func TestProduction_PoolMaxPendingRace(t *testing.T) {
	p := NewPool[int](4)
	p.WithMaxPending(200)
	defer p.Close()
	ctx := context.Background()

	n := 1000
	var counter atomic.Int64
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
	}
	p.Wait()
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
}

func TestProduction_PoolMultiWorkerTypes(t *testing.T) {
	// Submit, SubmitAt, TrySubmit mixed
	p := NewPool[int](8)
	defer p.Close()
	ctx := context.Background()

	var successCount, trySuccess atomic.Int64
	n := 500

	for i := 0; i < n; i++ {
		switch i % 3 {
		case 0:
			p.Submit(ctx, func(ctx context.Context) (int, error) {
				successCount.Add(1)
				return 1, nil
			})
		case 1:
			p.SubmitAt(i, ctx, func(ctx context.Context) (int, error) {
				successCount.Add(1)
				return 1, nil
			})
		case 2:
			err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
				successCount.Add(1)
				trySuccess.Add(1)
				return 1, nil
			})
			if err != nil {
				t.Logf("TrySubmit failed: %v", err)
			}
		}
	}
	p.Wait()
	t.Logf("successCount=%d trySuccess=%d", successCount.Load(), trySuccess.Load())
}
