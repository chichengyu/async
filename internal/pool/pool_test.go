package pool

import (
	"context"
	"errors"
	"math"
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

// ==================== SubmitAt 边界与并发 ====================

func TestSubmitAt_IndexBounds(t *testing.T) {
	p := NewPool[int](16)
	defer p.Close()
	ctx := context.Background()

	if err := p.SubmitAt(0, ctx, func(ctx context.Context) (int, error) { return 1, nil }); err != nil {
		t.Fatalf("SubmitAt(0): %v", err)
	}
	if err := p.SubmitAt(100, ctx, func(ctx context.Context) (int, error) { return 100, nil }); err != nil {
		t.Fatalf("SubmitAt(100): %v", err)
	}

	if err := p.SubmitAt(core.MaxPoolIndex+1, ctx, func(ctx context.Context) (int, error) {
		return 0, nil
	}); err != core.ErrInvalidIndex {
		t.Fatalf("SubmitAt(MaxPoolIndex+1) = %v, want ErrInvalidIndex", err)
	}

	if err := p.SubmitAt(math.MaxInt, ctx, func(ctx context.Context) (int, error) {
		return 0, nil
	}); err != core.ErrInvalidIndex {
		t.Fatalf("SubmitAt(MaxInt) = %v, want ErrInvalidIndex", err)
	}

	if err := p.SubmitAt(-1, ctx, func(ctx context.Context) (int, error) {
		return 0, nil
	}); err != core.ErrInvalidIndex {
		t.Fatalf("SubmitAt(-1) = %v, want ErrInvalidIndex", err)
	}
}

func TestProduction_SubmitAt_Concurrent(t *testing.T) {
	p := NewPool[int](16)
	defer p.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	concurrency := 100
	eachCount := 1000
	var errCount int64

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < eachCount; i++ {
				idx := gid*eachCount + i
				if err := p.SubmitAt(idx, ctx, func(ctx context.Context) (int, error) {
					return idx, nil
				}); err != nil {
					atomic.AddInt64(&errCount, 1)
				}
			}
		}(g)
	}
	wg.Wait()
	p.Close()

	if errCount > 0 {
		t.Errorf("concurrent SubmitAt: %d failures, want 0", errCount)
	}
}

func TestProduction_SubmitAt_ClosedPool(t *testing.T) {
	p := NewPool[int](16)
	p.Close()
	ctx := context.Background()

	err := p.SubmitAt(0, ctx, func(ctx context.Context) (int, error) {
		return 0, nil
	})
	if err == nil {
		t.Fatal("expected error on closed pool, got nil")
	}
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

// ==================== helper.go: Submit ====================

func TestHelper_Submit(t *testing.T) {
	ctx := context.Background()

	p, idx, err := Submit(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("Submit err = %v, want nil", err)
	}
	if idx < 0 {
		t.Fatalf("Submit idx = %d, want >= 0", idx)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != 42 {
		t.Fatalf("Submit result: %+v", results)
	}
}

func TestHelper_Submit_Error(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("submit_task_error")

	p, idx, err := Submit(ctx, func(ctx context.Context) (int, error) {
		return 0, sentinel
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("Submit err = %v, want nil", err)
	}
	_ = idx
	results := p.Wait()
	if len(results) != 1 {
		t.Fatalf("len = %d, want 1", len(results))
	}
	if !errors.Is(results[0].Err, sentinel) {
		t.Fatalf("result.Err = %v, want %v", results[0].Err, sentinel)
	}
}

func TestHelper_Submit_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := Submit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Skip("cancelled context submission behavior is implementation-defined")
	}
}

// ==================== helper.go: SubmitN ====================

func TestHelper_SubmitN_Zero(t *testing.T) {
	ctx := context.Background()
	p, results, err := SubmitN(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	}, 0)
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitN err = %v, want nil", err)
	}
	if len(results) != 0 {
		t.Fatalf("SubmitN(0) len = %d, want 0", len(results))
	}
	p.Wait()
}

func TestHelper_SubmitN_One(t *testing.T) {
	ctx := context.Background()
	p, results, err := SubmitN(ctx, func(ctx context.Context) (int, error) {
		return 100, nil
	}, 1)
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitN err = %v, want nil", err)
	}
	if len(results) != 1 {
		t.Fatalf("SubmitN(1) len = %d, want 1", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("SubmitN(1)[0].Err = %v, want nil", results[0].Err)
	}
	if results[0].Index < 0 {
		t.Fatalf("SubmitN(1)[0].Index = %d, want >= 0", results[0].Index)
	}
	out := p.Wait()
	if len(out) != 1 || out[0].Value != 100 {
		t.Fatalf("SubmitN(1) results: %+v", out)
	}
}

func TestHelper_SubmitN_Five(t *testing.T) {
	ctx := context.Background()
	p, results, err := SubmitN(ctx, func(ctx context.Context) (int, error) {
		return 7, nil
	}, 5)
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitN err = %v, want nil", err)
	}
	if len(results) != 5 {
		t.Fatalf("SubmitN(5) len = %d, want 5", len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("SubmitN(5)[%d].Err = %v", i, r.Err)
		}
	}
	out := p.Wait()
	if len(out) != 5 {
		t.Fatalf("SubmitN(5) output len = %d, want 5", len(out))
	}
	for _, o := range out {
		if o.Value != 7 {
			t.Fatalf("SubmitN(5) value = %d, want 7", o.Value)
		}
	}
}

func TestHelper_SubmitN_Hundred(t *testing.T) {
	ctx := context.Background()
	n := 100
	p, results, err := SubmitN(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	}, n)
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitN err = %v, want nil", err)
	}
	if len(results) != n {
		t.Fatalf("SubmitN(%d) len = %d", n, len(results))
	}
	out := p.Wait()
	if len(out) != n {
		t.Fatalf("SubmitN(%d) output len = %d", n, len(out))
	}
}

// ==================== helper.go: SubmitSafeN ====================

func TestHelper_SubmitSafeN_Zero(t *testing.T) {
	ctx := context.Background()
	p, results := SubmitSafeN(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	}, 0)
	defer p.Close()
	if len(results) != 0 {
		t.Fatalf("SubmitSafeN(0) len = %d, want 0", len(results))
	}
}

func TestHelper_SubmitSafeN_One(t *testing.T) {
	ctx := context.Background()
	p, results := SubmitSafeN(ctx, func(ctx context.Context) (int, error) {
		return 200, nil
	}, 1)
	defer p.Close()
	if len(results) != 1 {
		t.Fatalf("SubmitSafeN(1) len = %d, want 1", len(results))
	}
	out := p.Wait()
	if len(out) != 1 || out[0].Value != 200 {
		t.Fatalf("SubmitSafeN(1) results: %+v", out)
	}
}

func TestHelper_SubmitSafeN_Five(t *testing.T) {
	ctx := context.Background()
	p, results := SubmitSafeN(ctx, func(ctx context.Context) (int, error) {
		return 3, nil
	}, 5)
	defer p.Close()
	if len(results) != 5 {
		t.Fatalf("SubmitSafeN(5) len = %d, want 5", len(results))
	}
	out := p.Wait()
	if len(out) != 5 {
		t.Fatalf("SubmitSafeN(5) output len = %d", len(out))
	}
}

func TestHelper_SubmitSafeN_Hundred(t *testing.T) {
	ctx := context.Background()
	n := 100
	p, results := SubmitSafeN(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	}, n)
	defer p.Close()
	if len(results) != n {
		t.Fatalf("SubmitSafeN(%d) len = %d", n, len(results))
	}
	out := p.Wait()
	if len(out) != n {
		t.Fatalf("SubmitSafeN(%d) output len = %d", n, len(out))
	}
}

// ==================== helper.go: SubmitBatch ====================

func TestHelper_SubmitBatch_Empty(t *testing.T) {
	ctx := context.Background()
	items := []int{}
	p, results, err := SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 10, nil
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitBatch empty err = %v, want nil", err)
	}
	if len(results) != 0 {
		t.Fatalf("SubmitBatch empty len = %d, want 0", len(results))
	}
}

func TestHelper_SubmitBatch_One(t *testing.T) {
	ctx := context.Background()
	items := []int{5}
	p, results, err := SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 10, nil
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitBatch err = %v, want nil", err)
	}
	if len(results) != 1 {
		t.Fatalf("SubmitBatch len = %d, want 1", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("SubmitBatch[0].Err = %v", results[0].Err)
	}
	out := p.Wait()
	if len(out) != 1 || out[0].Value != 50 {
		t.Fatalf("SubmitBatch results: %+v", out)
	}
}

func TestHelper_SubmitBatch_Five(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3, 4, 5}
	p, results, err := SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * item, nil
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitBatch err = %v, want nil", err)
	}
	if len(results) != 5 {
		t.Fatalf("SubmitBatch len = %d, want 5", len(results))
	}
	out := p.Wait()
	sort.Slice(out, func(i, j int) bool { return out[i].Value < out[j].Value })
	expected := []int{1, 4, 9, 16, 25}
	for i, o := range out {
		if o.Value != expected[i] {
			t.Fatalf("SubmitBatch[%d] = %d, want %d", i, o.Value, expected[i])
		}
	}
}

func TestHelper_SubmitBatch_DifferentType(t *testing.T) {
	ctx := context.Background()
	items := []string{"a", "bb", "ccc"}
	p, results, err := SubmitBatch(ctx, items, func(ctx context.Context, item string) (int, error) {
		return len(item), nil
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitBatch err = %v, want nil", err)
	}
	if len(results) != 3 {
		t.Fatalf("SubmitBatch len = %d, want 3", len(results))
	}
	out := p.Wait()
	sort.Slice(out, func(i, j int) bool { return out[i].Value < out[j].Value })
	if out[0].Value != 1 || out[1].Value != 2 || out[2].Value != 3 {
		t.Fatalf("SubmitBatch string lengths: %+v", out)
	}
}

// ==================== helper.go: MapPool ====================

func TestHelper_MapPool_Empty(t *testing.T) {
	ctx := context.Background()
	items := []int{}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 2, nil
	}, 0)
	if err != nil {
		t.Fatalf("MapPool empty err = %v, want nil", err)
	}
	if len(results) != 0 {
		t.Fatalf("MapPool empty len = %d, want 0", len(results))
	}
}

func TestHelper_MapPool_One(t *testing.T) {
	ctx := context.Background()
	items := []int{7}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 3, nil
	}, 0)
	if err != nil {
		t.Fatalf("MapPool err = %v, want nil", err)
	}
	if len(results) != 1 || results[0].Value != 21 {
		t.Fatalf("MapPool results: %+v", results)
	}
}

func TestHelper_MapPool_Multiple(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3, 4, 5}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 10, nil
	}, 4)
	if err != nil {
		t.Fatalf("MapPool err = %v, want nil", err)
	}
	if len(results) != 5 {
		t.Fatalf("MapPool len = %d, want 5", len(results))
	}
	vals := make([]int, len(results))
	for i, r := range results {
		vals[i] = r.Value
	}
	sort.Ints(vals)
	for i, v := range vals {
		if v != (i+1)*10 {
			t.Fatalf("MapPool[%d] = %d, want %d", i, v, (i+1)*10)
		}
	}
}

func TestHelper_MapPool_ConcurrencyZero(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item, nil
	}, 0)
	if err != nil {
		t.Fatalf("MapPool concurrency=0 err = %v, want nil", err)
	}
	if len(results) != 3 {
		t.Fatalf("MapPool len = %d, want 3", len(results))
	}
}

func TestHelper_MapPool_ConcurrencyExceedsLen(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item, nil
	}, 100)
	if err != nil {
		t.Fatalf("MapPool concurrency>len err = %v, want nil", err)
	}
	if len(results) != 2 {
		t.Fatalf("MapPool len = %d, want 2", len(results))
	}
}

func TestHelper_MapPool_Error(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("map_error")
	items := []int{1, 2, 3, 4, 5}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		if item == 3 {
			return 0, sentinel
		}
		return item * 10, nil
	}, 2)
	if err != nil {
		t.Fatalf("MapPool err = %v, want nil", err)
	}
	errFound := false
	for _, r := range results {
		if r.Err != nil {
			errFound = true
			if !errors.Is(r.Err, sentinel) {
				t.Fatalf("MapPool error = %v, want %v", r.Err, sentinel)
			}
		}
	}
	if !errFound {
		t.Fatal("MapPool expected error in results")
	}
}

// ==================== helper.go: ForEachPool ====================

func TestHelper_ForEachPool_Empty(t *testing.T) {
	ctx := context.Background()
	items := []int{}
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		return nil
	}, 4)
	if err != nil {
		t.Fatalf("ForEachPool empty err = %v, want nil", err)
	}
	if p == nil {
		t.Fatal("ForEachPool returned nil pool")
	}
}

func TestHelper_ForEachPool_One(t *testing.T) {
	ctx := context.Background()
	var called atomic.Int64
	items := []int{42}
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		called.Add(1)
		if item != 42 {
			t.Errorf("item = %d, want 42", item)
		}
		return nil
	}, 4)
	if err != nil {
		t.Fatalf("ForEachPool err = %v, want nil", err)
	}
	_ = p
	if called.Load() != 1 {
		t.Fatalf("called = %d, want 1", called.Load())
	}
}

func TestHelper_ForEachPool_Multiple(t *testing.T) {
	ctx := context.Background()
	var sum atomic.Int64
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		sum.Add(int64(item))
		return nil
	}, 4)
	if err != nil {
		t.Fatalf("ForEachPool err = %v, want nil", err)
	}
	_ = p
	if sum.Load() != 55 {
		t.Fatalf("sum = %d, want 55", sum.Load())
	}
}

func TestHelper_ForEachPool_Error(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("foreach_error")
	items := []int{1, 2, 3, 4, 5}
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		if item == 3 {
			return sentinel
		}
		return nil
	}, 2)
	_ = p
	if err == nil {
		t.Fatal("ForEachPool expected error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("ForEachPool error = %v, want %v", err, sentinel)
	}
}

func TestHelper_ForEachPool_ConcurrencyZero(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int64
	items := make([]int, 10)
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		counter.Add(1)
		return nil
	}, 0)
	if err != nil {
		t.Fatalf("ForEachPool concurrency=0 err = %v, want nil", err)
	}
	_ = p
	if counter.Load() != 10 {
		t.Fatalf("counter = %d, want 10", counter.Load())
	}
}

func TestHelper_ForEachPool_ConcurrencyExceedsLen(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int64
	items := make([]int, 5)
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		counter.Add(1)
		return nil
	}, 100)
	if err != nil {
		t.Fatalf("ForEachPool concurrency>len err = %v, want nil", err)
	}
	_ = p
	if counter.Load() != 5 {
		t.Fatalf("counter = %d, want 5", counter.Load())
	}
}

// ==================== Resize Boundary Tests ====================

func TestResize_Zero(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	removed := p.Resize(0)
	if removed != 4 {
		t.Fatalf("Resize(0) removed = %d, want 4", removed)
	}
	if p.Size() != 0 {
		t.Fatalf("Size after Resize(0) = %d, want 0", p.Size())
	}
}

func TestResize_Negative(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	removed := p.Resize(-1)
	if removed != 5 {
		t.Fatalf("Resize(-1) removed = %d, want 5 (4 - (-1) = 5)", removed)
	}
	if p.Size() != -1 {
		t.Fatalf("Size after Resize(-1) = %d, want -1", p.Size())
	}
}

func TestResize_SameSize(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	changed := p.Resize(4)
	if changed != 0 {
		t.Fatalf("Resize(4) changed = %d, want 0", changed)
	}
	if p.Size() != 4 {
		t.Fatalf("Size after Resize(4) = %d, want 4", p.Size())
	}
}

func TestResize_VeryLarge(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()

	added := p.Resize(100)
	if added != 98 {
		t.Fatalf("Resize(100) added = %d, want 98", added)
	}
	if p.Size() != 100 {
		t.Fatalf("Size after Resize(100) = %d, want 100", p.Size())
	}
}

func TestResize_RoundTrip(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	if s := p.Size(); s != 4 {
		t.Fatalf("initial size = %d, want 4", s)
	}
	p.Resize(8)
	if s := p.Size(); s != 8 {
		t.Fatalf("after resize up: size = %d, want 8", s)
	}
	p.Resize(2)
	if s := p.Size(); s != 2 {
		t.Fatalf("after resize down: size = %d, want 2", s)
	}
	p.Resize(6)
	if s := p.Size(); s != 6 {
		t.Fatalf("after resize up again: size = %d, want 6", s)
	}
	p.Resize(4)
	if s := p.Size(); s != 4 {
		t.Fatalf("after resize down to original: size = %d, want 4", s)
	}
}

// ==================== CloseAndWaitTimeout Boundary Tests ====================

func TestCloseAndWaitTimeout_Zero(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 1, nil
	})
	ok, doneCh := p.CloseAndWaitTimeout(0)
	if ok {
		t.Log("immediate timeout returned ok=true (may race)")
	}

	select {
	case <-doneCh:
		t.Log("doneCh closed immediately with zero timeout")
	case <-time.After(200 * time.Millisecond):
		t.Log("doneCh not closed within extra wait")
	}
}

func TestCloseAndWaitTimeout_Negative(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 1, nil
	})
	ok, doneCh := p.CloseAndWaitTimeout(-1)
	if ok {
		t.Log("negative timeout returned ok=true (may race)")
	}

	select {
	case <-doneCh:
		t.Log("doneCh closed with negative timeout")
	case <-time.After(200 * time.Millisecond):
		t.Log("doneCh not closed within extra wait")
	}
}

// ==================== WithMaxPending Boundary Tests ====================

func TestWithMaxPending_Zero(t *testing.T) {
	p := NewPool[int](2)
	p.WithMaxPending(0)
	defer p.Close()
	ctx := context.Background()

	var done atomic.Int64
	var submitted atomic.Int64

	for i := 0; i < 10; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			done.Add(1)
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}
	p.Wait()
	t.Logf("WithMaxPending(0): submitted=%d done=%d", submitted.Load(), done.Load())
}

func TestWithMaxPending_Negative(t *testing.T) {
	p := NewPool[int](2)
	p.WithMaxPending(-1)
	defer p.Close()
	ctx := context.Background()

	var done atomic.Int64
	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			done.Add(1)
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
	}
	p.Wait()
	t.Logf("WithMaxPending(-1): done=%d", done.Load())
}

func TestWithMaxPending_BigValue(t *testing.T) {
	p := NewPool[int](2)
	p.WithMaxPending(1_000_000)
	defer p.Close()
	ctx := context.Background()

	var done atomic.Int64
	n := 200
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			done.Add(1)
			time.Sleep(5 * time.Millisecond)
			return 1, nil
		})
	}
	p.Wait()
	if done.Load() != int64(n) {
		t.Fatalf("done = %d, want %d", done.Load(), n)
	}
}

// ==================== Close Boundary Test ====================

func TestClose_CloseTwice(t *testing.T) {
	p := NewPool[int](4)
	p.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Logf("double close caused panic: %v", r)
		}
	}()
	p.Close()
}

func TestClose_SubmitAfterClose(t *testing.T) {
	p := NewPool[int](4)
	p.Close()
	err := p.Submit(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Error("Submit after Close should return an error")
	}
	t.Logf("Submit after Close error: %v", err)
}

func TestClose_TrySubmitAfterClose(t *testing.T) {
	p := NewPool[int](4)
	p.Close()
	err := p.TrySubmit(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Error("TrySubmit after Close should return an error")
	}
	t.Logf("TrySubmit after Close error: %v", err)
}

func TestClose_SubmitAtAfterClose(t *testing.T) {
	p := NewPool[int](4)
	p.Close()
	err := p.SubmitAt(0, context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Error("SubmitAt after Close should return an error")
	}
	t.Logf("SubmitAt after Close error: %v", err)
}

// ==================== Flush Tests ====================

func TestFlush_EmptyPool(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	flushed := p.Flush(100)
	if len(flushed) != 0 {
		t.Fatalf("Flush empty pool len = %d, want 0", len(flushed))
	}
}

func TestFlush_AfterTasks(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	n := 20
	for i := 0; i < n; i++ {
		v := i
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return v, nil
		})
	}

	flushed := p.Flush(100)
	t.Logf("Flushed %d results", len(flushed))
}

// ==================== Stats Boundary Tests ====================

func TestStats_NewPool(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	stats := p.Stats()
	if stats.TotalTask != 0 {
		t.Fatalf("new pool TotalTask = %d, want 0", stats.TotalTask)
	}
	if stats.SuccessTask != 0 {
		t.Fatalf("new pool SuccessTask = %d, want 0", stats.SuccessTask)
	}
	if stats.FailTask != 0 {
		t.Fatalf("new pool FailTask = %d, want 0", stats.FailTask)
	}
}

func TestStats_AfterTasks(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	n := 10
	for i := 0; i < n; i++ {
		var fn func(ctx context.Context) (int, error)
		if i%2 == 0 {
			fn = func(ctx context.Context) (int, error) {
				return i, nil
			}
		} else {
			fn = func(ctx context.Context) (int, error) {
				return 0, errors.New("odd error")
			}
		}
		p.Submit(ctx, fn)
	}
	p.Wait()

	stats := p.Stats()
	if stats.TotalTask != int64(n) {
		t.Fatalf("TotalTask = %d, want %d", stats.TotalTask, n)
	}
	if stats.SuccessTask != 5 {
		t.Fatalf("SuccessTask = %d, want 5", stats.SuccessTask)
	}
	if stats.FailTask != 5 {
		t.Fatalf("FailTask = %d, want 5", stats.FailTask)
	}
}

// ==================== ResizeAndWaitTimeout Boundaries ====================

func TestResizeAndWaitTimeout_NotifyCh(t *testing.T) {
	p := NewPool[int](8)
	defer p.Close()

	notify := make(chan struct{})
	go func() {
		p.ResizeAndWaitTimeout(2, 5*time.Second)
		close(notify)
	}()

	select {
	case <-notify:
		if p.Size() != 2 {
			t.Errorf("Size = %d, want 2", p.Size())
		}
	case <-time.After(6 * time.Second):
		t.Fatal("ResizeAndWaitTimeout did not complete in time")
	}
}

// ==================== Concurrent Resize Tests ====================

func TestConcurrent_Resize(t *testing.T) {
	p := NewPool[int](16)
	defer p.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			sizes := []int{4, 8, 2, 6, 10, 4}
			for _, sz := range sizes {
				p.Resize(sz)
				time.Sleep(time.Duration(gid+1) * time.Millisecond)
			}
		}(g)
	}

	for i := 0; i < 100; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(5 * time.Millisecond)
			return 1, nil
		})
	}

	wg.Wait()
	p.Wait()
	t.Logf("concurrent resize completed, final size=%d", p.Size())
}

// ==================== Flush Boundary Tests ====================

func TestFlush_Boundary_Zero(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(100, core.OverflowBlock)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	p.Wait()

	flushed := p.Flush(0)
	if len(flushed) != 0 {
		t.Logf("Flush(0) returned %d results (ring buffer may retain items)", len(flushed))
	}
}

func TestFlush_Boundary_Negative(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(100, core.OverflowBlock)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	p.Wait()

	flushed := p.Flush(-1)
	t.Logf("Flush(-1) returned %d results (negative treated as flush-all)", len(flushed))
}

// ==================== SubmitAt Cancelled Context ====================

func TestSubmitAt_CancelledContext(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.SubmitAt(0, ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Error("SubmitAt with cancelled context should return an error")
	}
	t.Logf("SubmitAt cancelled ctx error: %v", err)
}

// ==================== TrySubmit Cancelled Context ====================

func TestTrySubmit_CancelledContext(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	t.Logf("TrySubmit with cancelled ctx: err=%v (TrySubmit only checks pool state, not context)", err)
}

// ==================== WithTimeout Negative ====================

func TestWithTimeout_Negative(t *testing.T) {
	p := NewPool[int](4)
	p.WithTimeout(-1)
	defer p.Close()

	if p.timeout != -1 {
		t.Fatalf("WithTimeout(-1) timeout = %v, want -1", p.timeout)
	}

	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 1, nil
	})
	results := p.Wait()
	t.Logf("results with negative timeout: %d", len(results))
}

func TestWithSubmitTimeout_Negative(t *testing.T) {
	p := NewPool[int](4)
	p.WithSubmitTimeout(-1)
	defer p.Close()

	if p.submitTimeout != -1 {
		t.Fatalf("WithSubmitTimeout(-1) = %v, want -1", p.submitTimeout)
	}

	ctx := context.Background()
	err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	t.Logf("submit with negative submitTimeout: err=%v", err)
	p.Close()
}

// ==================== CloseByIdle Boundary ====================

func TestCloseByIdle_Zero(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return i, nil
		})
	}
	p.Wait()

	p.CloseByIdle(0)
	t.Log("CloseByIdle(0) completed")
}

func TestCloseByIdle_Negative(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(10 * time.Millisecond)
		return 1, nil
	})
	p.Wait()

	p.CloseByIdle(-1)
	t.Log("CloseByIdle(-1) completed")
}

// ==================== Config Boundary Tests ====================

func TestConfig_WithSize_Zero(t *testing.T) {
	cfg := Config{}.WithSize(0)
	if cfg.Size != 0 {
		t.Fatalf("WithSize(0) = %d, want 0", cfg.Size)
	}
}

func TestConfig_WithSize_Negative(t *testing.T) {
	cfg := Config{}.WithSize(-1)
	if cfg.Size != -1 {
		t.Fatalf("WithSize(-1) = %d, want -1", cfg.Size)
	}
}

func TestConfig_WithMaxResults_Zero(t *testing.T) {
	cfg := Config{}.WithMaxResults(0)
	if cfg.MaxResults != 0 {
		t.Fatalf("WithMaxResults(0) = %d, want 0", cfg.MaxResults)
	}
}

func TestConfig_WithRingBuf_Zero(t *testing.T) {
	cfg := Config{}.WithRingBuf(0)
	if cfg.RingBufCap != 0 {
		t.Fatalf("WithRingBuf(0) = %d, want 0", cfg.RingBufCap)
	}
}

func TestConfig_WithStreaming_Zero(t *testing.T) {
	cfg := Config{}.WithStreaming(0)
	if cfg.Streaming != 0 {
		t.Fatalf("WithStreaming(0) = %d, want 0", cfg.Streaming)
	}
}

// ==================== WaitTimeout Boundary ====================

func TestWaitTimeout_Negative(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 1, nil
	})

	results, ok := p.WaitTimeout(-1)
	t.Logf("WaitTimeout(-1): ok=%v, results=%d", ok, len(results))
}

func TestWaitTimeout_Zero(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 1, nil
	})

	results, ok := p.WaitTimeout(0)
	if ok {
		t.Log("WaitTimeout(0) returned immediately with complete results (race)")
	}
	_ = results
}

// ==================== ResizeAndWaitTimeout Boundary ====================

func TestResizeAndWaitTimeout_Zero(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	p.ResizeAndWaitTimeout(0, 2500*time.Millisecond)
	t.Logf("ResizeAndWaitTimeout(0): size=%d", p.Size())
}
