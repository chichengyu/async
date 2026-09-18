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

var errTest = errors.New("test error")

// ==================== Group 测试 ====================

func TestGroup_Go_Wait(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[string](4)
	for i := 0; i < 10; i++ {
		err := g.Go(ctx, func(ctx context.Context) (string, error) {
			return "ok", nil
		})
		if err != nil {
			t.Fatalf("Go error: %v", err)
		}
	}
	results := g.Wait()
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		if r.Value != "ok" {
			t.Fatalf("expected 'ok', got %s", r.Value)
		}
	}
}

func TestGroup_Go_ConcurrencyLimit(t *testing.T) {
	ctx := context.Background()
	var maxConcurrent int64
	var current int64
	g := NewGroup[int](2)
	for i := 0; i < 20; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			cur := atomic.AddInt64(&current, 1)
			for {
				old := atomic.LoadInt64(&maxConcurrent)
				if cur <= old || atomic.CompareAndSwapInt64(&maxConcurrent, old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt64(&current, -1)
			return i, nil
		})
	}
	results := g.Wait()
	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}
	if maxConcurrent > 2 {
		t.Fatalf("max concurrent should be <= 2, got %d", maxConcurrent)
	}
}

func TestGroup_GoAt_OrderedResults(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)
	for i := 0; i < 10; i++ {
		idx := i
		g.GoAt(idx, ctx, func(ctx context.Context) (int, error) {
			return idx * 10, nil
		})
	}
	results := g.Wait()
	for i, r := range results {
		if r.Value != i*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, i*10, r.Value)
		}
	}
}

func TestGroup_GoAt_Error(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)
	for i := 0; i < 10; i++ {
		idx := i
		g.GoAt(idx, ctx, func(ctx context.Context) (int, error) {
			if idx == 5 {
				return 0, errTest
			}
			return idx, nil
		})
	}
	results := g.Wait()
	if results[5].Err == nil {
		t.Fatal("expected error at index 5")
	}
	if !g.HasError() {
		t.Fatal("expected HasError to be true")
	}
	if g.FailCount() != 1 {
		t.Fatalf("expected 1 fail, got %d", g.FailCount())
	}
	if g.SuccessCount() != 9 {
		t.Fatalf("expected 9 success, got %d", g.SuccessCount())
	}
}

func TestGroup_FailFast(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)
	g.WithFailFast(ctx)
	var executed int32
	for i := 0; i < 20; i++ {
		idx := i
		g.GoAt(idx, ctx, func(ctx context.Context) (int, error) {
			if idx == 3 {
				return 0, errTest
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&executed, 1)
			return idx, nil
		})
	}
	results := g.Wait()
	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}
	if results[3].Err == nil || !errors.Is(results[3].Err, errTest) {
		t.Fatalf("expected errTest at index 3, got: %v", results[3].Err)
	}
	skipped := 0
	for i := 4; i < 20; i++ {
		if errors.Is(results[i].Err, ErrSkipped) {
			skipped++
		}
	}
	if skipped == 0 {
		t.Log("warning: no skipped tasks in fail fast (may have completed before cancellation)")
	}
}

func TestGroup_WaitTimeout(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](2)
	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 1, nil
		})
	}
	results, ok := g.WaitTimeout(50 * time.Millisecond)
	if ok {
		t.Fatal("expected timeout, got ok=true")
	}
	if len(results) < 10 {
		t.Logf("timeout results: %d (expected < 10)", len(results))
	}
}

func TestGroup_WaitContext(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](2)
	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 1, nil
		})
	}
	wCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	results, ok := g.WaitContext(wCtx)
	if ok {
		t.Fatal("expected timeout via context, got ok=true")
	}
	if len(results) < 10 {
		t.Logf("context timeout results: %d (expected < 10)", len(results))
	}
}

func TestGroup_Reset(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](2)
	for i := 0; i < 5; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	results := g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	g.Reset()
	for i := 0; i < 3; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return 2, nil
		})
	}
	results2 := g.Wait()
	if len(results2) != 3 {
		t.Fatalf("expected 3 results after reset, got %d", len(results2))
	}
	for _, r := range results2 {
		if r.Value != 2 {
			t.Fatalf("expected 2, got %d", r.Value)
		}
	}
}

func TestGroup_Errors_FirstError(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](2)
	for i := 0; i < 5; i++ {
		idx := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			if idx%2 == 0 {
				return 0, fmt.Errorf("error %d", idx)
			}
			return idx, nil
		})
	}
	g.Wait()
	errs := g.Errors()
	if len(errs) != 3 {
		t.Fatalf("expected 3 errors, got %d", len(errs))
	}
	first := g.FirstError()
	if first == nil {
		t.Fatal("expected non-nil first error")
	}
}

func TestGroup_Values(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](2)
	for i := 0; i < 5; i++ {
		idx := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			if idx == 2 {
				return 0, errTest
			}
			return idx, nil
		})
	}
	g.Wait()
	vals := g.Values()
	if len(vals) != 4 {
		t.Fatalf("expected 4 values, got %d", len(vals))
	}
}

func TestGroup_JoinErrors(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](2)
	for i := 0; i < 3; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return 0, errTest
		})
	}
	g.Wait()
	joined := g.JoinErrors()
	if joined == nil {
		t.Fatal("expected non-nil joined error")
	}
}

func TestGroup_Stats(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](2)
	for i := 0; i < 10; i++ {
		idx := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			if idx < 3 {
				return 0, errTest
			}
			return idx, nil
		})
	}
	g.Wait()
	if g.TotalCount() != 10 {
		t.Fatalf("expected 10 total, got %d", g.TotalCount())
	}
	if g.FailCount() != 3 {
		t.Fatalf("expected 3 fail, got %d", g.FailCount())
	}
	if g.SuccessCount() != 7 {
		t.Fatalf("expected 7 success, got %d", g.SuccessCount())
	}
}

func TestGroup_Active_Busy(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](3)
	var started sync.WaitGroup
	started.Add(3)
	var blockCh = make(chan struct{})
	for i := 0; i < 3; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			started.Done()
			<-blockCh
			return 1, nil
		})
	}
	started.Wait()
	time.Sleep(20 * time.Millisecond)
	if g.Active() != 3 {
		t.Fatalf("expected 3 active, got %d", g.Active())
	}
	if g.Busy() != 3 {
		t.Fatalf("expected 3 busy, got %d", g.Busy())
	}
	close(blockCh)
	g.Wait()
}

func TestGroup_NoResult(t *testing.T) {
	ctx := context.Background()
	nr := NewNoResult(4)
	var counter int32
	for i := 0; i < 10; i++ {
		nr.Go(ctx, func(ctx context.Context) error {
			atomic.AddInt32(&counter, 1)
			return nil
		})
	}
	nr.Wait()
	if atomic.LoadInt32(&counter) != 10 {
		t.Fatalf("expected 10, got %d", counter)
	}
}

func TestGroup_NoResult_FailFast(t *testing.T) {
	ctx := context.Background()
	nr := NewNoResult(4)
	nr.WithFailFast(ctx)
	var counter int32
	for i := 0; i < 20; i++ {
		idx := i
		nr.Go(ctx, func(ctx context.Context) error {
			if idx == 3 {
				panic("fail fast trigger")
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&counter, 1)
			return nil
		})
	}
	nr.Wait()
	if nr.FailCount() == 0 {
		t.Fatal("expected at least 1 failure")
	}
}

func TestGroup_WithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	g := NewGroup[int](4)
	g.WithContext(ctx)
	var executed int32
	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(100 * time.Millisecond):
				atomic.AddInt32(&executed, 1)
				return 1, nil
			}
		})
	}
	time.Sleep(10 * time.Millisecond)
	cancel()
	g.Wait()
	cnt := atomic.LoadInt32(&executed)
	t.Logf("executed before cancel: %d", cnt)
}

func TestGroup_WithTimeout(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](2)
	g.WithTimeout(50 * time.Millisecond)
	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 1, nil
		})
	}
	results := g.Wait()
	timeoutCount := 0
	for _, r := range results {
		if errors.Is(r.Err, context.DeadlineExceeded) {
			timeoutCount++
		}
	}
	t.Logf("timeout count: %d / %d", timeoutCount, len(results))
}

// ==================== Pool 测试 ====================

func TestPool_Submit_Wait(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	defer p.Close()
	for i := 0; i < 10; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
		if err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}
	results := p.Wait()
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Value != 1 {
			t.Fatalf("expected 1, got %d", r.Value)
		}
	}
}

func TestPool_SubmitAt_Ordered(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	defer p.Close()
	for i := 0; i < 10; i++ {
		idx := i
		err := p.SubmitAt(idx, ctx, func(ctx context.Context) (int, error) {
			return idx * 100, nil
		})
		if err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}
	results := p.Wait()
	for i, r := range results {
		if r.Value != i*100 {
			t.Fatalf("at index %d: expected %d, got %d", i, i*100, r.Value)
		}
	}
}

func TestPool_ConcurrencyLimit(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](3)
	defer p.Close()
	var maxConcurrent int64
	var current int64
	for i := 0; i < 30; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			cur := atomic.AddInt64(&current, 1)
			for {
				old := atomic.LoadInt64(&maxConcurrent)
				if cur <= old || atomic.CompareAndSwapInt64(&maxConcurrent, old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt64(&current, -1)
			return i, nil
		})
		if err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}
	p.Wait()
	if maxConcurrent > 3 {
		t.Fatalf("max concurrent should be <= 3, got %d", maxConcurrent)
	}
}

func TestPool_CloseAndWait(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
	}
	p.CloseAndWait()
	err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
}

func TestPool_CloseAndWaitTimeout_Ok(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
	}
	ok, wd := p.CloseAndWaitTimeout(2 * time.Second)
	if !ok {
		t.Fatal("expected ok=true, all tasks should finish within timeout")
	}
	if wd != nil {
		t.Fatal("expected nil workerDone when ok=true")
	}
}

func TestPool_CloseAndWaitTimeout_Timeout(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](2)
	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 1, nil
		})
	}
	ok, wd := p.CloseAndWaitTimeout(30 * time.Millisecond)
	if ok {
		t.Fatal("expected ok=false due to timeout")
	}
	if wd == nil {
		t.Fatal("expected non-nil workerDone when timeout")
	}
	select {
	case <-wd:
	case <-time.After(2 * time.Second):
		t.Fatal("workerDone channel did not close in time")
	}
}

func TestPool_Close(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	for i := 0; i < 5; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(20 * time.Millisecond)
			return 1, nil
		})
	}
	p.Close()
	p.Wait()
	err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
}

func TestPool_FailFast(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	p.WithFailFast(ctx)
	defer p.Close()
	for i := 0; i < 20; i++ {
		idx := i
		p.SubmitAt(idx, ctx, func(ctx context.Context) (int, error) {
			if idx == 2 {
				return 0, errTest
			}
			time.Sleep(50 * time.Millisecond)
			return idx, nil
		})
	}
	results := p.Wait()
	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}
	if results[2].Err == nil || !errors.Is(results[2].Err, errTest) {
		t.Fatalf("expected errTest at index 2, got: %v", results[2].Err)
	}
	skipped := 0
	for i := 3; i < 20; i++ {
		if errors.Is(results[i].Err, ErrSkipped) {
			skipped++
		}
	}
	t.Logf("skipped tasks in fail fast: %d", skipped)
}

func TestPool_Reset(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	defer p.CloseAndWait()
	for i := 0; i < 5; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	results := p.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	p.Reset()
	for i := 0; i < 3; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 2, nil
		})
	}
	results2 := p.Wait()
	if len(results2) != 3 {
		t.Fatalf("expected 3 results after reset, got %d", len(results2))
	}
	for _, r := range results2 {
		if r.Value != 2 {
			t.Fatalf("expected 2, got %d", r.Value)
		}
	}
}

func TestPool_TrySubmit(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](1)
	defer p.CloseAndWait()
	var started sync.WaitGroup
	started.Add(1)
	blockCh := make(chan struct{})
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		started.Done()
		<-blockCh
		return 1, nil
	})
	started.Wait()
	for i := 0; i < 2; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
		if err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}
	err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if !errors.Is(err, ErrSubmitTimeout) {
		t.Fatalf("expected ErrSubmitTimeout, got %v", err)
	}
	close(blockCh)
}

func TestPool_Resize_Expand(t *testing.T) {
	p := NewPool[int](2)
	defer p.CloseAndWait()
	initial := p.Size()
	if initial != 2 {
		t.Fatalf("expected initial size 2, got %d", initial)
	}
	added := p.Resize(5)
	if added != 3 {
		t.Fatalf("expected 3 added, got %d", added)
	}
	if p.Size() != 5 {
		t.Fatalf("expected size 5, got %d", p.Size())
	}
}

func TestPool_Resize_Shrink(t *testing.T) {
	p := NewPool[int](5)
	defer p.CloseAndWait()
	quit := p.Resize(2)
	if quit != 3 {
		t.Fatalf("expected 3 quit, got %d", quit)
	}
	if p.Size() != 2 {
		t.Fatalf("expected size 2, got %d", p.Size())
	}
}

func TestPool_ResizeAndWaitTimeout(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](5)
	defer p.Close()
	for i := 0; i < 5; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(50 * time.Millisecond)
			return 1, nil
		})
	}
	p.ResizeAndWaitTimeout(2, 500*time.Millisecond)
	if p.Size() != 2 {
		t.Fatalf("expected size 2 after resize, got %d", p.Size())
	}
	p.Wait()
}

func TestPool_NoResultPool(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	var counter int32
	for i := 0; i < 10; i++ {
		err := SubmitAction(pool, ctx, func(ctx context.Context) error {
			atomic.AddInt32(&counter, 1)
			return nil
		})
		if err != nil {
			t.Fatalf("SubmitAction error: %v", err)
		}
	}
	pool.Wait()
	if atomic.LoadInt32(&counter) != 10 {
		t.Fatalf("expected 10, got %d", counter)
	}
}

func TestPool_NoResultPool_FailFast(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	pool.WithFailFast(ctx)
	defer pool.Close()
	for i := 0; i < 20; i++ {
		idx := i
		SubmitAction(pool, ctx, func(ctx context.Context) error {
			if idx == 3 {
				panic("fail fast trigger")
			}
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}
	pool.Wait()
	if pool.FailCount() == 0 {
		t.Fatal("expected at least 1 failure")
	}
}

func TestPool_WaitTimeout(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](2)
	defer p.Close()
	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 1, nil
		})
	}
	results, ok := p.WaitTimeout(50 * time.Millisecond)
	if ok {
		t.Fatal("expected timeout, got ok=true")
	}
	if len(results) < 10 {
		t.Logf("timeout results: %d (expected < 10)", len(results))
	}
}

func TestPool_WaitContext(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](2)
	defer p.Close()
	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 1, nil
		})
	}
	wCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	results, ok := p.WaitContext(wCtx)
	if ok {
		t.Fatal("expected timeout via context")
	}
	if len(results) < 10 {
		t.Logf("context timeout results: %d (expected < 10)", len(results))
	}
}

func TestPool_Errors_FirstError_Values(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	defer p.Close()
	for i := 0; i < 5; i++ {
		idx := i
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			if idx%2 == 0 {
				return 0, fmt.Errorf("error %d", idx)
			}
			return idx, nil
		})
	}
	p.Wait()
	errs := p.Errors()
	if len(errs) != 3 {
		t.Fatalf("expected 3 errors, got %d", len(errs))
	}
	first := p.FirstError()
	if first == nil {
		t.Fatal("expected non-nil first error")
	}
	vals := p.Values()
	if len(vals) != 2 {
		t.Fatalf("expected 2 values, got %d", len(vals))
	}
}

func TestPool_JoinErrors(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	defer p.Close()
	for i := 0; i < 3; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 0, errTest
		})
	}
	p.Wait()
	joined := p.JoinErrors()
	if joined == nil {
		t.Fatal("expected non-nil joined error")
	}
}

func TestPool_Stats(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	defer p.Close()
	for i := 0; i < 10; i++ {
		idx := i
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			if idx < 3 {
				return 0, errTest
			}
			return idx, nil
		})
	}
	p.Wait()
	if p.TotalCount() != 10 {
		t.Fatalf("expected 10 total, got %d", p.TotalCount())
	}
	if p.FailCount() != 3 {
		t.Fatalf("expected 3 fail, got %d", p.FailCount())
	}
	if p.SuccessCount() != 7 {
		t.Fatalf("expected 7 success, got %d", p.SuccessCount())
	}
}

func TestPool_Active_Busy(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](3)
	defer p.Close()
	var started sync.WaitGroup
	started.Add(3)
	blockCh := make(chan struct{})
	for i := 0; i < 3; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			started.Done()
			<-blockCh
			return 1, nil
		})
	}
	started.Wait()
	time.Sleep(20 * time.Millisecond)
	if p.Active() != 3 {
		t.Fatalf("expected 3 active, got %d", p.Active())
	}
	if p.Busy() != 3 {
		t.Fatalf("expected 3 busy, got %d", p.Busy())
	}
	close(blockCh)
	p.Wait()
}

// ==================== Map / ForEach 测试 ====================

func TestMap(t *testing.T) {
	results := Map(context.Background(), []int{1, 2, 3, 4, 5}, 4, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	for i, r := range results {
		if r.Value != (i+1)*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, (i+1)*10, r.Value)
		}
	}
}

func TestMap_Serial(t *testing.T) {
	results := Map(context.Background(), []int{1, 2, 3}, 0, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestMapWithFailFast(t *testing.T) {
	results, err := MapWithFailFast(context.Background(), []int{1, 2, 3, 4, 5}, 4, func(ctx context.Context, v int) (int, error) {
		if v == 3 {
			return 0, errTest
		}
		time.Sleep(50 * time.Millisecond)
		return v, nil
	})
	if err == nil {
		t.Fatal("expected error from MapWithFailFast")
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	if results[2].Err == nil {
		t.Fatal("expected error at index 2")
	}
}

func TestForEach(t *testing.T) {
	var mu sync.Mutex
	collected := make([]int, 0)
	nr, err := ForEach(context.Background(), []int{1, 2, 3, 4, 5}, 4, func(ctx context.Context, v int) error {
		mu.Lock()
		collected = append(collected, v)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("ForEach error: %v", err)
	}
	if nr.HasError() {
		t.Fatal("ForEach should not have errors")
	}
	if len(collected) != 5 {
		t.Fatalf("expected 5 collected, got %d", len(collected))
	}
}

func TestForEach_Serial(t *testing.T) {
	collected := make([]int, 0)
	nr, err := ForEach(context.Background(), []int{1, 2, 3}, 0, func(ctx context.Context, v int) error {
		collected = append(collected, v)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEach error: %v", err)
	}
	_ = nr
	if len(collected) != 3 {
		t.Fatalf("expected 3 collected, got %d", len(collected))
	}
}

func TestForEachWithFailFast(t *testing.T) {
	var counter int32
	nr, err := ForEachWithFailFast(context.Background(), []int{1, 2, 3, 4, 5}, 4, func(ctx context.Context, v int) error {
		if v == 3 {
			panic("fail fast")
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err == nil {
		t.Fatal("expected error from ForEachWithFailFast")
	}
	_ = nr
	t.Logf("executed count: %d, fail count: %d", atomic.LoadInt32(&counter), nr.FailCount())
}

func TestMapChunk(t *testing.T) {
	slice := make([]int, 100)
	for i := range slice {
		slice[i] = i
	}
	results := MapChunk(context.Background(), slice, IO(), 10, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 10 {
		t.Fatalf("expected 10 chunk results, got %d", len(results))
	}
	total := 0
	for _, r := range results {
		total += r.Value
	}
	expected := 99 * 100 / 2
	if total != expected {
		t.Fatalf("expected total %d, got %d", expected, total)
	}
}

func TestForEachChunk(t *testing.T) {
	slice := make([]int, 100)
	for i := range slice {
		slice[i] = i
	}
	var total int64
	nr, err := ForEachChunk(context.Background(), slice, IO(), 10, func(ctx context.Context, chunk []int) error {
		for _, v := range chunk {
			atomic.AddInt64(&total, int64(v))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachChunk error: %v", err)
	}
	_ = nr
	expected := int64(99 * 100 / 2)
	if total != expected {
		t.Fatalf("expected total %d, got %d", expected, total)
	}
}

// ==================== Go / GoResult 测试 ====================

func TestGo(t *testing.T) {
	task := Go(context.Background(), func(ctx context.Context) {
		time.Sleep(10 * time.Millisecond)
	})
	err := task.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !task.Ok() {
		t.Fatal("expected task to be ok")
	}
}

func TestGo_Error(t *testing.T) {
	task := Go(context.Background(), func(ctx context.Context) {
		panic("test panic")
	})
	err := task.Wait()
	if err == nil {
		t.Fatal("expected panic error")
	}
	if task.Ok() {
		t.Fatal("expected task to not be ok")
	}
	if !task.IsPanic() {
		t.Fatal("expected task to be panic")
	}
}

func TestGoResult(t *testing.T) {
	result := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		return 100, nil
	})
	val, err := result.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 100 {
		t.Fatalf("expected 100, got %d", val)
	}
}

func TestGoResult_Error(t *testing.T) {
	result := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTest
	})
	_, err := result.Wait()
	if !errors.Is(err, errTest) {
		t.Fatalf("expected errTest, got %v", err)
	}
}

func TestGoResult_Panic(t *testing.T) {
	result := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		panic("test panic")
	})
	_, err := result.Wait()
	if err == nil {
		t.Fatal("expected panic error")
	}
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PanicError, got %T: %v", err, err)
	}
}

func TestGoResult_WaitTimeout(t *testing.T) {
	result := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		time.Sleep(200 * time.Millisecond)
		return 1, nil
	})
	_, err, ok := result.WaitTimeout(10 * time.Millisecond)
	if ok {
		t.Fatal("expected timeout, got ok=true")
	}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestGoResult_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	result := GoResult(ctx, func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	cancel()
	_, err := result.Wait()
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

// ==================== Retry 测试 ====================

func TestRetry_Success(t *testing.T) {
	attempts := 0
	err := Retry(context.Background(), 3, func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return errTest
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetry_Exhausted(t *testing.T) {
	attempts := 0
	err := Retry(context.Background(), 3, func(ctx context.Context) error {
		attempts++
		return errTest
	})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if attempts != 4 {
		t.Fatalf("expected 4 attempts, got %d", attempts)
	}
}

func TestRetry_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := Retry(ctx, 10, func(ctx context.Context) error {
		attempts++
		return errTest
	})
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
	t.Logf("attempts before cancel: %d", attempts)
}

func TestRetryWithBackoff(t *testing.T) {
	attempts := 0
	start := time.Now()
	err := RetryWithBackoff(context.Background(), 4, 10*time.Millisecond, func(ctx context.Context) error {
		attempts++
		return errTest
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 5 {
		t.Fatalf("expected 5 attempts, got %d", attempts)
	}
	if elapsed < 70*time.Millisecond {
		t.Fatalf("expected at least 70ms with backoff, got %v", elapsed)
	}
}

func TestRetryWithBackoff_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := RetryWithBackoff(ctx, 10, 100*time.Millisecond, func(ctx context.Context) error {
		return errTest
	})
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

// ==================== RateLimiter 测试 ====================

func TestRateLimiter_Wait(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	defer rl.Stop()
	start := time.Now()
	for i := 0; i < 10; i++ {
		err := rl.Wait(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	elapsed := time.Since(start)
	if elapsed < 800*time.Millisecond {
		t.Fatalf("expected at least 800ms, got %v", elapsed)
	}
}

func TestRateLimiter_Burst(t *testing.T) {
	rl := NewRateLimiterWithBurst(10, time.Second, 5)
	defer rl.Stop()
	start := time.Now()
	for i := 0; i < 5; i++ {
		err := rl.Wait(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	burstElapsed := time.Since(start)
	if burstElapsed > 50*time.Millisecond {
		t.Fatalf("burst should be near-instant, got %v", burstElapsed)
	}
	for i := 0; i < 3; i++ {
		rl.Wait(context.Background())
	}
	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond {
		t.Fatalf("expected at least 200ms total, got %v", elapsed)
	}
}

func TestRateLimiter_ContextCancel(t *testing.T) {
	rl := NewRateLimiter(1, time.Second)
	defer rl.Stop()
	_ = rl.Wait(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := rl.Wait(ctx)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

// ==================== Pipeline 测试 ====================

func TestPipeline_Empty(t *testing.T) {
	p := NewPipeline[int](context.Background())
	result, err := p.Run(42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
}

func TestPipeline_SingleStage(t *testing.T) {
	p := NewPipeline[int](context.Background(),
		func(ctx context.Context, input int) (int, error) {
			return input * 2, nil
		},
	)
	result, err := p.Run(21)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
}

func TestPipeline_MultipleStages(t *testing.T) {
	p := NewPipeline[int](context.Background(),
		func(ctx context.Context, input int) (int, error) {
			return input + 1, nil
		},
		func(ctx context.Context, input int) (int, error) {
			return input * 2, nil
		},
		func(ctx context.Context, input int) (int, error) {
			return input - 3, nil
		},
	)
	result, err := p.Run(10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 19 {
		t.Fatalf("expected 19, got %d", result)
	}
}

func TestPipeline_Error(t *testing.T) {
	p := NewPipeline[int](context.Background(),
		func(ctx context.Context, input int) (int, error) {
			return input + 1, nil
		},
		func(ctx context.Context, input int) (int, error) {
			return 0, errTest
		},
		func(ctx context.Context, input int) (int, error) {
			return input * 2, nil
		},
	)
	result, err := p.Run(10)
	if !errors.Is(err, errTest) {
		t.Fatalf("expected errTest, got %v", err)
	}
	if result != 11 {
		t.Fatalf("expected 11 (input to failed stage), got %d", result)
	}
}

func TestPipeline_WithTraceID(t *testing.T) {
	ctx := context.Background()
	p := NewPipeline[int](ctx,
		func(ctx context.Context, input int) (int, error) {
			return input, nil
		},
	)
	p.WithTraceID(ctx)
	result, err := p.Run(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 1 {
		t.Fatalf("expected 1, got %d", result)
	}
}

func TestPipeline_Stages(t *testing.T) {
	p := NewPipeline[int](context.Background(),
		func(ctx context.Context, input int) (int, error) { return input, nil },
		func(ctx context.Context, input int) (int, error) { return input, nil },
	)
	if p.Stages() != 2 {
		t.Fatalf("expected 2 stages, got %d", p.Stages())
	}
}

// ==================== 并发安全 / 竞态测试 ====================

func TestConcurrent_Group_MultipleGoers(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](8)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				g.Go(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				})
			}
		}()
	}
	wg.Wait()
	results := g.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

func TestConcurrent_Pool_MultipleSubmitters(t *testing.T) {
	ctx := context.Background()
	p := NewPool[string](16)
	defer p.Close()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				p.Submit(ctx, func(ctx context.Context) (string, error) {
					return "ok", nil
				})
			}
		}()
	}
	wg.Wait()
	results := p.Wait()
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

func TestConcurrent_Pool_ResizeDuringSubmit(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	defer p.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			p.Submit(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Millisecond)
				return 1, nil
			})
		}
	}()
	time.Sleep(10 * time.Millisecond)
	p.Resize(8)
	time.Sleep(10 * time.Millisecond)
	p.Resize(2)
	wg.Wait()
	p.Wait()
}

func TestConcurrent_Group_ResetAndReuse(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)
	for round := 0; round < 5; round++ {
		for i := 0; i < 20; i++ {
			g.Go(ctx, func(ctx context.Context) (int, error) {
				return round, nil
			})
		}
		results := g.Wait()
		if len(results) != 20 {
			t.Fatalf("round %d: expected 20 results, got %d", round, len(results))
		}
		for _, r := range results {
			if r.Value != round {
				t.Fatalf("round %d: expected %d, got %d", round, round, r.Value)
			}
		}
		g.Reset()
	}
}

func TestConcurrent_Pool_ResetAndReuse(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	defer p.CloseAndWait()
	for round := 0; round < 5; round++ {
		for i := 0; i < 20; i++ {
			p.Submit(ctx, func(ctx context.Context) (int, error) {
				return round, nil
			})
		}
		results := p.Wait()
		if len(results) != 20 {
			t.Fatalf("round %d: expected 20 results, got %d", round, len(results))
		}
		for _, r := range results {
			if r.Value != round {
				t.Fatalf("round %d: expected %d, got %d", round, round, r.Value)
			}
		}
		p.Reset()
	}
}

// ==================== 边界条件测试 ====================

func TestEdge_EmptySlice_Map(t *testing.T) {
	results := Map(context.Background(), []int{}, 4, func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestEdge_EmptySlice_ForEach(t *testing.T) {
	nr, err := ForEach(context.Background(), []int{}, 4, func(ctx context.Context, v int) error {
		panic("should not be called")
	})
	if err != nil {
		t.Fatalf("ForEach error: %v", err)
	}
	_ = nr
}

func TestEdge_ZeroConcurrency(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](0)
	for i := 0; i < 5; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	results := g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

func TestEdge_SingleItem(t *testing.T) {
	results := Map(context.Background(), []int{42}, 4, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Value != 84 {
		t.Fatalf("expected 84, got %d", results[0].Value)
	}
}

func TestEdge_Chunk_SingleElement(t *testing.T) {
	results := MapChunk(context.Background(), []int{5}, 1, 3, func(ctx context.Context, chunk []int) (int, error) {
		return chunk[0], nil
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Value != 5 {
		t.Fatalf("expected 5, got %d", results[0].Value)
	}
}

func TestEdge_Chunk_LessThanBatchSize(t *testing.T) {
	results := MapChunk(context.Background(), []int{1, 2}, 1, 5, func(ctx context.Context, chunk []int) (int, error) {
		return len(chunk), nil
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(results))
	}
	if results[0].Value != 2 {
		t.Fatalf("expected chunk size 2, got %d", results[0].Value)
	}
}

func TestEdge_Retry_MaxRetriesZero(t *testing.T) {
	err := Retry(context.Background(), 0, func(ctx context.Context) error {
		return errTest
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestEdge_Group_GoAfterWait(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)
	g.Go(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	g.Wait()
	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		return 2, nil
	})
	if !errors.Is(err, ErrGroupWaited) {
		t.Fatalf("expected ErrGroupWaited, got %v", err)
	}
}

func TestEdge_Pool_SubmitAfterWait(t *testing.T) {
	ctx := context.Background()
	p := NewPool[int](4)
	defer p.Close()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	p.Wait()
	err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 2, nil
	})
	if !errors.Is(err, ErrPoolWaited) {
		t.Fatalf("expected ErrPoolWaited, got %v", err)
	}
}

// ==================== 基准测试 ====================

func BenchmarkGroup_Go(b *testing.B) {
	ctx := context.Background()
	g := NewGroup[int](IO())
	for i := 0; i < b.N; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	g.Wait()
}

func BenchmarkPool_Submit(b *testing.B) {
	ctx := context.Background()
	p := NewPool[int](IO())
	defer p.Close()
	for i := 0; i < b.N; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	p.Wait()
}

func BenchmarkMap(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Map(context.Background(), slice, IO(), func(ctx context.Context, v int) (int, error) {
			return v * 2, nil
		})
	}
}

func BenchmarkMap_Serial(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Map(context.Background(), slice, 0, func(ctx context.Context, v int) (int, error) {
			return v * 2, nil
		})
	}
}

func BenchmarkGo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		task := Go(context.Background(), func(ctx context.Context) {
			time.Sleep(time.Microsecond)
		})
		task.Wait()
	}
}

func BenchmarkRetry(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Retry(context.Background(), 3, func(ctx context.Context) error {
			return nil
		})
	}
}

// ==================== 并发压力测试 ====================

// TestStress_Group_ConcurrentGoWait 大量 goroutine 并发 Go + Wait 边界测试
// 确保 Go 和 Wait 之间的 waiting/waited 保护正确，不会 panic
func TestStress_Group_ConcurrentGoWait(t *testing.T) {
	for round := 0; round < 100; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		var wg sync.WaitGroup
		// 多个 goroutine 并发 Go
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func(n int) {
				defer wg.Done()
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					time.Sleep(time.Microsecond)
					return n, nil
				})
			}(i)
		}
		wg.Wait()
		results := g.Wait()
		if len(results) != 10 {
			t.Fatalf("round %d: expected 10 results, got %d", round, len(results))
		}
	}
}

// TestStress_Group_WaitTimeout_ConcurrentAccess 并发访问 WaitTimeout 和结果读取
func TestStress_Group_WaitTimeout_ConcurrentAccess(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		for i := 0; i < 10; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				return 1, nil
			})
		}
		// 并发调用 WaitTimeout 和统计方法
		var wg sync.WaitGroup
		wg.Add(2)
		var results1, results2 []Result[int]
		var ok1, ok2 bool
		go func() {
			defer wg.Done()
			results1, ok1 = g.WaitTimeout(5 * time.Second)
		}()
		go func() {
			defer wg.Done()
			results2, ok2 = g.WaitTimeout(5 * time.Second)
		}()
		wg.Wait()
		if !ok1 || !ok2 {
			t.Fatalf("round %d: WaitTimeout should succeed", round)
		}
		if len(results1) != 10 || len(results2) != 10 {
			t.Fatalf("round %d: expected 10 results, got %d/%d", round, len(results1), len(results2))
		}
	}
}

// TestStress_Pool_ConcurrentSubmitWait 并发 Submit + Wait
func TestStress_Pool_ConcurrentSubmitWait(t *testing.T) {
	for round := 0; round < 100; round++ {
		p := NewPool[int](4)
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func(n int) {
				defer wg.Done()
				_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
					time.Sleep(time.Microsecond)
					return n, nil
				})
			}(i)
		}
		wg.Wait()
		results := p.Wait()
		p.Close()
		if len(results) != 10 {
			t.Fatalf("round %d: expected 10 results, got %d", round, len(results))
		}
	}
}

// TestStress_Pool_CloseAndWaitTimeout_Deadlock 检测 CloseAndWaitTimeout 的死锁
func TestStress_Pool_CloseAndWaitTimeout_Deadlock(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](8)
		ctx := context.Background()
		for i := 0; i < 20; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Millisecond)
				return 1, nil
			})
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			ok, workerDone := p.CloseAndWaitTimeout(3 * time.Second)
			if !ok && workerDone != nil {
				select {
				case <-workerDone:
				case <-time.After(5 * time.Second):
					t.Errorf("round %d: workerDone channel never closed", round)
				}
			}
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: CloseAndWaitTimeout deadlock detected", round)
		}
	}
}

// TestStress_Group_FailFast_Race 快速失败 + 并发提交的竞态
func TestStress_Group_FailFast_Race(t *testing.T) {
	for round := 0; round < 100; round++ {
		g, ctx := NewGroup[int](4).WithFailFast(context.Background())
		g.WithSubmitTimeout(500 * time.Millisecond)
		var wg sync.WaitGroup
		// 第一个任务会失败，触发 failFast
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return 0, errTest
		})
		// 并发提交更多任务，它们应该被取消
		wg.Add(9)
		for i := 0; i < 9; i++ {
			go func() {
				defer wg.Done()
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(100 * time.Millisecond):
						return 1, nil
					}
				})
			}()
		}
		wg.Wait()
		results := g.Wait()
		if len(results) != 10 {
			t.Fatalf("round %d: expected 10 results, got %d", round, len(results))
		}
	}
}

// TestStress_Pool_FailFast_Race 快速失败 + 池化并发提交
func TestStress_Pool_FailFast_Race(t *testing.T) {
	for round := 0; round < 50; round++ {
		p, ctx := NewPool[int](4).WithFailFast(context.Background())
		p.WithSubmitTimeout(500 * time.Millisecond)
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 0, errTest
		})
		var wg sync.WaitGroup
		wg.Add(9)
		for i := 0; i < 9; i++ {
			go func() {
				defer wg.Done()
				_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(100 * time.Millisecond):
						return 1, nil
					}
				})
			}()
		}
		wg.Wait()
		results := p.Wait()
		p.Close()
		if len(results) != 10 {
			t.Fatalf("round %d: expected 10 results, got %d", round, len(results))
		}
	}
}

// TestStress_Group_ResetAndReuse_Race 重置后复用的并发安全
func TestStress_Group_ResetAndReuse_Race(t *testing.T) {
	for round := 0; round < 100; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		for i := 0; i < 5; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				return 1, nil
			})
		}
		g.Wait()
		_, err := g.Reset()
		if err != nil {
			t.Fatalf("round %d: Reset failed: %v", round, err)
		}
		// 重置后立即并发提交
		var wg sync.WaitGroup
		wg.Add(5)
		for i := 0; i < 5; i++ {
			go func(n int) {
				defer wg.Done()
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return n, nil
				})
			}(i)
		}
		wg.Wait()
		results := g.Wait()
		if len(results) != 5 {
			t.Fatalf("round %d: expected 5 results after reset, got %d", round, len(results))
		}
	}
}

// TestStress_Pool_ResetAndReuse_Race 池化重置后复用的并发安全
func TestStress_Pool_ResetAndReuse_Race(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](4)
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				return 1, nil
			})
		}
		p.Wait()
		_, err := p.Reset()
		if err != nil {
			p.Close()
			t.Fatalf("round %d: Reset failed: %v", round, err)
		}
		var wg sync.WaitGroup
		wg.Add(5)
		for i := 0; i < 5; i++ {
			go func(n int) {
				defer wg.Done()
				_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
					return n, nil
				})
			}(i)
		}
		wg.Wait()
		results := p.Wait()
		p.Close()
		if len(results) != 5 {
			t.Fatalf("round %d: expected 5 results after reset, got %d", round, len(results))
		}
	}
}

// TestStress_Pool_Resize_Concurrent 并发提交时的缩扩容
func TestStress_Pool_Resize_Concurrent(t *testing.T) {
	for round := 0; round < 30; round++ {
		p := NewPool[int](10)
		ctx := context.Background()
		// 提交大量任务
		for i := 0; i < 50; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Microsecond * 10)
				return 1, nil
			})
		}
		// 在任务执行期间缩容
		done := make(chan struct{})
		go func() {
			defer close(done)
			p.Resize(3)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: Resize deadlock", round)
		}
		p.Wait()
		p.Close()
	}
}

// TestStress_Map_Concurrent 并发 Map 的数据一致性
func TestStress_Map_Concurrent(t *testing.T) {
	for round := 0; round < 50; round++ {
		slice := make([]int, 100)
		for i := range slice {
			slice[i] = i
		}
		results := Map(context.Background(), slice, 8, func(ctx context.Context, v int) (int, error) {
			return v * 2, nil
		})
		if len(results) != 100 {
			t.Fatalf("round %d: expected 100 results, got %d", round, len(results))
		}
		for i, r := range results {
			if r.Err != nil {
				t.Fatalf("round %d: result[%d] error: %v", round, i, r.Err)
			}
			if r.Value != i*2 {
				t.Fatalf("round %d: result[%d] expected %d, got %v", round, i, i*2, r.Value)
			}
		}
	}
}

// TestStress_WaitContext_Deadlock WaitContext 死锁检测
func TestStress_WaitContext_Deadlock(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		for i := 0; i < 10; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				return 1, nil
			})
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			results, ok := g.WaitContext(context.Background())
			if !ok {
				t.Errorf("round %d: WaitContext should succeed", round)
			}
			if len(results) != 10 {
				t.Errorf("round %d: expected 10 results, got %d", round, len(results))
			}
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: WaitContext deadlock detected", round)
		}
	}
}

// TestStress_WaitTimeout_CleanupGoroutine WaitTimeout 清理 goroutine 的正确性
func TestStress_WaitTimeout_CleanupGoroutine(t *testing.T) {
	// 设置较短的清理超时，验证清理 goroutine 能正常退出
	SetMaxCleanupDuration(200 * time.Millisecond)
	defer SetMaxCleanupDuration(30 * time.Minute)

	for round := 0; round < 20; round++ {
		ctx := context.Background()
		g := NewGroup[int](2)
		// 提交一个不响应 ctx.Done 的长任务
		blocked := make(chan struct{})
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			<-blocked // 永久阻塞，不响应 ctx
			return 1, nil
		})
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return 2, nil
		})

		done := make(chan struct{})
		go func() {
			defer close(done)
			results, ok := g.WaitTimeout(50 * time.Millisecond)
			if ok {
				t.Errorf("round %d: WaitTimeout should timeout", round)
			}
			// 即使超时，已完成的那个任务的结果也应该能拿到
			_ = results
		}()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			// 清理 goroutine 应该能在 200ms 内退出
			// 但 WaitTimeout 本身 50ms 就超时了
			close(blocked) // 释放阻塞任务
			t.Fatalf("round %d: WaitTimeout didn't return in time", round)
		}
		close(blocked) // 清理
	}
}

// TestStress_Group_GoAfterWait_Race Wait 之后 Go 的竞态保护
func TestStress_Group_GoAfterWait_Race(t *testing.T) {
	for round := 0; round < 200; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
		results := g.Wait()
		if len(results) != 1 {
			t.Fatalf("round %d: expected 1 result, got %d", round, len(results))
		}
		// 在 Wait 之后尝试 Go，应该返回 ErrGroupWaited
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			return 2, nil
		})
		if !errors.Is(err, ErrGroupWaited) {
			t.Fatalf("round %d: expected ErrGroupWaited, got %v", round, err)
		}
	}
}

// TestStress_Pool_SubmitAfterWait_Race Wait 之后 Submit 的竞态保护
func TestStress_Pool_SubmitAfterWait_Race(t *testing.T) {
	for round := 0; round < 100; round++ {
		p := NewPool[int](4)
		ctx := context.Background()
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
		p.Wait()
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 2, nil
		})
		p.Close()
		if !errors.Is(err, ErrPoolWaited) {
			t.Fatalf("round %d: expected ErrPoolWaited, got %v", round, err)
		}
	}
}

// TestStress_Group_GoAt_ConcurrentIndex 并发 GoAt 索引保护
func TestStress_Group_GoAt_ConcurrentIndex(t *testing.T) {
	for round := 0; round < 100; round++ {
		ctx := context.Background()
		g := NewGroup[int](8)
		var wg sync.WaitGroup
		n := 50
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(idx int) {
				defer wg.Done()
				_ = g.GoAt(idx, ctx, func(ctx context.Context) (int, error) {
					return idx * 10, nil
				})
			}(i)
		}
		wg.Wait()
		results := g.Wait()
		if len(results) != n {
			t.Fatalf("round %d: expected %d results, got %d", round, n, len(results))
		}
		for i, r := range results {
			if r.Err != nil {
				t.Fatalf("round %d: result[%d] error: %v", round, i, r.Err)
			}
			if r.Value != i*10 {
				t.Fatalf("round %d: result[%d] expected %d, got %v", round, i, i*10, r.Value)
			}
		}
	}
}

// TestStress_Pool_SubmitAt_ConcurrentIndex 并发 SubmitAt 索引保护
func TestStress_Pool_SubmitAt_ConcurrentIndex(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](8)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 50
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(idx int) {
				defer wg.Done()
				_ = p.SubmitAt(idx, ctx, func(ctx context.Context) (int, error) {
					return idx * 10, nil
				})
			}(i)
		}
		wg.Wait()
		results := p.Wait()
		p.Close()
		if len(results) != n {
			t.Fatalf("round %d: expected %d results, got %d", round, n, len(results))
		}
		for i, r := range results {
			if r.Err != nil {
				t.Fatalf("round %d: result[%d] error: %v", round, i, r.Err)
			}
			if r.Value != i*10 {
				t.Fatalf("round %d: result[%d] expected %d, got %v", round, i, i*10, r.Value)
			}
		}
	}
}

// TestStress_Group_Errors_ConcurrentAccess 并发访问 Errors/FirstError/JoinErrors
func TestStress_Group_Errors_ConcurrentAccess(t *testing.T) {
	for round := 0; round < 100; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		for i := 0; i < 10; i++ {
			idx := i
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				if idx%2 == 0 {
					return 0, fmt.Errorf("error %d", idx)
				}
				return idx, nil
			})
		}
		g.Wait()
		// 并发读取统计信息
		var wg sync.WaitGroup
		wg.Add(4)
		var errs []error
		var firstErr error
		var joinErr error
		var failCnt int64
		go func() {
			defer wg.Done()
			errs = g.Errors()
		}()
		go func() {
			defer wg.Done()
			firstErr = g.FirstError()
		}()
		go func() {
			defer wg.Done()
			joinErr = g.JoinErrors()
		}()
		go func() {
			defer wg.Done()
			failCnt = g.FailCount()
		}()
		wg.Wait()
		if len(errs) != 5 {
			t.Fatalf("round %d: expected 5 errors, got %d", round, len(errs))
		}
		if firstErr == nil {
			t.Fatalf("round %d: expected non-nil first error", round)
		}
		if joinErr == nil {
			t.Fatalf("round %d: expected non-nil join error", round)
		}
		if failCnt != 5 {
			t.Fatalf("round %d: expected 5 fail count, got %d", round, failCnt)
		}
	}
}

// TestStress_Pool_Close_ConcurrentSubmit 并发 Close + Submit
func TestStress_Pool_Close_ConcurrentSubmit(t *testing.T) {
	for round := 0; round < 100; round++ {
		p := NewPool[int](4)
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Microsecond)
				return 1, nil
			})
		}
		// 并发等待和关闭
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			p.Wait()
		}()
		go func() {
			defer wg.Done()
			p.Close()
		}()
		wg.Wait()
		// 关闭后 Submit 应该返回 ErrPoolClosed
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 2, nil
		})
		if err != nil {
			if !errors.Is(err, ErrPoolClosed) && !errors.Is(err, ErrPoolWaiting) {
				t.Fatalf("round %d: expected ErrPoolClosed or ErrPoolWaiting, got %v", round, err)
			}
		}
	}
}

// TestStress_Group_GoWithTimeout_Concurrent 并发 GoWithTimeout
func TestStress_Group_GoWithTimeout_Concurrent(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		var wg sync.WaitGroup
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func(n int) {
				defer wg.Done()
				_ = g.GoWithTimeout(ctx, 500*time.Millisecond, func(ctx context.Context) (int, error) {
					if n%2 == 0 {
						time.Sleep(10 * time.Millisecond)
						return n, nil
					}
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(1 * time.Second):
						return n, nil
					}
				})
			}(i)
		}
		wg.Wait()
		results := g.Wait()
		if len(results) != 10 {
			t.Fatalf("round %d: expected 10 results, got %d", round, len(results))
		}
	}
}

// TestStress_MixedGoAndGoAt 混用 Go 和 GoAt 的并发安全
func TestStress_MixedGoAndGoAt(t *testing.T) {
	for round := 0; round < 100; round++ {
		ctx := context.Background()
		g := NewGroup[int](8)
		var wg sync.WaitGroup
		// 先 GoAt 预分配
		_ = g.GoAt(0, ctx, func(ctx context.Context) (int, error) {
			return 0, nil
		})
		// 然后并发混用 Go 和 GoAt
		wg.Add(9)
		for i := 1; i < 5; i++ {
			go func(idx int) {
				defer wg.Done()
				_ = g.GoAt(idx, ctx, func(ctx context.Context) (int, error) {
					return idx * 10, nil
				})
			}(i)
		}
		for i := 0; i < 5; i++ {
			go func() {
				defer wg.Done()
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return 999, nil
				})
			}()
		}
		wg.Wait()
		results := g.Wait()
		if len(results) != 10 {
			t.Fatalf("round %d: expected 10 results, got %d", round, len(results))
		}
		// 检查 GoAt 的索引位置结果正确
		for i := 0; i < 5; i++ {
			if results[i].Err != nil {
				t.Fatalf("round %d: result[%d] error: %v", round, i, results[i].Err)
			}
			if results[i].Value != i*10 {
				t.Fatalf("round %d: result[%d] expected %d, got %v", round, i, i*10, results[i].Value)
			}
		}
	}
}

// TestStress_Pool_TrySubmit_Concurrent 并发 TrySubmit
func TestStress_Pool_TrySubmit_Concurrent(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](2) // 只有 2 个 worker，taskCh 容量 4
		ctx := context.Background()
		var wg sync.WaitGroup
		accepted := atomic.Int32{}
		rejected := atomic.Int32{}
		wg.Add(20)
		for i := 0; i < 20; i++ {
			go func() {
				defer wg.Done()
				err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
					time.Sleep(10 * time.Millisecond)
					return 1, nil
				})
				if err == nil {
					accepted.Add(1)
				} else if errors.Is(err, ErrSubmitTimeout) {
					rejected.Add(1)
				}
			}()
		}
		wg.Wait()
		results := p.Wait()
		p.Close()
		total := int(accepted.Load() + rejected.Load())
		if total != 20 {
			t.Fatalf("round %d: expected 20 total, got %d (accepted=%d, rejected=%d, results=%d)",
				round, total, accepted.Load(), rejected.Load(), len(results))
		}
	}
}

// TestStress_RateLimiter_Concurrent 并发 RateLimiter
func TestStress_RateLimiter_Concurrent(t *testing.T) {
	rl := NewRateLimiter(1000, time.Second)
	defer rl.Stop()
	ctx := context.Background()
	var wg sync.WaitGroup
	var success atomic.Int64
	var fail atomic.Int64
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if err := rl.Wait(ctx); err != nil {
					fail.Add(1)
					return
				}
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() != 1000 {
		t.Fatalf("expected 1000 successful, got %d", success.Load())
	}
}

// TestStress_AsyncResult_ConcurrentWait 并发等待 AsyncResult
func TestStress_AsyncResult_ConcurrentWait(t *testing.T) {
	for round := 0; round < 100; round++ {
		ar := GoResult(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return 42, nil
		})
		var wg sync.WaitGroup
		wg.Add(5)
		for i := 0; i < 5; i++ {
			go func() {
				defer wg.Done()
				val, err := ar.Wait()
				if err != nil || val != 42 {
					t.Errorf("round %d: unexpected result: val=%d, err=%v", round, val, err)
				}
			}()
		}
		wg.Wait()
	}
}

// TestStress_Pool_PendingReset_Race 并发 Pending 检查和 Reset
func TestStress_Pool_PendingReset_Race(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](4)
		ctx := context.Background()
		// 提交一些任务但不等待
		for i := 0; i < 10; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Microsecond)
				return 1, nil
			})
		}
		// 并发读取 Pending 和 Busy
		var wg sync.WaitGroup
		wg.Add(2)
		var pending, busy int
		go func() {
			defer wg.Done()
			pending = p.Pending()
		}()
		go func() {
			defer wg.Done()
			busy = p.Busy()
		}()
		wg.Wait()
		_ = pending
		_ = busy
		p.Wait()
		p.Close()
	}
}

// TestStress_Group_Active_ConcurrentRead 并发读取 Active
func TestStress_Group_Active_ConcurrentRead(t *testing.T) {
	for round := 0; round < 100; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		for i := 0; i < 20; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Microsecond * 100)
				return 1, nil
			})
		}
		// 并发读取 Active 和 Busy
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				a := g.Active()
				b := g.Busy()
				_ = a
				_ = b
				if a == 0 {
					return
				}
				time.Sleep(time.Microsecond)
			}
		}()
		g.Wait()
		<-done
	}
}

// TestStress_MapWithFailFast_ConcurrentError 并发 MapWithFailFast 错误传播
func TestStress_MapWithFailFast_ConcurrentError(t *testing.T) {
	for round := 0; round < 50; round++ {
		slice := make([]int, 50)
		for i := range slice {
			slice[i] = i
		}
		results, firstErr := MapWithFailFast(context.Background(), slice, 8, func(ctx context.Context, v int) (int, error) {
			if v == 5 {
				return 0, errTest
			}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(10 * time.Millisecond):
				return v * 2, nil
			}
		})
		if firstErr == nil {
			t.Fatalf("round %d: expected first error", round)
		}
		if len(results) != 50 {
			t.Fatalf("round %d: expected 50 results, got %d", round, len(results))
		}
	}
}

// TestStress_ForEachWithFailFast_ConcurrentError 并发 ForEachWithFailFast
func TestStress_ForEachWithFailFast_ConcurrentError(t *testing.T) {
	for round := 0; round < 50; round++ {
		slice := make([]int, 50)
		for i := range slice {
			slice[i] = i
		}
		nr, err := ForEachWithFailFast(context.Background(), slice, 8, func(ctx context.Context, v int) error {
			if v == 10 {
				return errTest
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Microsecond * 100):
				return nil
			}
		})
		if err == nil {
			t.Fatalf("round %d: expected first error", round)
		}
		_ = nr.FailCount()
		_ = nr.SuccessCount()
		_ = nr.TotalCount()
	}
}

// TestStress_RetryWithBackoff_Concurrent 并发 RetryWithBackoff
func TestStress_RetryWithBackoff_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	var success atomic.Int64
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			defer wg.Done()
			err := RetryWithBackoff(context.Background(), 3, 10*time.Millisecond, func(ctx context.Context) error {
				return nil
			})
			if err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() != 20 {
		t.Fatalf("expected 20 successful, got %d", success.Load())
	}
}

// TestStress_Pool_ConcurrentCloseAndResize 并发 Close 和 Resize
func TestStress_Pool_ConcurrentCloseAndResize(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](10)
		ctx := context.Background()
		for i := 0; i < 10; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Microsecond)
				return 1, nil
			})
		}
		p.Wait()
		// 并发 Close 和 Resize
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			p.Close()
		}()
		go func() {
			defer wg.Done()
			p.Resize(5)
		}()
		wg.Wait()
	}
}

// TestStress_Group_ConcurrentSubmitAndCancel 并发提交和取消 context
func TestStress_Group_ConcurrentSubmitAndCancel(t *testing.T) {
	for round := 0; round < 50; round++ {
		parentCtx, parentCancel := context.WithCancel(context.Background())
		g, ctx := NewGroup[int](4).WithContext(parentCtx)
		var wg sync.WaitGroup
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func() {
				defer wg.Done()
				err := g.Go(ctx, func(ctx context.Context) (int, error) {
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(100 * time.Millisecond):
						return 1, nil
					}
				})
				if err != nil {
					// 并发取消时 Go 可能被拒绝
					_ = err
				}
			}()
		}
		// 在提交过程中取消 context
		time.Sleep(5 * time.Millisecond)
		parentCancel()
		wg.Wait()
		results := g.Wait()
		// 所有任务要么成功要么被取消
		for _, r := range results {
			if r.Err != nil && !errors.Is(r.Err, context.Canceled) && !errors.Is(r.Err, ErrGroupWaiting) && !errors.Is(r.Err, ErrGroupWaited) {
				t.Fatalf("round %d: unexpected error: %v", round, r.Err)
			}
		}
	}
}

// TestStress_Pool_ConcurrentSubmitAndCancel 并发提交和取消 pool context
func TestStress_Pool_ConcurrentSubmitAndCancel(t *testing.T) {
	for round := 0; round < 50; round++ {
		parentCtx, parentCancel := context.WithCancel(context.Background())
		p, ctx := NewPool[int](4).WithContext(parentCtx)
		var wg sync.WaitGroup
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func() {
				defer wg.Done()
				err := p.Submit(ctx, func(ctx context.Context) (int, error) {
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(100 * time.Millisecond):
						return 1, nil
					}
				})
				if err != nil {
					_ = err
				}
			}()
		}
		time.Sleep(5 * time.Millisecond)
		parentCancel()
		wg.Wait()
		results := p.Wait()
		p.Close()
		for _, r := range results {
			if r.Err != nil && !errors.Is(r.Err, context.Canceled) && !errors.Is(r.Err, ErrPoolWaiting) && !errors.Is(r.Err, ErrPoolClosed) {
				t.Fatalf("round %d: unexpected error: %v", round, r.Err)
			}
		}
	}
}

// TestStress_NoResult_Concurrent 并发 NoResult
func TestStress_NoResult_Concurrent(t *testing.T) {
	for round := 0; round < 100; round++ {
		nr, ctx := NewNoResult(4).WithTraceID(context.Background())
		var wg sync.WaitGroup
		wg.Add(20)
		for i := 0; i < 20; i++ {
			go func(n int) {
				defer wg.Done()
				_ = nr.Go(ctx, func(ctx context.Context) error {
					if n%3 == 0 {
						return errTest
					}
					return nil
				})
			}(i)
		}
		wg.Wait()
		nr.Wait()
		total := nr.TotalCount()
		if total != 20 {
			t.Fatalf("round %d: expected 20 total, got %d", round, total)
		}
	}
}

// TestStress_NoResult_GoAfterWait 并发 NoResult Wait 后 Go
func TestStress_NoResult_GoAfterWait(t *testing.T) {
	for round := 0; round < 100; round++ {
		nr, ctx := NewNoResult(4).WithTraceID(context.Background())
		_ = nr.Go(ctx, func(ctx context.Context) error {
			return nil
		})
		nr.Wait()
		err := nr.Go(ctx, func(ctx context.Context) error {
			return nil
		})
		if !errors.Is(err, ErrGroupWaited) {
			t.Fatalf("round %d: expected ErrGroupWaited, got %v", round, err)
		}
	}
}

// TestStress_Group_PanicRecovery 并发 panic 恢复
func TestStress_Group_PanicRecovery(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		var wg sync.WaitGroup
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func(n int) {
				defer wg.Done()
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					if n == 5 {
						panic("concurrent panic test")
					}
					return n, nil
				})
			}(i)
		}
		wg.Wait()
		results := g.Wait()
		if len(results) != 10 {
			t.Fatalf("round %d: expected 10 results, got %d", round, len(results))
		}
		hasPanic := false
		for _, r := range results {
			if r.IsPanic() {
				hasPanic = true
				break
			}
		}
		if !hasPanic {
			t.Fatalf("round %d: expected at least one panic result", round)
		}
	}
}

// TestStress_Pool_PanicRecovery 并发池 panic 恢复
func TestStress_Pool_PanicRecovery(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](4)
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func(n int) {
				defer wg.Done()
				_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
					if n == 5 {
						panic("concurrent pool panic test")
					}
					return n, nil
				})
			}(i)
		}
		wg.Wait()
		results := p.Wait()
		p.Close()
		if len(results) != 10 {
			t.Fatalf("round %d: expected 10 results, got %d", round, len(results))
		}
		hasPanic := false
		for _, r := range results {
			if r.IsPanic() {
				hasPanic = true
				break
			}
		}
		if !hasPanic {
			t.Fatalf("round %d: expected at least one panic result", round)
		}
	}
}

// TestStress_Group_SubmitTimeout_Concurrent 并发提交超时
func TestStress_Group_SubmitTimeout_Concurrent(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()
		g := NewGroup[int](1) // 只有 1 个并发槽位
		g.WithSubmitTimeout(50 * time.Millisecond)
		// 第一个任务占用槽位
		blockCh := make(chan struct{})
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			<-blockCh
			return 1, nil
		})
		// 并发提交，应该超时
		var wg sync.WaitGroup
		var timeoutCnt atomic.Int64
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func() {
				defer wg.Done()
				err := g.Go(ctx, func(ctx context.Context) (int, error) {
					return 2, nil
				})
				if errors.Is(err, ErrSubmitTimeout) {
					timeoutCnt.Add(1)
				}
			}()
		}
		wg.Wait()
		close(blockCh) // 释放第一个任务
		g.Wait()
		if timeoutCnt.Load() == 0 {
			t.Fatalf("round %d: expected at least one timeout", round)
		}
	}
}

// TestStress_WaitTimeout_ConcurrentCallers 同一 Group 多个 goroutine 并发调用 WaitTimeout
func TestStress_WaitTimeout_ConcurrentCallers(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		for i := 0; i < 10; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Microsecond)
				return 1, nil
			})
		}
		var wg sync.WaitGroup
		allOk := atomic.Bool{}
		allOk.Store(true)
		wg.Add(3)
		for i := 0; i < 3; i++ {
			go func() {
				defer wg.Done()
				results, ok := g.WaitTimeout(5 * time.Second)
				if !ok {
					allOk.Store(false)
				}
				if len(results) != 10 {
					allOk.Store(false)
				}
			}()
		}
		wg.Wait()
		if !allOk.Load() {
			t.Fatalf("round %d: concurrent WaitTimeout failed", round)
		}
	}
}

// ==================== DefaultForEach 测试 ====================

func TestDefaultForEach(t *testing.T) {
	var mu sync.Mutex
	collected := make([]int, 0)
	nr, err := DefaultForEach(context.Background(), []int{1, 2, 3, 4, 5}, func(ctx context.Context, v int) error {
		mu.Lock()
		collected = append(collected, v)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEach error: %v", err)
	}
	if nr.HasError() {
		t.Fatal("DefaultForEach should not have errors")
	}
	if len(collected) != 5 {
		t.Fatalf("expected 5 collected, got %d", len(collected))
	}
}

func TestDefaultForEach_EmptySlice(t *testing.T) {
	nr, err := DefaultForEach(context.Background(), []int{}, func(ctx context.Context, v int) error {
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEach empty slice error: %v", err)
	}
	if nr.TotalCount() != 0 {
		t.Fatalf("expected 0 total, got %d", nr.TotalCount())
	}
}

func TestDefaultForEach_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	nr, err := DefaultForEach(ctx, []int{1, 2, 3}, func(ctx context.Context, v int) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected context canceled error")
	}
	if nr.TotalCount() != 3 {
		t.Fatalf("expected 3 total, got %d", nr.TotalCount())
	}
}

// ==================== ForEachWithFailFast 测试 ====================

func TestForEachWithFailFast_Success(t *testing.T) {
	var counter int32
	nr, err := ForEachWithFailFast(context.Background(), []int{1, 2, 3, 4, 5}, 4, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachWithFailFast error: %v", err)
	}
	if nr.FailCount() != 0 {
		t.Fatalf("expected 0 failures, got %d", nr.FailCount())
	}
}

func TestForEachWithFailFast_EmptySlice(t *testing.T) {
	nr, err := ForEachWithFailFast(context.Background(), []int{}, 4, func(ctx context.Context, v int) error {
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachWithFailFast empty slice error: %v", err)
	}
	if nr.TotalCount() != 0 {
		t.Fatalf("expected 0 total, got %d", nr.TotalCount())
	}
}

func TestForEachWithFailFast_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	nr, err := ForEachWithFailFast(ctx, []int{1, 2, 3}, 4, func(ctx context.Context, v int) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected context canceled error")
	}
	if nr.TotalCount() != 3 {
		t.Fatalf("expected 3 total, got %d", nr.TotalCount())
	}
}

func TestForEachWithFailFast_Serial(t *testing.T) {
	var counter int32
	nr, err := ForEachWithFailFast(context.Background(), []int{1, 2, 3}, 0, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachWithFailFast serial error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 3 {
		t.Fatalf("expected 3, got %d", counter)
	}
	_ = nr
}

// ==================== DefaultForEachWithFailFast 测试 ====================

func TestDefaultForEachWithFailFast(t *testing.T) {
	var counter int32
	nr, err := DefaultForEachWithFailFast(context.Background(), []int{1, 2, 3, 4, 5}, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachWithFailFast error: %v", err)
	}
	if nr.FailCount() != 0 {
		t.Fatalf("expected 0 failures, got %d", nr.FailCount())
	}
}

func TestDefaultForEachWithFailFast_Error(t *testing.T) {
	nr, err := DefaultForEachWithFailFast(context.Background(), []int{1, 2, 3, 4, 5}, func(ctx context.Context, v int) error {
		if v == 3 {
			return errTest
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected error from DefaultForEachWithFailFast")
	}
	if nr.FailCount() == 0 {
		t.Fatal("expected at least 1 failure")
	}
}

// ==================== ForEachWithTimeout 测试 ====================

func TestForEachWithTimeout(t *testing.T) {
	var counter int32
	nr, err := ForEachWithTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 500*time.Millisecond, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachWithTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 5 {
		t.Fatalf("expected 5, got %d", counter)
	}
	_ = nr
}

func TestForEachWithTimeout_Timeout(t *testing.T) {
	nr, err := ForEachWithTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 10*time.Millisecond, func(ctx context.Context, v int) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return nil
		}
	})
	if err == nil {
		t.Fatal("expected error due to timeout, but got nil")
	}
	if nr.FailCount() == 0 {
		t.Fatal("expected at least 1 timeout failure")
	}
}

func TestForEachWithTimeout_EmptySlice(t *testing.T) {
	nr, err := ForEachWithTimeout(context.Background(), []int{}, 4, time.Second, func(ctx context.Context, v int) error {
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachWithTimeout empty slice error: %v", err)
	}
	if nr.TotalCount() != 0 {
		t.Fatalf("expected 0 total, got %d", nr.TotalCount())
	}
}

func TestForEachWithTimeout_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	nr, err := ForEachWithTimeout(ctx, []int{1, 2, 3}, 4, time.Second, func(ctx context.Context, v int) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected context canceled error")
	}
	if nr.TotalCount() != 3 {
		t.Fatalf("expected 3 total, got %d", nr.TotalCount())
	}
}

func TestForEachWithTimeout_NoTimeout(t *testing.T) {
	var counter int32
	nr, err := ForEachWithTimeout(context.Background(), []int{1, 2, 3}, 4, 0, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachWithTimeout no-timeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 3 {
		t.Fatalf("expected 3, got %d", counter)
	}
	_ = nr
}

// ==================== DefaultForEachWithTimeout 测试 ====================

func TestDefaultForEachWithTimeout(t *testing.T) {
	var counter int32
	nr, err := DefaultForEachWithTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 500*time.Millisecond, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachWithTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 5 {
		t.Fatalf("expected 5, got %d", counter)
	}
	_ = nr
}

// ==================== ForEachWithFFTimeout 测试 ====================

func TestForEachWithFFTimeout(t *testing.T) {
	var counter int32
	nr, err := ForEachWithFFTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 500*time.Millisecond, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachWithFFTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 5 {
		t.Fatalf("expected 5, got %d", counter)
	}
	_ = nr
}

func TestForEachWithFFTimeout_FailFast(t *testing.T) {
	nr, err := ForEachWithFFTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 500*time.Millisecond, func(ctx context.Context, v int) error {
		if v == 2 {
			return errTest
		}
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	if err == nil {
		t.Fatal("expected error from ForEachWithFFTimeout")
	}
	if nr.FailCount() == 0 {
		t.Fatal("expected at least 1 failure")
	}
}

func TestForEachWithFFTimeout_EmptySlice(t *testing.T) {
	nr, err := ForEachWithFFTimeout(context.Background(), []int{}, 4, time.Second, func(ctx context.Context, v int) error {
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachWithFFTimeout empty slice error: %v", err)
	}
	if nr.TotalCount() != 0 {
		t.Fatalf("expected 0 total, got %d", nr.TotalCount())
	}
}

func TestForEachWithFFTimeout_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	nr, err := ForEachWithFFTimeout(ctx, []int{1, 2, 3}, 4, time.Second, func(ctx context.Context, v int) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected context canceled error")
	}
	if nr.TotalCount() != 3 {
		t.Fatalf("expected 3 total, got %d", nr.TotalCount())
	}
}

// ==================== DefaultForEachWithFFTimeout 测试 ====================

func TestDefaultForEachWithFFTimeout(t *testing.T) {
	var counter int32
	nr, err := DefaultForEachWithFFTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 500*time.Millisecond, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachWithFFTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 5 {
		t.Fatalf("expected 5, got %d", counter)
	}
	_ = nr
}

func TestDefaultForEachWithFFTimeout_FailFast(t *testing.T) {
	nr, err := DefaultForEachWithFFTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 500*time.Millisecond, func(ctx context.Context, v int) error {
		if v == 3 {
			return errTest
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected error from DefaultForEachWithFFTimeout")
	}
	if nr.FailCount() == 0 {
		t.Fatal("expected at least 1 failure")
	}
}

// ==================== ResultValues / ResultErrors 测试 ====================

func TestResultValues(t *testing.T) {
	results := []Result[int]{
		{Value: 1, Err: nil},
		{Value: 0, Err: errTest},
		{Value: 3, Err: nil},
		{Value: 0, Err: errors.New("another error")},
		{Value: 5, Err: nil},
	}
	vals := ResultValues(results)
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d", len(vals))
	}
	if vals[0] != 1 || vals[1] != 3 || vals[2] != 5 {
		t.Fatalf("unexpected values: %v", vals)
	}
}

func TestResultValues_AllSuccess(t *testing.T) {
	results := []Result[int]{
		{Value: 10, Err: nil},
		{Value: 20, Err: nil},
	}
	vals := ResultValues(results)
	if len(vals) != 2 {
		t.Fatalf("expected 2 values, got %d", len(vals))
	}
}

func TestResultValues_AllErrors(t *testing.T) {
	results := []Result[int]{
		{Value: 0, Err: errTest},
		{Value: 0, Err: errTest},
	}
	vals := ResultValues(results)
	if len(vals) != 0 {
		t.Fatalf("expected 0 values, got %d", len(vals))
	}
}

func TestResultValues_Empty(t *testing.T) {
	vals := ResultValues([]Result[int]{})
	if len(vals) != 0 {
		t.Fatalf("expected 0 values, got %d", len(vals))
	}
}

func TestResultErrors(t *testing.T) {
	results := []Result[int]{
		{Value: 1, Err: nil},
		{Value: 0, Err: errTest},
		{Value: 3, Err: nil},
		{Value: 0, Err: errors.New("another error")},
	}
	errs := ResultErrors(results)
	if len(errs) != 2 {
		t.Fatalf("expected 2 errors, got %d", len(errs))
	}
}

func TestResultErrors_AllSuccess(t *testing.T) {
	results := []Result[int]{
		{Value: 10, Err: nil},
		{Value: 20, Err: nil},
	}
	errs := ResultErrors(results)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d", len(errs))
	}
}

func TestResultErrors_Empty(t *testing.T) {
	errs := ResultErrors([]Result[int]{})
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d", len(errs))
	}
}

// ==================== Every / Some / AnyError / Partition 测试 ====================

func TestEvery_AllSuccess(t *testing.T) {
	results := []Result[int]{
		{Value: 1, Err: nil},
		{Value: 2, Err: nil},
		{Value: 3, Err: nil},
	}
	if !Every(results) {
		t.Fatal("expected Every to return true for all success")
	}
}

func TestEvery_HasError(t *testing.T) {
	results := []Result[int]{
		{Value: 1, Err: nil},
		{Value: 0, Err: errTest},
		{Value: 3, Err: nil},
	}
	if Every(results) {
		t.Fatal("expected Every to return false when has error")
	}
}

func TestEvery_Empty(t *testing.T) {
	if !Every([]Result[int]{}) {
		t.Fatal("expected Every to return true for empty slice")
	}
}

func TestSome_AllSuccess(t *testing.T) {
	results := []Result[int]{
		{Value: 1, Err: nil},
		{Value: 2, Err: nil},
	}
	if !Some(results) {
		t.Fatal("expected Some to return true")
	}
}

func TestSome_SomeSuccess(t *testing.T) {
	results := []Result[int]{
		{Value: 0, Err: errTest},
		{Value: 2, Err: nil},
		{Value: 0, Err: errTest},
	}
	if !Some(results) {
		t.Fatal("expected Some to return true when has at least one success")
	}
}

func TestSome_AllErrors(t *testing.T) {
	results := []Result[int]{
		{Value: 0, Err: errTest},
		{Value: 0, Err: errTest},
	}
	if Some(results) {
		t.Fatal("expected Some to return false for all errors")
	}
}

func TestSome_Empty(t *testing.T) {
	if Some([]Result[int]{}) {
		t.Fatal("expected Some to return false for empty slice")
	}
}

func TestAnyError_HasError(t *testing.T) {
	results := []Result[int]{
		{Value: 1, Err: nil},
		{Value: 0, Err: errTest},
	}
	if !AnyError(results) {
		t.Fatal("expected AnyError to return true")
	}
}

func TestAnyError_NoError(t *testing.T) {
	results := []Result[int]{
		{Value: 1, Err: nil},
		{Value: 2, Err: nil},
	}
	if AnyError(results) {
		t.Fatal("expected AnyError to return false")
	}
}

func TestAnyError_Empty(t *testing.T) {
	if AnyError([]Result[int]{}) {
		t.Fatal("expected AnyError to return false for empty slice")
	}
}

func TestPartition(t *testing.T) {
	results := []Result[int]{
		{Value: 1, Err: nil},
		{Value: 0, Err: errTest},
		{Value: 3, Err: nil},
		{Value: 0, Err: errors.New("another error")},
		{Value: 5, Err: nil},
	}
	successes, failures := Partition(results)
	if len(successes) != 3 {
		t.Fatalf("expected 3 successes, got %d", len(successes))
	}
	if len(failures) != 2 {
		t.Fatalf("expected 2 failures, got %d", len(failures))
	}
	if successes[0] != 1 || successes[1] != 3 || successes[2] != 5 {
		t.Fatalf("unexpected successes: %v", successes)
	}
}

func TestPartition_AllSuccess(t *testing.T) {
	results := []Result[int]{
		{Value: 10, Err: nil},
		{Value: 20, Err: nil},
	}
	successes, failures := Partition(results)
	if len(successes) != 2 {
		t.Fatalf("expected 2 successes, got %d", len(successes))
	}
	if len(failures) != 0 {
		t.Fatalf("expected 0 failures, got %d", len(failures))
	}
}

func TestPartition_AllErrors(t *testing.T) {
	results := []Result[int]{
		{Value: 0, Err: errTest},
		{Value: 0, Err: errTest},
	}
	successes, failures := Partition(results)
	if len(successes) != 0 {
		t.Fatalf("expected 0 successes, got %d", len(successes))
	}
	if len(failures) != 2 {
		t.Fatalf("expected 2 failures, got %d", len(failures))
	}
}

func TestPartition_Empty(t *testing.T) {
	successes, failures := Partition([]Result[int]{})
	if len(successes) != 0 {
		t.Fatalf("expected 0 successes, got %d", len(successes))
	}
	if len(failures) != 0 {
		t.Fatalf("expected 0 failures, got %d", len(failures))
	}
}

// ==================== Reduce 测试 ====================

func TestReduce(t *testing.T) {
	total, err := Reduce(context.Background(), []int{1, 2, 3, 4, 5}, 4,
		func(ctx context.Context, v int) (int, error) {
			return v * 10, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err != nil {
		t.Fatalf("Reduce error: %v", err)
	}
	if total != 150 {
		t.Fatalf("expected 150, got %d", total)
	}
}

func TestReduce_WithError(t *testing.T) {
	total, err := Reduce(context.Background(), []int{1, 2, 3, 4, 5}, 4,
		func(ctx context.Context, v int) (int, error) {
			if v == 3 {
				return 0, errTest
			}
			return v, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err == nil {
		t.Fatal("expected error from Reduce")
	}
	if total != 1+2+4+5 {
		t.Fatalf("expected %d, got %d", 1+2+4+5, total)
	}
}

func TestReduce_EmptySlice(t *testing.T) {
	total, err := Reduce(context.Background(), []int{}, 4,
		func(ctx context.Context, v int) (int, error) { return v, nil },
		0,
		func(acc, val int) int { return acc + val },
	)
	if err != nil {
		t.Fatalf("Reduce empty slice error: %v", err)
	}
	if total != 0 {
		t.Fatalf("expected 0, got %d", total)
	}
}

// ==================== DefaultReduce 测试 ====================

func TestDefaultReduce(t *testing.T) {
	total, err := DefaultReduce(context.Background(), []int{1, 2, 3, 4, 5},
		func(ctx context.Context, v int) (int, error) {
			return v * 2, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err != nil {
		t.Fatalf("DefaultReduce error: %v", err)
	}
	if total != 30 {
		t.Fatalf("expected 30, got %d", total)
	}
}

// ==================== ReduceWithFailFast 测试 ====================

func TestReduceWithFailFast(t *testing.T) {
	total, err := ReduceWithFailFast(context.Background(), []int{1, 2, 3, 4, 5}, 4,
		func(ctx context.Context, v int) (int, error) {
			return v * 10, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err != nil {
		t.Fatalf("ReduceWithFailFast error: %v", err)
	}
	if total != 150 {
		t.Fatalf("expected 150, got %d", total)
	}
}

func TestReduceWithFailFast_Error(t *testing.T) {
	total, err := ReduceWithFailFast(context.Background(), []int{1, 2, 3, 4, 5}, 4,
		func(ctx context.Context, v int) (int, error) {
			if v == 3 {
				return 0, errTest
			}
			time.Sleep(50 * time.Millisecond)
			return v, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err == nil {
		t.Fatal("expected error from ReduceWithFailFast")
	}
	_ = total
}

// ==================== DefaultReduceWithFailFast 测试 ====================

func TestDefaultReduceWithFailFast(t *testing.T) {
	total, err := DefaultReduceWithFailFast(context.Background(), []int{1, 2, 3, 4, 5},
		func(ctx context.Context, v int) (int, error) {
			return v, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err != nil {
		t.Fatalf("DefaultReduceWithFailFast error: %v", err)
	}
	if total != 15 {
		t.Fatalf("expected 15, got %d", total)
	}
}

// ==================== ReduceWithTimeout 测试 ====================

func TestReduceWithTimeout(t *testing.T) {
	total, err := ReduceWithTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 500*time.Millisecond,
		func(ctx context.Context, v int) (int, error) {
			return v * 10, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err != nil {
		t.Fatalf("ReduceWithTimeout error: %v", err)
	}
	if total != 150 {
		t.Fatalf("expected 150, got %d", total)
	}
}

func TestReduceWithTimeout_Timeout(t *testing.T) {
	total, err := ReduceWithTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 10*time.Millisecond,
		func(ctx context.Context, v int) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return v, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err != nil {
		t.Fatalf("ReduceWithTimeout timeout error: %v", err)
	}
	_ = total
}

// ==================== DefaultReduceWithTimeout 测试 ====================

func TestDefaultReduceWithTimeout(t *testing.T) {
	total, err := DefaultReduceWithTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 500*time.Millisecond,
		func(ctx context.Context, v int) (int, error) {
			return v, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err != nil {
		t.Fatalf("DefaultReduceWithTimeout error: %v", err)
	}
	if total != 15 {
		t.Fatalf("expected 15, got %d", total)
	}
}

// ==================== ReduceWithFFTimeout 测试 ====================

func TestReduceWithFFTimeout(t *testing.T) {
	total, err := ReduceWithFFTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 500*time.Millisecond,
		func(ctx context.Context, v int) (int, error) {
			return v * 10, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err != nil {
		t.Fatalf("ReduceWithFFTimeout error: %v", err)
	}
	if total != 150 {
		t.Fatalf("expected 150, got %d", total)
	}
}

func TestReduceWithFFTimeout_FailFast(t *testing.T) {
	total, err := ReduceWithFFTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 500*time.Millisecond,
		func(ctx context.Context, v int) (int, error) {
			if v == 3 {
				return 0, errTest
			}
			time.Sleep(50 * time.Millisecond)
			return v, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err == nil {
		t.Fatal("expected error from ReduceWithFFTimeout")
	}
	_ = total
}

// ==================== DefaultReduceWithFFTimeout 测试 ====================

func TestDefaultReduceWithFFTimeout(t *testing.T) {
	total, err := DefaultReduceWithFFTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 500*time.Millisecond,
		func(ctx context.Context, v int) (int, error) {
			return v, nil
		},
		0,
		func(acc, val int) int { return acc + val },
	)
	if err != nil {
		t.Fatalf("DefaultReduceWithFFTimeout error: %v", err)
	}
	if total != 15 {
		t.Fatalf("expected 15, got %d", total)
	}
}

// ==================== DefaultMapChunk 测试 ====================

func TestDefaultMapChunk(t *testing.T) {
	slice := make([]int, 50)
	for i := range slice {
		slice[i] = i
	}
	results := DefaultMapChunk(context.Background(), slice, 10, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 5 {
		t.Fatalf("expected 5 chunk results, got %d", len(results))
	}
	total := 0
	for _, r := range results {
		total += r.Value
	}
	expected := 49 * 50 / 2
	if total != expected {
		t.Fatalf("expected total %d, got %d", expected, total)
	}
}

// ==================== MapChunkWithFailFast 测试 ====================

func TestMapChunkWithFailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	results, err := MapChunkWithFailFast(context.Background(), slice, 4, 3, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			if v == 5 {
				return 0, errTest
			}
			sum += v
		}
		return sum, nil
	})
	if err == nil {
		t.Fatal("expected error from MapChunkWithFailFast")
	}
	_ = results
}

func TestMapChunkWithFailFast_Success(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results, err := MapChunkWithFailFast(context.Background(), slice, 4, 2, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if err != nil {
		t.Fatalf("MapChunkWithFailFast error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 chunk results, got %d", len(results))
	}
}

// ==================== DefaultMapChunkWithFailFast 测试 ====================

func TestDefaultMapChunkWithFailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results, err := DefaultMapChunkWithFailFast(context.Background(), slice, 2, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if err != nil {
		t.Fatalf("DefaultMapChunkWithFailFast error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 chunk results, got %d", len(results))
	}
}

// ==================== MapChunkWithTimeout 测试 ====================

func TestMapChunkWithTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results := MapChunkWithTimeout(context.Background(), slice, 4, 2, 500*time.Millisecond, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 3 {
		t.Fatalf("expected 3 chunk results, got %d", len(results))
	}
}

// ==================== DefaultMapChunkWithTimeout 测试 ====================

func TestDefaultMapChunkWithTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results := DefaultMapChunkWithTimeout(context.Background(), slice, 2, 500*time.Millisecond, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 3 {
		t.Fatalf("expected 3 chunk results, got %d", len(results))
	}
}

// ==================== MapChunkWithFFTimeout 测试 ====================

func TestMapChunkWithFFTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results, err := MapChunkWithFFTimeout(context.Background(), slice, 4, 2, 500*time.Millisecond, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if err != nil {
		t.Fatalf("MapChunkWithFFTimeout error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 chunk results, got %d", len(results))
	}
}

func TestMapChunkWithFFTimeout_FailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	results, err := MapChunkWithFFTimeout(context.Background(), slice, 4, 3, 500*time.Millisecond, func(ctx context.Context, chunk []int) (int, error) {
		for _, v := range chunk {
			if v == 5 {
				return 0, errTest
			}
		}
		return 1, nil
	})
	if err == nil {
		t.Fatal("expected error from MapChunkWithFFTimeout")
	}
	_ = results
}

// ==================== DefaultMapChunkWithFFTimeout 测试 ====================

func TestDefaultMapChunkWithFFTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results, err := DefaultMapChunkWithFFTimeout(context.Background(), slice, 2, 500*time.Millisecond, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if err != nil {
		t.Fatalf("DefaultMapChunkWithFFTimeout error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 chunk results, got %d", len(results))
	}
}

// ==================== MapChunked 测试 ====================

func TestMapChunked(t *testing.T) {
	slice := make([]int, 20)
	for i := range slice {
		slice[i] = i
	}
	results := MapChunked(context.Background(), slice, 4, 5, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Value != i*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, i*10, r.Value)
		}
	}
}

func TestMapChunked_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	slice := make([]int, 20)
	for i := range slice {
		slice[i] = i
	}
	var started sync.WaitGroup
	started.Add(1)
	go func() {
		started.Wait()
		cancel()
	}()
	var startedOnce sync.Once
	results := MapChunked(ctx, slice, 4, 5, func(ctx context.Context, v int) (int, error) {
		startedOnce.Do(started.Done)
		time.Sleep(200 * time.Millisecond)
		return v, nil
	})
	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}
}

// ==================== DefaultMapChunked 测试 ====================

func TestDefaultMapChunked(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	results := DefaultMapChunked(context.Background(), slice, 3, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Value != (i+1)*2 {
			t.Fatalf("at index %d: expected %d, got %d", i, (i+1)*2, r.Value)
		}
	}
}

// ==================== MapChunkedWithFailFast 测试 ====================

func TestMapChunkedWithFailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	results, err := MapChunkedWithFailFast(context.Background(), slice, 4, 3, func(ctx context.Context, v int) (int, error) {
		if v == 5 {
			return 0, errTest
		}
		time.Sleep(50 * time.Millisecond)
		return v, nil
	})
	if err == nil {
		t.Fatal("expected error from MapChunkedWithFailFast")
	}
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
}

func TestMapChunkedWithFailFast_Success(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results, err := MapChunkedWithFailFast(context.Background(), slice, 4, 2, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	if err != nil {
		t.Fatalf("MapChunkedWithFailFast error: %v", err)
	}
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}
}

// ==================== DefaultMapChunkedWithFailFast 测试 ====================

func TestDefaultMapChunkedWithFailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results, err := DefaultMapChunkedWithFailFast(context.Background(), slice, 2, func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if err != nil {
		t.Fatalf("DefaultMapChunkedWithFailFast error: %v", err)
	}
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}
}

// ==================== MapChunkedWithTimeout 测试 ====================

func TestMapChunkedWithTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results := MapChunkedWithTimeout(context.Background(), slice, 4, 2, 500*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Value != (i+1)*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, (i+1)*10, r.Value)
		}
	}
}

// ==================== DefaultMapChunkedWithTimeout 测试 ====================

func TestDefaultMapChunkedWithTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results := DefaultMapChunkedWithTimeout(context.Background(), slice, 2, 500*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}
}

// ==================== MapChunkedWithFFTimeout 测试 ====================

func TestMapChunkedWithFFTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results, err := MapChunkedWithFFTimeout(context.Background(), slice, 4, 2, 500*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	if err != nil {
		t.Fatalf("MapChunkedWithFFTimeout error: %v", err)
	}
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}
}

func TestMapChunkedWithFFTimeout_FailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	results, err := MapChunkedWithFFTimeout(context.Background(), slice, 4, 3, 500*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		if v == 5 {
			return 0, errTest
		}
		time.Sleep(50 * time.Millisecond)
		return v, nil
	})
	if err == nil {
		t.Fatal("expected error from MapChunkedWithFFTimeout")
	}
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
}

// ==================== DefaultMapChunkedWithFFTimeout 测试 ====================

func TestDefaultMapChunkedWithFFTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	results, err := DefaultMapChunkedWithFFTimeout(context.Background(), slice, 2, 500*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		return v, nil
	})
	if err != nil {
		t.Fatalf("DefaultMapChunkedWithFFTimeout error: %v", err)
	}
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}
}

// ==================== DefaultForEachChunk 测试 ====================

func TestDefaultForEachChunk(t *testing.T) {
	slice := make([]int, 50)
	for i := range slice {
		slice[i] = i
	}
	var total int64
	nr, err := DefaultForEachChunk(context.Background(), slice, 10, func(ctx context.Context, chunk []int) error {
		for _, v := range chunk {
			atomic.AddInt64(&total, int64(v))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachChunk error: %v", err)
	}
	_ = nr
	expected := int64(49 * 50 / 2)
	if total != expected {
		t.Fatalf("expected total %d, got %d", expected, total)
	}
}

// ==================== ForEachChunkWithFailFast 测试 ====================

func TestForEachChunkWithFailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	nr, err := ForEachChunkWithFailFast(context.Background(), slice, 4, 3, func(ctx context.Context, chunk []int) error {
		for _, v := range chunk {
			if v == 5 {
				return errTest
			}
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected error from ForEachChunkWithFailFast")
	}
	_ = nr
}

func TestForEachChunkWithFailFast_Success(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := ForEachChunkWithFailFast(context.Background(), slice, 4, 2, func(ctx context.Context, chunk []int) error {
		for range chunk {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachChunkWithFailFast error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

// ==================== DefaultForEachChunkWithFailFast 测试 ====================

func TestDefaultForEachChunkWithFailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := DefaultForEachChunkWithFailFast(context.Background(), slice, 2, func(ctx context.Context, chunk []int) error {
		for range chunk {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachChunkWithFailFast error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

// ==================== ForEachChunkWithTimeout 测试 ====================

func TestForEachChunkWithTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := ForEachChunkWithTimeout(context.Background(), slice, 4, 2, 500*time.Millisecond, func(ctx context.Context, chunk []int) error {
		for range chunk {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachChunkWithTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

// ==================== DefaultForEachChunkWithTimeout 测试 ====================

func TestDefaultForEachChunkWithTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := DefaultForEachChunkWithTimeout(context.Background(), slice, 2, 500*time.Millisecond, func(ctx context.Context, chunk []int) error {
		for range chunk {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachChunkWithTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

// ==================== ForEachChunkWithFFTimeout 测试 ====================

func TestForEachChunkWithFFTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := ForEachChunkWithFFTimeout(context.Background(), slice, 4, 2, 500*time.Millisecond, func(ctx context.Context, chunk []int) error {
		for range chunk {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachChunkWithFFTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

func TestForEachChunkWithFFTimeout_FailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	nr, err := ForEachChunkWithFFTimeout(context.Background(), slice, 4, 3, 500*time.Millisecond, func(ctx context.Context, chunk []int) error {
		for _, v := range chunk {
			if v == 5 {
				return errTest
			}
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected error from ForEachChunkWithFFTimeout")
	}
	_ = nr
}

// ==================== DefaultForEachChunkWithFFTimeout 测试 ====================

func TestDefaultForEachChunkWithFFTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := DefaultForEachChunkWithFFTimeout(context.Background(), slice, 2, 500*time.Millisecond, func(ctx context.Context, chunk []int) error {
		for range chunk {
			atomic.AddInt32(&counter, 1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachChunkWithFFTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

// ==================== ForEachChunked 测试 ====================

func TestForEachChunked(t *testing.T) {
	slice := make([]int, 20)
	for i := range slice {
		slice[i] = i
	}
	var total int64
	nr, err := ForEachChunked(context.Background(), slice, 4, 5, func(ctx context.Context, v int) error {
		atomic.AddInt64(&total, int64(v))
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachChunked error: %v", err)
	}
	expected := int64(19 * 20 / 2)
	if total != expected {
		t.Fatalf("expected total %d, got %d", expected, total)
	}
	if nr.TotalCount() != 20 {
		t.Fatalf("expected 20 total, got %d", nr.TotalCount())
	}
}

func TestForEachChunked_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	slice := make([]int, 20)
	for i := range slice {
		slice[i] = i
	}
	var started sync.WaitGroup
	started.Add(1)
	go func() {
		started.Wait()
		cancel()
	}()
	var startedOnce sync.Once
	nr, _ := ForEachChunked(ctx, slice, 4, 5, func(ctx context.Context, v int) error {
		startedOnce.Do(started.Done)
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	_ = nr
}

// ==================== DefaultForEachChunked 测试 ====================

func TestDefaultForEachChunked(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	var total int64
	nr, err := DefaultForEachChunked(context.Background(), slice, 3, func(ctx context.Context, v int) error {
		atomic.AddInt64(&total, int64(v))
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachChunked error: %v", err)
	}
	if total != 55 {
		t.Fatalf("expected 55, got %d", total)
	}
	if nr.TotalCount() != 10 {
		t.Fatalf("expected 10 total, got %d", nr.TotalCount())
	}
}

// ==================== ForEachChunkedWithFailFast 测试 ====================

func TestForEachChunkedWithFailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	nr, err := ForEachChunkedWithFailFast(context.Background(), slice, 4, 3, func(ctx context.Context, v int) error {
		if v == 5 {
			return errTest
		}
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	if err == nil {
		t.Fatal("expected error from ForEachChunkedWithFailFast")
	}
	if nr.TotalCount() != 10 {
		t.Fatalf("expected 10 total, got %d", nr.TotalCount())
	}
}

func TestForEachChunkedWithFailFast_Success(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := ForEachChunkedWithFailFast(context.Background(), slice, 4, 2, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachChunkedWithFailFast error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

// ==================== DefaultForEachChunkedWithFailFast 测试 ====================

func TestDefaultForEachChunkedWithFailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := DefaultForEachChunkedWithFailFast(context.Background(), slice, 2, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachChunkedWithFailFast error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

// ==================== ForEachChunkedWithTimeout 测试 ====================

func TestForEachChunkedWithTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := ForEachChunkedWithTimeout(context.Background(), slice, 4, 2, 500*time.Millisecond, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachChunkedWithTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

// ==================== DefaultForEachChunkedWithTimeout 测试 ====================

func TestDefaultForEachChunkedWithTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := DefaultForEachChunkedWithTimeout(context.Background(), slice, 2, 500*time.Millisecond, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachChunkedWithTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

// ==================== ForEachChunkedWithFFTimeout 测试 ====================

func TestForEachChunkedWithFFTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := ForEachChunkedWithFFTimeout(context.Background(), slice, 4, 2, 500*time.Millisecond, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachChunkedWithFFTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

func TestForEachChunkedWithFFTimeout_FailFast(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	nr, err := ForEachChunkedWithFFTimeout(context.Background(), slice, 4, 3, 500*time.Millisecond, func(ctx context.Context, v int) error {
		if v == 5 {
			return errTest
		}
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	if err == nil {
		t.Fatal("expected error from ForEachChunkedWithFFTimeout")
	}
	if nr.TotalCount() != 10 {
		t.Fatalf("expected 10 total, got %d", nr.TotalCount())
	}
}

// ==================== DefaultForEachChunkedWithFFTimeout 测试 ====================

func TestDefaultForEachChunkedWithFFTimeout(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6}
	var counter int32
	nr, err := DefaultForEachChunkedWithFFTimeout(context.Background(), slice, 2, 500*time.Millisecond, func(ctx context.Context, v int) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("DefaultForEachChunkedWithFFTimeout error: %v", err)
	}
	if atomic.LoadInt32(&counter) != 6 {
		t.Fatalf("expected 6, got %d", counter)
	}
	_ = nr
}

// ==================== NoResultPool 辅助函数测试 ====================

func TestDefaultNoResultPool(t *testing.T) {
	pool := DefaultNoResultPool()
	defer pool.Close()
	if pool.Size() <= 0 {
		t.Fatal("expected non-zero pool size")
	}
}

func TestTrySubmitAction(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(1)
	defer pool.Close()
	var started sync.WaitGroup
	started.Add(1)
	blockCh := make(chan struct{})
	_ = SubmitAction(pool, ctx, func(ctx context.Context) error {
		started.Done()
		<-blockCh
		return nil
	})
	started.Wait()
	_ = SubmitAction(pool, ctx, func(ctx context.Context) error {
		<-blockCh
		return nil
	})
	_ = SubmitAction(pool, ctx, func(ctx context.Context) error {
		<-blockCh
		return nil
	})
	err := TrySubmitAction(pool, ctx, func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, ErrSubmitTimeout) {
		t.Fatalf("expected ErrSubmitTimeout, got %v", err)
	}
	close(blockCh)
	pool.Wait()
}

func TestTrySubmitAction_Success(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	var counter int32
	err := TrySubmitAction(pool, ctx, func(ctx context.Context) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("TrySubmitAction error: %v", err)
	}
	pool.Wait()
	if atomic.LoadInt32(&counter) != 1 {
		t.Fatalf("expected 1, got %d", counter)
	}
}

func TestSubmitAtAction(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	var executed [3]bool
	for i := 0; i < 3; i++ {
		idx := i
		err := SubmitAtAction(pool, idx, ctx, func(ctx context.Context) error {
			executed[idx] = true
			return nil
		})
		if err != nil {
			t.Fatalf("SubmitAtAction error: %v", err)
		}
	}
	pool.Wait()
	for i := 0; i < 3; i++ {
		if !executed[i] {
			t.Fatalf("task %d not executed", i)
		}
	}
}

func TestSubmitAtAction_Error(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	err := SubmitAtAction(pool, 2, ctx, func(ctx context.Context) error {
		return errTest
	})
	if err != nil {
		t.Fatalf("SubmitAtAction submit error: %v", err)
	}
	pool.Wait()
	if pool.FailCount() != 1 {
		t.Fatalf("expected 1 failure, got %d", pool.FailCount())
	}
}

func TestTrySubmitAtAction(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(1)
	defer pool.Close()
	var started sync.WaitGroup
	started.Add(1)
	blockCh := make(chan struct{})
	_ = SubmitAtAction(pool, 0, ctx, func(ctx context.Context) error {
		started.Done()
		<-blockCh
		return nil
	})
	started.Wait()
	_ = SubmitAtAction(pool, 1, ctx, func(ctx context.Context) error {
		<-blockCh
		return nil
	})
	_ = SubmitAtAction(pool, 2, ctx, func(ctx context.Context) error {
		<-blockCh
		return nil
	})
	err := TrySubmitAtAction(pool, 3, ctx, func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, ErrSubmitTimeout) {
		t.Fatalf("expected ErrSubmitTimeout, got %v", err)
	}
	close(blockCh)
	pool.Wait()
}

func TestTrySubmitAtAction_Success(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	var executed bool
	err := TrySubmitAtAction(pool, 0, ctx, func(ctx context.Context) error {
		executed = true
		return nil
	})
	if err != nil {
		t.Fatalf("TrySubmitAtAction error: %v", err)
	}
	pool.Wait()
	if !executed {
		t.Fatal("task not executed")
	}
}

func TestGoAction(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	var counter int32
	GoAction(pool, ctx, func(ctx context.Context) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	pool.Wait()
	if atomic.LoadInt32(&counter) != 1 {
		t.Fatalf("expected 1, got %d", counter)
	}
}

func TestGoAction_Panic(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from GoAction when pool is closed")
		}
	}()
	pool.Close()
	pool.Wait()
	GoAction(pool, ctx, func(ctx context.Context) error {
		return nil
	})
}

func TestSubmitActionWithTimeout(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	var counter int32
	err := SubmitActionWithTimeout(pool, ctx, 500*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("SubmitActionWithTimeout error: %v", err)
	}
	pool.Wait()
	if atomic.LoadInt32(&counter) != 1 {
		t.Fatalf("expected 1, got %d", counter)
	}
}

func TestSubmitActionWithTimeout_Timeout(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	err := SubmitActionWithTimeout(pool, ctx, 10*time.Millisecond, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return nil
		}
	})
	if err != nil {
		t.Fatalf("SubmitActionWithTimeout submit error: %v", err)
	}
	pool.Wait()
	if pool.FailCount() == 0 {
		t.Fatal("expected at least 1 timeout failure")
	}
}

func TestSubmitAtActionWithTimeout(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	var executed bool
	err := SubmitAtActionWithTimeout(pool, 0, ctx, 500*time.Millisecond, func(ctx context.Context) error {
		executed = true
		return nil
	})
	if err != nil {
		t.Fatalf("SubmitAtActionWithTimeout error: %v", err)
	}
	pool.Wait()
	if !executed {
		t.Fatal("task not executed")
	}
}

func TestSubmitAtActionWithTimeout_Timeout(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	err := SubmitAtActionWithTimeout(pool, 0, ctx, 10*time.Millisecond, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return nil
		}
	})
	if err != nil {
		t.Fatalf("SubmitAtActionWithTimeout submit error: %v", err)
	}
	pool.Wait()
	if pool.FailCount() == 0 {
		t.Fatal("expected at least 1 timeout failure")
	}
}

func TestGoActionWithTimeout(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	var counter int32
	GoActionWithTimeout(pool, ctx, 500*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	pool.Wait()
	if atomic.LoadInt32(&counter) != 1 {
		t.Fatalf("expected 1, got %d", counter)
	}
}

func TestGoActionWithTimeout_Panic(t *testing.T) {
	ctx := context.Background()
	pool := NewNoResultPool(4)
	defer pool.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from GoActionWithTimeout when pool is closed")
		}
	}()
	pool.Close()
	pool.Wait()
	GoActionWithTimeout(pool, ctx, time.Second, func(ctx context.Context) error {
		return nil
	})
}

// ==================== DefaultMap 测试 ====================

func TestDefaultMap(t *testing.T) {
	results := DefaultMap(context.Background(), []int{1, 2, 3, 4, 5}, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	for i, r := range results {
		if r.Value != (i+1)*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, (i+1)*10, r.Value)
		}
	}
}

// ==================== DefaultMapWithFailFast 测试 ====================

func TestDefaultMapWithFailFast(t *testing.T) {
	results, err := DefaultMapWithFailFast(context.Background(), []int{1, 2, 3, 4, 5}, func(ctx context.Context, v int) (int, error) {
		if v == 3 {
			return 0, errTest
		}
		return v, nil
	})
	if err == nil {
		t.Fatal("expected error from DefaultMapWithFailFast")
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

// ==================== MapWithTimeout 测试 ====================

func TestMapWithTimeout(t *testing.T) {
	results := MapWithTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 500*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	for i, r := range results {
		if r.Value != (i+1)*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, (i+1)*10, r.Value)
		}
	}
}

func TestMapWithTimeout_Timeout(t *testing.T) {
	results := MapWithTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 10*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return v, nil
		}
	})
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	hasErr := false
	for _, r := range results {
		if r.Err != nil {
			hasErr = true
			break
		}
	}
	if !hasErr {
		t.Fatal("expected at least 1 timeout error")
	}
}

// ==================== DefaultMapWithTimeout 测试 ====================

func TestDefaultMapWithTimeout(t *testing.T) {
	results := DefaultMapWithTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 500*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	for i, r := range results {
		if r.Value != (i+1)*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, (i+1)*10, r.Value)
		}
	}
}

// ==================== MapWithFFTimeout 测试 ====================

func TestMapWithFFTimeout(t *testing.T) {
	results, err := MapWithFFTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 500*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	if err != nil {
		t.Fatalf("MapWithFFTimeout error: %v", err)
	}
	for i, r := range results {
		if r.Value != (i+1)*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, (i+1)*10, r.Value)
		}
	}
}

func TestMapWithFFTimeout_FailFast(t *testing.T) {
	results, err := MapWithFFTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 4, 500*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		if v == 3 {
			return 0, errTest
		}
		time.Sleep(50 * time.Millisecond)
		return v, nil
	})
	if err == nil {
		t.Fatal("expected error from MapWithFFTimeout")
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

// ==================== DefaultMapWithFFTimeout 测试 ====================

func TestDefaultMapWithFFTimeout(t *testing.T) {
	results, err := DefaultMapWithFFTimeout(context.Background(), []int{1, 2, 3, 4, 5}, 500*time.Millisecond, func(ctx context.Context, v int) (int, error) {
		return v * 10, nil
	})
	if err != nil {
		t.Fatalf("DefaultMapWithFFTimeout error: %v", err)
	}
	for i, r := range results {
		if r.Value != (i+1)*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, (i+1)*10, r.Value)
		}
	}
}
