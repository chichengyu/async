package group

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
	"github.com/chichengyu/async/internal/pool"
)

func init() {
	core.SetDefaultMaxResults(100000)
}

// ==================== NewGroup / DefaultGroup ====================

func TestNewGroup_Defaults(t *testing.T) {
	g := NewGroup[int](5)
	defer g.Close()
	if g.Worker() != 5 {
		t.Fatalf("expected concurrency 5, got %d", g.Worker())
	}
}

func TestNewGroup_ZeroConcurrency(t *testing.T) {
	g := NewGroup[int](0)
	defer g.Close()
	if g.Worker() != 1 {
		t.Fatalf("expected concurrency 1, got %d", g.Worker())
	}
}

func TestNewGroup_NegativeConcurrency(t *testing.T) {
	g := NewGroup[int](-5)
	defer g.Close()
	if g.Worker() != 1 {
		t.Fatalf("expected concurrency 1 for -5, got %d", g.Worker())
	}
}

func TestDefaultGroup(t *testing.T) {
	g := DefaultGroup[int]()
	defer g.Close()
	if g.Worker() != core.IO() {
		t.Fatalf("DefaultGroup Worker = %d, want %d", g.Worker(), core.IO())
	}
}

// ==================== Go - Basic Scenarios ====================

func TestGo_NormalExecution(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			return 42, nil
		})
		if err != nil {
			t.Errorf("Go(%d) error: %v", i, err)
		}
	}

	results := g.Wait()
	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Value != 42 || !r.Ok() {
			t.Errorf("unexpected result: %+v", r)
		}
	}
}

func TestGo_ErrorExecution(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	testErr := errors.New("intentional error")
	for i := 0; i < 10; i++ {
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			return 0, testErr
		})
		if err != nil {
			t.Errorf("Go(%d) submit error: %v", i, err)
		}
	}

	results := g.Wait()
	if g.FailCount() != 10 {
		t.Errorf("FailCount = %d, want 10", g.FailCount())
	}
	for _, r := range results {
		if r.Ok() {
			t.Errorf("expected error result, got %+v", r)
		}
	}
}

func TestGo_PanicRecovery(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		panic("boom")
	})
	if err != nil {
		t.Errorf("Go submit error: %v", err)
	}

	results := g.Wait()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].IsPanic() {
		t.Error("expected panic error")
	}
}

func TestGo_PanicWithErrorType(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		panic(errors.New("panic error"))
	})
	if err != nil {
		t.Errorf("Go submit error: %v", err)
	}

	results := g.Wait()
	if !results[0].IsPanic() {
		t.Error("expected panic result")
	}
}

func TestGo_NonDefaultResults(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		val := i
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			return val * 2, nil
		})
		if err != nil {
			t.Errorf("Go(%d) error: %v", i, err)
		}
	}

	results := g.Wait()
	if g.TotalCount() != 50 {
		t.Errorf("TotalCount = %d, want 50", g.TotalCount())
	}
	if g.SuccessCount() != 50 {
		t.Errorf("SuccessCount = %d, want 50", g.SuccessCount())
	}
	if g.FailCount() != 0 {
		t.Errorf("FailCount = %d, want 0", g.FailCount())
	}

	vals := make(map[int]bool)
	for _, r := range results {
		vals[r.Value] = true
	}
	for i := 0; i < 50; i++ {
		if !vals[i*2] {
			t.Errorf("missing result value: %d", i*2)
		}
	}
}

// ==================== Go - Context Cancel ====================

func TestGo_CanceledContext(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	})
	if err == nil {
		t.Error("Go with cancelled context should return error")
	}
}

func TestGo_TimeoutContext(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	})
	if err == nil {
		t.Error("Go with expired context should return error")
	}
}

// ==================== GoAt ====================

func TestGoAt_Basic(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.GoAt(0, ctx, func(ctx context.Context) (int, error) {
		return 100, nil
	})
	if err != nil {
		t.Errorf("GoAt(0): %v", err)
	}
	err = g.GoAt(1, ctx, func(ctx context.Context) (int, error) {
		return 200, nil
	})
	if err != nil {
		t.Errorf("GoAt(1): %v", err)
	}

	results := g.Wait()
	if len(results) < 2 {
		t.Fatalf("expected >=2 results, got %d", len(results))
	}
	found100, found200 := false, false
	for _, r := range results {
		if r.Value == 100 {
			found100 = true
		}
		if r.Value == 200 {
			found200 = true
		}
	}
	if !found100 || !found200 {
		t.Error("GoAt results missing expected values")
	}
}

// ==================== GoWithTimeout ====================

func TestGoWithTimeout(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.GoWithTimeout(ctx, 50*time.Millisecond, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})
	if err != nil {
		t.Errorf("GoWithTimeout submit error: %v", err)
	}

	results := g.Wait()
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0].Err == nil {
		t.Error("expected timeout error")
	}
}

// ==================== Go - After Wait ====================

func TestGo_AfterWait(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	g.Wait()

	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		return 2, nil
	})
	if !errors.Is(err, core.ErrGroupWaited) {
		t.Errorf("expected ErrGroupWaited after Wait, got %v", err)
	}
}

// ==================== Wait - Timeout / Context ====================

func TestWait_Timeout(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return 1, nil
		})
	}

	results, completed := g.WaitTimeout(50 * time.Millisecond)
	if completed {
		t.Error("expected completed=false (timeout)")
	}
	_ = results
}

func TestWait_ContextCancel(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return 1, nil
		})
	}

	waitCtx, waitCancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		waitCancel()
	}()
	results, completed := g.WaitContext(waitCtx)
	if completed {
		t.Error("expected completed=false on context cancel")
	}
	_ = results
}

func TestWait_Twice(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})

	results1 := g.Wait()
	results2 := g.Wait()
	if len(results1) != 1 || len(results2) != 1 {
		t.Errorf("Wait twice: len1=%d len2=%d", len(results1), len(results2))
	}
}

// ==================== WaitTimeout / WaitContext ====================

func TestWaitTimeout_CompletesBeforeTimeout(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return 100, nil
		})
	}

	results, completed := g.WaitTimeout(10 * time.Second)
	if !completed {
		t.Error("WaitTimeout should complete (ok=true)")
	}
	if len(results) != 20 {
		t.Errorf("WaitTimeout len = %d, want 20", len(results))
	}
}

func TestWaitTimeout_Empty(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	results, completed := g.WaitTimeout(time.Second)
	if !completed {
		t.Error("WaitTimeout on empty should return ok=true")
	}
	if len(results) != 0 {
		t.Errorf("WaitTimeout on empty: len=%d, want 0", len(results))
	}
}

func TestWaitContext_NormalComplete(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) { return 99, nil })
	results, completed := g.WaitContext(context.Background())
	if !completed {
		t.Error("WaitContext should complete (ok=true)")
	}
	if len(results) != 1 || results[0].Value != 99 {
		t.Errorf("WaitContext results: %+v", results)
	}
}

// ==================== FailFast ====================

func TestFailFast_Basic(t *testing.T) {
	g := NewGroup[int](4)
	ctx := g.ctx
	g, ctx = g.WithFailFast(ctx)
	defer g.Close()

	testErr := errors.New("fail fast error")
	var executed atomic.Int64
	for i := 0; i < 50; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			executed.Add(1)
			return 0, testErr
		})
	}

	results := g.Wait()
	if g.FailCount() < 1 {
		t.Error("should have failures")
	}
	_ = results
	t.Logf("executed: %d, results: %d", executed.Load(), len(results))
}

// ==================== WithTimeout (Group) ====================

func TestWithTimeout_Group(t *testing.T) {
	g := NewGroup[int](4)
	g.WithTimeout(50 * time.Millisecond)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})

	results := g.Wait()
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0].Err == nil {
		t.Error("expected timeout error")
	}
}

// ==================== Worker / Active / Busy ====================

func TestWorker_Active_Busy(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()

	if g.Worker() != 4 {
		t.Errorf("Worker = %d, want 4", g.Worker())
	}
	if g.Busy() != 0 {
		t.Errorf("Busy (idle) = %d, want 0", g.Busy())
	}
}

func TestStats(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	g.Go(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err") })
	g.Wait()

	stats := g.Stats()
	if stats.TotalTask != 2 {
		t.Errorf("TotalTask = %d, want 2", stats.TotalTask)
	}
	if stats.SuccessTask != 1 {
		t.Errorf("SuccessTask = %d, want 1", stats.SuccessTask)
	}
	if stats.FailTask != 1 {
		t.Errorf("FailTask = %d, want 1", stats.FailTask)
	}
}

// ==================== Results Accessors ====================

func TestErrors_HasError(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	testErr := errors.New("my error")
	g.Go(ctx, func(ctx context.Context) (int, error) { return 0, testErr })
	g.Go(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	g.Wait()

	errs := g.Errors()
	if len(errs) != 1 {
		t.Errorf("Errors len = %d, want 1", len(errs))
	}
	if errs[0] != testErr {
		t.Errorf("Errors[0] = %v, want %v", errs[0], testErr)
	}
	if !g.HasError() {
		t.Error("HasError should be true")
	}
}

func TestFirstError(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	testErr := errors.New("first error")
	g.Go(ctx, func(ctx context.Context) (int, error) { return 0, testErr })
	g.Wait()

	err := g.FirstError()
	if err == nil {
		t.Error("FirstError should return non-nil")
	}
	t.Logf("FirstError = %v", err)
}

func TestJoinErrors(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	g.Go(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	g.Wait()

	if err := g.JoinErrors(); err != nil {
		t.Errorf("JoinErrors with no errors should return nil, got: %v", err)
	}

	g2 := NewGroup[int](4)
	defer g2.Close()
	g2.Go(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err1") })
	g2.Go(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err2") })
	g2.Wait()

	err := g2.JoinErrors()
	if err == nil {
		t.Error("JoinErrors should return non-nil")
	}
}

func TestValues(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		val := i
		g.Go(ctx, func(ctx context.Context) (int, error) { return val * 10, nil })
	}
	g.Wait()

	vals := g.Values()
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

// ==================== Reset ====================

func TestReset(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	g.Wait()

	g2, err := g.Reset()
	if err != nil {
		t.Errorf("Reset error: %v", err)
	}

	g2.Go(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	results := g2.Wait()
	if len(results) != 1 || results[0].Value != 2 {
		t.Errorf("after Reset: %+v", results)
	}
	g2.Close()
}

// ==================== Shard / MultiGroup ====================

func TestShard(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	mg := g.Shard(4)
	defer mg.Close()

	var counter atomic.Int64
	for i := 0; i < 100; i++ {
		err := mg.Go(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
		if err != nil {
			t.Errorf("Shard.Go(%d): %v", i, err)
		}
	}
	results := mg.Wait()
	if counter.Load() != 100 {
		t.Errorf("counter = %d, want 100", counter.Load())
	}
	_ = len(results)
}

func TestDefaultShard(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	mg := g.DefaultShard()
	defer mg.Close()

	mg.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	results := mg.Wait()
	if len(results) != 1 || results[0].Value != 1 {
		t.Errorf("DefaultShard: %+v", results)
	}
}

func TestMultiGroup_WithTimeout(t *testing.T) {
	g := NewGroup[int](4)
	ctx := g.ctx
	mg := g.DefaultShard()
	mg.WithTimeout(50 * time.Millisecond)
	defer mg.Close()

	mg.Go(ctx, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})
	results := mg.Wait()
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0].Err == nil {
		t.Error("expected timeout error")
	}
}

func TestMultiGroup_Stats(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.DefaultShard()
	defer mg.Close()
	ctx := context.Background()

	mg.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	mg.Wait()

	if mg.TotalWorker() == 0 {
		t.Error("TotalWorker should be >0")
	}
	if mg.TotalTaskCount() != 1 {
		t.Errorf("TotalTaskCount = %d, want 1", mg.TotalTaskCount())
	}
	if mg.Errors() != nil {
		t.Errorf("Errors = %v, want nil", mg.Errors())
	}
	vals := mg.Values()
	if len(vals) != 1 || vals[0] != 1 {
		t.Errorf("Values = %v, want [1]", vals)
	}
}

func TestMultiGroup_GoKeyed(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.DefaultShard()
	defer mg.Close()
	ctx := context.Background()

	err := mg.GoKeyed(42, ctx, func(ctx context.Context) (int, error) {
		return 100, nil
	})
	if err != nil {
		t.Errorf("GoKeyed: %v", err)
	}
	results := mg.Wait()
	if len(results) != 1 || results[0].Value != 100 {
		t.Errorf("GoKeyed results: %+v", results)
	}
}

// ==================== StreamResults ====================

func TestStreamResults(t *testing.T) {
	g := NewGroup[int](4)
	g.WithStreaming(200)
	defer g.Close()
	ctx := context.Background()

	n := 50
	for i := 0; i < n; i++ {
		val := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return val, nil
		})
	}

	ch := g.StreamResults()
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

	g.Wait()
	wg.Wait()

	sort.Ints(collected)
	if len(collected) != n {
		t.Errorf("StreamResults collected = %d, want %d", len(collected), n)
	}
}

func TestResultCallback(t *testing.T) {
	var collected []int
	var mu sync.Mutex
	g := NewGroup[int](4)
	g.WithResultCallback(func(r core.Result[int]) {
		mu.Lock()
		collected = append(collected, r.Value)
		mu.Unlock()
	})
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		val := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return val, nil
		})
	}
	g.Wait()

	sort.Ints(collected)
	if len(collected) != 20 {
		t.Errorf("ResultCallback collected = %d, want 20", len(collected))
	}
}

// ==================== Builder ====================

func TestBuilder_Basic(t *testing.T) {
	g := NewGroupBuilder[int]().
		Worker(4).
		Timeout(10 * time.Second).
		FailFast().
		Build()
	defer g.Close()

	ctx := context.Background()
	g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	results := g.Wait()
	if len(results) != 1 || results[0].Value != 1 {
		t.Errorf("Builder basic: results = %+v", results)
	}
}

func TestBuilder_Run(t *testing.T) {
	err := NewGroupBuilder[int]().
		Worker(4).
		Timeout(10 * time.Second).
		Run(func(ctx context.Context, g *Group[int]) error {
			g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
			results := g.Wait()
			if len(results) != 1 || results[0].Value != 1 {
				t.Errorf("Run results: %+v", results)
			}
			return nil
		})
	if err != nil {
		t.Errorf("Run error: %v", err)
	}
}

func TestBuilder_DefaultWorker(t *testing.T) {
	g := NewGroupBuilder[int]().
		DefaultWorker().
		Build()
	defer g.Close()

	if g.Worker() != core.IO() {
		t.Errorf("DefaultWorker = %d, want %d", g.Worker(), core.IO())
	}
}

func TestBuilder_Streaming(t *testing.T) {
	g := NewGroupBuilder[int]().
		Worker(2).
		Streaming(10).
		Build()
	defer g.Close()
	ctx := context.Background()

	ch := g.StreamResults()
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

	for i := 0; i < 10; i++ {
		v := i
		g.Go(ctx, func(ctx context.Context) (int, error) { return v, nil })
	}
	g.Wait()
	wg.Wait()

	sort.Ints(collected)
	if len(collected) != 10 {
		t.Errorf("Streaming collected = %d, want 10", len(collected))
	}
}

// ==================== NoResult ====================

func TestNoResult_Basic(t *testing.T) {
	g := NewNoResult(4)
	defer g.Close()
	ctx := context.Background()

	var counter atomic.Int64
	for i := 0; i < 20; i++ {
		err := g.Go(ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		if err != nil {
			t.Errorf("NoResult Go(%d): %v", i, err)
		}
	}
	g.Wait()
	if counter.Load() != 20 {
		t.Errorf("counter = %d, want 20", counter.Load())
	}
}

func TestNoResult_Error(t *testing.T) {
	g := NewNoResult(4)
	defer g.Close()
	ctx := context.Background()

	testErr := errors.New("no result error")
	g.Go(ctx, func(ctx context.Context) error {
		return testErr
	})
	g.Wait()

	if !g.HasError() {
		t.Error("HasError should be true")
	}
	errs := g.Errors()
	if len(errs) != 1 || errs[0] != testErr {
		t.Errorf("NoResult errors: %v", errs)
	}
}

func TestNoResult_Panic(t *testing.T) {
	g := NewNoResult(4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) error {
		panic("no result panic")
	})
	g.Wait()
	if g.FailCount() != 1 {
		t.Errorf("FailCount = %d, want 1 (panic counts as failure)", g.FailCount())
	}
}

func TestNoResult_FailFast(t *testing.T) {
	g := NewNoResult(4)
	ctx := g.ctx
	g, ctx = g.WithFailFast(ctx)
	defer g.Close()

	var executed atomic.Int64
	for i := 0; i < 20; i++ {
		g.Go(ctx, func(ctx context.Context) error {
			executed.Add(1)
			return errors.New("fail")
		})
	}
	g.Wait()
	_ = executed.Load()
}

func TestNoResult_AfterWait(t *testing.T) {
	g := NewNoResult(4)
	defer g.Close()
	g.Wait()

	err := g.Go(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, core.ErrGroupWaited) {
		t.Errorf("expected ErrGroupWaited, got: %v", err)
	}
}

func TestDefaultNoResult(t *testing.T) {
	g := DefaultNoResult()
	defer g.Close()
	if g.Worker() != core.IO() {
		t.Errorf("DefaultNoResult Worker = %d, want %d", g.Worker(), core.IO())
	}
}

func TestNoResult_WithTimeout(t *testing.T) {
	g := NewNoResult(4)
	g.WithTimeout(50 * time.Millisecond)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return nil
		}
	})
	g.Wait()
	if g.FailCount() != 1 {
		t.Errorf("FailCount = %d, want 1 (timeout)", g.FailCount())
	}
}

// ==================== GroupNoResultBuilder ====================

func TestGroupNoResultBuilder(t *testing.T) {
	g := NewGroupNoResultBuilder().
		Worker(4).
		Timeout(10 * time.Second).
		Build()
	defer g.Close()

	ctx := context.Background()
	g.Go(ctx, func(ctx context.Context) error { return nil })
	g.Wait()
	if g.HasError() {
		t.Error("should have no errors")
	}
}

// ==================== WithTraceID ====================

func TestWithTraceID(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := g.ctx
	g, ctx = g.WithTraceID(ctx)

	traceID := core.GetTraceID(ctx)
	if traceID == "" {
		t.Error("WithTraceID should ensure trace_id in context")
	}
	if len(traceID) != 32 {
		t.Errorf("trace_id length = %d, want 32", len(traceID))
	}
}

func TestWithContext_Group(t *testing.T) {
	g := NewGroup[int](4)
	ctx := g.ctx
	g, ctx = g.WithContext(ctx)
	defer g.Close()

	g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	results := g.Wait()
	if len(results) != 1 {
		t.Errorf("WithContext results: %+v", results)
	}
}

// ==================== AutoScale ====================

func TestAutoScale(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()

	cfg := &core.AutoScaleConfig{
		MinWorkers:         2,
		MaxWorkers:         8,
		CheckInterval:      100 * time.Millisecond,
		ScaleUpThreshold:   0.8,
		ScaleDownThreshold: 0.2,
	}
	g.EnableAutoScale(cfg)
	if !g.IsAutoScaleEnabled() {
		t.Error("IsAutoScaleEnabled should be true")
	}

	g.DisableAutoScale()
	if g.IsAutoScaleEnabled() {
		t.Error("IsAutoScaleEnabled should be false after DisableAutoScale")
	}
}

// ==================== WithSubmitTimeout ====================

func TestWithSubmitTimeout(t *testing.T) {
	g := NewGroup[int](1)
	g.WithSubmitTimeout(10 * time.Millisecond)
	defer g.Close()
	ctx := context.Background()

	// Fill the only worker
	g.Go(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 1, nil
	})

	// Second task should timeout waiting for slot
	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		return 2, nil
	})
	if err != nil {
		t.Logf("submit timeout expected: %v", err)
	}
	g.Wait()
}

// ==================== GoAtWithTimeout ====================

func TestGoAtWithTimeout(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.GoAtWithTimeout(0, ctx, 30*time.Millisecond, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})
	if err != nil {
		t.Errorf("GoAtWithTimeout submit: %v", err)
	}
	results := g.Wait()
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0].Err == nil {
		t.Error("expected timeout error")
	}
}

// ==================== Production Concurrent Tests ====================

func TestProduction_MixedResults(t *testing.T) {
	g := NewGroup[int](runtime.NumCPU() * 2)
	defer g.Close()
	ctx := context.Background()

	var successCount, errorCount, panicCount atomic.Int64
	n := 1000

	for i := 0; i < n; i++ {
		mod := i % 5
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
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
			t.Errorf("Go(%d) submit error: %v", i, err)
		}
	}

	results := g.Wait()
	if len(results) != n {
		t.Errorf("results len = %d, want %d", len(results), n)
	}
	if g.SuccessCount() != successCount.Load() {
		t.Errorf("SuccessCount = %d, want %d", g.SuccessCount(), successCount.Load())
	}
	if g.FailCount() != errorCount.Load()+panicCount.Load() {
		t.Errorf("FailCount = %d, want %d (err+panic)", g.FailCount(), errorCount.Load()+panicCount.Load())
	}

	panicResults := 0
	for _, r := range results {
		if r.IsPanic() {
			panicResults++
		}
	}
	if panicResults != int(panicCount.Load()) {
		t.Errorf("panic results = %d, want %d", panicResults, panicCount.Load())
	}
}

func TestProduction_HighConcurrency(t *testing.T) {
	g := NewGroup[int](runtime.NumCPU() * 4)
	defer g.Close()
	ctx := context.Background()

	n := 5000
	var counter atomic.Int64

	for i := 0; i < n; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
	}

	results := g.Wait()
	if len(results) != n {
		t.Errorf("results len = %d, want %d", len(results), n)
	}
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
}

func TestProduction_FailFastRace(t *testing.T) {
	g := NewGroup[int](runtime.NumCPU() * 2)
	ctx := g.ctx
	g, ctx = g.WithFailFast(ctx)
	defer g.Close()

	n := 500
	var started atomic.Int64

	for i := 0; i < n; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			started.Add(1)
			return 0, errors.New("fail")
		})
	}

	g.Wait()
	if started.Load() < int64(n) {
		t.Logf("FailFast stopped at %d/%d", started.Load(), n)
	}
}

func TestProduction_NoResultHighConcurrency(t *testing.T) {
	g := NewNoResult(runtime.NumCPU() * 4)
	defer g.Close()
	ctx := context.Background()

	n := 5000
	var counter atomic.Int64

	for i := 0; i < n; i++ {
		g.Go(ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	g.Wait()
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
	if g.SuccessCount() != int64(n) {
		t.Errorf("SuccessCount = %d, want %d", g.SuccessCount(), n)
	}
}

func TestProduction_ContextCancelDuringExecution(t *testing.T) {
	g := NewGroup[int](runtime.NumCPU())
	defer g.Close()

	pCtx, pCancel := context.WithCancel(context.Background())
	g, ctx := g.WithContext(pCtx)
	_ = ctx

	n := 500
	var started, completed atomic.Int64

	for i := 0; i < n; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			started.Add(1)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(100 * time.Millisecond):
				completed.Add(1)
				return 1, nil
			}
		})
	}

	time.Sleep(20 * time.Millisecond)
	pCancel()
	g.Wait()

	t.Logf("started=%d completed=%d", started.Load(), completed.Load())
}

func TestProduction_BuilderRace(t *testing.T) {
	var wg sync.WaitGroup
	n := 50

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g := NewGroupBuilder[int]().
				Worker(runtime.NumCPU()).
				Timeout(10 * time.Second).
				Build()
			defer g.Close()

			ctx := context.Background()
			g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
			g.Wait()
		}()
	}
	wg.Wait()
}

func TestProduction_MultiGroupRace(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.DefaultShard()
	defer mg.Close()
	ctx := context.Background()

	n := 2000
	var counter atomic.Int64

	for i := 0; i < n; i++ {
		mg.Go(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
	}

	results := mg.Wait()
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
	if len(results) != n {
		t.Errorf("results len = %d, want %d", len(results), n)
	}
}

func TestProduction_ValuesRace(t *testing.T) {
	g := NewGroup[int](runtime.NumCPU() * 2)
	defer g.Close()
	ctx := context.Background()

	n := 1000
	for i := 0; i < n; i++ {
		val := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			if val%2 == 0 {
				return val, nil
			}
			return 0, errors.New("odd")
		})
	}

	g.Wait()
	vals := g.Values()
	// Should only contain successful results
	if len(vals) != n/2 {
		t.Errorf("Values len = %d, want %d", len(vals), n/2)
	}
	sort.Ints(vals)
	for i, v := range vals {
		if v != i*2 {
			t.Errorf("Values[%d] = %d, want %d", i, v, i*2)
		}
	}
}

// ==================== Closed Pool 不死锁 ====================

func TestProduction_GroupClosedPool_NoDeadlock(t *testing.T) {
	p := pool.NewPool[int](16)
	p.Close()

	ctx := context.Background()
	b := NewGroupBuilder[int]()
	b.Context(ctx)
	b.Worker(2)
	b.Pool(p)
	g := b.Build()

	for i := 0; i < 10; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	done := make(chan struct{})
	go func() {
		g.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait timed out after 5s — possible deadlock")
	}
}

func TestProduction_GroupSubmitError_Consistency(t *testing.T) {
	p := pool.NewPool[int](16)
	p.Close()

	ctx := context.Background()
	b := NewGroupBuilder[int]()
	b.Context(ctx)
	b.Worker(4)
	b.Pool(p)
	g := b.Build()

	taskCount := 200
	var submitErrCount int64
	var wg sync.WaitGroup

	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := g.Go(ctx, func(ctx context.Context) (int, error) {
				return idx, nil
			}); err != nil {
				atomic.AddInt64(&submitErrCount, 1)
			}
		}(i)
	}
	wg.Wait()

	results := g.Wait()
	failCnt := g.FailCount()

	if failCnt == 0 {
		t.Error("expected some failures when using closed pool")
	}
	if len(results) < 1 {
		t.Error("expected results to be populated even on failure")
	}
	_ = submitErrCount
}
