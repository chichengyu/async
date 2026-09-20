package group

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
)

var errTest = errors.New("test error")

// ==================== Group 基本功能测试 ====================

func TestNewGroup_Defaults(t *testing.T) {
	g := NewGroup[int](5)
	if g.Concurrency() != 5 {
		t.Fatalf("expected concurrency 5, got %d", g.Concurrency())
	}
	if g.timeout != core.GetDefaultTimeout() {
		t.Fatalf("expected default timeout, got %v", g.timeout)
	}
}

func TestNewGroup_ZeroConcurrency(t *testing.T) {
	g := NewGroup[int](0)
	if g.Concurrency() != 1 {
		t.Fatalf("expected concurrency 1 for zero input, got %d", g.Concurrency())
	}
}

func TestNewGroup_NegativeConcurrency(t *testing.T) {
	g := NewGroup[int](-5)
	if g.Concurrency() != 1 {
		t.Fatalf("expected concurrency 1 for negative input, got %d", g.Concurrency())
	}
}

func TestDefaultGroup(t *testing.T) {
	g := DefaultGroup[int]()
	if g.Concurrency() <= 0 {
		t.Fatal("expected positive concurrency")
	}
}

func TestGroup_Go_Wait_Basic(t *testing.T) {
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

func TestGroup_ConcurrencyLimit(t *testing.T) {
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
	g.Wait()
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
		t.Fatal("expected HasError=true")
	}
	if g.FailCount() != 1 {
		t.Fatalf("expected 1 fail, got %d", g.FailCount())
	}
	if g.SuccessCount() != 9 {
		t.Fatalf("expected 9 success, got %d", g.SuccessCount())
	}
}

func TestGroup_FailFast(t *testing.T) {
	g, ctx := NewGroup[int](4).WithFailFast(context.Background())
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
		t.Logf("timeout results: %d", len(results))
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
		t.Logf("context timeout results: %d", len(results))
	}
}

func TestGroup_Reset(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](2)
	for i := 0; i < 5; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	}
	g.Wait()
	_, err := g.Reset()
	if err != nil {
		t.Fatalf("Reset error: %v", err)
	}
	for i := 0; i < 3; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	}
	results := g.Wait()
	if len(results) != 3 {
		t.Fatalf("expected 3 after reset, got %d", len(results))
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
	if len(g.Errors()) != 3 {
		t.Fatalf("expected 3 errors, got %d", len(g.Errors()))
	}
	if g.FirstError() == nil {
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
			return idx * 10, nil
		})
	}
	g.Wait()
	vals := g.Values()
	if len(vals) != 4 {
		t.Fatalf("expected 4 values, got %d", len(vals))
	}
}

func TestGroup_JoinErrors(t *testing.T) {
	g := NewGroup[int](2)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return 0, errTest
		})
	}
	g.Wait()
	if err := g.JoinErrors(); err == nil {
		t.Fatal("expected non-nil joined error")
	}
}

func TestGroup_Stats(t *testing.T) {
	g := NewGroup[int](4)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
	}
	stats := g.Stats()
	if stats.Concurrency != 4 {
		t.Fatalf("expected concurrency 4, got %d", stats.Concurrency)
	}
	g.Wait()
}

func TestGroup_ChainConfigs(t *testing.T) {
	ctx := context.Background()
	g, ctx := NewGroup[int](4).WithTraceID(ctx)
	if g == nil {
		t.Fatal("expected non-nil after WithTraceID")
	}

	g2, ctx2 := NewGroup[int](4).WithContext(ctx)
	if g2 != nil && ctx2 != nil {
	}
}

func TestGroup_WithTimeout(t *testing.T) {
	g := NewGroup[int](2).WithTimeout(100 * time.Millisecond)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return 1, nil
			}
		})
	}
	results := g.Wait()
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

func TestGroup_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](2)
	g.Go(ctx, func(ctx context.Context) (int, error) {
		panic("unexpected panic")
	})
	results := g.Wait()
	if results[0].Err == nil {
		t.Fatal("expected panic error")
	}
	if !results[0].IsPanic() {
		t.Fatal("expected IsPanic=true")
	}
}

// ==================== NoResult 测试 ====================

func TestNoResult_Basic(t *testing.T) {
	nr := NewNoResult(4)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		nr.Go(ctx, func(ctx context.Context) error { return nil })
	}
	nr.Wait()
	if nr.FailCount() != 0 {
		t.Fatalf("expected 0 failures, got %d", nr.FailCount())
	}
}

func TestNoResult_Errors(t *testing.T) {
	nr := NewNoResult(4)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		idx := i
		nr.Go(ctx, func(ctx context.Context) error {
			if idx%2 == 0 {
				return errTest
			}
			return nil
		})
	}
	nr.Wait()
	if nr.FailCount() != 3 {
		t.Fatalf("expected 3 failures, got %d", nr.FailCount())
	}
}

func TestNoResult_GoAt(t *testing.T) {
	nr := NewNoResult(4)
	ctx := context.Background()
	nr.GoAt(0, ctx, func(ctx context.Context) error { return errTest })
	nr.GoAt(1, ctx, func(ctx context.Context) error { return nil })
	nr.Wait()
}

func TestNoResult_WaitTimeout(t *testing.T) {
	nr := NewNoResult(2)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		nr.Go(ctx, func(ctx context.Context) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		})
	}
	_, ok := nr.WaitTimeout(50 * time.Millisecond)
	if ok {
		t.Fatal("expected timeout")
	}
}

func TestNoResult_Reset(t *testing.T) {
	nr := NewNoResult(2)
	ctx := context.Background()
	nr.Go(ctx, func(ctx context.Context) error { return nil })
	nr.Wait()
	_, err := nr.Reset()
	if err != nil {
		t.Fatalf("reset error: %v", err)
	}
	nr.Go(ctx, func(ctx context.Context) error { return nil })
	nr.Wait()
}

// ==================== 高并发极限压力测试 ====================

func TestGroup_50KGoRoutines(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](100)
	n := 50000
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
	t.Logf("50K Group Go+Wait: submitted=%d, results=%d", submittedVal, len(results))
}

func TestGroup_200KGoRoutines(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 200K test in short mode")
	}
	ctx := context.Background()
	g := NewGroup[int](200)
	n := 200000
	var submitted atomic.Int64
	var counter atomic.Int64
	for i := 0; i < n; i++ {
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return int(counter.Load()), nil
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
	if counter.Load() != int64(submittedVal) {
		t.Fatalf("expected counter=%d, got %d", submittedVal, counter.Load())
	}
	t.Logf("200K Group Go+Wait: submitted=%d, counter=%d", submittedVal, counter.Load())
}

func TestGroup_ConcurrentGoWait_200Goroutines(t *testing.T) {
	for round := 0; round < 5; round++ {
		ctx := context.Background()
		g := NewGroup[int](200)
		var submitWg sync.WaitGroup
		goroutines := 200
		tasksPerGoroutine := 250
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
			t.Fatalf("round %d: expected %d, got %d", round, expected, len(results))
		}
	}
}

func TestGroup_FailFast_50K(t *testing.T) {
	for round := 0; round < 5; round++ {
		g, ctx := NewGroup[int](100).WithFFSubmitTO(context.Background(), 2*time.Second)
		n := 50000
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
			t.Fatalf("round %d: expected %d, got %d", round, n, len(results))
		}
		if results[0].Err == nil {
			t.Fatalf("round %d: expected first task to fail", round)
		}
	}
}

func TestGroup_Concurrency1_10K(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](1)
	n := 10000
	for i := 0; i < n; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	results := g.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
}

func TestGroup_CPUBound_500Concurrent(t *testing.T) {
	for round := 0; round < 3; round++ {
		g := NewGroup[int](core.CPU())
		ctx := context.Background()
		n := 500
		for i := 0; i < n; i++ {
			idx := i
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				sum := 0
				for j := 0; j < 100000; j++ {
					sum += j
				}
				return sum + idx, nil
			})
		}
		results := g.Wait()
		if len(results) != n {
			t.Fatalf("round %d: expected %d, got %d", round, n, len(results))
		}
	}
}

func TestNoResult_50K(t *testing.T) {
	nr := NewNoResult(200)
	ctx := context.Background()
	n := 50000
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

func TestGroup_ResetAndReuse_10K(t *testing.T) {
	g := NewGroup[int](50)
	ctx := context.Background()
	rounds := 10
	tasksPerRound := 1000
	for r := 0; r < rounds; r++ {
		for i := 0; i < tasksPerRound; i++ {
			g.Go(ctx, func(ctx context.Context) (int, error) {
				return r, nil
			})
		}
		results := g.Wait()
		if len(results) != tasksPerRound {
			t.Fatalf("round %d: expected %d, got %d", r, tasksPerRound, len(results))
		}
		if r < rounds-1 {
			_, err := g.Reset()
			if err != nil {
				t.Fatalf("round %d reset error: %v", r, err)
			}
		}
	}
}

func TestGroup_ConcurrentReset_Stress(t *testing.T) {
	var wg sync.WaitGroup
	errCh := make(chan error, 50)
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func(round int) {
			defer wg.Done()
			g := NewGroup[int](20)
			ctx := context.Background()
			for j := 0; j < 100; j++ {
				g.Go(ctx, func(ctx context.Context) (int, error) { return j, nil })
			}
			results := g.Wait()
			if len(results) != 100 {
				errCh <- fmt.Errorf("round %d: expected 100, got %d", round, len(results))
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestGroup_RaceBetweenGoAndWait(t *testing.T) {
	for round := 0; round < 100; round++ {
		g := NewGroup[int](10)
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				g.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			g.Wait()
		}()
		wg.Wait()
	}
}
