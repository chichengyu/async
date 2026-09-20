package pool

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

var errPoolTest = errors.New("pool test error")

// ==================== Pool 基本功能测试 ====================

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

func TestPool_ChainConfigs(t *testing.T) {
	ctx := context.Background()
	p, _ := NewPool[int](4).WithTraceID(ctx)
	if p == nil {
		t.Fatal("expected non-nil after chain")
	}
	defer p.Close()
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

// ==================== NoResultPool 测试 ====================

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

// ==================== 高并发极限压力测试 ====================

func TestPool_50K_SubmitWait(t *testing.T) {
	p := NewPool[int](50)
	defer p.Close()
	ctx := context.Background()
	n := 50000
	var submitted atomic.Int64
	for i := 0; i < n; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}
	results := p.Wait()
	expected := int(submitted.Load())
	if len(results) != expected {
		t.Fatalf("expected %d, got %d", expected, len(results))
	}
}

func TestPool_200K_SubmitWait(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 200K in short mode")
	}
	p := NewPool[int](200)
	defer p.Close()
	ctx := context.Background()
	n := 200000
	var submitted atomic.Int64
	var counter atomic.Int64
	for i := 0; i < n; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return int(counter.Load()), nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}
	results := p.Wait()
	expected := int(submitted.Load())
	if len(results) != expected {
		t.Fatalf("expected %d, got %d", expected, len(results))
	}
	if counter.Load() != int64(expected) {
		t.Fatalf("expected counter=%d, got %d", expected, counter.Load())
	}
}

func TestPool_ConcurrentSubmit_300Goroutines(t *testing.T) {
	for round := 0; round < 5; round++ {
		p := NewPool[int](100)
		ctx := context.Background()
		var submitWg sync.WaitGroup
		goroutines := 300
		tasksPerGoroutine := 167
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
		expected := int(totalSubmitted.Load())
		if len(results) != expected {
			t.Fatalf("round %d: expected %d, got %d", round, expected, len(results))
		}
		p.Close()
		t.Logf("round %d: %d tasks, %d results", round, expected, len(results))
	}
}

func TestPool_FailFast_50K(t *testing.T) {
	for round := 0; round < 5; round++ {
		p, ctx := NewPool[int](100).WithFFSubmitTO(context.Background(), 2*time.Second)
		n := 50000
		for i := 0; i < n; i++ {
			idx := i
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				if idx == 0 {
					return 0, errors.New("trigger fail fast")
				}
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-time.After(time.Millisecond * 100):
					return idx, nil
				}
			})
		}
		results := p.Wait()
		if len(results) != n {
			t.Fatalf("round %d: expected %d, got %d", round, n, len(results))
		}
		if results[0].Err == nil {
			t.Fatalf("round %d: expected first task to fail", round)
		}
		p.Close()
	}
}

func TestPool_SubmitAt_LargeIndices_10K(t *testing.T) {
	p := NewPool[int](20)
	defer p.Close()
	ctx := context.Background()
	n := 10000
	for i := 0; i < n; i++ {
		idx := i
		_ = p.SubmitAt(idx, ctx, func(ctx context.Context) (int, error) {
			if idx%1000 == 0 {
				return 0, fmt.Errorf("err at %d", idx)
			}
			return idx, nil
		})
	}
	results := p.Wait()
	if len(results) != n {
		t.Fatalf("expected %d, got %d", n, len(results))
	}
	errCount := 0
	for _, r := range results {
		if r.Err != nil {
			errCount++
		}
	}
	if errCount == 0 {
		t.Fatal("expected some errors")
	}
}

func TestPool_CPUBound_500Tasks(t *testing.T) {
	for round := 0; round < 5; round++ {
		p := NewPool[int](core.CPU() * 4)
		ctx := context.Background()
		n := 500
		for i := 0; i < n; i++ {
			idx := i
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				sum := 0
				for j := 0; j < 200000; j++ {
					sum += j
				}
				return sum + idx, nil
			})
		}
		results := p.Wait()
		if len(results) != n {
			t.Fatalf("round %d: expected %d, got %d", round, n, len(results))
		}
		p.Close()
	}
}

func TestNoResultPool_100K(t *testing.T) {
	nr := NewPool[struct{}](100)
	defer nr.Close()
	ctx := context.Background()
	n := 100000
	var counter atomic.Int64
	for i := 0; i < n; i++ {
		_ = nr.Submit(ctx, func(ctx context.Context) (struct{}, error) {
			counter.Add(1)
			return struct{}{}, nil
		})
	}
	nr.Wait()
	if counter.Load() != int64(n) {
		t.Fatalf("expected %d, got %d", n, counter.Load())
	}
}

func TestPool_MixedSuccessAndError_50K(t *testing.T) {
	p := NewPool[int](50)
	defer p.Close()
	ctx := context.Background()
	n := 50000
	var success, fail atomic.Int64
	for i := 0; i < n; i++ {
		idx := i
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			if idx%10 == 0 {
				return 0, errPoolTest
			}
			return idx, nil
		})
	}
	results := p.Wait()
	for _, r := range results {
		if r.Ok() {
			success.Add(1)
		} else {
			fail.Add(1)
		}
	}
	if success.Load()+fail.Load() != int64(n) {
		t.Fatalf("total mismatch: %d+%d != %d", success.Load(), fail.Load(), n)
	}
}

func TestPool_CloseRace_SubmitAndClose(t *testing.T) {
	for round := 0; round < 100; round++ {
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

func TestPool_TaskFullBufferHandling_100K(t *testing.T) {
	p := NewPool[int](10)
	defer p.Close()
	ctx := context.Background()
	n := 100000
	var submitted atomic.Int64
	var rejected atomic.Int64
	for i := 0; i < n; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		if err == nil {
			submitted.Add(1)
		} else {
			rejected.Add(1)
		}
	}
	results := p.Wait()
	if len(results) != int(submitted.Load()) {
		t.Fatalf("submitted %d, got %d", submitted.Load(), len(results))
	}
	t.Logf("100K: submitted=%d, rejected=%d", submitted.Load(), rejected.Load())
}

func TestPool_Concurrency1_10K(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	ctx := context.Background()
	n := 10000
	for i := 0; i < n; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	}
	results := p.Wait()
	if len(results) != n {
		t.Fatalf("expected %d, got %d", n, len(results))
	}
}
