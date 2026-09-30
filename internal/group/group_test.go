package group

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

var errTest = errors.New("test error")

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
// 一、Group 基本功能测试
// ============================================================

func TestNewGroup_Defaults(t *testing.T) {
	g := NewGroup[int](5)
	if g.Worker() != 5 {
		t.Fatalf("expected concurrency 5, got %d", g.Worker())
	}
	if g.timeout != core.GetDefaultTimeout() {
		t.Fatalf("expected default timeout, got %v", g.timeout)
	}
}

func TestNewGroup_ZeroConcurrency(t *testing.T) {
	g := NewGroup[int](0)
	if g.Worker() != 1 {
		t.Fatalf("expected concurrency 1 for zero input, got %d", g.Worker())
	}
}

func TestNewGroup_NegativeConcurrency(t *testing.T) {
	g := NewGroup[int](-5)
	if g.Worker() != 1 {
		t.Fatalf("expected concurrency 1 for negative input, got %d", g.Worker())
	}
}

func TestDefaultGroup(t *testing.T) {
	g := DefaultGroup[int]()
	if g.Worker() <= 0 {
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

// ============================================================
// 二、NoResult 测试
// ============================================================

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

// ============================================================
// 三、Group 流式消费测试
// ============================================================

func TestGroup_WithStreaming_Basic(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)
	g.WithStreaming(128)

	ch := g.StreamResults()
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

	for i := 0; i < 100; i++ {
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
		if err != nil {
			t.Fatalf("Go error: %v", err)
		}
	}

	results := g.Wait()
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

func TestGroup_WithStreaming_DefaultBufSize(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](8)
	g.WithStreaming(0)

	for i := 0; i < 50; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	g.Wait()
}

func TestGroup_WithResultCallback_Basic(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)

	var cbCount atomic.Int64
	g.WithResultCallback(func(r core.Result[int]) {
		cbCount.Add(1)
	})

	for i := 0; i < 100; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	g.Wait()

	if c := cbCount.Load(); c != 100 {
		t.Fatalf("callback count: expected 100, got %d", c)
	}
}

func TestGroup_WithResultCallback_Errors(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)

	var errCount atomic.Int64
	g.WithResultCallback(func(r core.Result[int]) {
		if r.Err != nil {
			errCount.Add(1)
		}
	})

	for i := 0; i < 50; i++ {
		idx := i
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			if idx%2 == 0 {
				return 0, errTest
			}
			return idx, nil
		})
	}
	g.Wait()

	if c := errCount.Load(); c != 25 {
		t.Fatalf("error callback count: expected 25, got %d", c)
	}
}

func TestGroup_WithStreaming_NoDeadlock_NoConsumer(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)
	g.WithStreaming(10)

	for i := 0; i < 10000; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	done := make(chan struct{})
	go func() {
		results := g.Wait()
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

func TestGroup_Streaming_WithGoAt(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)
	g.WithStreaming(64)

	var cbCount atomic.Int64
	g.WithResultCallback(func(r core.Result[int]) {
		cbCount.Add(1)
	})

	for i := 0; i < 10; i++ {
		idx := i
		_ = g.GoAt(idx, ctx, func(ctx context.Context) (int, error) {
			return idx * 10, nil
		})
	}

	results := g.Wait()
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
	if c := cbCount.Load(); c != 10 {
		t.Fatalf("callback count: expected 10, got %d", c)
	}
}

func TestGroup_Streaming_WaitTimeout(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)
	g.WithStreaming(64)

	for i := 0; i < 50; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	results, ok := g.WaitTimeout(5 * time.Second)
	if !ok {
		t.Fatal("WaitTimeout returned false")
	}
	if len(results) != 50 {
		t.Fatalf("expected 50 results, got %d", len(results))
	}
}

func TestGroup_Streaming_WaitContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	g := NewGroup[int](4)
	g.WithStreaming(64)

	for i := 0; i < 50; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	results, ok := g.WaitContext(ctx)
	if !ok {
		t.Fatal("WaitContext returned false")
	}
	if len(results) != 50 {
		t.Fatalf("expected 50 results, got %d", len(results))
	}
}

func TestGroup_StreamResults_ConcurrentConsumer(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](8)
	g.WithStreaming(256)

	var consumed atomic.Int64
	var wg sync.WaitGroup
	consumers := 4
	ch := g.StreamResults()

	wg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer wg.Done()
			for range ch {
				consumed.Add(1)
			}
		}()
	}

	n := 1000
	for i := 0; i < n; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	results := g.Wait()
	wg.Wait()

	if len(results) != n {
		t.Fatalf("Wait: expected %d results, got %d", n, len(results))
	}
	if c := consumed.Load(); c != int64(n) {
		t.Fatalf("concurrent consumers: expected %d consumed, got %d", n, c)
	}
}

func TestGroup_Streaming_ConcurrentGo(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](16)
	g.WithStreaming(128)

	var cbCount atomic.Int64
	g.WithResultCallback(func(r core.Result[int]) {
		cbCount.Add(1)
	})

	var wg sync.WaitGroup
	goroutines := 20
	tasksPerGoroutine := 200
	var submitted atomic.Int64

	wg.Add(goroutines)
	for gid := 0; gid < goroutines; gid++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < tasksPerGoroutine; i++ {
				err := g.Go(ctx, func(ctx context.Context) (int, error) {
					return gid*10000 + i, nil
				})
				if err == nil {
					submitted.Add(1)
				}
			}
		}(gid)
	}
	wg.Wait()

	results := g.Wait()
	expected := int(submitted.Load())
	if len(results) != expected {
		t.Fatalf("expected %d results, got %d", expected, len(results))
	}
	if c := cbCount.Load(); c != int64(expected) {
		t.Fatalf("callback count: expected %d, got %d", expected, c)
	}
}

func TestGroup_Streaming_PreservedAfterReset(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](4)
	g.WithStreaming(64)

	for i := 0; i < 50; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	g.Wait()

	_, err := g.Reset()
	if err != nil {
		t.Fatalf("Reset error: %v", err)
	}

	var cbCount atomic.Int64
	g.WithResultCallback(func(r core.Result[int]) {
		cbCount.Add(1)
	})

	for i := 0; i < 50; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return i * 2, nil
		})
	}
	results := g.Wait()

	if len(results) != 50 {
		t.Fatalf("after Reset: expected 50 results, got %d", len(results))
	}
	if c := cbCount.Load(); c != 50 {
		t.Fatalf("callback after Reset: expected 50, got %d", c)
	}
}

// ============================================================
// 四、NoResult 流式消费测试
// ============================================================

func TestNoResult_Streaming_Stress(t *testing.T) {
	ctx := context.Background()
	nr := NewNoResult(128)
	nr.WithStreaming(256)

	ch := nr.StreamResults()

	var consumed atomic.Int64
	consumerDone := make(chan struct{})
	go func() {
		for range ch {
			consumed.Add(1)
		}
		close(consumerDone)
	}()

	n := 10000
	for i := 0; i < n; i++ {
		_ = nr.Go(ctx, func(ctx context.Context) error {
			return nil
		})
	}

	nr.Wait()
	<-consumerDone

	c := consumed.Load()
	if c <= 0 {
		t.Fatal("NoResult streaming: consumed 0")
	}
	t.Logf("NoResult streaming: consumed=%d/%d", c, n)
}

func TestNoResult_Callback_Stress(t *testing.T) {
	ctx := context.Background()
	nr := NewNoResult(32)

	var cbCount atomic.Int64
	nr.WithResultCallback(func(r core.Result[struct{}]) {
		cbCount.Add(1)
	})

	n := 10000
	for i := 0; i < n; i++ {
		_ = nr.Go(ctx, func(ctx context.Context) error {
			return nil
		})
	}

	nr.Wait()

	if c := cbCount.Load(); c != int64(n) {
		t.Fatalf("NoResult callback: expected %d, got %d", n, c)
	}
	t.Logf("NoResult callback: count=%d", cbCount.Load())
}

// ============================================================
// 五、MultiGroup 流式消费测试
// ============================================================

func TestMultiGroup_Streaming_Shard8(t *testing.T) {
	ctx := context.Background()
	n := 10000
	mg := NewGroup[int](64).Shard(8)

	mg.WithStreaming(n)
	for i := 0; i < n; i++ {
		idx := i
		_ = mg.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
	}

	ch := mg.StreamResults()
	var consumed atomic.Int64
	consumerDone := make(chan struct{})
	go func() {
		for range ch {
			consumed.Add(1)
		}
		close(consumerDone)
	}()

	results := mg.Wait()
	<-consumerDone

	if len(results) != n {
		t.Fatalf("MultiGroup: expected %d results, got %d", n, len(results))
	}
	c := consumed.Load()
	if c <= 0 {
		t.Fatal("MultiGroup streaming: consumed 0")
	}
	t.Logf("MultiGroup Shard(8) streaming: consumed=%d/%d", c, n)
}

func TestMultiGroup_Callback_Shard8(t *testing.T) {
	ctx := context.Background()
	mg := NewGroup[int](64).Shard(8)

	var cbCount atomic.Int64
	mg.WithResultCallback(func(r core.Result[int]) {
		cbCount.Add(1)
	})

	n := 10000
	for i := 0; i < n; i++ {
		idx := i
		_ = mg.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
	}

	results := mg.Wait()

	if c := cbCount.Load(); c != int64(n) {
		t.Fatalf("MultiGroup callback: expected %d, got %d", n, c)
	}
	if len(results) != n {
		t.Fatalf("MultiGroup callback: expected %d results, got %d", n, len(results))
	}
	t.Logf("MultiGroup Shard(8) callback: count=%d", cbCount.Load())
}

// ============================================================
// 六、流式消费 Goroutine 泄漏测试
// ============================================================

func TestStreaming_NoGoroutineLeak_Repeat10(t *testing.T) {
	ctx := context.Background()
	base := runtime.NumGoroutine()

	for round := 0; round < 10; round++ {
		g := NewGroup[int](32)
		g.WithStreaming(128)

		ch := g.StreamResults()
		consumerDone := make(chan struct{})
		go func() {
			for range ch {
			}
			close(consumerDone)
		}()

		n := 10000
		for i := 0; i < n; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				return 0, nil
			})
		}

		g.Wait()
		<-consumerDone
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	if after > base+50 {
		t.Fatalf("goroutine leak: base=%d after=%d (diff=%d)", base, after, after-base)
	}
	t.Logf("No goroutine leak: base=%d after=%d repeat=10", base, after)
}

func TestResultCallback_NoGoroutineLeak_Repeat10(t *testing.T) {
	ctx := context.Background()
	base := runtime.NumGoroutine()

	for round := 0; round < 10; round++ {
		g := NewGroup[int](32)

		var cbCount atomic.Int64
		g.WithResultCallback(func(r core.Result[int]) {
			cbCount.Add(1)
		})

		n := 10000
		for i := 0; i < n; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				return 0, nil
			})
		}

		g.Wait()
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	if after > base+50 {
		t.Fatalf("goroutine leak: base=%d after=%d (diff=%d)", base, after, after-base)
	}
	t.Logf("No goroutine leak: base=%d after=%d repeat=10", base, after)
}

// ============================================================
// 七、流式消费吞吐量基准
// ============================================================

func TestStreaming_Throughput_RealTime(t *testing.T) {
	ctx := context.Background()

	sizes := []struct {
		name    string
		tasks   int
		concur  int
		bufSize int
	}{
		{"10K_conc64_buf512", 10_000, 64, 512},
		{"10K_conc128_buf1024", 10_000, 128, 1024},
	}

	for _, sc := range sizes {
		t.Run(sc.name, func(t *testing.T) {
			g := NewGroup[int](sc.concur)
			g.WithStreaming(sc.bufSize)

			ch := g.StreamResults()

			var maxLatencyNs atomic.Int64
			var consumed atomic.Int64
			start := time.Now()

			consumerDone := make(chan struct{})
			go func() {
				for r := range ch {
					consumed.Add(1)
					if r.Err == nil {
						lat := time.Since(time.Unix(0, int64(r.Value))).Nanoseconds()
						for {
							old := maxLatencyNs.Load()
							if lat <= old || maxLatencyNs.CompareAndSwap(old, lat) {
								break
							}
						}
					}
				}
				close(consumerDone)
			}()

			for i := 0; i < sc.tasks; i++ {
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return int(time.Now().UnixNano()), nil
				})
			}

			g.Wait()
			<-consumerDone
			elapsed := time.Since(start)

			ops := float64(sc.tasks) / elapsed.Seconds()
			c := consumed.Load()
			dropRate := float64(sc.tasks-int(c)) / float64(sc.tasks) * 100
			maxMs := float64(maxLatencyNs.Load()) / 1e6

			t.Logf("%s: %.0f ops/s | consumed=%d/%d | drop=%.2f%% | max_latency=%.2fms | elapsed=%v",
				sc.name, ops, c, sc.tasks, dropRate, maxMs, elapsed.Round(time.Millisecond))
		})
	}
}

func TestStreaming_Latency_p50_p99(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](64)
	g.WithStreaming(4096)

	ch := g.StreamResults()

	lats := make([]int64, 0, 20000)
	var mu sync.Mutex
	var consumed atomic.Int64

	consumerDone := make(chan struct{})
	go func() {
		for r := range ch {
			consumed.Add(1)
			if r.Err == nil {
				lat := time.Since(time.Unix(0, int64(r.Value))).Nanoseconds()
				mu.Lock()
				lats = append(lats, lat)
				mu.Unlock()
			}
		}
		close(consumerDone)
	}()

	n := 20000
	for i := 0; i < n; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return int(time.Now().UnixNano()), nil
		})
	}

	g.Wait()
	<-consumerDone

	mu.Lock()
	sorted := make([]int64, len(lats))
	copy(sorted, lats)
	mu.Unlock()

	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p50s := float64(sorted[len(sorted)*50/100]) / 1e3
	p99s := float64(sorted[len(sorted)*99/100]) / 1e3
	maxMs := float64(sorted[len(sorted)-1]) / 1e6

	t.Logf("latency 20K: p50=%.1fμs p99=%.1fμs max=%.2fms consumed=%d",
		p50s, p99s, maxMs, consumed.Load())
}

// ============================================================
// 八、四档并发压力测试（万/十万/百万/千万）
// ============================================================

func TestGroup_Go_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			g := NewGroup[int](16)

			var wg sync.WaitGroup
			goroutines := 50
			tasksPerGoroutine := tier.size / goroutines
			if tasksPerGoroutine < 1 {
				tasksPerGoroutine = 1
			}
			var submitted atomic.Int64

			wg.Add(goroutines)
			for gid := 0; gid < goroutines; gid++ {
				go func(gid int) {
					defer wg.Done()
					for i := 0; i < tasksPerGoroutine; i++ {
						if g.Go(ctx, func(ctx context.Context) (int, error) {
							return gid*10000 + i, nil
						}) == nil {
							submitted.Add(1)
						}
					}
				}(gid)
			}
			wg.Wait()
			results := g.Wait()
			if len(results) != int(submitted.Load()) {
				t.Fatalf("expected %d results, got %d", submitted.Load(), len(results))
			}
		})
	}
}

func TestGroup_ResetAndReuse(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			g := NewGroup[int](50)
			ctx := context.Background()
			rounds := 10
			tasksPerRound := tier.size / 10
			if tasksPerRound < 100 {
				tasksPerRound = 100
			}
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
		})
	}
}

func TestGroup_Concurrency1_Serial(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			g := NewGroup[int](1)
			for i := 0; i < tier.size; i++ {
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				})
			}
			results := g.Wait()
			if len(results) != tier.size {
				t.Fatalf("expected %d results, got %d", tier.size, len(results))
			}
		})
	}
}

func TestGroup_CPUBound(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			tasks := tier.size
			if tasks > 5000 {
				tasks = 5000
			}
			g := NewGroup[int](core.CPU())
			ctx := context.Background()
			for i := 0; i < tasks; i++ {
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
			if len(results) != tasks {
				t.Fatalf("expected %d, got %d", tasks, len(results))
			}
		})
	}
}

func TestGroup_FailFast_Large(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			g, ctx := NewGroup[int](100).WithFFSto(context.Background(), 2*time.Second)
			for i := 0; i < tier.size; i++ {
				idx := i
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					if idx == 0 {
						return 0, errors.New("trigger fail fast")
					}
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(time.Millisecond * 10):
						return idx, nil
					}
				})
			}
			results := g.Wait()
			if len(results) != tier.size {
				t.Fatalf("expected %d results, got %d", tier.size, len(results))
			}
			if results[0].Err == nil {
				t.Fatal("expected first task to fail")
			}
		})
	}
}

func TestNoResult_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			nr := NewNoResult(200)
			ctx := context.Background()
			var counter atomic.Int64
			for i := 0; i < tier.size; i++ {
				_ = nr.Go(ctx, func(ctx context.Context) error {
					counter.Add(1)
					return nil
				})
			}
			nr.Wait()
			if counter.Load() != int64(tier.size) {
				t.Fatalf("expected %d, got %d", tier.size, counter.Load())
			}
			if nr.FailCount() != 0 {
				t.Fatalf("expected 0 failures, got %d", nr.FailCount())
			}
		})
	}
}

// ============================================================
// 九、流式消费四档压力测试
// ============================================================

func TestGroup_Streaming_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			g := NewGroup[int](128)
			g.WithStreaming(4096)

			ch := g.StreamResults()

			var consumed atomic.Int64
			consumerDone := make(chan struct{})
			go func() {
				for range ch {
					consumed.Add(1)
				}
				close(consumerDone)
			}()

			var wg sync.WaitGroup
			producers := 16
			perProducer := tier.size / producers
			if perProducer < 1 {
				perProducer = 1
			}

			wg.Add(producers)
			for p := 0; p < producers; p++ {
				go func(offset int) {
					defer wg.Done()
					for i := 0; i < perProducer; i++ {
						_ = g.Go(ctx, func(ctx context.Context) (int, error) {
							return offset*perProducer + i, nil
						})
					}
				}(p)
			}
			wg.Wait()

			results := g.Wait()
			<-consumerDone

			if len(results) != tier.size {
				t.Fatalf("Wait: expected %d results, got %d", tier.size, len(results))
			}
			c := consumed.Load()
			if c <= 0 {
				t.Fatal("streaming: consumed 0")
			}
			t.Logf("%s streaming: consumed=%d/%d", tier.name, c, tier.size)
		})
	}
}

func TestGroup_Callback_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			g := NewGroup[int](128)

			var cbCount atomic.Int64
			g.WithResultCallback(func(r core.Result[int]) {
				cbCount.Add(1)
			})

			for i := 0; i < tier.size; i++ {
				idx := i
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return idx, nil
				})
			}

			results := g.Wait()

			if c := cbCount.Load(); c != int64(tier.size) {
				t.Fatalf("callback: expected %d, got %d", tier.size, c)
			}
			if len(results) != tier.size {
				t.Fatalf("Wait: expected %d results, got %d", tier.size, len(results))
			}
		})
	}
}

// ============================================================
// 十、Race 竞态测试（go test -race 专用）
// ============================================================

func TestGroup_Race_GoAndWait(t *testing.T) {
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

func TestGroup_Race_AddInFlightDefer(t *testing.T) {
	for round := 0; round < 300; round++ {
		g := NewGroup[int](64)
		ctx := context.Background()

		var goWg, waitWg sync.WaitGroup
		var submitted atomic.Int64

		producers := 50
		tasks := 200
		goWg.Add(producers)
		for p := 0; p < producers; p++ {
			go func(pid int) {
				defer goWg.Done()
				for i := 0; i < tasks; i++ {
					if g.Go(ctx, func(ctx context.Context) (int, error) {
						return pid*tasks + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(p)
		}

		waitWg.Add(1)
		go func() {
			defer waitWg.Done()
			goWg.Wait()
			g.Wait()
		}()

		waitWg.Wait()
	}
}

func TestGroup_Race_ConcurrentWait(t *testing.T) {
	for round := 0; round < 200; round++ {
		g := NewGroup[int](32)
		ctx := context.Background()

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				g.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(50 * time.Microsecond)
			g.Wait()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(80 * time.Microsecond)
			g.Wait()
		}()

		wg.Wait()
	}
}

func TestGroup_Race_WaitTimeout(t *testing.T) {
	for round := 0; round < 100; round++ {
		g := NewGroup[int](8)
		ctx := context.Background()

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				g.Go(ctx, func(ctx context.Context) (int, error) {
					return i, nil
				})
			}
		}()

		go func() {
			defer wg.Done()
			g.WaitTimeout(500 * time.Millisecond)
		}()

		wg.Wait()
	}
}

func TestGroup_Race_WaitContext(t *testing.T) {
	for round := 0; round < 100; round++ {
		g := NewGroup[int](8)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				g.Go(context.Background(), func(ctx context.Context) (int, error) {
					return i, nil
				})
			}
		}()

		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			g.WaitContext(ctx)
		}()

		wg.Wait()
	}
}

func TestGroup_Race_ConcurrentReset(t *testing.T) {
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

func TestGroup_Race_Streaming(t *testing.T) {
	for round := 0; round < 100; round++ {
		ctx := context.Background()
		g := NewGroup[int](32)
		g.WithStreaming(64)

		ch := g.StreamResults()
		go func() {
			for range ch {
			}
		}()

		for i := 0; i < 200; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				return i, nil
			})
		}
		g.Wait()
	}
}

func TestGroup_Race_Callback(t *testing.T) {
	for round := 0; round < 100; round++ {
		ctx := context.Background()
		g := NewGroup[int](32)

		var cbCount atomic.Int64
		g.WithResultCallback(func(r core.Result[int]) {
			cbCount.Add(1)
		})

		for i := 0; i < 200; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				return i, nil
			})
		}
		g.Wait()
	}
}

func TestGroup_Race_StreamReset(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](8)

	for round := 0; round < 50; round++ {
		g.WithStreaming(64)

		ch := g.StreamResults()
		go func() {
			for range ch {
			}
		}()

		for i := 0; i < 100; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				return round, nil
			})
		}

		g.Wait()
		_, err := g.Reset()
		if err != nil {
			t.Fatalf("round %d Reset error: %v", round, err)
		}
	}
}

// ============================================================
// 十一、MultiGroup Race 测试
// ============================================================

func TestMultiGroup_Race_GoWait(t *testing.T) {
	for round := 0; round < 100; round++ {
		mg := NewGroup[int](32).Shard(4)
		ctx := context.Background()

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				mg.Go(ctx, func(ctx context.Context) (int, error) {
					return i, nil
				})
			}
		}()

		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			mg.Wait()
		}()

		wg.Wait()
	}
}

// ============================================================
// 十二、极限 Large 压力场景
// ============================================================

func TestStreaming_Large_WithDrop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large test in short mode")
	}
	ctx := context.Background()
	g := NewGroup[int](128)
	g.WithStreaming(256)

	ch := g.StreamResults()

	var consumed atomic.Int64
	consumerDone := make(chan struct{})
	go func() {
		for range ch {
			consumed.Add(1)
		}
		close(consumerDone)
	}()

	n := 500_000
	for i := 0; i < n; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return 0, nil
		})
	}

	results := g.Wait()
	<-consumerDone

	total := consumed.Load()
	if total <= 0 {
		t.Fatal("slow consumer: consumed 0, expected at least some results")
	}
	if len(results) != n {
		t.Fatalf("Wait results: expected %d, got %d", n, len(results))
	}
	t.Logf("500K slow consumer: consumed=%d/%d (dropped=%d)", total, n, int64(n)-total)
}

func TestStreaming_MultiConsumer_NoDeadlock(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](64)
	g.WithStreaming(256)

	ch := g.StreamResults()

	var total atomic.Int64
	consumers := 8
	var wg sync.WaitGroup
	wg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer wg.Done()
			for range ch {
				total.Add(1)
			}
		}()
	}

	n := 100_000
	for i := 0; i < n; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return 0, nil
		})
	}

	results := g.Wait()
	wg.Wait()

	if len(results) != n {
		t.Fatalf("multi-consumer: expected %d results, got %d", n, len(results))
	}
	t.Logf("8 consumers 100K: total consumed=%d/%d", total.Load(), n)
}

func TestStreaming_BufferSizeImpact(t *testing.T) {
	ctx := context.Background()

	for _, buf := range []int{64, 256, 1024, 4096} {
		t.Run(fmt.Sprintf("buf_%d", buf), func(t *testing.T) {
			g := NewGroup[int](128)
			g.WithStreaming(buf)

			ch := g.StreamResults()

			var consumed atomic.Int64
			consumerDone := make(chan struct{})
			go func() {
				for range ch {
					consumed.Add(1)
				}
				close(consumerDone)
			}()

			n := 50_000
			for i := 0; i < n; i++ {
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return 0, nil
				})
			}

			g.Wait()
			<-consumerDone

			c := consumed.Load()
			dropRate := float64(n-int(c)) / float64(n) * 100
			t.Logf("buf=%5d: consumed=%d/%d | drop=%.2f%%", buf, c, n, dropRate)
		})
	}
}

func TestCallback_vs_Streaming_Latency(t *testing.T) {
	ctx := context.Background()

	run := func(name string, n, conc int, useStreaming bool, bufSize int) {
		t.Run(name, func(t *testing.T) {
			g := NewGroup[int](conc)
			var maxLatNs atomic.Int64
			var count atomic.Int64

			if useStreaming {
				g.WithStreaming(bufSize)
				ch := g.StreamResults()
				done := make(chan struct{})
				go func() {
					for r := range ch {
						count.Add(1)
						if r.Err == nil {
							lat := time.Since(time.Unix(0, int64(r.Value))).Nanoseconds()
							for {
								old := maxLatNs.Load()
								if lat <= old || maxLatNs.CompareAndSwap(old, lat) {
									break
								}
							}
						}
					}
					close(done)
				}()
				defer func() { <-done }()
			} else {
				g.WithResultCallback(func(r core.Result[int]) {
					count.Add(1)
					if r.Err == nil {
						lat := time.Since(time.Unix(0, int64(r.Value))).Nanoseconds()
						for {
							old := maxLatNs.Load()
							if lat <= old || maxLatNs.CompareAndSwap(old, lat) {
								break
							}
						}
					}
				})
			}

			for i := 0; i < n; i++ {
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return int(time.Now().UnixNano()), nil
				})
			}

			g.Wait()

			maxMs := float64(maxLatNs.Load()) / 1e6
			dropRate := float64(n-int(count.Load())) / float64(n) * 100
			t.Logf("%s %dK: max_latency=%.2fms drop=%.2f%% conc=%d",
				name, n/1000, maxMs, dropRate, conc)
		})
	}

	run("streaming_buf256", 20_000, 64, true, 256)
	run("streaming_buf4096", 20_000, 64, true, 4096)
	run("callback", 20_000, 64, false, 0)
}
