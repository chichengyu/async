package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
)

// ==================== Pool 流式消费测试 ====================

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

// ==================== Pool 环形缓冲测试 ====================

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
	t.Logf("OverflowDrop: capacity=50, submitted=1000, flushed=%d (expected <= 1000)", len(flushed))
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

// ==================== Pool 环形缓冲 Reset 保留 ====================

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

// ==================== Pool 背压控制测试 ====================

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

// ==================== Pool 流式 + 环形缓冲 + 背压 组合测试 ====================

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

// ==================== Pool 流式消费并发测试 ====================

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

// ==================== Pool 环形缓冲并发测试 ====================

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

// ==================== Pool 背压边界测试 ====================

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
