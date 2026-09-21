package group

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
)

// ==================== Group 流式消费测试 ====================

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

// ==================== Group 流式消费 + GoAt 配合 ====================

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

// ==================== Group 流式 + WaitTimeout ====================

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

// ==================== Group 流式消费并发测试 ====================

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

// ==================== Group Reset 保留流式配置 ====================

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
