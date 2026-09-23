package group

import (
	"context"
	"fmt"
	"runtime"
	"sort"
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

// ==================== 极限生产级压力测试 ====================

func TestStreaming_1M_SlowConsumer_DropsGracefully(t *testing.T) {
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

	n := 1_000_000
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
	t.Logf("1M slow consumer: consumed=%d/%d (dropped=%d)", total, n, int64(n)-total)
}

func TestStreaming_1M_FastConsumer_NoDrops(t *testing.T) {
	ctx := context.Background()
	n := 1_000_000
	g := NewGroup[int](128)
	g.WithStreaming(n)

	ch := g.StreamResults()

	var consumed atomic.Int64
	var mu sync.Mutex
	values := make([]int, 0)

	consumerDone := make(chan struct{})
	go func() {
		for r := range ch {
			consumed.Add(1)
			if r.Err == nil {
				mu.Lock()
				values = append(values, r.Value)
				mu.Unlock()
			}
		}
		close(consumerDone)
	}()

	for i := 0; i < n; i++ {
		idx := i
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
	}

	results := g.Wait()
	<-consumerDone

	total := consumed.Load()
	if total != int64(n) {
		t.Fatalf("fast consumer: expected %d consumed, got %d", n, total)
	}
	if len(results) != n {
		t.Fatalf("Wait results: expected %d, got %d", n, len(results))
	}
	t.Logf("1M fast consumer: consumed=%d (0 drops)", total)
}

func TestResultCallback_1M_ExtremeLoad(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](256)

	var cbCount atomic.Int64
	var sum atomic.Int64
	g.WithResultCallback(func(r core.Result[int]) {
		cbCount.Add(1)
		if r.Err == nil {
			sum.Add(int64(r.Value))
		}
	})

	n := 1_000_000
	for i := 0; i < n; i++ {
		idx := i
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
	}

	results := g.Wait()

	if c := cbCount.Load(); c != int64(n) {
		t.Fatalf("callback count: expected %d, got %d", n, c)
	}
	if len(results) != n {
		t.Fatalf("Wait results: expected %d, got %d", n, len(results))
	}
	expectedSum := int64(n-1) * int64(n) / 2
	if s := sum.Load(); s != expectedSum {
		t.Fatalf("sum: expected %d, got %d", expectedSum, s)
	}
	t.Logf("1M callback: count=%d sum=%d", cbCount.Load(), sum.Load())
}

func TestStreaming_ConcurrentGoWaitStream_NoDataRace(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](64)
	g.WithStreaming(1024)

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
	var wg sync.WaitGroup
	producers := 16
	perProducer := n / producers

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

	if len(results) != n {
		t.Fatalf("Wait results: expected %d, got %d", n, len(results))
	}
	c := consumed.Load()
	if c <= 0 {
		t.Fatal("concurrent stream: consumed 0")
	}
	t.Logf("500K concurrent Go+Wait+Stream: consumed=%d/%d results=%d", c, n, len(results))
}

func TestNoResult_Streaming_1M_Stress(t *testing.T) {
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

	n := 1_000_000
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
	t.Logf("NoResult 1M streaming: consumed=%d/%d", c, n)
}

func TestNoResult_Callback_1M_Stress(t *testing.T) {
	ctx := context.Background()
	nr := NewNoResult(256)

	var cbCount atomic.Int64
	nr.WithResultCallback(func(r core.Result[struct{}]) {
		cbCount.Add(1)
	})

	n := 1_000_000
	for i := 0; i < n; i++ {
		_ = nr.Go(ctx, func(ctx context.Context) error {
			return nil
		})
	}

	nr.Wait()

	if c := cbCount.Load(); c != int64(n) {
		t.Fatalf("NoResult callback 1M: expected %d, got %d", n, c)
	}
	t.Logf("NoResult 1M callback: count=%d", cbCount.Load())
}

func TestMultiGroup_Streaming_1M_Shard8(t *testing.T) {
	ctx := context.Background()
	n := 1_000_000
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
		t.Fatalf("MultiGroup 1M: expected %d results, got %d", n, len(results))
	}
	c := consumed.Load()
	if c <= 0 {
		t.Fatal("MultiGroup streaming: consumed 0")
	}
	pct := float64(c) / float64(n) * 100
	t.Logf("MultiGroup Shard(8) 1M streaming: consumed=%d/%d (%.1f%%)", c, n, pct)
}

func TestMultiGroup_Callback_1M_Shard8(t *testing.T) {
	ctx := context.Background()
	mg := NewGroup[int](64).Shard(8)

	var cbCount atomic.Int64
	mg.WithResultCallback(func(r core.Result[int]) {
		cbCount.Add(1)
	})

	n := 1_000_000
	for i := 0; i < n; i++ {
		idx := i
		_ = mg.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
	}

	results := mg.Wait()

	if c := cbCount.Load(); c != int64(n) {
		t.Fatalf("MultiGroup callback 1M: expected %d, got %d", n, c)
	}
	if len(results) != n {
		t.Fatalf("MultiGroup callback: expected %d results, got %d", n, len(results))
	}
	t.Logf("MultiGroup Shard(8) 1M callback: count=%d", cbCount.Load())
}

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

		n := 100_000
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

		n := 100_000
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

	n := 500_000
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
	t.Logf("8 consumers 500K: total consumed=%d/%d", total.Load(), n)
}

func TestStreaming_ResetAndReuse_NoPanic(t *testing.T) {
	ctx := context.Background()

	g := NewGroup[int](8)

	for round := 0; round < 100; round++ {
		g.WithStreaming(64)

		ch := g.StreamResults()
		go func() {
			for range ch {
			}
		}()

		n := 1000
		for i := 0; i < n; i++ {
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

	t.Log("100x Reset+Streaming reuse: no panic")
}

func TestStreaming_PipeConsumer_Backpressure(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](32)
	g.WithStreaming(256)

	ch := g.StreamResults()

	var processed atomic.Int64
	pipeDone := make(chan struct{})
	go func() {
		for r := range ch {
			if r.Err != nil {
				continue
			}
			processed.Add(1)
			time.Sleep(time.Microsecond)
		}
		close(pipeDone)
	}()

	n := 500_000
	for i := 0; i < n; i++ {
		idx := i
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
	}

	results := g.Wait()
	<-pipeDone

	p := processed.Load()
	if p <= 0 {
		t.Fatal("pipe consumer: processed 0")
	}
	if len(results) != n {
		t.Fatalf("pipe consumer: expected %d results, got %d", n, len(results))
	}
	t.Logf("pipe consumer 500K: processed=%d/%d (buffered drop OK)", p, n)
}

// ==================== 实时消费吞吐量基准 ====================

func TestStreaming_Throughput_RealTime(t *testing.T) {
	ctx := context.Background()

	sizes := []struct {
		name    string
		tasks   int
		concur  int
		bufSize int
	}{
		{"100K_conc64_buf512", 100_000, 64, 512},
		{"100K_conc256_buf1024", 100_000, 256, 1024},
		{"500K_conc128_buf2048", 500_000, 128, 2048},
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

			t.Logf("%s: %.0f ops/s | consumed=%d/%d | drop=%.2f%% | max_latency=%.2fms | elapsed=%v | buf=%d conc=%d",
				sc.name, ops, c, sc.tasks, dropRate, maxMs, elapsed.Round(time.Millisecond), sc.bufSize, sc.concur)

			if dropRate > 10 {
				t.Errorf("drop rate too high: %.2f%% (buf=%d)", dropRate, sc.bufSize)
			}
		})
	}
}

func TestStreaming_Throughput_SteadyFlow(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](512)
	g.WithStreaming(8192)

	ch := g.StreamResults()

	var consumed atomic.Int64
	var totalLatNs atomic.Int64
	var maxLatNs atomic.Int64

	consumerDone := make(chan struct{})
	go func() {
		for r := range ch {
			consumed.Add(1)
			if r.Err == nil {
				lat := time.Since(time.Unix(0, int64(r.Value))).Nanoseconds()
				totalLatNs.Add(lat)
				for {
					old := maxLatNs.Load()
					if lat <= old || maxLatNs.CompareAndSwap(old, lat) {
						break
					}
				}
			}
		}
		close(consumerDone)
	}()

	n := 2_000_000
	start := time.Now()
	for i := 0; i < n; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return int(time.Now().UnixNano()), nil
		})
	}

	g.Wait()
	<-consumerDone
	elapsed := time.Since(start)

	c := consumed.Load()
	ops := float64(c) / elapsed.Seconds()
	avgLatUs := float64(totalLatNs.Load()/c) / 1e3
	maxLatMs := float64(maxLatNs.Load()) / 1e6
	dropRate := float64(n-int(c)) / float64(n) * 100

	t.Logf("steady 2M: %.0f ops/s | consumed=%d | avg_latency=%.1fμs | max_latency=%.2fms | drop=%.2f%% | elapsed=%v",
		ops, c, avgLatUs, maxLatMs, dropRate, elapsed.Round(time.Millisecond))
}

func TestStreaming_Latency_p50_p99(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](64)
	g.WithStreaming(4096)

	ch := g.StreamResults()

	lats := make([]int64, 0, 200_000)
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

	n := 200_000
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
	p999s := float64(sorted[len(sorted)*999/1000]) / 1e3
	maxMs := float64(sorted[len(sorted)-1]) / 1e6

	dropRate := float64(n-int(consumed.Load())) / float64(n) * 100

	t.Logf("latency 200K: p50=%.1fμs p99=%.1fμs p999=%.1fμs max=%.2fms drop=%.2f%%",
		p50s, p99s, p999s, maxMs, dropRate)
}

func TestStreaming_ConsumerScaling(t *testing.T) {
	ctx := context.Background()

	for _, consumers := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("consumers_%d", consumers), func(t *testing.T) {
			g := NewGroup[int](256)
			g.WithStreaming(2048)

			ch := g.StreamResults()

			var total atomic.Int64
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

			n := 500_000
			start := time.Now()
			for i := 0; i < n; i++ {
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return 0, nil
				})
			}

			g.Wait()
			wg.Wait()
			elapsed := time.Since(start)

			c := total.Load()
			ops := float64(c) / elapsed.Seconds()
			dropRate := float64(n-int(c)) / float64(n) * 100

			t.Logf("consumers=%d: %.0f ops/s | consumed=%d/%d | drop=%.2f%% | %v",
				consumers, ops, c, n, dropRate, elapsed.Round(time.Millisecond))
		})
	}
}

func TestStreaming_BufferSizeImpact(t *testing.T) {
	ctx := context.Background()

	for _, buf := range []int{64, 256, 1024, 4096, 16384} {
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

			n := 200_000
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

	run("streaming_buf256", 200_000, 64, true, 256)
	run("streaming_buf4096", 200_000, 64, true, 4096)
	run("callback", 200_000, 64, false, 0)
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

// ==================== 千万级实时消费吞吐量 ====================

func TestStreaming_10M_RealTime_Throughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10M test in short mode")
	}
	ctx := context.Background()
	g := NewGroup[int](256)
	g.WithStreaming(16384)

	ch := g.StreamResults()

	var consumed atomic.Int64
	var maxLatNs atomic.Int64
	var totalLatNs atomic.Int64
	consumerDone := make(chan struct{})

	go func() {
		for r := range ch {
			consumed.Add(1)
			if r.Err == nil {
				lat := time.Since(time.Unix(0, int64(r.Value))).Nanoseconds()
				totalLatNs.Add(lat)
				for {
					old := maxLatNs.Load()
					if lat <= old || maxLatNs.CompareAndSwap(old, lat) {
						break
					}
				}
			}
		}
		close(consumerDone)
	}()

	n := 10_000_000
	start := time.Now()
	for i := 0; i < n; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return int(time.Now().UnixNano()), nil
		})
	}

	g.Wait()
	<-consumerDone
	elapsed := time.Since(start)

	c := consumed.Load()
	ops := float64(c) / elapsed.Seconds()
	avgLatUs := float64(totalLatNs.Load()/c) / 1e3
	maxLatMs := float64(maxLatNs.Load()) / 1e6
	dropRate := float64(n-int(c)) / float64(n) * 100

	t.Logf("10M real-time: %.0f ops/s | consumed=%d/%d | avg_latency=%.1fμs | max_latency=%.2fms | drop=%.2f%% | elapsed=%v",
		ops, c, n, avgLatUs, maxLatMs, dropRate, elapsed.Round(time.Millisecond))

	if c < int64(n) {
		t.Errorf("expected %d consumed, got %d", n, c)
	}
}

func TestStreaming_10M_Progressive_Consumer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10M test in short mode")
	}
	ctx := context.Background()
	g := NewGroup[int](128)
	g.WithStreaming(4096)

	ch := g.StreamResults()

	var progressive []int
	var consumed atomic.Int64
	var snapMu sync.Mutex

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	consumerDone := make(chan struct{})
	go func() {
		for {
			select {
			case r, ok := <-ch:
				if !ok {
					close(consumerDone)
					return
				}
				consumed.Add(1)
				_ = r
			case <-ticker.C:
				snapMu.Lock()
				progressive = append(progressive, int(consumed.Load()))
				snapMu.Unlock()
			}
		}
	}()

	n := 10_000_000
	start := time.Now()
	for i := 0; i < n; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return int(time.Now().UnixNano()), nil
		})
	}

	g.Wait()
	<-consumerDone
	elapsed := time.Since(start)

	c := consumed.Load()
	ops := float64(c) / elapsed.Seconds()

	t.Logf("10M progressive: %.0f ops/s | consumed=%d/%d | elapsed=%v | %d snapshots=%v",
		ops, c, n, elapsed.Round(time.Millisecond), len(progressive), progressive)
}

func TestStreaming_10M_Concurrent_GoStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10M test in short mode")
	}
	ctx := context.Background()
	g := NewGroup[int](256)
	g.WithStreaming(8192)

	ch := g.StreamResults()

	var consumed atomic.Int64
	consumerDone := make(chan struct{})
	go func() {
		for range ch {
			consumed.Add(1)
		}
		close(consumerDone)
	}()

	n := 10_000_000
	var wg sync.WaitGroup
	producers := 16
	perProducer := n / producers

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

	start := time.Now()
	wg.Wait()
	g.Wait()
	<-consumerDone
	elapsed := time.Since(start)

	c := consumed.Load()
	ops := float64(c) / elapsed.Seconds()
	dropRate := float64(n-int(c)) / float64(n) * 100

	t.Logf("10M concurrent Go+Stream: %.0f ops/s | consumed=%d/%d | drop=%.2f%% | elapsed=%v",
		ops, c, n, dropRate, elapsed.Round(time.Millisecond))
}
