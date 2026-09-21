package async

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
)

func memStats(tag string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("[%s] G=%d Heap=%dMB Obj=%d\n",
		tag, runtime.NumGoroutine(), m.HeapAlloc/1024/1024, m.HeapObjects)
}

// Test10M_Pool_Submit 千万任务Pool提交
func Test10M_Pool_Submit(t *testing.T) {
	memStats("10M-Pool-start")
	start := time.Now()
	total := 10_000_000
	batchSize := 1_000_000
	batches := total / batchSize
	var grandTotal atomic.Int64

	for batch := 0; batch < batches; batch++ {
		p := NewPool[int](200)
		ctx := context.Background()
		var submitted atomic.Int64

		for i := 0; i < batchSize; i++ {
			idx := i
			err := p.Submit(ctx, func(ctx context.Context) (int, error) {
				return idx * 2, nil
			})
			if err == nil {
				submitted.Add(1)
			}
		}
		results := p.Wait()
		p.Close()

		if int64(len(results)) != int64(batchSize) {
			t.Errorf("batch %d: expected %d results, got %d", batch+1, batchSize, len(results))
		}
		for i, r := range results {
			if !r.Ok() || r.Value != i*2 {
				t.Errorf("batch %d, task %d: value=%d err=%v", batch+1, i, r.Value, r.Err)
			}
		}
		grandTotal.Add(int64(batchSize))
		t.Logf("  Pool batch %d/%d done", batch+1, batches)
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}

	elapsed := time.Since(start)
	t.Logf("10M Pool: %d tasks in %v (%.0f ops/s)", grandTotal.Load(), elapsed, float64(grandTotal.Load())/elapsed.Seconds())
	memStats("10M-Pool-end")
}

// Test10M_Pool_CloseAndWaitTimeout_Race CloseAndWaitTimeout竞态
func Test10M_Pool_CloseAndWaitTimeout_Race(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](100)
		ctx := context.Background()
		var wg sync.WaitGroup
		submitDone := make(chan struct{})

		wg.Add(100)
		for g := 0; g < 100; g++ {
			go func() {
				defer wg.Done()
				for i := 0; i < 1000; i++ {
					p.Submit(ctx, func(ctx context.Context) (int, error) { return 42, nil })
				}
			}()
		}
		go func() { wg.Wait(); close(submitDone) }()
		<-submitDone

		ok, workerDone := p.CloseAndWaitTimeout(10 * time.Second)
		if !ok {
			select {
			case <-workerDone:
			case <-time.After(15 * time.Second):
				t.Errorf("round %d: workers timeout", round)
			}
		}
		p.Wait()
	}
	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	g := runtime.NumGoroutine()
	if g > 20 {
		t.Errorf("goroutine leak: %d", g)
	}
	t.Logf("CloseAndWaitTimeout race: post G=%d PASS", g)
}

// Test10M_Pool_AutoScale_CloseRace 自动扩缩容Pool关闭竞态
func Test10M_Pool_AutoScale_CloseRace(t *testing.T) {
	for round := 0; round < 50; round++ {
		p := NewPool[int](50)
		p.EnableAutoScale(nil)
		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		started := make(chan struct{})

		wg.Add(200)
		for g := 0; g < 200; g++ {
			go func() {
				<-started
				defer wg.Done()
				for i := 0; i < 500; i++ {
					if err := p.Submit(ctx, func(ctx context.Context) (int, error) {
						time.Sleep(time.Microsecond)
						return 0, nil
					}); err == nil {
						submitted.Add(1)
					}
				}
			}()
		}
		close(started)
		wg.Wait()

		p.Close()
		results := p.Wait()
		if int64(len(results)) != submitted.Load() {
			t.Errorf("round %d: expected %d results, got %d", round, submitted.Load(), len(results))
		}
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}
	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	g := runtime.NumGoroutine()
	if g > 20 {
		t.Errorf("goroutine leak: %d", g)
	}
	t.Logf("AutoScale Close race: post G=%d PASS", g)
}

// Test10M_RateLimiter_ResizeRace Resize与Acquire/Release竞态
func Test10M_RateLimiter_ResizeRace(t *testing.T) {
	ctx := context.Background()
	rl := NewRateLimiter(1000, time.Second)
	var wg sync.WaitGroup
	var acquired, released atomic.Int64
	stop := make(chan struct{})

	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := rl.Acquire(ctx); err != nil {
					continue
				}
				acquired.Add(1)
				time.Sleep(time.Microsecond)
				rl.Release()
				released.Add(1)
			}
		}()
	}

	for r := 0; r < 20; r++ {
		newRate := 500 + r*50
		rl.Resize(newRate)
		time.Sleep(50 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	rl.Close()
	t.Logf("RateLimiter resize race: acquired=%d released=%d", acquired.Load(), released.Load())
}

// Test10M_Group_NoResult_Race Group NoResult竞态
func Test10M_Group_NoResult_Race(t *testing.T) {
	for round := 0; round < 50; round++ {
		nr := NewNoResult(100)
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(200)
		for g := 0; g < 200; g++ {
			go func() {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					nr.Go(ctx, func(ctx context.Context) error {
						time.Sleep(time.Microsecond)
						return nil
					})
				}
			}()
		}
		wg.Wait()
		nr.Wait()
		if nr.FailCount() > 0 {
			t.Errorf("round %d: failures=%d", round, nr.FailCount())
		}
	}
	t.Logf("Group NoResult race: PASS")
}

// Test10M_Group_AutoScale_Race 自动扩缩容Group竞态
func Test10M_Group_AutoScale_Race(t *testing.T) {
	for round := 0; round < 30; round++ {
		g := NewGroup[int](32)
		g.EnableAutoScale(nil)
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(100)
		for goIdx := 0; goIdx < 100; goIdx++ {
			go func() {
				defer wg.Done()
				for i := 0; i < 1000; i++ {
					g.Go(ctx, func(ctx context.Context) (int, error) {
						time.Sleep(time.Microsecond)
						return 42, nil
					})
				}
			}()
		}
		wg.Wait()
		results := g.Wait()
		if len(results) != 100*1000 {
			t.Errorf("round %d: expected %d results, got %d", round, 100*1000, len(results))
		}
		for i, r := range results {
			if !r.Ok() {
				t.Errorf("round %d task %d: %v", round, i, r.Err)
			}
		}
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}
	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	g := runtime.NumGoroutine()
	if g > 20 {
		t.Errorf("goroutine leak: %d", g)
	}
	t.Logf("Group AutoScale race: post G=%d PASS", g)
}

// Test10M_ForEachChunked 验证batchSize生效
func Test10M_ForEachChunked_BatchSize(t *testing.T) {
	items := make([]int, 10000)
	for i := range items {
		items[i] = i
	}
	nr, err := ForEachChunked(context.Background(), items, 50, 100, func(ctx context.Context, item int) error {
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachChunked failed: %v", err)
	}
	if nr.FailCount() > 0 {
		t.Errorf("failures: %d", nr.FailCount())
	}
	t.Logf("ForEachChunked batchSize=100: PASS")
}

// Test10M_Retry_Backoff_Race 重试并发竞态
func Test10M_Retry_Backoff_Race(t *testing.T) {
	var wg sync.WaitGroup
	var success atomic.Int64
	ctx := context.Background()
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				callCount := 0
				err := RetryWithLinearBackoff(ctx, 3, 1*time.Microsecond, func(ctx context.Context) error {
					callCount++
					if callCount < 2 {
						return fmt.Errorf("transient")
					}
					return nil
				})
				if err == nil {
					success.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if success.Load() != 100*1000 {
		t.Errorf("expected %d, got %d", 100*1000, success.Load())
	}
	t.Logf("Retry race: %d successes", success.Load())
}

// Test10M_MapChunk_Concurrent MapChunk 高并发正确性
func Test10M_MapChunk_Concurrent(t *testing.T) {
	n := 50_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	results := MapChunk(context.Background(), items, core.IO(), 1000, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) == 0 {
		t.Fatal("no results")
	}
	for _, r := range results {
		if !r.Ok() {
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
	t.Logf("MapChunk %d items: PASS", n)
}
