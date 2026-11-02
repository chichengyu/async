package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/test"
)

func TestShardedRoundRobinCounterCrossesMaxInt(t *testing.T) {
	start := uint64(math.MaxInt) + 1
	t.Run("rate acquire", func(t *testing.T) {
		l := NewShardedRateLimiter(3, 3, time.Hour)
		defer l.Close()
		l.nextIdx.Store(start)
		if err := l.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
		l.Release()
	})
	t.Run("rate token", func(t *testing.T) {
		l := NewShardedRateLimiter(3, 3, time.Hour)
		defer l.Close()
		l.nextIdx.Store(start)
		token, err := l.Token(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		token.Release()
	})
	t.Run("bucket", func(t *testing.T) {
		l := NewShardedTokenBucket(3, 1, 2)
		l.nextIdx.Store(start)
		if !l.Allow() || !l.AllowN(1) {
			t.Fatal("expected tokens")
		}
	})
	t.Run("window", func(t *testing.T) {
		l := NewShardedSlidingWindowRateLimiter(3, 6, time.Hour)
		l.nextIdx.Store(start)
		if !l.Allow() || !l.AllowN(1) {
			t.Fatal("expected capacity")
		}
	})
	t.Run("adaptive", func(t *testing.T) {
		l := NewShardedAdaptiveRateLimiter(3, 3, 3)
		l.nextIdx.Store(start)
		l.recordIdx.Store(start)
		if err := l.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
		l.RecordOk()
		l.RecordFailure()
		l.Release()
	})
}

func TestShardedBuilderDefaultShardPaths(t *testing.T) {
	b := NewRatelimitBuilder(context.Background()).Sharded()
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) {
			want := core.IO()
			if want < 2 {
				want = 2
			}
			if got := DefaultShardCount(); got != want {
				t.Fatalf("DefaultShardCount = %d, want %d", got, want)
			}
			rate := b.RateLimiter().Rate(2).Per(time.Hour).Shards(0)
			bucket := b.TokenBucket().Rate(1).Capacity(1).Shards(0)
			window := b.SlidingWindow().Limit(2).Window(time.Hour).Shards(0)
			adaptive := b.Adaptive().MinWorker(1).MaxWorker(2).Shards(0)
			if explicit {
				rate.DefaultShards()
				bucket.DefaultShards()
				window.DefaultShards()
				adaptive.DefaultShards()
			}
			rl := rate.Build()
			t.Cleanup(rl.Close)
			al := adaptive.Build()
			t.Cleanup(al.Close)
			for name, got := range map[string]int{
				"rate": rl.ShardCount(), "bucket": bucket.Build().ShardCount(),
				"window": window.Build().ShardCount(), "adaptive": al.ShardCount(),
			} {
				if got != want {
					t.Errorf("%s shards = %d, want %d", name, got, want)
				}
			}
		})
	}
}

func TestRateLimiterConcurrentResizeAcquireClose(t *testing.T) {
	for iteration := 0; iteration < 25; iteration++ {
		rl := NewRateLimiter(4, time.Millisecond)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var ready sync.WaitGroup
		ready.Add(9)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ready.Done()
				for j := 0; j < 50; j++ {
					err := rl.Acquire(context.Background())
					if errors.Is(err, core.ErrRateLimiterStopped) {
						return
					}
					if err != nil {
						t.Errorf("Acquire: %v", err)
						return
					}
					rl.Release()
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ready.Done()
			for j := 0; j < 50; j++ {
				rl.Resize(1 + j%8)
			}
		}()
		close(start)
		ready.Wait()
		rl.Close()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: concurrent operations did not finish", iteration)
		}
	}
}

func TestSlidingWindowReclaimsExpiredHistoryWithoutLosingLiveRequests(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%t", batch), func(t *testing.T) {
			sw := NewSlidingWindowRateLimiter(3, time.Hour)
			now := time.Now()
			sw.timestamps = make([]time.Time, 4098, 8192)
			for i := 0; i < 4096; i++ {
				sw.timestamps[i] = now.Add(-2 * time.Hour)
			}
			sw.timestamps[4096], sw.timestamps[4097] = now, now
			allowed := false
			if batch {
				allowed = sw.AllowN(1)
			} else {
				allowed = sw.Allow()
			}
			if !allowed || sw.Allow() {
				t.Fatal("expired history must leave exactly one available request")
			}
			if len(sw.timestamps) != 3 || cap(sw.timestamps) > 1024 {
				t.Fatalf("live timestamps: len=%d cap=%d", len(sw.timestamps), cap(sw.timestamps))
			}
		})
	}
}

func TestSlidingWindowAllowNOverflowRegression(t *testing.T) {
	sw := NewSlidingWindowRateLimiter(2, time.Hour)
	if !sw.Allow() {
		t.Fatal("first request rejected")
	}
	if sw.AllowN(math.MaxInt) {
		t.Fatal("overflowing request accepted")
	}
	if !sw.AllowN(1) || sw.Allow() {
		t.Fatal("rejected request changed the window")
	}
	if !NewSlidingWindowRateLimiter(-1, time.Hour).AllowN(0) {
		t.Fatal("zero request must always succeed")
	}
}

func TestSlidingWindowConcurrentMixedRequestsRespectLimit(t *testing.T) {
	const limit = 100
	sw := NewSlidingWindowRateLimiter(limit, time.Hour)
	start := make(chan struct{})
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				if sw.Allow() {
					accepted.Add(1)
				}
			} else if sw.AllowN(3) {
				accepted.Add(3)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	got := accepted.Load()
	if got > limit {
		t.Fatalf("accepted %d requests, limit %d", got, limit)
	}
	for got < limit && sw.Allow() {
		got++
	}
	if got != limit {
		t.Fatalf("only %d requests could be accepted, want %d", got, limit)
	}
	if sw.Allow() {
		t.Fatal("allowed request beyond window limit")
	}
}

func TestShardedSlidingWindowConcurrentMixedRequestsRespectLimit(t *testing.T) {
	const shards, limit = 4, 100
	sw := NewShardedSlidingWindowRateLimiter(shards, limit, time.Hour)
	start := make(chan struct{})
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				if sw.Allow() {
					accepted.Add(1)
				}
			} else if sw.AllowN(3) {
				accepted.Add(3)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	got := accepted.Load()
	if got > limit {
		t.Fatalf("accepted %d requests, limit %d", got, limit)
	}
	for i := 0; i < shards; i++ {
		for sw.GetShard(i).Allow() {
			got++
		}
	}
	if got != limit {
		t.Fatalf("accepted plus remaining capacity = %d, want %d", got, limit)
	}
}

// ============================================================
// 一、RateLimiter 完整方法测试
// ============================================================

func TestRateLimiter_NewAndSize(t *testing.T) {
	rl := NewRateLimiter(100, time.Second)
	defer rl.Close()
	if rl.Size() != 100 {
		t.Fatalf("expected size 100, got %d", rl.Size())
	}
}

func TestTokenBucketNonpositiveDoesNotMintRegression(t *testing.T) {
	tb := NewTokenBucket(0, 1)
	if !tb.Allow() {
		t.Fatal("missing initial token")
	}
	for _, n := range []float64{0, -2, -100} {
		if !tb.AllowN(n) || tb.Allow() {
			t.Fatalf("request %v changed an exhausted bucket", n)
		}
	}
}

func TestShardedAdaptiveCanceledAcquireReleaseRegression(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(2, 2, 2)
	defer sa.Close()
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := sa.Acquire(ctx); err != nil {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := sa.Acquire(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("exhausted acquire error=%v", err)
	}
	sa.Release()
	sa.Release()
	if err := sa.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	sa.Release()
	for i := 0; i < 2; i++ {
		if available := sa.GetShard(i).base.Available(); available != 1 {
			t.Fatalf("shard %d lost its slot after balanced acquire/release: available=%d", i, available)
		}
	}
}

func TestShardedRateLimiterConcurrentAcquireRelease(t *testing.T) {
	const shards, capacity = 8, 64
	sl := NewShardedRateLimiter(shards, capacity, time.Hour).WithStrategy(Reject)
	defer sl.Close()
	var wg sync.WaitGroup
	var acquired atomic.Int64
	start := make(chan struct{})
	for i := 0; i < capacity; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := sl.Acquire(context.Background()); err == nil {
				acquired.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := acquired.Load(); got != capacity {
		t.Fatalf("acquired = %d, want %d", got, capacity)
	}
	if got := sl.Available(); got != 0 {
		t.Fatalf("available after acquire = %d", got)
	}
	start = make(chan struct{})
	for i := 0; i < capacity; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; sl.Release() }()
	}
	close(start)
	wg.Wait()
	for i := 0; i < shards; i++ {
		if got := sl.GetShard(i).Available(); got != capacity/shards {
			t.Fatalf("shard %d available = %d, want %d", i, got, capacity/shards)
		}
	}
}

func TestShardedRateLimiterFailedAcquireReleaseRegression(t *testing.T) {
	sl := NewShardedRateLimiter(2, 2, time.Hour).WithStrategy(Reject)
	defer sl.Close()
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := sl.Acquire(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := sl.Acquire(ctx); err == nil {
		t.Fatal("exhausted shard accepted an acquire")
	}
	sl.Release()
	sl.Release()
	if err := sl.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	sl.Release()
	for i := 0; i < 2; i++ {
		if available := sl.GetShard(i).Available(); available != 1 {
			t.Fatalf("shard %d lost its slot after balanced acquire/release: available=%d", i, available)
		}
	}
}

func TestShardedRateLimiterMixedTokenAndAcquire(t *testing.T) {
	sl := NewShardedRateLimiter(2, 4, time.Hour).WithStrategy(Reject)
	defer sl.Close()
	ctx := context.Background()
	token, err := sl.Token(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := sl.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sl.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); token.Release() }()
	go func() { defer wg.Done(); token.Release() }()
	go func() { defer wg.Done(); sl.Release() }()
	wg.Wait()
	if got := sl.Available(); got != 3 {
		t.Fatalf("available = %d, want 3 with one acquire outstanding", got)
	}
	sl.Release()
	for i := 0; i < 2; i++ {
		if got := sl.GetShard(i).Available(); got != 2 {
			t.Fatalf("shard %d available = %d, want 2", i, got)
		}
	}
}

func TestShardedRateLimiterResizeWithOutstandingRegression(t *testing.T) {
	sl := NewShardedRateLimiter(2, 2, time.Hour).WithStrategy(Reject)
	defer sl.Close()
	for i := 0; i < 2; i++ {
		if err := sl.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := sl.Acquire(context.Background()); err == nil {
		t.Fatal("exhausted acquire unexpectedly succeeded")
	}
	sl.Resize(4)
	sl.Release()
	sl.Release()
	if err := sl.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	sl.Release()
	for i := 0; i < 2; i++ {
		if available := sl.GetShard(i).Available(); available != 2 {
			t.Fatalf("shard %d after resize: available=%d, want 2", i, available)
		}
	}
}

func TestShardedAdaptiveAdjustmentWithOutstandingRegression(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(2, 2, 100)
	defer sa.Close()
	perShard := sa.GetShard(0).base.Size()
	for i := 0; i < perShard*2; i++ {
		if err := sa.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sa.Acquire(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("exhausted acquire error=%v", err)
	}
	for i := 0; i < 20; i++ {
		sa.RecordFailure()
	}
	for i := 0; i < 2; i++ {
		if size := sa.GetShard(i).base.Size(); size >= perShard {
			t.Fatalf("shard %d did not shrink: size=%d initial=%d", i, size, perShard)
		}
	}
	for i := 0; i < perShard*2; i++ {
		sa.Release()
	}
	if err := sa.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	sa.Release()
	for i := 0; i < 2; i++ {
		shard := sa.GetShard(i)
		if size, available := shard.base.Size(), shard.base.Available(); available != size {
			t.Fatalf("shard %d after adjustment: size=%d available=%d", i, size, available)
		}
	}
}

func TestShardedAdaptiveConcurrentReleaseAfterShrink(t *testing.T) {
	const shards = 4
	sa := NewShardedAdaptiveRateLimiter(shards, shards, 124)
	defer sa.Close()
	initial := sa.GetShard(0).base.Size()
	for i := 0; i < shards*initial; i++ {
		if err := sa.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < shards*10; i++ {
		sa.RecordFailure()
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < shards*initial; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; sa.Release() }()
	}
	close(start)
	wg.Wait()
	for i := 0; i < shards; i++ {
		shard := sa.GetShard(i).base
		if size, available := shard.Size(), shard.Available(); available != size || size >= initial {
			t.Fatalf("shard %d: initial=%d size=%d available=%d", i, initial, size, available)
		}
	}
}

func TestRateLimiterSlowRefillRegression(t *testing.T) {
	rl := NewRateLimiter(1, 10*time.Second).WithStrategy(Reject)
	defer rl.Close()
	if err := rl.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := rl.Acquire(context.Background()); err == nil {
		t.Fatal("1/10s limiter replenished a token after only 1.1s")
	}
}

func TestRateLimiterResizeChangesRefillCadenceRegression(t *testing.T) {
	rl := NewRateLimiter(1, time.Second)
	defer rl.Close()
	if err := rl.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	rl.Resize(10)
	for i := 0; i < 9; i++ {
		if err := rl.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("new 100ms refill cadence not applied: %v", err)
	}
}

func TestRateLimiterElapsedRefillAndResizeRegression(t *testing.T) {
	rl := newRateLimiterSimple(10)
	defer rl.Close()
	rl.perDuration = 10 * time.Second
	rl.rate.Store(10)
	now := time.Now()
	rl.refillLast = now
	for i := 0; i < 10; i++ {
		<-rl.tokens
	}
	for i := 1; i <= 9; i++ {
		if n := rl.refillLocked(now.Add(time.Duration(i) * 100 * time.Millisecond)); n != 0 {
			t.Fatalf("early token at tick %d", i)
		}
	}
	if n := rl.refillLocked(now.Add(time.Second)); n != 1 {
		t.Fatalf("fractional credit lost: added=%d", n)
	}
	<-rl.tokens
	// Use the same rate boundary calculation as Resize without a wall-clock delay.
	rl.rate.Store(100)
	if n := rl.refillLocked(now.Add(1100 * time.Millisecond)); n != 1 {
		t.Fatalf("new rate not applied to elapsed time: added=%d", n)
	}
	rl.refillLocked(now.Add(100 * time.Second))
	if rl.refillCredit != 0 || len(rl.tokens) != cap(rl.tokens) {
		t.Fatal("full bucket banked excess credit")
	}
	for len(rl.tokens) > 0 {
		<-rl.tokens
	}
	if n := rl.refillLocked(now.Add(100*time.Second + time.Millisecond)); n != 0 {
		t.Fatal("full bucket credit leaked into a future burst")
	}
}

func TestRateLimiterConcurrentSizeResizeRegression(t *testing.T) {
	rl := NewRateLimiter(1, time.Hour)
	defer rl.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			rl.Resize(1 + i%2)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if size := rl.Size(); size < 1 || size > 2 {
				t.Errorf("invalid size %d", size)
			}
		}
	}()
	wg.Wait()
}

func TestRateLimiter_NewZeroRate(t *testing.T) {
	rl := NewRateLimiter(0, time.Second)
	defer rl.Close()
	if rl.Size() != core.IO() {
		t.Fatalf("expected size core.IO()=%d, got %d", core.IO(), rl.Size())
	}
}

func TestRateLimiter_NewNegativeRate(t *testing.T) {
	rl := NewRateLimiter(-5, time.Second)
	defer rl.Close()
	if rl.Size() != core.IO() {
		t.Fatalf("expected size core.IO()=%d, got %d", core.IO(), rl.Size())
	}
}

func TestRateLimiter_NewWithBurst(t *testing.T) {
	rl := NewRateLimiterWithBurst(50, time.Second, 200)
	defer rl.Close()
	if rl.Size() != 50 {
		t.Fatalf("expected size 50, got %d", rl.Size())
	}
	if avail := rl.Available(); avail != 200 {
		t.Fatalf("available after burst init = %d, want 200", avail)
	}
}

func TestRateLimiter_NewWithBurstLessThanRate(t *testing.T) {
	rl := NewRateLimiterWithBurst(100, time.Second, 10)
	defer rl.Close()
	if rl.Size() != 100 {
		t.Fatalf("expected size 100, got %d", rl.Size())
	}
	if got := rl.Available(); got != 100 {
		t.Fatalf("initial available = %d, want 100", got)
	}
}

func TestRateLimiter_NewWithNonPositiveBurst(t *testing.T) {
	for _, burst := range []int{0, -1} {
		rl := NewRateLimiterWithBurst(10, time.Hour, burst)
		if got := rl.Available(); got != 10 {
			rl.Close()
			t.Fatalf("burst %d: initial available = %d, want 10", burst, got)
		}
		rl.Close()
	}
}

func TestRateLimiter_NewWithBurstZeroRate(t *testing.T) {
	rl := NewRateLimiterWithBurst(0, time.Second, 50)
	defer rl.Close()
	if rl.Size() != core.IO() {
		t.Fatalf("expected size core.IO()=%d, got %d", core.IO(), rl.Size())
	}
}

func TestRateLimiter_AcquireRelease(t *testing.T) {
	rl := NewRateLimiter(10, time.Hour)
	defer rl.Close()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := rl.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
	}
	for i := 0; i < 10; i++ {
		rl.Release()
	}
	if got := rl.Available(); got != 10 {
		t.Fatalf("available after release = %d, want 10", got)
	}
}

func TestRateLimiter_Wait(t *testing.T) {
	rl := NewRateLimiter(5, time.Second)
	defer rl.Close()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := rl.Wait(ctx); err != nil {
			t.Fatalf("wait %d failed: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		rl.Release()
	}
}

func TestRateLimiter_Token(t *testing.T) {
	rl := NewRateLimiter(5, time.Second)
	defer rl.Close()
	ctx := context.Background()
	tok, err := rl.Token(ctx)
	if err != nil {
		t.Fatalf("token acquire failed: %v", err)
	}
	tok.Release()

	tok2, err := rl.Token(ctx)
	if err != nil {
		t.Fatalf("token2 acquire failed: %v", err)
	}
	tok2.Release()
}

func TestRateLimiterTokenAcquisitionErrors(t *testing.T) {
	t.Run("canceled while exhausted", func(t *testing.T) {
		rl := newRateLimiterSimple(1)
		defer rl.Close()
		held, err := rl.Token(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		token, err := rl.Token(ctx)
		if token != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("Token = (%v, %v), want (nil, canceled)", token, err)
		}
		if got := rl.Available(); got != 0 {
			t.Fatalf("available = %d, want 0 while token is held", got)
		}
	})
	t.Run("closed", func(t *testing.T) {
		rl := newRateLimiterSimple(1)
		rl.Close()
		token, err := rl.Token(context.Background())
		if token != nil || !errors.Is(err, core.ErrRateLimiterStopped) {
			t.Fatalf("Token = (%v, %v), want (nil, stopped)", token, err)
		}
	})
}

func TestRateLimiter_TokenDoubleRelease(t *testing.T) {
	rl := NewRateLimiter(5, time.Second)
	defer rl.Close()
	ctx := context.Background()
	tok, err := rl.Token(ctx)
	if err != nil {
		t.Fatalf("token acquire failed: %v", err)
	}
	tok.Release()
	tok.Release()
}

func TestRateLimiterTokenConcurrentRelease(t *testing.T) {
	rl := newRateLimiterSimple(2)
	defer rl.Close()
	tok, err := rl.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	other, err := rl.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Release()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tok.Release()
		}()
	}
	close(start)
	wg.Wait()
	if got := rl.Available(); got != 1 {
		t.Fatalf("available = %d, want 1 while another token remains held", got)
	}
	if tok.limiter != nil {
		t.Fatal("released token still retains the limiter")
	}
}

func TestRateLimiterTokenConcurrentCloseAndRelease(t *testing.T) {
	rl := newRateLimiterSimple(1)
	tok, err := rl.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tok.Release()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		rl.Close()
	}()
	close(start)
	wg.Wait()
	tok.Release()
	if tok.limiter != nil {
		t.Fatal("released token still retains the limiter")
	}
	if err := rl.Acquire(context.Background()); !errors.Is(err, core.ErrRateLimiterStopped) {
		t.Fatalf("Acquire after Close = %v, want stopped", err)
	}
}

func TestRateLimiter_Available(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	defer rl.Close()
	avail := rl.Available()
	if avail < 0 || avail > 10 {
		t.Fatalf("expected available 0-10, got %d", avail)
	}
}

func TestRateLimiter_Size(t *testing.T) {
	rl := NewRateLimiter(7, time.Second)
	defer rl.Close()
	if rl.Size() != 7 {
		t.Fatalf("expected size 7, got %d", rl.Size())
	}
}

func TestRateLimiter_Resize(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	defer rl.Close()
	rl.Resize(20)
	if rl.Size() != 20 {
		t.Fatalf("expected size 20 after resize, got %d", rl.Size())
	}
}

func TestRateLimiter_ResizeToSame(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	defer rl.Close()
	rl.Resize(10)
	if rl.Size() != 10 {
		t.Fatalf("expected size 10, got %d", rl.Size())
	}
	rl.Resize(0)
	if rl.Size() != 10 {
		t.Fatalf("expected size 10 after Resize(0), got %d", rl.Size())
	}
	rl.Resize(-1)
	if rl.Size() != 10 {
		t.Fatalf("expected size 10 after Resize(-1), got %d", rl.Size())
	}
}

func TestRateLimiter_ResizeAfterClose(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	rl.Close()
	rl.Resize(20)
	if rl.Size() != 10 {
		t.Fatalf("expected size unchanged (10), got %d", rl.Size())
	}
}

func TestRateLimiter_ResizeGrowShrink(t *testing.T) {
	rl := NewRateLimiter(50, time.Second)
	defer rl.Close()

	rl.Resize(200)
	if rl.Size() != 200 {
		t.Fatalf("expected 200 after grow, got %d", rl.Size())
	}

	rl.Resize(10)
	if rl.Size() != 10 {
		t.Fatalf("expected 10 after shrink, got %d", rl.Size())
	}

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := rl.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d after shrink failed: %v", i, err)
		}
	}
}

func TestRateLimiter_WithTraceID(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	defer rl.Close()
	_, ctx := rl.WithTraceID(context.Background())
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("acquire with trace id failed: %v", err)
	}
	rl.Release()
}

func TestRateLimiterConcurrentWithTraceID(t *testing.T) {
	rl := NewRateLimiter(1, time.Hour)
	defer rl.Close()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ctx := rl.WithTraceID(context.Background())
			if ctx == nil {
				t.Error("WithTraceID returned nil context")
			}
		}()
	}
	wg.Wait()
}

func TestRateLimiter_WithTraceIDMultiple(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	defer rl.Close()
	rl2, ctx := rl.WithTraceID(context.Background())
	if rl2 != rl {
		t.Fatal("WithTraceID should return same instance")
	}
	if ctx == nil {
		t.Fatal("ctx should not be nil")
	}
}

func TestRateLimiter_Stop(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	rl.Stop()
	ctx := context.Background()
	if err := rl.Acquire(ctx); err == nil {
		t.Fatal("expected error after Stop")
	}
}

type acquireReadyContext struct {
	context.Context
	ready chan struct{}
}

func (c *acquireReadyContext) Value(key any) any { return nil }

func (c *acquireReadyContext) AfterFunc(f func()) func() bool {
	stop := context.AfterFunc(c.Context, f)
	close(c.ready)
	return stop
}

func TestRateLimiterCanceledWaiterDoesNotPreventAnotherAcquire(t *testing.T) {
	rl := NewRateLimiter(1, time.Hour)
	defer rl.Close()
	if err := rl.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceled := make(chan error, 1)
	remaining := make(chan error, 1)
	first := &acquireReadyContext{Context: ctx, ready: make(chan struct{})}
	otherCtx, otherCancel := context.WithCancel(context.Background())
	defer otherCancel()
	second := &acquireReadyContext{Context: otherCtx, ready: make(chan struct{})}
	go func() { canceled <- rl.Acquire(first) }()
	go func() { remaining <- rl.Acquire(second) }()
	<-first.ready
	<-second.ready
	// After registration, acquiring mu confirms both calls released it in cond.Wait.
	rl.mu.Lock()
	rl.mu.Unlock()
	cancel()
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Acquire = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}
	rl.Release()
	select {
	case err := <-remaining:
		if err != nil {
			t.Fatalf("remaining Acquire = %v", err)
		}
		rl.Release()
	case <-time.After(time.Second):
		t.Fatal("remaining waiter did not acquire released permit")
	}
}

func TestRateLimiterConcurrentCloseWakesWaiters(t *testing.T) {
	for _, strategy := range []Strategy{Block, BlockForce} {
		rl := NewRateLimiter(1, time.Hour).WithStrategy(strategy)
		if err := rl.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
		const waiters = 32
		errs := make(chan error, waiters)
		for i := 0; i < waiters; i++ {
			go func() { errs <- rl.Acquire(context.Background()) }()
		}
		var closers sync.WaitGroup
		closers.Add(4)
		for i := 0; i < 4; i++ {
			go func() { defer closers.Done(); rl.Close() }()
		}
		closers.Wait()
		for i := 0; i < waiters; i++ {
			select {
			case err := <-errs:
				if !errors.Is(err, core.ErrRateLimiterStopped) {
					t.Fatalf("strategy %v: Acquire returned %v", strategy, err)
				}
			case <-time.After(time.Second):
				t.Fatalf("strategy %v: waiter did not return", strategy)
			}
		}
	}
}

func TestRateLimiterConcurrentCloseWaitsForCleanup(t *testing.T) {
	for _, simple := range []bool{false, true} {
		t.Run(fmt.Sprintf("simple=%v", simple), func(t *testing.T) {
			var rl *RateLimiter
			if simple {
				rl = newRateLimiterSimple(1)
			} else {
				rl = NewRateLimiter(1, time.Hour)
			}
			rl.resizeMu.Lock()
			firstDone := make(chan struct{})
			go func() { rl.Close(); close(firstDone) }()
			deadline := time.Now().Add(5 * time.Second)
			for !rl.closed.Load() && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			if !rl.closed.Load() {
				rl.resizeMu.Unlock()
				<-firstDone
				t.Fatal("Close did not start")
			}
			secondDone := make(chan struct{})
			go func() { rl.Stop(); close(secondDone) }()
			select {
			case <-secondDone:
				rl.resizeMu.Unlock()
				<-firstDone
				t.Fatal("second Close returned before cleanup")
			case <-time.After(50 * time.Millisecond):
			}
			rl.resizeMu.Unlock()
			for _, done := range []<-chan struct{}{firstDone, secondDone} {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("Close did not finish")
				}
			}
		})
	}
}

func TestRateLimiter_CloseMultipleSafety(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	rl.Close()
	rl.Close()
	rl.Close()
	ctx := context.Background()
	if err := rl.Acquire(ctx); err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestRateLimiter_ReleaseAfterClose(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	ctx := context.Background()
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	rl.Close()
	rl.Release()
}

// ============================================================
// 二、RateLimiter 策略测试 (Block / Reject / BlockForce)
// ============================================================

func TestRateLimiter_BlockStrategy(t *testing.T) {
	rl := NewRateLimiter(1, time.Second)
	rl.WithStrategy(Block)
	defer rl.Close()
	ctx := context.Background()
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- rl.Acquire(ctx) }()
	time.Sleep(50 * time.Millisecond)
	rl.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second acquire: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second acquire didn't complete in time")
	}
}

func TestRateLimiter_BlockStrategy_CtxCancel(t *testing.T) {
	rl := NewRateLimiter(1, time.Hour)
	rl.WithStrategy(Block)
	defer rl.Close()
	ctx, cancel := context.WithCancel(context.Background())
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	done := make(chan error)
	go func() {
		done <- rl.Acquire(ctx)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Acquire = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire didn't cancel in time")
	}
}

func TestRateLimiter_RejectStrategy(t *testing.T) {
	rl := newRateLimiterSimple(2)
	defer rl.Close()
	rl.WithStrategy(Reject)
	ctx := context.Background()

	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}
	if err := rl.Acquire(ctx); err == nil {
		t.Fatal("third acquire should be rejected (no tokens left)")
	} else if !errors.Is(err, core.ErrRateLimitExceeded) {
		t.Fatalf("expected ErrRateLimitExceeded, got: %v", err)
	}
	rl.Release()
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("acquire after release should succeed, got: %v", err)
	}
}

func TestRateLimiter_BlockForceStrategy(t *testing.T) {
	rl := NewRateLimiter(1, time.Second)
	rl.WithStrategy(BlockForce)
	defer rl.Close()
	ctx := context.Background()
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	done := make(chan bool)
	go func() {
		if err := rl.Acquire(ctx); err != nil {
			t.Errorf("blockforce acquire failed: %v", err)
		}
		done <- true
	}()
	time.Sleep(50 * time.Millisecond)
	rl.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("BlockForce acquire didn't complete")
	}
}

func TestRateLimiter_BlockForceIgnoresCancel(t *testing.T) {
	rl := NewRateLimiter(1, time.Second)
	rl.WithStrategy(BlockForce)
	defer rl.Close()
	ctx, cancel := context.WithCancel(context.Background())
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	done := make(chan error)
	go func() {
		done <- rl.Acquire(ctx)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("BlockForce should ignore context cancel")
	default:
	}
	rl.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("BlockForce acquire after release failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("BlockForce acquire didn't complete after release")
	}
}

type closeWaitContext struct {
	context.Context
	registered chan struct{}
	once       sync.Once
}

func (c *closeWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.registered) })
	return c.Context.Done()
}

func TestRateLimiterAcquireWaiterAfterChannelClose(t *testing.T) {
	rl := NewRateLimiter(1, time.Hour)
	if err := rl.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := &closeWaitContext{Context: context.Background(), registered: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- rl.Acquire(ctx) }()
	<-ctx.registered
	rl.mu.Lock()
	rl.closed.Store(true)
	close(rl.refillStop)
	rl.resizeMu.Lock()
	close(rl.tokens)
	rl.resizeMu.Unlock()
	rl.cond.Broadcast()
	rl.mu.Unlock()
	<-rl.refillDone
	select {
	case err := <-done:
		if !errors.Is(err, core.ErrRateLimiterStopped) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting Acquire did not return after Close")
	}
}

func TestRateLimiterCloseWakesWaitingAcquire(t *testing.T) {
	rl := NewRateLimiter(1, time.Hour)
	if err := rl.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := &closeWaitContext{Context: context.Background(), registered: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- rl.Acquire(ctx) }()
	<-ctx.registered
	rl.Close()
	select {
	case err := <-done:
		if !errors.Is(err, core.ErrRateLimiterStopped) {
			t.Fatalf("Acquire after Close: got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting Acquire did not return after Close")
	}
}

func TestShardedRateLimiterCloseWakesAllWaitingAcquires(t *testing.T) {
	rl := NewShardedRateLimiter(2, 2, time.Hour)
	t.Cleanup(rl.Close)
	for i := 0; i < 2; i++ {
		if err := rl.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		ctx := &closeWaitContext{Context: context.Background(), registered: make(chan struct{})}
		go func() { done <- rl.Acquire(ctx) }()
		select {
		case <-ctx.registered:
		case <-time.After(5 * time.Second):
			t.Fatal("Acquire did not begin waiting")
		}
	}
	rl.Close()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, core.ErrRateLimiterStopped) {
				t.Fatalf("Acquire after Close: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waiting Acquire did not return after Close")
		}
	}
	rl.Release()
	rl.Release()
}

func TestRateLimiter_AcquireAfterClose(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	rl.Close()
	ctx := context.Background()
	err := rl.Acquire(ctx)
	if err == nil {
		t.Fatal("expected error after close")
	}
	if !errors.Is(err, core.ErrRateLimiterStopped) {
		t.Fatalf("Acquire after Close = %v, want %v", err, core.ErrRateLimiterStopped)
	}
}

// ============================================================
// 三、RateLimiter Resize 并发场景测试
// ============================================================

func TestRateLimiter_AcquireRelease_DuringResize(t *testing.T) {
	for round := 0; round < 5; round++ {
		rl := NewRateLimiter(50, time.Second)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 200
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				if err := rl.Acquire(ctx); err == nil {
					time.Sleep(time.Microsecond)
					rl.Release()
				}
			}()
		}
		go func() {
			for i := 0; i < 10; i++ {
				rl.Resize(30 + i*10)
				time.Sleep(time.Millisecond)
			}
		}()
		wg.Wait()
		rl.Close()
	}
}

// ============================================================
// 四、TokenBucket 完整测试
// ============================================================

func TestTokenBucket_Basic(t *testing.T) {
	tb := NewTokenBucket(100, 10)
	for i := 0; i < 10; i++ {
		if !tb.Allow() {
			t.Fatalf("allow %d failed", i)
		}
	}
}

func TestTokenBucket_Exhausted(t *testing.T) {
	tb := NewTokenBucket(10, 5)
	allowed := 0
	for i := 0; i < 100; i++ {
		if tb.Allow() {
			allowed++
		}
	}
	if allowed < 5 {
		t.Fatalf("should allow at least 5 tokens, got %d", allowed)
	}
	t.Logf("Initial burst: %d tokens used", allowed)
}

func TestTokenBucket_AllowN(t *testing.T) {
	tb := NewTokenBucket(100, 20)
	if !tb.AllowN(5) {
		t.Fatal("expected allow 5 tokens")
	}
	if !tb.AllowN(0) {
		t.Fatal("AllowN(0) should always return true")
	}
}

func TestTokenBucketConcurrentMixedRequestsRespectCapacity(t *testing.T) {
	const capacity = 100
	tb := NewTokenBucket(0, capacity)
	var consumed atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			n := int64(i%3 + 1)
			if tb.AllowN(float64(n)) {
				consumed.Add(n)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if got := consumed.Load(); got > capacity || got < capacity-2 {
		t.Fatalf("consumed %d tokens, capacity %d", got, capacity)
	}
}

func TestShardedTokenBucketConcurrentCapacity(t *testing.T) {
	const shards, capacity = 4, 25
	bucket := NewShardedTokenBucket(shards, 0, capacity)
	start := make(chan struct{})
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < shards*capacity*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if bucket.Allow() {
				accepted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := accepted.Load(); got != shards*capacity {
		t.Fatalf("accepted %d requests, want %d", got, shards*capacity)
	}
	for i := 0; i < shards; i++ {
		if bucket.GetShard(i).Allow() {
			t.Errorf("shard %d retained a token", i)
		}
	}
}

func TestTokenBucket_AllowN_Negative(t *testing.T) {
	tb := NewTokenBucket(100, 10)
	if !tb.AllowN(-1) {
		t.Fatal("AllowN(-1) should return true")
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	tb := NewTokenBucket(1000, 5)
	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Fatalf("allow %d failed", i)
		}
	}
	if tb.Allow() {
		t.Log("token should be exhausted, but refill may have occurred")
	}
	time.Sleep(10 * time.Millisecond)
	allowed := false
	for i := 0; i < 100; i++ {
		if tb.Allow() {
			allowed = true
			break
		}
	}
	if !allowed {
		t.Log("no token after 10ms (rate too low for measurable refill)")
	}
}

// ============================================================
// 五、SlidingWindowRateLimiter 完整测试
// ============================================================

func TestSlidingWindowRateLimiter_Basic(t *testing.T) {
	sw := NewSlidingWindowRateLimiter(10, time.Second)
	for i := 0; i < 10; i++ {
		if !sw.Allow() {
			t.Fatalf("allow %d failed", i)
		}
	}
}

func TestSlidingWindowRateLimiter_Exceeded(t *testing.T) {
	sw := NewSlidingWindowRateLimiter(3, time.Second)
	allowed := 0
	for i := 0; i < 10; i++ {
		if sw.Allow() {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("expected 3 allowed, got %d", allowed)
	}
}

func TestSlidingWindowRateLimiter_AllowN(t *testing.T) {
	sw := NewSlidingWindowRateLimiter(10, time.Second)
	if !sw.AllowN(5) {
		t.Fatal("AllowN(5) should succeed when limit is 10")
	}
	if !sw.AllowN(5) {
		t.Fatal("AllowN(5) should succeed: 10/10 used")
	}
	if sw.AllowN(1) {
		t.Fatal("AllowN(1) should fail when all 10 tokens used")
	}
}

func TestSlidingWindowRateLimiter_AllowN_Zero(t *testing.T) {
	sw := NewSlidingWindowRateLimiter(5, time.Second)
	if !sw.AllowN(0) {
		t.Fatal("AllowN(0) should return true")
	}
	if !sw.AllowN(-1) {
		t.Fatal("AllowN(-1) should return true")
	}
}

func TestSlidingWindowRateLimiter_WindowExpiry(t *testing.T) {
	sw := NewSlidingWindowRateLimiter(5, 10*time.Millisecond)
	for i := 0; i < 5; i++ {
		if !sw.Allow() {
			t.Fatalf("allow %d failed", i)
		}
	}
	if sw.Allow() {
		t.Fatal("6th should be rejected")
	}
	time.Sleep(15 * time.Millisecond)
	if !sw.Allow() {
		t.Fatal("after window expiry, should allow")
	}
}

// ============================================================
// 六、AdaptiveRateLimiter 完整测试
// ============================================================

func TestAdaptiveRateLimiter_Basic(t *testing.T) {
	al := NewAdaptiveRateLimiter(5, 5)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := al.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
	}
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := al.Acquire(ctx2); err == nil {
		al.Release()
		t.Fatal("6th acquire should timeout when all tokens taken")
	}
}

func TestAdaptiveShrinkWaitsForOutstandingPermits(t *testing.T) {
	al := NewAdaptiveRateLimiter(1, 19)
	al.base.WithStrategy(Reject)
	for i := 0; i < 10; i++ {
		if err := al.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		al.RecordFailure()
	}
	if got := al.base.Size(); got != 5 {
		t.Fatalf("size = %d, want 5", got)
	}
	for i := 0; i < 5; i++ {
		al.Release()
		if err := al.Acquire(context.Background()); !errors.Is(err, core.ErrRateLimitExceeded) {
			t.Fatalf("after %d releases: Acquire = %v, want rate limit exceeded", i+1, err)
		}
	}
	al.Release()
	if err := al.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire after outstanding count fell below limit: %v", err)
	}
}

func TestAdaptiveConcurrentReleaseAfterShrink(t *testing.T) {
	al := NewAdaptiveRateLimiter(1, 127)
	for i := 0; i < 64; i++ {
		if err := al.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	al.base.Resize(8)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	acquired := make(chan error, 1)
	go func() { acquired <- al.Acquire(ctx) }()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; al.Release() }()
	}
	close(start)
	wg.Wait()
	if err := <-acquired; err != nil {
		t.Fatalf("waiting Acquire: %v", err)
	}
	if got := al.base.Available(); got != 3 {
		t.Fatalf("available = %d, want 3 after waiter acquired", got)
	}
}

func TestAdaptiveResizePreservesOutstandingAcrossChanges(t *testing.T) {
	al := NewAdaptiveRateLimiter(1, 19)
	al.base.WithStrategy(Reject)
	for i := 0; i < 10; i++ {
		if err := al.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	al.base.Resize(3)
	al.Release()
	al.Release()
	al.base.Resize(6)
	for i := 0; i < 2; i++ {
		al.Release()
		if err := al.Acquire(context.Background()); !errors.Is(err, core.ErrRateLimitExceeded) {
			t.Fatalf("after resize and %d releases: Acquire = %v", i+1, err)
		}
	}
	al.Release()
	if err := al.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire after outstanding count fell below limit: %v", err)
	}
}

func TestAdaptiveRateLimiterSmallRangeCanIncrease(t *testing.T) {
	for maxRate := 2; maxRate <= 9; maxRate++ {
		t.Run(fmt.Sprintf("max=%d", maxRate), func(t *testing.T) {
			al := NewAdaptiveRateLimiter(1, maxRate)
			for i := 0; i < maxRate*10; i++ {
				al.RecordOk()
			}
			if got := al.base.Size(); got != maxRate {
				t.Fatalf("size after successful calls = %d, want %d", got, maxRate)
			}
		})
	}
}

func TestAdaptiveConcurrentRecordsReachAdjustmentThreshold(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failures int
		wantSize int
	}{
		{name: "all_ok", wantSize: 11},
		{name: "mixed_at_threshold", failures: 2, wantSize: 10},
		{name: "mixed_above_threshold", failures: 3, wantSize: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for round := 0; round < 1000; round++ {
				al := NewAdaptiveRateLimiter(1, 19)
				start := make(chan struct{})
				var wg sync.WaitGroup
				wg.Add(10)
				for i := 0; i < 10; i++ {
					go func(fail bool) {
						defer wg.Done()
						<-start
						if fail {
							al.RecordFailure()
						} else {
							al.RecordOk()
						}
					}(i < tc.failures)
				}
				close(start)
				wg.Wait()
				if got := al.base.Size(); got != tc.wantSize {
					t.Fatalf("round %d: size = %d, want %d after %d failures", round, got, tc.wantSize, tc.failures)
				}
			}
		})
	}
}

func TestAdaptiveConcurrentAdjustmentReturnsAllPermits(t *testing.T) {
	const workers = 32
	const iterations = 100
	al := NewAdaptiveRateLimiter(2, 24)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if err := al.Acquire(ctx); err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				if (worker+j)%3 == 0 {
					al.RecordFailure()
				} else {
					al.RecordOk()
				}
				al.Release()
			}
		}(i)
	}
	wg.Wait()
	if size, available := al.base.Size(), al.base.Available(); size < 2 || size > 24 || available != size {
		t.Fatalf("size=%d available=%d", size, available)
	}
}

func TestAdaptiveRateLimiter_MinEqualsMax(t *testing.T) {
	al := NewAdaptiveRateLimiter(5, 5)
	ctx := context.Background()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordOk()
	al.Release()
}

func TestAdaptiveRateLimiter_MinGreaterThanMax(t *testing.T) {
	al := NewAdaptiveRateLimiter(10, 5)
	ctx := context.Background()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordOk()
	al.Release()
}

func TestAdaptiveRateLimiter_MinZero(t *testing.T) {
	al := NewAdaptiveRateLimiter(0, 5)
	ctx := context.Background()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordOk()
	al.Release()
}

func TestAdaptiveRateLimiter_RecordOk(t *testing.T) {
	al := NewAdaptiveRateLimiter(10, 100)
	ctx := context.Background()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordOk()
	al.Release()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire after success failed: %v", err)
	}
	al.RecordOk()
	al.Release()
}

func TestAdaptiveRateLimiter_RecordFailure(t *testing.T) {
	al := NewAdaptiveRateLimiter(10, 100)
	ctx := context.Background()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordFailure()
	al.Release()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire after failure failed: %v", err)
	}
	al.RecordFailure()
	al.Release()
}

func TestAdaptiveRateLimiter_RecordMixed(t *testing.T) {
	al := NewAdaptiveRateLimiter(10, 100)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if err := al.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		if i%2 == 0 {
			al.RecordOk()
		} else {
			al.RecordFailure()
		}
		al.Release()
	}
}

// ============================================================
// 七、ShardedRateLimiter 完整测试
// ============================================================

func TestShardedRateLimiter_Basic(t *testing.T) {
	sl := NewShardedRateLimiter(4, 100, time.Second)
	defer sl.Close()
	if sl.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sl.ShardCount())
	}
	if sl.TotalRate() < 1 {
		t.Fatalf("total rate should be > 0, got %d", sl.TotalRate())
	}
}

func TestShardedRateLimiter_ZeroShards(t *testing.T) {
	sl := NewShardedRateLimiter(0, 100, time.Second)
	defer sl.Close()
	if sl.ShardCount() < 2 {
		t.Fatalf("expected at least 2 shards, got %d", sl.ShardCount())
	}
}

func TestShardedRateLimiter_AcquireRelease(t *testing.T) {
	sl := NewShardedRateLimiter(4, 100, time.Second)
	defer sl.Close()
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if err := sl.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
	}
	for i := 0; i < 50; i++ {
		sl.Release()
	}
}

func TestShardedRateLimiter_Wait(t *testing.T) {
	sl := NewShardedRateLimiter(4, 50, time.Second)
	defer sl.Close()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := sl.Wait(ctx); err != nil {
			t.Fatalf("wait %d failed: %v", i, err)
		}
	}
	for i := 0; i < 10; i++ {
		sl.Release()
	}
}

func TestShardedRateLimiter_Token(t *testing.T) {
	sl := NewShardedRateLimiter(4, 100, time.Second)
	defer sl.Close()
	ctx := context.Background()
	tok, err := sl.Token(ctx)
	if err != nil {
		t.Fatalf("token acquire failed: %v", err)
	}
	tok.Release()
}

func TestShardedRateLimiter_WithBurst(t *testing.T) {
	sl := NewShardedRateLimiterWithBurst(4, 50, time.Second, 200)
	defer sl.Close()
	if sl.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sl.ShardCount())
	}
	if sl.Size() < 4 {
		t.Fatalf("expected size >= 4, got %d", sl.Size())
	}
}

func TestShardedRateLimiter_WithStrategy(t *testing.T) {
	sl := NewShardedRateLimiter(4, 100, time.Second)
	defer sl.Close()
	result := sl.WithStrategy(Reject)
	if result != sl {
		t.Fatal("WithStrategy should return same instance")
	}
	ctx := context.Background()
	for i := 0; i < sl.Size(); i++ {
		if err := sl.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
	}
	if err := sl.Acquire(ctx); err == nil {
		t.Fatal("expected rejection after all tokens consumed")
	}
}

func TestShardedRateLimiter_Size(t *testing.T) {
	sl := NewShardedRateLimiter(4, 100, time.Second)
	defer sl.Close()
	if sl.Size() < 4 {
		t.Fatalf("expected size >= 4, got %d", sl.Size())
	}
}

func TestShardedRateLimiter_Available(t *testing.T) {
	sl := NewShardedRateLimiter(4, 100, time.Second)
	defer sl.Close()
	avail := sl.Available()
	if avail < 0 {
		t.Fatalf("available should be >= 0, got %d", avail)
	}
}

func TestShardedSlidingWindowTotalLimitBelowShardCount(t *testing.T) {
	sw := NewShardedSlidingWindowRateLimiter(4, 2, time.Hour)
	allowed := 0
	for i := 0; i < 8; i++ {
		if sw.Allow() {
			allowed++
		}
	}
	if got := sw.TotalLimit(); got != allowed {
		t.Fatalf("TotalLimit = %d, actual allowed = %d", got, allowed)
	}
}

func TestShardedRateLimiterTotalRateBelowShardCount(t *testing.T) {
	sl := NewShardedRateLimiter(4, 2, time.Hour)
	defer sl.Close()
	if got, want := sl.TotalRate(), sl.Size(); got != want {
		t.Fatalf("initial TotalRate = %d, Size = %d", got, want)
	}
	sl.Resize(1)
	if got, want := sl.TotalRate(), sl.Size(); got != want {
		t.Fatalf("resized TotalRate = %d, Size = %d", got, want)
	}
}

func TestShardedRateLimiterTotalRateTracksShard(t *testing.T) {
	sl := NewShardedRateLimiterWithBurst(4, 2, time.Hour, 8)
	defer sl.Close()
	if got := sl.TotalRate(); got != 4 {
		t.Fatalf("initial TotalRate = %d, want 4", got)
	}
	sl.GetShard(0).Resize(3)
	if got := sl.TotalRate(); got != 6 {
		t.Fatalf("TotalRate after shard resize = %d, want 6", got)
	}
}

func TestShardedRateLimiterResizeAfterClose(t *testing.T) {
	sl := NewShardedRateLimiter(2, 10, time.Hour)
	sl.Close()
	sl.Resize(20)
	if got := sl.TotalRate(); got != 10 {
		t.Fatalf("TotalRate after Close and Resize = %d, want 10", got)
	}
	if got := sl.Size(); got != 10 {
		t.Fatalf("Size after Close and Resize = %d, want 10", got)
	}
}

func TestShardedRateLimiterResizeConcurrentClose(t *testing.T) {
	for i := 0; i < 100; i++ {
		sl := NewShardedRateLimiter(2, 10, time.Hour)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); sl.Resize(20) }()
		go func() { defer wg.Done(); sl.Close() }()
		wg.Wait()
		if got, want := sl.TotalRate(), sl.Size(); got != want {
			t.Fatalf("TotalRate = %d, Size = %d", got, want)
		}
	}
}

func TestShardedRateLimiterResizeNonPositive(t *testing.T) {
	sl := NewShardedRateLimiter(2, 10, time.Hour)
	defer sl.Close()
	for _, rate := range []int{0, -1} {
		sl.Resize(rate)
		if got := sl.TotalRate(); got != 10 {
			t.Fatalf("Resize(%d): TotalRate = %d, want 10", rate, got)
		}
		if got := sl.Size(); got != 10 {
			t.Fatalf("Resize(%d): Size = %d, want 10", rate, got)
		}
	}
}

func TestShardedRateLimiter_Resize(t *testing.T) {
	sl := NewShardedRateLimiter(4, 100, time.Second)
	defer sl.Close()
	sl.Resize(200)
	if sl.Size() < 4 {
		t.Fatalf("expected size >= 4 after resize, got %d", sl.Size())
	}
}

func TestShardedRateLimiter_GetShard(t *testing.T) {
	sl := NewShardedRateLimiter(4, 100, time.Second)
	defer sl.Close()
	shard := sl.GetShard(0)
	if shard == nil {
		t.Fatal("GetShard(0) should not be nil")
	}
	if sl.GetShard(-1) != nil {
		t.Fatal("GetShard(-1) should be nil")
	}
	if sl.GetShard(4) != nil {
		t.Fatal("GetShard(4) should be nil (out of bounds)")
	}
}

func TestShardedRateLimiter_Close(t *testing.T) {
	sl := NewShardedRateLimiter(4, 100, time.Second)
	sl.Close()
	ctx := context.Background()
	if err := sl.Acquire(ctx); err == nil {
		t.Fatal("expected error after Close")
	}
}

// ============================================================
// 八、ShardedTokenBucket 完整测试
// ============================================================

func TestShardedTokenBucket_Basic(t *testing.T) {
	st := NewShardedTokenBucket(4, 100, 20)
	if st.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", st.ShardCount())
	}
}

func TestShardedTokenBucket_ZeroShards(t *testing.T) {
	st := NewShardedTokenBucket(0, 100, 20)
	if st.ShardCount() < 2 {
		t.Fatalf("expected at least 2 shards, got %d", st.ShardCount())
	}
}

func TestShardedTokenBucket_Allow(t *testing.T) {
	st := NewShardedTokenBucket(4, 100, 50)
	allowed := 0
	for i := 0; i < 200; i++ {
		if st.Allow() {
			allowed++
		}
	}
	if allowed < 50 {
		t.Fatalf("expected at least 50 tokens, got %d", allowed)
	}
	t.Logf("allowed=%d", allowed)
}

func TestShardedTokenBucket_AllowN(t *testing.T) {
	st := NewShardedTokenBucket(4, 100, 50)
	if !st.AllowN(5) {
		t.Fatal("AllowN(5) should succeed")
	}
	if !st.AllowN(0) {
		t.Fatal("AllowN(0) should return true")
	}
}

func TestShardedTokenBucket_GetShard(t *testing.T) {
	st := NewShardedTokenBucket(4, 100, 20)
	shard := st.GetShard(0)
	if shard == nil {
		t.Fatal("GetShard(0) should not be nil")
	}
	if st.GetShard(-1) != nil {
		t.Fatal("GetShard(-1) should be nil")
	}
	if st.GetShard(4) != nil {
		t.Fatal("GetShard(4) should be nil")
	}
}

// ============================================================
// 九、ShardedSlidingWindowRateLimiter 完整测试
// ============================================================

func TestShardedSlidingWindowRateLimiter_Basic(t *testing.T) {
	sw := NewShardedSlidingWindowRateLimiter(4, 100, time.Second)
	if sw.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sw.ShardCount())
	}
	if sw.TotalLimit() < 1 {
		t.Fatalf("total limit should be > 0, got %d", sw.TotalLimit())
	}
}

func TestShardedSlidingWindowRateLimiter_ZeroShards(t *testing.T) {
	sw := NewShardedSlidingWindowRateLimiter(0, 100, time.Second)
	if sw.ShardCount() < 2 {
		t.Fatalf("expected at least 2 shards, got %d", sw.ShardCount())
	}
}

func TestShardedSlidingWindowRateLimiter_Allow(t *testing.T) {
	sw := NewShardedSlidingWindowRateLimiter(4, 200, time.Second)
	allowed := 0
	for i := 0; i < 200; i++ {
		if sw.Allow() {
			allowed++
		}
	}
	if allowed < 4 {
		t.Fatalf("expected at least 4 allowed, got %d", allowed)
	}
	t.Logf("allowed=%d", allowed)
}

func TestShardedSlidingWindowRateLimiter_AllowN(t *testing.T) {
	sw := NewShardedSlidingWindowRateLimiter(4, 100, time.Second)
	if !sw.AllowN(1) {
		t.Fatal("AllowN(1) should succeed")
	}
	if !sw.AllowN(0) {
		t.Fatal("AllowN(0) should succeed")
	}
}

func TestShardedSlidingWindowRateLimiter_GetShard(t *testing.T) {
	sw := NewShardedSlidingWindowRateLimiter(4, 100, time.Second)
	shard := sw.GetShard(0)
	if shard == nil {
		t.Fatal("GetShard(0) should not be nil")
	}
	if sw.GetShard(-1) != nil {
		t.Fatal("GetShard(-1) should be nil")
	}
	if sw.GetShard(4) != nil {
		t.Fatal("GetShard(4) should be nil")
	}
}

// ============================================================
// 十、ShardedAdaptiveRateLimiter 完整测试
// ============================================================

func TestShardedAdaptiveRateLimiter_Basic(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(4, 20, 200)
	defer sa.Close()
	if sa.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sa.ShardCount())
	}
	if sa.TotalMin() < 1 {
		t.Fatalf("total min should be > 0, got %d", sa.TotalMin())
	}
	if sa.TotalMax() < 1 {
		t.Fatalf("total max should be > 0, got %d", sa.TotalMax())
	}
}

func TestShardedAdaptiveRateLimiter_ZeroShards(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(0, 20, 200)
	defer sa.Close()
	if sa.ShardCount() < 2 {
		t.Fatalf("expected at least 2 shards, got %d", sa.ShardCount())
	}
}

func TestShardedAdaptiveRateLimiter_AcquireRelease(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(4, 20, 200)
	defer sa.Close()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := sa.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
	}
	for i := 0; i < 10; i++ {
		sa.Release()
	}
}

func TestShardedAdaptiveRateLimiter_RecordOk(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(4, 20, 200)
	defer sa.Close()
	ctx := context.Background()
	if err := sa.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	sa.RecordOk()
	sa.Release()
}

func TestShardedAdaptiveRateLimiter_RecordFailure(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(4, 20, 200)
	defer sa.Close()
	ctx := context.Background()
	if err := sa.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	sa.RecordFailure()
	sa.Release()
}

func TestShardedAdaptiveRateLimiter_RecordMixed(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(4, 20, 200)
	defer sa.Close()
	ctx := context.Background()
	for i := 0; i < 30; i++ {
		if err := sa.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		if i%2 == 0 {
			sa.RecordOk()
		} else {
			sa.RecordFailure()
		}
		sa.Release()
	}
}

func TestShardedAdaptiveRateLimiter_Close(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(4, 20, 200)
	sa.Close()
	ctx := context.Background()
	if err := sa.Acquire(ctx); err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestShardedAdaptiveRateLimiter_GetShard(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(4, 20, 200)
	defer sa.Close()
	shard := sa.GetShard(0)
	if shard == nil {
		t.Fatal("GetShard(0) should not be nil")
	}
	if sa.GetShard(-1) != nil {
		t.Fatal("GetShard(-1) should be nil")
	}
	if sa.GetShard(4) != nil {
		t.Fatal("GetShard(4) should be nil")
	}
}

// ============================================================
// 十一、Helper 函数测试
// ============================================================

func TestDefaultShardCount(t *testing.T) {
	n := DefaultShardCount()
	if n < 2 {
		t.Fatalf("expected at least 2, got %d", n)
	}
}

func TestWithRateLimiter(t *testing.T) {
	var called bool
	err := WithRateLimiter(100, time.Second, func(rl *RateLimiter) error {
		called = true
		if rl == nil {
			t.Fatal("rate limiter should not be nil")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("fn was not called")
	}
}

func TestWithRateLimiter_Error(t *testing.T) {
	expectedErr := errors.New("test error")
	err := WithRateLimiter(100, time.Second, func(rl *RateLimiter) error {
		return expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

func TestWithRateLimiterWithBurst(t *testing.T) {
	var called bool
	err := WithRateLimiterWithBurst(50, time.Second, 200, func(rl *RateLimiter) error {
		called = true
		if rl == nil {
			t.Fatal("rate limiter should not be nil")
		}
		rl.Acquire(context.Background())
		rl.Release()
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("fn was not called")
	}
}

func TestWithShardedRateLimiter(t *testing.T) {
	var called bool
	err := WithShardedRateLimiter(4, 100, time.Second, func(sl *ShardedRateLimiter) error {
		called = true
		if sl == nil {
			t.Fatal("sharded rate limiter should not be nil")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("fn was not called")
	}
}

func TestWithShardedRateLimiterWithBurst(t *testing.T) {
	var called bool
	err := WithShardedRateLimiterWithBurst(4, 100, time.Second, 500, func(sl *ShardedRateLimiter) error {
		called = true
		if sl == nil {
			t.Fatal("rate limiter should not be nil")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("fn was not called")
	}
}

func TestWithShardedAdaptiveRateLimiter(t *testing.T) {
	var called bool
	err := WithShardedAdaptiveRateLimiter(4, 20, 200, func(sa *ShardedAdaptiveRateLimiter) error {
		called = true
		if sa == nil {
			t.Fatal("sharded adaptive rate limiter should not be nil")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("fn was not called")
	}
}

// ============================================================
// 十二、Builder 完整测试
// ============================================================

func TestRatelimitBuilder_RateLimiter(t *testing.T) {
	ctx := context.Background()
	rl := NewRatelimitBuilder(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
	defer rl.Close()
	if rl.Size() != 100 {
		t.Fatalf("expected size 100, got %d", rl.Size())
	}
}

func TestRatelimitBuilder_RateLimiterWithBurst(t *testing.T) {
	ctx := context.Background()
	rl := NewRatelimitBuilder(ctx).RateLimiter().Rate(50).Per(time.Second).Burst(200).Build()
	defer rl.Close()
	if rl.Size() != 50 {
		t.Fatalf("expected size 50, got %d", rl.Size())
	}
}

func TestRatelimitBuilder_RateLimiterDefaults(t *testing.T) {
	ctx := context.Background()
	rl := NewRatelimitBuilder(ctx).RateLimiter().DefaultRate().DefaultPer().Build()
	defer rl.Close()
	if rl.Size() != core.IO() {
		t.Fatalf("expected size core.IO()=%d, got %d", core.IO(), rl.Size())
	}
}

func TestRatelimitBuilder_RateLimiterDefaultBurst(t *testing.T) {
	ctx := context.Background()
	rl := NewRatelimitBuilder(ctx).RateLimiter().Rate(10).Per(time.Second).DefaultBurst().Build()
	defer rl.Close()
	if rl.Size() != 10 {
		t.Fatalf("expected size 10, got %d", rl.Size())
	}
}

func TestRatelimitBuilder_TokenBucket(t *testing.T) {
	ctx := context.Background()
	tb := NewRatelimitBuilder(ctx).TokenBucket().Rate(10).Capacity(20).Build()
	if !tb.Allow() {
		t.Fatal("expected Allow to succeed")
	}
}

func TestRatelimitBuilder_TokenBucketDefaults(t *testing.T) {
	ctx := context.Background()
	tb := NewRatelimitBuilder(ctx).TokenBucket().DefaultRate().DefaultCapacity().Build()
	if !tb.Allow() {
		t.Fatal("expected Allow to succeed")
	}
}

func TestRatelimitBuilder_SlidingWindow(t *testing.T) {
	ctx := context.Background()
	sw := NewRatelimitBuilder(ctx).SlidingWindow().Limit(100).Window(time.Second).Build()
	if !sw.Allow() {
		t.Fatal("expected Allow to succeed")
	}
}

func TestRatelimitBuilder_SlidingWindowDefaults(t *testing.T) {
	ctx := context.Background()
	sw := NewRatelimitBuilder(ctx).SlidingWindow().DefaultLimit().DefaultWindow().Build()
	if !sw.Allow() {
		t.Fatal("expected Allow to succeed")
	}
}

func TestRatelimitBuilder_Adaptive(t *testing.T) {
	ctx := context.Background()
	al := NewRatelimitBuilder(ctx).Adaptive().MinWorker(5).MaxWorker(100).Build()
	ctx2 := context.Background()
	if err := al.Acquire(ctx2); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordOk()
	al.Release()
}

func TestRatelimitBuilder_AdaptiveDefaults(t *testing.T) {
	ctx := context.Background()
	al := NewRatelimitBuilder(ctx).Adaptive().DefaultMinWorker().DefaultMaxWorker().Build()
	ctx2 := context.Background()
	if err := al.Acquire(ctx2); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordOk()
	al.Release()
}

func TestRatelimitBuilder_ShardedRateLimiter(t *testing.T) {
	ctx := context.Background()
	sl := NewRatelimitBuilder(ctx).Sharded().RateLimiter().Shards(4).Rate(100).Per(time.Second).Build()
	defer sl.Close()
	if sl.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sl.ShardCount())
	}
}

func TestRatelimitBuilder_ShardedRateLimiterWithBurst(t *testing.T) {
	ctx := context.Background()
	sl := NewRatelimitBuilder(ctx).Sharded().RateLimiter().Shards(4).Rate(50).Per(time.Second).Burst(200).Build()
	defer sl.Close()
	if sl.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sl.ShardCount())
	}
}

func TestRatelimitBuilder_ShardedRateLimiterDefaults(t *testing.T) {
	ctx := context.Background()
	sl := NewRatelimitBuilder(ctx).Sharded().RateLimiter().DefaultShards().DefaultRate().DefaultPer().Build()
	defer sl.Close()
	if sl.ShardCount() < 2 {
		t.Fatalf("expected at least 2 shards, got %d", sl.ShardCount())
	}
}

func TestRatelimitBuilder_ShardedTokenBucket(t *testing.T) {
	ctx := context.Background()
	st := NewRatelimitBuilder(ctx).Sharded().TokenBucket().Shards(4).Rate(10).Capacity(20).Build()
	if st.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", st.ShardCount())
	}
	if !st.Allow() {
		t.Fatal("expected Allow to succeed")
	}
}

func TestRatelimitBuilder_ShardedTokenBucketDefaults(t *testing.T) {
	ctx := context.Background()
	st := NewRatelimitBuilder(ctx).Sharded().TokenBucket().DefaultShards().DefaultRate().DefaultCapacity().Build()
	if st.ShardCount() < 2 {
		t.Fatalf("expected at least 2 shards, got %d", st.ShardCount())
	}
	if !st.Allow() {
		t.Fatal("expected Allow to succeed")
	}
}

func TestRatelimitBuilder_ShardedSlidingWindow(t *testing.T) {
	ctx := context.Background()
	sw := NewRatelimitBuilder(ctx).Sharded().SlidingWindow().Shards(4).Limit(100).Window(time.Second).Build()
	if sw.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sw.ShardCount())
	}
	if !sw.Allow() {
		t.Fatal("expected Allow to succeed")
	}
}

func TestRatelimitBuilder_ShardedSlidingWindowDefaults(t *testing.T) {
	ctx := context.Background()
	sw := NewRatelimitBuilder(ctx).Sharded().SlidingWindow().DefaultShards().DefaultLimit().DefaultWindow().Build()
	if sw.ShardCount() < 2 {
		t.Fatalf("expected at least 2 shards, got %d", sw.ShardCount())
	}
	if !sw.Allow() {
		t.Fatal("expected Allow to succeed")
	}
}

func TestRatelimitBuilder_ShardedAdaptive(t *testing.T) {
	ctx := context.Background()
	sa := NewRatelimitBuilder(ctx).Sharded().Adaptive().Shards(4).MinWorker(20).MaxWorker(200).Build()
	defer sa.Close()
	if sa.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sa.ShardCount())
	}
}

func TestRatelimitBuilder_ShardedAdaptiveDefaults(t *testing.T) {
	ctx := context.Background()
	sa := NewRatelimitBuilder(ctx).Sharded().Adaptive().DefaultShards().DefaultMinWorker().DefaultMaxWorker().Build()
	defer sa.Close()
	if sa.ShardCount() < 2 {
		t.Fatalf("expected at least 2 shards, got %d", sa.ShardCount())
	}
}

func TestRatelimitBuilder_Context(t *testing.T) {
	ctx := context.Background()
	b := NewRatelimitBuilder(ctx).Context(ctx)
	if b == nil {
		t.Fatal("Context should return the builder")
	}
	rl := b.RateLimiter().Rate(10).Per(time.Second).Build()
	defer rl.Close()
	if rl.Size() != 10 {
		t.Fatalf("expected size 10, got %d", rl.Size())
	}
}

func TestRatelimitBuilder_Logger(t *testing.T) {
	ctx := context.Background()
	b := NewRatelimitBuilder(ctx).Logger(nil)
	if b == nil {
		t.Fatal("Logger should return the builder")
	}
	b.DefaultLogger()
}

func TestSubBuilder_Context(t *testing.T) {
	ctx := context.Background()
	rl := NewRatelimitBuilder(ctx).RateLimiter().Context(ctx).Rate(10).Per(time.Second).Build()
	defer rl.Close()
	if rl.Size() != 10 {
		t.Fatalf("expected size 10, got %d", rl.Size())
	}
}

func TestSubBuilder_Logger(t *testing.T) {
	ctx := context.Background()
	rl := NewRatelimitBuilder(ctx).RateLimiter().Logger(nil).DefaultLogger().Rate(10).Per(time.Second).Build()
	defer rl.Close()
	if rl.Size() != 10 {
		t.Fatalf("expected size 10, got %d", rl.Size())
	}
}

func TestTokenBucketSubBuilder_Context(t *testing.T) {
	ctx := context.Background()
	tb := NewRatelimitBuilder(ctx).TokenBucket().Context(ctx).Rate(10).Capacity(20).Build()
	if !tb.Allow() {
		t.Fatal("Allow should succeed")
	}
}

func TestSlidingWindowSubBuilder_Context(t *testing.T) {
	ctx := context.Background()
	sw := NewRatelimitBuilder(ctx).SlidingWindow().Context(ctx).Limit(100).Window(time.Second).Build()
	if !sw.Allow() {
		t.Fatal("Allow should succeed")
	}
}

func TestAdaptiveSubBuilder_Context(t *testing.T) {
	ctx := context.Background()
	al := NewRatelimitBuilder(ctx).Adaptive().Context(ctx).MinWorker(5).MaxWorker(100).Build()
	ctx2 := context.Background()
	if err := al.Acquire(ctx2); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordOk()
	al.Release()
}

func TestShardedRatelimitBuilder_Context(t *testing.T) {
	ctx := context.Background()
	sl := NewRatelimitBuilder(ctx).Sharded().Context(ctx).RateLimiter().Shards(4).Rate(100).Per(time.Second).Build()
	defer sl.Close()
	if sl.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sl.ShardCount())
	}
}

func TestShardedSubBuilder_Context(t *testing.T) {
	ctx := context.Background()
	sl := NewRatelimitBuilder(ctx).Sharded().RateLimiter().Context(ctx).Shards(4).Rate(100).Per(time.Second).Build()
	defer sl.Close()
	if sl.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sl.ShardCount())
	}
}

func TestShardedSubBuilder_Logger(t *testing.T) {
	ctx := context.Background()
	sl := NewRatelimitBuilder(ctx).Sharded().RateLimiter().Logger(nil).DefaultLogger().Shards(4).Rate(100).Per(time.Second).Build()
	defer sl.Close()
	if sl.ShardCount() != 4 {
		t.Fatalf("expected 4 shards, got %d", sl.ShardCount())
	}
}

// ============================================================
// 十三、并发压力测试（万/十万/百万/千万）
// ============================================================

func TestRateLimiter_AcquireRelease_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			rl := NewRateLimiter(tier.Size/10, time.Second)
			defer rl.Close()
			ctx := context.Background()
			var wg sync.WaitGroup
			var success, fail atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					if err := rl.Acquire(ctx); err == nil {
						success.Add(1)
						time.Sleep(time.Microsecond)
						rl.Release()
					} else {
						fail.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("success=%d fail=%d", success.Load(), fail.Load())
		})
	}
}

func TestTokenBucket_Allow_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			bucketSize := tier.Size * 2
			if bucketSize < 2000 {
				bucketSize = 2000
			}
			tb := NewTokenBucket(float64(bucketSize), float64(bucketSize))
			var wg sync.WaitGroup
			var allowed atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					if tb.Allow() {
						allowed.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("allowed=%d", allowed.Load())
		})
	}
}

func TestSlidingWindow_Allow_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			sw := NewSlidingWindowRateLimiter(tier.Size, time.Second)
			var wg sync.WaitGroup
			var allowed atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					if sw.Allow() {
						allowed.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("allowed=%d (limit=%d)", allowed.Load(), tier.Size)
		})
	}
}

func TestAdaptiveRateLimiter_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			maxConc := tier.Size / 100
			if maxConc < 200 {
				maxConc = 200
			}
			al := NewAdaptiveRateLimiter(maxConc, maxConc)
			var wg sync.WaitGroup
			var success, fail atomic.Int64
			ctx := context.Background()
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					if err := al.Acquire(ctx); err == nil {
						success.Add(1)
						time.Sleep(time.Microsecond)
						al.RecordOk()
						al.Release()
					} else {
						fail.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("success=%d fail=%d", success.Load(), fail.Load())
		})
	}
}

func TestRateLimiter_Token_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			rl := NewRateLimiter(tier.Size, time.Second)
			defer rl.Close()
			ctx := context.Background()
			var wg sync.WaitGroup
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					tok, err := rl.Token(ctx)
					if err == nil {
						tok.Release()
					}
				}()
			}
			wg.Wait()
		})
	}
}

func TestRateLimiter_BlockForce_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			rl := NewRateLimiter(tier.Size, time.Second)
			rl.WithStrategy(BlockForce)
			defer rl.Close()
			ctx := context.Background()
			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					if err := rl.Acquire(ctx); err == nil {
						success.Add(1)
						rl.Release()
					}
				}()
			}
			wg.Wait()
			t.Logf("success=%d", success.Load())
		})
	}
}

func TestShardedRateLimiter_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			sl := NewShardedRateLimiter(8, tier.Size/10, time.Second)
			defer sl.Close()
			ctx := context.Background()
			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					if err := sl.Acquire(ctx); err == nil {
						success.Add(1)
						time.Sleep(time.Microsecond)
						sl.Release()
					}
				}()
			}
			wg.Wait()
			t.Logf("success=%d", success.Load())
		})
	}
}

func TestShardedTokenBucket_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			st := NewShardedTokenBucket(8, float64(tier.Size), float64(tier.Size))
			var wg sync.WaitGroup
			var allowed atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					if st.Allow() {
						allowed.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("allowed=%d", allowed.Load())
		})
	}
}

func TestShardedSlidingWindow_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			sw := NewShardedSlidingWindowRateLimiter(8, tier.Size, time.Second)
			var wg sync.WaitGroup
			var allowed atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					if sw.Allow() {
						allowed.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("allowed=%d", allowed.Load())
		})
	}
}

func TestShardedAdaptive_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			sa := NewShardedAdaptiveRateLimiter(8, tier.Size/100, tier.Size/10)
			defer sa.Close()
			ctx := context.Background()
			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					if err := sa.Acquire(ctx); err == nil {
						success.Add(1)
						time.Sleep(time.Microsecond)
						sa.RecordOk()
						sa.Release()
					}
				}()
			}
			wg.Wait()
			t.Logf("success=%d", success.Load())
		})
	}
}

// ============================================================
// 十四、Race 竞态测试
// ============================================================

func TestRace_RateLimiter_BlockAcquireRelease(t *testing.T) {
	for round := 0; round < 3; round++ {
		rl := NewRateLimiter(100, time.Second)
		rl.WithStrategy(Block)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 2000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				rl.Acquire(ctx)
				time.Sleep(10 * time.Microsecond)
				rl.Release()
			}()
		}
		wg.Wait()
		rl.Close()
	}
}

func TestRace_TokenBucket_ConcurrentAllow(t *testing.T) {
	for round := 0; round < 3; round++ {
		tb := NewTokenBucket(500, 500)
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				tb.Allow()
			}()
		}
		wg.Wait()
	}
}

func TestRace_SlidingWindow_ConcurrentAllow(t *testing.T) {
	for round := 0; round < 3; round++ {
		sw := NewSlidingWindowRateLimiter(1000, time.Second)
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				sw.Allow()
			}()
		}
		wg.Wait()
	}
}

func TestRace_RateLimiter_ResizeConcurrent(t *testing.T) {
	for round := 0; round < 3; round++ {
		rl := NewRateLimiter(100, time.Second)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				rl.Acquire(ctx)
				rl.Release()
			}()
		}
		go func() {
			for i := 0; i < 5; i++ {
				rl.Resize(50 + i*10)
				time.Sleep(time.Millisecond)
			}
		}()
		wg.Wait()
		rl.Close()
	}
}

func TestRace_RateLimiter_StrategySwitch(t *testing.T) {
	for round := 0; round < 3; round++ {
		rl := NewRateLimiter(100, time.Second)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				rl.Acquire(ctx)
				rl.Release()
			}()
		}
		go func() {
			strategies := []Strategy{Block, Reject, BlockForce}
			for i := 0; i < 10; i++ {
				rl.WithStrategy(strategies[i%3])
				time.Sleep(time.Microsecond)
			}
		}()
		wg.Wait()
		rl.Close()
	}
}

func TestRace_AdaptiveRateLimiter_ConcurrentRecord(t *testing.T) {
	for round := 0; round < 3; round++ {
		al := NewAdaptiveRateLimiter(50, 500)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(idx int) {
				defer wg.Done()
				if err := al.Acquire(ctx); err == nil {
					if idx%2 == 0 {
						al.RecordOk()
					} else {
						al.RecordFailure()
					}
					al.Release()
				}
			}(i)
		}
		wg.Wait()
		al.base.Close()
	}
}

func TestRace_ShardedRateLimiter_Concurrent(t *testing.T) {
	for round := 0; round < 3; round++ {
		sl := NewShardedRateLimiter(4, 200, time.Second)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				sl.Acquire(ctx)
				sl.Release()
			}()
		}
		wg.Wait()
		sl.Close()
	}
}

func TestRace_ShardedTokenBucket_Concurrent(t *testing.T) {
	for round := 0; round < 3; round++ {
		st := NewShardedTokenBucket(4, 500, 500)
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				st.Allow()
			}()
		}
		wg.Wait()
	}
}

func TestRace_ShardedSlidingWindow_Concurrent(t *testing.T) {
	for round := 0; round < 3; round++ {
		sw := NewShardedSlidingWindowRateLimiter(4, 1000, time.Second)
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				sw.Allow()
			}()
		}
		wg.Wait()
	}
}

func TestRace_ShardedAdaptive_Concurrent(t *testing.T) {
	for round := 0; round < 3; round++ {
		sa := NewShardedAdaptiveRateLimiter(4, 100, 500)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				if err := sa.Acquire(ctx); err == nil {
					sa.RecordOk()
					sa.Release()
				}
			}()
		}
		wg.Wait()
		sa.Close()
	}
}

func TestRace_RateLimiter_TokenConcurrent(t *testing.T) {
	for round := 0; round < 3; round++ {
		rl := NewRateLimiter(200, time.Second)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				tok, err := rl.Token(ctx)
				if err == nil {
					tok.Release()
				}
			}()
		}
		wg.Wait()
		rl.Close()
	}
}

func TestRace_TokenBucket_AllowN_Concurrent(t *testing.T) {
	for round := 0; round < 3; round++ {
		tb := NewTokenBucket(500, 500)
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				tb.AllowN(1)
			}()
		}
		wg.Wait()
	}
}

func TestRace_SlidingWindow_AllowN_Concurrent(t *testing.T) {
	for round := 0; round < 3; round++ {
		sw := NewSlidingWindowRateLimiter(1000, time.Second)
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				sw.AllowN(1)
			}()
		}
		wg.Wait()
	}
}
