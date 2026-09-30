package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

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
// 一、RateLimiter 基础测试
// ============================================================

func TestRateLimiter_NewAndSize(t *testing.T) {
	rl := NewRateLimiter(100, time.Second)
	defer rl.Close()
	if rl.Size() != 100 {
		t.Fatalf("expected size 100, got %d", rl.Size())
	}
}

func TestRateLimiter_AcquireRelease(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
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
	if rl.Available() != 10 {
		t.Logf("available tokens: %d (may not be exact 10 due to refill)", rl.Available())
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
}

func TestRateLimiter_Resize(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	defer rl.Close()
	rl.Resize(20)
	if rl.Size() != 20 {
		t.Fatalf("expected size 20 after resize, got %d", rl.Size())
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

// ============================================================
// 二、RateLimiter 策略 & 边界测试
// ============================================================

func TestRateLimiter_BlockStrategy(t *testing.T) {
	rl := NewRateLimiter(1, time.Second)
	rl.WithStrategy(Block)
	defer rl.Close()
	ctx := context.Background()
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	done := make(chan bool)
	go func() {
		if err := rl.Acquire(ctx); err != nil {
			t.Logf("second acquire: %v", err)
		}
		done <- true
	}()
	time.Sleep(50 * time.Millisecond)
	rl.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second acquire didn't complete in time")
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

func TestRateLimiter_Close(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	rl.Close()
	ctx := context.Background()
	if err := rl.Acquire(ctx); err == nil {
		t.Fatal("expected error after close")
	}
}

// ============================================================
// 三、TokenBucket 测试
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

// ============================================================
// 四、SlidingWindowRateLimiter 测试
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

// ============================================================
// 五、AdaptiveRateLimiter 测试
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

func TestAdaptiveRateLimiter_RecordSuccess(t *testing.T) {
	al := NewAdaptiveRateLimiter(10, 100)
	ctx := context.Background()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordSuccess()
	al.Release()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire after success failed: %v", err)
	}
	al.RecordSuccess()
	al.Release()
}

// ============================================================
// 六、四档并发压力测试（万/十万/百万/千万）
// ============================================================

func TestRateLimiter_AcquireRelease_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			rl := NewRateLimiter(tier.size/10, time.Second)
			defer rl.Close()
			ctx := context.Background()
			var wg sync.WaitGroup
			var success, fail atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
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
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			bucketSize := tier.size * 2
			if bucketSize < 2000 {
				bucketSize = 2000
			}
			tb := NewTokenBucket(float64(bucketSize), float64(bucketSize))
			var wg sync.WaitGroup
			var allowed atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
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
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			sw := NewSlidingWindowRateLimiter(tier.size, time.Second)
			var wg sync.WaitGroup
			var allowed atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func() {
					defer wg.Done()
					if sw.Allow() {
						allowed.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("allowed=%d (limit=%d)", allowed.Load(), tier.size)
		})
	}
}

func TestAdaptiveRateLimiter_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			maxConc := tier.size / 100
			if maxConc < 200 {
				maxConc = 200
			}
			al := NewAdaptiveRateLimiter(maxConc, maxConc)
			var wg sync.WaitGroup
			var success, fail atomic.Int64
			ctx := context.Background()
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func() {
					defer wg.Done()
					if err := al.Acquire(ctx); err == nil {
						success.Add(1)
						time.Sleep(time.Microsecond)
						al.RecordSuccess()
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
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			rl := NewRateLimiter(tier.size, time.Second)
			defer rl.Close()
			ctx := context.Background()
			var wg sync.WaitGroup
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
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
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			rl := NewRateLimiter(tier.size, time.Second)
			rl.WithStrategy(BlockForce)
			defer rl.Close()
			ctx := context.Background()
			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
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

// ============================================================
// 七、Race 竞态测试
// ============================================================

func TestRace_RateLimiter_BlockAcquireRelease(t *testing.T) {
	for round := 0; round < 10; round++ {
		rl := NewRateLimiter(100, time.Second)
		rl.WithStrategy(Block)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 5000
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
	for round := 0; round < 10; round++ {
		tb := NewTokenBucket(2000, 2000)
		var wg sync.WaitGroup
		n := 5000
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
	for round := 0; round < 10; round++ {
		sw := NewSlidingWindowRateLimiter(5000, time.Second)
		var wg sync.WaitGroup
		n := 5000
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
	for round := 0; round < 10; round++ {
		rl := NewRateLimiter(100, time.Second)
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 5000
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
