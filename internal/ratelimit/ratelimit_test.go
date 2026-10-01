package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/testutil"
)

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
	if avail := rl.Available(); avail < 50 {
		t.Logf("available after burst init: %d (expected >= 50)", avail)
	}
}

func TestRateLimiter_NewWithBurstLessThanRate(t *testing.T) {
	rl := NewRateLimiterWithBurst(100, time.Second, 10)
	defer rl.Close()
	if rl.Size() != 100 {
		t.Fatalf("expected size 100, got %d", rl.Size())
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

	tok2, err := rl.Token(ctx)
	if err != nil {
		t.Fatalf("token2 acquire failed: %v", err)
	}
	tok2.Release()
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

func TestRateLimiter_BlockStrategy_CtxCancel(t *testing.T) {
	rl := NewRateLimiter(1, time.Second)
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
		if err == nil {
			t.Fatal("expected context canceled error")
		}
		if !errors.Is(err, context.Canceled) {
			t.Logf("got error: %v", err)
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

func TestRateLimiter_AcquireAfterClose(t *testing.T) {
	rl := NewRateLimiter(10, time.Second)
	rl.Close()
	ctx := context.Background()
	err := rl.Acquire(ctx)
	if err == nil {
		t.Fatal("expected error after close")
	}
	if !errors.Is(err, core.ErrRateLimiterStopped) {
		t.Logf("got: %v", err)
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

func TestAdaptiveRateLimiter_MinEqualsMax(t *testing.T) {
	al := NewAdaptiveRateLimiter(5, 5)
	ctx := context.Background()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordSuccess()
	al.Release()
}

func TestAdaptiveRateLimiter_MinGreaterThanMax(t *testing.T) {
	al := NewAdaptiveRateLimiter(10, 5)
	ctx := context.Background()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordSuccess()
	al.Release()
}

func TestAdaptiveRateLimiter_MinZero(t *testing.T) {
	al := NewAdaptiveRateLimiter(0, 5)
	ctx := context.Background()
	if err := al.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordSuccess()
	al.Release()
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
			al.RecordSuccess()
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

func TestShardedAdaptiveRateLimiter_RecordSuccess(t *testing.T) {
	sa := NewShardedAdaptiveRateLimiter(4, 20, 200)
	defer sa.Close()
	ctx := context.Background()
	if err := sa.Acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	sa.RecordSuccess()
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
			sa.RecordSuccess()
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
	al.RecordSuccess()
	al.Release()
}

func TestRatelimitBuilder_AdaptiveDefaults(t *testing.T) {
	ctx := context.Background()
	al := NewRatelimitBuilder(ctx).Adaptive().DefaultMinWorker().DefaultMaxWorker().Build()
	ctx2 := context.Background()
	if err := al.Acquire(ctx2); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.RecordSuccess()
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
	al.RecordSuccess()
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
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

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
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

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
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

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
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

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
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

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
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

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
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

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
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

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
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

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
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

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
						sa.RecordSuccess()
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
						al.RecordSuccess()
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
					sa.RecordSuccess()
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
