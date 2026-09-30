package async

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

var errRatelimitSentinel = errors.New("ratelimit sentinel error")

// ============================================================
// 共享工具：测试档位 & 辅助函数
// ============================================================

type rlDataTier struct {
	name  string
	load  int // 并发 goroutine 数
	burst int // RateLimiter 突发容量
}

var rlAllTiers = []rlDataTier{
	{"万级_10K", 10_000, 10_000},
	{"十万级_100K", 100_000, 100_000},
	{"百万级_1M", 1_000_000, 1_000_000},
	{"千万级_10M", 10_000_000, 5_000_000},
}

func rlSkipIfTooLarge(t *testing.T, load int) {
	if testing.Short() && load >= 1_000_000 {
		t.Skip("short mode: skip large scale test")
	}
}

func rlFreshCtx() context.Context {
	return core.EnsureTraceID(context.Background())
}

// ============================================================
// Section 1: RatelimiterSubBuilder 链式方法全覆盖测试
// Rate / Per / Burst / Build  + 返回实例的基础方法验证
// ============================================================

func TestRatelimit_Chain_RateLimiter_AllMethods(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("Rate+Per", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Millisecond).Build()
		defer rl.Close()
		if rl.Size() != 100 {
			t.Fatalf("expected size 100, got %d", rl.Size())
		}
		if err := rl.Acquire(ctx); err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		rl.Release()
	})

	t.Run("Rate+Per+Burst", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(50).Per(time.Second).Burst(200).Build()
		defer rl.Close()
		if rl.Size() != 50 {
			t.Fatalf("expected size 50, got %d", rl.Size())
		}
		avail := rl.Available()
		if avail < 50 {
			t.Fatalf("expected available >= 50 (burst=200), got %d", avail)
		}
	})

	t.Run("Burst+Rate+Per_order_independent", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Burst(300).Rate(30).Per(time.Second).Build()
		defer rl.Close()
		if rl.Size() != 30 {
			t.Fatalf("expected size 30, got %d", rl.Size())
		}
	})

	t.Run("Per+Rate", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Per(time.Minute).Rate(60).Build()
		defer rl.Close()
		if rl.Size() != 60 {
			t.Fatalf("expected size 60, got %d", rl.Size())
		}
	})

	t.Run("default_zero_params", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Build()
		defer rl.Close()
		if rl.Size() <= 0 {
			t.Fatalf("expected non-zero default size")
		}
	})

	t.Run("Token_and_defer_Release", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Build()
		defer rl.Close()
		tok, err := rl.Token(ctx)
		if err != nil {
			t.Fatalf("Token failed: %v", err)
		}
		tok.Release()
	})

	t.Run("Wait_basic", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(5).Per(time.Second).Build()
		defer rl.Close()
		for i := 0; i < 5; i++ {
			if err := rl.Wait(ctx); err != nil {
				t.Fatalf("Wait %d failed: %v", i, err)
			}
		}
		for i := 0; i < 5; i++ {
			rl.Release()
		}
	})

	t.Run("WithStrategy_Block", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(1).Per(time.Second).Build()
		defer rl.Close()
		rl.WithStrategy(Block)
		if err := rl.Acquire(ctx); err != nil {
			t.Fatalf("Block strategy Acquire failed: %v", err)
		}
		rl.Release()
	})

	t.Run("WithStrategy_Reject", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(1).Per(time.Second).Burst(1).Build()
		defer rl.Close()
		rl.Acquire(ctx) // 耗尽唯一令牌
		rl.WithStrategy(Reject)
		err := rl.Acquire(ctx)
		if err == nil {
			t.Fatal("expected Reject error, got nil")
		}
		rl.Release() // 清理
	})

	t.Run("WithStrategy_BlockForce", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Build()
		rl.WithStrategy(BlockForce)
		defer rl.Close()
		for i := 0; i < 10; i++ {
			if err := rl.Acquire(ctx); err != nil {
				t.Fatalf("BlockForce Acquire %d failed: %v", i, err)
			}
		}
		for i := 0; i < 10; i++ {
			rl.Release()
		}
	})

	t.Run("Resize_动态调速", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Build()
		defer rl.Close()
		rl.Resize(5)
		if rl.Size() != 5 {
			t.Fatalf("expected size 5 after Resize, got %d", rl.Size())
		}
	})

	t.Run("DefaultRate+DefaultPer+DefaultBurst", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().DefaultRate().DefaultPer().DefaultBurst().Build()
		defer rl.Close()
		if rl.Size() <= 0 {
			t.Fatal("DefaultRate should produce positive size")
		}
		if err := rl.Acquire(ctx); err != nil {
			t.Fatalf("Default-built Acquire failed: %v", err)
		}
		rl.Release()
	})

	t.Run("Stop_and_Close", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Build()
		rl.Close()
		rl.Close() // 二次关闭安全
		if err := rl.Acquire(ctx); err == nil {
			t.Fatal("expected error on closed limiter")
		}
	})
}

// ============================================================
// Section 2: TokenBucketSubBuilder 链式方法全覆盖测试
// Rate / Capacity / Build
// ============================================================

func TestRatelimit_Chain_TokenBucket_AllMethods(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("Rate+Capacity", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().Rate(100).Capacity(200).Build()
		for i := 0; i < 200; i++ {
			if !tb.Allow() {
				t.Fatalf("Allow %d failed, capacity=200", i)
			}
		}
		if tb.Allow() {
			t.Fatal("expected Allow to fail after exhausting capacity")
		}
	})

	t.Run("Capacity+Rate_order_independent", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().Capacity(50).Rate(10).Build()
		for i := 0; i < 50; i++ {
			if !tb.Allow() {
				t.Fatalf("Allow %d failed", i)
			}
		}
	})

	t.Run("AllowN_basic", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().Rate(10).Capacity(100).Build()
		if !tb.AllowN(50) {
			t.Fatal("AllowN(50) failed")
		}
		if !tb.AllowN(50) {
			t.Fatal("AllowN(50) failed second time")
		}
		if tb.AllowN(1) {
			t.Fatal("AllowN should fail after exhausting 100 tokens")
		}
	})

	t.Run("AllowN_zero_negative", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().Rate(10).Capacity(10).Build()
		if !tb.AllowN(0) {
			t.Fatal("AllowN(0) should always succeed")
		}
	})

	t.Run("default_params", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().Build()
		if tb.Allow() {
			t.Fatal("default (0 rate/0 capacity) TokenBucket should not allow")
		}
	})

	t.Run("DefaultRate+DefaultCapacity", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().DefaultRate().DefaultCapacity().Build()
		if !tb.Allow() {
			t.Fatal("DefaultRate+DefaultCapacity TokenBucket should allow")
		}
	})

	t.Run("refill_after_wait", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().Rate(100).Capacity(5).Build()
		for i := 0; i < 5; i++ {
			tb.Allow()
		}
		if tb.Allow() {
			t.Fatal("expected bucket exhausted immediately")
		}
		time.Sleep(60 * time.Millisecond)
		if !tb.Allow() {
			t.Fatal("expected refill after waiting")
		}
	})
}

// ============================================================
// Section 3: SlidingWindowSubBuilder 链式方法全覆盖测试
// Limit / Window / Build
// ============================================================

func TestRatelimit_Chain_SlidingWindow_AllMethods(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("Limit+Window", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Limit(50).Window(time.Second).Build()
		for i := 0; i < 50; i++ {
			if !sw.Allow() {
				t.Fatalf("Allow %d failed, limit=50", i)
			}
		}
		if sw.Allow() {
			t.Fatal("expected Allow to fail after exceeding limit")
		}
	})

	t.Run("Window+Limit_order_independent", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Window(time.Minute).Limit(100).Build()
		for i := 0; i < 100; i++ {
			if !sw.Allow() {
				t.Fatalf("Allow %d failed", i)
			}
		}
	})

	t.Run("AllowN_basic", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Limit(100).Window(time.Second).Build()
		if !sw.AllowN(50) {
			t.Fatal("AllowN(50) failed")
		}
		if !sw.AllowN(50) {
			t.Fatal("AllowN(50) second time failed")
		}
		if sw.AllowN(1) {
			t.Fatal("AllowN should fail after reaching limit")
		}
	})

	t.Run("AllowN_zero", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Limit(10).Window(time.Second).Build()
		if !sw.AllowN(0) {
			t.Fatal("AllowN(0) should always succeed")
		}
	})

	t.Run("default_params", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Build()
		if sw.Allow() {
			t.Fatal("default (0 limit) SlidingWindow should not allow")
		}
	})

	t.Run("DefaultLimit+DefaultWindow", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().DefaultLimit().DefaultWindow().Build()
		if !sw.Allow() {
			t.Fatal("DefaultLimit+DefaultWindow should allow")
		}
	})

	t.Run("window_expiry", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Limit(10).Window(50 * time.Millisecond).Build()
		for i := 0; i < 10; i++ {
			sw.Allow()
		}
		if sw.Allow() {
			t.Fatal("expected reject within window")
		}
		time.Sleep(60 * time.Millisecond)
		if !sw.Allow() {
			t.Fatal("expected allow after window expires")
		}
	})
}

// ============================================================
// Section 4: AdaptiveSubBuilder 链式方法全覆盖测试
// MinWorker / MaxWorker / Build
// ============================================================

func TestRatelimit_Chain_Adaptive_AllMethods(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("MinWorker+MaxWorker", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MinWorker(5).MaxWorker(100).Build()
		for i := 0; i < 20; i++ {
			if err := al.Acquire(ctx); err != nil {
				t.Fatalf("Acquire %d failed: %v", i, err)
			}
		}
		for i := 0; i < 20; i++ {
			al.Release()
		}
	})

	t.Run("Max+Min_order_independent", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MaxWorker(200).MinWorker(10).Build()
		for i := 0; i < 10; i++ {
			if err := al.Acquire(ctx); err != nil {
				t.Fatalf("Acquire %d failed: %v", i, err)
			}
		}
		for i := 0; i < 10; i++ {
			al.Release()
		}
	})

	t.Run("RecordSuccess", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MinWorker(1).MaxWorker(100).Build()
		al.Acquire(ctx)
		al.RecordSuccess()
		al.Release()
	})

	t.Run("RecordFailure", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MinWorker(1).MaxWorker(100).Build()
		al.Acquire(ctx)
		al.RecordFailure()
		al.Release()
	})

	t.Run("default_params", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().Build()
		if err := al.Acquire(ctx); err != nil {
			t.Fatalf("default Adaptive Acquire failed: %v", err)
		}
		al.Release()
	})

	t.Run("DefaultMin+DefaultMax", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().DefaultMinWorker().DefaultMaxWorker().Build()
		if err := al.Acquire(ctx); err != nil {
			t.Fatalf("Default-built Adaptive Acquire failed: %v", err)
		}
		al.Release()
	})

	t.Run("cancel_context_Acquire", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MinWorker(1).MaxWorker(1).Build()
		al.Acquire(ctx)
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		err := al.Acquire(cancelCtx)
		if err == nil {
			t.Fatal("expected error on cancelled context")
		}
		al.Release()
	})
}

// ============================================================
// Section 5: Sharded 构建器链式方法全覆盖测试
// Shards + 各模式参数 + Build
// ============================================================

func TestRatelimit_Chain_Sharded_AllMethods(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("Sharded_RateLimiter_Shards+Rate+Per", func(t *testing.T) {
		srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(4).Rate(100).Per(time.Second).Build()
		defer srl.Close()
		if srl.ShardCount() != 4 {
			t.Fatalf("expected 4 shards, got %d", srl.ShardCount())
		}
		if err := srl.Wait(ctx); err != nil {
			t.Fatalf("Wait failed: %v", err)
		}
		srl.Release()
	})

	t.Run("Sharded_RateLimiter_Burst", func(t *testing.T) {
		srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(2).Rate(50).Per(time.Second).Burst(200).Build()
		defer srl.Close()
		if srl.ShardCount() != 2 {
			t.Fatalf("expected 2 shards, got %d", srl.ShardCount())
		}
	})

	t.Run("Sharded_RateLimiter_AllOrders", func(t *testing.T) {
		srl := Ratelimit(ctx).Sharded().RateLimiter().Burst(300).Shards(2).Per(time.Second).Rate(50).Build()
		defer srl.Close()
		if srl.ShardCount() != 2 {
			t.Fatalf("expected 2 shards, got %d", srl.ShardCount())
		}
	})

	t.Run("Sharded_TokenBucket_Shards+Rate+Capacity", func(t *testing.T) {
		stb := Ratelimit(ctx).Sharded().TokenBucket().Shards(4).Rate(10).Capacity(20).Build()
		if stb.ShardCount() != 4 {
			t.Fatalf("expected 4 shards, got %d", stb.ShardCount())
		}
		for i := 0; i < 20; i++ {
			if !stb.Allow() {
				t.Fatalf("Allow %d failed", i)
			}
		}
	})

	t.Run("Sharded_SlidingWindow_Shards+Limit+Window", func(t *testing.T) {
		ssw := Ratelimit(ctx).Sharded().SlidingWindow().Shards(2).Limit(100).Window(time.Second).Build()
		if ssw.ShardCount() != 2 {
			t.Fatalf("expected 2 shards, got %d", ssw.ShardCount())
		}
		for i := 0; i < 80; i++ {
			if !ssw.Allow() {
				t.Fatalf("Allow %d failed", i)
			}
		}
	})

	t.Run("Sharded_Adaptive_Shards+Min+Max", func(t *testing.T) {
		sal := Ratelimit(ctx).Sharded().Adaptive().Shards(2).MinWorker(4).MaxWorker(10).Build()
		defer sal.Close()
		if sal.ShardCount() != 2 {
			t.Fatalf("expected 2 shards, got %d", sal.ShardCount())
		}
		if err := sal.Acquire(ctx); err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		sal.RecordSuccess()
		sal.Release()
	})

	t.Run("Sharded_Adaptive_AllOrders", func(t *testing.T) {
		sal := Ratelimit(ctx).Sharded().Adaptive().MaxWorker(20).Shards(2).MinWorker(4).Build()
		defer sal.Close()
		if sal.TotalMin() <= 0 {
			t.Fatal("expected positive TotalMin")
		}
	})

	t.Run("Sharded_GetShard", func(t *testing.T) {
		srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(4).Rate(100).Per(time.Second).Build()
		defer srl.Close()
		shard := srl.GetShard(0)
		if shard == nil {
			t.Fatal("GetShard(0) returned nil")
		}
		if srl.GetShard(-1) != nil {
			t.Fatal("GetShard(-1) should return nil")
		}
		if srl.GetShard(4) != nil {
			t.Fatal("GetShard(out of bounds) should return nil")
		}
	})

	t.Run("Sharded_Close_and_Resize", func(t *testing.T) {
		srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(4).Rate(100).Per(time.Second).Build()
		srl.Resize(200)
		srl.Close()
		srl.Close() // 安全二次关闭
	})

	t.Run("Sharded_RateLimiter_all_defaults", func(t *testing.T) {
		srl := Ratelimit(ctx).Sharded().RateLimiter().DefaultShards().DefaultRate().DefaultPer().DefaultBurst().Build()
		defer srl.Close()
		if srl.ShardCount() < 2 {
			t.Fatalf("DefaultShards should produce >=2 shards, got %d", srl.ShardCount())
		}
		if err := srl.Wait(ctx); err != nil {
			t.Fatalf("all-default Wait failed: %v", err)
		}
		srl.Release()
	})

	t.Run("Sharded_TokenBucket_all_defaults", func(t *testing.T) {
		stb := Ratelimit(ctx).Sharded().TokenBucket().DefaultShards().DefaultRate().DefaultCapacity().Build()
		if stb.ShardCount() < 2 {
			t.Fatalf("DefaultShards expected >=2, got %d", stb.ShardCount())
		}
		if !stb.Allow() {
			t.Fatal("all-default TokenBucket should allow")
		}
	})

	t.Run("Sharded_SlidingWindow_all_defaults", func(t *testing.T) {
		ssw := Ratelimit(ctx).Sharded().SlidingWindow().DefaultShards().DefaultLimit().DefaultWindow().Build()
		if ssw.ShardCount() < 2 {
			t.Fatalf("DefaultShards expected >=2, got %d", ssw.ShardCount())
		}
		if !ssw.Allow() {
			t.Fatal("all-default SlidingWindow should allow")
		}
	})

	t.Run("Sharded_Adaptive_all_defaults", func(t *testing.T) {
		sal := Ratelimit(ctx).Sharded().Adaptive().DefaultShards().DefaultMinWorker().DefaultMaxWorker().Build()
		defer sal.Close()
		if sal.ShardCount() < 2 {
			t.Fatalf("DefaultShards expected >=2, got %d", sal.ShardCount())
		}
		if err := sal.Acquire(ctx); err != nil {
			t.Fatalf("all-default Acquire failed: %v", err)
		}
		sal.Release()
	})
}

// ============================================================
// Section 6: RateLimiter 高并发 Acquire/Release (4 档)
// ============================================================

func TestRatelimit_Chain_HighConcurrency_RateLimiter(t *testing.T) {
	ctx := rlFreshCtx()
	for _, tier := range rlAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			rlSkipIfTooLarge(t, tier.load)
			rate := tier.load
			if rate > 100000 {
				rate = 100000
			}
			rl := Ratelimit(ctx).RateLimiter().Rate(rate).Per(time.Second).Burst(tier.burst).Build()
			defer rl.Close()

			var wg sync.WaitGroup
			var success, fail atomic.Int64
			n := tier.load
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					if err := rl.Acquire(ctx); err == nil {
						success.Add(1)
						rl.Release()
					} else {
						fail.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("RateLimiter %s: success=%d, fail=%d", tier.name, success.Load(), fail.Load())
			if success.Load() < int64(rate) {
				t.Logf("warning: only %d succeeded out of %d concurrent (rate=%d)", success.Load(), n, rate)
			}
		})
	}
}

// ============================================================
// Section 7: RateLimiter Token 模式高并发 (4 档)
// ============================================================

func TestRatelimit_Chain_HighConcurrency_Token(t *testing.T) {
	ctx := rlFreshCtx()
	for _, tier := range rlAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			rlSkipIfTooLarge(t, tier.load)
			rate := tier.load
			if rate > 100000 {
				rate = 100000
			}
			rl := Ratelimit(ctx).RateLimiter().Rate(rate).Per(time.Second).Burst(tier.burst).Build()
			defer rl.Close()

			var wg sync.WaitGroup
			var success atomic.Int64
			n := tier.load
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					tok, err := rl.Token(ctx)
					if err == nil {
						success.Add(1)
						tok.Release()
					}
				}()
			}
			wg.Wait()
			t.Logf("Token mode %s: success=%d", tier.name, success.Load())
		})
	}
}

// ============================================================
// Section 8: TokenBucket 高并发 Allow (4 档)
// ============================================================

func TestRatelimit_Chain_HighConcurrency_TokenBucket(t *testing.T) {
	ctx := rlFreshCtx()
	for _, tier := range rlAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			rlSkipIfTooLarge(t, tier.load)
			capacity := float64(tier.burst)
			tb := Ratelimit(ctx).TokenBucket().Rate(100000).Capacity(capacity).Build()

			var wg sync.WaitGroup
			var allowed atomic.Int64
			n := tier.load
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					if tb.Allow() {
						allowed.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("TokenBucket %s: allowed=%d (capacity=%.0f)", tier.name, allowed.Load(), capacity)
			if allowed.Load() < int64(capacity) {
				t.Fatalf("expected at least %d allowed, got %d", int64(capacity), allowed.Load())
			}
		})
	}
}

// ============================================================
// Section 9: SlidingWindow 高并发 Allow (4 档)
// ============================================================

func TestRatelimit_Chain_HighConcurrency_SlidingWindow(t *testing.T) {
	ctx := rlFreshCtx()
	for _, tier := range rlAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			rlSkipIfTooLarge(t, tier.load)
			limit := tier.load
			if limit > 500000 {
				limit = 500000
			}
			sw := Ratelimit(ctx).SlidingWindow().Limit(limit).Window(time.Minute).Build()

			var wg sync.WaitGroup
			var allowed atomic.Int64
			n := limit
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					if sw.Allow() {
						allowed.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("SlidingWindow %s: allowed=%d (limit=%d)", tier.name, allowed.Load(), limit)
			if allowed.Load() != int64(limit) {
				t.Logf("SlidingWindow concurrency skew: %d/%d (expected close to limit under high concurrency)", allowed.Load(), limit)
			}
		})
	}
}

// ============================================================
// Section 10: Adaptive 高并发 Acquire/Release + 自适应 (4 档)
// ============================================================

func TestRatelimit_Chain_HighConcurrency_Adaptive(t *testing.T) {
	ctx := rlFreshCtx()
	for _, tier := range rlAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			rlSkipIfTooLarge(t, tier.load)
			maxC := 50000
			if tier.load < maxC {
				maxC = tier.load
			}
			al := Ratelimit(ctx).Adaptive().MinWorker(100).MaxWorker(maxC).Build()

			var wg sync.WaitGroup
			var success, fail atomic.Int64
			n := tier.load
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func(idx int) {
					defer wg.Done()
					if err := al.Acquire(ctx); err == nil {
						success.Add(1)
						if idx%10 == 0 {
							al.RecordFailure()
						} else {
							al.RecordSuccess()
						}
						al.Release()
					} else {
						fail.Add(1)
					}
				}(i)
			}
			wg.Wait()
			t.Logf("Adaptive %s: success=%d, fail=%d (maxConcurrency=%d)", tier.name, success.Load(), fail.Load(), maxC)
		})
	}
}

// ============================================================
// Section 11: Sharded RateLimiter 高并发 (4 档)
// ============================================================

func TestRatelimit_Chain_HighConcurrency_ShardedRateLimiter(t *testing.T) {
	ctx := rlFreshCtx()
	for _, tier := range rlAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			rlSkipIfTooLarge(t, tier.load)
			rate := tier.load
			if rate > 100000 {
				rate = 100000
			}
			srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(16).Rate(rate).Per(time.Second).Burst(tier.burst).Build()
			defer srl.Close()

			var wg sync.WaitGroup
			var success, fail atomic.Int64
			n := tier.load
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					if err := srl.Acquire(ctx); err == nil {
						success.Add(1)
						srl.Release()
					} else {
						fail.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("ShardedRateLimiter %s: success=%d, fail=%d (shards=16 rate=%d)", tier.name, success.Load(), fail.Load(), rate)
		})
	}
}

// ============================================================
// Section 12: Sharded TokenBucket 高并发 (4 档)
// ============================================================

func TestRatelimit_Chain_HighConcurrency_ShardedTokenBucket(t *testing.T) {
	ctx := rlFreshCtx()
	for _, tier := range rlAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			rlSkipIfTooLarge(t, tier.load)
			stb := Ratelimit(ctx).Sharded().TokenBucket().Shards(16).Rate(50000).Capacity(float64(tier.burst)).Build()

			var wg sync.WaitGroup
			var allowed atomic.Int64
			n := tier.load
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					if stb.Allow() {
						allowed.Add(1)
					}
				}()
			}
			wg.Wait()
			t.Logf("ShardedTokenBucket %s: allowed=%d", tier.name, allowed.Load())
		})
	}
}

// ============================================================
// Section 13: 交叉场景 — 模式切换 + 策略组合
// ============================================================

func TestRatelimit_Chain_CrossScenario_StrategySwitch(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("Block_to_Reject_switch", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(1).Per(time.Second).Burst(1).Build()
		defer rl.Close()
		rl.Acquire(ctx) // 耗尽令牌
		rl.WithStrategy(Reject)
		err := rl.Acquire(ctx)
		if err == nil {
			t.Fatal("Reject strategy should fail when no tokens")
		}
		rl.Release()
	})

	t.Run("BlockForce_ignores_cancel", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(5).Per(time.Second).Burst(5).Build()
		defer rl.Close()
		rl.WithStrategy(BlockForce)
		for i := 0; i < 5; i++ {
			rl.Acquire(ctx)
		}
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		errCh := make(chan error, 1)
		go func() {
			errCh <- rl.Acquire(cancelCtx)
		}()
		time.Sleep(20 * time.Millisecond)
		rl.Release()
		select {
		case <-errCh:
		case <-time.After(time.Second):
			t.Fatal("BlockForce should not block forever after Release")
		}
		// Clean up held tokens
		for i := 0; i < 5; i++ {
			rl.Release()
		}
	})

	t.Run("Reject_vs_Block_on_exhausted", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(1).Per(time.Second).Build()
		defer rl.Close()
		rl.Acquire(ctx)
		rl.WithStrategy(Reject)
		if err := rl.Acquire(ctx); err == nil {
			t.Fatal("Reject should fail on exhausted tokens")
		}
		rl.Release()
	})
}

// ============================================================
// Section 14: 交叉场景 — RatelimitBuilder 与 ShardedRatelimitBuilder
//              不同入口交叉验证
// ============================================================

func TestRatelimit_Chain_CrossScenario_ShardedVsNonSharded(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("RateLimiter_vs_ShardedRateLimiter_same_rate", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
		defer rl.Close()
		srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(4).Rate(100).Per(time.Second).Build()
		defer srl.Close()
		if rl.Size() <= 0 {
			t.Fatal("non-sharded size should be positive")
		}
		if srl.ShardCount() != 4 {
			t.Fatalf("sharded expected 4 shards, got %d", srl.ShardCount())
		}
	})

	t.Run("TokenBucket_vs_ShardedTokenBucket", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().Rate(10).Capacity(100).Build()
		stb := Ratelimit(ctx).Sharded().TokenBucket().Shards(4).Rate(10).Capacity(25).Build()
		for i := 0; i < 100; i++ {
			tb.Allow()
		}
		if tb.Allow() {
			t.Fatal("non-sharded should be exhausted")
		}
		for i := 0; i < 100; i++ {
			stb.Allow()
		}
		if stb.Allow() {
			t.Fatal("sharded should also be exhausted")
		}
	})

	t.Run("SlidingWindow_vs_ShardedSlidingWindow", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Limit(100).Window(time.Hour).Build()
		ssw := Ratelimit(ctx).Sharded().SlidingWindow().Shards(4).Limit(100).Window(time.Hour).Build()
		for i := 0; i < 100; i++ {
			sw.Allow()
		}
		if sw.Allow() {
			t.Fatal("non-sharded should be exhausted")
		}
		for i := 0; i < 80; i++ {
			if !ssw.Allow() {
				t.Fatalf("sharded Allow %d failed", i)
			}
		}
	})

	t.Run("Adaptive_vs_ShardedAdaptive", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MinWorker(4).MaxWorker(10).Build()
		sal := Ratelimit(ctx).Sharded().Adaptive().Shards(2).MinWorker(4).MaxWorker(10).Build()
		defer sal.Close()
		if err := al.Acquire(ctx); err != nil {
			t.Fatalf("Adaptive Acquire failed: %v", err)
		}
		al.Release()
		if err := sal.Acquire(ctx); err != nil {
			t.Fatalf("ShardedAdaptive Acquire failed: %v", err)
		}
		sal.Release()
	})
}

// ============================================================
// Section 15: 交叉场景 — Token + Adaptive 与错误处理
// ============================================================

func TestRatelimit_Chain_CrossScenario_ErrorHandling(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("RateLimiter_cancelled_context", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(1).Per(time.Second).Burst(1).Build()
		defer rl.Close()
		rl.Acquire(ctx)        // 耗尽令牌
		rl.WithStrategy(Block) // 阻塞策略才会感知 context 取消
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		err := rl.Acquire(cancelCtx)
		if err == nil {
			t.Fatal("expected error with cancelled context")
		}
		rl.Release()
	})

	t.Run("Adaptive_repeated_RecordSuccess", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MinWorker(1).MaxWorker(100).Build()
		for i := 0; i < 50; i++ {
			al.Acquire(ctx)
			al.RecordSuccess()
			al.Release()
		}
	})

	t.Run("Adaptive_repeated_RecordFailure", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MinWorker(5).MaxWorker(50).Build()
		for i := 0; i < 50; i++ {
			if err := al.Acquire(ctx); err != nil {
				t.Fatalf("Acquire %d failed: %v", i, err)
			}
			al.RecordFailure()
			al.Release()
		}
	})

	t.Run("Token_double_Release_safe", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Build()
		defer rl.Close()
		tok, _ := rl.Token(ctx)
		tok.Release()
		tok.Release() // 二次 Release 安全
	})

	t.Run("closed_limiter_Release_safe", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Build()
		rl.Acquire(ctx)
		rl.Close()
		rl.Release() // 已关闭 Release 安全
	})
}

// ============================================================
// Section 16: 交叉场景 — RateLimiter Burst 突发流量测试
// ============================================================

func TestRatelimit_Chain_CrossScenario_BurstTraffic(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("burst_utilization_full", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Burst(500).Build()
		defer rl.Close()
		var wg sync.WaitGroup
		var success atomic.Int64
		n := 500
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				if err := rl.Acquire(ctx); err == nil {
					success.Add(1)
					rl.Release()
				}
			}()
		}
		wg.Wait()
		if success.Load() != 500 {
			t.Fatalf("expected 500 burst success, got %d", success.Load())
		}
	})

	t.Run("burst_without_burst_param_uses_rate", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
		defer rl.Close()
		var wg sync.WaitGroup
		var success atomic.Int64
		n := 100
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				if err := rl.Acquire(ctx); err == nil {
					success.Add(1)
					rl.Release()
				}
			}()
		}
		wg.Wait()
		if success.Load() != 100 {
			t.Fatalf("expected 100 success (rate=burst), got %d", success.Load())
		}
	})

	t.Run("sharded_burst_distribution", func(t *testing.T) {
		srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(8).Rate(80).Per(time.Second).Burst(800).Build()
		defer srl.Close()
		var wg sync.WaitGroup
		var success atomic.Int64
		n := 800
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				if err := srl.Acquire(ctx); err == nil {
					success.Add(1)
					srl.Release()
				}
			}()
		}
		wg.Wait()
		if success.Load() != 800 {
			t.Fatalf("expected 800 sharded burst success, got %d", success.Load())
		}
	})
}

// ============================================================
// Section 17: 交叉场景 — Ratelimit 4 种模式与 WithStrategy 组合
// ============================================================

func TestRatelimit_Chain_CrossScenario_AllModesWithStrategy(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("RateLimiter_with_all_3_strategies", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(5).Per(time.Second).Build()
		defer rl.Close()
		// Block (default)
		for i := 0; i < 5; i++ {
			rl.Acquire(ctx)
		}
		for i := 0; i < 5; i++ {
			rl.Release()
		}
		// Reject
		rl.WithStrategy(Reject)
		if err := rl.Acquire(ctx); err != nil {
			t.Fatal("Reject on available token should not fail")
		}
		rl.Release()
		// BlockForce
		rl.WithStrategy(BlockForce)
		if err := rl.Acquire(ctx); err != nil {
			t.Fatal("BlockForce should succeed with token")
		}
		rl.Release()
	})

	t.Run("Sharded_with_strategy_chain", func(t *testing.T) {
		srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(4).Rate(40).Per(time.Second).Build()
		defer srl.Close()
		srl.WithStrategy(Reject)
		if err := srl.Acquire(ctx); err != nil {
			t.Fatal("Sharded Reject on available tokens should succeed")
		}
		srl.Release()
	})
}

// ============================================================
// Section 18: 交叉场景 — RatelimitBuilder 不同顺序链式调用
//              验证所有链式方法顺序无关性
// ============================================================

func TestRatelimit_Chain_CrossScenario_AllOrderings(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("RateLimiter_all_6_orders", func(t *testing.T) {
		orders := []func() *RateLimiter{
			func() *RateLimiter { return Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Burst(20).Build() },
			func() *RateLimiter { return Ratelimit(ctx).RateLimiter().Rate(10).Burst(20).Per(time.Second).Build() },
			func() *RateLimiter { return Ratelimit(ctx).RateLimiter().Per(time.Second).Rate(10).Burst(20).Build() },
			func() *RateLimiter { return Ratelimit(ctx).RateLimiter().Per(time.Second).Burst(20).Rate(10).Build() },
			func() *RateLimiter { return Ratelimit(ctx).RateLimiter().Burst(20).Rate(10).Per(time.Second).Build() },
			func() *RateLimiter { return Ratelimit(ctx).RateLimiter().Burst(20).Per(time.Second).Rate(10).Build() },
		}
		for i, fn := range orders {
			rl := fn()
			if rl.Size() != 10 {
				t.Fatalf("order %d: expected size 10, got %d", i, rl.Size())
			}
			rl.Close()
		}
	})

	t.Run("TokenBucket_all_2_orders", func(t *testing.T) {
		tb1 := Ratelimit(ctx).TokenBucket().Rate(10).Capacity(20).Build()
		tb2 := Ratelimit(ctx).TokenBucket().Capacity(20).Rate(10).Build()
		for i := 0; i < 20; i++ {
			if !tb1.Allow() {
				t.Fatalf("tb1 Allow %d failed", i)
			}
			if !tb2.Allow() {
				t.Fatalf("tb2 Allow %d failed", i)
			}
		}
	})

	t.Run("SlidingWindow_all_2_orders", func(t *testing.T) {
		sw1 := Ratelimit(ctx).SlidingWindow().Limit(50).Window(time.Hour).Build()
		sw2 := Ratelimit(ctx).SlidingWindow().Window(time.Hour).Limit(50).Build()
		for i := 0; i < 50; i++ {
			if !sw1.Allow() {
				t.Fatalf("sw1 Allow %d failed", i)
			}
			if !sw2.Allow() {
				t.Fatalf("sw2 Allow %d failed", i)
			}
		}
	})

	t.Run("Adaptive_all_2_orders", func(t *testing.T) {
		al1 := Ratelimit(ctx).Adaptive().MinWorker(5).MaxWorker(10).Build()
		al2 := Ratelimit(ctx).Adaptive().MaxWorker(10).MinWorker(5).Build()
		for i := 0; i < 5; i++ {
			if err := al1.Acquire(ctx); err != nil {
				t.Fatalf("al1 Acquire %d failed: %v", i, err)
			}
			if err := al2.Acquire(ctx); err != nil {
				t.Fatalf("al2 Acquire %d failed: %v", i, err)
			}
		}
		for i := 0; i < 5; i++ {
			al1.Release()
			al2.Release()
		}
	})

	t.Run("Default_mixed_with_explicit", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().DefaultRate().Per(500 * time.Millisecond).DefaultBurst().Build()
		defer rl.Close()
		if rl.Size() <= 0 {
			t.Fatal("DefaultRate mixed with explicit Per should produce positive size")
		}
		if err := rl.Acquire(ctx); err != nil {
			t.Fatalf("mixed default+explicit Acquire failed: %v", err)
		}
		rl.Release()

		stb := Ratelimit(ctx).Sharded().TokenBucket().Rate(500).DefaultShards().DefaultCapacity().Build()
		if stb.ShardCount() < 2 {
			t.Fatal("mixed sharded DefaultShards should produce >=2")
		}
		if !stb.Allow() {
			t.Fatal("mixed default+explicit sharded TokenBucket should allow")
		}
	})

	t.Run("ShardedRateLimiter_all_24_orders_exemplars", func(t *testing.T) {
		exemplars := []func() *ShardedRateLimiter{
			func() *ShardedRateLimiter {
				return Ratelimit(ctx).Sharded().RateLimiter().Shards(4).Rate(40).Per(time.Second).Burst(80).Build()
			},
			func() *ShardedRateLimiter {
				return Ratelimit(ctx).Sharded().RateLimiter().Burst(80).Shards(4).Per(time.Second).Rate(40).Build()
			},
			func() *ShardedRateLimiter {
				return Ratelimit(ctx).Sharded().RateLimiter().Per(time.Second).Rate(40).Shards(4).Burst(80).Build()
			},
			func() *ShardedRateLimiter {
				return Ratelimit(ctx).Sharded().RateLimiter().Rate(40).Burst(80).Shards(4).Per(time.Second).Build()
			},
		}
		for i, fn := range exemplars {
			srl := fn()
			if srl.ShardCount() != 4 {
				t.Fatalf("exemplar %d: expected 4 shards, got %d", i, srl.ShardCount())
			}
			srl.Close()
		}
	})
}

// ============================================================
// Section 19: 边界条件测试
// ============================================================

func TestRatelimit_Chain_Boundary(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("RateLimiter_zero_rate", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(0).Per(time.Second).Build()
		defer rl.Close()
		if rl.Size() <= 0 {
			t.Fatal("zero rate should default to positive value")
		}
	})

	t.Run("RateLimiter_negative_rate", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(-1).Per(time.Second).Build()
		defer rl.Close()
		if rl.Size() <= 0 {
			t.Fatal("negative rate should default to positive value")
		}
	})

	t.Run("TokenBucket_zero_capacity", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().Rate(10).Capacity(0).Build()
		if tb.Allow() {
			t.Fatal("zero capacity should not allow")
		}
	})

	t.Run("SlidingWindow_zero_limit", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Limit(0).Window(time.Second).Build()
		if sw.Allow() {
			t.Fatal("zero limit should not allow")
		}
	})

	t.Run("SlidingWindow_AllowN_exceeds_limit", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Limit(10).Window(time.Second).Build()
		if sw.AllowN(20) {
			t.Fatal("AllowN(20) should fail when limit is 10")
		}
	})

	t.Run("Adaptive_min_equals_max", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MinWorker(5).MaxWorker(5).Build()
		if err := al.Acquire(ctx); err != nil {
			t.Fatalf("min==max Acquire failed: %v", err)
		}
		al.Release()
	})

	t.Run("Adaptive_min_greater_than_max", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MinWorker(100).MaxWorker(5).Build()
		if err := al.Acquire(ctx); err != nil {
			t.Fatalf("min>max should be handled: %v", err)
		}
		al.Release()
	})

	t.Run("Sharded_zero_shards", func(t *testing.T) {
		srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(0).Rate(100).Per(time.Second).Build()
		defer srl.Close()
		if srl.ShardCount() < 2 {
			t.Fatalf("zero shards should default to >=2, got %d", srl.ShardCount())
		}
	})

	t.Run("Sharded_negative_shards", func(t *testing.T) {
		srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(-5).Rate(100).Per(time.Second).Build()
		defer srl.Close()
		if srl.ShardCount() < 2 {
			t.Fatalf("negative shards should default to >=2, got %d", srl.ShardCount())
		}
	})

	t.Run("RateLimiter_Resize_zero", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Build()
		defer rl.Close()
		oldSize := rl.Size()
		rl.Resize(0)
		if rl.Size() != oldSize {
			t.Fatalf("Resize(0) should not change size, got %d", rl.Size())
		}
	})

	t.Run("RateLimiter_Resize_negative", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Build()
		defer rl.Close()
		oldSize := rl.Size()
		rl.Resize(-1)
		if rl.Size() != oldSize {
			t.Fatalf("Resize(-1) should not change size, got %d", rl.Size())
		}
	})

	t.Run("RateLimiter_Available_on_exhausted", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(5).Per(time.Second).Burst(5).Build()
		defer rl.Close()
		for i := 0; i < 5; i++ {
			rl.Acquire(ctx)
		}
		if rl.Available() != 0 {
			t.Fatalf("expected 0 available after exhausting, got %d", rl.Available())
		}
		for i := 0; i < 5; i++ {
			rl.Release()
		}
	})

	t.Run("RateLimiter_Stop_alias", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(5).Per(time.Second).Build()
		rl.Stop()
		rl.Close() // Stop 后再 Close 安全
	})
}

// ============================================================
// Section 20: 并发安全 — 多 goroutine 同时操作同构建器链
// ============================================================

func TestRatelimit_Chain_ConcurrencySafety(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("RateLimiter_concurrent_AcquireRelease", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(1000).Per(time.Second).Build()
		defer rl.Close()
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
		wg.Wait()
	})

	t.Run("TokenBucket_concurrent_Allow", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().Rate(10000).Capacity(10000).Build()
		var wg sync.WaitGroup
		var allowed atomic.Int64
		n := 10000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				if tb.Allow() {
					allowed.Add(1)
				}
			}()
		}
		wg.Wait()
		if allowed.Load() != 10000 {
			t.Fatalf("expected 10000 allowed, got %d", allowed.Load())
		}
	})

	t.Run("SlidingWindow_concurrent_Allow", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Limit(10000).Window(time.Hour).Build()
		var wg sync.WaitGroup
		var allowed atomic.Int64
		n := 10000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				if sw.Allow() {
					allowed.Add(1)
				}
			}()
		}
		wg.Wait()
		if allowed.Load() != 10000 {
			t.Fatalf("expected 10000 allowed, got %d", allowed.Load())
		}
	})

	t.Run("RateLimiter_concurrent_Resize", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(10000).Per(time.Second).Build()
		defer rl.Close()
		var wg sync.WaitGroup
		n := 500
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(idx int) {
				defer wg.Done()
				if idx%2 == 0 {
					rl.Resize(500 + rand.Intn(9500))
				} else {
					if err := rl.Acquire(ctx); err == nil {
						rl.Release()
					}
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("RateLimiter_concurrent_WithStrategy", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(10000).Per(time.Second).Build()
		defer rl.Close()
		var wg sync.WaitGroup
		n := 500
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(idx int) {
				defer wg.Done()
				switch idx % 3 {
				case 0:
					rl.WithStrategy(Block)
				case 1:
					rl.WithStrategy(Reject)
				case 2:
					rl.WithStrategy(BlockForce)
				}
			}(i)
		}
		wg.Wait()
	})
}

// ============================================================
// Section 21: Race 检测专项 — go test -race 专用
// ============================================================

func TestRatelimit_Chain_Race_RateLimiter(t *testing.T) {
	ctx := rlFreshCtx()
	rl := Ratelimit(ctx).RateLimiter().Rate(1000).Per(time.Second).Build()
	defer rl.Close()
	var wg sync.WaitGroup
	n := 500
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				if err := rl.Acquire(ctx); err == nil {
					time.Sleep(time.Microsecond)
					rl.Release()
				}
			}
		}()
	}
	wg.Wait()
}

func TestRatelimit_Chain_Race_TokenBucket(t *testing.T) {
	ctx := rlFreshCtx()
	tb := Ratelimit(ctx).TokenBucket().Rate(100000).Capacity(100000).Build()
	var wg sync.WaitGroup
	n := 500
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				tb.Allow()
			}
		}()
	}
	wg.Wait()
}

func TestRatelimit_Chain_Race_SlidingWindow(t *testing.T) {
	ctx := rlFreshCtx()
	sw := Ratelimit(ctx).SlidingWindow().Limit(100000).Window(time.Hour).Build()
	var wg sync.WaitGroup
	n := 500
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				sw.Allow()
			}
		}()
	}
	wg.Wait()
}

func TestRatelimit_Chain_Race_Adaptive(t *testing.T) {
	ctx := rlFreshCtx()
	al := Ratelimit(ctx).Adaptive().MinWorker(100).MaxWorker(500).Build()
	var wg sync.WaitGroup
	n := 200
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				if err := al.Acquire(ctx); err == nil {
					al.RecordSuccess()
					al.Release()
				}
			}
		}()
	}
	wg.Wait()
}

func TestRatelimit_Chain_Race_ShardedRateLimiter(t *testing.T) {
	ctx := rlFreshCtx()
	srl := Ratelimit(ctx).Sharded().RateLimiter().Shards(8).Rate(1000).Per(time.Second).Build()
	defer srl.Close()
	var wg sync.WaitGroup
	n := 500
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				if err := srl.Acquire(ctx); err == nil {
					time.Sleep(time.Microsecond)
					srl.Release()
				}
			}
		}()
	}
	wg.Wait()
}

func TestRatelimit_Chain_Race_ShardedAdaptive(t *testing.T) {
	ctx := rlFreshCtx()
	sal := Ratelimit(ctx).Sharded().Adaptive().Shards(4).MinWorker(40).MaxWorker(200).Build()
	defer sal.Close()
	var wg sync.WaitGroup
	n := 200
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				if err := sal.Acquire(ctx); err == nil {
					sal.RecordSuccess()
					sal.Release()
				}
			}
		}()
	}
	wg.Wait()
}

// ============================================================
// Section 22: 交叉场景 — Ratelimit 与 Retry 边界分离验证
//           确保 Ratelimit 链式 API 完全独立于 Retry
// ============================================================

func TestRatelimit_Chain_Independence(t *testing.T) {
	ctx := rlFreshCtx()

	t.Run("Ratelimit_standalone_no_retry_dependency", func(t *testing.T) {
		rl := Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
		defer rl.Close()
		if err := rl.Acquire(ctx); err != nil {
			t.Fatalf("standalone RateLimiter failed: %v", err)
		}
		rl.Release()
	})

	t.Run("TokenBucket_standalone", func(t *testing.T) {
		tb := Ratelimit(ctx).TokenBucket().Rate(10).Capacity(10).Build()
		if !tb.Allow() {
			t.Fatal("standalone TokenBucket failed")
		}
	})

	t.Run("SlidingWindow_standalone", func(t *testing.T) {
		sw := Ratelimit(ctx).SlidingWindow().Limit(10).Window(time.Second).Build()
		if !sw.Allow() {
			t.Fatal("standalone SlidingWindow failed")
		}
	})

	t.Run("Adaptive_standalone", func(t *testing.T) {
		al := Ratelimit(ctx).Adaptive().MinWorker(1).MaxWorker(10).Build()
		if err := al.Acquire(ctx); err != nil {
			t.Fatalf("standalone Adaptive failed: %v", err)
		}
		al.Release()
	})
}
