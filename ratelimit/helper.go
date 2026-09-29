package ratelimit

import (
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ──────────────────────────── DefaultShardCount ────────────────────────────

// DefaultShardCount 返回默认分片数（runtime.GOMAXPROCS(0)，最少 2）。
// 可在创建 ShardedPool/ShardedGroup/ShardedRateLimiter 等时使用。
func DefaultShardCount() int {
	n := core.IO()
	if n < 2 {
		return 2
	}
	return n
}

// ──────────────────────────── With*：自动 Close 便捷包装函数 ────────────────────────────
// 以下函数在 fn 执行完毕后自动调用 Close() 释放后台资源，
// 用户无需手动 defer Close()。

// WithRateLimiter 创建限流器并执行 fn，执行完毕后自动 Close。
// fn 接收已创建好的 *RateLimiter，可返回 error。
//
// 示例：
//
//	err := ratelimit.WithRateLimiter(1000, time.Second, func(rl *RateLimiter) error {
//	    rl.Wait(ctx)
//	    return doRequest()
//	})
func WithRateLimiter(rate int, perDuration time.Duration, fn func(rl *RateLimiter) error) error {
	rl := NewRateLimiter(rate, perDuration)
	defer rl.Close()
	return fn(rl)
}

// WithRateLimiterWithBurst 创建带突发容量的限流器并执行 fn，执行完毕后自动 Close。
//
// 示例：
//
//	err := ratelimit.WithRateLimiterWithBurst(50, time.Second, 200, func(rl *RateLimiter) error {
//	    rl.Wait(ctx)
//	    return doRequest()
//	})
func WithRateLimiterWithBurst(rate int, perDuration time.Duration, burst int, fn func(rl *RateLimiter) error) error {
	rl := NewRateLimiterWithBurst(rate, perDuration, burst)
	defer rl.Close()
	return fn(rl)
}

// WithShardedRateLimiter 创建分片限流器并执行 fn，执行完毕后自动 Close。
// shards <= 0 时自动使用 DefaultShardCount()。
//
// 示例：
//
//	err := ratelimit.WithShardedRateLimiter(16, 10000, time.Second, func(rl *ShardedRateLimiter) error {
//	    rl.Wait(ctx)
//	    return doRequest()
//	})
func WithShardedRateLimiter(shards int, rate int, perDuration time.Duration, fn func(rl *ShardedRateLimiter) error) error {
	rl := NewShardedRateLimiter(shards, rate, perDuration)
	defer rl.Close()
	return fn(rl)
}

// WithShardedRateLimiterWithBurst 创建带突发容量的分片限流器并执行 fn，执行完毕后自动 Close。
//
// 示例：
//
//	err := ratelimit.WithShardedRateLimiterWithBurst(16, 100, time.Second, 500, func(rl *ShardedRateLimiter) error {
//	    rl.Wait(ctx)
//	    return doRequest()
//	})
func WithShardedRateLimiterWithBurst(shards int, rate int, perDuration time.Duration, burst int, fn func(rl *ShardedRateLimiter) error) error {
	rl := NewShardedRateLimiterWithBurst(shards, rate, perDuration, burst)
	defer rl.Close()
	return fn(rl)
}

// WithShardedAdaptiveRateLimiter 创建分片自适应限流器并执行 fn，执行完毕后自动 Close。
// shards <= 0 时自动使用 DefaultShardCount()。
//
// 示例：
//
//	err := ratelimit.WithShardedAdaptiveRateLimiter(16, 32, 800, func(sa *ShardedAdaptiveRateLimiter) error {
//	    sa.Acquire(ctx)
//	    if err := doRequest(); err != nil { sa.RecordFailure() } else { sa.RecordSuccess() }
//	    sa.Release()
//	    return err
//	})
func WithShardedAdaptiveRateLimiter(shards int, minRate int, maxRate int, fn func(sa *ShardedAdaptiveRateLimiter) error) error {
	sa := NewShardedAdaptiveRateLimiter(shards, minRate, maxRate)
	defer sa.Close()
	return fn(sa)
}
