package ratelimit

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ──────────────────────────── 入口：RatelimitBuilder ────────────────────────────

// RatelimitBuilder 限流器链式构建器入口，通过 async.Ratelimit(ctx) 创建。
// 每个子 builder 只暴露对应模式的参数方法，Go 类型系统天然隔离不通用参数。
//
// 使用示例：
//
//	// RateLimiter 模式
//	rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
//
//	// TokenBucket 模式
//	tb := async.Ratelimit(ctx).TokenBucket().Rate(10).Capacity(20).Build()
//
//	// SlidingWindow 模式
//	sw := async.Ratelimit(ctx).SlidingWindow().Limit(100).Window(10*time.Second).Build()
//
//	// Adaptive 模式
//	al := async.Ratelimit(ctx).Adaptive().MinConcurrency(5).MaxConcurrency(100).Build()
//
//	// 分片模式
//	srl := async.Ratelimit(ctx).Sharded().RateLimiter().Rate(100).Per(time.Second).Shards(16).Build()
type RatelimitBuilder struct {
	ctx context.Context
}

// NewRatelimitBuilder 创建限流器链式构建器。
func NewRatelimitBuilder(ctx context.Context) *RatelimitBuilder {
	return &RatelimitBuilder{ctx: ctx}
}

// RateLimiter 返回 RateLimiter 子构建器。
func (b *RatelimitBuilder) RateLimiter() *RatelimiterSubBuilder {
	return &RatelimiterSubBuilder{ctx: b.ctx}
}

// TokenBucket 返回 TokenBucket 子构建器。
func (b *RatelimitBuilder) TokenBucket() *TokenBucketSubBuilder {
	return &TokenBucketSubBuilder{ctx: b.ctx}
}

// SlidingWindow 返回 SlidingWindow 子构建器。
func (b *RatelimitBuilder) SlidingWindow() *SlidingWindowSubBuilder {
	return &SlidingWindowSubBuilder{ctx: b.ctx}
}

// Adaptive 返回 Adaptive 子构建器。
func (b *RatelimitBuilder) Adaptive() *AdaptiveSubBuilder {
	return &AdaptiveSubBuilder{ctx: b.ctx}
}

// Sharded 返回分片限流器入口构建器。
func (b *RatelimitBuilder) Sharded() *ShardedRatelimitBuilder {
	return &ShardedRatelimitBuilder{ctx: b.ctx}
}

// ──────────────────────────── RatelimiterSubBuilder ────────────────────────────

// RatelimiterSubBuilder RateLimiter 模式子构建器，设置令牌补充速率/窗口/突发容量后 Build。
//
// 链式调用示例：
//
//	rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Burst(200).Build()
type RatelimiterSubBuilder struct {
	ctx         context.Context
	rate        int
	perDuration time.Duration
	burst       int
}

// Rate 设置速率：每 PerDuration 内允许的操作次数。
func (b *RatelimiterSubBuilder) Rate(n int) *RatelimiterSubBuilder {
	b.rate = n
	return b
}

// Per 设置时间窗口大小。
func (b *RatelimiterSubBuilder) Per(d time.Duration) *RatelimiterSubBuilder {
	b.perDuration = d
	return b
}

// Burst 设置突发容量，允许短时间超过 Rate 速率的请求数。
func (b *RatelimiterSubBuilder) Burst(n int) *RatelimiterSubBuilder {
	b.burst = n
	return b
}

// Build 构建 RateLimiter 实例。
func (b *RatelimiterSubBuilder) Build() *RateLimiter {
	if b.burst > 0 {
		return NewRateLimiterWithBurst(b.rate, b.perDuration, b.burst)
	}
	return NewRateLimiter(b.rate, b.perDuration)
}

// DefaultRate 使用默认速率（GOMAXPROCS）。
func (b *RatelimiterSubBuilder) DefaultRate() *RatelimiterSubBuilder {
	b.rate = core.IO()
	return b
}

// DefaultPer 使用默认时间窗口（1 秒）。
func (b *RatelimiterSubBuilder) DefaultPer() *RatelimiterSubBuilder {
	b.perDuration = time.Second
	return b
}

// DefaultBurst 使用默认突发容量（GOMAXPROCS × 2）。
func (b *RatelimiterSubBuilder) DefaultBurst() *RatelimiterSubBuilder {
	b.burst = core.IO() * 2
	return b
}

// ──────────────────────────── TokenBucketSubBuilder ────────────────────────────

// TokenBucketSubBuilder TokenBucket 模式子构建器，设置每秒令牌生成速率与最大容量后 Build。
//
// 链式调用示例：
//
//	tb := async.Ratelimit(ctx).TokenBucket().Rate(10).Capacity(20).Build()
type TokenBucketSubBuilder struct {
	ctx      context.Context
	rate     float64
	capacity float64
}

// Rate 设置每秒生成的令牌数。
func (b *TokenBucketSubBuilder) Rate(n float64) *TokenBucketSubBuilder {
	b.rate = n
	return b
}

// Capacity 设置最大令牌容量（决定允许的突发流量大小）。
func (b *TokenBucketSubBuilder) Capacity(n float64) *TokenBucketSubBuilder {
	b.capacity = n
	return b
}

// Build 构建 TokenBucket 实例。
func (b *TokenBucketSubBuilder) Build() *TokenBucket {
	return NewTokenBucket(b.rate, b.capacity)
}

// DefaultRate 使用默认令牌生成速率（GOMAXPROCS）。
func (b *TokenBucketSubBuilder) DefaultRate() *TokenBucketSubBuilder {
	b.rate = float64(core.IO())
	return b
}

// DefaultCapacity 使用默认容量（GOMAXPROCS × 2）。
func (b *TokenBucketSubBuilder) DefaultCapacity() *TokenBucketSubBuilder {
	b.capacity = float64(core.IO() * 2)
	return b
}

// ──────────────────────────── SlidingWindowSubBuilder ────────────────────────────

// SlidingWindowSubBuilder SlidingWindow 模式子构建器，设置窗口大小与限制次数后 Build。
//
// 链式调用示例：
//
//	sw := async.Ratelimit(ctx).SlidingWindow().Limit(100).Window(10*time.Second).Build()
type SlidingWindowSubBuilder struct {
	ctx    context.Context
	limit  int
	window time.Duration
}

// Limit 设置时间窗口内允许的最大请求数。
func (b *SlidingWindowSubBuilder) Limit(n int) *SlidingWindowSubBuilder {
	b.limit = n
	return b
}

// Window 设置时间窗口大小。
func (b *SlidingWindowSubBuilder) Window(d time.Duration) *SlidingWindowSubBuilder {
	b.window = d
	return b
}

// Build 构建 SlidingWindowRateLimiter 实例。
func (b *SlidingWindowSubBuilder) Build() *SlidingWindowRateLimiter {
	return NewSlidingWindowRateLimiter(b.limit, b.window)
}

// DefaultLimit 使用默认限制次数（GOMAXPROCS）。
func (b *SlidingWindowSubBuilder) DefaultLimit() *SlidingWindowSubBuilder {
	b.limit = core.IO()
	return b
}

// DefaultWindow 使用默认时间窗口（1 秒）。
func (b *SlidingWindowSubBuilder) DefaultWindow() *SlidingWindowSubBuilder {
	b.window = time.Second
	return b
}

// ──────────────────────────── AdaptiveSubBuilder ────────────────────────────

// AdaptiveSubBuilder Adaptive 模式子构建器，设置并发度范围后 Build。
//
// 链式调用示例：
//
//	al := async.Ratelimit(ctx).Adaptive().MinConcurrency(5).MaxConcurrency(100).Build()
type AdaptiveSubBuilder struct {
	ctx     context.Context
	minRate int
	maxRate int
}

// MinConcurrency 设置最小并发度（负载低时不会低于此值）。
func (b *AdaptiveSubBuilder) MinConcurrency(n int) *AdaptiveSubBuilder {
	b.minRate = n
	return b
}

// MaxConcurrency 设置最大并发度（负载高时不会超过此值）。
func (b *AdaptiveSubBuilder) MaxConcurrency(n int) *AdaptiveSubBuilder {
	b.maxRate = n
	return b
}

// Build 构建 AdaptiveRateLimiter 实例。
func (b *AdaptiveSubBuilder) Build() *AdaptiveRateLimiter {
	return NewAdaptiveRateLimiter(b.minRate, b.maxRate)
}

// DefaultMinConcurrency 使用默认最小并发度（GOMAXPROCS）。
func (b *AdaptiveSubBuilder) DefaultMinConcurrency() *AdaptiveSubBuilder {
	b.minRate = core.IO()
	return b
}

// DefaultMaxConcurrency 使用默认最大并发度（GOMAXPROCS × 10）。
func (b *AdaptiveSubBuilder) DefaultMaxConcurrency() *AdaptiveSubBuilder {
	b.maxRate = core.IO() * 10
	return b
}

// ──────────────────────────── 分片入口：ShardedRatelimitBuilder ────────────────────────────

// ShardedRatelimitBuilder 分片限流器入口构建器，通过 RatelimitBuilder.Sharded() 创建。
// 各分片独立运行，round-robin 分发请求，将锁竞争降低到 1/N。
type ShardedRatelimitBuilder struct {
	ctx context.Context
}

// RateLimiter 返回分片 RateLimiter 子构建器。
func (b *ShardedRatelimitBuilder) RateLimiter() *ShardedRatelimiterSubBuilder {
	return &ShardedRatelimiterSubBuilder{ctx: b.ctx}
}

// TokenBucket 返回分片 TokenBucket 子构建器。
func (b *ShardedRatelimitBuilder) TokenBucket() *ShardedTokenBucketSubBuilder {
	return &ShardedTokenBucketSubBuilder{ctx: b.ctx}
}

// SlidingWindow 返回分片 SlidingWindow 子构建器。
func (b *ShardedRatelimitBuilder) SlidingWindow() *ShardedSlidingWindowSubBuilder {
	return &ShardedSlidingWindowSubBuilder{ctx: b.ctx}
}

// Adaptive 返回分片 Adaptive 子构建器。
func (b *ShardedRatelimitBuilder) Adaptive() *ShardedAdaptiveSubBuilder {
	return &ShardedAdaptiveSubBuilder{ctx: b.ctx}
}

// ──────────────────────────── ShardedRatelimiterSubBuilder ────────────────────────────

// ShardedRatelimiterSubBuilder 分片 RateLimiter 子构建器，设置分片数/速率/窗口/突发后 Build。
//
// 链式调用示例：
//
//	srl := async.Ratelimit(ctx).Sharded().RateLimiter().Shards(16).Rate(10000).Per(time.Second).Build()
type ShardedRatelimiterSubBuilder struct {
	ctx         context.Context
	shards      int
	rate        int
	perDuration time.Duration
	burst       int
}

// Shards 设置水平分片数。<=0 时自动使用 DefaultShardCount()。
func (b *ShardedRatelimiterSubBuilder) Shards(n int) *ShardedRatelimiterSubBuilder {
	b.shards = n
	return b
}

// Rate 设置总速率（每 PerDuration），平均分配到各分片。
func (b *ShardedRatelimiterSubBuilder) Rate(n int) *ShardedRatelimiterSubBuilder {
	b.rate = n
	return b
}

// Per 设置时间窗口大小。
func (b *ShardedRatelimiterSubBuilder) Per(d time.Duration) *ShardedRatelimiterSubBuilder {
	b.perDuration = d
	return b
}

// Burst 设置突发容量，均匀分配到各分片。
func (b *ShardedRatelimiterSubBuilder) Burst(n int) *ShardedRatelimiterSubBuilder {
	b.burst = n
	return b
}

// Build 构建 ShardedRateLimiter 实例。
func (b *ShardedRatelimiterSubBuilder) Build() *ShardedRateLimiter {
	if b.burst > 0 {
		return NewShardedRateLimiterWithBurst(b.shards, b.rate, b.perDuration, b.burst)
	}
	return NewShardedRateLimiter(b.shards, b.rate, b.perDuration)
}

// DefaultShards 使用默认分片数（GOMAXPROCS，最少 2）。
func (b *ShardedRatelimiterSubBuilder) DefaultShards() *ShardedRatelimiterSubBuilder {
	b.shards = DefaultShardCount()
	return b
}

// DefaultRate 使用默认速率（GOMAXPROCS）。
func (b *ShardedRatelimiterSubBuilder) DefaultRate() *ShardedRatelimiterSubBuilder {
	b.rate = core.IO()
	return b
}

// DefaultPer 使用默认时间窗口（1 秒）。
func (b *ShardedRatelimiterSubBuilder) DefaultPer() *ShardedRatelimiterSubBuilder {
	b.perDuration = time.Second
	return b
}

// DefaultBurst 使用默认突发容量（GOMAXPROCS × 2）。
func (b *ShardedRatelimiterSubBuilder) DefaultBurst() *ShardedRatelimiterSubBuilder {
	b.burst = core.IO() * 2
	return b
}

// ──────────────────────────── ShardedTokenBucketSubBuilder ────────────────────────────

// ShardedTokenBucketSubBuilder 分片 TokenBucket 子构建器，设置分片数/速率/容量后 Build。
//
// 链式调用示例：
//
//	stb := async.Ratelimit(ctx).Sharded().TokenBucket().Shards(16).Rate(10).Capacity(20).Build()
type ShardedTokenBucketSubBuilder struct {
	ctx      context.Context
	shards   int
	rate     float64
	capacity float64
}

// Shards 设置水平分片数。
func (b *ShardedTokenBucketSubBuilder) Shards(n int) *ShardedTokenBucketSubBuilder {
	b.shards = n
	return b
}

// Rate 设置每片每秒生成的令牌数。
func (b *ShardedTokenBucketSubBuilder) Rate(n float64) *ShardedTokenBucketSubBuilder {
	b.rate = n
	return b
}

// Capacity 设置每片最大令牌容量。
func (b *ShardedTokenBucketSubBuilder) Capacity(n float64) *ShardedTokenBucketSubBuilder {
	b.capacity = n
	return b
}

// Build 构建 ShardedTokenBucket 实例。
func (b *ShardedTokenBucketSubBuilder) Build() *ShardedTokenBucket {
	return NewShardedTokenBucket(b.shards, b.rate, b.capacity)
}

// DefaultShards 使用默认分片数（GOMAXPROCS，最少 2）。
func (b *ShardedTokenBucketSubBuilder) DefaultShards() *ShardedTokenBucketSubBuilder {
	b.shards = DefaultShardCount()
	return b
}

// DefaultRate 使用默认令牌生成速率（GOMAXPROCS）。
func (b *ShardedTokenBucketSubBuilder) DefaultRate() *ShardedTokenBucketSubBuilder {
	b.rate = float64(core.IO())
	return b
}

// DefaultCapacity 使用默认容量（GOMAXPROCS × 2）。
func (b *ShardedTokenBucketSubBuilder) DefaultCapacity() *ShardedTokenBucketSubBuilder {
	b.capacity = float64(core.IO() * 2)
	return b
}

// ──────────────────────────── ShardedSlidingWindowSubBuilder ────────────────────────────

// ShardedSlidingWindowSubBuilder 分片 SlidingWindow 子构建器，设置分片数/限制/窗口后 Build。
//
// 链式调用示例：
//
//	ssw := async.Ratelimit(ctx).Sharded().SlidingWindow().Shards(16).Limit(10000).Window(time.Second).Build()
type ShardedSlidingWindowSubBuilder struct {
	ctx    context.Context
	shards int
	limit  int
	window time.Duration
}

// Shards 设置水平分片数。
func (b *ShardedSlidingWindowSubBuilder) Shards(n int) *ShardedSlidingWindowSubBuilder {
	b.shards = n
	return b
}

// Limit 设置总限制次数（每窗口），平均分配到各分片。
func (b *ShardedSlidingWindowSubBuilder) Limit(n int) *ShardedSlidingWindowSubBuilder {
	b.limit = n
	return b
}

// Window 设置时间窗口大小。
func (b *ShardedSlidingWindowSubBuilder) Window(d time.Duration) *ShardedSlidingWindowSubBuilder {
	b.window = d
	return b
}

// Build 构建 ShardedSlidingWindowRateLimiter 实例。
func (b *ShardedSlidingWindowSubBuilder) Build() *ShardedSlidingWindowRateLimiter {
	return NewShardedSlidingWindowRateLimiter(b.shards, b.limit, b.window)
}

// DefaultShards 使用默认分片数（GOMAXPROCS，最少 2）。
func (b *ShardedSlidingWindowSubBuilder) DefaultShards() *ShardedSlidingWindowSubBuilder {
	b.shards = DefaultShardCount()
	return b
}

// DefaultLimit 使用默认限制次数（GOMAXPROCS）。
func (b *ShardedSlidingWindowSubBuilder) DefaultLimit() *ShardedSlidingWindowSubBuilder {
	b.limit = core.IO()
	return b
}

// DefaultWindow 使用默认时间窗口（1 秒）。
func (b *ShardedSlidingWindowSubBuilder) DefaultWindow() *ShardedSlidingWindowSubBuilder {
	b.window = time.Second
	return b
}

// ──────────────────────────── ShardedAdaptiveSubBuilder ────────────────────────────

// ShardedAdaptiveSubBuilder 分片 Adaptive 子构建器，设置分片数/最小/最大并发度后 Build。
//
// 链式调用示例：
//
//	sal := async.Ratelimit(ctx).Sharded().Adaptive().Shards(16).MinConcurrency(32).MaxConcurrency(800).Build()
type ShardedAdaptiveSubBuilder struct {
	ctx     context.Context
	shards  int
	minRate int
	maxRate int
}

// Shards 设置水平分片数。
func (b *ShardedAdaptiveSubBuilder) Shards(n int) *ShardedAdaptiveSubBuilder {
	b.shards = n
	return b
}

// MinConcurrency 设置最小并发度，平均分配到各分片。
func (b *ShardedAdaptiveSubBuilder) MinConcurrency(n int) *ShardedAdaptiveSubBuilder {
	b.minRate = n
	return b
}

// MaxConcurrency 设置最大并发度，平均分配到各分片。
func (b *ShardedAdaptiveSubBuilder) MaxConcurrency(n int) *ShardedAdaptiveSubBuilder {
	b.maxRate = n
	return b
}

// Build 构建 ShardedAdaptiveRateLimiter 实例。
func (b *ShardedAdaptiveSubBuilder) Build() *ShardedAdaptiveRateLimiter {
	return NewShardedAdaptiveRateLimiter(b.shards, b.minRate, b.maxRate)
}

// DefaultShards 使用默认分片数（GOMAXPROCS，最少 2）。
func (b *ShardedAdaptiveSubBuilder) DefaultShards() *ShardedAdaptiveSubBuilder {
	b.shards = DefaultShardCount()
	return b
}

// DefaultMinConcurrency 使用默认最小并发度（GOMAXPROCS）。
func (b *ShardedAdaptiveSubBuilder) DefaultMinConcurrency() *ShardedAdaptiveSubBuilder {
	b.minRate = core.IO()
	return b
}

// DefaultMaxConcurrency 使用默认最大并发度（GOMAXPROCS × 10）。
func (b *ShardedAdaptiveSubBuilder) DefaultMaxConcurrency() *ShardedAdaptiveSubBuilder {
	b.maxRate = core.IO() * 10
	return b
}
