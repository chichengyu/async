package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/chichengyu/async/internal/ratelimit"
)

// limitMode 限流模式枚举。
type limitMode int

const (
	modeNone limitMode = iota
	modeRateLimiter
	modeTokenBucket
	modeSlidingWindow
	modeAdaptive
)

// ──────────────────────────── RetryChain[T] ────────────────────────────

// RetryChain 泛型重试链式构建器，提供声明式重试 API。
//
// 支持 4 种限流模式：
//   - RateLimiter()   —— 令牌补充限流器，阻塞等待令牌
//   - TokenBucket()   —— 经典令牌桶，非阻塞检查
//   - SlidingWindow() —— 滑动窗口精确计数
//   - Adaptive()      —— 自适应并发限流，根据成功率动态调整
//
// 链式调用顺序：策略 → 配置 → 限流(可选) → 终端执行
//
// 使用示例：
//
//	// 纯指数退避
//	val, err := retry.New[string](ctx).
//	    Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second).
//	    Execute(fn)
//
//	// RateLimiter 限速
//	val, err := retry.New[string](ctx).
//	    Exponential().MaxRetries(5).Backoff(100*time.Millisecond, 10*time.Second).
//	    RateLimiter().Rate(10).Per(time.Second).Shards(8).
//	    Execute(fn)
//
//	// TokenBucket 限速
//	err := retry.NewVoid(ctx).
//	    Exponential().MaxRetries(10).Backoff(100*time.Millisecond, 5*time.Second).
//	    TokenBucket().Rate(5).Capacity(20).
//	    ExecuteVoid(fn)
//
//	// SlidingWindow 滑动窗口
//	val, err := retry.New[string](ctx).
//	    Linear().MaxRetries(5).Backoff(200*time.Millisecond).
//	    SlidingWindow().Limit(100).Window(10*time.Second).Shards(8).
//	    Execute(fn)
//
//	// Adaptive 自适应限流
//	err := retry.NewVoid(ctx).
//	    Exponential().MaxRetries(10).Backoff(50*time.Millisecond, 5*time.Second).
//	    Adaptive().MinWorker(5).MaxWorker(100).Shards(16).
//	    ExecuteVoid(fn)
type RetryChain[T any] struct {
	ctx context.Context // 请求上下文

	maxRetries     int           // 最大重试次数
	initialBackoff time.Duration // 初始退避时间
	maxBackoff     time.Duration // 最大退避时间上限
	perCallTimeout time.Duration // 单次调用超时（0=不限）
	isLinear       bool          // 退避策略：true=线性，false=指数

	// 限流模式
	mode limitMode // 当前限流模式

	// RateLimiter / SlidingWindow 共用参数
	rlRate   int           // RateLimiter 速率（每时间窗口操作次数）
	rlPer    time.Duration // RateLimiter 时间窗口
	rlBurst  int           // RateLimiter 突发容量
	rlShards int           // RateLimiter 水平分片数
	swLimit  int           // SlidingWindow 窗口内最大请求数
	swWindow time.Duration // SlidingWindow 时间窗口
	swShards int           // SlidingWindow 水平分片数

	// TokenBucket 参数
	tbRate     float64 // TokenBucket 每秒令牌生成速率
	tbCapacity float64 // TokenBucket 最大令牌容量

	// Adaptive 参数
	adMinRate int // Adaptive 最小并发度
	adMaxRate int // Adaptive 最大并发度
	adShards  int // Adaptive 水平分片数
}

// ── 构造函数 ──

// New 创建带返回值的重试链式构建器。
//
// 示例：
//
//	val, err := retry.New[string](ctx).
//	    Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second).
//	    Execute(myFunc)
func New[T any](ctx context.Context) *RetryChain[T] {
	return &RetryChain[T]{
		ctx:            ctx,
		maxRetries:     3,
		initialBackoff: 100 * time.Millisecond,
		maxBackoff:     30 * time.Second,
	}
}

// NewVoid 创建无返回值的重试链式构建器（fn 只返回 error）。
//
// 示例：
//
//	err := retry.NewVoid(ctx).
//	    Linear().MaxRetries(5).Backoff(200*time.Millisecond).
//	    ExecuteVoid(myFunc)
func NewVoid(ctx context.Context) *RetryChain[struct{}] {
	return New[struct{}](ctx)
}

// ── 策略配置 ──

// Exponential 设置指数退避策略（默认）。退避公式：min(initialBackoff * 2^attempt, maxBackoff)。
// 每次重试等待时间翻倍，直至达到 maxBackoff 上限。
//
// 示例：
//
//	retry.New[string](ctx).Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second)
func (c *RetryChain[T]) Exponential() *RetryChain[T] {
	c.isLinear = false
	return c
}

// Linear 设置线性退避策略，每次重试等待相同的 backoff 时间。
//
// 示例：
//
//	retry.NewVoid(ctx).Linear().MaxRetries(5).Backoff(1*time.Second)
func (c *RetryChain[T]) Linear() *RetryChain[T] {
	c.isLinear = true
	return c
}

// ── 退避参数 ──

// MaxRetries 设置最大重试次数（总执行次数 = MaxRetries + 1）。
//
// n: 最大重试次数，n < 0 时设为 0。
//
// 示例：
//
//	.MaxRetries(3)  // 最多 3 次重试 = 共 4 次尝试
func (c *RetryChain[T]) MaxRetries(n int) *RetryChain[T] {
	if n < 0 {
		n = 0
	}
	c.maxRetries = n
	return c
}

// Backoff 设置退避时间参数。
// initial 为首次重试等待时间，max 为退避上限（0 = 无上限）。
// 线性策略下 initial 是固定等待时间，max 被忽略。
//
// 示例：
//
//	// 指数：100ms → 200ms → 400ms → 800ms（上限 5s）
//	.Backoff(100*time.Millisecond, 5*time.Second)
//
//	// 线性：每次等 500ms
//	.Linear().Backoff(500*time.Millisecond)
func (c *RetryChain[T]) Backoff(initial, max time.Duration) *RetryChain[T] {
	c.initialBackoff = initial
	c.maxBackoff = max
	return c
}

// PerCallTimeout 设置每次 fn 调用的超时时间。超时 != 取消，超时后会继续重试。
// 0 表示不设置每次调用超时（默认）。
//
// 示例：
//
//	// 每次调用最多 2 秒，超时后自动重试
//	.PerCallTimeout(2 * time.Second)
func (c *RetryChain[T]) PerCallTimeout(d time.Duration) *RetryChain[T] {
	c.perCallTimeout = d
	return c
}

// DefaultConfig 恢复默认配置：指数退避，最多重试 3 次，退避 100ms~30s，无每次调用超时。
//
// 示例：
//
//	// 重置到默认再微调
//	retry.New[string](ctx).DefaultConfig().MaxRetries(10)
func (c *RetryChain[T]) DefaultConfig() *RetryChain[T] {
	c.isLinear = false
	c.maxRetries = 3
	c.initialBackoff = 100 * time.Millisecond
	c.maxBackoff = 30 * time.Second
	c.perCallTimeout = 0
	return c
}

// DefaultMaxRetries 使用默认最大重试次数（3）。
func (c *RetryChain[T]) DefaultMaxRetries() *RetryChain[T] {
	c.maxRetries = 3
	return c
}

// DefaultBackoff 使用默认退避参数：初始 100ms，最大 30s。
func (c *RetryChain[T]) DefaultBackoff() *RetryChain[T] {
	c.initialBackoff = 100 * time.Millisecond
	c.maxBackoff = 30 * time.Second
	return c
}

// DefaultPerCallTimeout 使用默认单次调用超时（不限时）。
func (c *RetryChain[T]) DefaultPerCallTimeout() *RetryChain[T] {
	c.perCallTimeout = 0
	return c
}

// DefaultRate 使用默认限流速率（10/s）。
func (c *RetryChain[T]) DefaultRate() *RetryChain[T] {
	c.rlRate = 0
	return c
}

// DefaultPer 使用默认限流时间窗口（1s）。
func (c *RetryChain[T]) DefaultPer() *RetryChain[T] {
	c.rlPer = 0
	return c
}

// DefaultBurst 使用默认突发容量（0=不开启）。
func (c *RetryChain[T]) DefaultBurst() *RetryChain[T] {
	c.rlBurst = 0
	return c
}

// DefaultCapacity 使用默认令牌桶容量（速率 × 2）。
func (c *RetryChain[T]) DefaultCapacity() *RetryChain[T] {
	c.tbCapacity = 0
	return c
}

// DefaultLimit 使用默认滑动窗口限制（100）。
func (c *RetryChain[T]) DefaultLimit() *RetryChain[T] {
	c.swLimit = 0
	return c
}

// DefaultWindow 使用默认滑动窗口大小（1s）。
func (c *RetryChain[T]) DefaultWindow() *RetryChain[T] {
	c.swWindow = 0
	return c
}

// DefaultMinWorker 使用默认最小并发度（1）。
func (c *RetryChain[T]) DefaultMinWorker() *RetryChain[T] {
	c.adMinRate = 0
	return c
}

// DefaultMaxWorker 使用默认最大并发度（最小并发度 × 10）。
func (c *RetryChain[T]) DefaultMaxWorker() *RetryChain[T] {
	c.adMaxRate = 0
	return c
}

// DefaultShards 使用默认分片数（不启用分片模式）。
func (c *RetryChain[T]) DefaultShards() *RetryChain[T] {
	c.rlShards = 0
	c.swShards = 0
	c.adShards = 0
	return c
}

// ──────────────────────── 模式一：RateLimiter ────────────────────────

// RateLimiter 启用 RateLimiter 限流模式。每次重试前阻塞等待令牌，被限流时阻塞。
// 配置方法：Rate / Per / Burst / Shards。
//
// 示例：
//
//	retry.New[string](ctx).Exponential().MaxRetries(5).Backoff(100*time.Millisecond, 5*time.Second).
//	    RateLimiter().Rate(10).Per(time.Second).Execute(fn)
func (c *RetryChain[T]) RateLimiter() *RetryChain[T] {
	c.mode = modeRateLimiter
	return c
}

// Rate 设置 RateLimiter 速率：每 PerDuration 内允许的操作次数（int 版，RateLimiter/SlidingWindow 共用）。
// TokenBucket 使用的是 float64 版 Rate（参数类型不同，Go 不支持方法重载，靠模式区分即可）。
//
// 示例：
//
//	.RateLimiter().Rate(100).Per(time.Second)  // 每秒 100 次
func (c *RetryChain[T]) Rate(n int) *RetryChain[T] {
	c.rlRate = n
	return c
}

// Per 设置 RateLimiter 时间窗口大小。
//
// 示例：
//
//	.RateLimiter().Rate(10).Per(time.Minute)  // 每分钟 10 次
func (c *RetryChain[T]) Per(d time.Duration) *RetryChain[T] {
	c.rlPer = d
	return c
}

// Burst 设置 RateLimiter 突发容量，允许短时间超过 Rate 速率的请求数。
//
// 示例：
//
//	.RateLimiter().Rate(50).Per(time.Second).Burst(200)
func (c *RetryChain[T]) Burst(n int) *RetryChain[T] {
	c.rlBurst = n
	return c
}

// ──────────────────────── 模式二：TokenBucket ────────────────────────

// TokenBucket 启用 TokenBucket 令牌桶限流模式。每次重试前检查令牌，拿不到时跳过等待退避重试。
// 配置方法：Rate（float64 版）/ Capacity。
//
// 示例：
//
//	retry.NewVoid(ctx).Exponential().MaxRetries(10).Backoff(100*time.Millisecond, 5*time.Second).
//	    TokenBucket().Rate(10).Capacity(20).ExecuteVoid(fn)
func (c *RetryChain[T]) TokenBucket() *RetryChain[T] {
	c.mode = modeTokenBucket
	return c
}

// Capacity 设置 TokenBucket 容量（最大积压令牌数）。
//
// 示例：
//
//	.TokenBucket().Rate(10).Capacity(20)  // 每秒 10 个令牌，最多积压 20 个
func (c *RetryChain[T]) Capacity(n float64) *RetryChain[T] {
	c.tbCapacity = n
	return c
}

// ──────────────────────── 模式三：SlidingWindow ────────────────────────

// SlidingWindow 启用滑动窗口限流模式。在指定时间窗口内精确限制请求数，比固定窗口更平滑。
// 每次重试前检查窗口计数，超过限制时跳过等待退避重试。
// 配置方法：Limit / Window / Shards。
//
// 示例：
//
//	retry.New[string](ctx).Linear().MaxRetries(5).Backoff(200*time.Millisecond).
//	    SlidingWindow().Limit(100).Window(10*time.Second).Shards(8).Execute(fn)
func (c *RetryChain[T]) SlidingWindow() *RetryChain[T] {
	c.mode = modeSlidingWindow
	return c
}

// Limit 设置滑动窗口在指定时间窗口内允许的最大请求数。
//
// 示例：
//
//	.SlidingWindow().Limit(100).Window(10*time.Second)  // 每 10 秒最多 100 次
func (c *RetryChain[T]) Limit(n int) *RetryChain[T] {
	c.swLimit = n
	return c
}

// Window 设置滑动窗口的时间窗口大小。
//
// 示例：
//
//	.SlidingWindow().Limit(50).Window(5*time.Second)  // 每 5 秒最多 50 次
func (c *RetryChain[T]) Window(d time.Duration) *RetryChain[T] {
	c.swWindow = d
	return c
}

// ──────────────────────── 模式四：Adaptive ────────────────────────

// Adaptive 启用自适应限流模式。根据成功率自动调整并发度（成功率高→扩并发，失败率高→缩并发）。
// 每次重试前 Acquire 并发槽位，执行完毕后 Release。
// 配置方法：MinWorker / MaxWorker / Shards。
//
// 示例：
//
//	err := retry.NewVoid(ctx).Exponential().MaxRetries(10).Backoff(50*time.Millisecond, 5*time.Second).
//	    Adaptive().MinWorker(5).MaxWorker(100).Shards(16).ExecuteVoid(fn)
func (c *RetryChain[T]) Adaptive() *RetryChain[T] {
	c.mode = modeAdaptive
	return c
}

// MinWorker 设置 AdaptiveRateLimiter 的最小并发度（负载低时不会低于此值）。
//
// 示例：
//
//	.Adaptive().MinWorker(5).MaxWorker(100)  // 并发度 5~100 自适应
func (c *RetryChain[T]) MinWorker(n int) *RetryChain[T] {
	c.adMinRate = n
	return c
}

// MaxWorker 设置 AdaptiveRateLimiter 的最大并发度（负载高时不会超过此值）。
//
// 示例：
//
//	.Adaptive().MinWorker(10).MaxWorker(200)
func (c *RetryChain[T]) MaxWorker(n int) *RetryChain[T] {
	c.adMaxRate = n
	return c
}

// ──────────────────────── 通用：分片配置 ────────────────────────

// Shards 设置限流器水平分片数，降低锁竞争以支持极限高并发（4 种模式通用）。
// n <= 0 时自动使用 DefaultShardCount()。
//
// 示例：
//
//	.RateLimiter().Rate(10000).Per(time.Second).Shards(16)
//	.SlidingWindow().Limit(500).Window(time.Second).Shards(8)
func (c *RetryChain[T]) Shards(n int) *RetryChain[T] {
	if n <= 0 {
		n = ratelimit.DefaultShardCount()
	}
	c.rlShards = n
	c.swShards = n
	c.adShards = n
	return c
}

// ── 终端方法 ──

// Execute 执行重试链，返回 (T, error)。
// 重试耗尽后返回的错误包含所有尝试的错误信息。
//
// 示例：
//
//	val, err := retry.New[string](ctx).
//	    RateLimiter().Rate(10).Per(time.Second).
//	    Execute(func(ctx context.Context) (string, error) {
//	        return httpGet(ctx, url)
//	    })
func (c *RetryChain[T]) Execute(fn func(context.Context) (T, error)) (T, error) {
	var (
		rl  *ratelimit.RateLimiter
		tb  *ratelimit.TokenBucket
		sw  *ratelimit.SlidingWindowRateLimiter
		al  *ratelimit.AdaptiveRateLimiter
		srl *ratelimit.ShardedRateLimiter
		stb *ratelimit.ShardedTokenBucket
		ssw *ratelimit.ShardedSlidingWindowRateLimiter
		sal *ratelimit.ShardedAdaptiveRateLimiter
	)

	switch c.mode {
	case modeRateLimiter:
		if c.rlShards > 1 {
			srl = c.buildShardedRateLimiter()
			defer srl.Close()
			return c.executeCore(fn, srl, nil, nil, nil, nil, nil, nil, nil)
		}
		rl = c.buildRateLimiter()
		defer rl.Close()
		return c.executeCore(fn, nil, nil, nil, nil, rl, nil, nil, nil)
	case modeTokenBucket:
		if c.rlShards > 1 {
			stb = ratelimit.NewShardedTokenBucket(c.rlShards, c.tbRateOrDefault(), c.tbCapacityOrDefault())
			return c.executeCore(fn, nil, nil, nil, nil, nil, stb, nil, nil)
		}
		tb = c.buildTokenBucket()
		return c.executeCore(fn, nil, tb, nil, nil, nil, nil, nil, nil)
	case modeSlidingWindow:
		if c.swShards > 1 {
			ssw = c.buildShardedSlidingWindow()
			return c.executeCore(fn, nil, nil, nil, nil, nil, nil, ssw, nil)
		}
		sw = c.buildSlidingWindow()
		return c.executeCore(fn, nil, nil, sw, nil, nil, nil, nil, nil)
	case modeAdaptive:
		if c.adShards > 1 {
			sal = c.buildShardedAdaptive()
			defer sal.Close()
			return c.executeCore(fn, nil, nil, nil, nil, nil, nil, nil, sal)
		}
		al = c.buildAdaptive()
		return c.executeCore(fn, nil, nil, nil, al, nil, nil, nil, nil)
	default:
		return c.executeCore(fn, nil, nil, nil, nil, nil, nil, nil, nil)
	}
}

// ExecuteVoid 无返回值版 Execute，fn 只返回 error。
//
// 示例：
//
//	err := retry.NewVoid(ctx).
//	    RateLimiter().Rate(10).Per(time.Second).
//	    ExecuteVoid(func(ctx context.Context) error {
//	        return sendMessage(ctx, msg)
//	    })
func (c *RetryChain[T]) ExecuteVoid(fn func(context.Context) error) error {
	_, err := c.Execute(func(ctx context.Context) (T, error) {
		var zero T
		return zero, fn(ctx)
	})
	return err
}

// Run 统一执行方法，fn 签名为 func() error（无 context 参数，链内已有 ctx）。
// 是 Execute / ExecuteVoid 的简化版，适合绝大多数重试场景。
//
// 示例：
//
//	err := retry.NewVoid(ctx).
//	    Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second).
//	    Run(func() error {
//	        return doSomething()
//	    })
//
//	err := retry.NewVoid(ctx).
//	    RateLimiter().Rate(10).Per(time.Second).
//	    Run(func() error {
//	        return callAPI()
//	    })
func (c *RetryChain[T]) Run(fn func() error) error {
	_, err := c.Execute(func(ctx context.Context) (T, error) {
		var zero T
		return zero, fn()
	})
	return err
}

// RunVoid 与 ExecuteVoid 相同，fn 签名为 func(context.Context) error，使用 Run* 统一命名。
//
// 示例：
//
//	err := retry.NewVoid(ctx).
//	    TokenBucket().Rate(5).Capacity(20).
//	    RunVoid(func(ctx context.Context) error {
//	        return callAPI(ctx, req)
//	    })
func (c *RetryChain[T]) RunVoid(fn func(context.Context) error) error {
	return c.ExecuteVoid(fn)
}

// ── 内部构建方法 ──

func (c *RetryChain[T]) rateOrDefault() int {
	if c.rlRate <= 0 {
		return 10
	}
	return c.rlRate
}

func (c *RetryChain[T]) perOrDefault() time.Duration {
	if c.rlPer <= 0 {
		return time.Second
	}
	return c.rlPer
}

func (c *RetryChain[T]) tbRateOrDefault() float64 {
	if c.tbRate <= 0 {
		return 10
	}
	return c.tbRate
}

func (c *RetryChain[T]) tbCapacityOrDefault() float64 {
	if c.tbCapacity <= 0 {
		return c.tbRateOrDefault() * 2
	}
	return c.tbCapacity
}

func (c *RetryChain[T]) slLimitOrDefault() int {
	if c.swLimit <= 0 {
		return 100
	}
	return c.swLimit
}

func (c *RetryChain[T]) slWindowOrDefault() time.Duration {
	if c.swWindow <= 0 {
		return time.Second
	}
	return c.swWindow
}

func (c *RetryChain[T]) adMinOrDefault() int {
	if c.adMinRate <= 0 {
		return 1
	}
	return c.adMinRate
}

func (c *RetryChain[T]) adMaxOrDefault() int {
	if c.adMaxRate <= 0 {
		return c.adMinOrDefault() * 10
	}
	return c.adMaxRate
}

func (c *RetryChain[T]) buildRateLimiter() *ratelimit.RateLimiter {
	if c.rlBurst > 0 {
		return ratelimit.NewRateLimiterWithBurst(c.rateOrDefault(), c.perOrDefault(), c.rlBurst)
	}
	return ratelimit.NewRateLimiter(c.rateOrDefault(), c.perOrDefault())
}

func (c *RetryChain[T]) buildShardedRateLimiter() *ratelimit.ShardedRateLimiter {
	if c.rlBurst > 0 {
		return ratelimit.NewShardedRateLimiterWithBurst(c.rlShards, c.rateOrDefault(), c.perOrDefault(), c.rlBurst)
	}
	return ratelimit.NewShardedRateLimiter(c.rlShards, c.rateOrDefault(), c.perOrDefault())
}

func (c *RetryChain[T]) buildTokenBucket() *ratelimit.TokenBucket {
	return ratelimit.NewTokenBucket(c.tbRateOrDefault(), c.tbCapacityOrDefault())
}

func (c *RetryChain[T]) buildSlidingWindow() *ratelimit.SlidingWindowRateLimiter {
	return ratelimit.NewSlidingWindowRateLimiter(c.slLimitOrDefault(), c.slWindowOrDefault())
}

func (c *RetryChain[T]) buildShardedSlidingWindow() *ratelimit.ShardedSlidingWindowRateLimiter {
	return ratelimit.NewShardedSlidingWindowRateLimiter(c.swShards, c.slLimitOrDefault(), c.slWindowOrDefault())
}

func (c *RetryChain[T]) buildAdaptive() *ratelimit.AdaptiveRateLimiter {
	return ratelimit.NewAdaptiveRateLimiter(c.adMinOrDefault(), c.adMaxOrDefault())
}

func (c *RetryChain[T]) buildShardedAdaptive() *ratelimit.ShardedAdaptiveRateLimiter {
	return ratelimit.NewShardedAdaptiveRateLimiter(c.adShards, c.adMinOrDefault(), c.adMaxOrDefault())
}

// ── 内部执行核心 ──

func (c *RetryChain[T]) executeCore(
	fn func(context.Context) (T, error),
	srl *ratelimit.ShardedRateLimiter,
	tb *ratelimit.TokenBucket,
	sw *ratelimit.SlidingWindowRateLimiter,
	al *ratelimit.AdaptiveRateLimiter,
	rl *ratelimit.RateLimiter,
	stb *ratelimit.ShardedTokenBucket,
	ssw *ratelimit.ShardedSlidingWindowRateLimiter,
	sal *ratelimit.ShardedAdaptiveRateLimiter,
) (T, error) {
	var zero T
	initialBackoff := c.initialBackoff
	maxBackoff := c.maxBackoff
	if c.isLinear {
		maxBackoff = initialBackoff
	}

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		// ── 获取限流许可 ──
		acquired, err := c.acquire(attempt, initialBackoff, maxBackoff, rl, srl, tb, sw, al, stb, ssw, sal)
		if err != nil {
			if attempt == c.maxRetries {
				return zero, fmt.Errorf("retry exhausted after %d attempts: %w", c.maxRetries+1, err)
			}
			continue
		}
		if !acquired {
			continue
		}

		// ── 执行 fn ──
		val, err := c.invokeSafely(fn)
		// Adaptive 记录结果
		if al != nil {
			if err == nil {
				al.RecordSuccess()
			} else {
				al.RecordFailure()
			}
		}
		if sal != nil {
			if err == nil {
				sal.RecordSuccess()
			} else {
				sal.RecordFailure()
			}
		}
		// 释放 RateLimiter/Adaptive/Sharded* 占用的槽位
		if rl != nil {
			rl.Release()
		}
		if srl != nil {
			srl.Release()
		}
		if al != nil {
			al.Release()
		}
		if sal != nil {
			sal.Release()
		}

		if err == nil {
			return val, nil
		}
		if attempt == c.maxRetries {
			return zero, fmt.Errorf("retry exhausted after %d attempts: %w", c.maxRetries+1, err)
		}
		backoff := computeBackoff(attempt, initialBackoff, maxBackoff)
		if backoff > 0 {
			select {
			case <-c.ctx.Done():
				return zero, c.ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	panic("unreachable")
}

// acquire 尝试获取限流许可，返回 (是否获取成功, 错误)。
// 对于 TokenBucket/SlidingWindow，未获取到返回 acquired=false 并自动等待退避。
// 对于 RateLimiter/Adaptive，错误表示 ctx 取消。
func (c *RetryChain[T]) acquire(
	attempt int,
	initialBackoff, maxBackoff time.Duration,
	rl *ratelimit.RateLimiter,
	srl *ratelimit.ShardedRateLimiter,
	tb *ratelimit.TokenBucket,
	sw *ratelimit.SlidingWindowRateLimiter,
	al *ratelimit.AdaptiveRateLimiter,
	stb *ratelimit.ShardedTokenBucket,
	ssw *ratelimit.ShardedSlidingWindowRateLimiter,
	sal *ratelimit.ShardedAdaptiveRateLimiter,
) (acquired bool, err error) {
	switch {
	case rl != nil:
		if err := rl.Wait(c.ctx); err != nil {
			return false, err
		}
		return true, nil
	case srl != nil:
		if err := srl.Wait(c.ctx); err != nil {
			return false, err
		}
		return true, nil
	case al != nil:
		if err := al.Acquire(c.ctx); err != nil {
			return false, err
		}
		return true, nil
	case sal != nil:
		if err := sal.Acquire(c.ctx); err != nil {
			return false, err
		}
		return true, nil
	case tb != nil:
		if !tb.Allow() {
			return false, nil
		}
		return true, nil
	case stb != nil:
		if !stb.Allow() {
			return false, nil
		}
		return true, nil
	case sw != nil:
		if !sw.Allow() {
			return false, nil
		}
		return true, nil
	case ssw != nil:
		if !ssw.Allow() {
			return false, nil
		}
		return true, nil
	default:
		return true, nil
	}
}

func (c *RetryChain[T]) invokeSafely(fn func(context.Context) (T, error)) (val T, err error) {
	if c.perCallTimeout > 0 {
		ctx, cancel := context.WithTimeout(c.ctx, c.perCallTimeout)
		defer cancel()
		return invokeSafely(ctx, fn)
	}
	return invokeSafely(c.ctx, fn)
}
