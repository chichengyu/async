// Package ratelimit 提供速率限制器。
//
// 包含以下限流算法：
//   - RateLimiter：基于定时补充的令牌桶，支持 Block/Reject/BlockForce 三种策略
//   - SlidingWindowRateLimiter：滑动窗口限流，精确控制时间窗口内的请求数
//   - TokenBucket：经典令牌桶，按速率持续补充令牌
//   - AdaptiveRateLimiter：自适应限流，根据成功率自动调整并发度
//
// 使用示例：
//
//	// RateLimiter：每秒100个请求
//	rl := ratelimit.NewRateLimiter(100, time.Second)
//	defer rl.Close()
//	rl.Wait(ctx)
//	doRequest()
//	rl.Release()
//
//	// TokenBucket：自带令牌自动补充
//	tb := ratelimit.NewTokenBucket(10, 20)
//	if tb.Allow() {
//	    doRequest()
//	}
//
//	// AdaptiveRateLimiter：自适应调整并发
//	al := ratelimit.NewAdaptiveRateLimiter(5, 100)
//	al.Acquire(ctx)
//	if err := doRequest(); err == nil {
//	    al.RecordSuccess()
//	} else {
//	    al.RecordFailure()
//	}
//	al.Release()
package ratelimit

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Strategy 定义令牌耗尽时的行为策略。
type Strategy int

const (
	Block      Strategy = iota // 阻塞等待令牌（默认）
	Reject                     // 立即返回错误
	BlockForce                 // 强制阻塞，忽略 ctx 取消
)

// RateLimiter controls the concurrency rate.
type RateLimiter struct {
	tokens   chan struct{} // 令牌 channel（缓冲大小=并发度）
	size     int32         // 并发度（atomic 原子操作，动态调整后同步）
	strat    atomic.Value  // 令牌耗尽时的处理策略（Strategy）
	closed   atomic.Bool   // 是否已关闭
	mu       sync.Mutex    // 保护以下字段的互斥锁
	resizeMu sync.RWMutex  // 保护 Resize 期间禁止并发的 Acquire/Release
	ctx      context.Context

	rate        atomic.Int32  // 每 perDuration 补充的令牌数（atomic，支持 Resize 动态更新）
	perDuration time.Duration // 令牌补充周期
	refillStop  chan struct{} // 停止令牌补充 goroutine 的信号
	refillDone  chan struct{} // 令牌补充 goroutine 已退出的信号

	cond *sync.Cond // BlockForce 策略下用于高效阻塞等待令牌，替代 busy-wait
}

// NewRateLimiter 创建定时补充令牌的限流器。
//
// 参数：
//   - rate：每 perDuration 内允许的操作次数，rate <= 0 时使用 core.IO()
//   - perDuration：时间窗口大小
//
// 使用示例：
//
//	// 每秒最多 100 个请求
//	rl := ratelimit.NewRateLimiter(100, time.Second)
//	rl.Wait(ctx)
//	doRequest()
//	rl.Release()
func NewRateLimiter(rate int, perDuration time.Duration) *RateLimiter {
	if rate <= 0 {
		rate = core.IO()
	}
	rl := &RateLimiter{
		tokens:      make(chan struct{}, rate),
		size:        int32(rate),
		ctx:         context.Background(),
		perDuration: perDuration,
		refillStop:  make(chan struct{}),
		refillDone:  make(chan struct{}),
	}
	rl.rate.Store(int32(rate))
	rl.cond = sync.NewCond(&rl.mu)
	rl.strat.Store(Block)
	for i := 0; i < rate; i++ {
		rl.tokens <- struct{}{}
	}
	rl.startRefill()
	return rl
}

// NewRateLimiterWithBurst 创建支持突发容量的限流器。
//
// 参数：
//   - rate：每 perDuration 内允许的操作次数
//   - perDuration：时间窗口大小
//   - burst：突发容量，允许短时间内超过 rate 速率的请求数
//
// 使用示例：
//
//	// 每秒 50 个，初始可突发 200 个
//	rl := ratelimit.NewRateLimiterWithBurst(50, time.Second, 200)
func NewRateLimiterWithBurst(rate int, perDuration time.Duration, burst int) *RateLimiter {
	if rate <= 0 {
		rate = core.IO()
	}
	cap := burst
	if cap < rate {
		cap = rate
	}
	rl := &RateLimiter{
		tokens:      make(chan struct{}, cap),
		size:        int32(rate),
		ctx:         context.Background(),
		perDuration: perDuration,
		refillStop:  make(chan struct{}),
		refillDone:  make(chan struct{}),
	}
	rl.rate.Store(int32(rate))
	rl.cond = sync.NewCond(&rl.mu)
	rl.strat.Store(Block)
	for i := 0; i < burst; i++ {
		rl.tokens <- struct{}{}
	}
	rl.startRefill()
	return rl
}

func newRateLimiterSimple(rate int) *RateLimiter {
	if rate <= 0 {
		rate = core.IO()
	}
	rl := &RateLimiter{
		tokens: make(chan struct{}, rate),
		size:   int32(rate),
		ctx:    context.Background(),
	}
	rl.cond = sync.NewCond(&rl.mu)
	rl.strat.Store(Block)
	for i := 0; i < rate; i++ {
		rl.tokens <- struct{}{}
	}
	return rl
}

// batchSizeForRate 根据给定速率计算每次批量补充的令牌数量。
// tickInterval 由调用方根据速率区间决定，本方法只负责计算该速率下
// 每次补充操作应填充的令牌数。
func (rl *RateLimiter) batchSizeForRate(rate int) int {
	interval := rl.perDuration / time.Duration(rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	const minBatchInterval = 100 * time.Millisecond
	const maxBatchInterval = time.Second
	if interval < minBatchInterval {
		bs := int(minBatchInterval / interval)
		if bs < 1 {
			return 1
		}
		return bs
	}
	if interval > maxBatchInterval {
		bs := int(float64(rate) * maxBatchInterval.Seconds() / rl.perDuration.Seconds())
		if bs < 1 {
			return 1
		}
		return bs
	}
	return 1
}

func (rl *RateLimiter) startRefill() {
	rate := int(rl.rate.Load())
	if rate <= 0 {
		rate = 1
	}
	interval := rl.perDuration / time.Duration(rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	const minBatchInterval = 100 * time.Millisecond
	const maxBatchInterval = time.Second

	var tickInterval time.Duration
	if interval < minBatchInterval {
		tickInterval = minBatchInterval
	} else if interval > maxBatchInterval {
		tickInterval = maxBatchInterval
	} else {
		tickInterval = interval
	}

	ticker := time.NewTicker(tickInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-rl.refillStop:
				close(rl.refillDone)
				return
			case <-ticker.C:
				// 动态读取最新 rate 以支持 Resize 后速率实时生效
				r := int(rl.rate.Load())
				if r <= 0 {
					r = 1
				}
				curBatch := rl.batchSizeForRate(r)
				rl.resizeMu.RLock()
				added := 0
				for i := 0; i < curBatch; i++ {
					select {
					case rl.tokens <- struct{}{}:
						added++
					default:
						break
					}
				}
				rl.resizeMu.RUnlock()
				if added > 0 {
					rl.mu.Lock()
					for i := 0; i < added; i++ {
						rl.cond.Signal()
					}
					rl.mu.Unlock()
				}
			}
		}
	}()
}

// Wait 获取令牌（等同 Acquire），阻塞直到获取成功或 ctx 取消。
//
// 参数：
//   - ctx：上下文，取消后返回 ctx.Err()
//
// 使用示例：
//
//	rl.Wait(ctx)  // 等待令牌
//	doRequest()
//	rl.Release()  // 归还令牌
func (rl *RateLimiter) Wait(ctx context.Context) error {
	return rl.Acquire(ctx)
}

// Stop 等同 Close，停止限流器。
func (rl *RateLimiter) Stop() {
	rl.Close()
}

// WithStrategy 设置限流策略：Block（阻塞等待）、Reject（立即拒绝）、BlockForce（强制阻塞）。
//
// 参数：
//   - s：策略常量（Block/Reject/BlockForce）
//
// 使用示例：
//
//	// 令牌不够时立即拒绝而非阻塞
//	rl.WithStrategy(ratelimit.Reject)
func (rl *RateLimiter) WithStrategy(s Strategy) *RateLimiter {
	rl.strat.Store(s)
	return rl
}

// WithTraceID 绑定 TraceID 用于日志追踪。
//
// 参数：
//   - ctx：上下文，若不含 trace_id 则自动生成
//
// 使用示例：
//
//	rl, ctx := ratelimit.NewRateLimiter(100, time.Second).WithTraceID(ctx)
func (rl *RateLimiter) WithTraceID(ctx context.Context) (*RateLimiter, context.Context) {
	ctx = core.EnsureTraceID(ctx)
	rl.ctx = ctx
	return rl, ctx
}

// Acquire 根据当前策略获取令牌。默认 Block 策略下阻塞等待令牌或 ctx 取消。
// 当限流器通过 Resize 动态调整速率导致内部通道重建时，Acquire 会自动重试获取新令牌。
//
// 参数：
//   - ctx：上下文，取消后返回 ctx.Err()（Block 策略下）
//
// 使用示例：
//
//	if err := rl.Acquire(ctx); err != nil {
//	    return err // ctx 取消或限流器已停止
//	}
//	defer rl.Release()
//	doRequest()
func (rl *RateLimiter) Acquire(ctx context.Context) error {
	if rl.closed.Load() {
		return core.ErrRateLimiterStopped
	}
	strat, _ := rl.strat.Load().(Strategy)
	switch strat {
	case Reject:
		rl.resizeMu.RLock()
		defer rl.resizeMu.RUnlock()
		select {
		case _, ok := <-rl.tokens:
			if !ok {
				return rl.Acquire(ctx)
			}
			return nil
		default:
			return core.ErrRateLimitExceeded
		}
	case BlockForce:
		rl.mu.Lock()
		defer rl.mu.Unlock()
		for {
			select {
			case _, ok := <-rl.tokens:
				if ok {
					return nil
				}
				continue
			default:
			}
			if rl.closed.Load() {
				return core.ErrRateLimiterStopped
			}
			rl.cond.Wait()
		}
	default:
		rl.mu.Lock()
		defer rl.mu.Unlock()
		stop := context.AfterFunc(ctx, func() { rl.cond.Broadcast() })
		defer stop()
		for {
			select {
			case _, ok := <-rl.tokens:
				if ok {
					return nil
				}
				continue
			default:
			}
			if rl.closed.Load() {
				return core.ErrRateLimiterStopped
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			rl.cond.Wait()
		}
	}
}

// Release 归还令牌到限流器。通常在 defer 中调用。
// 关闭的限流器上调用 Release 会安全忽略（不会 panic）。
//
// 使用示例：
//
//	rl.Acquire(ctx)
//	defer rl.Release()
//	doRequest()
func (rl *RateLimiter) Release() {
	rl.resizeMu.RLock()

	if rl.closed.Load() {
		rl.resizeMu.RUnlock()
		return
	}

	sent := false
	select {
	case rl.tokens <- struct{}{}:
		sent = true
	default:
	}
	rl.resizeMu.RUnlock()

	if sent {
		rl.mu.Lock()
		rl.cond.Signal()
		rl.mu.Unlock()
	}
}

// Close 停止限流器的令牌补充 goroutine 并关闭令牌通道。多次调用安全。
//
// 使用示例：
//
//	rl := ratelimit.NewRateLimiter(100, time.Second)
//	defer rl.Close()
func (rl *RateLimiter) Close() {
	rl.mu.Lock()
	if !rl.closed.CompareAndSwap(false, true) {
		rl.mu.Unlock()
		return
	}
	if rl.refillStop != nil {
		close(rl.refillStop)
	}
	rl.cond.Broadcast()
	rl.mu.Unlock()

	if rl.refillDone != nil {
		<-rl.refillDone
	}

	rl.resizeMu.Lock()
	close(rl.tokens)
	rl.resizeMu.Unlock()
}

// Resize 动态调整限流速率。newRate <= 0 或等于当前值时忽略。
// 已关闭的限流器上调用无效。
//
// 参数：
//   - newRate：新的每秒允许操作次数
//
// 使用示例：
//
//	// 高峰期扩大速率
//	rl.Resize(200)
func (rl *RateLimiter) Resize(newRate int) {
	if newRate <= 0 {
		return
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.closed.Load() {
		return
	}

	current := int(rl.size)
	if newRate == current {
		return
	}

	rl.resizeMu.Lock()
	defer rl.resizeMu.Unlock()

	oldTokens := rl.tokens
	newTokens := make(chan struct{}, newRate)
	for i := 0; i < cap(oldTokens); i++ {
		select {
		case <-oldTokens:
		default:
		}
	}

	rl.tokens = newTokens
	rl.size = int32(newRate)
	rl.rate.Store(int32(newRate))
	for i := 0; i < newRate; i++ {
		newTokens <- struct{}{}
	}
	close(oldTokens)
	rl.cond.Broadcast()
}

// Size 返回当前速率限制值（每秒允许的操作数）。
func (rl *RateLimiter) Size() int {
	return int(atomic.LoadInt32(&rl.size))
}

// Available 返回当前可用令牌数。
func (rl *RateLimiter) Available() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return len(rl.tokens)
}

// Token 包装 RateLimiter，支持通过 defer t.Release() 自动归还令牌。
//
// 使用示例：
//
//	t, err := rl.Token(ctx)
//	if err != nil {
//	    return err
//	}
//	defer t.Release()
//	doRequest()
type Token struct {
	limiter *RateLimiter // 关联的限流器（Release 时归还令牌）
}

// Release 归还令牌。
func (t *Token) Release() {
	if t.limiter != nil {
		t.limiter.Release()
		t.limiter = nil
	}
}

// Token 获取一个自动释放的令牌包装器，配合 defer 使用更安全。
//
// 参数：
//   - ctx：上下文，取消后返回 ctx.Err()
//
// 使用示例：
//
//	t, err := rl.Token(ctx)
//	if err != nil {
//	    return err
//	}
//	defer t.Release()
func (rl *RateLimiter) Token(ctx context.Context) (*Token, error) {
	if err := rl.Acquire(ctx); err != nil {
		return nil, err
	}
	return &Token{limiter: rl}, nil
}

// ──────────────────────────── SlidingWindowRateLimiter ────────────────────────────
//
// 滑动窗口限流器在指定时间窗口内精确限制请求数，比固定窗口更平滑。
//
// 使用示例：
//
//	sw := ratelimit.NewSlidingWindowRateLimiter(100, 10*time.Second)
//	if sw.Allow() {
//	    doRequest()
//	}

type SlidingWindowRateLimiter struct {
	limit      int           // 时间窗口内允许的最大请求数
	window     time.Duration // 时间窗口大小
	mu         sync.Mutex    // 保护 timestamps 的互斥锁
	timestamps []time.Time   // 请求时间戳记录（滑动窗口）
}

// NewSlidingWindowRateLimiter 创建滑动窗口限流器。
//
// 参数：
//   - limit：时间窗口内允许的最大请求数
//   - window：时间窗口大小
//
// 使用示例：
//
//	// 每 10 秒最多 100 次
//	sw := ratelimit.NewSlidingWindowRateLimiter(100, 10*time.Second)
func NewSlidingWindowRateLimiter(limit int, window time.Duration) *SlidingWindowRateLimiter {
	return &SlidingWindowRateLimiter{
		limit:  limit,
		window: window,
	}
}

// Allow 检查是否允许 1 次请求通过。线程安全。
func (sw *SlidingWindowRateLimiter) Allow() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-sw.window)
	cutoffIdx := sort.Search(len(sw.timestamps), func(i int) bool {
		return sw.timestamps[i].After(cutoff)
	})
	sw.timestamps = sw.timestamps[cutoffIdx:]
	if len(sw.timestamps) < sw.limit {
		sw.timestamps = append(sw.timestamps, now)
		return true
	}
	return false
}

// AllowN 检查是否允许 n 次请求同时通过。
//
// 参数：
//   - n：请求次数（n <= 0 时始终返回 true）
func (sw *SlidingWindowRateLimiter) AllowN(n int) bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-sw.window)
	cutoffIdx := sort.Search(len(sw.timestamps), func(i int) bool {
		return sw.timestamps[i].After(cutoff)
	})
	sw.timestamps = sw.timestamps[cutoffIdx:]
	if len(sw.timestamps)+n <= sw.limit {
		for j := 0; j < n; j++ {
			sw.timestamps = append(sw.timestamps, now)
		}
		return true
	}
	return false
}

// ──────────────────────────── TokenBucket ────────────────────────────
//
// TokenBucket 是经典令牌桶算法实现。令牌按 rate 速率持续补充，最多存储 capacity 个。
// 适合需要平滑流量整形、允许一定突发流量的场景。
//
// 使用示例：
//
//	tb := ratelimit.NewTokenBucket(10, 20) // 每秒10个令牌，最多存20个
//	if tb.Allow() {
//	    doRequest()
//	}

type TokenBucket struct {
	rate       float64    // 每秒生成的令牌数
	capacity   float64    // 最大令牌容量（允许的突发流量大小）
	tokens     float64    // 当前可用令牌数
	lastRefill time.Time  // 上次补充令牌的时间
	mu         sync.Mutex // 保护 tokens/lastRefill 的互斥锁
}

// NewTokenBucket 创建令牌桶。
//
// 参数：
//   - rate：每秒生成的令牌数
//   - capacity：最大令牌容量（决定允许的突发流量大小）
//
// 使用示例：
//
//	// 每秒 10 个令牌，最多存储 20 个（允许 2 秒的突发流量）
//	tb := ratelimit.NewTokenBucket(10, 20)
func NewTokenBucket(rate float64, capacity float64) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity,
		lastRefill: time.Now(),
	}
}

// Allow 尝试消耗 1 个令牌，成功返回 true。线程安全，无阻塞。
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

// AllowN 尝试消耗 n 个令牌，成功返回 true。
//
// 参数：
//   - n：需要消耗的令牌数（n <= 0 时始终返回 true）
func (tb *TokenBucket) AllowN(n float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = math.Min(tb.capacity, tb.tokens+elapsed*tb.rate)
	tb.lastRefill = now

	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}
	return false
}

// ──────────────────────────── AdaptiveRateLimiter ────────────────────────────
//
// AdaptiveRateLimiter 根据成功率自动调整并发度：成功率高时扩大并发，失败率高时缩容。
// 适合下游服务质量波动的场景，如调用第三方 API。
//
// 使用示例：
//
//	al := ratelimit.NewAdaptiveRateLimiter(5, 100) // 并发度范围 5-100
//	al.Acquire(ctx)
//	if err := callAPI(ctx); err == nil {
//	    al.RecordSuccess()
//	} else {
//	    al.RecordFailure()
//	}
//	al.Release()

type AdaptiveRateLimiter struct {
	base       *RateLimiter // 底层 RateLimiter 实例
	minRate    int          // 最小并发度（负载低时不会低于此值）
	maxRate    int          // 最大并发度（负载高时不会超过此值）
	current    int32        // 当前有效并发度（atomic 原子操作）
	success    int64        // 成功计数（用于自适应调整）
	failure    int64        // 失败计数（用于自适应调整）
	adjustUp   float64      // 上调阈值（成功率超此值则增加并发）
	adjustDown float64      // 下调阈值（失败率超此值则减少并发）
	mu         sync.RWMutex // 保护 Resize 与并发 Acquire 的读写锁
}

// NewAdaptiveRateLimiter 创建自适应限流器。
// 初始并发度 = (minRate + maxRate) / 2。
//
// 参数：
//   - minRate：最小并发度（负载低时也不会低于此值）
//   - maxRate：最大并发度（负载高时也不会超过此值）
//
// 使用示例：
//
//	// 并发度在 5-100 之间自适应调整
//	al := ratelimit.NewAdaptiveRateLimiter(5, 100)
func NewAdaptiveRateLimiter(minRate, maxRate int) *AdaptiveRateLimiter {
	if minRate <= 0 {
		minRate = 1
	}
	if maxRate <= minRate {
		maxRate = minRate
	}
	init := (minRate + maxRate) / 2
	a := &AdaptiveRateLimiter{
		base:       newRateLimiterSimple(init),
		minRate:    minRate,
		maxRate:    maxRate,
		current:    int32(init),
		adjustUp:   0.2,
		adjustDown: 0.5,
	}
	return a
}

// Acquire 获取一个并发槽位。阻塞直到 ctx 取消或获取成功。
//
// 参数：
//   - ctx：上下文，取消后返回 ctx.Err()
func (a *AdaptiveRateLimiter) Acquire(ctx context.Context) error {
	a.mu.RLock()
	base := a.base
	a.mu.RUnlock()
	return base.Acquire(ctx)
}

// Release 释放一个并发槽位。
func (a *AdaptiveRateLimiter) Release() {
	a.mu.RLock()
	base := a.base
	a.mu.RUnlock()
	base.Release()
}

// RecordSuccess 记录一次成功调用，用于自适应调整算法。
func (a *AdaptiveRateLimiter) RecordSuccess() {
	atomic.AddInt64(&a.success, 1)
	a.maybeAdjust()
}

// RecordFailure 记录一次失败调用，用于自适应调整算法。
func (a *AdaptiveRateLimiter) RecordFailure() {
	atomic.AddInt64(&a.failure, 1)
	a.maybeAdjust()
}

func (a *AdaptiveRateLimiter) maybeAdjust() {
	s := atomic.LoadInt64(&a.success)
	f := atomic.LoadInt64(&a.failure)
	total := s + f
	if total < 10 {
		return
	}

	failRate := float64(f) / float64(total)
	current := int(atomic.LoadInt32(&a.current))

	a.mu.Lock()
	defer a.mu.Unlock()

	if failRate > a.adjustUp {
		newRate := int(float64(current) * (1 - a.adjustDown))
		if newRate < a.minRate {
			newRate = a.minRate
		}
		a.base.Resize(newRate)
		atomic.StoreInt32(&a.current, int32(newRate))
		atomic.StoreInt64(&a.success, 0)
		atomic.StoreInt64(&a.failure, 0)
	} else if failRate < a.adjustUp/2 && current < a.maxRate {
		newRate := current + int(float64(a.maxRate-current)*a.adjustUp)
		if newRate > a.maxRate {
			newRate = a.maxRate
		}
		a.base.Resize(newRate)
		atomic.StoreInt32(&a.current, int32(newRate))
		atomic.StoreInt64(&a.success, 0)
		atomic.StoreInt64(&a.failure, 0)
	}
}

// ──────────────────────────── ShardedRateLimiter ────────────────────────────

// ShardedRateLimiter 将限流器水平分片为 N 个独立实例，支持极限高并发场景。
// 每个分片独立的 goroutine 补充令牌，将锁竞争降低到 1/N。
//
// 使用示例：
//
//	// 每秒 10000 请求，分 16 片，每片 ≈625/s
//	rl := ratelimit.NewShardedRateLimiter(16, 10000, time.Second)
//	defer rl.Close()
//	rl.Wait(ctx)
//	doRequest()
//	rl.Release()
type ShardedRateLimiter struct {
	limiters []*RateLimiter
	nextIdx  atomic.Uint64
	total    int32
}

// NewShardedRateLimiter 创建分片限流器。
// shards<=0 时默认使用 runtime.GOMAXPROCS(0)，最少 2 片。
// 总速率 = rate，每片速率 = rate/shards。
//
// 参数：
//   - shards：分片数
//   - rate：总允许速率（每 perDuration）
//   - perDuration：时间窗口
func NewShardedRateLimiter(shards int, rate int, perDuration time.Duration) *ShardedRateLimiter {
	if shards <= 0 {
		shards = core.IO()
		if shards < 2 {
			shards = 2
		}
	}
	perShard := rate / shards
	if perShard < 1 {
		perShard = 1
	}
	limiters := make([]*RateLimiter, shards)
	for i := 0; i < shards; i++ {
		limiters[i] = NewRateLimiter(perShard, perDuration)
	}
	return &ShardedRateLimiter{
		limiters: limiters,
		total:    int32(shards * perShard),
	}
}

// NewShardedRateLimiterWithBurst 创建支持突发容量的分片限流器。
// burst 会被均匀分配到各分片。
func NewShardedRateLimiterWithBurst(shards int, rate int, perDuration time.Duration, burst int) *ShardedRateLimiter {
	if shards <= 0 {
		shards = core.IO()
		if shards < 2 {
			shards = 2
		}
	}
	perShardRate := rate / shards
	if perShardRate < 1 {
		perShardRate = 1
	}
	perShardBurst := burst / shards
	if perShardBurst < perShardRate {
		perShardBurst = perShardRate
	}
	limiters := make([]*RateLimiter, shards)
	for i := 0; i < shards; i++ {
		limiters[i] = NewRateLimiterWithBurst(perShardRate, perDuration, perShardBurst)
	}
	return &ShardedRateLimiter{
		limiters: limiters,
		total:    int32(shards * perShardRate),
	}
}

// ShardCount 返回分片数。
func (sl *ShardedRateLimiter) ShardCount() int {
	return len(sl.limiters)
}

// TotalRate 返回所有分片的总速率（可能略小于创建时的 rate，由整数除法导致）。
func (sl *ShardedRateLimiter) TotalRate() int {
	return int(atomic.LoadInt32(&sl.total))
}

// GetShard 获取指定分片的 RateLimiter 实例。
// idx 越界返回 nil。
func (sl *ShardedRateLimiter) GetShard(idx int) *RateLimiter {
	if idx < 0 || idx >= len(sl.limiters) {
		return nil
	}
	return sl.limiters[idx]
}

// Wait 获取令牌，round-robin 分发到某个分片。
func (sl *ShardedRateLimiter) Wait(ctx context.Context) error {
	return sl.Acquire(ctx)
}

// Acquire 获取令牌，round-robin 分发。
func (sl *ShardedRateLimiter) Acquire(ctx context.Context) error {
	idx := int(sl.nextIdx.Add(1)-1) % len(sl.limiters)
	return sl.limiters[idx].Acquire(ctx)
}

// Release 归还令牌，round-robin 分发。
func (sl *ShardedRateLimiter) Release() {
	idx := int(sl.nextIdx.Add(1)-1) % len(sl.limiters)
	sl.limiters[idx].Release()
}

// Close 关闭所有分片的限流器。
func (sl *ShardedRateLimiter) Close() {
	for _, l := range sl.limiters {
		l.Close()
	}
}

// Size 返回所有分片并发度之和。
func (sl *ShardedRateLimiter) Size() int {
	var total int
	for _, l := range sl.limiters {
		total += l.Size()
	}
	return total
}

// Available 返回所有分片当前可用令牌总数。
func (sl *ShardedRateLimiter) Available() int {
	var total int
	for _, l := range sl.limiters {
		total += l.Available()
	}
	return total
}

// WithStrategy 为所有分片设置策略。
func (sl *ShardedRateLimiter) WithStrategy(s Strategy) *ShardedRateLimiter {
	for _, l := range sl.limiters {
		l.WithStrategy(s)
	}
	return sl
}

// Resize 调整总速率，平均分配到各分片。
// 注意：应在无活跃 Acquire/Release 时调用，否则可能短暂超发或欠发。
func (sl *ShardedRateLimiter) Resize(newTotalRate int) {
	shards := len(sl.limiters)
	perShard := newTotalRate / shards
	if perShard < 1 {
		perShard = 1
	}
	for _, l := range sl.limiters {
		l.Resize(perShard)
	}
	atomic.StoreInt32(&sl.total, int32(shards*perShard))
}

// Token 获取一个带 Release 的令牌句柄，round-robin 分发。
func (sl *ShardedRateLimiter) Token(ctx context.Context) (*Token, error) {
	idx := int(sl.nextIdx.Add(1)-1) % len(sl.limiters)
	return sl.limiters[idx].Token(ctx)
}

// ──────────────────────────── ShardedTokenBucket ────────────────────────────

// ShardedTokenBucket 将令牌桶水平分片，支持极限高并发下的 Allow() 调用。
// 各分片独立补充令牌，总速率 = rate * shards。
//
// 使用示例：
//
//	// 16 片，每片 100/s，总速率 ≈1600/s
//	tb := ratelimit.NewShardedTokenBucket(16, 100, 200)
//	if tb.Allow() {
//	    doRequest()
//	}
type ShardedTokenBucket struct {
	buckets []*TokenBucket
	nextIdx atomic.Uint64
}

// NewShardedTokenBucket 创建分片令牌桶。
// shards<=0 时默认 runtime.GOMAXPROCS(0)，最少 2 片。
func NewShardedTokenBucket(shards int, rate float64, capacity float64) *ShardedTokenBucket {
	if shards <= 0 {
		shards = core.IO()
		if shards < 2 {
			shards = 2
		}
	}
	buckets := make([]*TokenBucket, shards)
	for i := 0; i < shards; i++ {
		buckets[i] = NewTokenBucket(rate, capacity)
	}
	return &ShardedTokenBucket{buckets: buckets}
}

// Allow 获取一个令牌，round-robin 分发。
func (st *ShardedTokenBucket) Allow() bool {
	idx := int(st.nextIdx.Add(1)-1) % len(st.buckets)
	return st.buckets[idx].Allow()
}

// AllowN 获取 n 个令牌，round-robin 分发。
func (st *ShardedTokenBucket) AllowN(n float64) bool {
	idx := int(st.nextIdx.Add(1)-1) % len(st.buckets)
	return st.buckets[idx].AllowN(n)
}

// ShardCount 返回分片数。
func (st *ShardedTokenBucket) ShardCount() int {
	return len(st.buckets)
}

// GetShard 获取指定分片的 TokenBucket 实例。
func (st *ShardedTokenBucket) GetShard(idx int) *TokenBucket {
	if idx < 0 || idx >= len(st.buckets) {
		return nil
	}
	return st.buckets[idx]
}

// ──────────────────────────── ShardedSlidingWindowRateLimiter ────────────────────────────

// ShardedSlidingWindowRateLimiter 将滑动窗口限流器水平分片，支持极限高并发下的 Allow()。
// 每个分片独立维护时间窗口，limit 平均分配到各分片。
//
// 注意：由于分片间独立，瞬时总允许量可能略高于总 limit（各分片窗口边界不对齐）。
// 适用场景：对精度要求不高但需极高吞吐的限流。
//
// 使用示例：
//
//	// 16 片，总限流 10000/秒
//	sw := ratelimit.NewShardedSlidingWindowRateLimiter(16, 10000, time.Second)
//	if sw.Allow() {
//	    doRequest()
//	}
type ShardedSlidingWindowRateLimiter struct {
	limiters []*SlidingWindowRateLimiter
	nextIdx  atomic.Uint64
	total    int
}

// NewShardedSlidingWindowRateLimiter 创建分片滑动窗口限流器。
// shards<=0 时默认 runtime.GOMAXPROCS(0)，最少 2 片。
func NewShardedSlidingWindowRateLimiter(shards int, limit int, window time.Duration) *ShardedSlidingWindowRateLimiter {
	if shards <= 0 {
		shards = core.IO()
		if shards < 2 {
			shards = 2
		}
	}
	perShard := limit / shards
	if perShard < 1 {
		perShard = 1
	}
	limiters := make([]*SlidingWindowRateLimiter, shards)
	for i := 0; i < shards; i++ {
		limiters[i] = NewSlidingWindowRateLimiter(perShard, window)
	}
	return &ShardedSlidingWindowRateLimiter{
		limiters: limiters,
		total:    shards * perShard,
	}
}

// Allow round-robin 分发到一个分片检查。
func (sw *ShardedSlidingWindowRateLimiter) Allow() bool {
	idx := int(sw.nextIdx.Add(1)-1) % len(sw.limiters)
	return sw.limiters[idx].Allow()
}

// AllowN round-robin 分发。
func (sw *ShardedSlidingWindowRateLimiter) AllowN(n int) bool {
	idx := int(sw.nextIdx.Add(1)-1) % len(sw.limiters)
	return sw.limiters[idx].AllowN(n)
}

// ShardCount 返回分片数。
func (sw *ShardedSlidingWindowRateLimiter) ShardCount() int {
	return len(sw.limiters)
}

// TotalLimit 返回所有分片总 limit（可能略小于创建值，由整数除法导致）。
func (sw *ShardedSlidingWindowRateLimiter) TotalLimit() int {
	return sw.total
}

// GetShard 获取指定分片实例。
func (sw *ShardedSlidingWindowRateLimiter) GetShard(idx int) *SlidingWindowRateLimiter {
	if idx < 0 || idx >= len(sw.limiters) {
		return nil
	}
	return sw.limiters[idx]
}

// ──────────────────────────── ShardedAdaptiveRateLimiter ────────────────────────────

// ShardedAdaptiveRateLimiter 将自适应限流器水平分片，各分片独立根据成功率调整并发。
// 适合极限高并发下需要自适应保护的场景。每片的 min/max 平均分配。
//
// 使用示例：
//
//	// 16 片，每片 min≈2, max≈50
//	al := ratelimit.NewShardedAdaptiveRateLimiter(16, 32, 800)
//	al.Acquire(ctx)
//	if err := doRequest(); err == nil {
//	    al.RecordSuccess()
//	} else {
//	    al.RecordFailure()
//	}
//	al.Release()
type ShardedAdaptiveRateLimiter struct {
	limiters []*AdaptiveRateLimiter
	nextIdx  atomic.Uint64
}

// NewShardedAdaptiveRateLimiter 创建分片自适应限流器。
// shards<=0 时默认 runtime.GOMAXPROCS(0)，最少 2 片。
func NewShardedAdaptiveRateLimiter(shards int, minRate int, maxRate int) *ShardedAdaptiveRateLimiter {
	if shards <= 0 {
		shards = core.IO()
		if shards < 2 {
			shards = 2
		}
	}
	perMin := minRate / shards
	if perMin < 1 {
		perMin = 1
	}
	perMax := maxRate / shards
	if perMax < perMin {
		perMax = perMin
	}
	limiters := make([]*AdaptiveRateLimiter, shards)
	for i := 0; i < shards; i++ {
		limiters[i] = NewAdaptiveRateLimiter(perMin, perMax)
	}
	return &ShardedAdaptiveRateLimiter{limiters: limiters}
}

// Acquire round-robin 分发获取令牌。
func (sa *ShardedAdaptiveRateLimiter) Acquire(ctx context.Context) error {
	idx := int(sa.nextIdx.Add(1)-1) % len(sa.limiters)
	return sa.limiters[idx].Acquire(ctx)
}

// Release round-robin 分发释放令牌。
func (sa *ShardedAdaptiveRateLimiter) Release() {
	idx := int(sa.nextIdx.Add(1)-1) % len(sa.limiters)
	sa.limiters[idx].Release()
}

// RecordSuccess round-robin 分发记录成功。
func (sa *ShardedAdaptiveRateLimiter) RecordSuccess() {
	idx := int(sa.nextIdx.Add(1)-1) % len(sa.limiters)
	sa.limiters[idx].RecordSuccess()
}

// RecordFailure round-robin 分发记录失败。
func (sa *ShardedAdaptiveRateLimiter) RecordFailure() {
	idx := int(sa.nextIdx.Add(1)-1) % len(sa.limiters)
	sa.limiters[idx].RecordFailure()
}

// Close 关闭所有分片。
func (sa *ShardedAdaptiveRateLimiter) Close() {
	for _, l := range sa.limiters {
		l.base.Close()
	}
}

// ShardCount 返回分片数。
func (sa *ShardedAdaptiveRateLimiter) ShardCount() int {
	return len(sa.limiters)
}

// GetShard 获取指定分片实例。
func (sa *ShardedAdaptiveRateLimiter) GetShard(idx int) *AdaptiveRateLimiter {
	if idx < 0 || idx >= len(sa.limiters) {
		return nil
	}
	return sa.limiters[idx]
}

// TotalMin 返回所有分片 minRate 之和。
func (sa *ShardedAdaptiveRateLimiter) TotalMin() int {
	var total int
	for _, l := range sa.limiters {
		total += l.minRate
	}
	return total
}

// TotalMax 返回所有分片 maxRate 之和。
func (sa *ShardedAdaptiveRateLimiter) TotalMax() int {
	var total int
	for _, l := range sa.limiters {
		total += l.maxRate
	}
	return total
}

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
