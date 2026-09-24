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
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/core"
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

	rate        int           // 每 perDuration 补充的令牌数
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
		rate:        rate,
		perDuration: perDuration,
		refillStop:  make(chan struct{}),
		refillDone:  make(chan struct{}),
	}
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
		rate:        rate,
		perDuration: perDuration,
		refillStop:  make(chan struct{}),
		refillDone:  make(chan struct{}),
	}
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

func (rl *RateLimiter) startRefill() {
	interval := rl.perDuration / time.Duration(rl.rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	// 批量补充：当速率很高时（如 10000/s），逐令牌补充会产生极大的 ticker 开销。
	// 改为按较粗粒度周期性补充，每次补充一批令牌，大幅降低 CPU 消耗。
	const minBatchInterval = 100 * time.Millisecond
	const maxBatchInterval = time.Second

	var batchSize int
	var tickInterval time.Duration

	if interval < minBatchInterval {
		// 高速率场景：批量补充
		batchSize = int(minBatchInterval / interval)
		if batchSize < 1 {
			batchSize = 1
		}
		tickInterval = minBatchInterval
	} else if interval > maxBatchInterval {
		// 极低速率场景：最长 1 秒补充一次，每次补充少量
		batchSize = int(float64(rl.rate) * maxBatchInterval.Seconds() / rl.perDuration.Seconds())
		if batchSize < 1 {
			batchSize = 1
		}
		tickInterval = maxBatchInterval
	} else {
		// 中等速率：逐令牌补充（间隔在合理范围内）
		batchSize = 1
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
				rl.resizeMu.RLock()
				added := 0
				for i := 0; i < batchSize; i++ {
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
					rl.cond.Broadcast()
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
	if rl.closed.Load() {
		return
	}
	rl.resizeMu.RLock()
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
		if rl.refillDone != nil {
			<-rl.refillDone
		}
	}
	close(rl.tokens)
	rl.cond.Broadcast()
	rl.mu.Unlock()
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
	n := 0
	for _, ts := range sw.timestamps {
		if ts.After(cutoff) {
			break
		}
		n++
	}
	sw.timestamps = sw.timestamps[n:]
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
	i := 0
	for _, ts := range sw.timestamps {
		if ts.After(cutoff) {
			break
		}
		i++
	}
	sw.timestamps = sw.timestamps[i:]
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
