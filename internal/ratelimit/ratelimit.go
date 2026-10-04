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
			refill:
				for i := 0; i < curBatch; i++ {
					select {
					case rl.tokens <- struct{}{}:
						added++
					default:
						break refill
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
		for {
			if rl.closed.Load() {
				return core.ErrRateLimiterStopped
			}
			rl.resizeMu.RLock()
			select {
			case _, ok := <-rl.tokens:
				rl.resizeMu.RUnlock()
				if ok {
					return nil
				}
				continue
			default:
				rl.resizeMu.RUnlock()
				return core.ErrRateLimitExceeded
			}
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
		stop := context.AfterFunc(ctx, func() {
			rl.mu.Lock()
			rl.cond.Broadcast()
			rl.mu.Unlock()
		})
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
	inFlight := cap(oldTokens) - len(oldTokens)
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
	fill := newRate - inFlight
	if fill < 0 {
		fill = 0
	}
	for i := 0; i < fill; i++ {
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
