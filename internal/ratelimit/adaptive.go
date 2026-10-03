package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
)

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
	s := atomic.SwapInt64(&a.success, 0)
	f := atomic.SwapInt64(&a.failure, 0)
	if s == 0 && f == 0 {
		return
	}
	total := s + f
	if total < 10 {
		atomic.AddInt64(&a.success, s)
		atomic.AddInt64(&a.failure, f)
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
	} else if failRate < a.adjustUp/2 && current < a.maxRate {
		newRate := current + int(float64(a.maxRate-current)*a.adjustUp)
		if newRate > a.maxRate {
			newRate = a.maxRate
		}
		a.base.Resize(newRate)
		atomic.StoreInt32(&a.current, int32(newRate))
	}
}
