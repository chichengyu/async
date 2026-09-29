package ratelimit

import (
	"math"
	"sync"
	"time"
)

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
