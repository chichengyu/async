package ratelimit

import (
	"sort"
	"sync"
	"time"
)

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
