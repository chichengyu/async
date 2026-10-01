package ratelimit

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/internal/core"
)

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
	limiters   []*RateLimiter
	nextIdx    atomic.Uint64
	releaseIdx atomic.Uint64
	total      int32
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
	idx := int(sl.releaseIdx.Add(1)-1) % len(sl.limiters)
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
	limiters   []*AdaptiveRateLimiter
	nextIdx    atomic.Uint64
	releaseIdx atomic.Uint64
	recordIdx  atomic.Uint64
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
