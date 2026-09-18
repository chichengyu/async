// Package ratelimit 提供速率限制器。
package ratelimit

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jxue/async/core"
	"github.com/rs/zerolog/log"
)

// Strategy defines the behaviour when the limiter is drained.
type Strategy int

const (
	Block      Strategy = iota // block until tokens are available or ctx done
	Reject                     // immediately return ErrRateLimiterStopped
	BlockForce                 // block until tokens available, ignoring ctx cancellation
)

// RateLimiter controls the concurrency rate.
type RateLimiter struct {
	tokens chan struct{}
	size   int32
	strat  atomic.Value
	closed atomic.Bool
	mu     sync.Mutex
	ctx    context.Context

	rate        int
	perDuration time.Duration
	refillStop  chan struct{}
	refillDone  chan struct{}
}

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
	rl.strat.Store(Block)
	rl.startRefill()
	return rl
}

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
	rl.strat.Store(Block)
	return rl
}

func (rl *RateLimiter) startRefill() {
	interval := rl.perDuration / time.Duration(rl.rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		defer close(rl.refillDone)
		for {
			select {
			case <-rl.refillStop:
				return
			case <-ticker.C:
				select {
				case rl.tokens <- struct{}{}:
				default:
				}
			}
		}
	}()
}

func (rl *RateLimiter) Wait(ctx context.Context) error {
	return rl.Acquire(ctx)
}

func (rl *RateLimiter) Stop() {
	rl.Close()
}

func (rl *RateLimiter) WithStrategy(s Strategy) *RateLimiter {
	rl.strat.Store(s)
	return rl
}

func (rl *RateLimiter) WithTraceID(ctx context.Context) (*RateLimiter, context.Context) {
	ctx = core.EnsureTraceID(ctx)
	rl.ctx = ctx
	return rl, ctx
}

func (rl *RateLimiter) Acquire(ctx context.Context) error {
	if rl.closed.Load() {
		return core.ErrRateLimiterStopped
	}
	strat, _ := rl.strat.Load().(Strategy)
	switch strat {
	case Reject:
		select {
		case rl.tokens <- struct{}{}:
			return nil
		default:
			return core.ErrRateLimiterStopped
		}
	case BlockForce:
		rl.tokens <- struct{}{}
		return nil
	default:
		select {
		case rl.tokens <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (rl *RateLimiter) Release() {
	if rl.closed.Load() {
		log.Ctx(rl.ctx).Warn().Msg("rate limiter release on closed limiter, token may be lost")
	}
	select {
	case <-rl.tokens:
	default:
	}
}

func (rl *RateLimiter) Close() {
	if rl.closed.CompareAndSwap(false, true) {
		if rl.refillStop != nil {
			close(rl.refillStop)
		}
		if rl.refillDone != nil {
			<-rl.refillDone
		}
		close(rl.tokens)
	}
}

func (rl *RateLimiter) Resize(newRate int) {
	if newRate <= 0 {
		return
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()

	current := int(rl.size)
	if newRate == current {
		return
	}

	newTokens := make(chan struct{}, newRate)
	for i := 0; i < current; i++ {
		select {
		case <-rl.tokens:
		default:
		}
	}
	for i := 0; i < newRate; i++ {
		newTokens <- struct{}{}
	}
	if rl.closed.Load() {
		close(newTokens)
	}
	rl.tokens = newTokens
	rl.size = int32(newRate)
}

func (rl *RateLimiter) Size() int {
	return int(atomic.LoadInt32(&rl.size))
}

func (rl *RateLimiter) Available() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return len(rl.tokens)
}

// Token wraps RateLimiter with auto-release via defer.
type Token struct {
	limiter *RateLimiter
}

func (t *Token) Release() {
	if t.limiter != nil {
		t.limiter.Release()
		t.limiter = nil
	}
}

func (rl *RateLimiter) Token(ctx context.Context) (*Token, error) {
	if err := rl.Acquire(ctx); err != nil {
		return nil, err
	}
	return &Token{limiter: rl}, nil
}

// ──────────────────────────── SlidingWindowRateLimiter ────────────────────────────

type SlidingWindowRateLimiter struct {
	limit      int
	window     time.Duration
	mu         sync.Mutex
	timestamps []time.Time
}

func NewSlidingWindowRateLimiter(limit int, window time.Duration) *SlidingWindowRateLimiter {
	return &SlidingWindowRateLimiter{
		limit:  limit,
		window: window,
	}
}

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

type TokenBucket struct {
	rate       float64
	capacity   float64
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
}

func NewTokenBucket(rate float64, capacity float64) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

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

type AdaptiveRateLimiter struct {
	base       *RateLimiter
	minRate    int
	maxRate    int
	current    int32
	success    int64
	failure    int64
	adjustUp   float64
	adjustDown float64
	mu         sync.Mutex
}

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

func (a *AdaptiveRateLimiter) Acquire(ctx context.Context) error {
	return a.base.Acquire(ctx)
}

func (a *AdaptiveRateLimiter) Release() {
	a.base.Release()
}

func (a *AdaptiveRateLimiter) RecordSuccess() {
	atomic.AddInt64(&a.success, 1)
	a.maybeAdjust()
}

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
