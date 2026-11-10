package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

type scopedLimiter interface{ Acquire(context.Context) error }

func TestLimiterScopeClosesOnEveryOutcome(t *testing.T) {
	for _, kind := range []string{"single", "burst", "sharded", "sharded-burst", "sharded-adaptive"} {
		for _, outcome := range []string{"success", "error", "panic"} {
			t.Run(kind+"/"+outcome, func(t *testing.T) {
				businessErr := errors.New("business failure")
				var bases []*RateLimiter
				var captured scopedLimiter
				calls := 0
				callback := func(l scopedLimiter) error {
					captured = l
					calls++
					if err := l.Acquire(context.Background()); err != nil {
						t.Fatal(err)
					}
					switch outcome {
					case "error":
						return businessErr
					case "panic":
						panic(businessErr)
					}
					return nil
				}
				var err error
				var recovered any
				func() {
					defer func() { recovered = recover() }()
					switch kind {
					case "single":
						err = WithRateLimiter(2, time.Hour, func(l *RateLimiter) error { bases = []*RateLimiter{l}; return callback(l) })
					case "burst":
						err = WithRateLimiterWithBurst(2, time.Hour, 4, func(l *RateLimiter) error { bases = []*RateLimiter{l}; return callback(l) })
					case "sharded":
						err = WithShardedRateLimiter(2, 4, time.Hour, func(l *ShardedRateLimiter) error { bases = l.limiters; return callback(l) })
					case "sharded-burst":
						err = WithShardedRateLimiterWithBurst(2, 4, time.Hour, 8, func(l *ShardedRateLimiter) error { bases = l.limiters; return callback(l) })
					case "sharded-adaptive":
						err = WithShardedAdaptiveRateLimiter(2, 2, 4, func(l *ShardedAdaptiveRateLimiter) error {
							for _, shard := range l.limiters {
								bases = append(bases, shard.base)
							}
							return callback(l)
						})
					}
				}()
				if calls != 1 || len(bases) == 0 {
					t.Fatalf("callback count=%d bases=%d", calls, len(bases))
				}
				switch outcome {
				case "success":
					if err != nil || recovered != nil {
						t.Fatalf("success: err=%v panic=%v", err, recovered)
					}
				case "error":
					if err != businessErr || recovered != nil {
						t.Fatalf("error identity lost: err=%v panic=%v", err, recovered)
					}
				case "panic":
					if recovered != businessErr {
						t.Fatalf("panic identity lost: %v", recovered)
					}
				}
				for i, base := range bases {
					if !base.closed.Load() {
						t.Fatalf("shard %d remains open", i)
					}
					if base.refillDone != nil {
						select {
						case <-base.refillDone:
						default:
							t.Fatalf("shard %d refill remains live", i)
						}
					}
				}
				if err := captured.Acquire(context.Background()); !errors.Is(err, core.ErrRateLimiterStopped) {
					t.Fatalf("escaped limiter Acquire=%v", err)
				}
			})
		}
	}
}
