package ratelimit

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

func TestAdaptiveInitialMidpointAcrossConstructors(t *testing.T) {
	for _, bounds := range [][2]int{{-9, -1}, {0, 0}, {0, 8}, {1, 1}, {1, 2}, {1, 3}, {2, 8}, {3, 8}, {4, 3}, {8, 9}, {8, 10}, {31, 64}} {
		for mode := range 4 {
			t.Run(fmt.Sprintf("%d/%d/mode=%d", bounds[0], bounds[1], mode), func(t *testing.T) {
				var limiter *AdaptiveRateLimiter
				switch mode {
				case 0:
					limiter = NewAdaptiveRateLimiter(bounds[0], bounds[1])
				case 1:
					limiter = NewRatelimitBuilder(context.Background()).Adaptive().MinWorker(bounds[0]).MaxWorker(bounds[1]).Build()
				case 2:
					limiter = NewShardedAdaptiveRateLimiter(1, bounds[0], bounds[1]).GetShard(0)
				case 3:
					limiter = NewRatelimitBuilder(context.Background()).Sharded().Adaptive().Shards(1).MinWorker(bounds[0]).MaxWorker(bounds[1]).Build().GetShard(0)
				}
				defer limiter.base.Close()
				low := max(bounds[0], 1)
				high := max(bounds[1], low)
				// In a sharded constructor total<shards is promoted to 1
				// before the per-shard constructor normalizes its bounds.
				want := (low + high) / 2 // Small independent arithmetic oracle.
				if limiter.minRate != low || limiter.maxRate != high || int(atomic.LoadInt32(&limiter.current)) != want || limiter.base.Size() != want || limiter.base.Available() != want {
					t.Fatalf("midpoint state bounds=[%d,%d] current=%d size=%d available=%d want=[%d,%d]/%d", limiter.minRate, limiter.maxRate, atomic.LoadInt32(&limiter.current), limiter.base.Size(), limiter.base.Available(), low, high, want)
				}
				for range want {
					if err := limiter.Acquire(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := limiter.Acquire(ctx); err != context.Canceled {
					t.Fatalf("exhausted Acquire=%v", err)
				}
				for range want {
					limiter.Release()
				}
				if limiter.base.Available() != want {
					t.Fatal("releases did not restore initial capacity")
				}
			})
		}
	}
}
