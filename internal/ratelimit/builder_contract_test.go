package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Observe constructed state and resource ownership, without waiting for ticks.
// Defaults are explicit field assignments, not implicit production presets.
func TestLimiterBuilderDefaultsAndOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	entry := NewRatelimitBuilder(ctx)
	t.Run("rate-per-and-independent-builds", func(t *testing.T) {
		b := entry.RateLimiter().Rate(3).Per(time.Hour).Burst(7)
		first, second := b.Build(), b.Build()
		defer first.Close()
		defer second.Close()
		if first == second || first.tokens == second.tokens || first.Size() != 3 || first.Available() != 7 {
			t.Fatal("Build must create independent burst buckets")
		}
		if first.ctx.Err() != nil || first.Acquire(ctx) != nil {
			t.Fatal("saved builder context must not cancel available instance permits")
		}
		first.Close()
		if second.Available() != 7 || second.Acquire(context.Background()) != nil {
			t.Fatal("closing one build affected another")
		}
		b.DefaultRate()
		third := b.Build()
		defer third.Close()
		if third.Size() != core.IO() || third.perDuration != time.Hour {
			t.Fatal("DefaultRate changed Per or did not set IO")
		}
		fresh := entry.RateLimiter().Build()
		defer fresh.Close()
		if fresh.perDuration != 0 || fresh.Size() != core.IO() {
			t.Fatal("fresh subbuilder inherited prior settings")
		}
		explicit := entry.RateLimiter().DefaultPer().Build()
		defer explicit.Close()
		if explicit.perDuration != time.Second {
			t.Fatal("DefaultPer did not select one second")
		}
	})
	t.Run("token-and-window-zero-versus-default", func(t *testing.T) {
		zero := entry.TokenBucket().Build()
		if zero.Allow() || zero.rate != 0 || zero.capacity != 0 {
			t.Fatal("zero token builder unexpectedly supplied presets")
		}
		b := entry.TokenBucket().Rate(0).Capacity(1.5)
		a, other := b.Build(), b.Build()
		if a == other || !a.AllowN(1.5) || a.Allow() || !other.AllowN(1.5) {
			t.Fatal("token builds share budgets or change fractional capacity")
		}
		preset := b.DefaultRate().DefaultCapacity().Build()
		if preset.rate != float64(core.IO()) || preset.capacity != float64(core.IO()*2) {
			t.Fatal("token explicit defaults differ")
		}
		window := entry.SlidingWindow().Build()
		if window.Allow() || window.window != 0 || window.limit != 0 {
			t.Fatal("zero window builder unexpectedly supplied presets")
		}
		window = entry.SlidingWindow().DefaultLimit().DefaultWindow().Build()
		if window.limit != core.IO() || window.window != time.Second || !window.Allow() {
			t.Fatal("window explicit defaults differ")
		}
	})
	t.Run("sharded-and-adaptive-normalization", func(t *testing.T) {
		rate := entry.Sharded().RateLimiter().Shards(3).Build()
		defer rate.Close()
		window := entry.Sharded().SlidingWindow().Shards(3).Build()
		adaptive := entry.Sharded().Adaptive().Shards(3).Build()
		defer adaptive.Close()
		if rate.TotalRate() != 3 || window.TotalLimit() != 3 || adaptive.TotalMin() != 3 || adaptive.TotalMax() != 3 {
			t.Fatal("zero total must normalize to at least one per shard")
		}
		for i := 0; i < 3; i++ {
			if rate.GetShard(i).perDuration != 0 || window.GetShard(i).window != 0 {
				t.Fatal("zero periods were silently defaulted")
			}
		}
		ordinary := entry.Adaptive().Build()
		defer ordinary.base.Close()
		if ordinary.minRate != 1 || ordinary.maxRate != 1 || ordinary.base.Size() != 1 {
			t.Fatal("adaptive zero bounds must normalize to one")
		}
	})
}
