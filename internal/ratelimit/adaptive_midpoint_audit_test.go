//go:build audit

package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"testing"
)

// A chan struct{} does not allocate one byte per permit, but initializing this
// many permits is substantial work. Exercise the actual constructor only in
// non-short 386 audits, where adding two valid int32 bounds can overflow int.
func TestAdaptiveMidpointInt32Boundary386(t *testing.T) {
	if strconv.IntSize != 32 || testing.Short() {
		t.Skip("requires a non-short 32-bit execution")
	}
	maximum := int(^uint(0) >> 1)
	want := 1 + (maximum-1)/2
	for mode := range 4 {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			var limiter *AdaptiveRateLimiter
			switch mode {
			case 0:
				limiter = NewAdaptiveRateLimiter(1, maximum)
			case 1:
				limiter = NewRatelimitBuilder(context.Background()).Adaptive().MinWorker(1).MaxWorker(maximum).Build()
			case 2:
				limiter = NewShardedAdaptiveRateLimiter(1, 1, maximum).GetShard(0)
			case 3:
				limiter = NewRatelimitBuilder(context.Background()).Sharded().Adaptive().Shards(1).MinWorker(1).MaxWorker(maximum).Build().GetShard(0)
			}
			defer limiter.base.Close()
			if got := limiter.base.Size(); got != want {
				t.Fatalf("adaptive midpoint size=%d want=%d", got, want)
			}
			if got := limiter.base.Available(); got != want {
				t.Fatalf("adaptive midpoint available=%d want=%d", got, want)
			}
			if err := limiter.Acquire(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := limiter.base.Available(); got != want-1 {
				t.Fatalf("acquired available=%d want=%d", got, want-1)
			}
			limiter.Release()
			if got := limiter.base.Available(); got != want {
				t.Fatalf("released available=%d want=%d", got, want)
			}
		})
	}
}
