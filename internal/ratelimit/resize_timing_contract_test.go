package ratelimit

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// Changing the rate halfway to a tick must not apply the new rate to elapsed
// time. The fractional token accumulated at the old rate is still available.
func TestRateLimiterResizeBetweenTicks(t *testing.T) {
	for _, tc := range []struct {
		name      string
		old, next int
	}{
		{"decrease", 3, 1},
		{"increase", 1, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rl := NewRateLimiterWithBurst(tc.old, 200*time.Millisecond, 8).WithStrategy(Reject)
				defer rl.Close()
				for range 8 {
					if err := rl.Acquire(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				time.Sleep(50 * time.Millisecond)
				rl.Resize(tc.next)
				if got := rl.Available(); got != 0 {
					t.Fatalf("resize supplied %d tokens before tick", got)
				}
				time.Sleep(50 * time.Millisecond)
				synctest.Wait()
				// oldRate*50ms + nextRate*50ms == 200ms, exactly one token.
				if got := rl.Available(); got != 1 {
					t.Fatalf("first tick available=%d, want one token from split rates", got)
				}
			})
		})
	}
}
