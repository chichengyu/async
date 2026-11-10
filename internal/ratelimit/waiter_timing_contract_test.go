package ratelimit

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// synctest.Wait establishes that every waiter is blocked before cancellation,
// release, resize or shutdown. No scheduling sleeps or polling deadlines.
func TestRateLimiterWaiterTransitions(t *testing.T) {
	for _, action := range []string{"cancel", "force-release", "resize", "close", "refill"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rl := NewRateLimiter(2, time.Second)
				defer rl.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if action == "force-release" {
					rl.WithStrategy(BlockForce)
				}
				for range 2 {
					if err := rl.Acquire(ctx); err != nil {
						t.Fatal(err)
					}
				}
				results := make(chan error, 2)
				for range 2 {
					go func() { results <- rl.Acquire(ctx) }()
				}
				synctest.Wait()
				select {
				case err := <-results:
					t.Fatalf("waiter returned before transition: %v", err)
				default:
				}
				var want error
				switch action {
				case "cancel":
					cancel()
					want = context.Canceled
				case "force-release":
					cancel()
					synctest.Wait()
					select {
					case err := <-results:
						t.Fatalf("BlockForce obeyed cancellation: %v", err)
					default:
					}
					rl.Release()
					rl.Release()
				case "resize":
					rl.Resize(4)
				case "close":
					rl.Close()
					want = core.ErrRateLimiterStopped
				case "refill":
					time.Sleep(time.Second)
				}
				synctest.Wait()
				for range 2 {
					select {
					case err := <-results:
						if !errors.Is(err, want) {
							t.Fatalf("transition error=%v want=%v", err, want)
						}
					default:
						t.Fatal("transition left a waiter blocked")
					}
				}
			})
		})
	}
}
