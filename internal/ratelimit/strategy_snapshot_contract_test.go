package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// A policy update applies to future calls. A blocked acquisition retains the
// cancellation policy it selected before sleeping, including Token and Wait.
func TestLimiterPendingStrategySnapshot(t *testing.T) {
	for _, shards := range []int{0, 1, 3} {
		for _, api := range []string{"acquire", "wait", "token"} {
			for _, transition := range [][2]Strategy{{Block, Reject}, {Block, BlockForce}, {BlockForce, Block}, {BlockForce, Reject}} {
				for _, finish := range []string{"release", "close"} {
					t.Run(fmt.Sprintf("shards=%d/%s/%d-to-%d/%s", shards, api, transition[0], transition[1], finish), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							var acquire func(context.Context) error
							var release, closeLimiter func()
							var strategy func(Strategy)
							if shards == 0 {
								rl := NewRateLimiter(1, time.Hour)
								release, closeLimiter = rl.Release, rl.Close
								strategy = func(s Strategy) { rl.WithStrategy(s) }
								switch api {
								case "wait":
									acquire = rl.Wait
								case "token":
									acquire = func(ctx context.Context) error {
										token, err := rl.Token(ctx)
										if err == nil {
											token.Release()
										}
										return err
									}
								default:
									acquire = rl.Acquire
								}
								if err := rl.Acquire(context.Background()); err != nil {
									t.Fatal(err)
								}
							} else {
								rl := NewShardedRateLimiter(shards, shards, time.Hour)
								release, closeLimiter = rl.Release, rl.Close
								strategy = func(s Strategy) { rl.WithStrategy(s) }
								switch api {
								case "wait":
									acquire = rl.Wait
								case "token":
									acquire = func(ctx context.Context) error {
										token, err := rl.Token(ctx)
										if err == nil {
											token.Release()
										}
										return err
									}
								default:
									acquire = rl.Acquire
								}
								for i := 0; i < shards; i++ {
									if err := rl.Acquire(context.Background()); err != nil {
										t.Fatal(err)
									}
								}
							}
							defer closeLimiter()
							strategy(transition[0])
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							done := make(chan error, 1)
							go func() { done <- acquire(ctx) }()
							synctest.Wait()
							select {
							case err := <-done:
								t.Fatalf("waiter escaped before switch: %v", err)
							default:
							}
							strategy(transition[1])
							cancel()
							synctest.Wait()
							if transition[0] == Block {
								select {
								case err := <-done:
									if err != context.Canceled {
										t.Fatalf("original Block lost cancellation: %v", err)
									}
								default:
									t.Fatal("switch changed pending Block cancellation")
								}
							} else {
								select {
								case err := <-done:
									t.Fatalf("switch changed pending BlockForce: %v", err)
								default:
								}
								var want error
								if finish == "close" {
									closeLimiter()
									want = core.ErrRateLimiterStopped
								} else {
									count := shards
									if count == 0 {
										count = 1
									}
									for i := 0; i < count; i++ {
										release()
									}
								}
								synctest.Wait()
								select {
								case err := <-done:
									if err != want {
										t.Fatalf("finish error=%v want=%v", err, want)
									}
								default:
									t.Fatal("original BlockForce did not finish")
								}
							}
						})
					})
				}
			}
		}
	}
}

func TestLimiterNewAcquisitionStrategy(t *testing.T) {
	for _, shards := range []int{0, 1, 3} {
		for _, api := range []string{"acquire", "wait", "token"} {
			for _, before := range []Strategy{Block, Reject, BlockForce} {
				for _, after := range []Strategy{Block, Reject, BlockForce} {
					t.Run(fmt.Sprintf("shards=%d/%s/%d-to-%d", shards, api, before, after), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							var acquire func(context.Context) error
							var closeLimiter func()
							var strategy func(Strategy)
							if shards == 0 {
								rl := NewRateLimiter(1, time.Hour)
								closeLimiter = rl.Close
								strategy = func(s Strategy) { rl.WithStrategy(s) }
								switch api {
								case "wait":
									acquire = rl.Wait
								case "token":
									acquire = func(ctx context.Context) error {
										token, err := rl.Token(ctx)
										if err == nil {
											token.Release()
										}
										return err
									}
								default:
									acquire = rl.Acquire
								}
								if err := rl.Acquire(context.Background()); err != nil {
									t.Fatal(err)
								}
							} else {
								rl := NewShardedRateLimiter(shards, shards, time.Hour)
								closeLimiter = rl.Close
								strategy = func(s Strategy) { rl.WithStrategy(s) }
								switch api {
								case "wait":
									acquire = rl.Wait
								case "token":
									acquire = func(ctx context.Context) error {
										token, err := rl.Token(ctx)
										if err == nil {
											token.Release()
										}
										return err
									}
								default:
									acquire = rl.Acquire
								}
								for i := 0; i < shards; i++ {
									if err := rl.Acquire(context.Background()); err != nil {
										t.Fatal(err)
									}
								}
							}
							defer closeLimiter()
							strategy(before)
							strategy(after)
							ctx, cancel := context.WithCancel(context.Background())
							cancel()
							done := make(chan error, 1)
							go func() { done <- acquire(ctx) }()
							synctest.Wait()
							var want error
							switch after {
							case Block:
								want = context.Canceled
							case Reject:
								want = core.ErrRateLimitExceeded
							case BlockForce:
								select {
								case err := <-done:
									t.Fatalf("new BlockForce escaped cancellation: %v", err)
								default:
								}
								closeLimiter()
								synctest.Wait()
								want = core.ErrRateLimiterStopped
							}
							select {
							case err := <-done:
								if err != want {
									t.Fatalf("new acquisition error=%v want=%v", err, want)
								}
							default:
								t.Fatal("new acquisition retained previous strategy")
							}
						})
					})
				}
			}
		}
	}
}
