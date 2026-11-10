package ratelimit

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Exercise only the public admission methods; unlike the permit fuzz model,
// this test never changes the private base strategy to Reject.
func TestAdaptivePublicBlockAdmission(t *testing.T) {
	for _, builder := range []bool{false, true} {
		for _, sharded := range []bool{false, true} {
			name := "constructor"
			if builder {
				name = "builder"
			}
			if sharded {
				name += "/sharded"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var l interface {
						Acquire(context.Context) error
						Release()
					}
					slots := 1
					if sharded {
						slots = 2
						var s *ShardedAdaptiveRateLimiter
						if builder {
							s = NewRatelimitBuilder(context.Background()).Sharded().Adaptive().Shards(2).MinWorker(2).MaxWorker(2).Build()
						} else {
							s = NewShardedAdaptiveRateLimiter(2, 2, 2)
						}
						defer s.Close()
						l = s
					} else if builder {
						l = NewRatelimitBuilder(context.Background()).Adaptive().MinWorker(1).MaxWorker(1).Build()
					} else {
						l = NewAdaptiveRateLimiter(1, 1)
					}
					canceled, cancel := context.WithCancel(context.Background())
					cancel()
					for range slots {
						if err := l.Acquire(canceled); err != nil {
							t.Fatalf("available slot with canceled context: %v", err)
						}
					}
					if err := l.Acquire(canceled); !errors.Is(err, context.Canceled) {
						t.Fatalf("exhausted canceled admission: %v", err)
					}
					result := make(chan error, 1)
					go func() { result <- l.Acquire(context.Background()) }()
					synctest.Wait()
					// Advancing time cannot replenish a concurrency slot.
					time.Sleep(time.Hour)
					select {
					case err := <-result:
						t.Fatalf("exhausted waiter returned without release: %v", err)
					default:
					}
					for range slots {
						l.Release()
					}
					if err := <-result; err != nil {
						t.Fatalf("released waiter: %v", err)
					}
					l.Release()
				})
			})
		}
	}
}

func TestShardedAdaptiveWaiterStaysOnSelectedShard(t *testing.T) {
	for _, closeWaiter := range []bool{false, true} {
		name := "cancel"
		if closeWaiter {
			name = "close"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := NewShardedAdaptiveRateLimiter(2, 2, 2)
				defer l.Close()
				// Drain shard 0 directly, leaving shard 1 free. This failed
				// outer admission must not own or release the direct permit.
				if err := l.GetShard(0).Acquire(context.Background()); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- l.Acquire(ctx) }()
				synctest.Wait()
				select {
				case err := <-result:
					t.Fatalf("selected empty shard did not block: %v", err)
				default:
				}
				want := error(context.Canceled)
				if closeWaiter {
					l.Close()
					want = core.ErrRateLimiterStopped
				} else {
					cancel()
				}
				if err := <-result; !errors.Is(err, want) {
					t.Fatalf("waiter=%v want=%v", err, want)
				}
				l.GetShard(0).Release()
			})
		})
	}
}
