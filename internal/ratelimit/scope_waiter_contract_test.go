package ratelimit

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

func TestLimiterScopeClosesWaitersWithoutJoiningBusiness(t *testing.T) {
	for _, kind := range []string{"single", "burst", "sharded", "sharded-burst", "sharded-adaptive"} {
		strategies := []Strategy{Block, BlockForce}
		if kind == "sharded-adaptive" {
			strategies = []Strategy{Block}
		}
		for _, strategy := range strategies {
			for _, outcome := range []string{"success", "error", "panic"} {
				name := kind + "/block/" + outcome
				if strategy == BlockForce {
					name = kind + "/force/" + outcome
				}
				t.Run(name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						var bases []*RateLimiter
						var acquire func(context.Context) error
						var release func()
						var closeAgain func()
						waiters := make(chan error, 2)
						businessGate, businessDone := make(chan struct{}), make(chan struct{})
						defer func() {
							select {
							case <-businessGate:
							default:
								close(businessGate)
							}
							if closeAgain != nil {
								closeAgain()
							}
						}()
						businessErr := errors.New("scope waiter outcome")
						calls, held := 0, 0
						callback := func() error {
							calls++
							for _, base := range bases {
								base.WithStrategy(strategy)
								held += base.Available()
							}
							for range held {
								if err := acquire(context.Background()); err != nil {
									t.Fatal(err)
								}
							}
							go func() {
								<-businessGate
								for range held {
									release()
								}
								close(businessDone)
							}()
							waitCtx, cancel := context.WithCancel(context.Background())
							t.Cleanup(cancel)
							if strategy == BlockForce {
								cancel()
							}
							for range len(bases) {
								go func() { waiters <- acquire(waitCtx) }()
							}
							synctest.Wait()
							select {
							case err := <-waiters:
								t.Fatalf("scope waiter escaped before Close: %v", err)
							default:
							}
							switch outcome {
							case "error":
								return businessErr
							case "panic":
								panic(businessErr)
							default:
								return nil
							}
						}
						var err error
						var recovered any
						func() {
							defer func() { recovered = recover() }()
							setSingle := func(l *RateLimiter) error {
								bases, acquire, release, closeAgain = []*RateLimiter{l}, l.Acquire, l.Release, l.Close
								return callback()
							}
							setSharded := func(l *ShardedRateLimiter) error {
								bases, acquire, release, closeAgain = l.limiters, l.Acquire, l.Release, l.Close
								return callback()
							}
							switch kind {
							case "single":
								err = WithRateLimiter(1, time.Hour, setSingle)
							case "burst":
								err = WithRateLimiterWithBurst(1, time.Hour, 2, setSingle)
							case "sharded":
								err = WithShardedRateLimiter(2, 2, time.Hour, setSharded)
							case "sharded-burst":
								err = WithShardedRateLimiterWithBurst(2, 2, time.Hour, 4, setSharded)
							case "sharded-adaptive":
								err = WithShardedAdaptiveRateLimiter(2, 2, 2, func(l *ShardedAdaptiveRateLimiter) error {
									for _, shard := range l.limiters {
										bases = append(bases, shard.base)
									}
									acquire, release, closeAgain = l.Acquire, l.Release, l.Close
									return callback()
								})
							}
						}()
						if calls != 1 {
							t.Fatalf("callback calls=%d", calls)
						}
						switch outcome {
						case "success":
							if err != nil || recovered != nil {
								t.Fatalf("success err=%v panic=%v", err, recovered)
							}
						case "error":
							if err != businessErr || recovered != nil {
								t.Fatalf("error identity err=%v panic=%v", err, recovered)
							}
						case "panic":
							if recovered != businessErr {
								t.Fatalf("panic identity=%v", recovered)
							}
						}
						synctest.Wait()
						select {
						case <-businessDone:
							t.Fatal("scope unexpectedly completed gated business")
						default:
						}
						for range len(bases) {
							select {
							case waiterErr := <-waiters:
								if waiterErr != core.ErrRateLimiterStopped {
									t.Fatalf("closed waiter=%v", waiterErr)
								}
							default:
								t.Fatal("scope close left waiter blocked")
							}
						}
						for _, base := range bases {
							if !base.closed.Load() {
								t.Fatal("scope returned with open base")
							}
							if base.refillDone != nil {
								select {
								case <-base.refillDone:
								default:
									t.Fatal("scope returned with live refill")
								}
							}
							if base.Available() != 0 {
								t.Fatal("unexpected free permit after draining")
							}
						}
						close(businessGate)
						synctest.Wait()
						select {
						case <-businessDone:
						default:
							t.Fatal("post-close Release blocked business")
						}
						for _, base := range bases {
							if base.Available() != 0 {
								t.Fatal("post-close Release restored a permit")
							}
						}
						if err := acquire(context.Background()); err != core.ErrRateLimiterStopped {
							t.Fatalf("escaped acquire=%v", err)
						}
						closeAgain()
					})
				})
			}
		}
	}
}
