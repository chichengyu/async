package retry

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/ratelimit"
)

type retryPermitFixture struct {
	rl      *ratelimit.RateLimiter
	srl     *ratelimit.ShardedRateLimiter
	al      *ratelimit.AdaptiveRateLimiter
	sal     *ratelimit.ShardedAdaptiveRateLimiter
	acquire func(context.Context) error
	release func()
	slots   int
}

func newRetryPermitFixture(t *testing.T, mode string) retryPermitFixture {
	t.Helper()
	var f retryPermitFixture
	f.slots = 1
	switch mode {
	case "rate":
		f.rl = ratelimit.NewRateLimiter(1, time.Hour)
		t.Cleanup(f.rl.Close)
		f.acquire, f.release = f.rl.Acquire, f.rl.Release
	case "sharded-rate":
		f.srl = ratelimit.NewShardedRateLimiter(2, 2, time.Hour)
		t.Cleanup(f.srl.Close)
		f.acquire, f.release, f.slots = f.srl.Acquire, f.srl.Release, 2
	case "adaptive":
		f.al = ratelimit.NewAdaptiveRateLimiter(1, 1)
		f.acquire, f.release = f.al.Acquire, f.al.Release
	case "sharded-adaptive":
		f.sal = ratelimit.NewShardedAdaptiveRateLimiter(2, 2, 2)
		t.Cleanup(f.sal.Close)
		f.acquire, f.release, f.slots = f.sal.Acquire, f.sal.Release, 2
	default:
		t.Fatalf("unknown limiter mode %s", mode)
	}
	return f
}

func (f retryPermitFixture) execute(c *RetryChain[int], fn func(context.Context) (int, error)) (int, error) {
	return c.executeCore(fn, f.srl, nil, nil, f.al, f.rl, nil, nil, f.sal)
}

func (f retryPermitFixture) checkAllReturned(t *testing.T) {
	t.Helper()
	if f.rl != nil && f.rl.Available() != f.slots {
		t.Fatalf("permit not returned: available=%d want=%d", f.rl.Available(), f.slots)
	}
	if f.srl != nil && f.srl.Available() != f.slots {
		t.Fatalf("permit not returned: available=%d want=%d", f.srl.Available(), f.slots)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < f.slots; i++ {
		// A canceled context still takes a free slot, but cannot block on a leak.
		if err := f.acquire(canceled); err != nil {
			t.Fatalf("permit not returned: acquire %d/%d: %v", i+1, f.slots, err)
		}
	}
	if err := f.acquire(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("extra permit created: %v", err)
	}
	for i := 0; i < f.slots; i++ {
		f.release()
	}
}

func TestRetryChainPermitCleanupOnAbnormalExit(t *testing.T) {
	for _, mode := range []string{"rate", "sharded-rate", "adaptive", "sharded-adaptive"} {
		for _, timeout := range []time.Duration{0, time.Hour} {
			for _, outcome := range []string{"success", "error", "business-panic", "goexit", "logger-panic"} {
				t.Run(fmt.Sprintf("%s/timeout=%v/%s", mode, timeout, outcome), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						fixture := newRetryPermitFixture(t, mode)
						parent, cancel := context.WithCancel(context.Background())
						defer cancel()
						chain := New[int](parent).MaxRetries(0).Backoff(0, 0).PerCallTimeout(timeout)
						marker := errors.New("business failure")
						loggerMarker := errors.New("logger panic")
						previous := core.GetLogger()
						defer core.SetLogger(previous)
						if outcome == "logger-panic" {
							core.SetLogger(timeoutPanickingLogger{marker: loggerMarker})
						}
						var child context.Context
						var recovered any
						var value int
						var err error
						calls, returned := 0, false
						done := make(chan struct{})
						go func() {
							defer close(done)
							defer func() { recovered = recover() }()
							value, err = fixture.execute(chain, func(ctx context.Context) (int, error) {
								child = ctx
								calls++
								switch outcome {
								case "goexit":
									runtime.Goexit()
								case "logger-panic", "business-panic":
									panic(marker)
								case "error":
									return 7, marker
								}
								return 7, nil
							})
							returned = true
						}()
						<-done
						if calls != 1 || child == nil || parent.Err() != nil {
							t.Fatal("callback or parent changed")
						}
						if timeout > 0 && child.Err() != context.Canceled {
							t.Fatal("per-call context not cleaned up")
						}
						switch outcome {
						case "goexit":
							if returned || recovered != nil {
								t.Fatal("Goexit replaced by return or panic")
							}
						case "logger-panic":
							if returned || recovered != loggerMarker {
								t.Fatal("logger panic swallowed or replaced")
							}
						case "success":
							if !returned || value != 7 || err != nil || recovered != nil {
								t.Fatal("success changed")
							}
						case "error":
							if !returned || value != 0 || !errors.Is(err, marker) || recovered != nil {
								t.Fatal("business error changed")
							}
						case "business-panic":
							var pe *core.PanicError
							if !returned || value != 0 || !errors.As(err, &pe) || pe.Value != marker || recovered != nil {
								t.Fatal("business panic recovery changed")
							}
						}
						fixture.checkAllReturned(t)
					})
				})
			}
		}
	}
}

func TestRetryChainPermitReturnedBeforeBackoff(t *testing.T) {
	for _, mode := range []string{"rate", "sharded-rate", "adaptive", "sharded-adaptive"} {
		for _, success := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/success=%t", mode, success), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					fixture := newRetryPermitFixture(t, mode)
					parent, cancel := context.WithCancel(context.Background())
					defer cancel()
					chain := New[int](parent).MaxRetries(1).Backoff(time.Second, time.Second)
					marker := errors.New("retry")
					start := time.Now()
					calls := 0
					var value int
					var err error
					done := make(chan struct{})
					go func() {
						defer close(done)
						value, err = fixture.execute(chain, func(context.Context) (int, error) {
							calls++
							if calls == 2 && success {
								return 7, nil
							}
							return 9, marker
						})
					}()
					synctest.Wait()
					if calls != 1 || time.Now() != start {
						t.Fatal("first failure/backoff timing changed")
					}
					fixture.checkAllReturned(t)
					<-done
					if calls != 2 || time.Since(start) != time.Second {
						t.Fatal("retry timing changed or permit held across attempts")
					}
					if success {
						if value != 7 || err != nil {
							t.Fatal("retry success changed")
						}
					} else if value != 0 || !errors.Is(err, marker) {
						t.Fatal("retry error changed")
					}
					fixture.checkAllReturned(t)
				})
			})
		}
	}
}

func TestRetryChainFailedAcquireDoesNotReleaseOthersPermit(t *testing.T) {
	for _, mode := range []string{"rate", "sharded-rate", "adaptive", "sharded-adaptive"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fixture := newRetryPermitFixture(t, mode)
				for i := 0; i < fixture.slots; i++ {
					if err := fixture.acquire(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				parent, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				calls := 0
				_, err := fixture.execute(New[int](parent).MaxRetries(0), func(context.Context) (int, error) { calls++; return 7, nil })
				if !errors.Is(err, context.DeadlineExceeded) || calls != 0 {
					t.Fatal("rejected attempt called business or replaced cancellation")
				}
				if err := fixture.acquire(parent); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("failed acquisition returned another holder's permit: %v", err)
				}
				for i := 0; i < fixture.slots; i++ {
					fixture.release()
				}
				fixture.checkAllReturned(t)
			})
		})
	}
}
