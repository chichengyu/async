package retry

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Cancel the original parent after one successful execution: reconstructing the
// limiter or resetting retry policy must not detach the next run from it.
func TestRetryDefaultConfigRetainsParentCancellation(t *testing.T) {
	for _, mode := range []string{"none", "rate", "token", "window", "adaptive"} {
		for _, shards := range []int{1, 2} {
			for endpoint := range 4 {
				t.Run(fmt.Sprintf("%s/shards%d/endpoint%d", mode, shards, endpoint), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						parent, cancel := context.WithCancel(context.Background())
						defer cancel()
						c := New[int](parent).Shards(shards)
						switch mode {
						case "rate":
							c.RateLimiter()
						case "token":
							c.TokenBucket()
						case "window":
							c.SlidingWindow()
						case "adaptive":
							c.Adaptive()
						}
						calls := 0
						call := func(context.Context) (int, error) { calls++; return 7, nil }
						for run := range 2 {
							if run == 1 {
								cancel()
							}
							c.DefaultConfig()
							start := time.Now()
							var err error
							switch endpoint {
							case 0:
								_, err = c.Execute(call)
							case 1:
								err = c.ExecuteVoid(func(ctx context.Context) error { _, e := call(ctx); return e })
							case 2:
								err = c.Run(func() error { _, e := call(nil); return e })
							case 3:
								err = c.RunVoid(func(ctx context.Context) error { _, e := call(ctx); return e })
							}
							if (run == 0 && err != nil) || (run == 1 && !errors.Is(err, context.Canceled)) || calls != 1 || time.Since(start) != 0 {
								t.Fatalf("run %d err=%v calls=%d elapsed=%v", run, err, calls, time.Since(start))
							}
						}
					})
				})
			}
		}
	}
}

// DefaultConfig must preserve the selected limiter and its arguments. Literal
// virtual-time schedules and expected admission budgets check terminal behavior
// independently of the configuration snapshot, twice on the same builder.
func TestRetryDefaultConfigPreservesLimiterAndFreshExecution(t *testing.T) {
	for _, mode := range []string{"none", "rate", "token", "window", "adaptive"} {
		for _, shards := range []int{1, 2} {
			for endpoint := range 4 {
				for outcome := range 3 {
					t.Run(fmt.Sprintf("%s/shards%d/endpoint%d/outcome%d", mode, shards, endpoint, outcome), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							type contextKey struct{}
							marker := new(int)
							parent := context.WithValue(context.Background(), contextKey{}, marker)
							c := New[int](parent).Context(parent).Linear().MaxRetries(8).Backoff(9*time.Second, 11*time.Second).PerCallTimeout(time.Nanosecond).Rate(3).Per(time.Hour).Burst(7).Capacity(0.5).Limit(1).Window(time.Hour).MinWorker(2).MaxWorker(6).Shards(shards)
							switch mode {
							case "rate":
								c.RateLimiter()
							case "token":
								c.TokenBucket()
							case "window":
								c.SlidingWindow()
							case "adaptive":
								c.Adaptive()
							}
							want := *c
							want.isLinear = false
							want.maxRetries = 3
							want.initialBackoff = 100 * time.Millisecond
							want.maxBackoff = 30 * time.Second
							want.perCallTimeout = 0
							if c.DefaultConfig() != c || *c != want {
								t.Fatal("DefaultConfig changed retained state or failed to reset retry policy")
							}
							failure := errors.New("default configuration business error")
							panicValue := new(int)
							for run := range 2 {
								start := time.Now()
								var starts []time.Duration
								call := func(ctx context.Context) (int, error) {
									if ctx != nil {
										if ctx.Value(contextKey{}) != marker || ctx.Err() != nil {
											t.Fatal("retained parent context lost")
										}
										if _, ok := ctx.Deadline(); ok {
											t.Fatal("DefaultConfig retained per-call deadline")
										}
									}
									starts = append(starts, time.Since(start))
									switch outcome {
									case 0:
										return 42, nil
									case 1:
										return -42, failure
									default:
										panic(panicValue)
									}
								}
								var value int
								var err error
								switch endpoint {
								case 0:
									value, err = c.Execute(call)
								case 1:
									err = c.ExecuteVoid(func(ctx context.Context) error { _, e := call(ctx); return e })
								case 2:
									err = c.Run(func() error { _, e := call(nil); return e })
								case 3:
									err = c.RunVoid(func(ctx context.Context) error { _, e := call(ctx); return e })
								}
								wantTimes := []time.Duration{0, 100 * time.Millisecond, 300 * time.Millisecond, 700 * time.Millisecond}
								wantElapsed := 700 * time.Millisecond
								rejected := mode == "token" || (mode == "window" && outcome != 0)
								if mode == "token" {
									wantTimes = nil
								} else if outcome == 0 {
									wantTimes = wantTimes[:1]
									wantElapsed = 0
								} else if mode == "window" {
									wantTimes = wantTimes[:shards]
								}
								if !slices.Equal(starts, wantTimes) || time.Since(start) != wantElapsed {
									t.Fatalf("run %d starts=%v elapsed=%v want %v/%v", run, starts, time.Since(start), wantTimes, wantElapsed)
								}
								var pe *core.PanicError
								if rejected {
									if err == nil || !strings.Contains(err.Error(), "rate limit exceeded") || errors.Is(err, failure) || errors.As(err, &pe) {
										t.Fatalf("run %d lost final limiter rejection: %v", run, err)
									}
								} else {
									switch outcome {
									case 0:
										if err != nil {
											t.Fatal(err)
										}
									case 1:
										if !errors.Is(err, failure) {
											t.Fatal("lost business error")
										}
									case 2:
										if !errors.As(err, &pe) || pe.Value != panicValue {
											t.Fatal("lost panic identity")
										}
									}
								}
								wantValue := 0
								if endpoint == 0 && outcome == 0 && !rejected {
									wantValue = 42
								}
								if value != wantValue {
									t.Fatalf("value=%d want=%d", value, wantValue)
								}
								if *c != want {
									t.Fatal("terminal mutated builder configuration")
								}
							}
						})
					})
				}
			}
		}
	}
}
