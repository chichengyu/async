package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// Literal schedules are independent of computeBackoff. Callback duration is
// separate from the gap after failure; neither success nor exhaustion may sleep.
func TestRetryBusinessBackoffSchedule(t *testing.T) {
	for _, tc := range []struct {
		name         string
		initial, cap time.Duration
		exp, linear  []time.Duration
	}{
		{"capped", 2 * time.Second, 5 * time.Second, []time.Duration{0, 2, 6, 11}, []time.Duration{0, 2, 4, 6}},
		{"uncapped", 2 * time.Second, 0, []time.Duration{0, 2, 6, 14}, []time.Duration{0, 2, 4, 6}},
		{"negative-cap", 2 * time.Second, -time.Second, []time.Duration{0, 2, 6, 14}, []time.Duration{0, 2, 4, 6}},
		{"initial-exceeds-cap", 10 * time.Second, 3 * time.Second, []time.Duration{0, 3, 6, 9}, []time.Duration{0, 10, 20, 30}},
		{"zero", 0, time.Second, []time.Duration{0, 0, 0, 0}, []time.Duration{0, 0, 0, 0}},
		{"negative", -time.Second, time.Second, []time.Duration{0, 0, 0, 0}, []time.Duration{0, 0, 0, 0}},
	} {
		for engine := range 10 {
			for _, success := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/engine=%d/success=%v", tc.name, engine, success), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						start := time.Now()
						const work = 250 * time.Millisecond
						failure := errors.New("business failure")
						var times []time.Duration
						fn := func(context.Context) (int, error) {
							times = append(times, time.Since(start))
							time.Sleep(work)
							if success && len(times) == 2 {
								return 42, nil
							}
							return -1, failure
						}
						ctx := context.Background()
						void := func(ctx context.Context) error { _, err := fn(ctx); return err }
						var value int
						var err error
						switch engine {
						case 0:
							value, err = RetryWithBackoff(ctx, fn, 3, tc.initial, tc.cap)
						case 1:
							err = RetryWithBackoffVoid(ctx, void, 3, tc.initial, tc.cap)
						case 2:
							r := RetryWithBackoffResult(ctx, fn, 3, tc.initial, tc.cap)
							value, err = r.Value, r.Err
						case 3:
							value, err = RetryWithConfig(ctx, fn, 3, tc.initial, tc.cap)
						case 4:
							err = RetryWithConfigVoid(ctx, void, 3, tc.initial, tc.cap)
						case 5:
							value, err = New[int](ctx).Exponential().MaxRetries(3).Backoff(tc.initial, tc.cap).Execute(fn)
						case 6:
							value, err = RetryWithLinearBackoff(ctx, fn, 3, tc.initial)
						case 7:
							err = RetryWithLinearBackoffVoid(ctx, void, 3, tc.initial)
						case 8:
							r := RetryWithLinearBackoffResult(ctx, fn, 3, tc.initial)
							value, err = r.Value, r.Err
						case 9:
							value, err = New[int](ctx).Linear().MaxRetries(3).Backoff(tc.initial, tc.cap).Execute(fn)
						}
						want := slices.Clone(tc.exp)
						if engine >= 6 {
							want = slices.Clone(tc.linear)
						}
						if success {
							want = want[:2]
						}
						for i := range want {
							want[i] = want[i]*time.Second + time.Duration(i)*work
						}
						if !slices.Equal(times, want) {
							t.Fatalf("attempt starts=%v want=%v", times, want)
						}
						if elapsed := time.Since(start); elapsed != want[len(want)-1]+work {
							t.Fatalf("unexpected terminal delay: elapsed=%v", elapsed)
						}
						if success {
							if err != nil {
								t.Fatal(err)
							}
							if engine != 1 && engine != 4 && engine != 7 && value != 42 {
								t.Fatalf("success value=%d", value)
							}
						} else if value != 0 || !errors.Is(err, failure) {
							t.Fatalf("failure value=%d err=%v", value, err)
						}
					})
				})
			}
		}
	}
}

// Fast failures must cancel each child before the next attempt, not just at the
// end of the retry call. Only opts[0] configures the functional entry points.
func TestRetryPerCallContextLifetime(t *testing.T) {
	for engine := range 3 {
		for _, timeout := range []time.Duration{-time.Second, 0, 2 * time.Second} {
			for _, panics := range []bool{false, true} {
				t.Run(fmt.Sprintf("engine=%d/timeout=%v/panic=%v", engine, timeout, panics), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						parent, cancel := context.WithCancel(context.Background())
						defer cancel()
						var contexts []context.Context
						fn := func(ctx context.Context) (int, error) {
							if n := len(contexts); n > 0 && timeout > 0 && contexts[n-1].Err() != context.Canceled {
								t.Fatal("previous child context still live at next attempt")
							}
							deadline, hasDeadline := ctx.Deadline()
							if timeout > 0 {
								if ctx == parent || !hasDeadline || deadline.Sub(time.Now()) != timeout || ctx.Err() != nil {
									t.Fatalf("incorrect per-call deadline: present=%v remaining=%v err=%v", hasDeadline, deadline.Sub(time.Now()), ctx.Err())
								}
							} else if ctx != parent || hasDeadline {
								t.Fatal("disabled timeout changed parent context")
							}
							contexts = append(contexts, ctx)
							if panics {
								panic("timing contract")
							}
							return -1, errors.New("retry")
						}
						var err error
						opts := []TimeoutOpt{{PerCallTimeout: timeout}, {PerCallTimeout: 5 * time.Second}}
						switch engine {
						case 0:
							_, err = RetryWithConfig(parent, fn, 2, time.Second, 0, opts...)
						case 1:
							err = RetryWithConfigVoid(parent, func(ctx context.Context) error { _, err := fn(ctx); return err }, 2, time.Second, 0, opts...)
						case 2:
							_, err = New[int](parent).MaxRetries(2).Backoff(time.Second, 0).PerCallTimeout(timeout).Execute(fn)
						}
						if err == nil || len(contexts) != 3 || parent.Err() != nil {
							t.Fatalf("calls=%d parent=%v err=%v", len(contexts), parent.Err(), err)
						}
						if timeout > 0 {
							for i, ctx := range contexts {
								if ctx.Err() != context.Canceled {
									t.Fatalf("child %d not canceled: %v", i, ctx.Err())
								}
								if i > 0 && ctx == contexts[i-1] {
									t.Fatal("child context reused")
								}
							}
						}
					})
				})
			}
		}
	}
}

func TestRetryPerCallDeadlineSchedule(t *testing.T) {
	for engine := range 3 {
		for _, parentLimit := range []time.Duration{5 * time.Second, time.Hour} {
			t.Run(fmt.Sprintf("engine=%d/parent=%v", engine, parentLimit), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					start := time.Now()
					parent, cancel := context.WithTimeout(context.Background(), parentLimit)
					defer cancel()
					var times []time.Duration
					fn := func(ctx context.Context) (int, error) {
						times = append(times, time.Since(start))
						deadline, ok := ctx.Deadline()
						want := min(time.Since(start)+2*time.Second, parentLimit)
						if !ok || deadline.Sub(start) != want {
							t.Fatalf("deadline=%v want offset=%v", deadline, want)
						}
						<-ctx.Done()
						return -1, ctx.Err()
					}
					var err error
					switch engine {
					case 0:
						_, err = RetryWithConfig(parent, fn, 2, time.Second, 0, TimeoutOpt{PerCallTimeout: 2 * time.Second})
					case 1:
						err = RetryWithConfigVoid(parent, func(ctx context.Context) error { _, err := fn(ctx); return err }, 2, time.Second, 0, TimeoutOpt{PerCallTimeout: 2 * time.Second})
					case 2:
						_, err = New[int](parent).MaxRetries(2).Backoff(time.Second, 0).PerCallTimeout(2 * time.Second).Execute(fn)
					}
					wantTimes, wantElapsed := []time.Duration{0, 3 * time.Second, 7 * time.Second}, 9*time.Second
					if parentLimit == 5*time.Second {
						wantTimes, wantElapsed = wantTimes[:2], parentLimit
					}
					if !slices.Equal(times, wantTimes) || time.Since(start) != wantElapsed || !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("times=%v elapsed=%v err=%v", times, time.Since(start), err)
					}
				})
			})
		}
	}
}

// Huge retry budgets and duration limits must not prevent canceling an actual
// backoff timer. No huge loop is needed to exercise the first waiting attempt.
func TestRetryExtremeBackoffCancellation(t *testing.T) {
	for engine := range 3 {
		for _, cap := range []time.Duration{0, time.Duration(math.MaxInt64)} {
			t.Run(fmt.Sprintf("engine=%d/cap=%v", engine, cap), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					go func() { time.Sleep(time.Second); cancel() }()
					calls := 0
					fn := func(context.Context) (int, error) { calls++; return -1, errors.New("failed") }
					start := time.Now()
					var value int
					var err error
					switch engine {
					case 0:
						value, err = RetryWithBackoff(ctx, fn, math.MaxInt, time.Duration(math.MaxInt64), cap)
					case 1:
						value, err = RetryWithConfig(ctx, fn, math.MaxInt, time.Duration(math.MaxInt64), cap)
					case 2:
						value, err = New[int](ctx).MaxRetries(math.MaxInt).Backoff(time.Duration(math.MaxInt64), cap).Execute(fn)
					}
					if value != 0 || !errors.Is(err, context.Canceled) || calls != 1 || time.Since(start) != time.Second {
						t.Fatalf("value=%d err=%v calls=%d elapsed=%v", value, err, calls, time.Since(start))
					}
				})
			})
		}
	}
}

// Actual elapsed fake time refills tokens and expires windows. Rejections must
// consume retry attempts and backoff gaps even though no callback runs.
func TestRetryLimiterBackoffTimeline(t *testing.T) {
	for _, mode := range []string{"token", "window"} {
		for _, shards := range []int{1, 2} {
			for _, linear := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/shards=%d/linear=%v", mode, shards, linear), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						start := time.Now()
						failure := errors.New("timeline business failure")
						var times []time.Duration
						chain := New[int](context.Background()).MaxRetries(4).Backoff(400*time.Millisecond, 1200*time.Millisecond)
						if linear {
							chain.Linear()
						} else {
							chain.Exponential()
						}
						if mode == "token" {
							chain.TokenBucket().Rate(1).Capacity(1)
						} else {
							chain.SlidingWindow().Limit(shards).Window(time.Second)
						}
						chain.Shards(shards)
						value, err := chain.Execute(func(context.Context) (int, error) {
							times = append(times, time.Since(start))
							return -1, failure
						})
						want := []time.Duration{0, 1200 * time.Millisecond, 2400 * time.Millisecond, 3600 * time.Millisecond}
						wantElapsed := 3600 * time.Millisecond
						if shards == 2 {
							want = []time.Duration{0, 400 * time.Millisecond, 1200 * time.Millisecond, 2400 * time.Millisecond, 3600 * time.Millisecond}
						}
						if linear {
							wantElapsed = 1600 * time.Millisecond
							want = []time.Duration{0, 1200 * time.Millisecond}
							if shards == 2 {
								want = []time.Duration{0, 400 * time.Millisecond, 1600 * time.Millisecond}
							}
						}
						if value != 0 || !slices.Equal(times, want) || time.Since(start) != wantElapsed {
							t.Fatalf("value=%d starts=%v want=%v elapsed=%v", value, times, want, time.Since(start))
						}
						if linear && shards == 1 {
							if err == nil || !strings.Contains(err.Error(), "rate limit exceeded") || errors.Is(err, failure) {
								t.Fatalf("final denial error=%v", err)
							}
						} else if !errors.Is(err, failure) {
							t.Fatalf("final business error=%v", err)
						}
					})
				})
			}
		}
	}
}
