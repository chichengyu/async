package ratelimit

import (
	"context"
	"fmt"
	"math"
	"testing"
	"testing/synctest"
	"time"
)

func TestSlidingWindowDurationBuilderAndShardPaths(t *testing.T) {
	for _, window := range []time.Duration{time.Duration(math.MinInt64), 0, time.Hour} {
		for mode := range 4 {
			t.Run(fmt.Sprintf("%d/mode=%d", window, mode), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var allowN func(int) bool
					switch mode {
					case 0:
						allowN = NewSlidingWindowRateLimiter(2, window).AllowN
					case 1:
						allowN = NewRatelimitBuilder(context.Background()).SlidingWindow().Limit(2).Window(window).Build().AllowN
					case 2:
						allowN = NewShardedSlidingWindowRateLimiter(2, 4, window).AllowN
					case 3:
						allowN = NewRatelimitBuilder(context.Background()).Sharded().SlidingWindow().Shards(2).Limit(4).Window(window).Build().AllowN
					}
					initial := 1
					if mode >= 2 {
						initial = 2
					}
					for step := range initial + 3 {
						want := window <= 0 || step < initial
						if got := allowN(2); got != want {
							t.Fatalf("step=%d admitted=%v want=%v", step, got, want)
						}
					}
					if allowN(3) {
						t.Fatal("oversized per-shard request accepted")
					}
				})
			})
		}
	}
}

func TestSlidingWindowNonPositiveDurationExpires(t *testing.T) {
	for _, window := range []time.Duration{0, -1, -time.Second, time.Duration(math.MinInt64), time.Duration(math.MinInt64 + 1)} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/batch=%v", window, batch), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					limiter := NewSlidingWindowRateLimiter(2, window)
					for step := range 5 {
						var allowed bool
						if batch {
							allowed = limiter.AllowN(2)
						} else {
							allowed = limiter.Allow()
						}
						if !allowed {
							t.Fatalf("nonpositive window retained prior calls: step=%d records=%d", step, len(limiter.timestamps))
						}
					}
					if limiter.AllowN(3) {
						t.Fatal("oversized batch admitted")
					}
				})
			})
		}
	}
}

// Age comparisons avoid negating a duration and model admission with integer
// nanosecond records. Real calls run in a virtual clock, never by changing state.
func FuzzSlidingWindowDurationBoundaries(f *testing.F) {
	f.Add(int64(math.MinInt64), []byte{0, 0, 0, 1, 2, 3, 8, 9})
	f.Add(int64(math.MaxInt64), []byte{0, 1, 2, 3, 8, 9, 10})
	f.Add(int64(1), []byte{0, 0, 4, 1, 8, 0, 2})
	f.Add(int64(0), []byte{0, 1, 2, 3, 0})
	f.Fuzz(func(t *testing.T, duration int64, operations []byte) {
		if len(operations) > 64 {
			operations = operations[:64]
		}
		synctest.Test(t, func(t *testing.T) {
			window := time.Duration(duration)
			limiter := NewSlidingWindowRateLimiter(4, window)
			var records []int64
			var elapsed int64
			for step, operation := range operations {
				if operation%4 == 3 {
					delta := []time.Duration{time.Nanosecond, time.Second, time.Hour}[int(operation/4)%3]
					time.Sleep(delta)
					elapsed += int64(delta)
					continue
				}
				n := int(operation/4)%8 - 1
				if operation%4 == 0 {
					n = 1
				}
				if n > 0 {
					var live []int64
					for _, timestamp := range records {
						if duration > 0 && elapsed-timestamp < duration {
							live = append(live, timestamp)
						}
					}
					records = live
				}
				want := n <= 0 || n <= 4-len(records)
				var got bool
				if operation%4 == 0 {
					got = limiter.Allow()
				} else {
					got = limiter.AllowN(n)
				}
				if got != want {
					t.Fatalf("step=%d duration=%d request=%d admitted=%v want=%v", step, duration, n, got, want)
				}
				if got && n > 0 {
					for range n {
						records = append(records, elapsed)
					}
				}
				if len(limiter.timestamps) != len(records) {
					t.Fatalf("step=%d retained=%d want=%d", step, len(limiter.timestamps), len(records))
				}
			}
		})
	})
}

func TestSlidingWindowExtremePositiveDurationRetains(t *testing.T) {
	for _, window := range []time.Duration{time.Nanosecond, time.Hour, time.Duration(math.MaxInt64)} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/batch=%v", window, batch), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					limiter := NewSlidingWindowRateLimiter(2, window)
					if batch {
						if !limiter.AllowN(2) {
							t.Fatal("initial batch rejected")
						}
					} else {
						if !limiter.Allow() || !limiter.Allow() {
							t.Fatal("initial calls rejected")
						}
					}
					if limiter.Allow() || limiter.AllowN(1) {
						t.Fatal("live records expired early")
					}
					if window == time.Duration(math.MaxInt64) {
						// synctest's absolute clock cannot advance a full MaxInt64
						// duration from year 2000; check retention after a finite step.
						time.Sleep(time.Hour)
						if limiter.Allow() || limiter.AllowN(1) {
							t.Fatal("extreme window expired after one hour")
						}
						return
					}
					time.Sleep(window)
					if !limiter.AllowN(2) {
						t.Fatal("records retained at exact boundary")
					}
				})
			})
		}
	}
}
