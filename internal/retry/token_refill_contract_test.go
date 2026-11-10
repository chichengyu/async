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
)

// Public Execute must combine attempt-counting, refill during linear backoff
// and per-shard routing. The oracle uses integer quarter-tokens and no limiter.
func TestRetryTokenRefillDuringBackoff(t *testing.T) {
	for _, rate := range []int{1, 2} {
		for _, shardCount := range []int{1, 2} {
			for _, gap := range []int{1, 2, 4} {
				for _, succeed := range []bool{false, true} {
					t.Run(fmt.Sprintf("rate=%d/shards=%d/gap=%d/success=%v", rate, shardCount, gap, succeed), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							n := max(shardCount, 1)
							balance, last := make([]int, n), make([]int, n)
							for i := range balance {
								balance[i] = 4
							}
							var wantTimes []time.Duration
							wantSuccess, wantDenied, finalAttempt := false, false, 4
							for attempt := 0; attempt < 5; attempt++ {
								index, now := attempt%n, attempt*gap
								balance[index] = min(4, balance[index]+(now-last[index])*rate)
								last[index] = now
								wantDenied = balance[index] < 4
								if wantDenied {
									continue
								}
								balance[index] -= 4
								wantTimes = append(wantTimes, time.Duration(now)*250*time.Millisecond)
								if succeed && len(wantTimes) == 3 {
									wantSuccess, finalAttempt = true, attempt
									break
								}
							}
							failure := errors.New("business failure")
							chain := New[int](context.Background()).Linear().MaxRetries(4).Backoff(time.Duration(gap)*250*time.Millisecond, 0).TokenBucket().Rate(rate).Capacity(1).Shards(shardCount)
							start := time.Now()
							var times []time.Duration
							value, err := chain.Execute(func(context.Context) (int, error) {
								times = append(times, time.Since(start))
								if succeed && len(times) == 3 {
									return 42, nil
								}
								return -99, failure
							})
							if !slices.Equal(times, wantTimes) || time.Since(start) != time.Duration(finalAttempt*gap)*250*time.Millisecond {
								t.Fatalf("callback times=%v want=%v elapsed=%v final attempt=%d", times, wantTimes, time.Since(start), finalAttempt)
							}
							switch {
							case wantSuccess:
								if value != 42 || err != nil {
									t.Fatalf("success value=%d err=%v", value, err)
								}
							case wantDenied:
								if value != 0 || err == nil || errors.Is(err, failure) || !strings.Contains(err.Error(), "rate limit exceeded") {
									t.Fatalf("final rejection value=%d err=%v", value, err)
								}
							default:
								if value != 0 || !errors.Is(err, failure) {
									t.Fatalf("final business failure value=%d err=%v", value, err)
								}
							}
						})
					})
				}
			}
		}
	}
}
