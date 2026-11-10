package retry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/ratelimit"
)

// Non-refilling token buckets and hour-long windows make the attempt budget
// independent of wall-clock scheduling. Limiter denials consume attempts but
// cannot invoke the business callback or preserve an earlier business error.
func FuzzRetryLimiterAttemptBudget(f *testing.F) {
	f.Add([]byte{1}, uint8(15), uint8(0), uint8(3))
	f.Add([]byte{1, 2, 0}, uint8(8), uint8(4), uint8(2))
	f.Add([]byte{2}, uint8(7), uint8(2), uint8(1))
	f.Add([]byte{0}, uint8(0), uint8(0), uint8(0))
	f.Add([]byte{1}, uint8(0), uint8(0), uint8(0)) // Business error on the final allowed attempt.
	f.Add([]byte{2}, uint8(0), uint8(0), uint8(0)) // Panic on the final allowed attempt.
	f.Fuzz(func(t *testing.T, data []byte, attemptsByte, limitByte, shardsByte uint8) {
		if len(data) == 0 {
			data = []byte{1}
		}
		attempts, limit, shards := 1+int(attemptsByte%16), 1+int(limitByte%5), 1+int(shardsByte%4)
		businessErr := errors.New("limiter budget business failure")
		for engine := range 4 {
			n := 1
			if engine%2 == 1 {
				n = shards
			}
			budget := make([]int, n)
			for i := range budget {
				budget[i] = limit
				if engine == 3 {
					budget[i] = max(1, limit/n)
					if limit >= n && i < limit%n {
						budget[i]++
					}
				}
			}
			wantCalls, wantKind, wantValue := 0, 3, 0
			for attempt := range attempts {
				idx := attempt % n
				if budget[idx] == 0 {
					wantKind = 3
					continue
				}
				budget[idx]--
				wantKind = int(data[wantCalls%len(data)] % 3)
				wantCalls++
				if wantKind == 0 {
					wantValue = wantCalls * 17
					break
				}
			}
			var tb *ratelimit.TokenBucket
			var stb *ratelimit.ShardedTokenBucket
			switch engine {
			case 0:
				tb = ratelimit.NewTokenBucket(0, float64(limit))
			case 1:
				stb = ratelimit.NewShardedTokenBucket(n, 0, float64(limit))
			}
			calls := 0
			fn := func(context.Context) (int, error) {
				kind := data[calls%len(data)] % 3
				calls++
				if kind == 2 {
					panic(calls)
				}
				if kind == 1 {
					return -calls, businessErr
				}
				return calls * 17, nil
			}
			chain := New[int](context.Background()).MaxRetries(attempts-1).Backoff(0, 0)
			var value int
			var err error
			if engine >= 2 {
				chain.SlidingWindow().Limit(limit).Window(time.Hour)
				if engine == 3 {
					chain.Shards(n)
				}
				value, err = chain.Execute(fn)
			} else {
				value, err = chain.executeCore(fn, nil, tb, nil, nil, nil, stb, nil, nil)
			}
			if calls != wantCalls || value != wantValue {
				t.Fatalf("engine %d calls/value=%d/%d, want %d/%d", engine, calls, value, wantCalls, wantValue)
			}
			var pe *core.PanicError
			switch wantKind {
			case 0:
				if err != nil {
					t.Fatal(err)
				}
			case 1:
				if !errors.Is(err, businessErr) {
					t.Fatalf("engine %d lost business error: %v", engine, err)
				}
			case 2:
				if !errors.As(err, &pe) || pe.Value != wantCalls {
					t.Fatalf("engine %d wrong panic: %v", engine, err)
				}
			case 3:
				if err == nil || !strings.Contains(err.Error(), "rate limit exceeded") || errors.Is(err, businessErr) || errors.As(err, &pe) {
					t.Fatalf("engine %d wrong rejection error: %v", engine, err)
				}
			}
		}
	})
}
