package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Track accepted permits separately from the limiter. Adjustment may change the
// target, but cannot create room while outstanding work still exceeds it.
func FuzzLimiterPermitState(f *testing.F) {
	f.Add([]byte{1, 31, 0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 1, 1, 0, 2})
	f.Add([]byte{255, 255, 2, 0, 1, 3, 0, 0, 2, 1})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		if len(data) > 514 {
			data = data[:514]
		}
		minimum := 1 + int(data[0]%8)
		maximum := minimum + int(data[1]%32)
		adaptive := NewAdaptiveRateLimiter(minimum, maximum)
		defer adaptive.base.Close()
		adaptive.base.WithStrategy(Reject)
		held := 0

		// Zero rate isolates fractional consumption from elapsed wall-clock time.
		capacity := float64(1+data[0]%32) / 2
		bucket := NewTokenBucket(0, capacity)
		tokens := capacity
		limit := 1 + int(data[1]%32)
		window := NewSlidingWindowRateLimiter(limit, time.Hour)
		accepted := 0
		ctx := context.Background()
		for step, operation := range data[2:] {
			size := adaptive.base.Size()
			switch operation % 4 {
			case 0:
				err := adaptive.Acquire(ctx)
				if held < size {
					if err != nil {
						t.Fatalf("step %d: room available, Acquire=%v", step, err)
					}
					held++
				} else if !errors.Is(err, core.ErrRateLimitExceeded) {
					t.Fatalf("step %d: held=%d size=%d Acquire=%v", step, held, size, err)
				}
			case 1:
				if held > 0 {
					adaptive.Release()
					held--
				}
			case 2:
				adaptive.RecordOk()
			case 3:
				adaptive.RecordFailure()
			}
			size = adaptive.base.Size()
			if size < minimum || size > maximum {
				t.Fatalf("step %d: target %d outside [%d,%d]", step, size, minimum, maximum)
			}
			if got, want := adaptive.base.Available(), max(size-held, 0); got != want {
				t.Fatalf("step %d: target=%d held=%d available=%d want=%d", step, size, held, got, want)
			}

			request := float64(int(operation%24)-2) / 2
			wantBucket := request <= 0 || request <= tokens
			if got := bucket.AllowN(request); got != wantBucket {
				t.Fatalf("step %d: bucket request=%v tokens=%v got=%v", step, request, tokens, got)
			}
			if wantBucket && request > 0 {
				tokens -= request
			}
			n := int(operation%24) - 2
			wantWindow := n <= 0 || accepted+n <= limit
			if got := window.AllowN(n); got != wantWindow {
				t.Fatalf("step %d: window request=%d accepted=%d limit=%d got=%v", step, n, accepted, limit, got)
			}
			if wantWindow && n > 0 {
				accepted += n
			}
		}
	})
}
