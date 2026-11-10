package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Finite outcome sequences supply an independent attempt-count/error oracle.
// Backoff is disabled; this model does not cover timers or rate limit schedules.
func FuzzRetryAttemptModel(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{1, 2, 0}, uint8(7))
	f.Add([]byte{2, 1, 2}, uint8(255))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) == 0 {
			data = []byte{0}
		}
		if len(data) > 8 {
			data = data[:8]
		}
		retries := int(config%10) - 2
		maxCalls := max(0, retries) + 1
		wantCalls, lastAction := maxCalls, byte(0)
		for i := range maxCalls {
			lastAction = data[i%len(data)] % 3
			if lastAction == 0 {
				wantCalls = i + 1
				break
			}
		}
		wantErr := errors.New("model retry failure")
		for engine := range 7 {
			calls := 0
			fn := func(context.Context) (int, error) {
				action := data[calls%len(data)] % 3
				calls++
				switch action {
				case 1:
					return -calls, wantErr
				case 2:
					panic(calls)
				default:
					return calls * 17, nil
				}
			}
			ctx := context.Background()
			var value int
			var err error
			switch engine {
			case 0:
				value, err = RetryWithBackoff(ctx, fn, retries, 0, 0)
			case 1:
				value, err = RetryWithLinearBackoff(ctx, fn, retries, 0)
			case 2:
				value, err = RetryWithConfig(ctx, fn, retries, 0, 0, TimeoutOpt{PerCallTimeout: time.Hour})
			case 3:
				r := RetryWithBackoffResult(ctx, fn, retries, 0, 0)
				value, err = r.Value, r.Err
			case 4:
				value, err = New[int](ctx).Exponential().MaxRetries(retries).Backoff(0, 0).Execute(fn)
			case 5:
				value, err = New[int](ctx).Linear().MaxRetries(retries).Backoff(0, time.Second).Execute(fn)
			case 6:
				r := RetryWithLinearBackoffResult(ctx, fn, retries, 0)
				value, err = r.Value, r.Err
			}
			if calls != wantCalls {
				t.Fatalf("engine %d: calls %d want %d", engine, calls, wantCalls)
			}
			switch lastAction {
			case 0:
				if value != wantCalls*17 || err != nil {
					t.Fatalf("engine %d success: %d, %v", engine, value, err)
				}
			case 1:
				if value != 0 || !errors.Is(err, wantErr) {
					t.Fatalf("engine %d error: %d, %v", engine, value, err)
				}
			case 2:
				var pe *core.PanicError
				if value != 0 || !errors.As(err, &pe) || pe.Value != wantCalls {
					t.Fatalf("engine %d panic: %d, %v", engine, value, err)
				}
			}
		}
	})
}
