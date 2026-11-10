package retry

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Virtual time avoids wall-clock tolerances and real backoff sleeps. The final
// failed attempt must return immediately; only gaps between attempts may wait.
func TestBindRetryWorkerBackoffSchedule(t *testing.T) {
	for _, tc := range []struct {
		name         string
		initial, cap time.Duration
		want         []time.Duration
	}{
		{"capped", 2 * time.Second, 5 * time.Second, []time.Duration{0, 2 * time.Second, 6 * time.Second, 11 * time.Second}},
		{"uncapped", 2 * time.Second, 0, []time.Duration{0, 2 * time.Second, 6 * time.Second, 14 * time.Second}},
		{"initial-exceeds-cap", 10 * time.Second, 3 * time.Second, []time.Duration{0, 3 * time.Second, 6 * time.Second, 9 * time.Second}},
		{"zero", 0, time.Second, []time.Duration{0, 0, 0, 0}},
		{"negative", -time.Second, time.Second, []time.Duration{0, 0, 0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				var times []time.Duration
				backend := contractWorkerBackend{submit: func(context.Context, func(context.Context) error) error {
					times = append(times, time.Since(start))
					return core.ErrSubmitTimeout
				}}
				err := BindRetryToWorker(context.Background(), backend, func(context.Context) error {
					t.Fatal("submission helper executed business function")
					return nil
				}, 3, tc.initial, tc.cap)
				if !errors.Is(err, core.ErrSubmitTimeout) || len(times) != len(tc.want) {
					t.Fatalf("err=%v times=%v", err, times)
				}
				for i, want := range tc.want {
					if times[i] != want {
						t.Fatalf("attempt %d at %v, want %v", i, times[i], want)
					}
				}
				if elapsed := time.Since(start); elapsed != tc.want[len(tc.want)-1] {
					t.Fatalf("waited after exhaustion: elapsed=%v", elapsed)
				}
			})
		})
	}
}

func TestBindRetryWorkerCancelBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { time.Sleep(time.Second); cancel() }()
		calls := 0
		backend := contractWorkerBackend{submit: func(context.Context, func(context.Context) error) error {
			calls++
			return core.ErrSubmitTimeout
		}}
		start := time.Now()
		err := BindRetryToWorker(ctx, backend, nil, 3, time.Hour, time.Hour)
		if !errors.Is(err, context.Canceled) || calls != 1 || time.Since(start) != time.Second {
			t.Fatalf("err=%v calls=%d elapsed=%v", err, calls, time.Since(start))
		}
	})
}

func TestBindRetryWorkerCannotInterruptBackend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		entered, release := make(chan struct{}), make(chan struct{})
		returned := make(chan error, 1)
		backend := contractWorkerBackend{submit: func(context.Context, func(context.Context) error) error {
			close(entered)
			<-release
			return nil
		}}
		go func() { returned <- BindRetryToWorker(ctx, backend, nil, 3, 0, 0) }()
		<-entered
		cancel()
		synctest.Wait()
		select {
		case err := <-returned:
			t.Fatalf("returned before backend completion: %v", err)
		default:
		}
		close(release)
		if err := <-returned; err != nil {
			t.Fatalf("backend success overridden by cancellation: %v", err)
		}
	})
}
