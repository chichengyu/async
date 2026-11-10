package group

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// A pre-canceled submission records and notifies before Go returns. Rejection
// after Wait appends an error directly and must not be counted as a notification.
func TestResultCallbackSubmissionBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls, business atomic.Int32
	notified := make(chan core.Result[int], 2)
	g := NewGroup[int](1).WithResultCallback(func(r core.Result[int]) {
		calls.Add(1)
		notified <- r
	})
	defer g.Close()
	fn := func(context.Context) (int, error) { business.Add(1); return 42, nil }
	if err := g.Go(ctx, fn); !errors.Is(err, context.Canceled) {
		t.Fatalf("submission error=%v", err)
	}
	select {
	case r := <-notified:
		if !r.Occupied || !errors.Is(r.Err, context.Canceled) {
			t.Fatalf("notification=%+v", r)
		}
	default:
		t.Fatal("canceled submission returned before notification")
	}
	if results := g.Wait(); len(results) != 1 || !errors.Is(results[0].Err, context.Canceled) {
		t.Fatalf("results=%v", results)
	}
	if err := g.Go(context.Background(), fn); !errors.Is(err, core.ErrGroupWaited) {
		t.Fatalf("late submission error=%v", err)
	}
	if calls.Load() != 1 || business.Load() != 0 {
		t.Fatalf("notifications=%d business=%d", calls.Load(), business.Load())
	}
	results := g.Wait()
	if len(results) != 2 || !errors.Is(results[1].Err, core.ErrGroupWaited) {
		t.Fatalf("late rejection not retained: %v", results)
	}
}
