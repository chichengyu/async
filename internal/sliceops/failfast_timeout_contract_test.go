package sliceops

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// Verify the trigger error remains in the actual results even when cancellation
// rejection is recorded first. The aggregate does not promise temporal order.
func TestEachTimeoutFailFastRetainsTriggerAndRejectedResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		total, failed, first, results := NewRunner[int, int](context.Background(), []int{1, 2, 3}, Seq().FF().TO(time.Second)).Each(func(ctx context.Context, _ int) error {
			calls++
			<-ctx.Done()
			return ctx.Err()
		})
		if total != 3 || failed != 3 || calls != 1 || len(results) != 3 {
			t.Fatalf("total=%d failed=%d calls=%d results=%d", total, failed, calls, len(results))
		}
		deadline, canceled := 0, 0
		for _, result := range results {
			if !result.Occupied {
				t.Fatal("missing result")
			}
			switch {
			case errors.Is(result.Err, context.DeadlineExceeded):
				deadline++
			case errors.Is(result.Err, context.Canceled):
				canceled++
			default:
				t.Fatalf("unexpected result=%v", result.Err)
			}
		}
		if deadline != 1 || canceled != 2 {
			t.Fatalf("deadline=%d canceled=%d", deadline, canceled)
		}
		if first != results[0].Err {
			t.Fatalf("aggregate=%v first recorded=%v", first, results[0].Err)
		}
	})
}
