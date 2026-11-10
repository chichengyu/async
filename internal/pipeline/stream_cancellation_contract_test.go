package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/chichengyu/async/internal/core"
)

// Fill the 4096-entry input buffer while eight callbacks are held at a
// barrier. Cancellation must unblock submission, preserve errors from already
// running callbacks, and wait until those callbacks actually return.
func TestExecuteStreamCanceledBackpressureContract(t *testing.T) {
	for _, panics := range []bool{false, true} {
		name := "business"
		if panics {
			name = "panic"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const count, workers = 4200, 8
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				started := make(chan int, workers)
				release := make(chan struct{})
				var downstream atomic.Int64
				businessErr := errors.New("running item failed after cancellation")
				type outcome struct {
					results []core.Result[int]
					err     error
				}
				done := make(chan outcome, 1)
				items := make([]int, count)
				for i := range items {
					items[i] = i
				}
				go func() {
					results, err := ExecuteStream(ctx, []Stage[int]{
						{Name: "first", Concurrency: workers},
						{Name: "second", Concurrency: workers},
					}, items, func(_ context.Context, stage string, value int) (int, error) {
						if stage == "second" {
							downstream.Add(1)
							return value, nil
						}
						started <- value
						<-release
						if panics {
							panic("running item panic after cancellation")
						}
						return value, businessErr
					})
					done <- outcome{results, err}
				}()
				running := make(map[int]bool, workers)
				for i := 0; i < workers; i++ {
					running[<-started] = true
				}
				synctest.Wait() // callbacks and input submission are blocked
				cancel()
				synctest.Wait()
				select {
				case <-done:
					t.Error("execution returned before running callbacks completed")
				default:
				}
				close(release)
				got := <-done
				if len(got.results) != count || !errors.Is(got.err, context.Canceled) {
					t.Fatalf("cancellation length=%d error=%v", len(got.results), got.err)
				}
				occupied := 0
				for i, r := range got.results {
					if !running[i] {
						if r.Occupied || r.Err != nil || r.Value != 0 {
							t.Fatalf("unstarted slot %d recorded: %+v", i, r)
						}
						continue
					}
					occupied++
					if !r.Occupied {
						t.Fatalf("running failure lost at index %d", i)
					}
					if panics {
						var pe *core.PanicError
						if !errors.As(r.Err, &pe) || r.Value != 0 {
							t.Fatalf("panic slot %d: %+v", i, r)
						}
					} else if !errors.Is(r.Err, businessErr) || r.Value != i {
						t.Fatalf("business slot %d: %+v", i, r)
					}
				}
				if occupied != workers || downstream.Load() != 0 {
					t.Fatalf("occupied=%d downstream=%d", occupied, downstream.Load())
				}
			})
		})
	}
}

func TestExecuteStreamCanceledEmptyBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name   string
		stages []Stage[int]
		items  []int
	}{
		{name: "no-stages", items: []int{0, 7}},
		{name: "no-items", stages: []Stage[int]{{Name: "unused", Concurrency: 1}}},
		{name: "both-empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			results, err := ExecuteStream(ctx, tc.stages, tc.items, func(context.Context, string, int) (int, error) {
				t.Error("empty boundary invoked callback")
				return 0, nil
			})
			if err != nil || len(results) != len(tc.items) {
				t.Fatalf("empty boundary length=%d error=%v", len(results), err)
			}
			for i, r := range results {
				if !r.Occupied || r.Err != nil || r.Value != tc.items[i] {
					t.Fatalf("passthrough slot %d: %+v", i, r)
				}
			}
		})
	}
}
