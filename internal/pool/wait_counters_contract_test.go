package pool

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// A saved result can precede synchronous notification completion. Successful
// waiting must include that barrier and the task's business counter cleanup.
func TestPoolCompletedWaitIncludesBusinessCounters(t *testing.T) {
	for _, entry := range []string{"wait", "context", "timeout", "close"} {
		for _, outcome := range []string{"success", "error", "panic", "goexit"} {
			t.Run(fmt.Sprintf("%s/%s", entry, outcome), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					p := NewPool[int](2).WithTimeout(0).WithMaxResults(0)
					entered, release := make(chan struct{}), make(chan struct{})
					defer func() {
						select {
						case <-release:
						default:
							close(release)
						}
						p.Close()
					}()
					var notices atomic.Int32
					p.WithResultCallback(func(core.Result[int]) {
						if notices.Add(1) == 1 {
							close(entered)
							<-release
						}
					})
					marker := errors.New("expected business failure")
					if err := p.Submit(context.Background(), func(context.Context) (int, error) {
						switch outcome {
						case "error":
							return 17, marker
						case "panic":
							panic(marker)
						case "goexit":
							runtime.Goexit()
						}
						return 17, nil
					}); err != nil {
						t.Fatal(err)
					}
					<-entered
					if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 23, nil }); err != nil {
						t.Fatal(err)
					}
					type result struct {
						values       []core.Result[int]
						ok           bool
						active, busy int
					}
					returned := make(chan result, 1)
					go func() {
						var values []core.Result[int]
						ok := true
						switch entry {
						case "wait":
							values = p.Wait()
						case "context":
							values, ok = p.WaitContext(context.Background())
						case "timeout":
							values, ok = p.WaitTimeout(time.Hour)
						case "close":
							values = p.WaitAndClose()
						}
						returned <- result{values, ok, p.Active(), p.Busy()}
					}()
					synctest.Wait()
					select {
					case <-returned:
						t.Fatal("wait bypassed result notification")
					default:
					}
					wantBusy := 1
					if outcome == "panic" || outcome == "goexit" {
						wantBusy = 0 // Busy's defer precedes exceptional result notification.
					}
					if p.Active() != 1 || p.Busy() != wantBusy {
						t.Fatalf("notification must still own counters: %v", p.Stats())
					}
					close(release)
					got := <-returned // Capture counters before any test-side scheduler barrier.
					if !got.ok || got.active != 0 || got.busy != 0 || len(got.values) != 2 || notices.Load() != 2 {
						t.Fatalf("completed=%v active=%d busy=%d results=%v notices=%d", got.ok, got.active, got.busy, got.values, notices.Load())
					}
					first, second := got.values[0], got.values[1]
					if !first.Occupied || !second.Occupied || second.Value != 23 || second.Err != nil {
						t.Fatalf("results=%v", got.values)
					}
					switch outcome {
					case "success":
						if first.Value != 17 || first.Err != nil {
							t.Fatalf("first=%v", first)
						}
					case "error":
						if first.Value != 17 || !errors.Is(first.Err, marker) {
							t.Fatalf("first=%v", first)
						}
					case "panic":
						var pe *core.PanicError
						if !errors.As(first.Err, &pe) || pe == nil || pe.Value != marker {
							t.Fatalf("first=%v", first)
						}
					case "goexit":
						if !errors.Is(first.Err, core.ErrTaskGoexit) {
							t.Fatalf("first=%v", first)
						}
					}
					wantFailures := int64(1)
					if outcome == "success" {
						wantFailures = 0
					}
					if p.FailCount() != wantFailures || p.OkCount() != 2-wantFailures {
						t.Fatalf("stats=%v", p.Stats())
					}
				})
			})
		}
	}
}
