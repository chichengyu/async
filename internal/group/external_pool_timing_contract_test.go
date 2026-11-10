package group

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

func TestGroupExternalPoolExecutionDeadlinesStartAfterQueue(t *testing.T) {
	for _, poolTimeout := range []time.Duration{0, 2 * time.Second, 5 * time.Second} {
		for _, groupTimeout := range []time.Duration{0, time.Second, 3 * time.Second} {
			t.Run(fmt.Sprintf("pool=%v/group=%v", poolTimeout, groupTimeout), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					p := pool.NewPool[int](1).WithTimeout(poolTimeout).WithMaxResults(0)
					defer p.Close()
					releaseSlot := make(chan struct{})
					defer func() {
						select {
						case <-releaseSlot:
						default:
							close(releaseSlot)
						}
					}()
					slotStarted := make(chan struct{})
					if err := p.SubmitWithCompletionTimeout(context.Background(), func(context.Context) (int, error) { close(slotStarted); <-releaseSlot; return 100, nil }, func(core.Result[int]) {}, time.Hour); err != nil {
						t.Fatal(err)
					}
					<-slotStarted
					g := NewGroup[int](1).WithPool(p).WithTimeout(time.Hour)
					defer g.Close()
					callbackStarted := make(chan context.Context, 1)
					finishNoDeadline := make(chan struct{})
					// Explicit per-item zero must clear the Group deadline while still
					// respecting the independent timeout of the injected pool.
					if err := g.GoAtWithTimeout(0, context.Background(), groupTimeout, func(ctx context.Context) (int, error) {
						callbackStarted <- ctx
						if _, ok := ctx.Deadline(); ok {
							<-ctx.Done()
						} else {
							<-finishNoDeadline
						}
						return 42, ctx.Err()
					}); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					time.Sleep(10 * time.Second)
					synctest.Wait()
					if len(callbackStarted) != 0 || g.Active() != 1 || g.Busy() != 0 || p.Pending() != 1 {
						t.Fatal("queued group callback started or disappeared")
					}
					close(releaseSlot)
					callbackCtx := <-callbackStarted
					wantTimeout := poolTimeout
					if groupTimeout > 0 && (wantTimeout == 0 || groupTimeout < wantTimeout) {
						wantTimeout = groupTimeout
					}
					d, hasDeadline := callbackCtx.Deadline()
					if hasDeadline != (wantTimeout > 0) || hasDeadline && d.Sub(time.Now()) != wantTimeout {
						t.Fatalf("callback deadline=%v has=%v now=%v want duration=%v", d, hasDeadline, time.Now(), wantTimeout)
					}
					if wantTimeout > 0 {
						time.Sleep(wantTimeout)
					} else {
						close(finishNoDeadline)
					}
					results := g.Wait()
					var wantErr error
					var wantFailed int64
					if wantTimeout > 0 {
						wantErr, wantFailed = context.DeadlineExceeded, 1
					}
					if len(results) != 1 || !results[0].Occupied || results[0].Value != 42 || !errors.Is(results[0].Err, wantErr) || g.FailCount() != wantFailed || g.OkCount() != 1-wantFailed || g.Active() != 0 || g.Busy() != 0 {
						t.Fatalf("Group result/counts: %+v failed=%d ok=%d", results, g.FailCount(), g.OkCount())
					}
					// The Group owns neither Wait nor Reset of the pool. It remains open.
					if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 7, nil }); err != nil {
						t.Fatalf("group wait ended external pool: %v", err)
					}
					poolResults := p.Wait()
					if len(poolResults) != 3 || poolResults[0].Value != 100 || poolResults[1].Value != 0 || poolResults[1].Err != nil || poolResults[2].Value != 7 || p.FailCount() != 0 || p.OkCount() != 3 {
						t.Fatalf("pool wrapper outcome/counts: %+v failed=%d ok=%d", poolResults, p.FailCount(), p.OkCount())
					}
				})
			})
		}
	}
}

func TestGroupBusinessFailureDoesNotTriggerExternalPoolFailFast(t *testing.T) {
	for _, outcome := range []string{"error", "panic", "timeout"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, poolCtx := pool.NewPool[int](1).WithTimeout(0).WithMaxResults(0).WithFailFast(context.Background())
				defer p.Close()
				g, groupCtx := NewGroup[int](1).WithPool(p).WithTimeout(time.Second).WithFailFast(poolCtx)
				defer g.Close()
				wantErr := errors.New("business failure")
				var callbacks atomic.Int32
				g.WithResultCallback(func(core.Result[int]) { callbacks.Add(1) })
				if err := g.Go(groupCtx, func(ctx context.Context) (int, error) {
					switch outcome {
					case "error":
						return 42, wantErr
					case "panic":
						panic("business panic")
					default:
						<-ctx.Done()
						return 42, ctx.Err()
					}
				}); err != nil {
					t.Fatal(err)
				}
				if outcome == "timeout" {
					time.Sleep(time.Second)
				}
				synctest.Wait()
				// Check before Wait: Wait itself cancels the Group context and
				// would otherwise mask a missing business FailFast notification.
				if !errors.Is(groupCtx.Err(), context.Canceled) || poolCtx.Err() != nil {
					t.Fatalf("business FailFast before wait: group=%v pool=%v", groupCtx.Err(), poolCtx.Err())
				}
				results := g.Wait()
				if len(results) != 1 || results[0].Err == nil || g.FailCount() != 1 || g.OkCount() != 0 || callbacks.Load() != 1 || !errors.Is(groupCtx.Err(), context.Canceled) || poolCtx.Err() != nil {
					t.Fatalf("failure isolation: results=%+v callbacks=%d group=%v pool=%v", results, callbacks.Load(), groupCtx.Err(), poolCtx.Err())
				}
				switch outcome {
				case "error":
					if results[0].Value != 42 || !errors.Is(results[0].Err, wantErr) {
						t.Fatal("business error lost")
					}
				case "panic":
					var pe *core.PanicError
					if results[0].Value != 0 || !errors.As(results[0].Err, &pe) || pe.Value != "business panic" {
						t.Fatal("business panic lost")
					}
				case "timeout":
					if results[0].Value != 42 || !errors.Is(results[0].Err, context.DeadlineExceeded) {
						t.Fatal("business deadline lost")
					}
				}
				if err := p.Submit(poolCtx, func(context.Context) (int, error) { return 7, nil }); err != nil {
					t.Fatal(err)
				}
				r := p.Wait()
				if len(r) != 2 || r[0].Err != nil || r[0].Value != 0 || r[1].Err != nil || r[1].Value != 7 || p.FailCount() != 0 || p.OkCount() != 2 {
					t.Fatalf("pool wrapper/marker=%+v failures=%d", r, p.FailCount())
				}
			})
		})
	}
}

func TestGroupSubmitTimeoutDoesNotCoverExternalPoolDispatch(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%v", indexed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := pool.NewPool[int](1).WithTimeout(0).WithMaxResults(0).WithMaxPending(1).WithOverflow(core.OverflowBlock).WithSubmitTimeout(3 * time.Second)
				defer p.Close()
				release, started := make(chan struct{}), make(chan struct{})
				defer func() {
					select {
					case <-release:
					default:
						close(release)
					}
				}()
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 100, nil }); err != nil {
					t.Fatal(err)
				}
				<-started
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 101, nil }); err != nil {
					t.Fatal(err)
				}
				g, groupCtx := NewGroup[int](1).WithPool(p).WithTimeout(time.Second).WithSubmitTimeout(time.Second).WithFailFast(context.Background())
				defer g.Close()
				var calls atomic.Int32
				recorded := make(chan core.Result[int], 1)
				g.WithResultCallback(func(r core.Result[int]) { recorded <- r })
				fn := func(context.Context) (int, error) { calls.Add(1); return 42, nil }
				begin := time.Now()
				var submitErr error
				if indexed {
					submitErr = g.GoAt(0, groupCtx, fn)
				} else {
					submitErr = g.Go(groupCtx, fn)
				}
				if submitErr != nil || time.Since(begin) != 0 {
					t.Fatalf("group queue admission: %v elapsed=%v", submitErr, time.Since(begin))
				}
				synctest.Wait()
				time.Sleep(2 * time.Second)
				synctest.Wait()
				if len(recorded) != 0 || calls.Load() != 0 || g.Active() != 1 || g.Busy() != 0 || groupCtx.Err() != nil {
					t.Fatal("Group timeout incorrectly ended external-pool dispatch wait")
				}
				time.Sleep(time.Second)
				r := <-recorded
				synctest.Wait()
				if !r.Occupied || !errors.Is(r.Err, core.ErrSubmitTimeout) || r.Value != 0 || calls.Load() != 0 || g.FailCount() != 1 || g.Active() != 0 || groupCtx.Err() != nil || p.Pending() != 1 {
					t.Fatalf("external dispatch rejection: %+v calls=%d ctx=%v", r, calls.Load(), groupCtx.Err())
				}
				results := g.Wait()
				if len(results) != 1 || !errors.Is(results[0].Err, core.ErrSubmitTimeout) {
					t.Fatalf("Group lost pool dispatch rejection: %+v", results)
				}
				close(release)
				poolResults := p.Wait()
				if len(poolResults) != 3 || p.OkCount() != 2 || p.FailCount() != 1 || !errors.Is(poolResults[2].Err, core.ErrSubmitTimeout) {
					t.Fatalf("pool dispatch rejection stats/results: %+v", poolResults)
				}
			})
		})
	}
}
