package pool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// A blocked submission owns a registered task even before it reaches taskCh.
// Its synchronous rejection notifications can outlive every worker, so the
// first timed-close worker signal cannot stand in for complete cleanup.
func TestTimedCloseWorkerExitBeforeSubmissionNotifications(t *testing.T) {
	for _, pendingLimit := range []bool{false, true} {
		for _, notificationPanic := range []bool{false, true} {
			t.Run(fmt.Sprintf("pendingLimit=%v/notificationPanic=%v", pendingLimit, notificationPanic), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					p := NewPool[int](1).WithTimeout(0).WithMaxResults(0).WithStreaming(8)
					if pendingLimit {
						p.WithMaxPending(1).WithOverflow(core.OverflowBlock)
					}
					parent, parentCancel := context.WithCancel(context.Background())
					defer parentCancel()
					_, poolCtx := p.WithContext(parent)
					started, releaseWork := make(chan context.Context, 1), make(chan struct{})
					observerStarted, releaseObserver := make(chan struct{}), make(chan struct{})
					completionStarted, releaseCompletion := make(chan struct{}), make(chan struct{})
					defer func() {
						for _, ch := range []chan struct{}{releaseWork, releaseObserver, releaseCompletion} {
							select {
							case <-ch:
							default:
								close(ch)
							}
						}
					}()
					var notices, business atomic.Int32
					p.WithResultCallback(func(r core.Result[int]) {
						notices.Add(1)
						if errors.Is(r.Err, core.ErrPoolClosed) {
							close(observerStarted)
							<-releaseObserver
							if notificationPanic {
								panic("rejection observer")
							}
						}
					})
					if err := p.Submit(poolCtx, func(ctx context.Context) (int, error) {
						business.Add(1)
						started <- ctx
						<-releaseWork
						return 10, ctx.Err()
					}); err != nil {
						t.Fatal(err)
					}
					workCtx := <-started
					queued := 2
					if pendingLimit {
						queued = 1
					}
					for i := 0; i < queued; i++ {
						if err := p.Submit(poolCtx, func(ctx context.Context) (int, error) { business.Add(1); return 20, ctx.Err() }); err != nil {
							t.Fatal(err)
						}
					}
					submitted := make(chan error, 1)
					go func() {
						submitted <- p.SubmitWithCompletion(poolCtx, func(context.Context) (int, error) { business.Add(1); return 99, nil }, func(r core.Result[int]) {
							if !r.Occupied || !errors.Is(r.Err, core.ErrPoolClosed) {
								t.Errorf("rejected completion=%+v", r)
							}
							close(completionStarted)
							<-releaseCompletion
							if notificationPanic {
								panic("rejection completion")
							}
						})
					}()
					synctest.Wait()
					if p.TotalCount() != int64(queued+2) {
						t.Fatalf("blocked submission not registered: %v", p.Stats())
					}
					ok, workersDone := p.CloseAndWaitTimeout(time.Second)
					if ok || workersDone == nil {
						t.Fatalf("first close complete=%v signal=%v", ok, workersDone)
					}
					if workCtx.Err() != nil || poolCtx.Err() != nil || parent.Err() != nil {
						t.Fatal("Close canceled live business or parent context")
					}
					<-observerStarted
					select {
					case <-workersDone:
						t.Fatal("worker signal preceded running business")
					default:
					}
					close(releaseWork)
					<-workersDone
					synctest.Wait()
					// A channel-blocked sender owns a pending slot until rejection
					// notifications return. A MaxPending waiter has none yet.
					wantPending := 1
					if pendingLimit {
						wantPending = 0
					}
					if business.Load() != int32(queued+1) || p.OkCount() != int64(queued+1) || p.FailCount() != 0 || p.Active() != 0 || p.Busy() != 0 || p.Pending() != wantPending {
						t.Fatalf("workers done with pending notification: calls=%d stats=%+v", business.Load(), p.Stats())
					}
					stream := p.StreamResults()
					var runningResults, queuedResults, rejectedResults int
					for i := 0; i < queued+2; i++ {
						select {
						case r, open := <-stream:
							if !open || !r.Occupied {
								t.Fatalf("stream result=%+v open=%v", r, open)
							}
							switch {
							case r.Err == nil && r.Value == 10:
								runningResults++
							case r.Err == nil && r.Value == 20:
								queuedResults++
							case errors.Is(r.Err, core.ErrPoolClosed) && r.Value == 0:
								rejectedResults++
							default:
								t.Fatalf("unexpected stream result=%+v", r)
							}
						default:
							t.Fatal("stored outcome missing from stream")
						}
					}
					if runningResults != 1 || queuedResults != queued || rejectedResults != 1 {
						t.Fatalf("stream counts: running=%d queued=%d rejected=%d", runningResults, queuedResults, rejectedResults)
					}
					select {
					case _, open := <-stream:
						t.Fatalf("stream closed before submission notification: open=%v", open)
					default:
					}
					ok, cleanupDone := p.CloseAndWaitTimeout(time.Second)
					if ok || cleanupDone == nil || cleanupDone == workersDone {
						t.Fatalf("repeated close complete=%v signal=%v", ok, cleanupDone)
					}
					closed := make(chan struct{})
					go func() { p.Close(); close(closed) }()
					close(releaseObserver)
					<-completionStarted
					synctest.Wait()
					if p.Pending() != wantPending || p.FailCount() != 0 {
						t.Fatalf("completion still owns rejection cleanup: %+v", p.Stats())
					}
					for _, signal := range []<-chan struct{}{cleanupDone, closed} {
						select {
						case <-signal:
							t.Fatal("cleanup bypassed completion")
						default:
						}
					}
					select {
					case <-submitted:
						t.Fatal("submission returned before completion")
					default:
					}
					close(releaseCompletion)
					if err := <-submitted; !errors.Is(err, core.ErrPoolClosed) {
						t.Fatalf("submission=%v", err)
					}
					<-cleanupDone
					<-closed
					if _, open := <-stream; open {
						t.Fatal("stream still open after cleanup")
					}
					if p.FailCount() != 1 || p.Pending() != 0 || notices.Load() != int32(queued+2) {
						t.Fatalf("final failures=%d notices=%d", p.FailCount(), notices.Load())
					}
					results := p.Wait()
					if len(results) != queued+2 || results[0].Value != 10 || results[0].Err != nil || !errors.Is(results[queued+1].Err, core.ErrPoolClosed) {
						t.Fatalf("final results=%v", results)
					}
					if poolCtx.Err() != context.Canceled || parent.Err() != nil {
						t.Fatal("Wait context cleanup did not preserve caller parent")
					}
					if _, err := p.Reset(); err != nil {
						t.Fatal(err)
					}
					if p.TotalCount() != 0 || p.FailCount() != 0 || p.OkCount() != 0 || p.StreamResults() == stream {
						t.Fatal("Reset retained batch state")
					}
					p.WithResultCallback(nil)
					if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 7, nil }); err != nil {
						t.Fatal(err)
					}
					if next := p.WaitAndClose(); len(next) != 1 || next[0].Value != 7 || next[0].Err != nil {
						t.Fatalf("reset results=%v", next)
					}
				})
			})
		}
	}
}
