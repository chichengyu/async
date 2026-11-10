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

// A saved result does not mean the synchronous notification or task cleanup has
// finished. Exercise the public lifecycle with both execution backends.
func TestGroupBoundedWaitIncludesResultNotification(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			for _, waitContext := range []bool{false, true} {
				t.Run(fmt.Sprintf("external=%v/failed=%v/context=%v", external, failed, waitContext), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						p := pool.NewPool[int](1).WithTimeout(0)
						defer p.Close()
						started, release := make(chan struct{}), make(chan struct{})
						defer func() {
							select {
							case <-release:
							default:
								close(release)
							}
						}()
						var notifications atomic.Int32
						g := NewGroup[int](1).WithTimeout(0).WithStreaming(4).WithResultCallback(func(core.Result[int]) {
							if notifications.Add(1) == 1 {
								close(started)
								<-release
							}
						})
						if external {
							g.WithPool(p)
						}
						var wantErr error
						if failed {
							wantErr = errors.New("business failure")
						}
						if err := g.GoAt(3, context.Background(), func(context.Context) (int, error) { return 42, wantErr }); err != nil {
							t.Fatal(err)
						}
						<-started
						var partial []core.Result[int]
						var ok bool
						if waitContext {
							ctx, cancel := context.WithCancel(context.Background())
							cancel()
							partial, ok = g.WaitContext(ctx)
						} else {
							partial, ok = g.WaitTimeout(time.Second)
						}
						if ok || len(partial) != 4 || !partial[3].Occupied || partial[3].Value != 42 || !errors.Is(partial[3].Err, wantErr) {
							t.Fatalf("partial=%v complete=%v", partial, ok)
						}
						for i := 0; i < 3; i++ {
							if partial[i].Occupied {
								t.Fatalf("sparse slot %d occupied", i)
							}
						}
						if g.Active() != 1 || g.Busy() != 1 || g.OkCount() != 0 || notifications.Load() != 1 {
							t.Fatalf("callback barrier: active=%d busy=%d ok=%d notifications=%d", g.Active(), g.Busy(), g.OkCount(), notifications.Load())
						}
						stream := g.StreamResults()
						if r := <-stream; !r.Occupied || r.Value != 42 || !errors.Is(r.Err, wantErr) {
							t.Fatalf("stream=%+v", r)
						}
						select {
						case _, open := <-stream:
							t.Fatalf("stream ended before notification: open=%v", open)
						default:
						}
						if _, err := g.Reset(); err == nil {
							t.Fatal("Reset accepted blocked notification")
						}
						partial[3].Value = -1
						if err := g.Go(context.Background(), func(context.Context) (int, error) { t.Error("late task ran"); return 99, nil }); !errors.Is(err, core.ErrGroupWaiting) {
							t.Fatalf("late submission=%v", err)
						}
						again, ok := g.WaitTimeout(0)
						if ok || len(again) != 5 || again[3].Value != 42 || !errors.Is(again[4].Err, core.ErrGroupWaiting) || notifications.Load() != 1 {
							t.Fatalf("repeat snapshot=%v complete=%v notifications=%d", again, ok, notifications.Load())
						}
						done := make(chan []core.Result[int], 1)
						go func() { done <- g.Wait() }()
						synctest.Wait()
						select {
						case <-done:
							t.Fatal("Wait returned before notification")
						default:
						}
						close(release)
						final := <-done
						wantFailures := int64(1)
						if failed {
							wantFailures++
						}
						if len(final) != 5 || final[3].Value != 42 || !errors.Is(final[3].Err, wantErr) || g.TotalCount() != 2 || g.FailCount() != wantFailures || g.OkCount() != 2-wantFailures || g.Active() != 0 || g.Busy() != 0 {
							t.Fatalf("final=%v stats=%+v", final, g.Stats())
						}
						if _, open := <-stream; open {
							t.Fatal("stream did not close after final Wait")
						}
						if len(partial) != 4 || partial[3].Value != -1 {
							t.Fatal("saved slice changed old snapshot")
						}
						if _, err := g.Reset(); err != nil {
							t.Fatal(err)
						}
						if g.TotalCount() != 0 || g.FailCount() != 0 || g.OkCount() != 0 || stream == g.StreamResults() {
							t.Fatal("Reset retained old state")
						}
						if err := g.Go(context.Background(), func(context.Context) (int, error) { return 9, nil }); err != nil {
							t.Fatal(err)
						}
						if next := g.Wait(); len(next) != 1 || next[0].Value != 9 || next[0].Err != nil || notifications.Load() != 2 {
							t.Fatalf("reset results=%v notifications=%d", next, notifications.Load())
						}
						if external {
							if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 7, nil }); err != nil {
								t.Fatalf("Group reset closed shared pool: %v", err)
							}
						}
						_ = p.Wait()
					})
				})
			}
		}
	}
}

func TestNoResultBoundedWaitCountsSparseSlotsDuringNotification(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, waitContext := range []bool{false, true} {
			t.Run(fmt.Sprintf("failed=%v/context=%v", failed, waitContext), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					started, release := make(chan struct{}), make(chan struct{})
					defer func() {
						select {
						case <-release:
						default:
							close(release)
						}
					}()
					nr := NewNoResult(1).WithTimeout(0).WithResultCallback(func(core.Result[struct{}]) { close(started); <-release })
					var wantErr error
					if failed {
						wantErr = errors.New("business failure")
					}
					if err := nr.GoAt(3, context.Background(), func(context.Context) error { return wantErr }); err != nil {
						t.Fatal(err)
					}
					<-started
					var completed int64
					var ok bool
					if waitContext {
						ctx, cancel := context.WithCancel(context.Background())
						cancel()
						completed, ok = nr.WaitContext(ctx)
					} else {
						completed, ok = nr.WaitTimeout(time.Second)
					}
					if completed != 4 || ok || nr.TotalCount() != 1 || nr.OkCount() != 0 || nr.Active() != 1 {
						t.Fatalf("completed=%d ok=%v stats=%+v", completed, ok, nr.Stats())
					}
					if _, err := nr.Reset(); err == nil {
						t.Fatal("Reset accepted unfinished notification")
					}
					close(release)
					nr.Wait()
					wantFailures := int64(0)
					if failed {
						wantFailures = 1
					}
					if nr.FailCount() != wantFailures || nr.OkCount() != 1-wantFailures || nr.Active() != 0 || !errors.Is(nr.FirstError(), wantErr) {
						t.Fatalf("final stats=%+v error=%v", nr.Stats(), nr.FirstError())
					}
					if completed, ok = nr.WaitContext(context.Background()); completed != 4 || !ok {
						t.Fatalf("final completed=%d ok=%v", completed, ok)
					}
					if _, err := nr.Reset(); err != nil {
						t.Fatal(err)
					}
					nr.WithResultCallback(nil)
					if err := nr.Go(context.Background(), func(context.Context) error { return nil }); err != nil {
						t.Fatal(err)
					}
					nr.Wait()
					if nr.TotalCount() != 1 || nr.OkCount() != 1 || nr.FailCount() != 0 {
						t.Fatalf("reset stats=%+v", nr.Stats())
					}
				})
			})
		}
	}
}
