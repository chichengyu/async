package group

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
	"github.com/chichengyu/async/internal/pool"
)

// Task completion must balance the accepted task even when it does not return.
func TestGroupBusinessGoexitCompletesAcceptedTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := NewGroup[int](1).WithTimeout(0)
		exited := make(chan struct{})
		if err := g.Go(context.Background(), func(context.Context) (int, error) {
			defer close(exited)
			runtime.Goexit()
			return 0, nil
		}); err != nil {
			t.Fatal(err)
		}
		<-exited
		synctest.Wait()
		if g.workerCount.Load() != 1 {
			t.Errorf("Goexit lost worker capacity: %d", g.workerCount.Load())
		}
		results, ok := g.WaitTimeout(time.Second)
		if !ok || g.Active() != 0 || g.Busy() != 0 || len(results) != 1 || !errors.Is(results[0].Err, core.ErrTaskGoexit) || results[0].IsPanic() || g.FailCount() != 1 {
			t.Errorf("Goexit never completed: ok=%t active=%d busy=%d fail=%d results=%+v", ok, g.Active(), g.Busy(), g.FailCount(), results)
		}
		if !ok {
			// Test-only repair lets the original wait goroutine exit after recording
			// the behavioral failure; production receives no such repair path.
			g.active.Add(-1)
			g.busy.Add(-1)
			g.wg.Done()
		}
		g.Wait()
	})
}

func TestGroupGoexitCapacityRoutingAndFailFast(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, indexed := range []bool{false, true} {
			for _, failFast := range []bool{false, true} {
				t.Run(fmt.Sprintf("external=%t/indexed=%t/failfast=%t", external, indexed, failFast), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						g := NewGroup[int](1).WithTimeout(time.Hour).WithStreaming(4)
						var p *pool.Pool[int]
						if external {
							p = pool.NewPool[int](1).WithTimeout(0).WithMaxResults(0)
							g.WithPool(p)
							defer p.Close()
						}
						ctx := context.Background()
						if failFast {
							g, ctx = g.WithFailFast(ctx)
						}
						entered, release := make(chan context.Context, 1), make(chan struct{})
						fn := func(child context.Context) (int, error) {
							entered <- child
							<-release
							runtime.Goexit()
							return 0, nil
						}
						var err error
						if indexed {
							err = g.GoAt(2, ctx, fn)
						} else {
							err = g.Go(ctx, fn)
						}
						if err != nil {
							t.Fatal(err)
						}
						child := <-entered
						if g.Active() != 1 || g.Busy() != 1 {
							t.Fatalf("running task not counted: active=%d busy=%d", g.Active(), g.Busy())
						}
						close(release)
						synctest.Wait()
						if failFast != (ctx.Err() != nil) || child.Err() == nil || g.Active() != 0 || g.Busy() != 0 {
							t.Errorf("cleanup: parent=%v child=%v active=%d busy=%d", ctx.Err(), child.Err(), g.Active(), g.Busy())
						}
						// An independent parent proves capacity survives even after FailFast.
						if err := g.Go(context.Background(), func(context.Context) (int, error) { return 7, nil }); err != nil {
							t.Fatal(err)
						}
						synctest.Wait()
						results, ok := g.WaitTimeout(time.Second)
						wantLen, failed, success := 2, 0, 1
						if indexed {
							wantLen, failed, success = 3, 2, 1
						}
						if !ok || len(results) != wantLen || !errors.Is(results[failed].Err, core.ErrTaskGoexit) || results[success].Value != 7 || results[success].Err != nil || g.FailCount() != 1 || g.OkCount() != 1 {
							t.Errorf("terminal: ok=%t results=%+v fail=%d success=%d", ok, results, g.FailCount(), g.OkCount())
						}
						var streamed int
						for range g.StreamResults() {
							streamed++
						}
						if streamed != 2 {
							t.Errorf("streamed=%d", streamed)
						}
						if external {
							pr := p.Wait()
							if len(pr) != 2 || !errors.Is(pr[0].Err, core.ErrTaskGoexit) || pr[1].Err != nil || p.Active() != 0 || p.Busy() != 0 {
								t.Errorf("external completion: results=%+v active=%d busy=%d", pr, p.Active(), p.Busy())
							}
						}
					})
				})
			}
		}
	}
}

func TestGroupGoexitSuccessorRespectsShrinkAndReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := NewGroup[int](2).WithTimeout(0)
		entered, release := make(chan struct{}, 2), make(chan struct{})
		for i := 0; i < 2; i++ {
			if err := g.Go(context.Background(), func(context.Context) (int, error) {
				entered <- struct{}{}
				<-release
				runtime.Goexit()
				return 0, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		<-entered
		<-entered
		g.resizeWorkerPool(1)
		close(release)
		synctest.Wait()
		var running, peak atomic.Int32
		for i := 0; i < 6; i++ {
			value := i
			if err := g.Go(context.Background(), func(context.Context) (int, error) {
				n := running.Add(1)
				defer running.Add(-1)
				for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
				}
				time.Sleep(time.Millisecond)
				return value, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		results, ok := g.WaitTimeout(time.Second)
		if !ok || len(results) != 8 || g.FailCount() != 2 || g.OkCount() != 6 || peak.Load() != 1 || g.workerCount.Load() != 0 {
			t.Fatalf("shrink lost capacity/bounds: ok=%t results=%+v failed=%d success=%d peak=%d workers=%d", ok, results, g.FailCount(), g.OkCount(), peak.Load(), g.workerCount.Load())
		}
		if _, err := g.Reset(); err != nil {
			t.Fatal(err)
		}
		if err := g.Go(context.Background(), func(context.Context) (int, error) { return 9, nil }); err != nil {
			t.Fatal(err)
		}
		results = g.Wait()
		if len(results) != 1 || results[0].Value != 9 || results[0].Err != nil || g.FailCount() != 0 || g.OkCount() != 1 {
			t.Errorf("next batch: %+v failed=%d success=%d", results, g.FailCount(), g.OkCount())
		}
	})
}
