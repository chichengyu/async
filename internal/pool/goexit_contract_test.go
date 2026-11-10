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

// A task ending its goroutine must not strand the next accepted task or its
// completion notification. Missing worker replacement or Done must fail here.
func TestPoolBusinessGoexitCompletesAndPreservesCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool[int](1).WithTimeout(0).WithMaxResults(0)
		firstExited := make(chan struct{})
		completed := make(chan core.Result[int], 2)
		if err := p.SubmitWithCompletion(context.Background(), func(context.Context) (int, error) {
			defer close(firstExited)
			runtime.Goexit()
			return 0, nil
		}, func(r core.Result[int]) { completed <- r }); err != nil {
			t.Fatal(err)
		}
		<-firstExited
		synctest.Wait()
		if p.Active() != 0 || p.Busy() != 0 {
			t.Errorf("finished Goexit task still active/busy: %d/%d", p.Active(), p.Busy())
		}
		if err := p.SubmitWithCompletion(context.Background(), func(context.Context) (int, error) {
			return 7, nil
		}, func(r core.Result[int]) { completed <- r }); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		results, ok := p.WaitTimeout(time.Second)
		if !ok {
			t.Error("Goexit stranded the next accepted task")
		}
		p.Close() // Original Pool can drain the orphan; never leave the red test hung.
		if len(results) != 2 || !results[0].Occupied || !errors.Is(results[0].Err, core.ErrTaskGoexit) || results[0].IsPanic() || results[1].Value != 7 || results[1].Err != nil {
			t.Errorf("terminal results: %+v", results)
		}
		if len(completed) != 2 || p.FailCount() != 1 || p.OkCount() != 1 || p.Active() != 0 || p.Busy() != 0 {
			t.Errorf("completion/stats: callbacks=%d fail=%d ok=%d active=%d busy=%d", len(completed), p.FailCount(), p.OkCount(), p.Active(), p.Busy())
		}
	})
}

func TestPoolGoexitIndexedFailFastAndClose(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, failFast := range []bool{false, true} {
			for _, closeDuringTask := range []bool{false, true} {
				t.Run(fmt.Sprintf("indexed=%t/failfast=%t/close=%t", indexed, failFast, closeDuringTask), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						p := NewPool[int](1).WithTimeout(time.Hour).WithMaxResults(0).WithStreaming(4)
						ctx := context.Background()
						if failFast {
							p, ctx = p.WithFailFast(ctx)
						}
						entered, release := make(chan context.Context, 1), make(chan struct{})
						var completions atomic.Int32
						fn := func(child context.Context) (int, error) {
							entered <- child
							<-release
							runtime.Goexit()
							return 0, nil
						}
						complete := func(r core.Result[int]) {
							if !errors.Is(r.Err, core.ErrTaskGoexit) {
								t.Errorf("completion error=%v", r.Err)
							}
							completions.Add(1)
						}
						var err error
						if indexed {
							err = p.SubmitAtWithCompletion(2, ctx, fn, complete)
						} else {
							err = p.SubmitWithCompletion(ctx, fn, complete)
						}
						if err != nil {
							t.Fatal(err)
						}
						child := <-entered
						closed := make(chan struct{})
						if closeDuringTask {
							go func() { p.Close(); close(closed) }()
							synctest.Wait()
						}
						close(release)
						synctest.Wait()
						if failFast != (ctx.Err() != nil) || child.Err() == nil {
							t.Errorf("context cleanup: parent=%v child=%v", ctx.Err(), child.Err())
						}
						results, ok := p.WaitTimeout(time.Second)
						wantLen, index := 1, 0
						if indexed {
							wantLen, index = 3, 2
						}
						if !ok || len(results) != wantLen || !errors.Is(results[index].Err, core.ErrTaskGoexit) || completions.Load() != 1 || p.FailCount() != 1 || p.OkCount() != 0 || p.Active() != 0 || p.Busy() != 0 {
							t.Errorf("terminal: ok=%t results=%+v callbacks=%d fail=%d success=%d active=%d busy=%d", ok, results, completions.Load(), p.FailCount(), p.OkCount(), p.Active(), p.Busy())
						}
						stream := p.StreamResults()
						r, open := <-stream
						if !open || !errors.Is(r.Err, core.ErrTaskGoexit) {
							t.Errorf("stream result=%+v open=%t", r, open)
						}
						if _, open = <-stream; open {
							t.Error("stream not closed or duplicate result")
						}
						p.Close()
						if closeDuringTask {
							<-closed
						}
					})
				})
			}
		}
	}
}

func TestPoolGoexitSuccessorRespectsShrinkAndReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool[int](2).WithTimeout(0).WithMaxResults(0)
		entered, release := make(chan struct{}, 2), make(chan struct{})
		for i := 0; i < 2; i++ {
			if err := p.Submit(context.Background(), func(context.Context) (int, error) {
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
		p.Resize(1)
		close(release)
		synctest.Wait()
		var running, peak atomic.Int32
		for i := 0; i < 6; i++ {
			value := i
			if err := p.Submit(context.Background(), func(context.Context) (int, error) {
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
		results, ok := p.WaitTimeout(time.Second)
		if !ok || len(results) != 8 || p.FailCount() != 2 || p.OkCount() != 6 || peak.Load() != 1 || p.Size() != 1 {
			t.Fatalf("shrink lost capacity/bounds: ok=%t results=%+v failed=%d success=%d peak=%d size=%d", ok, results, p.FailCount(), p.OkCount(), peak.Load(), p.Size())
		}
		p.Reset()
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 9, nil }); err != nil {
			t.Fatal(err)
		}
		results = p.Wait()
		if len(results) != 1 || results[0].Value != 9 || results[0].Err != nil || p.FailCount() != 0 || p.OkCount() != 1 {
			t.Errorf("next batch: %+v failed=%d success=%d", results, p.FailCount(), p.OkCount())
		}
		p.Close()
	})
}

func TestForEachPoolGoexitReportsBeyondRetentionLimit(t *testing.T) {
	previous := core.GetDefaultMaxResults()
	defer core.SetDefaultMaxResults(previous)
	core.SetDefaultMaxResults(1)
	var calls atomic.Int32
	p, err := ForEachPool(context.Background(), []int{1, 2, 3}, func(_ context.Context, value int) error {
		calls.Add(1)
		if value == 2 {
			runtime.Goexit()
		}
		return nil
	}, 1)
	if !errors.Is(err, core.ErrTaskGoexit) || calls.Load() != 3 || p.FailCount() != 1 || p.OkCount() != 2 || p.Active() != 0 || p.Busy() != 0 {
		t.Fatalf("aggregate: err=%v calls=%d fail=%d success=%d active=%d busy=%d", err, calls.Load(), p.FailCount(), p.OkCount(), p.Active(), p.Busy())
	}
	if p.FirstError() != nil {
		t.Fatal("fixture must keep the failed task outside retained results")
	}
}

func TestPoolGoexitRetiresBeforeTakingQueuedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPool[int](2).WithTimeout(0).WithMaxResults(0)
		entered, release := make(chan struct{}, 2), make(chan struct{})
		for range 2 {
			if err := p.Submit(context.Background(), func(context.Context) (int, error) {
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
		var running, peak atomic.Int32
		for range 2 {
			if err := p.Submit(context.Background(), func(context.Context) (int, error) {
				n := running.Add(1)
				defer running.Add(-1)
				for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
				}
				time.Sleep(time.Millisecond)
				return 7, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		p.Resize(1)
		close(release)
		results := p.Wait()
		if peak.Load() != 1 || len(results) != 4 || p.FailCount() != 2 || p.OkCount() != 2 {
			t.Errorf("successor skipped pending retirement: peak=%d results=%+v failed=%d success=%d", peak.Load(), results, p.FailCount(), p.OkCount())
		}
		p.Close()
	})
}
