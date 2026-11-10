package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

var deadlinePipelineModes = []string{"default", "serial", "native", "shard", "autoscale", "pool", "stream"}

func TestPipelinePoolObserverStageBarrierDependsOnFailFast(t *testing.T) {
	for _, ff := range []bool{false, true} {
		t.Run(fmt.Sprintf("ff=%v", ff), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pl := pool.NewPool[int](2).WithTimeout(0).WithMaxResults(0)
				defer pl.Close()
				release := make(chan struct{})
				defer close(release)
				started := make(chan struct{}, 2)
				noticed := make(chan int, 2)
				done := make(chan struct{})
				b := NewPipelineBuilder([]int{0, 1}).Timeout(time.Minute).Stage("only", 2).
					OnResult(func(_ string, r core.Result[int]) { noticed <- r.Value }).Parallel().Pool(pl)
				if ff {
					b.FailFast()
				}
				go func() {
					results, err := b.Run(func(_ context.Context, _ string, value int) (int, error) {
						started <- struct{}{}
						if value == 1 {
							<-release
						}
						return value, nil
					})
					if err != nil || len(results) != 2 {
						t.Errorf("pool stage outcome=%+v/%v", results, err)
					}
					close(done)
				}()
				<-started
				<-started
				synctest.Wait()
				if ff {
					select {
					case value := <-noticed:
						if value != 0 {
							t.Errorf("early observer=%d", value)
						}
					default:
						t.Error("FailFast observer delayed behind unrelated task")
					}
				} else {
					select {
					case value := <-noticed:
						t.Errorf("stage barrier bypassed: observed %d", value)
					default:
					}
				}
				select {
				case <-done:
					t.Error("stage returned before second task")
				default:
				}
				release <- struct{}{}
				<-done
				wantRemaining := 2
				if ff {
					wantRemaining = 1
				}
				if len(noticed) != wantRemaining {
					t.Errorf("remaining notices=%d want %d", len(noticed), wantRemaining)
				}
			})
		})
	}
}

func TestPipelineDeadlineDoesNotInterruptObserver(t *testing.T) {
	for _, mode := range deadlinePipelineModes {
		for _, ff := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ff=%v", mode, ff), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					start := time.Now()
					entered, expired, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
					defer close(release)
					observer := func(stage string, _ core.Result[int]) {
						if stage != "last" {
							return
						}
						close(entered)
						time.Sleep(6 * time.Second)
						close(expired)
						<-release
					}
					run, cleanup := deadlinePipeline(mode, context.Background(), ff, observer)
					defer cleanup()
					done := make(chan struct{})
					go func() {
						run(func(context.Context, string, int) (int, error) { return 1, nil })
						close(done)
					}()
					<-entered
					<-expired
					synctest.Wait()
					if !time.Now().Equal(start.Add(6 * time.Second)) {
						t.Errorf("observer timeline=%v", time.Now())
					}
					select {
					case <-done:
						t.Error("returned while observer still running")
					default:
					}
					release <- struct{}{}
					<-done
				})
			})
		}
	}
}

func deadlinePipeline(mode string, parent context.Context, ff bool, cb func(string, core.Result[int])) (func(func(context.Context, string, int) (int, error)) ([]core.Result[int], error), func()) {
	b := NewPipelineBuilder([]int{0}).Context(parent).Timeout(5*time.Second).Stage("first", 1).Stage("last", 1).OnResult(cb)
	if ff {
		b.FailFast()
	}
	cleanup := func() {}
	switch mode {
	case "default":
		return b.Run, cleanup
	case "serial":
		return b.Serial().Run, cleanup
	case "native":
		return b.Parallel().Run, cleanup
	case "shard":
		return b.Parallel().Shard(2).Run, cleanup
	case "autoscale":
		return b.Parallel().DefaultAutoScale().Run, cleanup
	case "pool":
		pl := pool.NewPool[int](1).WithTimeout(0).WithMaxResults(0)
		return b.Parallel().Pool(pl).Run, pl.Close
	case "stream":
		return b.Stream().Run, cleanup
	default:
		panic(mode)
	}
}

// The second stage starts one second later but shares the same absolute
// deadline. Expiration does not join the callback or overwrite its result.
func TestPipelineOverallDeadlineWaitsForCallbacks(t *testing.T) {
	for _, mode := range deadlinePipelineModes {
		for _, ff := range []bool{false, true} {
			for _, parentEarlier := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/ff=%v/parent=%v", mode, ff, parentEarlier), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						start := time.Now()
						parent, cancel := context.WithCancel(context.Background())
						budget := 5 * time.Second
						if parentEarlier {
							cancel()
							parent, cancel = context.WithTimeout(context.Background(), 3*time.Second)
							budget = 3 * time.Second
						}
						defer cancel()
						run, cleanup := deadlinePipeline(mode, parent, ff, nil)
						defer cleanup()
						entered, expired, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
						defer close(release)
						type outcome struct {
							results []core.Result[int]
							err     error
						}
						done := make(chan outcome, 1)
						go func() {
							results, err := run(func(ctx context.Context, stage string, value int) (int, error) {
								deadline, ok := ctx.Deadline()
								if !ok || !deadline.Equal(start.Add(budget)) {
									t.Errorf("shared deadline at %s = %v/%v want %v", stage, deadline, ok, start.Add(budget))
								}
								if stage == "first" {
									time.Sleep(time.Second)
									return value + 10, nil
								}
								close(entered)
								<-ctx.Done()
								close(expired)
								<-release
								return value + 20, ctx.Err()
							})
							done <- outcome{results, err}
						}()
						<-entered
						<-expired
						if !time.Now().Equal(start.Add(budget)) {
							t.Errorf("expiration time=%v", time.Now())
						}
						synctest.Wait()
						select {
						case <-done:
							t.Error("returned while callback still running")
						default:
						}
						// Release without double close in failure cleanup.
						release <- struct{}{}
						got := <-done
						if len(got.results) != 1 || got.results[0].Value != 30 || !errors.Is(got.results[0].Err, context.DeadlineExceeded) {
							t.Fatalf("expired business result=%+v error=%v", got.results, got.err)
						}
						if mode == "stream" {
							if !errors.Is(got.err, context.DeadlineExceeded) || !got.results[0].Occupied {
								t.Errorf("stream completion=%+v/%v", got.results, got.err)
							}
						} else if got.err != nil {
							t.Errorf("top-level error=%v", got.err)
						}
						if !parentEarlier && parent.Err() != nil {
							t.Errorf("execution canceled caller parent: %v", parent.Err())
						}
					})
				})
			}
		}
	}
}

// OnResult observes a completed business call, but its timing relative to
// the task's context cleanup depends on the execution path and FailFast.
func TestPipelineObserverContextCleanupByPath(t *testing.T) {
	for _, mode := range deadlinePipelineModes {
		for _, ff := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ff=%v", mode, ff), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var mu sync.Mutex
					contexts := make(map[string]context.Context)
					notices := make(map[string]int)
					wantCanceled := mode == "serial" || mode == "native" || (mode == "default" && ff) || (mode == "pool" && !ff)
					observer := func(stage string, result core.Result[int]) {
						mu.Lock()
						defer mu.Unlock()
						ctx := contexts[stage]
						if ctx == nil {
							t.Errorf("observer before business at %s", stage)
							return
						}
						if (ctx.Err() != nil) != wantCanceled {
							t.Errorf("observer context at %s=%v want canceled=%v", stage, ctx.Err(), wantCanceled)
						}
						if result.Err != nil {
							t.Errorf("observer result=%+v", result)
						}
						notices[stage]++
					}
					parent, cancel := context.WithCancel(context.Background())
					defer cancel()
					run, cleanup := deadlinePipeline(mode, parent, ff, observer)
					defer cleanup()
					results, err := run(func(ctx context.Context, stage string, value int) (int, error) {
						mu.Lock()
						contexts[stage] = ctx
						mu.Unlock()
						time.Sleep(time.Second)
						return value + 1, nil
					})
					if err != nil || len(results) != 1 || results[0].Err != nil || results[0].Value != 2 {
						t.Fatalf("business outcome=%+v/%v", results, err)
					}
					mu.Lock()
					defer mu.Unlock()
					for _, stage := range []string{"first", "last"} {
						if notices[stage] != 1 || contexts[stage].Err() == nil {
							t.Errorf("cleanup at %s notices=%d err=%v", stage, notices[stage], contexts[stage].Err())
						}
					}
					if parent.Err() != nil {
						t.Errorf("caller parent canceled: %v", parent.Err())
					}
				})
			})
		}
	}
}
