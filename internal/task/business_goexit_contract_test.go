package task

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

func TestBusinessGoexitIsFailureAcrossTaskEntrypoints(t *testing.T) {
	for _, mode := range []string{"go", "act", "result", "result-act", "bounded", "bounded-act", "bounded-result", "bounded-result-act"} {
		t.Run(mode, func(t *testing.T) {
			fn := func(context.Context) (int, error) { runtime.Goexit(); return 0, nil }
			act := func(context.Context) error { runtime.Goexit(); return nil }
			runner := NewBoundedRunner(1)
			var wait func() (int, error)
			ctx := context.Background()
			switch mode {
			case "go":
				ar := Go(ctx, fn)
				wait = ar.Wait
			case "act":
				ar := GoAct(ctx, act)
				wait = func() (int, error) { _, err := ar.Wait(); return 0, err }
			case "result":
				task := GoResult(ctx, fn)
				defer task.Cancel()
				wait = task.Result
			case "result-act":
				task := GoResultAct(ctx, act)
				defer task.Cancel()
				wait = func() (int, error) { _, err := task.Result(); return 0, err }
			case "bounded":
				ar := BoundedGo(runner, ctx, fn)
				wait = ar.Wait
			case "bounded-act":
				ar := BoundedGoAct(runner, ctx, act)
				wait = func() (int, error) { _, err := ar.Wait(); return 0, err }
			case "bounded-result":
				task := BoundedGoResult(runner, ctx, fn)
				defer task.Cancel()
				wait = task.Result
			case "bounded-result-act":
				task := boundedGoResultAct(runner, ctx, act)
				defer task.Cancel()
				wait = func() (int, error) { _, err := task.Result(); return 0, err }
			}
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					v, err := wait()
					if v != 0 || !errors.Is(err, core.ErrTaskGoexit) {
						t.Errorf("value=%d err=%v", v, err)
					}
				}()
			}
			wg.Wait()
			if runner.Available() != 1 {
				t.Fatalf("available=%d", runner.Available())
			}
		})
	}
}

func TestTaskBuilderBusinessGoexitCleansTimeoutContext(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		for mode := 0; mode < 4; mode++ {
			b := NewTaskBuilder[int]().WithTimeout(time.Hour)
			if bounded {
				b = b.Bounded(1)
			}
			var taskContext context.Context
			fn := func(ctx context.Context) (int, error) { taskContext = ctx; runtime.Goexit(); return 0, nil }
			act := func(ctx context.Context) error { _, err := fn(ctx); return err }
			var err error
			switch mode {
			case 0:
				_, err = b.Go(fn).Wait()
			case 1:
				_, err = b.GoAct(act).Wait()
			case 2:
				task := b.GoResult(fn)
				defer task.Cancel()
				_, err = task.Result()
			case 3:
				task := b.GoResultAct(act)
				defer task.Cancel()
				_, err = task.Result()
			}
			if !errors.Is(err, core.ErrTaskGoexit) || taskContext == nil || taskContext.Err() != context.Canceled {
				t.Fatalf("bounded=%v mode=%d err=%v context=%v", bounded, mode, err, taskContext)
			}
			if bounded && b.bounded.Busy() != 0 {
				t.Fatalf("busy=%d", b.bounded.Busy())
			}
		}
	}
}
