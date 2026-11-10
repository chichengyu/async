package task

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// Task.Result is a cached function field rather than AsyncResult.Wait. Copies
// of a Task must share one business outcome even when cancellation races reads.
func FuzzTaskResultCache(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{0, 1, 2, 3, 4, 5}, uint8(1))
	f.Add([]byte{3, 2, 1, 0, 255}, uint8(2))
	f.Add([]byte{0, 1, 2, 3, 4, 5}, uint8(3))
	f.Fuzz(func(t *testing.T, operations []byte, action uint8) {
		if len(operations) > 16 {
			operations = operations[:16]
		}
		for engine := range 4 {
			gate, start := make(chan struct{}), make(chan struct{})
			wantErr := errors.New("result field model failure")
			var calls atomic.Int32
			fn := func(context.Context) (int, error) {
				calls.Add(1)
				<-gate
				switch action % 4 {
				case 1:
					return 19, wantErr
				case 2:
					panic(73)
				case 3:
					runtime.Goexit()
					return 0, nil
				default:
					return 37, nil
				}
			}
			var handle Task[int]
			var runner *BoundedRunner
			switch engine {
			case 0:
				handle = GoResult(context.Background(), fn)
			case 1:
				runner = NewBoundedRunner(1)
				handle = BoundedGoResult(runner, context.Background(), fn)
			case 2, 3:
				fnAct := func(ctx context.Context) error {
					_, err := fn(ctx)
					return err
				}
				var act Task[NoResult]
				if engine == 2 {
					act = GoResultAct(context.Background(), fnAct)
				} else {
					runner = NewBoundedRunner(1)
					act = boundedGoResultAct(runner, context.Background(), fnAct)
				}
				handle = Task[int]{Ctx: act.Ctx, Cancel: act.Cancel, Result: func() (int, error) { _, err := act.Result(); return 0, err }}
			}
			check := func(value int, err error) {
				t.Helper()
				wantValue := 37
				if action%4 == 1 {
					wantValue = 19
				}
				if action%4 >= 2 || engine >= 2 {
					wantValue = 0
				}
				if value != wantValue {
					t.Errorf("engine %d value=%d want=%d", engine, value, wantValue)
				}
				switch action % 4 {
				case 0:
					if err != nil {
						t.Errorf("success error: %v", err)
					}
				case 1:
					if !errors.Is(err, wantErr) {
						t.Errorf("business error: %v", err)
					}
				case 2:
					var pe *core.PanicError
					if !errors.As(err, &pe) || pe.Value != 73 {
						t.Errorf("panic error: %v", err)
					}
				case 3:
					if !errors.Is(err, core.ErrTaskGoexit) {
						t.Errorf("Goexit error: %v", err)
					}
				}
			}
			var wg sync.WaitGroup
			for _, operation := range operations {
				copyOfTask := handle
				wg.Go(func() {
					<-start
					if operation%3 == 0 {
						copyOfTask.Cancel()
					} else {
						check(copyOfTask.Result())
					}
				})
			}
			wg.Go(func() { <-start; close(gate) })
			close(start)
			wg.Wait()
			check(handle.Result())
			handle.Cancel()
			check(handle.Result())
			if !errors.Is(handle.Ctx.Err(), context.Canceled) || calls.Load() != 1 || runner != nil && runner.Busy() != 0 {
				t.Fatalf("engine %d ctx=%v calls=%d", engine, handle.Ctx.Err(), calls.Load())
			}
		}
	})
}
