package task

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Concurrent waiting, cancellation of a wait and early timeout must never
// replace the actual cached business outcome or execute the business twice.
func FuzzTaskConcurrentCache(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8}, uint8(3))
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{0, 1, 2, 3, 4, 5}, uint8(1))
	f.Add([]byte{5, 4, 3, 2, 1, 0}, uint8(2))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8}, uint8(0))
	f.Add([]byte{8, 7, 6, 5, 4, 3, 2, 1, 0}, uint8(1))
	f.Add([]byte{6, 7, 8, 2, 0, 5}, uint8(2))
	f.Fuzz(func(t *testing.T, operations []byte, action uint8) {
		if len(operations) > 16 {
			operations = operations[:16]
		}
		wantErr := errors.New("model task error")
		check := func(value int, err error) {
			t.Helper()
			switch action % 4 {
			case 0:
				if value != 37 || err != nil {
					t.Errorf("success cache: %d, %v", value, err)
				}
			case 1:
				if value != 19 || !errors.Is(err, wantErr) {
					t.Errorf("error cache: %d, %v", value, err)
				}
			case 2:
				var pe *core.PanicError
				if value != 0 || !errors.As(err, &pe) || pe.Value != 73 {
					t.Errorf("panic cache: %d, %v", value, err)
				}
			case 3:
				if value != 0 || !errors.Is(err, core.ErrTaskGoexit) {
					t.Errorf("Goexit cache: %d, %v", value, err)
				}
			}
		}
		for engine := range 4 {
			gate, start := make(chan struct{}), make(chan struct{})
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
			var ar *AsyncResult[int]
			var runner *BoundedRunner
			if engine == 0 {
				ar = Go(context.Background(), fn)
			} else if engine == 1 {
				runner = NewBoundedRunner(1)
				ar = BoundedGo(runner, context.Background(), fn)
			}
			if engine < 2 {
				checkConcurrentCache(t, operations, action, start, gate, ar, check)
			} else {
				act := func(ctx context.Context) error { _, err := fn(ctx); return err }
				var ar *AsyncResult[NoResult]
				if engine == 2 {
					ar = GoAct(context.Background(), act)
				} else {
					runner = NewBoundedRunner(1)
					ar = BoundedGoAct(runner, context.Background(), act)
				}
				checkConcurrentCache(t, operations, action, start, gate, ar, func(_ NoResult, err error) {
					value := 0
					if action%4 == 0 {
						value = 37
					} else if action%4 == 1 {
						value = 19
					}
					check(value, err)
				})
			}
			if calls.Load() != 1 || (runner != nil && runner.Busy() != 0) {
				t.Errorf("engine %d calls %d", engine, calls.Load())
			}
		}
	})
}

// The same externally observed contract applies to value and action tasks,
// although Go eagerly populates its cache and GoAct only sends on its channel.
func checkConcurrentCache[T comparable](t *testing.T, operations []byte, action uint8, start, gate chan struct{}, ar *AsyncResult[T], check func(T, error)) {
	var zero T
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	var wg sync.WaitGroup
	for _, operation := range operations {
		wg.Go(func() {
			<-start
			switch operation % 9 {
			case 0:
				check(ar.Wait())
			case 1:
				check(ar.Values())
			case 2:
				ch := ar.WaitCh()
				r, ok := <-ch
				if !ok {
					t.Error("WaitCh closed before result")
				} else {
					check(r.Value, r.Err)
				}
				if _, ok := <-ch; ok {
					t.Error("WaitCh produced duplicate result")
				}
			case 3:
				v, err, ok := ar.WaitContext(canceled)
				if ok {
					check(v, err)
				} else if v != zero || !errors.Is(err, context.Canceled) {
					t.Errorf("canceled wait: %v, %v", v, err)
				}
			case 4:
				v, err, ok := ar.WaitTimeout(0)
				if ok {
					check(v, err)
				} else if v != zero || !errors.Is(err, core.ErrTimeout) {
					t.Errorf("timed wait: %v, %v", v, err)
				}
			case 5:
				v, err := ar.Cancel()
				if !errors.Is(err, context.Canceled) {
					check(v, err)
				} else if v != zero {
					t.Errorf("canceled result value %v", v)
				}
			case 6:
				queried := ar.Error()
				_, actual := ar.Wait()
				if queried != actual {
					t.Error("Error changed cached error identity")
				}
			case 7:
				if ar.Ok() != (action%4 == 0) {
					t.Error("Ok disagrees with business outcome")
				}
			case 8:
				if ar.IsPanic() != (action%4 == 2) {
					t.Error("IsPanic disagrees with business outcome")
				}
			}
		})
	}
	wg.Go(func() { <-start; close(gate) })
	close(start)
	wg.Wait()
	check(ar.Wait())
	v, err, ok := ar.WaitTimeout(time.Nanosecond)
	if !ok {
		t.Error("cached result timed out")
	}
	check(v, err)
	v, err, ok = ar.WaitContext(canceled)
	if !ok {
		t.Error("canceled context replaced cached result")
	}
	check(v, err)
}
