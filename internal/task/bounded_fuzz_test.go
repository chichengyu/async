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

// Compare semaphore capacity and every task outcome across the four entry paths.
// No user callback blocks, so every acquired slot must eventually be returned.
func FuzzBoundedRunnerMixedOutcomes(f *testing.F) {
	f.Add([]byte{3, 0, 1, 2, 3, 0}, uint8(0))
	f.Add([]byte{3, 3, 3, 3, 3}, uint8(12))
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{0, 1, 2, 0, 1, 2, 0, 1}, uint8(0))
	f.Add([]byte{2, 1, 0, 2, 1, 0, 2, 1}, uint8(5))
	f.Add([]byte{0, 0, 1, 2, 1, 2, 0, 2}, uint8(11))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 24 {
			data = data[:24]
		}
		maximum := int(config%4) + 1
		businessErr := errors.New("bounded business failure")
		for engine := range 4 {
			runner := NewBoundedRunnerBuilder().Max(maximum).Build()
			ctx, cancel := context.WithCancel(context.Background())
			if config&4 != 0 {
				cancel()
			}
			var active, peak atomic.Int32
			visits := make([]atomic.Int32, len(data))
			values := make([]int, len(data))
			errorsByItem := make([]error, len(data))
			start := make(chan struct{})
			var workers sync.WaitGroup
			for i, b := range data {
				workers.Go(func() {
					<-start
					fn := func(context.Context) (int, error) {
						visits[i].Add(1)
						count := active.Add(1)
						defer active.Add(-1)
						for previous := peak.Load(); count > previous; previous = peak.Load() {
							if peak.CompareAndSwap(previous, count) {
								break
							}
						}
						runtime.Gosched()
						switch b % 4 {
						case 1:
							return i + 1, businessErr
						case 2:
							panic(i + 1)
						case 3:
							runtime.Goexit()
							return 0, nil
						default:
							return i + 1, nil
						}
					}
					switch engine {
					case 0:
						values[i], errorsByItem[i] = BoundedGo(runner, ctx, fn).Wait()
					case 1:
						_, errorsByItem[i] = BoundedGoAct(runner, ctx, func(ctx context.Context) error {
							_, err := fn(ctx)
							return err
						}).Wait()
					case 2:
						task := BoundedGoResult(runner, ctx, fn)
						values[i], errorsByItem[i] = task.Result()
						task.Cancel()
					case 3:
						task := boundedGoResultAct(runner, ctx, func(ctx context.Context) error {
							_, err := fn(ctx)
							return err
						})
						_, errorsByItem[i] = task.Result()
						task.Cancel()
					}
				})
			}
			if config&8 != 0 {
				workers.Go(func() { <-start; runtime.Gosched(); cancel() })
			}
			close(start)
			workers.Wait()
			cancel()
			for i, b := range data {
				err, calls := errorsByItem[i], visits[i].Load()
				var pe *core.PanicError
				switch calls {
				case 0:
					if !errors.Is(err, context.Canceled) || values[i] != 0 || config&12 == 0 {
						t.Fatalf("engine=%d item=%d unexecuted value=%d err=%v config=%d", engine, i, values[i], err, config)
					}
				case 1:
					if b%4 == 3 {
						if !errors.Is(err, core.ErrTaskGoexit) || errors.As(err, &pe) || values[i] != 0 {
							t.Fatalf("engine=%d item=%d Goexit value=%d err=%v", engine, i, values[i], err)
						}
					} else if b%4 == 2 {
						if !errors.As(err, &pe) || pe.Value != i+1 || values[i] != 0 {
							t.Fatalf("engine=%d item=%d panic result=%d err=%v", engine, i, values[i], err)
						}
					} else {
						var wantErr error
						if b%4 == 1 {
							wantErr = businessErr
						}
						if !errors.Is(err, wantErr) || (engine%2 == 0 && values[i] != i+1) {
							t.Fatalf("engine=%d item=%d executed value=%d err=%v want=%v", engine, i, values[i], err, wantErr)
						}
					}
				default:
					t.Fatalf("engine=%d item=%d executed %d times", engine, i, calls)
				}
			}
			if peak.Load() > int32(maximum) || active.Load() != 0 || runner.Max() != maximum || runner.Busy() != 0 || runner.Available() != maximum {
				t.Fatalf("engine=%d peak=%d active=%d max=%d busy=%d available=%d want capacity=%d", engine, peak.Load(), active.Load(), runner.Max(), runner.Busy(), runner.Available(), maximum)
			}
		}
	})
}
