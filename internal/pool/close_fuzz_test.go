package pool

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

// No scheduler outcome is prescribed: Close, cancellation and admission may
// race. Every submission must still terminate exactly once, and every business
// callback may execute at most once. User callbacks always finish promptly.
func FuzzPoolConcurrentClose(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 255}, uint8(7))
	f.Add([]byte{2, 2, 3, 3, 0}, uint8(255))
	f.Add([]byte{0, 2, 1, 3, 4, 6, 7, 8, 10}, uint8(24))
	f.Add([]byte{0, 0, 2, 2, 1, 3, 4, 6}, uint8(12))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 24 {
			data = data[:24]
		}
		p := NewPool[int](int(config%4) + 1).WithMaxResults(0).
			WithMaxPending(int(config%5) + 1).WithOverflow(core.OverflowStrategy(config % 3))
		defer p.Close()
		if config&16 != 0 {
			p.EnableAutoScale(&core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 4, CheckInterval: time.Hour})
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		start := make(chan struct{})
		visits, completions := make([]atomic.Int32, len(data)), make([]atomic.Int32, len(data))
		terminal := make([]core.Result[int], len(data))
		var wg sync.WaitGroup
		for i, b := range data {
			wg.Go(func() {
				<-start
				taskCtx := context.Background()
				if b%2 == 1 {
					taskCtx = ctx
				}
				err := p.SubmitWithCompletion(taskCtx, func(context.Context) (int, error) {
					visits[i].Add(1)
					if config&8 != 0 {
						runtime.Gosched()
					}
					if b%4 == 2 {
						panic(i)
					}
					return i + 1, nil
				}, func(r core.Result[int]) {
					if completions[i].Add(1) != 1 {
						t.Errorf("duplicate completion %d", i)
						return
					}
					terminal[i] = r
				})
				if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, core.ErrPoolClosed) && !errors.Is(err, core.ErrPoolWaiting) && !errors.Is(err, core.ErrQueueOverflow) {
					t.Errorf("unexpected submit error %d: %v", i, err)
				}
			})
		}
		if config&8 != 0 {
			wg.Go(func() {
				<-start
				for step := range 8 {
					p.Resize(1 + step%4)
					runtime.Gosched()
				}
			})
			wg.Go(func() {
				<-start
				for step := range 4 {
					p.ResizeAndWaitTimeout(1+step%3, time.Nanosecond)
				}
			})
		}
		if config&16 != 0 {
			wg.Go(func() { <-start; p.DisableAutoScale() })
		}
		wg.Go(func() { <-start; cancel() })
		wg.Go(func() {
			<-start
			if config&4 != 0 {
				p.CloseAndWaitTimeout(time.Nanosecond)
			}
			p.Close()
		})
		wg.Go(func() { <-start; p.Close() })
		close(start)
		wg.Wait()
		results := p.Wait()
		if len(results) != len(data) {
			t.Fatalf("%d outcomes want %d", len(results), len(data))
		}
		failures, successes := 0, 0
		for i, r := range terminal {
			if completions[i].Load() != 1 || !r.Occupied {
				t.Fatalf("item %d completion count %d: %+v", i, completions[i].Load(), r)
			}
			var pe *core.PanicError
			switch {
			case r.Err == nil:
				successes++
				if visits[i].Load() != 1 || r.Value != i+1 || data[i]%4 == 2 {
					t.Fatalf("invalid success %d: %+v visits %d", i, r, visits[i].Load())
				}
			case errors.As(r.Err, &pe):
				failures++
				if pe.Value != i || data[i]%4 != 2 || visits[i].Load() != 1 {
					t.Fatalf("invalid panic %d: %+v", i, r)
				}
			default:
				failures++
				if visits[i].Load() != 0 || (!errors.Is(r.Err, context.Canceled) && !errors.Is(r.Err, core.ErrPoolClosed) && !errors.Is(r.Err, core.ErrPoolWaiting) && !errors.Is(r.Err, core.ErrQueueOverflow)) {
					t.Fatalf("invalid rejection %d: %+v visits %d", i, r, visits[i].Load())
				}
			}
		}
		for i, r := range results {
			if !r.Occupied {
				t.Fatalf("empty retained outcome %d", i)
			}
		}
		if p.TotalCount() != int64(len(data)) || p.FailCount() != int64(failures) || p.OkCount() != int64(successes) || p.Pending() != 0 || p.Busy() != 0 || p.Active() != 0 {
			t.Fatalf("stats %+v want success/fail %d/%d", p.Stats(), successes, failures)
		}
	})
}
