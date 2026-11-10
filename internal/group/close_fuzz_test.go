package group

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// Admission, cancellation and waiting can win in any order. After all callers
// return, every attempt must have one occupied outcome; every business
// callback must run at most once. Configuration and Reset are not concurrent.
func FuzzGroupConcurrentClose(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 255}, uint8(3))
	f.Add([]byte{2, 2, 1, 0}, uint8(255))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 24 {
			data = data[:24]
		}
		g := NewGroup[int](int(config%4) + 1)
		if config&1 != 0 {
			p := pool.NewPool[int](int(config%3) + 1).WithMaxResults(0)
			defer p.Close()
			g.WithPool(p)
		}
		defer g.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wantErr := errors.New("model business error")
		visits := make([]atomic.Int32, len(data))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, b := range data {
			wg.Go(func() {
				<-start
				taskCtx := context.Background()
				if b&1 != 0 {
					taskCtx = ctx
				}
				fn := func(context.Context) (int, error) {
					visits[i].Add(1)
					switch b % 3 {
					case 1:
						return i + 1, wantErr
					case 2:
						panic(i + 1)
					default:
						return i + 1, nil
					}
				}
				var err error
				if config&2 != 0 && b&4 != 0 {
					err = g.GoAtWithTimeout(i*2, taskCtx, time.Hour, fn)
				} else if config&2 != 0 {
					err = g.GoAt(i*2, taskCtx, fn)
				} else if b&4 != 0 {
					err = g.GoWithTimeout(taskCtx, time.Hour, fn)
				} else {
					err = g.Go(taskCtx, fn)
				}
				if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, core.ErrGroupWaited) && !errors.Is(err, core.ErrGroupWaiting) {
					t.Errorf("unexpected submission error %d: %v", i, err)
				}
			})
		}
		wg.Go(func() { <-start; cancel() })
		wg.Go(func() {
			<-start
			for range int(config % 8) {
				runtime.Gosched()
			}
			g.Wait()
		})
		wg.Go(func() { <-start; g.WaitContext(ctx); g.Close() })
		close(start)
		wg.Wait()
		results := g.Wait()
		if config&2 == 0 && len(results) != len(data) {
			t.Fatalf("%d results want %d", len(results), len(data))
		}
		seen := make([]bool, len(data))
		successes, failures, executed, occupied := 0, 0, 0, 0
		for _, r := range results {
			if !r.Occupied {
				if config&2 == 0 || r.Err != nil || r.Value != 0 {
					t.Fatalf("invalid empty outcome: %+v", r)
				}
				continue
			}
			occupied++
			id, kind := r.Value, byte(0)
			var pe *core.PanicError
			switch {
			case r.Err == nil:
				successes++
			case errors.Is(r.Err, wantErr):
				failures++
				kind = 1
			case errors.As(r.Err, &pe):
				failures++
				kind = 2
				var ok bool
				id, ok = pe.Value.(int)
				if !ok || r.Value != 0 {
					t.Fatalf("invalid panic: %+v", r)
				}
			default:
				failures++
				if r.Value != 0 || (!errors.Is(r.Err, context.Canceled) && !errors.Is(r.Err, core.ErrGroupWaited) && !errors.Is(r.Err, core.ErrGroupWaiting)) {
					t.Fatalf("invalid rejection: %+v", r)
				}
				continue
			}
			if id < 1 || id > len(data) || seen[id-1] || data[id-1]%3 != kind || visits[id-1].Load() != 1 {
				t.Fatalf("invalid executed outcome: %+v, id %d kind %d", r, id, kind)
			}
			seen[id-1] = true
			executed++
		}
		actualExecuted := 0
		for i := range visits {
			n := visits[i].Load()
			if n > 1 || (n == 1) != seen[i] {
				t.Fatalf("item %d visits %d, outcome retained %t", i, n, seen[i])
			}
			actualExecuted += int(n)
		}
		if occupied != len(data) || actualExecuted != executed || g.TotalCount() != int64(len(data)) || g.OkCount() != int64(successes) || g.FailCount() != int64(failures) || g.Active() != 0 || g.Busy() != 0 {
			t.Fatalf("stats %+v, want success/fail %d/%d executed %d/%d", g.Stats(), successes, failures, actualExecuted, executed)
		}
	})
}
