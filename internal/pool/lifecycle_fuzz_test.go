package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// The oracle counts terminal submissions independently of retained result slots.
// The ring holds every overflow item, so the union must contain every outcome
// exactly once. Configuration and resets are sequential, without FailFast.
func FuzzPoolRetainedLifecycle(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{0, 1, 2, 3, 4, 255}, uint8(3))
	f.Add([]byte{3, 3, 0, 2, 1}, uint8(255))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 48 {
			data = data[:48]
		}
		limit := int(config % 9)
		p := NewPool[int](int(config%4)+1).WithMaxResults(limit).WithRingBuffer(48, core.OverflowError)
		defer p.Close()
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		wantErr := errors.New("model business error")
		for round := range 2 {
			visits := make([]atomic.Int32, len(data))
			completed := make([]atomic.Int32, len(data))
			wantFailures, wantCanceled, wantPanics := 0, 0, 0
			for i, b := range data {
				action := (int(b) + round) % 4
				ctx := context.Background()
				if action != 0 {
					wantFailures++
				}
				if action == 3 {
					ctx = canceled
					wantCanceled++
				}
				if action == 2 {
					wantPanics++
				}
				err := p.SubmitWithCompletion(ctx, func(context.Context) (int, error) {
					visits[i].Add(1)
					switch action {
					case 1:
						return i + 1, wantErr
					case 2:
						panic(i)
					default:
						return i + 1, nil
					}
				}, func(r core.Result[int]) {
					completed[i].Add(1)
					if !r.Occupied || (r.Err != nil) != (action != 0) {
						t.Errorf("completion %d action %d: %+v", i, action, r)
					}
				})
				if (action == 3 && !errors.Is(err, context.Canceled)) || (action != 3 && err != nil) {
					t.Fatalf("submission %d action %d: %v", i, action, err)
				}
			}
			results := p.Wait()
			if len(results) != len(data) {
				t.Fatalf("limit %d: %d results want %d", limit, len(results), len(data))
			}
			seen := make([]bool, len(data))
			failures, cancellations, panics := 0, 0, 0
			for _, r := range results {
				if !r.Occupied {
					t.Fatal("unoccupied outcome")
				}
				if r.Err != nil {
					failures++
				}
				var pe *core.PanicError
				switch {
				case errors.Is(r.Err, context.Canceled):
					cancellations++
				case errors.As(r.Err, &pe):
					panics++
					i, ok := pe.Value.(int)
					if !ok || i < 0 || i >= len(data) || seen[i] || (int(data[i])+round)%4 != 2 {
						t.Fatalf("invalid panic: %+v", pe)
					}
					seen[i] = true
				default:
					i := r.Value - 1
					if i < 0 || i >= len(data) || seen[i] {
						t.Fatalf("invalid/duplicate result: %+v", r)
					}
					action := (int(data[i]) + round) % 4
					if (action != 0 && action != 1) || (action == 1 && !errors.Is(r.Err, wantErr)) || (action == 0 && r.Err != nil) {
						t.Fatalf("result %d action %d: %+v", i, action, r)
					}
					seen[i] = true
				}
			}
			if failures != wantFailures || cancellations != wantCanceled || panics != wantPanics {
				t.Fatalf("outcomes fail/cancel/panic=%d/%d/%d want %d/%d/%d", failures, cancellations, panics, wantFailures, wantCanceled, wantPanics)
			}
			for i, b := range data {
				wantVisits := int32(1)
				if (int(b)+round)%4 == 3 {
					wantVisits = 0
				}
				if visits[i].Load() != wantVisits || completed[i].Load() != 1 {
					t.Fatalf("item %d: visits %d want %d, completions %d", i, visits[i].Load(), wantVisits, completed[i].Load())
				}
			}
			if p.TotalCount() != int64(len(data)) || p.FailCount() != int64(wantFailures) || p.OkCount() != int64(len(data)-wantFailures) || p.Pending() != 0 || p.Busy() != 0 {
				t.Fatalf("terminal stats: %+v", p.Stats())
			}
			if p.Wait() != nil {
				t.Fatal("second Wait returned results")
			}
			if round == 0 {
				if config%2 == 0 {
					p.ResetWait()
				} else if _, err := p.Reset(); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
}
