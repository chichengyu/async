package group

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

func FuzzGroupGoexitMixedOutcomes(f *testing.F) {
	f.Add([]byte{3, 0, 1, 2, 3, 3, 0}, uint8(0))
	f.Add([]byte{3, 3, 3, 3, 0}, uint8(7))
	f.Add([]byte{}, uint8(1))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 32 {
			data = data[:32]
		}
		g := NewGroup[int](int(config%4) + 1).WithTimeout(0)
		if config&4 != 0 {
			p := pool.NewPool[int](int(config%3) + 1).WithTimeout(0).WithMaxResults(0)
			g.WithPool(p)
			defer p.Close()
		}
		visits := make([]atomic.Int32, len(data))
		businessError := errors.New("business")
		wantFailures := int64(0)
		for i, action := range data {
			if action%4 != 0 {
				wantFailures++
			}
			err := g.GoAt(i, context.Background(), func(context.Context) (int, error) {
				visits[i].Add(1)
				runtime.Gosched()
				switch action % 4 {
				case 1:
					return i + 1, businessError
				case 2:
					panic(i)
				case 3:
					runtime.Goexit()
				}
				return i + 1, nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		results := g.Wait()
		if len(results) != len(data) || g.FailCount() != wantFailures || g.OkCount() != int64(len(data))-wantFailures || g.Active() != 0 || g.Busy() != 0 || g.workerCount.Load() != 0 {
			t.Fatalf("results=%+v fail=%d ok=%d active=%d busy=%d workers=%d", results, g.FailCount(), g.OkCount(), g.Active(), g.Busy(), g.workerCount.Load())
		}
		for i, action := range data {
			r := results[i]
			if visits[i].Load() != 1 || !r.Occupied {
				t.Fatalf("submission %d visits=%d result=%+v", i, visits[i].Load(), r)
			}
			switch action % 4 {
			case 0:
				if r.Value != i+1 || r.Err != nil {
					t.Fatalf("success %d: %+v", i, r)
				}
			case 1:
				if r.Value != i+1 || r.Err != businessError {
					t.Fatalf("business error %d: %+v", i, r)
				}
			case 2:
				var pe *core.PanicError
				if !errors.As(r.Err, &pe) || pe.Value != i || r.Value != 0 {
					t.Fatalf("panic %d: %+v", i, r)
				}
			case 3:
				if !errors.Is(r.Err, core.ErrTaskGoexit) || r.IsPanic() || r.Value != 0 {
					t.Fatalf("Goexit %d: %+v", i, r)
				}
			}
		}
	})
}
