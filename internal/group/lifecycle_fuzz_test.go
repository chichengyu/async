package group

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// Indexed results give a scheduling-independent oracle across owned workers,
// an injected pool, repeated Wait and Reset. No timeout/cancellation or concurrent
// configuration is modeled; callback actions are pure successes/errors/panics.
func FuzzGroupIndexedLifecycle(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 255}, uint8(3))
	f.Add([]byte{2, 2, 1, 0}, uint8(255))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 32 {
			data = data[:32]
		}
		ctx := context.Background()
		wantErr := errors.New("model task error")
		workers := int(config%4) + 1
		for engine := range 2 {
			g := NewGroup[int](workers)
			defer g.Close()
			if engine == 1 {
				p := pool.NewPool[int](int(config%3) + 1).WithMaxResults(0)
				defer p.Close()
				g.WithPool(p)
			}
			for round := range 2 {
				visits := make([]atomic.Int32, len(data))
				var callbacks atomic.Int32
				g.WithResultCallback(func(core.Result[int]) { callbacks.Add(1) })
				wantFailures := int64(0)
				for i, b := range data {
					if (int(b)+round)%3 != 0 {
						wantFailures++
					}
					err := g.GoAt(i, ctx, func(context.Context) (int, error) {
						visits[i].Add(1)
						switch (int(b) + round) % 3 {
						case 1:
							return i, wantErr
						case 2:
							panic(i)
						default:
							return i*7 + round, nil
						}
					})
					if err != nil {
						t.Fatalf("engine %d round %d submission %d: %v", engine, round, i, err)
					}
				}
				for read := range 2 {
					results := g.Wait()
					if len(results) != len(data) {
						t.Fatalf("engine %d round %d read %d: %d results want %d", engine, round, read, len(results), len(data))
					}
					for i, result := range results {
						if !result.Occupied || visits[i].Load() != 1 {
							t.Fatalf("engine %d item %d: %+v visits %d", engine, i, result, visits[i].Load())
						}
						switch (int(data[i]) + round) % 3 {
						case 0:
							if result.Err != nil || result.Value != i*7+round {
								t.Fatalf("success %d: %+v", i, result)
							}
						case 1:
							if !errors.Is(result.Err, wantErr) || result.Value != i {
								t.Fatalf("business failure %d: %+v", i, result)
							}
						case 2:
							var pe *core.PanicError
							if !errors.As(result.Err, &pe) || pe.Value != i || result.Value != 0 {
								t.Fatalf("panic %d: %+v", i, result)
							}
						}
					}
				}
				if g.TotalCount() != int64(len(data)) || g.FailCount() != wantFailures || g.OkCount() != int64(len(data))-wantFailures || g.Active() != 0 || g.Busy() != 0 || callbacks.Load() != int32(len(data)) {
					t.Fatalf("engine %d round %d stats %+v callbacks %d", engine, round, g.Stats(), callbacks.Load())
				}
				if round == 0 {
					if _, err := g.Reset(); err != nil {
						t.Fatal(err)
					}
					if g.TotalCount() != 0 || g.FailCount() != 0 || g.OkCount() != 0 {
						t.Fatal("Reset retained counters")
					}
				}
			}
			g.Close()
		}
	})
}
