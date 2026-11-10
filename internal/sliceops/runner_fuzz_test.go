package sliceops

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Pure callbacks and unique input indexes allow an independent exact-once/error
// oracle across execution strategies; no FailFast/cancellation/overflow here.
func FuzzSliceRunnerStrategies(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{0, 1, 2, 3, 255}, uint8(2))
	f.Add([]byte{2, 2, 2, 1, 0, 128}, uint8(255))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 32 {
			data = data[:32]
		}
		items := make([]int, len(data))
		wantFailures := int64(0)
		for i, b := range data {
			items[i] = i
			if b%3 != 0 {
				wantFailures++
			}
		}
		workers := int(config%4) + 1
		policies := []Policy{Seq(), Par(workers), Par(workers).Shard(int(config%7) + 1), Par(workers).Chunk(int(config%5) + 1), Par(workers).TO(time.Hour), Par(workers).WithDefaultPool()}
		wantErr := errors.New("model callback failure")
		for mode, p := range policies {
			r := NewRunner[int, int](context.Background(), items, p)
			visits := make([]atomic.Int32, len(items))
			fn := func(_ context.Context, i int) (int, error) {
				visits[i].Add(1)
				switch data[i] % 3 {
				case 1:
					return i, wantErr
				case 2:
					panic(i)
				default:
					return i*3 + 1, nil
				}
			}
			results, _ := r.Map(fn) // Summary error selection differs; each item is authoritative.
			if len(results) != len(items) {
				t.Fatalf("mode %d Map count %d want %d", mode, len(results), len(items))
			}
			for i, result := range results {
				if visits[i].Load() != 1 {
					t.Fatalf("mode %d Map item %d visits %d", mode, i, visits[i].Load())
				}
				switch data[i] % 3 {
				case 0:
					if result.Err != nil || result.Value != i*3+1 {
						t.Fatalf("mode %d Map success %d: %+v", mode, i, result)
					}
				case 1:
					if !errors.Is(result.Err, wantErr) || result.Value != i {
						t.Fatalf("mode %d Map error %d: %+v", mode, i, result)
					}
				case 2:
					var pe *core.PanicError
					if !errors.As(result.Err, &pe) || pe.Value != i {
						t.Fatalf("mode %d Map panic %d: %+v", mode, i, result)
					}
				}
			}
			eachVisits := make([]atomic.Int32, len(items))
			total, failures, err, _ := r.Each(func(_ context.Context, i int) error {
				eachVisits[i].Add(1)
				switch data[i] % 3 {
				case 1:
					return wantErr
				case 2:
					panic(i)
				default:
					return nil
				}
			})
			if total != int64(len(items)) || failures != wantFailures || (err != nil) != (wantFailures > 0) {
				t.Fatalf("mode %d Each stats %d %d %v", mode, total, failures, err)
			}
			for i := range items {
				if eachVisits[i].Load() != 1 {
					t.Fatalf("mode %d Each item %d calls %d", mode, i, eachVisits[i].Load())
				}
			}
			streamVisits := make([]atomic.Int32, len(items))
			seen := make([]bool, len(items))
			for result := range r.Stream(func(_ context.Context, i int) (int, error) {
				streamVisits[i].Add(1)
				if data[i]%3 != 0 {
					return i, wantErr
				}
				return i, nil
			}, int(config%3)) {
				i := result.Value
				if i < 0 || i >= len(items) || seen[i] || errors.Is(result.Err, wantErr) != (data[i]%3 != 0) {
					t.Fatalf("mode %d Stream result %+v", mode, result)
				}
				seen[i] = true
			}
			for i := range items {
				if !seen[i] || streamVisits[i].Load() != 1 {
					t.Fatalf("mode %d Stream item %d seen %v visits %d", mode, i, seen[i], streamVisits[i].Load())
				}
			}
		}
	})
}
