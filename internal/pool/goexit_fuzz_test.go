package pool

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// The model assigns each submission its outcome independently; Goexit tasks
// must complete exactly once without reducing capacity for later submissions.
func FuzzPoolGoexitMixedOutcomes(f *testing.F) {
	f.Add([]byte{3, 0, 1, 2, 3, 3, 0}, uint8(0))
	f.Add([]byte{3, 3, 3, 3, 0}, uint8(3))
	f.Add([]byte{}, uint8(1))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 32 {
			data = data[:32]
		}
		p := NewPool[int](int(config%4) + 1).WithTimeout(0).WithMaxResults(0)
		defer p.Close()
		visits, completions := make([]atomic.Int32, len(data)), make([]atomic.Int32, len(data))
		businessError := errors.New("business")
		wantFailures := int64(0)
		for i, action := range data {
			if action%4 != 0 {
				wantFailures++
			}
			err := p.SubmitWithCompletion(context.Background(), func(context.Context) (int, error) {
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
			}, func(r core.Result[int]) {
				completions[i].Add(1)
				if !r.Occupied || (r.Err != nil) != (action%4 != 0) {
					t.Errorf("completion %d action %d: %+v", i, action%4, r)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		results := p.Wait()
		if len(results) != len(data) || p.FailCount() != wantFailures || p.OkCount() != int64(len(data))-wantFailures || p.Active() != 0 || p.Busy() != 0 {
			t.Fatalf("results=%+v fail=%d ok=%d active=%d busy=%d", results, p.FailCount(), p.OkCount(), p.Active(), p.Busy())
		}
		for i, action := range data {
			r := results[i]
			if visits[i].Load() != 1 || completions[i].Load() != 1 {
				t.Fatalf("submission %d visits=%d completions=%d", i, visits[i].Load(), completions[i].Load())
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
