package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// Model explicit result positions without calling allocation/storage helpers.
// Complete each valid task before the next submission so overwrite order is fixed.
func FuzzPoolIndexedResults(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{48, 0, 49, 1, 50, 2, 51}, uint8(0))
	f.Add([]byte{59, 16, 33, 0, 48, 11, 32}, uint8(3))
	f.Add([]byte{48, 48, 0, 0, 16, 32, 60}, uint8(0))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 32 {
			data = data[:32]
		}
		limit := int(config % 5)
		p := NewPool[int](1).WithMaxResults(limit)
		defer p.Close()
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		businessErr := errors.New("indexed business failure")
		expected := make(map[int]core.Result[int])
		next, total, failed := 0, 0, 0
		for step, b := range data {
			index, mode := int(b%12), b/16%4
			ctx := context.Background()
			want := core.Result[int]{Value: step + 1, Occupied: true}
			wantCalls := int32(1)
			var wantSubmit error
			switch mode {
			case 0:
				ctx = canceled
				want.Value, want.Err = 0, context.Canceled
				wantSubmit, wantCalls = context.Canceled, 0
			case 1:
				index = -1
				if b&1 != 0 {
					index = core.MaxPoolIndex + 1
				}
				want.Value, want.Err = 0, core.ErrInvalidIndex
				wantSubmit, wantCalls = core.ErrInvalidIndex, 0
			case 2:
				want.Err = businessErr
			}
			var calls, completions atomic.Int32
			completed := make(chan core.Result[int], 1)
			err := p.SubmitAtWithCompletion(index, ctx, func(context.Context) (int, error) {
				calls.Add(1)
				return step + 1, want.Err
			}, func(r core.Result[int]) {
				if completions.Add(1) == 1 {
					completed <- r
				}
			})
			got := <-completed
			if !errors.Is(err, wantSubmit) || got != want || calls.Load() != wantCalls || completions.Load() != 1 {
				t.Fatalf("step %d completion=%+v want=%+v submit=%v want=%v calls=%d completions=%d", step, got, want, err, wantSubmit, calls.Load(), completions.Load())
			}
			if mode == 1 {
				continue // Illegal positions have a completion, but no stored outcome or counters.
			}
			total++
			if want.Err != nil {
				failed++
			}
			position := index
			if mode == 0 && index < next {
				position = next // A rejected reused position preserves the earlier result.
			}
			if position >= next {
				next = position + 1
			}
			expected[position] = want
		}
		results := p.Wait()
		p.Close() // Wait releases at task completion, just before worker Active is decremented.
		wantLength := next
		if limit > 0 && wantLength > limit {
			wantLength = limit
		}
		if len(results) != wantLength {
			t.Fatalf("result length %d want %d", len(results), wantLength)
		}
		for i, got := range results {
			if got != expected[i] {
				t.Fatalf("slot %d got=%+v want=%+v", i, got, expected[i])
			}
		}
		if p.TotalCount() != int64(total) || p.FailCount() != int64(failed) || p.OkCount() != int64(total-failed) || p.Pending() != 0 || p.Busy() != 0 || p.Active() != 0 {
			t.Fatalf("indexed counters=%+v want total/fail=%d/%d", p.Stats(), total, failed)
		}
	})
}
