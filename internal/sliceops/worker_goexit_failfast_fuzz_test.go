package sliceops

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// With one worker, FailFast must prevent every business invocation following
// the first failure, whether it returns an error, panics, or exits the goroutine.
func FuzzSliceWorkerFailFastOutcomes(f *testing.F) {
	for mode := uint8(0); mode < 4; mode++ {
		f.Add([]byte{0, 3, 0, 0}, mode, true)
		f.Add([]byte{3, 0, 2, 1}, mode, false)
	}
	f.Fuzz(func(t *testing.T, input []byte, mode uint8, failFast bool) {
		if len(input) > 24 {
			input = input[:24]
		}
		items := make([]int, len(input))
		calls := make([]atomic.Int32, len(input))
		stop := len(input)
		for i, b := range input {
			items[i] = i + 1
			if failFast && stop == len(input) && b%4 != 0 {
				stop = i
			}
		}
		businessErr := errors.New("business")
		fn := func(_ context.Context, v int) (int, error) {
			calls[v-1].Add(1)
			switch input[v-1] % 4 {
			case 1:
				return v * 10, businessErr
			case 2:
				panic(v)
			case 3:
				runtime.Goexit()
			}
			return v * 10, nil
		}
		p := Par(1).Buf(1)
		if failFast {
			p = p.FF()
		}
		if mode%4 == 0 || mode%4 == 2 {
			pl := pool.NewPool[int](1)
			defer pl.Close()
			p = p.WithPool(pl)
		}
		counts := [4]int{}
		observe := func(err error) {
			var pe *core.PanicError
			switch {
			case err == nil:
				counts[0]++
			case errors.Is(err, businessErr):
				counts[1]++
			case errors.As(err, &pe):
				counts[2]++
			case errors.Is(err, core.ErrTaskGoexit):
				counts[3]++
			case errors.Is(err, context.Canceled):
			default:
				t.Fatalf("unexpected err=%v", err)
			}
		}
		switch mode % 4 {
		case 0:
			rs, _ := execMap(context.Background(), items, fn, p)
			for _, r := range rs {
				observe(r.Err)
			}
		case 3:
			for r := range execEachStream(context.Background(), items, func(ctx context.Context, v int) error { _, err := fn(ctx, v); return err }, p) {
				observe(r.Err)
			}
		default:
			for r := range execStream(context.Background(), items, fn, p) {
				observe(r.Err)
			}
		}
		wantCounts := [4]int{}
		for i, b := range input {
			wantCalls := int32(0)
			if i <= stop {
				wantCalls = 1
				wantCounts[b%4]++
			}
			if calls[i].Load() != wantCalls {
				t.Fatalf("index=%d calls=%d want=%d mode=%d FF=%v input=%v", i, calls[i].Load(), wantCalls, mode%4, failFast, input)
			}
		}
		if counts != wantCounts {
			t.Fatalf("observed=%v want=%v mode=%d FF=%v input=%v", counts, wantCounts, mode%4, failFast, input)
		}
	})
}
