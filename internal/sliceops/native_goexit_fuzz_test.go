package sliceops

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Removing successor handoff, losing a shard cursor, suppressing Goexit errors,
// or counting a failure twice must disagree with this per-input outcome model.
func FuzzSliceNativeMixedTerminalOutcomes(f *testing.F) {
	f.Add([]byte{3, 0, 1, 2, 3, 0}, uint8(0), uint8(0), uint8(2))
	f.Add([]byte{3, 3, 3, 3, 3, 3, 3}, uint8(1), uint8(1), uint8(3))
	f.Add([]byte{3, 1, 2, 0}, uint8(2), uint8(2), uint8(1))
	f.Fuzz(func(t *testing.T, input []byte, workers, mode, shards uint8) {
		if len(input) > 32 {
			input = input[:32]
		}
		items := make([]int, len(input))
		calls := make([]atomic.Int32, len(input))
		for i := range items {
			items[i] = i + 1
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
		p := Par(int(workers%4) + 1)
		if mode%3 != 0 {
			p = p.Shard(int(shards%8) + 1).TO(time.Hour)
		}
		if mode%3 == 2 {
			total, fails, first, rs := execEach(context.Background(), items, func(ctx context.Context, v int) error { _, err := fn(ctx, v); return err }, p)
			wantFails := int64(0)
			for _, b := range input {
				if b%4 != 0 {
					wantFails++
				}
			}
			if total != int64(len(items)) || fails != wantFails || (first == nil) != (wantFails == 0) || len(rs) != len(items) {
				t.Fatalf("total=%d fails=%d want=%d first=%v results=%d", total, fails, wantFails, first, len(rs))
			}
			for i, r := range rs {
				if calls[i].Load() != 1 || !r.Occupied {
					t.Fatalf("index=%d calls=%d result=%+v", i, calls[i].Load(), r)
				}
				checkSliceNativeOutcome(t, input[i]%4, r.Err, businessErr)
			}
			return
		}
		rs, err := execMap(context.Background(), items, fn, p)
		if err != nil || len(rs) != len(items) {
			t.Fatalf("err=%v len=%d want=%d", err, len(rs), len(items))
		}
		for i, r := range rs {
			want := (i + 1) * 10
			if input[i]%4 >= 2 {
				want = 0
			}
			if calls[i].Load() != 1 || r.Value != want || r.Occupied != (mode%3 != 0) {
				t.Fatalf("index=%d calls=%d result=%+v want=%d", i, calls[i].Load(), r, want)
			}
			checkSliceNativeOutcome(t, input[i]%4, r.Err, businessErr)
		}
	})
}

func checkSliceNativeOutcome(t *testing.T, outcome byte, err, businessErr error) {
	t.Helper()
	var pe *core.PanicError
	switch outcome {
	case 0:
		if err != nil {
			t.Fatalf("success err=%v", err)
		}
	case 1:
		if !errors.Is(err, businessErr) {
			t.Fatalf("business err=%v", err)
		}
	case 2:
		if !errors.As(err, &pe) {
			t.Fatalf("panic err=%v", err)
		}
	case 3:
		if !errors.Is(err, core.ErrTaskGoexit) || errors.As(err, &pe) {
			t.Fatalf("Goexit err=%v", err)
		}
	}
}
