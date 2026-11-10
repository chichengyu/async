package maps

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/sliceops"
)

// A fixed outcome for every actual chunk permits an order-independent oracle:
// no test assumption depends on Go's randomized map iteration or chunk keys.
func FuzzMapNativeTerminalOutcomes(f *testing.F) {
	for mode := uint8(0); mode < 5; mode++ {
		f.Add(uint8(9), uint8(1), uint8(3), uint8(2), mode, uint8(3), false)
		f.Add(uint8(8), uint8(1), uint8(2), uint8(3), mode, uint8(3), true)
	}
	f.Fuzz(func(t *testing.T, size, workers, shards, batch, mode, outcome uint8, failFast bool) {
		n := int(size % 25)
		chunkSize := int(batch%6) + 1
		data := make(map[int]int, n)
		for i := 1; i <= n; i++ {
			data[i] = i
		}
		var calls, observedElements atomic.Int32
		seen := make([]atomic.Int32, n)
		businessErr := errors.New("business")
		invoke := func() (int, error) {
			calls.Add(1)
			switch outcome % 4 {
			case 1:
				return 17, businessErr
			case 2:
				panic("business")
			case 3:
				runtime.Goexit()
			}
			return 17, nil
		}
		p := sliceops.Par(int(workers%4) + 1).Shard(int(shards%8) + 1).Chunk(chunkSize).TO(time.Hour)
		if failFast {
			p = p.FF().Worker(1)
		}
		r := NewMapRunner[int, int, int](context.Background(), data, p)
		fn := func(_ context.Context, k, v int) (int, error) {
			if k != v || k < 1 || k > n {
				t.Errorf("entry=%d/%d", k, v)
			}
			seen[k-1].Add(1)
			observedElements.Add(1)
			return invoke()
		}
		batchFn := func(_ context.Context, entries []MapEntry[int, int]) (int, error) {
			for _, e := range entries {
				seen[e.Key-1].Add(1)
			}
			observedElements.Add(int32(len(entries)))
			return invoke()
		}
		wantCalls := n
		if mode%5 >= 3 {
			wantCalls = (n + chunkSize - 1) / chunkSize
		}
		if failFast && outcome%4 != 0 && wantCalls > 0 {
			wantCalls = 1
		}
		var terminal error
		switch mode % 5 {
		case 0, 3:
			var result *MapResult[int, int]
			if mode%5 == 0 {
				result = r.Map(fn)
			} else {
				result = r.MapBatch(batchFn)
			}
			terminal = result.Error()
			wantLen := wantCalls
			if outcome%4 != 0 {
				wantLen = 0
			}
			if result.Len() != wantLen {
				t.Fatalf("results=%d want=%d", result.Len(), wantLen)
			}
			for _, v := range result.Data() {
				if v != 17 {
					t.Fatalf("value=%d", v)
				}
			}
		case 1, 4:
			var total, fails int64
			if mode%5 == 1 {
				total, fails, terminal = r.ForEach(func(ctx context.Context, k, v int) error { _, e := fn(ctx, k, v); return e })
			} else {
				total, fails, terminal = r.ForEachBatch(func(ctx context.Context, es []MapEntry[int, int]) error { _, e := batchFn(ctx, es); return e })
			}
			wantTotal := int64(n)
			if failFast && outcome%4 != 0 {
				wantTotal = 0
				if n > 0 {
					wantTotal = 1
					if mode%5 == 4 {
						wantTotal = int64(min(n, chunkSize))
					}
				}
			}
			wantFails := int64(0)
			if outcome%4 != 0 {
				wantFails = wantTotal
			}
			if total != wantTotal || fails != wantFails || int64(observedElements.Load()) != wantTotal {
				t.Fatalf("total=%d fails=%d observed=%d want=%d/%d", total, fails, observedElements.Load(), wantTotal, wantFails)
			}
		case 2:
			count := 0
			for result := range r.Stream(fn, 1) {
				count++
				terminal = result.Err
				checkMapNativeError(t, outcome%4, result.Err, businessErr)
				wantValue := 17
				if outcome%4 >= 2 {
					wantValue = 0
				}
				if result.Value != wantValue {
					t.Fatalf("result=%+v want=%d", result, wantValue)
				}
			}
			if count != wantCalls {
				t.Fatalf("results=%d want=%d", count, wantCalls)
			}
		}
		if calls.Load() != int32(wantCalls) {
			t.Fatalf("calls=%d want=%d mode=%d FF=%v outcome=%d", calls.Load(), wantCalls, mode%5, failFast, outcome%4)
		}
		if n > 0 {
			checkMapNativeError(t, outcome%4, terminal, businessErr)
		} else if terminal != nil {
			t.Fatalf("empty err=%v", terminal)
		}
		for i := range seen {
			if seen[i].Load() > 1 || ((!failFast || outcome%4 == 0) && seen[i].Load() != 1) {
				t.Fatalf("seen[%d]=%d", i, seen[i].Load())
			}
		}
	})
}

func checkMapNativeError(t *testing.T, outcome uint8, err, businessErr error) {
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
