package maps

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/sliceops"
)

// The oracle records the actual randomized batch membership, then derives
// outcomes from its smallest key. It never assumes map iteration order.
func FuzzMapMixedTerminalStrategies(f *testing.F) {
	for mode := uint8(0); mode < 5; mode++ {
		for backend := uint8(0); backend < 3; backend++ {
			f.Add([]byte{3, 0, 2, 1, 0, 3, 0, 2, 1}, mode, backend, uint8(2), uint8(3), false)
			f.Add([]byte{0, 3, 0, 1, 2, 0, 3}, mode, backend, uint8(3), uint8(1), true)
		}
	}
	f.Fuzz(func(t *testing.T, input []byte, mode, backend, batch, config uint8, failFast bool) {
		if len(input) > 32 {
			input = input[:32]
		}
		n := len(input)
		data := make(map[int]int, n)
		for k := range input {
			data[k+1] = (k + 1) * 7
		}
		workers := int(config%4) + 1
		if failFast {
			workers = 1
		}
		policy := sliceops.Par(workers).Shard(int(config%7) + 1).Chunk(int(batch%7) + 1).TO(time.Hour)
		if failFast {
			policy = policy.FF()
		}
		switch backend % 3 {
		case 1:
			policy = policy.WithDefaultPool()
		case 2:
			pl := pool.NewPool[int](workers).WithMaxResults(1).WithRingBuffer(1, core.OverflowDrop)
			defer pl.Close()
			policy = policy.WithPool(pl)
		}
		type call struct {
			head, size, value int
			outcome           byte
			ctx               context.Context
		}
		var mu sync.Mutex
		var calls []call
		seen := make([]int, n)
		businessErr := errors.New("mixed batch error")
		invoke := func(ctx context.Context, entries []MapEntry[int, int]) (int, error) {
			minimum, value := n+1, 0
			mu.Lock()
			for _, e := range entries {
				if e.Key < 1 || e.Key > n || e.Val != e.Key*7 {
					t.Errorf("unexpected entry=%+v", e)
					continue
				}
				seen[e.Key-1]++
				minimum = min(minimum, e.Key)
				value += e.Val
			}
			outcome := input[minimum-1] % 4
			calls = append(calls, call{entries[0].Key, len(entries), value, outcome, ctx})
			mu.Unlock()
			switch outcome {
			case 1:
				return value, businessErr
			case 2:
				panic("mixed batch panic")
			case 3:
				runtime.Goexit()
			}
			return value, nil
		}
		fn := func(ctx context.Context, k, v int) (int, error) {
			return invoke(ctx, []MapEntry[int, int]{{Key: k, Val: v}})
		}
		r := NewMapRunner[int, int, int](context.Background(), data, policy)
		var mapped *MapResult[int, int]
		var terminal error
		var total, failures int64
		var streamed []core.Result[int]
		switch mode % 5 {
		case 0:
			mapped = r.Map(fn)
		case 1:
			total, failures, terminal = r.ForEach(func(ctx context.Context, k, v int) error { _, e := fn(ctx, k, v); return e })
		case 2:
			for result := range r.Stream(fn, 1) {
				streamed = append(streamed, result)
			}
		case 3:
			mapped = r.MapBatch(invoke)
		case 4:
			total, failures, terminal = r.ForEachBatch(func(ctx context.Context, es []MapEntry[int, int]) error { _, e := invoke(ctx, es); return e })
		}
		wantValues := make(map[int]int)
		wantCounts := [4]int{}
		wantStream := make(map[[2]int]int)
		var visited, failedElements int64
		for i, c := range calls {
			wantCounts[c.outcome]++
			streamValue := c.value
			if c.outcome >= 2 {
				streamValue = 0
			}
			wantStream[[2]int{int(c.outcome), streamValue}]++
			if failFast && backend%3 != 1 && c.outcome != 0 && i != len(calls)-1 {
				t.Fatalf("FailFast invoked business after failure: %+v", calls)
			}
			visited += int64(c.size)
			if c.outcome == 0 {
				wantValues[c.head] = c.value
			} else {
				failedElements += int64(c.size)
			}
			if c.ctx.Err() != context.Canceled {
				t.Fatalf("callback context not cleaned: %v", c.ctx.Err())
			}
		}
		for k, count := range seen {
			if count > 1 || (!failFast && count != 1) {
				t.Fatalf("key=%d visits=%d FF=%v", k+1, count, failFast)
			}
		}
		// The default pool uses its own IO() workers, ignoring policy.Worker.
		if failFast && backend%3 != 1 && wantCounts[1]+wantCounts[2]+wantCounts[3] > 1 {
			t.Fatalf("FailFast invoked later failures: %v", wantCounts)
		}
		if failedElements == 0 && visited != int64(n) {
			t.Fatalf("stopped without failure: visited=%d want=%d", visited, n)
		}
		if mapped != nil {
			terminal = mapped.Error()
			if mapped.Len() != len(wantValues) {
				t.Fatalf("mapped=%v want=%v", mapped.Data(), wantValues)
			}
			for k, v := range wantValues {
				if got, ok := mapped.Data()[k]; !ok || got != v {
					t.Fatalf("key=%d got=%d/%v want=%d", k, got, ok, v)
				}
			}
		}
		if mode%5 == 1 || mode%5 == 4 {
			wantTotal, wantFailures := visited, failedElements
			if backend%3 != 0 {
				wantTotal = int64(n)
				wantFailures += int64(n) - visited
			}
			if total != wantTotal || failures != wantFailures {
				t.Fatalf("stats=%d/%d want=%d/%d", total, failures, wantTotal, wantFailures)
			}
		}
		if mode%5 != 2 {
			if (terminal != nil) != (failedElements > 0) {
				t.Fatalf("terminal=%v failures=%d", terminal, failedElements)
			}
			if terminal != nil {
				var pe *core.PanicError
				switch {
				case errors.Is(terminal, businessErr) && wantCounts[1] > 0:
				case errors.As(terminal, &pe) && wantCounts[2] > 0:
				case errors.Is(terminal, core.ErrTaskGoexit) && wantCounts[3] > 0:
				default:
					t.Fatalf("terminal=%v does not match business outcomes=%v", terminal, wantCounts)
				}
			}
		} else {
			gotCounts := [4]int{}
			gotStream := make(map[[2]int]int)
			for _, result := range streamed {
				var pe *core.PanicError
				outcome := 0
				switch {
				case result.Err == nil:
					gotCounts[0]++
				case errors.Is(result.Err, businessErr):
					gotCounts[1]++
					outcome = 1
				case errors.As(result.Err, &pe):
					gotCounts[2]++
					outcome = 2
				case errors.Is(result.Err, core.ErrTaskGoexit):
					gotCounts[3]++
					outcome = 3
				case backend%3 != 0 && errors.Is(result.Err, context.Canceled):
					continue
				default:
					t.Fatalf("unexpected stream error=%v", result.Err)
				}
				gotStream[[2]int{outcome, result.Value}]++
			}
			if gotCounts != wantCounts {
				t.Fatalf("stream=%v want=%v", gotCounts, wantCounts)
			}
			if len(gotStream) != len(wantStream) {
				t.Fatalf("stream values=%v want=%v", gotStream, wantStream)
			}
			for key, count := range wantStream {
				if gotStream[key] != count {
					t.Fatalf("stream key=%v count=%d want=%d", key, gotStream[key], count)
				}
			}
		}
	})
}
