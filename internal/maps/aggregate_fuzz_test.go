package maps

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/sliceops"
)

type aggregateExecutor interface {
	Filter(func(context.Context, int, int) bool) (*Map[int, int], error)
	Reduce(int, func(context.Context, int, int, int) (int, error)) (int, error)
}

// The oracle uses fixed input slots and callback visit history, never map order.
// Positive timeouts are deliberately long: this checks context ownership and
// cleanup, while real deadline expiry is covered by TestMapTimeoutAllCallbacksRegression.
func FuzzMapAggregateContracts(f *testing.F) {
	f.Add([]byte{}, uint8(0), uint8(0))
	f.Add([]byte{1, 2, 255, 0}, uint8(0), uint8(0))
	f.Add([]byte{1, 2, 3}, uint8(3), uint8(0))
	f.Add([]byte{128, 7, 9}, uint8(7), uint8(1))
	f.Add([]byte{4, 5, 6}, uint8(11), uint8(0))
	f.Add([]byte{1, 2}, uint8(15), uint8(1))
	f.Fuzz(func(t *testing.T, input []byte, flags, stopByte uint8) {
		if len(input) > 16 {
			input = input[:16]
		}
		data := make(map[int]int, len(input))
		for k, b := range input {
			data[k] = int(int8(b))
		}
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		if flags&1 != 0 {
			cancel()
		}
		timed := flags&2 != 0
		outcome := (flags >> 2) % 3 // success, business error, panic
		stop := 1 + int(stopByte)%max(1, len(input))
		businessErr := errors.New("aggregate model failure")
		panicValue := &struct{ tag string }{"aggregate model panic"}
		for mode := 0; mode < 7; mode++ {
			newExecutor := func() aggregateExecutor {
				policy := sliceops.Seq()
				switch mode {
				case 1:
					policy = sliceops.Par(4).Shard(3).FF()
				case 2:
					policy = sliceops.Par(4).WithDefaultPool().FF()
				}
				if timed {
					policy = policy.TO(time.Hour)
				}
				if mode < 3 {
					return NewMapRunner[int, int, int](parent, data, policy)
				}
				chain := NewMapChain[int, int](parent, data)
				duration := time.Duration(0)
				if timed {
					duration = time.Hour
				}
				switch mode {
				case 3:
					return chain.Worker(4).Shards(3).FailFast().Timeout(duration)
				case 4:
					return chain.Serial().FailFast().Timeout(duration)
				case 5:
					return chain.Parallel().Worker(4).Shards(3).FailFast().Timeout(duration)
				default:
					return chain.DefaultPool().FailFast().Timeout(duration)
				}
			}
			for _, method := range []string{"filter", "reduce"} {
				executor := newExecutor()
				seen := make([]bool, len(input))
				contexts := make([]context.Context, 0, len(input))
				calls, sum := 0, 19
				check := func(ctx context.Context, k, v int) {
					if k < 0 || k >= len(input) || seen[k] || v != int(int8(input[k])) {
						t.Fatalf("mode %d %s invalid/duplicate input %d=%d", mode, method, k, v)
					}
					seen[k] = true
					if _, hasDeadline := ctx.Deadline(); hasDeadline != timed {
						t.Fatalf("mode %d %s deadline=%v want %v", mode, method, hasDeadline, timed)
					}
					if (ctx.Err() != nil) != (flags&1 != 0) {
						t.Fatalf("mode %d %s parent cancellation=%v", mode, method, ctx.Err())
					}
					for _, previous := range contexts {
						if timed && (previous == ctx || !errors.Is(previous.Err(), context.Canceled)) {
							t.Fatalf("mode %d %s previous callback context still live/reused", mode, method)
						}
					}
					contexts = append(contexts, ctx)
					calls++
					sum += v
					if outcome == 2 && calls == stop {
						panic(panicValue)
					}
				}
				var filtered *Map[int, int]
				var value int
				var err error
				var recovered any
				func() {
					defer func() { recovered = recover() }()
					if method == "filter" {
						filtered, err = executor.Filter(func(ctx context.Context, k, v int) bool {
							check(ctx, k, v)
							return (k+v)%3 == 0
						})
					} else {
						value, err = executor.Reduce(19, func(ctx context.Context, acc, k, v int) (int, error) {
							if acc != sum {
								t.Fatalf("mode %d accumulator=%d want %d", mode, acc, sum)
							}
							check(ctx, k, v)
							if outcome == 1 && calls == stop {
								return sum - 70000, businessErr
							}
							return acc + v, nil
						})
					}
				}()
				for _, ctx := range contexts {
					if timed && !errors.Is(ctx.Err(), context.Canceled) {
						t.Fatalf("mode %d %s callback context not released: %v", mode, method, ctx.Err())
					}
				}
				wantPanic := outcome == 2 && len(input) > 0
				if (recovered != nil) != wantPanic || (wantPanic && recovered != panicValue) {
					t.Fatalf("mode %d %s panic=%v want propagated=%v", mode, method, recovered, wantPanic)
				}
				wantCalls := len(input)
				failed := method == "reduce" && outcome == 1 && len(input) > 0
				if wantPanic || failed {
					wantCalls = stop
				}
				if calls != wantCalls || errors.Is(err, businessErr) != failed {
					t.Fatalf("mode %d %s calls=%d want %d err=%v", mode, method, calls, wantCalls, err)
				}
				if !wantPanic && method == "reduce" {
					want := sum
					if failed {
						want -= 70000
					}
					if value != want || (!failed && err != nil) {
						t.Fatalf("mode %d reduce=%d,%v want %d", mode, value, err, want)
					}
				}
				if !wantPanic && method == "filter" {
					if err != nil || filtered == nil {
						t.Fatalf("mode %d filter=%v,%v", mode, filtered, err)
					}
					wantLen := 0
					for k, b := range input {
						v := int(int8(b))
						got, ok := filtered.Get(k)
						keep := (k+v)%3 == 0
						if ok != keep || (ok && got != v) {
							t.Fatalf("mode %d filter key %d=%d,%v", mode, k, got, ok)
						}
						if keep {
							wantLen++
						}
					}
					if filtered.Len() != wantLen {
						t.Fatalf("mode %d filter length=%d want %d", mode, filtered.Len(), wantLen)
					}
					filtered.Set(-1, 123) // New result must not alias input keys.
				}
				if len(data) != len(input) {
					t.Fatalf("mode %d %s changed input size", mode, method)
				}
				for k, b := range input {
					if v, ok := data[k]; !ok || v != int(int8(b)) {
						t.Fatalf("mode %d %s changed input %d", mode, method, k)
					}
				}
			}
		}
	})
}
