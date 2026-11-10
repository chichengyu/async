package maps

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/sliceops"
)

func TestMapNativeGoexitRecordsAndKeepsRemainingWork(t *testing.T) {
	for _, mode := range []string{"map", "each", "stream", "map-batch", "each-batch"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			r := NewMapRunner[int, int, int](context.Background(), map[int]int{1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6}, sliceops.Par(1).Shard(3).Chunk(1))
			fn := func(_ context.Context, _ int, v int) (int, error) {
				if calls.Add(1) == 1 {
					runtime.Goexit()
				}
				return v * 10, nil
			}
			if mode == "stream" {
				n, failures := 0, 0
				for result := range r.Stream(fn, 1) {
					n++
					if errors.Is(result.Err, core.ErrTaskGoexit) {
						failures++
					} else if result.Err != nil || result.Value == 0 {
						t.Errorf("result=%+v", result)
					}
				}
				if n != 6 || failures != 1 {
					t.Fatalf("results=%d failures=%d calls=%d", n, failures, calls.Load())
				}
			} else if mode == "each" || mode == "each-batch" {
				var total, fails int64
				var err error
				if mode == "each-batch" {
					total, fails, err = r.ForEachBatch(func(ctx context.Context, entries []MapEntry[int, int]) error {
						_, err := fn(ctx, entries[0].Key, entries[0].Val)
						return err
					})
				} else {
					total, fails, err = r.ForEach(func(ctx context.Context, k, v int) error { _, err := fn(ctx, k, v); return err })
				}
				if total != 6 || fails != 1 || !errors.Is(err, core.ErrTaskGoexit) {
					t.Fatalf("total=%d fails=%d err=%v calls=%d", total, fails, err, calls.Load())
				}
			} else {
				var result *MapResult[int, int]
				if mode == "map-batch" {
					result = r.MapBatch(func(ctx context.Context, entries []MapEntry[int, int]) (int, error) {
						return fn(ctx, entries[0].Key, entries[0].Val)
					})
				} else {
					result = r.Map(fn)
				}
				if result.Len() != 5 || !errors.Is(result.Error(), core.ErrTaskGoexit) {
					t.Fatalf("values=%v err=%v calls=%d", result.Data(), result.Error(), calls.Load())
				}
			}
			if calls.Load() != 6 {
				t.Fatalf("calls=%d", calls.Load())
			}
		})
	}
}

func TestMapNativeMixedOutcomesAndContextCleanup(t *testing.T) {
	for _, mode := range []string{"map", "each", "stream", "map-batch", "each-batch"} {
		t.Run(mode, func(t *testing.T) {
			data := map[int]int{1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6}
			businessErr := errors.New("business")
			var mu sync.Mutex
			var contexts []context.Context
			var calls atomic.Int32
			fn := func(ctx context.Context, k, v int) (int, error) {
				mu.Lock()
				contexts = append(contexts, ctx)
				mu.Unlock()
				calls.Add(1)
				switch k {
				case 1, 5:
					runtime.Goexit()
				case 2:
					panic("business")
				case 3:
					return v * 10, businessErr
				}
				return v * 10, nil
			}
			r := NewMapRunner[int, int, int](context.Background(), data, sliceops.Par(3).Shard(2).Chunk(1).TO(time.Hour))
			switch mode {
			case "map", "map-batch":
				var result *MapResult[int, int]
				if mode == "map" {
					result = r.Map(fn)
				} else {
					result = r.MapBatch(func(ctx context.Context, es []MapEntry[int, int]) (int, error) { return fn(ctx, es[0].Key, es[0].Val) })
				}
				if result.Error() == nil || result.Len() != 2 || result.Data()[4] != 40 || result.Data()[6] != 60 {
					t.Fatalf("result=%v err=%v", result.Data(), result.Error())
				}
			case "each", "each-batch":
				var total, fails int64
				var err error
				if mode == "each" {
					total, fails, err = r.ForEach(func(ctx context.Context, k, v int) error { _, e := fn(ctx, k, v); return e })
				} else {
					total, fails, err = r.ForEachBatch(func(ctx context.Context, es []MapEntry[int, int]) error {
						_, e := fn(ctx, es[0].Key, es[0].Val)
						return e
					})
				}
				if total != 6 || fails != 4 || err == nil {
					t.Fatalf("total=%d fails=%d err=%v", total, fails, err)
				}
			case "stream":
				goexits, panics, business, success := 0, 0, 0, 0
				for result := range r.Stream(fn, 1) {
					switch {
					case errors.Is(result.Err, core.ErrTaskGoexit):
						goexits++
						if result.Value != 0 {
							t.Errorf("Goexit value=%d", result.Value)
						}
					case result.IsPanic():
						panics++
						if result.Value != 0 {
							t.Errorf("panic value=%d", result.Value)
						}
					case errors.Is(result.Err, businessErr):
						business++
						if result.Value != 30 {
							t.Errorf("business value=%d", result.Value)
						}
					case result.Err == nil:
						success++
						if result.Value != 40 && result.Value != 60 {
							t.Errorf("success value=%d", result.Value)
						}
					default:
						t.Errorf("unexpected result=%+v", result)
					}
				}
				if goexits != 2 || panics != 1 || business != 1 || success != 2 {
					t.Fatalf("outcomes=%d/%d/%d/%d", goexits, panics, business, success)
				}
			}
			if calls.Load() != 6 || len(contexts) != 6 {
				t.Fatalf("calls=%d contexts=%d", calls.Load(), len(contexts))
			}
			for _, ctx := range contexts {
				if ctx.Err() != context.Canceled {
					t.Fatalf("task context err=%v", ctx.Err())
				}
			}
		})
	}
}
