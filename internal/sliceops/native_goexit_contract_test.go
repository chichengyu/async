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

func TestSliceNativeGoexitRecordsAndKeepsRemainingWork(t *testing.T) {
	for _, mode := range []string{"map", "map-ff", "shard-map", "shard-each"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			fn := func(_ context.Context, v int) (int, error) {
				calls.Add(1)
				if v == 1 {
					runtime.Goexit()
				}
				return v * 10, nil
			}
			items := []int{1, 2, 3, 4, 5, 6}
			var results []core.Result[int]
			var err error
			switch mode {
			case "map":
				results, err = execMap(context.Background(), items, fn, Par(1))
			case "map-ff":
				results, err = execMap(context.Background(), items, fn, Par(1).FF())
			case "shard-map":
				results, err = execMap(context.Background(), items, fn, Par(1).Shard(3))
			case "shard-each":
				total, fail, first, rs := execEach(context.Background(), items, func(ctx context.Context, v int) error { _, e := fn(ctx, v); return e }, Par(1).Shard(3))
				if total != 6 || fail != 1 || !errors.Is(first, core.ErrTaskGoexit) {
					t.Errorf("total=%d fail=%d first=%v", total, fail, first)
				}
				if calls.Load() != 6 || len(rs) != 6 || !errors.Is(rs[0].Err, core.ErrTaskGoexit) {
					t.Errorf("calls=%d results=%+v", calls.Load(), rs)
				}
				for _, r := range rs {
					if !r.Occupied {
						t.Errorf("unoccupied=%+v", r)
					}
				}
				return
			}
			if len(results) != 6 || !errors.Is(results[0].Err, core.ErrTaskGoexit) || results[0].Value != 0 || results[0].IsPanic() {
				t.Errorf("results=%+v", results)
			}
			if mode == "map-ff" {
				if calls.Load() != 1 || !errors.Is(err, core.ErrTaskGoexit) {
					t.Errorf("calls=%d err=%v", calls.Load(), err)
				}
				for _, r := range results[1:] {
					if !errors.Is(r.Err, context.Canceled) {
						t.Errorf("not canceled=%+v", r)
					}
				}
			} else {
				if calls.Load() != 6 || err != nil {
					t.Errorf("lost work calls=%d err=%v", calls.Load(), err)
				}
				for i, r := range results[1:] {
					if r.Err != nil || r.Value != (i+2)*10 {
						t.Errorf("result[%d]=%+v", i+1, r)
					}
				}
			}
		})
	}
}

func TestSliceNativeGoexitFailFastAndCancellation(t *testing.T) {
	for _, each := range []bool{false, true} {
		for _, parentCancel := range []bool{false, true} {
			ctx, cancel := context.WithCancel(context.Background())
			p := Par(1).Shard(3).TO(time.Hour)
			if !parentCancel {
				p = p.FF()
			}
			var calls atomic.Int32
			var taskContext context.Context
			fn := func(ctx context.Context, v int) (int, error) {
				calls.Add(1)
				taskContext = ctx
				if parentCancel {
					cancel()
				}
				runtime.Goexit()
				return 0, nil
			}
			items := []int{1, 2, 3, 4, 5, 6}
			if each {
				total, fails, err, rs := execEach(ctx, items, func(ctx context.Context, v int) error { _, e := fn(ctx, v); return e }, p)
				if total != 6 || fails != 6 || !errors.Is(err, core.ErrTaskGoexit) || len(rs) != 6 {
					t.Fatalf("total=%d fails=%d err=%v results=%+v", total, fails, err, rs)
				}
				for i, r := range rs {
					want := error(context.Canceled)
					if i == 0 {
						want = core.ErrTaskGoexit
					}
					if !r.Occupied || !errors.Is(r.Err, want) {
						t.Fatalf("index=%d result=%+v want=%v", i, r, want)
					}
				}
			} else {
				rs, err := execMap(ctx, items, fn, p)
				if (!parentCancel && !errors.Is(err, core.ErrTaskGoexit)) || (parentCancel && err != nil) || len(rs) != 6 {
					t.Fatalf("err=%v results=%+v", err, rs)
				}
				for i, r := range rs {
					want := error(context.Canceled)
					if i == 0 {
						want = core.ErrTaskGoexit
					}
					if !r.Occupied || !errors.Is(r.Err, want) {
						t.Fatalf("index=%d result=%+v want=%v", i, r, want)
					}
				}
			}
			cancel()
			if calls.Load() != 1 || taskContext == nil || taskContext.Err() != context.Canceled {
				t.Fatalf("calls=%d taskContext=%v", calls.Load(), taskContext)
			}
		}
	}
}
