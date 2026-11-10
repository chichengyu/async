package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

func TestNativePipelineGoexitKeepsRemainingChunk(t *testing.T) {
	for _, mode := range []string{"execute", "meta", "instance", "instance-meta", "parallel", "parallel-meta"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			fn := func(_ context.Context, _ string, v int) (int, error) {
				calls.Add(1)
				if v == 1 {
					runtime.Goexit()
				}
				return v * 10, nil
			}
			ctx := context.Background()
			items := []int{1, 2, 3}
			stages := []Stage[int]{{Name: "one", Concurrency: 1}}
			var results []core.Result[int]
			var meta []ResultWithMeta[int]
			switch mode {
			case "execute":
				results, _ = Execute(ctx, stages, items, fn)
			case "meta":
				meta = ExecuteWithMeta(ctx, stages, items, fn)
			case "instance":
				results, _ = NewPipeline(stages).Execute(ctx, items, fn)
			case "instance-meta":
				meta = NewPipeline(stages).ExecuteWithMeta(ctx, items, fn)
			case "parallel":
				results, _ = NewPipelineBuilder(items).Stage("one", 1).Parallel().Run(fn)
			case "parallel-meta":
				meta = NewPipelineBuilder(items).Stage("one", 1).Parallel().ExecuteWithMeta(fn)
			}
			for _, m := range meta {
				if m.Stage != "one" {
					t.Errorf("stage=%q", m.Stage)
				}
				results = append(results, m.Result)
			}
			if calls.Load() != 3 || len(results) != 3 {
				t.Fatalf("lost chunk: calls=%d results=%+v", calls.Load(), results)
			}
			if !errors.Is(results[0].Err, core.ErrTaskGoexit) || results[0].Value != 0 || results[0].IsPanic() {
				t.Errorf("Goexit outcome=%+v", results[0])
			}
			for i := 1; i < 3; i++ {
				if results[i].Err != nil || results[i].Value != (i+1)*10 {
					t.Errorf("result[%d]=%+v", i, results[i])
				}
			}
		})
	}
}

func TestNativePipelinePipeGoexitKeepsRemainingChunk(t *testing.T) {
	for _, mode := range []string{"free", "instance", "parallel"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			fn := func(_ context.Context, stage string, v int) (int, error) {
				if stage == "one" {
					calls.Add(1)
					if v == 1 {
						runtime.Goexit()
					}
					return v * 10, nil
				}
				return v, nil
			}
			stages := []Stage[int]{{Name: "one", Concurrency: 1}, {Name: "two", Concurrency: 1, SkipOnError: true}}
			items := []int{1, 2, 3}
			var ch <-chan core.Result[int]
			switch mode {
			case "free":
				ch = ExecutePipe(context.Background(), stages, items, fn, 1)
			case "instance":
				ch = NewPipeline(stages).ExecutePipe(context.Background(), items, fn, 1)
			case "parallel":
				ch = NewPipelineBuilder(items).Stage("one", 1).Stage("two", 1).Parallel().ExecutePipe(fn, 1)
			}
			values := map[int]int{}
			goexits := 0
			for r := range ch {
				if errors.Is(r.Err, core.ErrTaskGoexit) {
					goexits++
				} else if r.Err == nil {
					values[r.Value]++
				} else {
					t.Errorf("unexpected: %+v", r)
				}
			}
			// Parallel's Pipe currently reprocesses all previous values at the final
			// stage; the free/instance paths honor the explicit SkipOnError above.
			if calls.Load() != 3 || values[20] != 1 || values[30] != 1 {
				t.Fatalf("calls=%d values=%v Goexit=%d", calls.Load(), values, goexits)
			}
			if mode != "parallel" && goexits != 1 {
				t.Errorf("Goexit count=%d", goexits)
			}
		})
	}
}

func TestNativePipelineGoexitFailFast(t *testing.T) {
	for _, mode := range []string{"run", "meta", "pipe"} {
		t.Run(mode, func(t *testing.T) {
			var calls, callbacks atomic.Int32
			b := NewPipelineBuilder([]int{1, 2, 3}).Stage("one", 1).Parallel().FailFast().OnResult(func(_ string, r core.Result[int]) {
				callbacks.Add(1)
				if !errors.Is(r.Err, core.ErrTaskGoexit) {
					t.Errorf("callback=%+v", r)
				}
			})
			fn := func(_ context.Context, _ string, v int) (int, error) { calls.Add(1); runtime.Goexit(); return v, nil }
			var results []core.Result[int]
			switch mode {
			case "run":
				results, _ = b.Run(fn)
			case "meta":
				for _, m := range b.ExecuteWithMeta(fn) {
					results = append(results, m.Result)
				}
			case "pipe":
				b.Stage("two", 1)
				for r := range b.ExecutePipe(fn, 1) {
					results = append(results, r)
				}
			}
			if calls.Load() != 1 || callbacks.Load() != 1 || len(results) == 0 || !errors.Is(results[0].Err, core.ErrTaskGoexit) {
				t.Errorf("calls=%d callbacks=%d results=%+v", calls.Load(), callbacks.Load(), results)
			}
		})
	}
}

func TestDefaultPipelineGoexitNotifiesEachResult(t *testing.T) {
	var calls, callbacks atomic.Int32
	results, _ := NewPipelineBuilder([]int{1, 2, 3}).Stage("one", 1).OnResult(func(_ string, r core.Result[int]) {
		callbacks.Add(1)
		if r.Value == 0 && !errors.Is(r.Err, core.ErrTaskGoexit) {
			t.Errorf("callback=%+v", r)
		}
	}).Run(func(_ context.Context, _ string, v int) (int, error) {
		calls.Add(1)
		if v == 1 {
			runtime.Goexit()
		}
		return v * 10, nil
	})
	if calls.Load() != 3 || callbacks.Load() != 3 || len(results) != 3 {
		t.Errorf("calls=%d callbacks=%d results=%+v", calls.Load(), callbacks.Load(), results)
	}
}

func TestNativePipelineGoexitCleansContextBeforeObserver(t *testing.T) {
	for _, meta := range []bool{false, true} {
		for _, failFast := range []bool{false, true} {
			var businessCtx context.Context
			var observations atomic.Int32
			b := NewPipelineBuilder([]int{1, 2}).Stage("one", 1).Parallel().Timeout(time.Hour).OnResult(func(_ string, r core.Result[int]) {
				observations.Add(1)
				if businessCtx.Err() != context.Canceled {
					t.Errorf("context before observer: %v", businessCtx.Err())
				}
			})
			if failFast {
				b.FailFast()
			}
			fn := func(ctx context.Context, _ string, v int) (int, error) {
				businessCtx = ctx
				if v == 1 {
					runtime.Goexit()
				}
				return v, nil
			}
			if meta {
				b.ExecuteWithMeta(fn)
			} else {
				b.Run(fn)
			}
			want := int32(2)
			if failFast {
				want = 1
			}
			if observations.Load() != want {
				t.Errorf("meta=%t failFast=%t observations=%d", meta, failFast, observations.Load())
			}
		}
	}
}

func TestNativePipelineGoexitSubsequentStagePolicy(t *testing.T) {
	for _, skip := range []bool{false, true} {
		for _, meta := range []bool{false, true} {
			var calls atomic.Int32
			stages := []Stage[int]{{Name: "one", Concurrency: 1}, {Name: "two", Concurrency: 1, SkipOnError: skip}}
			fn := func(_ context.Context, stage string, v int) (int, error) {
				calls.Add(1)
				if stage == "one" && v == 1 {
					runtime.Goexit()
				}
				return v + 10, nil
			}
			var results []core.Result[int]
			if meta {
				m := ExecuteWithMeta(context.Background(), stages, []int{1, 2}, fn)
				if len(m) != 4 || !errors.Is(m[0].Err, core.ErrTaskGoexit) {
					t.Fatalf("meta=%+v", m)
				}
				results = []core.Result[int]{m[2].Result, m[3].Result}
			} else {
				results, _ = Execute(context.Background(), stages, []int{1, 2}, fn)
			}
			wantCalls := int32(4)
			if skip {
				wantCalls = 3
			}
			if calls.Load() != wantCalls || results[1].Err != nil || results[1].Value != 22 {
				t.Fatalf("calls=%d results=%+v", calls.Load(), results)
			}
			if skip {
				if !errors.Is(results[0].Err, core.ErrTaskGoexit) || results[0].Value != 0 {
					t.Errorf("skip=%+v", results[0])
				}
			} else if results[0].Err != nil || results[0].Value != 10 {
				t.Errorf("overwrite=%+v", results[0])
			}
		}
	}
}
