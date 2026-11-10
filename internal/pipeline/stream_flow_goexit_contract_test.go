package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/chichengyu/async/internal/core"
)

func TestStreamBusinessGoexitPreservesInputAndCapacity(t *testing.T) {
	for _, builder := range []bool{false, true} {
		var calls atomic.Int32
		fn := func(_ context.Context, _ string, v int) (int, error) {
			calls.Add(1)
			if v == 1 {
				runtime.Goexit()
			}
			return v * 10, nil
		}
		var results []core.Result[int]
		var err error
		if builder {
			results, err = NewPipelineBuilder([]int{1, 2, 3}).Stage("one", 1).Stream().Run(fn)
		} else {
			results, err = ExecuteStream(context.Background(), []Stage[int]{{Name: "one", Concurrency: 1}}, []int{1, 2, 3}, fn)
		}
		if err != nil || len(results) != 3 || calls.Load() != 3 {
			t.Errorf("builder=%t lost capacity: calls=%d results=%+v err=%v", builder, calls.Load(), results, err)
			continue
		}
		if !results[0].Occupied || !errors.Is(results[0].Err, core.ErrTaskGoexit) || results[0].Value != 0 || results[0].IsPanic() {
			t.Errorf("Goexit=%+v", results[0])
		}
		for i := 1; i < 3; i++ {
			if !results[i].Occupied || results[i].Err != nil || results[i].Value != (i+1)*10 {
				t.Errorf("result[%d]=%+v", i, results[i])
			}
		}
	}
}

func TestFlowBusinessGoexitPreservesAcceptedInputAndCounters(t *testing.T) {
	var calls atomic.Int32
	fl := NewFlow(context.Background(), FlowConfig[int]{Stages: []Stage[int]{{Name: "one", Concurrency: 1}}, BufSize: 4}, func(_ context.Context, _ string, v int) (int, error) {
		calls.Add(1)
		if v == 1 {
			runtime.Goexit()
		}
		return v * 10, nil
	})
	for _, v := range []int{1, 2, 3} {
		if _, err := fl.Submit(context.Background(), v); err != nil {
			t.Fatal(err)
		}
	}
	results := fl.Close()
	if len(results) != 3 || calls.Load() != 3 {
		t.Errorf("accepted tasks lost: calls=%d results=%+v", calls.Load(), results)
	}
	if fl.SubmitCount() != 3 || fl.ErrCount() != 1 || fl.Stats().Stages[0].Active != 0 {
		t.Errorf("terminal stats=%+v errors=%d", fl.Stats(), fl.ErrCount())
	}
	if len(results) > 0 && (!errors.Is(results[0].Err, core.ErrTaskGoexit) || results[0].Value != 0 || results[0].IsPanic()) {
		t.Errorf("Goexit=%+v", results[0])
	}
}

func TestStreamFlowGoexitBackpressureAndStagePolicy(t *testing.T) {
	for _, mode := range []string{"stream", "flow"} {
		for _, skip := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/overwrite", true: "/skip"}[skip], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					items := make([]int, 128)
					for i := range items {
						items[i] = i + 1
					}
					stages := []Stage[int]{{Name: "one", Concurrency: 1}, {Name: "two", Concurrency: 1, SkipOnError: skip}}
					var first, second atomic.Int32
					fn := func(_ context.Context, stage string, v int) (int, error) {
						if stage == "one" {
							first.Add(1)
							runtime.Goexit()
						}
						second.Add(1)
						return v + 10, nil
					}
					var results []core.Result[int]
					if mode == "stream" {
						var err error
						results, err = ExecuteStream(context.Background(), stages, items, fn)
						if err != nil {
							t.Fatal(err)
						}
					} else {
						fl := NewFlow(context.Background(), FlowConfig[int]{Stages: stages, BufSize: 1}, fn)
						collected := make(chan []core.Result[int], 1)
						go func() {
							var rs []core.Result[int]
							for r := range fl.Results() {
								rs = append(rs, r)
							}
							collected <- rs
						}()
						for _, v := range items {
							if _, err := fl.Submit(context.Background(), v); err != nil {
								t.Fatal(err)
							}
						}
						results = fl.Close()
						results = append(results, (<-collected)...)
						stats := fl.Stats()
						if stats.TotalError != 128 || stats.TotalSubmit != 128 || stats.TotalResult != 128 || stats.Stages[0].Active != 0 || stats.Stages[1].Active != 0 {
							t.Errorf("stats=%+v", stats)
						}
					}
					wantSecond := int32(128)
					if skip {
						wantSecond = 0
					}
					if len(results) != 128 || first.Load() != 128 || second.Load() != wantSecond {
						t.Fatalf("results=%d first=%d second=%d", len(results), first.Load(), second.Load())
					}
					for _, r := range results {
						if !r.Occupied {
							t.Errorf("unoccupied=%+v", r)
						}
						if skip {
							if r.Value != 0 || !errors.Is(r.Err, core.ErrTaskGoexit) {
								t.Errorf("skipped=%+v", r)
							}
						} else if r.Value != 10 || r.Err != nil {
							t.Errorf("overwritten=%+v", r)
						}
					}
				})
			})
		}
	}
}

func TestStreamGoexitFailFastRetainsFailure(t *testing.T) {
	for _, meta := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			var calls, callbacks atomic.Int32
			b := NewPipelineBuilder([]int{1, 2, 3}).Stage("one", 1).Stage("two", 1).Stream().FailFast().OnResult(func(_ string, r core.Result[int]) {
				callbacks.Add(1)
				if !errors.Is(r.Err, core.ErrTaskGoexit) {
					t.Errorf("observer=%+v", r)
				}
			})
			fn := func(_ context.Context, _ string, v int) (int, error) { calls.Add(1); runtime.Goexit(); return v, nil }
			var results []core.Result[int]
			if meta {
				for _, m := range b.ExecuteWithMeta(fn) {
					if m.Stage != "two" {
						t.Errorf("stage=%q", m.Stage)
					}
					results = append(results, m.Result)
				}
			} else {
				var err error
				results, err = b.Run(fn)
				if !errors.Is(err, context.Canceled) {
					t.Errorf("terminal err=%v", err)
				}
			}
			if calls.Load() != 1 || callbacks.Load() != 1 || len(results) != 3 || !results[0].Occupied || !errors.Is(results[0].Err, core.ErrTaskGoexit) {
				t.Errorf("calls=%d callbacks=%d results=%+v", calls.Load(), callbacks.Load(), results)
			}
			for _, r := range results[1:] {
				if r.Occupied {
					t.Errorf("unexpected completed=%+v", r)
				}
			}
		})
	}
}

func TestStreamFlowGoexitParentCancellationCleansWorkers(t *testing.T) {
	for _, mode := range []string{"stream", "builder", "flow"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var calls atomic.Int32
				fn := func(_ context.Context, _ string, v int) (int, error) {
					calls.Add(1)
					if v == 1 {
						cancel()
						runtime.Goexit()
					}
					return v * 10, nil
				}
				stages := []Stage[int]{{Name: "one", Concurrency: 1}, {Name: "two", Concurrency: 1}}
				if mode == "flow" {
					start := make(chan struct{})
					fl := NewFlow(ctx, FlowConfig[int]{Stages: stages, BufSize: 4}, func(ctx context.Context, s string, v int) (int, error) { <-start; return fn(ctx, s, v) })
					for _, v := range []int{1, 2, 3} {
						if _, err := fl.Submit(context.Background(), v); err != nil {
							t.Fatal(err)
						}
					}
					close(start)
					fl.Close()
					for _, s := range fl.Stats().Stages {
						if s.Active != 0 {
							t.Errorf("stage=%+v", s)
						}
					}
					if fl.ErrCount() != 1 || calls.Load() < 1 {
						t.Errorf("errors=%d calls=%d", fl.ErrCount(), calls.Load())
					}
					return
				}
				var results []core.Result[int]
				var err error
				if mode == "stream" {
					results, err = ExecuteStream(ctx, stages, []int{1, 2, 3}, fn)
				} else {
					results, err = NewPipelineBuilder([]int{1, 2, 3}).Context(ctx).Stage("one", 1).Stage("two", 1).Stream().Run(fn)
				}
				if !errors.Is(err, context.Canceled) || calls.Load() != 1 || len(results) != 3 || !results[0].Occupied || !errors.Is(results[0].Err, core.ErrTaskGoexit) {
					t.Errorf("err=%v calls=%d results=%+v", err, calls.Load(), results)
				}
			})
		})
	}
}
