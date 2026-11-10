package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

func FuzzNativePipelineMixedTerminalOutcomes(f *testing.F) {
	f.Add([]byte{3, 0, 1, 2, 3}, uint8(1), uint8(0))
	f.Add([]byte{3, 3, 3, 3}, uint8(2), uint8(5))
	f.Add([]byte{0, 1, 2, 3}, uint8(3), uint8(3))
	f.Fuzz(func(t *testing.T, input []byte, workers, mode uint8) {
		if len(input) > 32 {
			input = input[:32]
		}
		items := make([]int, len(input))
		calls := make([]atomic.Int32, len(input))
		for i := range items {
			items[i] = i + 1
		}
		businessErr := errors.New("business")
		fn := func(_ context.Context, _ string, value int) (int, error) {
			index := value - 1
			calls[index].Add(1)
			switch input[index] % 4 {
			case 1:
				return value * 10, businessErr
			case 2:
				panic(value)
			case 3:
				runtime.Goexit()
			}
			return value * 10, nil
		}
		stages := []Stage[int]{{Name: "one", Concurrency: int(workers%4) + 1}}
		var results []core.Result[int]
		var meta []ResultWithMeta[int]
		var callbacks atomic.Int32
		ctx := context.Background()
		switch mode % 7 {
		case 0:
			results, _ = Execute(ctx, stages, items, fn)
		case 1:
			meta = ExecuteWithMeta(ctx, stages, items, fn)
		case 2:
			results, _ = NewPipeline(stages).Execute(ctx, items, fn)
		case 3:
			meta = NewPipeline(stages).ExecuteWithMeta(ctx, items, fn)
		case 4:
			results, _ = NewPipelineBuilder(items).Stage("one", stages[0].Concurrency).Parallel().Run(fn)
		case 5:
			meta = NewPipelineBuilder(items).Stage("one", stages[0].Concurrency).Parallel().ExecuteWithMeta(fn)
		case 6:
			results, _ = NewPipelineBuilder(items).Stage("one", stages[0].Concurrency).OnResult(func(_ string, _ core.Result[int]) { callbacks.Add(1) }).Run(fn)
		}
		if mode%7 == 6 && int(callbacks.Load()) != len(items) {
			t.Fatalf("callbacks=%d want=%d", callbacks.Load(), len(items))
		}
		for _, m := range meta {
			if m.Stage != "one" {
				t.Fatalf("stage=%q", m.Stage)
			}
			results = append(results, m.Result)
		}
		if len(results) != len(items) {
			t.Fatalf("results=%+v input=%v", results, input)
		}
		for i, r := range results {
			if calls[i].Load() != 1 {
				t.Errorf("calls[%d]=%d", i, calls[i].Load())
			}
			expectedValue := (i + 1) * 10
			switch input[i] % 4 {
			case 0:
				if r.Err != nil {
					t.Errorf("success=%+v", r)
				}
			case 1:
				if !errors.Is(r.Err, businessErr) {
					t.Errorf("business error=%+v", r)
				}
			case 2:
				expectedValue = 0
				if !r.IsPanic() {
					t.Errorf("panic=%+v", r)
				}
			case 3:
				expectedValue = 0
				if !errors.Is(r.Err, core.ErrTaskGoexit) || r.IsPanic() {
					t.Errorf("Goexit=%+v", r)
				}
			}
			if r.Value != expectedValue {
				t.Errorf("value[%d]=%d want=%d", i, r.Value, expectedValue)
			}
		}
	})
}
