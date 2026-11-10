package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

func FuzzStreamFlowMixedTerminalOutcomes(f *testing.F) {
	f.Add([]byte{3, 0, 1, 2, 3}, uint8(0), uint8(0))
	f.Add([]byte{3, 3, 3, 3}, uint8(1), uint8(2))
	f.Add([]byte{0, 1, 2, 3}, uint8(3), uint8(1))
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
		fn := func(_ context.Context, _ string, v int) (int, error) {
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
		n := int(workers%4) + 1
		stages := []Stage[int]{{Name: "one", Concurrency: n}}
		var results []core.Result[int]
		var callbacks atomic.Int32
		if mode%3 == 0 {
			var err error
			results, err = ExecuteStream(context.Background(), stages, items, fn)
			if err != nil {
				t.Fatal(err)
			}
		} else if mode%3 == 1 {
			var err error
			results, err = NewPipelineBuilder(items).Stage("one", n).Stream().OnResult(func(_ string, _ core.Result[int]) { callbacks.Add(1) }).Run(fn)
			if err != nil || int(callbacks.Load()) != len(items) {
				t.Fatalf("err=%v callbacks=%d want=%d", err, callbacks.Load(), len(items))
			}
		} else {
			fl := NewFlow(context.Background(), FlowConfig[int]{Stages: stages, BufSize: 4}, fn)
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
			wantFailures := int64(0)
			for _, v := range input {
				if v%4 != 0 {
					wantFailures++
				}
			}
			stats := fl.Stats()
			if fl.ErrCount() != wantFailures || fl.SubmitCount() != int64(len(items)) || stats.TotalResult != int64(len(items)) || stats.Stages[0].Active != 0 {
				t.Errorf("stats=%+v wantFailures=%d", stats, wantFailures)
			}
		}
		if len(results) != len(items) {
			t.Fatalf("results=%+v input=%v", results, input)
		}
		type outcome struct{ value, kind int }
		want := map[outcome]int{}
		for i, b := range input {
			if calls[i].Load() != 1 {
				t.Errorf("calls[%d]=%d", i, calls[i].Load())
			}
			v := (i + 1) * 10
			k := int(b % 4)
			if k == 3 {
				v = 0
			} else if k == 2 {
				v = 0
				if mode%3 == 2 {
					v = i + 1
				}
			}
			want[outcome{v, k}]++
		}
		for _, r := range results {
			if !r.Occupied {
				t.Errorf("unoccupied=%+v", r)
			}
			k := 0
			if errors.Is(r.Err, core.ErrTaskGoexit) {
				k = 3
				if r.IsPanic() {
					t.Errorf("Goexit panic=%+v", r)
				}
			} else if r.IsPanic() {
				k = 2
			} else if errors.Is(r.Err, businessErr) {
				k = 1
			} else if r.Err != nil {
				t.Errorf("unknown=%+v", r)
			}
			want[outcome{r.Value, k}]--
		}
		for k, count := range want {
			if count != 0 {
				t.Errorf("outcome=%+v count delta=%d", k, count)
			}
		}
	})
}
