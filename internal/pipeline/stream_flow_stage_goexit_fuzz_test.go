package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

func FuzzStreamFlowStageOutcomeHistory(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5}, uint8(0))
	f.Add([]byte{3, 3, 1, 2, 0, 0}, uint8(1))
	f.Add([]byte{1, 2}, uint8(2))
	f.Fuzz(func(t *testing.T, data []byte, mode uint8) {
		if len(data) < 2 {
			return
		}
		n := len(data) - 2
		if n > 16 {
			n = 16
		}
		stageCount := 1 + int(data[0]%3)
		stages := make([]Stage[int], stageCount)
		for i := range stages {
			stages[i] = Stage[int]{Name: string(rune('a' + i)), Concurrency: 1 + int(data[i%len(data)]%3), SkipOnError: data[(i+1)%len(data)]%2 == 1}
		}
		items := make([]int, n)
		for i := range items {
			items[i] = i + 1
		}
		type outcome struct{ value, kind int }
		want := map[outcome]int{}
		wantCalls := make([]int32, stageCount)
		wantFailures := int64(0)
		for _, v := range items {
			kind := 0
			for s, stage := range stages {
				if stage.SkipOnError && kind != 0 {
					continue
				}
				wantCalls[s]++
				kind = (v + s + int(data[s%len(data)])) % 4
				switch kind {
				case 0, 1:
					v = v*2 + s + 1
				case 2:
					if mode%2 == 0 {
						v = 0
					}
				case 3:
					v = 0
				}
				if kind != 0 {
					wantFailures++
				}
			}
			want[outcome{v, kind}]++
		}
		calls := make([]atomic.Int32, stageCount)
		businessErr := errors.New("stage failure")
		fn := func(_ context.Context, stage string, v int) (int, error) {
			s := int(stage[0] - 'a')
			calls[s].Add(1)
			switch (v + s + int(data[s%len(data)])) % 4 {
			case 1:
				return v*2 + s + 1, businessErr
			case 2:
				panic(v)
			case 3:
				runtime.Goexit()
			}
			return v*2 + s + 1, nil
		}
		var results []core.Result[int]
		if mode%2 == 0 {
			var err error
			results, err = ExecuteStream(context.Background(), stages, items, fn)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			fl := NewFlow(context.Background(), FlowConfig[int]{Stages: stages, BufSize: 1 + int(data[1]%4)}, fn)
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
			if stats.TotalError != wantFailures || stats.TotalSubmit != int64(n) || stats.TotalResult != int64(n) {
				t.Errorf("stats=%+v wantFailures=%d", stats, wantFailures)
			}
			for _, s := range stats.Stages {
				if s.Active != 0 {
					t.Errorf("active=%+v", s)
				}
			}
		}
		if len(results) != n {
			t.Fatalf("results=%+v want=%d", results, n)
		}
		for i := range calls {
			if calls[i].Load() != wantCalls[i] {
				t.Errorf("calls[%d]=%d want=%d", i, calls[i].Load(), wantCalls[i])
			}
		}
		for _, r := range results {
			if !r.Occupied {
				t.Errorf("unoccupied=%+v", r)
			}
			kind := 0
			if errors.Is(r.Err, core.ErrTaskGoexit) {
				kind = 3
			} else if r.IsPanic() {
				kind = 2
			} else if errors.Is(r.Err, businessErr) {
				kind = 1
			} else if r.Err != nil {
				t.Errorf("unknown error=%+v", r)
			}
			want[outcome{r.Value, kind}]--
		}
		for key, delta := range want {
			if delta != 0 {
				t.Errorf("outcome=%+v delta=%d", key, delta)
			}
		}
	})
}
