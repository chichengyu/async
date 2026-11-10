package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// Flow has a different panic-value contract from finite-input execution: it
// preserves the stage input. Check its own oracle, including concurrent result
// consumption competing with Close's drain and per-stage failure statistics.
func FuzzFlowLifecycle(f *testing.F) {
	f.Add([]byte{3, 16, 1, 2, 3, 4, 5})
	f.Add([]byte{0, 0})
	f.Add([]byte{255, 255, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		byteAt := func(i int) byte { return data[i%len(data)] }
		stageCount, itemCount := 1+int(data[0]%4), int(data[1]%17)
		type item struct{ ID, Value int }
		stages := make([]Stage[item], stageCount)
		for s := range stages {
			stages[s] = Stage[item]{Name: string(rune('a' + s)), Concurrency: 1 + int(byteAt(s+2)%3), SkipOnError: byteAt(s+3)%2 == 0}
		}
		businessErr := errors.New("flow model failure")
		invoke := func(_ context.Context, stage string, in item) (item, error) {
			s := int(stage[0] - 'a')
			mode := (in.ID + s + int(byteAt(s+4))) % 7
			if mode == 0 {
				panic("flow model panic")
			}
			in.Value = in.Value*2 + s
			if mode == 1 {
				return in, businessErr
			}
			return in, nil
		}
		type expected struct {
			value item
			kind  int
		}
		want := make([]expected, itemCount)
		wantFailures := int64(0)
		for id := range want {
			value, kind := item{ID: id, Value: id + 1}, 0
			for s, stage := range stages {
				if stage.SkipOnError && kind != 0 {
					continue
				}
				mode := (id + s + int(byteAt(s+4))) % 7
				if mode == 0 {
					kind = 2
					wantFailures++
					continue
				}
				value.Value = value.Value*2 + s
				kind = 0
				if mode == 1 {
					kind = 1
					wantFailures++
				}
			}
			want[id] = expected{value, kind}
		}
		ctx := context.Background()
		flow := NewFlow(ctx, FlowConfig[item]{Stages: stages, BufSize: 1 + int(byteAt(2)%4)}, invoke)
		drained := make(chan []core.Result[item], 1)
		go func() {
			var results []core.Result[item]
			for result := range flow.Results() {
				results = append(results, result)
			}
			drained <- results
		}()
		for id := range want {
			if index, err := flow.Submit(ctx, item{ID: id, Value: id + 1}); err != nil || index != id {
				t.Fatalf("submit[%d]: index=%d error=%v", id, index, err)
			}
		}
		got := flow.Close()
		got = append(got, (<-drained)...)
		if len(got) != itemCount {
			t.Fatalf("got %d results, want %d", len(got), itemCount)
		}
		seen := make([]bool, itemCount)
		for _, result := range got {
			id := result.Value.ID
			if id < 0 || id >= itemCount || seen[id] {
				t.Fatalf("duplicate or invalid result: %+v", result)
			}
			seen[id] = true
			var pe *core.PanicError
			kind := 0
			if errors.As(result.Err, &pe) {
				kind = 2
			} else if errors.Is(result.Err, businessErr) {
				kind = 1
			} else if result.Err != nil {
				t.Fatalf("unexpected error: %v", result.Err)
			}
			if result.Value != want[id].value || kind != want[id].kind || !result.Occupied {
				t.Fatalf("result[%d]=%+v kind=%d, want %+v", id, result, kind, want[id])
			}
		}
		stats := flow.Stats()
		if stats.TotalSubmit != int64(itemCount) || stats.TotalResult != int64(itemCount) || stats.TotalError != wantFailures || !stats.Closed {
			t.Fatalf("unexpected final stats: %+v, want failures=%d", stats, wantFailures)
		}
		if len(flow.Close()) != 0 {
			t.Fatal("second Close returned duplicate results")
		}
		if _, err := flow.Submit(ctx, item{}); !errors.Is(err, core.ErrPoolClosed) {
			t.Fatalf("Submit after Close: %v", err)
		}
	})
}

// Race shutdown against independent producers and canceled enqueue contexts.
// Successful submissions must appear exactly once while the pipeline context
// stays live. External pipeline cancellation may drop results, but must never
// duplicate results, create results for rejected items or prevent cleanup.
func FuzzFlowConcurrentSubmitClose(f *testing.F) {
	f.Add([]byte{1, 8, 0, 1, 2, 3})
	f.Add([]byte{3, 31, 255, 255})
	f.Add([]byte{255, 31, 0, 1, 255}) // External pipeline cancellation.
	f.Add([]byte{0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		stageCount := 1 + int(data[0]%3)
		itemCount := 1 + int(data[1]%32)
		stages := make([]Stage[int], stageCount)
		for i := range stages {
			stages[i] = Stage[int]{Name: "increment", Concurrency: 1 + int(data[i%len(data)]%3)}
		}
		ctx, cancelPipeline := context.WithCancel(context.Background())
		defer cancelPipeline()
		cancelExternally := data[0]&128 != 0
		flow := NewFlow(ctx, FlowConfig[int]{Stages: stages, BufSize: 1},
			func(_ context.Context, _ string, value int) (int, error) { return value + 1, nil })
		// This first item is accepted before the shutdown race starts.
		if _, err := flow.Submit(ctx, 0); err != nil {
			t.Fatal(err)
		}
		type outcome struct {
			id, index int
			err       error
		}
		outcomes := make(chan outcome, itemCount)
		start := make(chan struct{})
		var producers sync.WaitGroup
		for worker := 0; worker < 4; worker++ {
			producers.Add(1)
			go func() {
				defer producers.Done()
				<-start
				for id := worker + 1; id <= itemCount; id += 4 {
					submitCtx, cancel := context.WithCancel(ctx)
					if data[id%len(data)]%2 != 0 {
						cancel()
					}
					index, err := flow.Submit(submitCtx, id)
					cancel()
					outcomes <- outcome{id, index, err}
				}
			}()
		}
		closed := make(chan []core.Result[int], 1)
		go func() {
			<-start
			if cancelExternally {
				cancelPipeline()
			}
			closed <- flow.Close()
		}()
		close(start)
		producers.Wait()
		close(outcomes)
		want := map[int]bool{0: true}
		indexes := map[int]bool{0: true}
		for result := range outcomes {
			if result.err == nil {
				if result.index < 0 || indexes[result.index] {
					t.Fatalf("successful submit reused/invalid index: %+v", result)
				}
				want[result.id], indexes[result.index] = true, true
			} else if result.index != -1 || (!errors.Is(result.err, core.ErrPoolClosed) && !errors.Is(result.err, context.Canceled)) {
				t.Fatalf("unexpected rejection: %+v", result)
			}
		}
		results := <-closed
		if len(results) > len(want) || (!cancelExternally && len(results) != len(want)) {
			t.Fatalf("received %d results for %d accepted submissions", len(results), len(want))
		}
		accepted := len(want)
		for _, result := range results {
			id := result.Value - stageCount
			if result.Err != nil || !result.Occupied || !want[id] {
				t.Fatalf("duplicate, rejected or incorrect result: %+v", result)
			}
			delete(want, id)
		}
		stats := flow.Stats()
		if stats.TotalSubmit != int64(accepted) || stats.TotalResult != int64(len(results)) || stats.TotalError != 0 || !stats.Closed {
			t.Fatalf("incorrect final stats: %+v, accepted=%d", stats, accepted)
		}
		for _, stage := range stats.Stages {
			if stage.Active != 0 || stage.InputBuf != 0 || stage.OutputBuf != 0 {
				t.Fatalf("stage did not finish draining: %+v", stage)
			}
		}
	})
}
