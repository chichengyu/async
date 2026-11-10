package pipeline

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/chichengyu/async/internal/core"
)

func drainContractPipe(mode string, items []int, parent context.Context, stages bool) *PipelinePipeBuilder[int] {
	b := NewPipelineBuilder(items).Context(parent)
	if stages {
		b.Stage("only", 1)
	}
	switch mode {
	case "serial":
		return b.Serial().Pipe()
	case "parallel":
		return b.Parallel().Pipe()
	default:
		return b.Pipe()
	}
}

func TestPipeDrainWaitsForCallbackAndKeepsErrors(t *testing.T) {
	for _, mode := range []string{"default", "serial", "parallel"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				wantErr := fmt.Errorf("business failure")
				handle := drainContractPipe(mode, []int{7}, parent, true).Run(func(context.Context, string, int) (int, error) { return 9, wantErr })
				entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				defer close(release)
				var calls atomic.Int32
				go func() {
					handle.Drain(func(r core.Result[int]) {
						calls.Add(1)
						if r.Value != 9 || r.Err != wantErr {
							t.Errorf("drain replaced business result: %+v", r)
						}
						close(entered)
						<-release
					})
					close(done)
				}()
				<-entered
				cancel()
				synctest.Wait()
				select {
				case <-done:
					t.Error("Drain returned while consumer callback was blocked")
				default:
				}
				release <- struct{}{}
				<-done
				if calls.Load() != 1 {
					t.Fatalf("consumer calls=%d", calls.Load())
				}
			})
		})
	}
}

func TestPipeDrainPanicConsumesCurrentItemOnly(t *testing.T) {
	for _, mode := range []string{"default", "serial", "parallel"} {
		for _, cancelAfterPanic := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancel=%v", mode, cancelAfterPanic), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					parent, cancel := context.WithCancel(context.Background())
					defer cancel()
					var business atomic.Int32
					// Fits the entire output so panic tests can inspect all remaining
					// results without scheduling-dependent sends or cancellation.
					handle := drainContractPipe(mode, []int{1, 2, 3}, parent, true).Buf(3).Run(func(_ context.Context, _ string, value int) (int, error) {
						business.Add(1)
						return value, nil
					})
					synctest.Wait()
					if len(handle.Receive()) != 3 {
						t.Fatalf("producer did not finish: buffered=%d", len(handle.Receive()))
					}
					marker := &struct{ label string }{"consumer panic"}
					var recovered any
					seen := map[int]int{}
					func() {
						defer func() { recovered = recover() }()
						handle.Drain(func(r core.Result[int]) { seen[r.Value]++; panic(marker) })
					}()
					if recovered != marker {
						t.Fatalf("consumer panic replaced or swallowed: %v", recovered)
					}
					if len(handle.Receive()) != 2 {
						t.Errorf("panic did not consume exactly current item: remaining=%d", len(handle.Receive()))
					}
					if cancelAfterPanic {
						cancel()
					}
					handle.Drain(func(r core.Result[int]) {
						if r.Err != nil {
							t.Errorf("consumer panic changed producer result: %v", r.Err)
						}
						seen[r.Value]++
					})
					if len(seen) != 3 || seen[1] != 1 || seen[2] != 1 || seen[3] != 1 || business.Load() != 3 {
						t.Fatalf("results replayed/lost: seen=%v business=%d", seen, business.Load())
					}
				})
			})
		}
	}
}

func TestPipeDrainNilCallbackDependsOnAvailableResults(t *testing.T) {
	for _, mode := range []string{"default", "serial", "parallel"} {
		for _, shape := range []string{"empty-input", "empty-stages", "one-result"} {
			t.Run(mode+"/"+shape, func(t *testing.T) {
				items := []int{7}
				if shape == "empty-input" {
					items = nil
				}
				handle := drainContractPipe(mode, items, context.Background(), shape != "empty-stages").Run(func(_ context.Context, _ string, value int) (int, error) { return value, nil })
				var recovered any
				func() { defer func() { recovered = recover() }(); handle.Drain(nil) }()
				if (recovered != nil) != (shape == "one-result") {
					t.Fatalf("nil callback panic=%v shape=%s", recovered, shape)
				}
				// The item causing a nil callback panic was already received.
				for r := range handle.Receive() {
					t.Errorf("consumed item replayed: %+v", r)
				}
				// Repeating Drain on a closed empty channel never calls fn.
				handle.Drain(nil)
			})
		}
	}
}
