package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

func TestFlowBuilderConfigurationAndBuildContext(t *testing.T) {
	for _, entry := range []string{"run", "build"} {
		for _, buf := range []int{-1, 0, 1, 7} {
			for _, fails := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/buf=%d/fails=%v", entry, buf, fails), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						type contextKey struct{}
						captured := context.WithValue(context.Background(), contextKey{}, "captured")
						explicit := context.WithValue(context.Background(), contextKey{}, "explicit")
						var observer atomic.Int32
						// Flow must not implicitly submit these initial items, nor
						// inherit the one-shot timeout/fail-fast/observer settings.
						source := NewPipelineBuilder([]int{999}).Context(captured).Timeout(time.Nanosecond).FailFast().
							OnResult(func(string, core.Result[int]) { observer.Add(1) }).MaxResults(1).
							Stage("a", 1).Stage("b", 1).Stage("c", 1)
						fb := source.Flow().Stage("branch", 1).BufSize(buf)
						source.Stage("source-later", 1).Context(context.Background())
						wantContext := "captured"
						if entry == "build" {
							// Build's explicit context must supersede even a canceled
							// context set directly on this FlowBuilder.
							canceled, cancel := context.WithCancel(captured)
							cancel()
							fb.Context(canceled)
							wantContext = "explicit"
						}
						var calls atomic.Int32
						fn := func(ctx context.Context, stage string, value int) (int, error) {
							calls.Add(1)
							time.Sleep(2 * time.Nanosecond)
							if ctx.Value(contextKey{}) != wantContext || ctx.Err() != nil {
								t.Errorf("flow context=%v/%v want %s", ctx.Value(contextKey{}), ctx.Err(), wantContext)
							}
							if _, ok := ctx.Deadline(); ok {
								t.Error("one-shot timeout inherited by Flow")
							}
							switch stage {
							case "a":
								if fails {
									return value + 1, errors.New("recover in next stage")
								}
								return value + 1, nil
							case "b", "c":
								return value + 1, nil
							case "branch":
								return value + 10, nil
							default:
								t.Errorf("late stage leaked into Flow: %s", stage)
								return -1, nil
							}
						}
						var fl *Flow[int]
						if entry == "build" {
							fl = fb.Build(explicit, fn)
						} else {
							fl = fb.Run(fn)
						}
						defer fl.Close()
						// Mutating the builder after creation does not reconfigure
						// the existing Flow's copied stage fields or channels.
						fb.Stage("flow-later", 1).BufSize(19).Context(context.Background())
						wantBuf := buf
						if wantBuf <= 0 {
							wantBuf = 256
						}
						if cap(fl.Results()) != wantBuf {
							t.Errorf("flow buffer=%d want %d", cap(fl.Results()), wantBuf)
						}
						for _, value := range []int{1, 2} {
							index, err := fl.Submit(context.Background(), value)
							if err != nil || index != value-1 {
								t.Fatalf("submit index=%d err=%v", index, err)
							}
							r := <-fl.Results()
							if r.Err != nil || !r.Occupied || r.Value != value+13 {
								t.Fatalf("flow result=%+v want %d", r, value+13)
							}
						}
						if remaining := fl.Close(); len(remaining) != 0 {
							t.Errorf("unexpected initial/extra input results=%+v", remaining)
						}
						if calls.Load() != 8 || observer.Load() != 0 || fl.SubmitCount() != 2 {
							t.Errorf("calls=%d one-shot observer=%d submitted=%d", calls.Load(), observer.Load(), fl.SubmitCount())
						}
					})
				})
			}
		}
	}
}
