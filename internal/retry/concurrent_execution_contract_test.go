package retry

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Fixed configuration is read by concurrent executions. Holding the first
// callback of both executions proves that each receives independent permits;
// final contexts and errors are observed only after both owners have unwound.
func TestRetryConcurrentExecutionIsolation(t *testing.T) {
	for _, mode := range []string{"none", "rate", "token", "window", "adaptive"} {
		for _, shards := range []int{1, 3} {
			for endpoint := range 4 {
				for _, outcome := range []string{"success", "error", "panic", "goexit", "canceled-success", "canceled-error"} {
					t.Run(fmt.Sprintf("%s/shards%d/endpoint%d/%s", mode, shards, endpoint, outcome), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							type contextKey struct{}
							marker := new(int)
							parent, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, marker))
							defer cancel()
							c := New[int](parent).MaxRetries(2).Backoff(0, 0).PerCallTimeout(time.Hour).
								Rate(1).Per(time.Hour).Burst(1).Capacity(8).Limit(8).Window(time.Hour).
								MinWorker(1).MaxWorker(1).Shards(shards)
							switch mode {
							case "rate":
								c.RateLimiter()
							case "token":
								c.TokenBucket()
							case "window":
								c.SlidingWindow()
							case "adaptive":
								c.Adaptive()
							}
							before := *c
							begin := time.Now()
							type execution struct {
								calls    int
								contexts []context.Context
								value    int
								err      error
								returned bool
								done     chan struct{}
							}
							runs := [2]execution{{done: make(chan struct{})}, {done: make(chan struct{})}}
							gate := make(chan struct{})
							started := make(chan int, 2)
							failures := [2]error{errors.New("first business error"), errors.New("second business error")}
							for id := range runs {
								go func() {
									r := &runs[id]
									defer close(r.done)
									call := func(ctx context.Context) (int, error) {
										r.calls++
										if ctx != nil {
											if ctx.Value(contextKey{}) != marker || ctx.Err() != nil {
												panic("lost live parent")
											}
											r.contexts = append(r.contexts, ctx)
										}
										if r.calls == 1 {
											started <- id
											<-gate
										}
										switch outcome {
										case "error", "canceled-error":
											return -id - 1, failures[id]
										case "panic":
											panic(failures[id])
										case "goexit":
											runtime.Goexit()
										}
										return id + 11, nil
									}
									switch endpoint {
									case 0:
										r.value, r.err = c.Execute(call)
									case 1:
										r.err = c.ExecuteVoid(func(ctx context.Context) error { _, err := call(ctx); return err })
									case 2:
										r.err = c.Run(func() error { _, err := call(nil); return err })
									case 3:
										r.err = c.RunVoid(func(ctx context.Context) error { _, err := call(ctx); return err })
									}
									r.returned = true
								}()
							}
							synctest.Wait()
							if len(started) != 2 {
								gotStarts := len(started)
								// Release a mistakenly shared limiter before reporting a failure.
								close(gate)
								<-runs[0].done
								<-runs[1].done
								t.Fatalf("simultaneous starts=%d want=2", gotStarts)
							}
							if endpoint != 2 && runs[0].contexts[0] == runs[1].contexts[0] {
								t.Error("executions share a per-call context")
							}
							canceled := outcome == "canceled-success" || outcome == "canceled-error"
							if canceled {
								cancel()
							}
							close(gate)
							for id := range runs {
								r := &runs[id]
								<-r.done
								wantCalls := 1
								if outcome == "error" || outcome == "panic" {
									wantCalls = 3
								}
								if r.calls != wantCalls || r.returned != (outcome != "goexit") {
									t.Fatalf("run%d calls=%d returned=%v", id, r.calls, r.returned)
								}
								switch outcome {
								case "success", "canceled-success":
									wantValue := 0
									if endpoint == 0 {
										wantValue = id + 11
									}
									if r.err != nil || r.value != wantValue {
										t.Fatalf("run%d value=%d error=%v", id, r.value, r.err)
									}
								case "error":
									if r.value != 0 || !errors.Is(r.err, failures[id]) || errors.Is(r.err, failures[1-id]) {
										t.Fatalf("run%d error identity: %v", id, r.err)
									}
								case "canceled-error":
									if r.value != 0 || !errors.Is(r.err, context.Canceled) || errors.Is(r.err, failures[id]) {
										t.Fatalf("run%d cancellation: %v", id, r.err)
									}
								case "panic":
									var pe *core.PanicError
									if !errors.As(r.err, &pe) || pe.Value != failures[id] {
										t.Fatalf("run%d panic identity: %v", id, r.err)
									}
								}
								for i, ctx := range r.contexts {
									if ctx.Err() != context.Canceled {
										t.Fatalf("run%d attempt%d context retained: %v", id, i, ctx.Err())
									}
									for _, previous := range r.contexts[:i] {
										if ctx == previous {
											t.Fatal("attempt reused per-call context")
										}
									}
								}
							}
							if (parent.Err() != nil) != canceled || *c != before {
								t.Fatal("execution changed parent or builder configuration")
							}
							if time.Since(begin) != 0 {
								t.Fatalf("immediate retries unexpectedly waited: %v", time.Since(begin))
							}
						})
					})
				}
			}
		}
	}
}
