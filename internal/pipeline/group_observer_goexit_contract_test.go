package pipeline

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Group records business Goexit, but the pipeline adapter must also publish
// the entered stage's terminal result to its observer before that worker exits.
func TestPipelineGroupObserverIncludesBusinessGoexit(t *testing.T) {
	for _, mode := range []string{"pipe", "shard", "autoscale", "pool"} {
		for _, failFast := range []bool{false, true} {
			for _, terminal := range []string{"success", "error", "panic", "goexit", "cancel-goexit"} {
				t.Run(fmt.Sprintf("%s/ff=%v/%s", mode, failFast, terminal), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						parent, cancel := context.WithCancel(context.Background())
						defer cancel()
						marker := errors.New("business")
						var mu sync.Mutex
						var notices []core.Result[int]
						observer := func(stage string, r core.Result[int]) {
							if stage != "only" {
								t.Errorf("stage=%q", stage)
							}
							mu.Lock()
							notices = append(notices, r)
							mu.Unlock()
						}
						builder := NewPipelineBuilder([]int{17}).Context(parent).Stage("only", 1).OnResult(observer)
						if failFast {
							builder.FailFast()
						}
						fn := func(context.Context, string, int) (int, error) {
							switch terminal {
							case "error":
								return 19, marker
							case "panic":
								panic(marker)
							case "cancel-goexit":
								cancel()
								runtime.Goexit()
							case "goexit":
								runtime.Goexit()
							}
							return 19, nil
						}
						var results []core.Result[int]
						switch mode {
						case "pipe":
							for r := range builder.ExecutePipe(fn, 1) {
								results = append(results, r)
							}
						case "shard":
							results, _ = builder.Parallel().Shard(2).Run(fn)
						case "autoscale":
							results, _ = builder.Parallel().AutoScale(&core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 1, CheckInterval: time.Hour}).Run(fn)
						case "pool":
							results, _ = builder.Parallel().DefaultPool().MaxResults(0).Run(fn)
						}
						synctest.Wait()
						if len(notices) != 1 {
							t.Fatalf("entered business terminal observer count=%d want=1 results=%+v", len(notices), results)
						}
						r := notices[0]
						if !r.Occupied {
							t.Fatal("observer result is not occupied")
						}
						switch terminal {
						case "success":
							if r.Err != nil || r.Value != 19 {
								t.Fatalf("success=%+v", r)
							}
						case "error":
							if !errors.Is(r.Err, marker) || r.Value != 19 {
								t.Fatalf("error=%+v", r)
							}
						case "panic":
							var pe *core.PanicError
							if !errors.As(r.Err, &pe) || pe.Value != marker {
								t.Fatalf("panic=%+v", r)
							}
						default:
							if !errors.Is(r.Err, core.ErrTaskGoexit) || r.Value != 0 || r.IsPanic() {
								t.Fatalf("goexit=%+v", r)
							}
						}
					})
				})
			}
		}
	}
}

func TestPipelineGroupObserverAbnormalExitIsNotReplayed(t *testing.T) {
	for _, mode := range []string{"pipe", "shard", "autoscale"} {
		for _, failFast := range []bool{false, true} {
			for _, termination := range []string{"panic", "goexit"} {
				t.Run(fmt.Sprintf("%s/ff=%v/%s", mode, failFast, termination), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						marker := errors.New("observer")
						var mu sync.Mutex
						count := 0
						b := NewPipelineBuilder([]int{17}).Stage("only", 1).OnResult(func(_ string, r core.Result[int]) {
							mu.Lock()
							count++
							mu.Unlock()
							if r.Value != 19 || r.Err != nil {
								t.Errorf("business outcome=%+v", r)
							}
							if termination == "panic" {
								panic(marker)
							}
							runtime.Goexit()
						})
						if failFast {
							b.FailFast()
						}
						fn := func(context.Context, string, int) (int, error) { return 19, nil }
						var results []core.Result[int]
						switch mode {
						case "pipe":
							for r := range b.ExecutePipe(fn, 1) {
								results = append(results, r)
							}
						case "shard":
							results, _ = b.Parallel().Shard(2).Run(fn)
						case "autoscale":
							results, _ = b.Parallel().AutoScale(&core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 1, CheckInterval: time.Hour}).Run(fn)
						}
						synctest.Wait()
						if count != 1 || len(results) != 1 {
							t.Fatalf("observer replay/count=%d results=%+v", count, results)
						}
						if termination == "panic" {
							var pe *core.PanicError
							if !errors.As(results[0].Err, &pe) || pe.Value != marker {
								t.Fatalf("observer panic lost: %+v", results)
							}
						} else if !errors.Is(results[0].Err, core.ErrTaskGoexit) {
							t.Fatalf("observer Goexit lost: %+v", results)
						}
					})
				})
			}
		}
	}
}

// Count every actual business invocation rather than assuming which queued
// tasks survive FailFast. Later stages may overwrite earlier errors by design.
func TestPipelineGroupObserverMatchesEnteredMultistageBusiness(t *testing.T) {
	for _, mode := range []string{"pipe", "shard", "autoscale"} {
		for _, failFast := range []bool{false, true} {
			for target := 0; target < 2; target++ {
				for _, terminal := range []string{"success", "error", "panic", "goexit"} {
					t.Run(fmt.Sprintf("%s/ff=%v/stage=%d/%s", mode, failFast, target, terminal), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							marker := errors.New("business")
							var mu sync.Mutex
							var entered, observed, failed [2]int
							observer := func(stage string, r core.Result[int]) {
								index := 0
								if stage == "two" {
									index = 1
								}
								mu.Lock()
								defer mu.Unlock()
								observed[index]++
								if r.Err != nil {
									failed[index]++
								}
							}
							b := NewPipelineBuilder([]int{1, 2, 3}).Stage("one", 1).Stage("two", 1).OnResult(observer)
							if failFast {
								b.FailFast()
							}
							fn := func(_ context.Context, stage string, v int) (int, error) {
								index := 0
								if stage == "two" {
									index = 1
								}
								mu.Lock()
								entered[index]++
								mu.Unlock()
								if index == target && v%10 == 1 {
									switch terminal {
									case "error":
										return v, marker
									case "panic":
										panic(marker)
									case "goexit":
										runtime.Goexit()
									}
								}
								return v + 10, nil
							}
							switch mode {
							case "pipe":
								for range b.ExecutePipe(fn, 1) {
								}
							case "shard":
								b.Parallel().Shard(2).Run(fn)
							case "autoscale":
								b.Parallel().AutoScale(&core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 1, CheckInterval: time.Hour}).Run(fn)
							}
							synctest.Wait()
							if entered != observed {
								t.Fatalf("entered=%v observed=%v", entered, observed)
							}
							if terminal == "success" {
								if entered != [2]int{3, 3} || failed != [2]int{} {
									t.Fatalf("success entered=%v failed=%v", entered, failed)
								}
							} else {
								if failed[target] != 1 {
									t.Fatalf("missing failure: entered=%v observed=%v failed=%v", entered, observed, failed)
								}
								if failFast && target == 0 && entered[1] != 0 {
									t.Fatalf("failed stage entered next stage: %v", entered)
								}
								if !failFast && entered != [2]int{3, 3} {
									t.Fatalf("ordinary continuation lost: %v", entered)
								}
							}
						})
					})
				}
			}
		}
	}
}
