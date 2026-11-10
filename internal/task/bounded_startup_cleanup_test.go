package task

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"

	"github.com/chichengyu/async/internal/core"
)

type boundedStartupLogger struct {
	stage  string
	goexit bool
	marker any
}

func TestBoundedStartupTransfersPermitToCallback(t *testing.T) {
	previousLogger, previousTrace := core.GetLogger(), core.GetTraceLogEnabled()
	defer core.SetLogger(previousLogger)
	defer core.SetTraceLogEnabled(previousTrace)
	core.SetLogger(nil)
	for engine := range 4 {
		for _, trace := range []bool{false, true} {
			for _, outcome := range []string{"success", "error", "panic", "goexit"} {
				t.Run(fmt.Sprintf("engine=%d/trace=%t/%s", engine, trace, outcome), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						core.SetTraceLogEnabled(trace)
						runner := NewBoundedRunner(1)
						release, entered := make(chan struct{}), make(chan struct{})
						marker := fmt.Errorf("business failed")
						fn := func(context.Context) (int, error) {
							close(entered)
							<-release
							switch outcome {
							case "error":
								return 7, marker
							case "panic":
								panic(marker)
							case "goexit":
								runtime.Goexit()
							}
							return 7, nil
						}
						act := func(ctx context.Context) error { _, err := fn(ctx); return err }
						var wait func() (int, error)
						switch engine {
						case 0:
							wait = BoundedGo(runner, context.Background(), fn).Wait
						case 1:
							r := BoundedGoAct(runner, context.Background(), act)
							wait = func() (int, error) { _, err := r.Wait(); return 0, err }
						case 2:
							r := BoundedGoResult(runner, context.Background(), fn)
							t.Cleanup(r.Cancel)
							wait = r.Result
						case 3:
							r := boundedGoResultAct(runner, context.Background(), act)
							t.Cleanup(r.Cancel)
							wait = func() (int, error) { _, err := r.Result(); return 0, err }
						}
						<-entered
						if runner.Available() != 0 || runner.Busy() != 1 {
							t.Fatal("startup released worker-owned permit")
						}
						close(release)
						synctest.Wait()
						value, err := wait()
						wantValue := 0
						if engine%2 == 0 && (outcome == "success" || outcome == "error") {
							wantValue = 7
						}
						if value != wantValue {
							t.Fatalf("value=%d want=%d", value, wantValue)
						}
						if outcome == "error" && err != marker {
							t.Fatal("business error changed")
						}
						if outcome == "panic" {
							pe, ok := err.(*core.PanicError)
							if !ok || pe.Value != marker {
								t.Fatal("business panic changed")
							}
						} else if outcome == "goexit" {
							if err != core.ErrTaskGoexit {
								t.Fatalf("Goexit err=%v", err)
							}
						} else if outcome != "error" && err != nil {
							t.Fatal("normal result changed")
						}
						if runner.Available() != 1 || runner.Busy() != 0 {
							t.Fatal("worker failed to release permit")
						}
					})
				})
			}
		}
	}
}

func (l boundedStartupLogger) Log(context.Context, core.LogLevel, string, ...core.LogField) {}
func (l boundedStartupLogger) exit() {
	if l.goexit {
		runtime.Goexit()
	}
	panic(l.marker)
}
func (l boundedStartupLogger) With(...core.LogField) core.Logger {
	if l.stage == "With" {
		l.exit()
	}
	return l
}
func (l boundedStartupLogger) WithContext(ctx context.Context) context.Context {
	if l.stage == "WithContext" {
		l.exit()
	}
	return ctx
}

func TestBoundedStartupAbnormalExitReturnsPermit(t *testing.T) {
	previousLogger, previousTrace := core.GetLogger(), core.GetTraceLogEnabled()
	defer core.SetLogger(previousLogger)
	defer core.SetTraceLogEnabled(previousTrace)
	core.SetTraceLogEnabled(true)
	for engine := range 4 {
		for _, stage := range []string{"With", "WithContext"} {
			for _, goexit := range []bool{false, true} {
				t.Run(fmt.Sprintf("engine=%d/%s/goexit=%t", engine, stage, goexit), func(t *testing.T) {
					runner := NewBoundedRunner(1)
					marker := new(int)
					core.SetLogger(boundedStartupLogger{stage: stage, goexit: goexit, marker: marker})
					done := make(chan struct{})
					returned, called := false, false
					var recovered any
					go func() {
						defer close(done)
						defer func() { recovered = recover() }()
						fn := func(context.Context) (int, error) { called = true; return 7, nil }
						act := func(ctx context.Context) error { _, err := fn(ctx); return err }
						switch engine {
						case 0:
							BoundedGo(runner, context.Background(), fn)
						case 1:
							BoundedGoAct(runner, context.Background(), act)
						case 2:
							BoundedGoResult(runner, context.Background(), fn)
						case 3:
							boundedGoResultAct(runner, context.Background(), act)
						}
						returned = true
					}()
					<-done
					if returned || called || (goexit && recovered != nil) || (!goexit && recovered != marker) {
						t.Fatalf("startup exit changed: returned=%t called=%t panic=%v", returned, called, recovered)
					}
					if runner.Available() != 1 || runner.Busy() != 0 {
						t.Fatalf("startup leaked permit: available=%d busy=%d", runner.Available(), runner.Busy())
					}
				})
			}
		}
	}
}
