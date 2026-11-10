package task

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Normalize only the value type; each engine still uses its real terminal.
type builderContractHandle struct {
	ctx    context.Context
	cancel context.CancelFunc
	result func() (int, error)
}

func launchBuilderContract(b *TaskBuilder[int], engine string, fn func(context.Context) (int, error)) builderContractHandle {
	switch engine {
	case "Go":
		ar := b.Go(fn)
		return builderContractHandle{result: ar.Wait, cancel: func() {}}
	case "GoAct":
		ar := b.GoAct(func(ctx context.Context) error { _, err := fn(ctx); return err })
		return builderContractHandle{result: func() (int, error) { _, err := ar.Wait(); return 0, err }, cancel: func() {}}
	case "GoResult":
		h := b.GoResult(fn)
		return builderContractHandle{ctx: h.Ctx, cancel: h.Cancel, result: h.Result}
	case "GoResultAct":
		h := b.GoResultAct(func(ctx context.Context) error { _, err := fn(ctx); return err })
		return builderContractHandle{ctx: h.Ctx, cancel: h.Cancel, result: func() (int, error) { _, err := h.Result(); return 0, err }}
	default:
		panic(engine)
	}
}

func TestTaskBuilderTimeoutExcludesAdmissionAndJoinsCallback(t *testing.T) {
	for _, engine := range []string{"Go", "GoAct", "GoResult", "GoResultAct"} {
		t.Run(engine, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := NewTaskBuilder[int]().Bounded(1).WithTimeout(2 * time.Second)
				releaseSlot := make(chan struct{})
				first := BoundedGo(b.bounded, context.Background(), func(context.Context) (int, error) { <-releaseSlot; return 1, nil })
				started := make(chan context.Context, 1)
				releaseCallback := make(chan struct{})
				submitted := make(chan builderContractHandle, 1)
				go func() {
					submitted <- launchBuilderContract(b, engine, func(ctx context.Context) (int, error) { started <- ctx; <-releaseCallback; return 42, nil })
				}()
				synctest.Wait()
				time.Sleep(10 * time.Second)
				synctest.Wait()
				if len(submitted) != 0 || len(started) != 0 || b.bounded.Busy() != 1 {
					t.Fatal("builder timeout ended admission wait or started callback without slot")
				}
				close(releaseSlot)
				first.Wait()
				h := <-submitted
				defer h.cancel()
				callbackCtx := <-started
				deadline, ok := callbackCtx.Deadline()
				if !ok || deadline.Sub(time.Now()) != 2*time.Second {
					t.Fatalf("callback deadline = %v, %v at %v", deadline, ok, time.Now())
				}
				completed := make(chan struct{})
				var value int
				var resultErr error
				go func() { value, resultErr = h.result(); close(completed) }()
				time.Sleep(time.Second)
				if callbackCtx.Err() != nil {
					t.Fatal("callback deadline includes admission wait")
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if !errors.Is(callbackCtx.Err(), context.DeadlineExceeded) || b.bounded.Busy() != 1 {
					t.Fatalf("deadline/slot before callback return: err=%v busy=%d", callbackCtx.Err(), b.bounded.Busy())
				}
				select {
				case <-completed:
					t.Fatal("timeout synthesized result before callback returned")
				default:
				}
				if h.ctx != nil {
					_, hasDeadline := h.ctx.Deadline()
					if h.ctx.Err() != nil || hasDeadline {
						t.Fatal("callback deadline leaked into Task.Ctx")
					}
				}
				close(releaseCallback)
				<-completed
				want := 0
				if engine == "Go" || engine == "GoResult" {
					want = 42
				}
				if resultErr != nil || value != want || b.bounded.Busy() != 0 {
					t.Fatalf("late success/slot: value=%d err=%v busy=%d", value, resultErr, b.bounded.Busy())
				}
				if value2, err2 := h.result(); value2 != want || err2 != nil {
					t.Fatal("cached late result changed")
				}
				if v, err := BoundedGo(b.bounded, context.Background(), func(context.Context) (int, error) { return 7, nil }).Wait(); v != 7 || err != nil {
					t.Fatal("slot not reusable")
				}
			})
		})
	}
}

func TestTaskBuilderParentDeadlineRejectsBlockedAdmission(t *testing.T) {
	for _, engine := range []string{"Go", "GoAct", "GoResult", "GoResultAct"} {
		t.Run(engine, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				b := NewTaskBuilder[int]().Context(parent).Bounded(1).WithTimeout(time.Second)
				release := make(chan struct{})
				first := BoundedGo(b.bounded, context.Background(), func(context.Context) (int, error) { <-release; return 1, nil })
				called := false
				submitted := make(chan builderContractHandle, 1)
				go func() {
					submitted <- launchBuilderContract(b, engine, func(context.Context) (int, error) { called = true; return 42, nil })
				}()
				synctest.Wait()
				time.Sleep(2 * time.Second)
				synctest.Wait()
				if len(submitted) != 0 {
					t.Fatal("callback timeout incorrectly limited admission")
				}
				time.Sleep(time.Second)
				h := <-submitted
				defer h.cancel()
				if v, err := h.result(); v != 0 || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("admission deadline result = %d, %v", v, err)
				}
				if called || b.bounded.Busy() != 1 {
					t.Fatal("rejected admission invoked callback or released another task's slot")
				}
				if h.ctx != nil && !errors.Is(h.ctx.Err(), context.DeadlineExceeded) {
					t.Fatal("rejected Task.Ctx lost parent deadline")
				}
				close(release)
				first.Wait()
				if b.bounded.Busy() != 0 {
					t.Fatal("original slot not released")
				}
			})
		})
	}
}

func TestTaskBuilderCallbackContextCleanupAndCapturedTimeout(t *testing.T) {
	for _, engine := range []string{"Go", "GoAct", "GoResult", "GoResultAct"} {
		for _, bounded := range []bool{false, true} {
			for _, outcome := range []string{"success", "error", "panic"} {
				t.Run(fmt.Sprintf("%s/bounded=%v/%s", engine, bounded, outcome), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						b := NewTaskBuilder[int]().WithTimeout(2 * time.Second)
						if bounded {
							b.Bounded(1)
						}
						started := make(chan context.Context, 1)
						release := make(chan struct{})
						wantErr := errors.New("business error")
						h := launchBuilderContract(b, engine, func(ctx context.Context) (int, error) {
							started <- ctx
							<-release
							if outcome == "panic" {
								panic("builder panic")
							}
							if outcome == "error" {
								return 42, wantErr
							}
							return 42, nil
						})
						defer h.cancel()
						// Terminal has returned: changing the builder cannot change its closure.
						b.WithTimeout(time.Hour)
						ctx := <-started
						if d, ok := ctx.Deadline(); !ok || d.Sub(time.Now()) != 2*time.Second {
							t.Fatal("terminal did not capture timeout")
						}
						close(release)
						v, err := h.result()
						wantValue := 0
						if outcome != "panic" && (engine == "Go" || engine == "GoResult") {
							wantValue = 42
						}
						var panicErr *core.PanicError
						if v != wantValue || outcome == "success" && err != nil || outcome == "error" && !errors.Is(err, wantErr) || outcome == "panic" && (!errors.As(err, &panicErr) || panicErr.Value != "builder panic") {
							t.Fatalf("business outcome: value=%d err=%v", v, err)
						}
						if !errors.Is(ctx.Err(), context.Canceled) {
							t.Fatalf("callback context not cleaned after %s: %v", outcome, ctx.Err())
						}
						if h.ctx != nil && h.ctx.Err() != nil {
							t.Fatal("Task.Ctx automatically ended on callback completion")
						}
						h.cancel()
						if h.ctx != nil && !errors.Is(h.ctx.Err(), context.Canceled) {
							t.Fatal("Task.Cancel did not cancel outer context")
						}
						if v2, err2 := h.result(); v2 != v || err2 != err {
							t.Fatal("cancel changed cached business result")
						}
						if bounded && b.bounded.Busy() != 0 {
							t.Fatal("callback cleanup leaked slot")
						}
					})
				})
			}
		}
	}
}
