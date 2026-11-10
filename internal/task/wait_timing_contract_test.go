package task

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

func TestAsyncResultEarlyWaitPreservesBusinessAndNotifications(t *testing.T) {
	for _, engine := range []string{"Go", "GoAct", "BoundedGo", "BoundedGoAct"} {
		for _, outcome := range []string{"success", "error", "panic"} {
			for _, wait := range []string{"Cancel", "WaitTimeout", "WaitContext"} {
				t.Run(fmt.Sprintf("%s/%s/%s", engine, outcome, wait), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						parent, cancel := context.WithCancel(context.Background())
						defer cancel()
						runner := NewBoundedRunner(1)
						release, started := make(chan struct{}), make(chan context.Context, 1)
						defer func() {
							select {
							case <-release:
							default:
								close(release)
							}
						}()
						wantErr := errors.New("business outcome")
						var calls atomic.Int32
						fn := func(ctx context.Context) (int, error) {
							calls.Add(1)
							started <- ctx
							<-release
							switch outcome {
							case "error":
								return 42, wantErr
							case "panic":
								panic(wantErr)
							default:
								return 42, nil
							}
						}
						switch engine {
						case "Go":
							checkEarlyWaitContract(t, Go(parent, fn), started, release, parent, nil, outcome, wait, 42, wantErr, &calls)
						case "BoundedGo":
							checkEarlyWaitContract(t, BoundedGo(runner, parent, fn), started, release, parent, runner, outcome, wait, 42, wantErr, &calls)
						case "GoAct":
							checkEarlyWaitContract(t, GoAct(parent, func(ctx context.Context) error { _, err := fn(ctx); return err }), started, release, parent, nil, outcome, wait, NoResult{}, wantErr, &calls)
						case "BoundedGoAct":
							checkEarlyWaitContract(t, BoundedGoAct(runner, parent, func(ctx context.Context) error { _, err := fn(ctx); return err }), started, release, parent, runner, outcome, wait, NoResult{}, wantErr, &calls)
						}
					})
				})
			}
		}
	}
}

func checkEarlyWaitContract[T comparable](t *testing.T, ar *AsyncResult[T], started <-chan context.Context, release chan struct{}, parent context.Context, runner *BoundedRunner, outcome, method string, want T, businessErr error, calls *atomic.Int32) {
	t.Helper()
	businessCtx := <-started
	synctest.Wait()
	begin := time.Now()
	var value T
	var err error
	var ok bool
	var duration time.Duration
	switch method {
	case "Cancel":
		value, err = ar.Cancel()
		duration = time.Millisecond
	case "WaitTimeout":
		value, err, ok = ar.WaitTimeout(2 * time.Second)
		duration = 2 * time.Second
	case "WaitContext":
		waitCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		value, err, ok = ar.WaitContext(waitCtx)
		duration = 3 * time.Second
	}
	var zero T
	wantWaitErr := error(core.ErrTimeout)
	if method == "Cancel" {
		wantWaitErr = context.Canceled
	}
	if method == "WaitContext" {
		wantWaitErr = context.DeadlineExceeded
	}
	if value != zero || ok || !errors.Is(err, wantWaitErr) || time.Since(begin) != duration {
		t.Fatalf("early wait value=%v err=%v ok=%v elapsed=%v want=%v", value, err, ok, time.Since(begin), duration)
	}
	if parent.Err() != nil || businessCtx.Err() != nil || calls.Load() != 1 {
		t.Fatal("early wait canceled or repeated business")
	}
	if runner != nil && (runner.Busy() != 1 || runner.Available() != 0) {
		t.Fatal("early wait released active callback permit")
	}
	// Create competing getResult readers after measuring virtual time:
	// sync.Once contention cannot advance synctest's clock.
	channels := []<-chan core.Result[T]{ar.WaitCh(), ar.WaitCh(), ar.WaitCh()}
	if channels[0] == channels[1] {
		t.Fatal("WaitCh reused notification channel")
	}
	for _, ch := range channels {
		select {
		case <-ch:
			t.Fatal("notification before business completion")
		default:
		}
	}
	close(release)
	value, err = ar.Wait()
	if outcome == "panic" {
		want = zero
	}
	check := func(v T, e error) {
		t.Helper()
		if v != want {
			t.Fatalf("final value=%v want=%v", v, want)
		}
		var pe *core.PanicError
		switch outcome {
		case "success":
			if e != nil {
				t.Fatalf("success error=%v", e)
			}
		case "error":
			if e != businessErr {
				t.Fatalf("business error identity=%v", e)
			}
		case "panic":
			if !errors.As(e, &pe) || pe.Value != businessErr || errors.Is(e, businessErr) {
				t.Fatalf("panic identity/error chain=%v", e)
			}
		}
	}
	check(value, err)
	for _, ch := range channels {
		r, open := <-ch
		if !open {
			t.Fatal("WaitCh closed without result")
		}
		check(r.Value, r.Err)
		if r.Err != err {
			t.Fatal("notifications changed error identity")
		}
		if _, open := <-ch; open {
			t.Fatal("WaitCh produced multiple results")
		}
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	begin = time.Now()
	v, e, completed := ar.WaitContext(canceled)
	if !completed {
		t.Fatal("canceled wait overrode cached result")
	}
	check(v, e)
	v, e, completed = ar.WaitTimeout(-time.Second)
	if !completed {
		t.Fatal("negative timeout overrode cached result")
	}
	check(v, e)
	v, e = ar.Cancel()
	check(v, e)
	if time.Now() != begin || calls.Load() != 1 {
		t.Fatal("cached access waited or executed business again")
	}
	if ar.Ok() != (outcome == "success") || ar.IsPanic() != (outcome == "panic") {
		t.Fatal("queries disagree with cached result")
	}
	synctest.Wait()
	if parent.Err() != nil || businessCtx.Err() != nil {
		t.Fatal("result retrieval canceled business context")
	}
	if runner != nil {
		if runner.Busy() != 0 || runner.Available() != 1 {
			t.Fatal("completed callback leaked permit")
		}
		if v, e := BoundedGo(runner, parent, func(context.Context) (int, error) { return 7, nil }).Wait(); v != 7 || e != nil {
			t.Fatal("permit not reusable")
		}
	}
}

func TestAsyncResultCancelAcceptsGraceWindowResult(t *testing.T) {
	for _, engine := range []string{"Go", "GoAct", "BoundedGo", "BoundedGoAct"} {
		for _, outcome := range []string{"success", "error", "panic"} {
			t.Run(engine+"/"+outcome, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := context.Background()
					runner := NewBoundedRunner(1)
					begin := time.Now()
					wantErr := errors.New("grace business error")
					fn := func(ctx context.Context) (int, error) {
						if ctx.Err() != nil {
							t.Error("Cancel canceled callback context")
						}
						time.Sleep(500 * time.Microsecond)
						switch outcome {
						case "error":
							return 42, wantErr
						case "panic":
							panic(wantErr)
						default:
							return 42, nil
						}
					}
					switch engine {
					case "Go":
						checkGraceWindow(t, Go(ctx, fn), begin, outcome, 42, wantErr)
					case "BoundedGo":
						checkGraceWindow(t, BoundedGo(runner, ctx, fn), begin, outcome, 42, wantErr)
					case "GoAct":
						checkGraceWindow(t, GoAct(ctx, func(ctx context.Context) error { _, err := fn(ctx); return err }), begin, outcome, NoResult{}, wantErr)
					case "BoundedGoAct":
						checkGraceWindow(t, BoundedGoAct(runner, ctx, func(ctx context.Context) error { _, err := fn(ctx); return err }), begin, outcome, NoResult{}, wantErr)
					}
					synctest.Wait()
					if runner.Busy() != 0 {
						t.Fatal("grace completion leaked permit")
					}
				})
			})
		}
	}
}

func checkGraceWindow[T comparable](t *testing.T, ar *AsyncResult[T], begin time.Time, outcome string, want T, wantErr error) {
	t.Helper()
	// Let the callback finish even when a timing assertion fails; otherwise a
	// deliberately broken Cancel can leave sleepers in the synctest bubble.
	defer func() {
		time.Sleep(time.Millisecond)
		synctest.Wait()
	}()
	v, err := ar.Cancel()
	if outcome == "panic" {
		var zero T
		want = zero
	}
	if time.Since(begin) != 500*time.Microsecond || v != want {
		t.Fatalf("grace result=%v elapsed=%v", v, time.Since(begin))
	}
	var pe *core.PanicError
	if outcome == "success" && err != nil || outcome == "error" && err != wantErr || outcome == "panic" && (!errors.As(err, &pe) || pe.Value != wantErr) {
		t.Fatalf("grace outcome=%s err=%v", outcome, err)
	}
	if v2, e2 := ar.Wait(); v2 != v || e2 != err {
		t.Fatal("grace cache changed")
	}
}
