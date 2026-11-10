package core_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

func TestSafeCallOutcomeContracts(t *testing.T) {
	old := core.GetLogger()
	defer core.SetLogger(old)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	businessErr := errors.New("business")
	var typedNil *int
	for _, ctx := range []context.Context{nil, canceled} {
		for _, void := range []bool{false, true} {
			for _, kind := range []string{"success", "error", "panic-string", "panic-error", "panic-typed-nil", "nil-fn"} {
				t.Run(fmt.Sprintf("ctxnil=%t/void=%t/%s", ctx == nil, void, kind), func(t *testing.T) {
					recorder := &traceLogRecorder{}
					core.SetLogger(recorder)
					calls := 0
					var panicValue any
					fn := func(got context.Context, item int) (int, error) {
						calls++
						if got != ctx || item != 21 {
							t.Fatalf("callback arguments changed: ctx=%v item=%d", got, item)
						}
						switch kind {
						case "error":
							return 42, businessErr
						case "panic-string":
							panicValue = "boom"
						case "panic-error":
							panicValue = businessErr
						case "panic-typed-nil":
							panicValue = typedNil
						default:
							return 42, nil
						}
						panic(panicValue)
					}
					if kind == "nil-fn" {
						fn = nil
					}
					var value int
					var err error
					if void {
						var action func(context.Context, int) error
						if fn != nil {
							action = func(ctx context.Context, n int) error { _, err := fn(ctx, n); return err }
						}
						err = core.SafeCallVoid(ctx, 21, action)
					} else {
						value, err = core.SafeCall(ctx, 21, fn)
					}
					if kind == "success" || kind == "error" {
						wantErr := error(nil)
						if kind == "error" {
							wantErr = businessErr
						}
						if err != wantErr || (!void && value != 42) || calls != 1 || len(recorder.records) != 0 {
							t.Fatalf("ordinary result changed: value=%d err=%v calls=%d logs=%d", value, err, calls, len(recorder.records))
						}
						return
					}
					pe, ok := err.(*core.PanicError)
					if !ok || value != 0 || !strings.Contains(string(pe.Stack), "SafeCall") {
						t.Fatalf("panic contract: value=%d err=%#v", value, err)
					}
					wantCalls := 1
					if kind == "nil-fn" {
						wantCalls = 0
					} else if pe.Value != panicValue {
						t.Fatalf("panic value = %#v, want %#v", pe.Value, panicValue)
					}
					if calls != wantCalls || len(recorder.records) != 1 || recorder.records[0].ctx != ctx || recorder.records[0].level != core.LevelError {
						t.Fatalf("panic callback/log contract: calls=%d logs=%#v", calls, recorder.records)
					}
					if errors.Is(err, businessErr) || pe.Unwrap() != nil {
						t.Fatal("PanicError unexpectedly unwraps original value")
					}
					wrapped := core.Result[int]{Err: fmt.Errorf("outer: %w", pe)}
					if !wrapped.IsPanic() || !strings.Contains(fmt.Sprintf("%+v", pe), string(pe.Stack)) {
						t.Fatal("wrapped panic detection or stack formatting lost")
					}
				})
			}
		}
	}
}

type safeCallPanickingLogger struct{}

func (safeCallPanickingLogger) Log(context.Context, core.LogLevel, string, ...core.LogField) {
	panic("logger failed")
}
func (l safeCallPanickingLogger) With(...core.LogField) core.Logger             { return l }
func (safeCallPanickingLogger) WithContext(ctx context.Context) context.Context { return ctx }

func TestSafeCallLoggerPanicPropagates(t *testing.T) {
	old := core.GetLogger()
	defer core.SetLogger(old)
	core.SetLogger(safeCallPanickingLogger{})
	for _, void := range []bool{false, true} {
		func() {
			defer func() {
				if got := recover(); got != "logger failed" {
					t.Fatalf("backend panic = %#v", got)
				}
			}()
			if void {
				_ = core.SafeCallVoid(context.Background(), 0, func(context.Context, int) error { panic("business failed") })
			} else {
				_, _ = core.SafeCall(context.Background(), 0, func(context.Context, int) (int, error) { panic("business failed") })
			}
		}()
	}
}

func TestSafeCallGoexitRunsDefers(t *testing.T) {
	for _, void := range []bool{false, true} {
		finished := make(chan bool, 1)
		go func() {
			returned := false
			defer func() { finished <- returned }()
			if void {
				_ = core.SafeCallVoid(context.Background(), 0, func(context.Context, int) error { runtime.Goexit(); return nil })
			} else {
				_, _ = core.SafeCall(context.Background(), 0, func(context.Context, int) (int, error) { runtime.Goexit(); return 0, nil })
			}
			returned = true
		}()
		if <-finished {
			t.Fatal("Goexit unexpectedly returned from SafeCall")
		}
	}
}
