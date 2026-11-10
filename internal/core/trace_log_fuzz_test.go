package core_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

type traceLogMarker struct{}
type traceLogRecord struct {
	ctx    context.Context
	level  core.LogLevel
	msg    string
	fields []core.LogField
}
type traceLogRecorder struct {
	with, attach int
	withFields   [][]core.LogField
	records      []traceLogRecord
}

func (r *traceLogRecorder) Log(ctx context.Context, level core.LogLevel, msg string, fields ...core.LogField) {
	r.records = append(r.records, traceLogRecord{ctx, level, msg, append([]core.LogField(nil), fields...)})
}
func (r *traceLogRecorder) With(fields ...core.LogField) core.Logger {
	r.with++
	r.withFields = append(r.withFields, append([]core.LogField(nil), fields...))
	return traceLogAttached{r}
}
func (r *traceLogRecorder) WithContext(ctx context.Context) context.Context {
	r.attach++
	return context.WithValue(ctx, traceLogMarker{}, r.attach)
}

type traceLogAttached struct{ *traceLogRecorder }

// An independent state oracle for observable trace preservation, logger
// injection and log-level filtering. No concurrent global configuration.
func FuzzTraceLogContract(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
	f.Add([]byte{5, 0, 3, 1, 4, 0, 6, 9, 9, 7, 8, 10, 11})
	f.Add([]byte{3, 6, 5, 6, 4, 6, 2, 6, 12, 13})
	f.Add([]byte{24, 9, 38, 9, 52, 9, 66, 9, 80, 9, 94, 9, 7, 11, 9, 7, 13, 11, 7})
	f.Add([]byte{2, 6, 7, 6, 7}) // Direct logs retain trace fields with injection disabled.
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 48 {
			ops = ops[:48]
		}
		oldLogger, oldKey := core.GetLogger(), core.GetTraceIDKey()
		oldEnabled, oldLevel := core.GetTraceLogEnabled(), core.GetTaskFailLogLevel()
		defer func() {
			core.SetLogger(oldLogger)
			core.SetTraceIDKey(oldKey)
			core.SetTraceLogEnabled(oldEnabled)
			core.SetTaskFailLogLevel(oldLevel)
		}()
		type traceKey int
		key := traceKey(42)
		core.SetTraceIDKey(key)
		recorder := &traceLogRecorder{}
		core.SetLogger(recorder)
		core.SetTraceLogEnabled(true)
		core.SetTaskFailLogLevel(core.LogLevelError)
		ctx := context.Background()
		var value any
		marker, with, attach := 0, 0, 0
		enabled, silent := true, false
		taskLevel := core.TaskLogLevel(0)
		businessErr := errors.New("business")
		for step, op := range ops {
			previousWith := with
			switch op % 14 {
			case 0, 1: // Existing value must be preserved, including invalid values.
				previous := ctx
				if op%14 == 0 {
					ctx = core.EnsureTraceID(ctx)
				} else {
					ctx = core.WithTraceID(ctx, "")
				}
				if value == nil {
					generated, ok := ctx.Value(key).(string)
					if !ok || len(generated) != 32 {
						t.Fatalf("generated trace = %v", ctx.Value(key))
					}
					if _, err := hex.DecodeString(generated); err != nil {
						t.Fatal(err)
					}
					value = generated
					if enabled && !silent {
						with++
						attach++
						marker = attach
					}
				} else if ctx != previous {
					t.Fatal("Ensure replaced an existing trace context")
				}
			case 2: // Explicit ID overrides any old value.
				value = fmt.Sprintf("request-%d", step)
				ctx = core.WithTraceID(ctx, value.(string))
				if enabled && !silent {
					with++
					attach++
					marker = attach
				}
			case 3, 4, 5:
				ctx = context.Background()
				marker = 0
				switch op % 14 {
				case 3:
					value = nil
				case 4:
					value = ""
					ctx = context.WithValue(ctx, key, value)
				case 5:
					value = 17
					ctx = context.WithValue(ctx, key, value)
				}
			case 6:
				enabled = !enabled
				core.SetTraceLogEnabled(enabled)
			case 7, 8, 9:
				before := len(recorder.records)
				level := core.LevelInfo
				msg := "direct"
				var fields []core.LogField
				if tid, ok := value.(string); ok && tid != "" {
					fields = append(fields, core.LogField{Key: "trace_id", Value: tid})
				}
				expectedCtx := ctx
				if op%14 == 7 {
					fields = append(fields, core.LogField{Key: "trace_id", Value: "caller"})
					core.LogCtxInfo(ctx, msg, core.Str("trace_id", "caller"))
				} else if op%14 == 8 {
					level, msg, expectedCtx = core.LevelWarn, "global", context.Background()
					fields = []core.LogField{{Key: "count", Value: int64(step)}}
					core.LogWarn(msg, core.Int64("count", int64(step)))
				} else {
					msg = "task"
					fields = append(fields, core.LogField{Key: "error", Value: businessErr})
					core.LogTaskFail(ctx, businessErr, msg)
					level = core.LogLevel(taskLevel)
				}
				shouldLog := !silent && (op%14 != 9 || taskLevel >= 0 && taskLevel <= 3)
				if !shouldLog {
					if len(recorder.records) != before {
						t.Fatal("filtered log emitted")
					}
				} else {
					if len(recorder.records) != before+1 {
						t.Fatal("log omitted")
					}
					got := recorder.records[before]
					if got.ctx != expectedCtx || got.level != level || got.msg != msg || len(got.fields) != len(fields) {
						t.Fatalf("log envelope mismatch: %+v", got)
					}
					for i := range fields {
						if got.fields[i] != fields[i] {
							t.Fatalf("field[%d]=%v want %v", i, got.fields[i], fields[i])
						}
					}
				}
			case 10:
				taskLevel = core.TaskLogLevel(int(op/14)%7 - 1)
				core.SetTaskFailLogLevel(taskLevel)
			case 11:
				silent = !silent
				if silent {
					core.SetLogger(nil)
				} else {
					core.SetLogger(traceLogAttached{recorder})
				}
			case 12:
				var got string
				var panicked bool
				func() { defer func() { panicked = recover() != nil }(); got = core.GetTraceID(ctx) }()
				want, valid := value.(string)
				if value != nil && !valid {
					if !panicked {
						t.Fatal("GetTraceID accepted a non-string")
					}
				} else if panicked || got != want {
					t.Fatalf("trace read=%q panic=%v want=%q", got, panicked, want)
				}
			case 13:
				// LogCtx accepts nil and does not attach a trace field.
				before := len(recorder.records)
				core.LogCtxError(nil, "nil")
				if silent && len(recorder.records) != before {
					t.Fatal("silent logger emitted nil-context log")
				}
				if !silent && (len(recorder.records) != before+1 || recorder.records[before].ctx != nil || len(recorder.records[before].fields) != 0) {
					t.Fatal("nil log context mismatch")
				}
			}
			if ctx.Value(key) != value {
				t.Fatalf("step %d: trace value mismatch", step)
			}
			if with > previousWith {
				fields := recorder.withFields[len(recorder.withFields)-1]
				if len(fields) != 1 || fields[0].Key != "trace_id" || fields[0].Value != value {
					t.Fatalf("injection fields=%v want trace_id=%v", fields, value)
				}
			}
			gotMarker, _ := ctx.Value(traceLogMarker{}).(int)
			if gotMarker != marker || recorder.with != with || recorder.attach != attach {
				t.Fatalf("step %d: injection got=%d/%d/%d want=%d/%d/%d", step, gotMarker, recorder.with, recorder.attach, marker, with, attach)
			}
			if core.GetTraceLogEnabled() != enabled || core.GetTaskFailLogLevel() != taskLevel {
				t.Fatal("global setting read mismatch")
			}
		}
	})
}
