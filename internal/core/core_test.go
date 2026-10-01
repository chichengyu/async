package core_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// testLogger is a thread-safe logger for testing.
type testLogger struct {
	mu     sync.Mutex
	logs   []string
	levels []core.LogLevel
	withOk bool
	ctxOk  bool
}

func (tl *testLogger) Log(ctx context.Context, level core.LogLevel, msg string, fields ...core.LogField) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.logs = append(tl.logs, fmt.Sprintf("%d:%s:%v", level, msg, fields))
	tl.levels = append(tl.levels, level)
}

func (tl *testLogger) With(fields ...core.LogField) core.Logger {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.withOk = true
	return tl
}

func (tl *testLogger) WithContext(ctx context.Context) context.Context {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.ctxOk = true
	return ctx
}

func (tl *testLogger) LogCount() int {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return len(tl.logs)
}

func (tl *testLogger) Reset() {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.logs = nil
	tl.levels = nil
}

// ==================== Global Settings Tests ====================

func TestSetDefaultTimeout(t *testing.T) {
	old := core.GetDefaultTimeout()
	defer core.SetDefaultTimeout(old)

	// Test setting a valid timeout
	core.SetDefaultTimeout(10 * time.Second)
	if got := core.GetDefaultTimeout(); got != 10*time.Second {
		t.Errorf("GetDefaultTimeout() = %v, want 10s", got)
	}

	// Test zero timeout
	core.SetDefaultTimeout(0)
	if got := core.GetDefaultTimeout(); got != 0 {
		t.Errorf("GetDefaultTimeout() after zero = %v, want 0", got)
	}
}

func TestSetDefaultTimeout_Concurrent(t *testing.T) {
	old := core.GetDefaultTimeout()
	defer core.SetDefaultTimeout(old)

	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			core.SetDefaultTimeout(time.Duration(i+1) * time.Millisecond)
			_ = core.GetDefaultTimeout()
		}(i)
	}
	wg.Wait()
}

func TestSetSubmitTimeout(t *testing.T) {
	old := core.GetSubmitTimeout()
	defer core.SetSubmitTimeout(old)

	core.SetSubmitTimeout(3 * time.Second)
	if got := core.GetSubmitTimeout(); got != 3*time.Second {
		t.Errorf("GetSubmitTimeout() = %v, want 3s", got)
	}

	// Zero should not change
	core.SetSubmitTimeout(0)
	if got := core.GetSubmitTimeout(); got != 3*time.Second {
		t.Errorf("GetSubmitTimeout() after zero = %v, want 3s (unchanged)", got)
	}

	// Negative should not change
	core.SetSubmitTimeout(-1 * time.Second)
	if got := core.GetSubmitTimeout(); got != 3*time.Second {
		t.Errorf("GetSubmitTimeout() after negative = %v, want 3s (unchanged)", got)
	}
}

func TestSetSubmitTimeout_Concurrent(t *testing.T) {
	old := core.GetSubmitTimeout()
	defer core.SetSubmitTimeout(old)

	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			core.SetSubmitTimeout(time.Duration(i+1) * time.Millisecond)
			_ = core.GetSubmitTimeout()
		}(i)
	}
	wg.Wait()
}

func TestSetMaxCleanupDuration(t *testing.T) {
	old := core.GetMaxCleanupDuration()
	defer core.SetMaxCleanupDuration(old)

	core.SetMaxCleanupDuration(5 * time.Minute)
	if got := core.GetMaxCleanupDuration(); got != 5*time.Minute {
		t.Errorf("GetMaxCleanupDuration() = %v, want 5m", got)
	}

	core.SetMaxCleanupDuration(0)
	if got := core.GetMaxCleanupDuration(); got != 0 {
		t.Errorf("GetMaxCleanupDuration() after zero = %v, want 0", got)
	}
}

func TestSetMaxCleanupDuration_Concurrent(t *testing.T) {
	old := core.GetMaxCleanupDuration()
	defer core.SetMaxCleanupDuration(old)

	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			core.SetMaxCleanupDuration(time.Duration(i) * time.Second)
			_ = core.GetMaxCleanupDuration()
		}(i)
	}
	wg.Wait()
}

func TestSetDefaultMaxResults(t *testing.T) {
	old := core.GetDefaultMaxResults()
	defer core.SetDefaultMaxResults(old)

	core.SetDefaultMaxResults(10000)
	if got := core.GetDefaultMaxResults(); got != 10000 {
		t.Errorf("GetDefaultMaxResults() = %v, want 10000", got)
	}

	// Zero should be allowed
	core.SetDefaultMaxResults(0)
	if got := core.GetDefaultMaxResults(); got != 0 {
		t.Errorf("GetDefaultMaxResults() after zero = %v, want 0", got)
	}

	// Negative should not change
	core.SetDefaultMaxResults(-1)
	if got := core.GetDefaultMaxResults(); got != 0 {
		t.Errorf("GetDefaultMaxResults() after -1 = %v, want 0", got)
	}
}

func TestSetDefaultMaxResults_Concurrent(t *testing.T) {
	old := core.GetDefaultMaxResults()
	defer core.SetDefaultMaxResults(old)

	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			core.SetDefaultMaxResults(i * 100)
			_ = core.GetDefaultMaxResults()
		}(i)
	}
	wg.Wait()
}

func TestSetDefaultRingBuffer(t *testing.T) {
	oldCap := core.GetDefaultRingBufferCap()
	oldStrat := core.GetDefaultOverflowStrategy()
	defer core.SetDefaultRingBuffer(oldCap, oldStrat)

	core.SetDefaultRingBuffer(5000, core.OverflowDrop)
	if got := core.GetDefaultRingBufferCap(); got != 5000 {
		t.Errorf("GetDefaultRingBufferCap() = %v, want 5000", got)
	}
	if got := core.GetDefaultOverflowStrategy(); got != core.OverflowDrop {
		t.Errorf("GetDefaultOverflowStrategy() = %v, want OverflowDrop", got)
	}

	core.SetDefaultRingBuffer(0, core.OverflowBlock)
	if got := core.GetDefaultRingBufferCap(); got != 0 {
		t.Errorf("GetDefaultRingBufferCap() after zero = %v, want 0", got)
	}
	if got := core.GetDefaultOverflowStrategy(); got != core.OverflowBlock {
		t.Errorf("GetDefaultOverflowStrategy() after change = %v, want OverflowBlock", got)
	}
}

// ==================== Log Level Tests ====================

func TestSetTaskFailLogLevel(t *testing.T) {
	old := core.GetTaskFailLogLevel()
	defer core.SetTaskFailLogLevel(old)

	core.SetTaskFailLogLevel(core.LogLevelWarn)
	if got := core.GetTaskFailLogLevel(); got != core.LogLevelWarn {
		t.Errorf("GetTaskFailLogLevel() = %v, want LogLevelWarn", got)
	}

	core.SetTaskFailLogLevel(core.LogLevelSilent)
	if got := core.GetTaskFailLogLevel(); got != core.LogLevelSilent {
		t.Errorf("GetTaskFailLogLevel() = %v, want LogLevelSilent", got)
	}
}

func TestSetTaskFailLogLevel_Concurrent(t *testing.T) {
	old := core.GetTaskFailLogLevel()
	defer core.SetTaskFailLogLevel(old)

	levels := []core.TaskLogLevel{core.LogLevelError, core.LogLevelWarn, core.LogLevelInfo, core.LogLevelDebug, core.LogLevelSilent}
	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			core.SetTaskFailLogLevel(levels[i%len(levels)])
			_ = core.GetTaskFailLogLevel()
		}(i)
	}
	wg.Wait()
}

func TestSetTraceLogEnabled(t *testing.T) {
	old := core.GetTraceLogEnabled()
	defer core.SetTraceLogEnabled(old)

	core.SetTraceLogEnabled(false)
	if got := core.GetTraceLogEnabled(); got != false {
		t.Errorf("GetTraceLogEnabled() = %v, want false", got)
	}

	core.SetTraceLogEnabled(true)
	if got := core.GetTraceLogEnabled(); got != true {
		t.Errorf("GetTraceLogEnabled() = %v, want true", got)
	}
}

func TestSetTraceLogEnabled_Concurrent(t *testing.T) {
	old := core.GetTraceLogEnabled()
	defer core.SetTraceLogEnabled(old)

	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			core.SetTraceLogEnabled(true)
			_ = core.GetTraceLogEnabled()
		}()
	}
	wg.Wait()
}

// ==================== Result Tests ====================

func TestResult_Ok(t *testing.T) {
	r := core.Result[int]{Value: 42, Err: nil}
	if !r.Ok() {
		t.Error("Result.Ok() should be true when Err is nil")
	}

	r2 := core.Result[int]{Value: 0, Err: errors.New("fail")}
	if r2.Ok() {
		t.Error("Result.Ok() should be false when Err is not nil")
	}
}

func TestResult_IsPanic(t *testing.T) {
	r := core.Result[int]{Value: 42, Err: core.NewPanicError("boom")}
	if !r.IsPanic() {
		t.Error("Result.IsPanic() should be true for PanicError")
	}

	r2 := core.Result[int]{Value: 0, Err: errors.New("normal error")}
	if r2.IsPanic() {
		t.Error("Result.IsPanic() should be false for normal error")
	}

	r3 := core.Result[int]{Value: 100, Err: nil}
	if r3.IsPanic() {
		t.Error("Result.IsPanic() should be false when Err is nil")
	}
}

func TestResult_Error(t *testing.T) {
	r := core.Result[int]{Err: errors.New("test err")}
	if r.Error() == nil {
		t.Error("Result.Error() should return the error")
	}
	if r.Error().Error() != "test err" {
		t.Errorf("Result.Error() = %v, want 'test err'", r.Error())
	}

	r2 := core.Result[int]{Value: 42}
	if r2.Error() != nil {
		t.Error("Result.Error() should be nil when Err is nil")
	}
}

func TestResult_String(t *testing.T) {
	r := core.Result[int]{Value: 42, Err: nil}
	if got := r.String(); got != "value=42" {
		t.Errorf("Result.String() = %q, want 'value=42'", got)
	}

	r2 := core.Result[int]{Err: errors.New("fail")}
	if got := r2.String(); got != "err=fail" {
		t.Errorf("Result.String() = %q, want 'err=fail'", got)
	}
}

func TestResult_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	results := make([]core.Result[int], 1000)
	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			results[i] = core.Result[int]{Value: i, Err: nil}
		} else {
			results[i] = core.Result[int]{Value: 0, Err: errors.New("err")}
		}
	}

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for _, r := range results {
				_ = r.Ok()
				_ = r.IsPanic()
				_ = r.Error()
				_ = r.String()
			}
		}(g)
	}
	wg.Wait()
}

// ==================== PanicError Tests ====================

func TestPanicError_Error(t *testing.T) {
	pe := core.NewPanicError("test panic")
	if got := pe.Error(); got != "panic: test panic" {
		t.Errorf("PanicError.Error() = %q, want 'panic: test panic'", got)
	}
}

func TestPanicError_Unwrap(t *testing.T) {
	pe := core.NewPanicError("boom")
	if pe.Unwrap() != nil {
		t.Error("PanicError.Unwrap() should return nil")
	}
}

func TestPanicError_Format(t *testing.T) {
	pe := core.NewPanicError("test panic")
	s := fmt.Sprintf("%v", pe)
	if !strings.Contains(s, "panic: test panic") {
		t.Errorf("PanicError %%v format = %q, want contains 'panic: test panic'", s)
	}

	s2 := fmt.Sprintf("%+v", pe)
	if !strings.Contains(s2, "panic: test panic") || !strings.Contains(s2, "core_test.go") {
		t.Errorf("PanicError %%+v format should contain stack trace, got: %s", s2)
	}
}

func TestNewPanicError(t *testing.T) {
	pe := core.NewPanicError("value")
	if pe.Value != "value" {
		t.Errorf("NewPanicError.Value = %v, want 'value'", pe.Value)
	}
	if len(pe.Stack) == 0 {
		t.Error("NewPanicError.Stack should not be empty")
	}
}

func TestPanicError_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pe := core.NewPanicError(i)
			_ = pe.Error()
			_ = pe.Unwrap()
			_ = fmt.Sprintf("%+v", pe)
		}(i)
	}
	wg.Wait()
}

// ==================== MergeCancel Tests ====================

func TestMergeCancel(t *testing.T) {
	called1 := false
	called2 := false
	c1 := func() { called1 = true }
	c2 := func() { called2 = true }

	merged := core.MergeCancel(c1, c2)
	merged()

	if !called1 || !called2 {
		t.Errorf("MergeCancel: called1=%v called2=%v, both should be true", called1, called2)
	}
}

func TestMergeCancel_NilOld(t *testing.T) {
	called := false
	c := func() { called = true }
	merged := core.MergeCancel(nil, c)
	merged()
	if !called {
		t.Error("MergeCancel with nil old should still call new")
	}
}

func TestMergeCancel_Concurrent(t *testing.T) {
	var count1, count2 atomic.Int64
	c1 := func() { count1.Add(1) }
	c2 := func() { count2.Add(1) }

	merged := core.MergeCancel(c1, c2)

	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			merged()
		}()
	}
	wg.Wait()

	// Each cancel func may be called multiple times (once per goroutine), that's fine for atomic
	if count1.Load() < 1 || count2.Load() < 1 {
		t.Errorf("Both cancels should have been called at least once: c1=%d c2=%d", count1.Load(), count2.Load())
	}
}

// ==================== Concurrency Calculation Tests ====================

func TestCPU(t *testing.T) {
	got := core.CPU()
	if got != runtime.NumCPU() {
		t.Errorf("CPU() = %d, want %d", got, runtime.NumCPU())
	}
}

func TestIO(t *testing.T) {
	got := core.IO()
	expected := runtime.NumCPU() * 2
	if got != expected {
		t.Errorf("IO() = %d, want %d", got, expected)
	}
}

func TestIOMulti(t *testing.T) {
	got := core.IOMulti(4)
	expected := runtime.NumCPU() * 4
	if got != expected {
		t.Errorf("IOMulti(4) = %d, want %d", got, expected)
	}

	// Zero or negative should default to 2
	got2 := core.IOMulti(0)
	if got2 != runtime.NumCPU()*2 {
		t.Errorf("IOMulti(0) = %d, want %d", got2, runtime.NumCPU()*2)
	}

	got3 := core.IOMulti(-1)
	if got3 != runtime.NumCPU()*2 {
		t.Errorf("IOMulti(-1) = %d, want %d", got3, runtime.NumCPU()*2)
	}
}

func TestWithConfig(t *testing.T) {
	// Positive: return as-is
	got := core.WithConfig(5)
	if got != 5 {
		t.Errorf("WithConfig(5) = %d, want 5", got)
	}

	// Zero: return IO()
	got2 := core.WithConfig(0)
	if got2 != core.IO() {
		t.Errorf("WithConfig(0) = %d, want %d", got2, core.IO())
	}

	// Negative: return IO()
	got3 := core.WithConfig(-1)
	if got3 != core.IO() {
		t.Errorf("WithConfig(-1) = %d, want %d", got3, core.IO())
	}
}

func TestWithConfig_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = core.WithConfig(i)
		}(i)
	}
	wg.Wait()
}

// ==================== TraceID Tests ====================

func TestEnsureTraceID(t *testing.T) {
	ctx := core.EnsureTraceID(context.Background())
	traceID := core.GetTraceID(ctx)
	if traceID == "" {
		t.Error("EnsureTraceID should generate a trace_id")
	}
	if len(traceID) != 32 {
		t.Errorf("trace_id length = %d, want 32", len(traceID))
	}
}

func TestEnsureTraceID_AlreadyExists(t *testing.T) {
	ctx := core.EnsureTraceID(context.Background())
	traceID1 := core.GetTraceID(ctx)
	ctx2 := core.EnsureTraceID(ctx)
	traceID2 := core.GetTraceID(ctx2)
	if traceID1 != traceID2 {
		t.Errorf("EnsureTraceID should preserve existing trace_id: %s != %s", traceID1, traceID2)
	}
}

func TestGetTraceID_NoTraceID(t *testing.T) {
	ctx := context.Background()
	if got := core.GetTraceID(ctx); got != "" {
		t.Errorf("GetTraceID without trace_id should return empty, got: %q", got)
	}
}

func TestWithTraceID(t *testing.T) {
	ctx := core.WithTraceID(context.Background(), "custom-trace-id")
	traceID := core.GetTraceID(ctx)
	if traceID != "custom-trace-id" {
		t.Errorf("WithTraceID: got %q, want 'custom-trace-id'", traceID)
	}
}

func TestWithTraceID_Empty(t *testing.T) {
	ctx := core.WithTraceID(context.Background(), "")
	traceID := core.GetTraceID(ctx)
	if traceID == "" || len(traceID) != 32 {
		t.Errorf("WithTraceID with empty should auto-generate, got: %s (len=%d)", traceID, len(traceID))
	}
}

func TestNewTraceID(t *testing.T) {
	id1 := core.NewTraceID()
	id2 := core.NewTraceID()
	if id1 == "" || id2 == "" {
		t.Error("NewTraceID should not return empty")
	}
	if id1 == id2 {
		t.Error("NewTraceID should generate unique IDs")
	}
	if len(id1) != 32 || len(id2) != 32 {
		t.Errorf("NewTraceID length: id1=%d id2=%d, both want 32", len(id1), len(id2))
	}
}

func TestSetTraceIDKey(t *testing.T) {
	if got := core.GetTraceIDKey(); got == nil {
		t.Error("GetTraceIDKey() should return a non-nil key by default")
	}
}

func TestTraceID_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	n := 100
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := core.EnsureTraceID(context.Background())
			ids[i] = core.GetTraceID(ctx)
		}(i)
	}
	wg.Wait()

	// Check no collisions among generated IDs
	seen := make(map[string]bool)
	for _, id := range ids {
		if seen[id] {
			t.Errorf("Duplicate trace_id generated: %s", id)
		}
		seen[id] = true
	}
}

// ==================== SafeCall Tests ====================

func TestSafeCall_Success(t *testing.T) {
	val, err := core.SafeCall(context.Background(), 42, func(ctx context.Context, item int) (string, error) {
		return fmt.Sprintf("val-%d", item), nil
	})
	if err != nil {
		t.Errorf("SafeCall should succeed, got err: %v", err)
	}
	if val != "val-42" {
		t.Errorf("SafeCall value = %q, want 'val-42'", val)
	}
}

func TestSafeCall_Error(t *testing.T) {
	val, err := core.SafeCall(context.Background(), 1, func(ctx context.Context, item int) (string, error) {
		return "", errors.New("intentional error")
	})
	if err == nil || err.Error() != "intentional error" {
		t.Errorf("SafeCall should return error, got: %v", err)
	}
	if val != "" {
		t.Errorf("SafeCall value on error should be zero, got: %q", val)
	}
}

func TestSafeCall_Panic(t *testing.T) {
	val, err := core.SafeCall(context.Background(), 1, func(ctx context.Context, item int) (string, error) {
		panic("boom")
	})
	if err == nil {
		t.Fatal("SafeCall should capture panic as error")
	}
	pe, ok := err.(*core.PanicError)
	if !ok {
		t.Errorf("SafeCall panic error should be *PanicError, got %T", err)
	}
	if pe.Value != "boom" {
		t.Errorf("PanicError.Value = %v, want 'boom'", pe.Value)
	}
	if val != "" {
		t.Errorf("SafeCall value on panic should be zero, got: %q", val)
	}
}

func TestSafeCallVoid_Success(t *testing.T) {
	err := core.SafeCallVoid(context.Background(), 42, func(ctx context.Context, item int) error {
		return nil
	})
	if err != nil {
		t.Errorf("SafeCallVoid should succeed, got err: %v", err)
	}
}

func TestSafeCallVoid_Error(t *testing.T) {
	err := core.SafeCallVoid(context.Background(), 1, func(ctx context.Context, item int) error {
		return errors.New("void error")
	})
	if err == nil || err.Error() != "void error" {
		t.Errorf("SafeCallVoid should return error, got: %v", err)
	}
}

func TestSafeCallVoid_Panic(t *testing.T) {
	err := core.SafeCallVoid(context.Background(), 1, func(ctx context.Context, item int) error {
		panic("void boom")
	})
	if err == nil {
		t.Fatal("SafeCallVoid should capture panic")
	}
	pe, ok := err.(*core.PanicError)
	if !ok {
		t.Errorf("SafeCallVoid panic error should be *PanicError, got %T", err)
	}
	if pe.Value != "void boom" {
		t.Errorf("PanicError.Value = %v, want 'void boom'", pe.Value)
	}
}

func TestSafeCall_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	n := 100
	var successCount, errorCount, panicCount atomic.Int64

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mod := i % 3
			switch mod {
			case 0:
				val, err := core.SafeCall(context.Background(), i, func(ctx context.Context, item int) (int, error) {
					return item * 2, nil
				})
				if err == nil && val == i*2 {
					successCount.Add(1)
				}
			case 1:
				_, err := core.SafeCall(context.Background(), i, func(ctx context.Context, item int) (int, error) {
					return 0, errors.New("planned error")
				})
				if err != nil {
					errorCount.Add(1)
				}
			case 2:
				err := core.SafeCallVoid(context.Background(), i, func(ctx context.Context, item int) error {
					panic("concurrent panic")
				})
				if err != nil {
					if _, ok := err.(*core.PanicError); ok {
						panicCount.Add(1)
					}
				}
			}
		}(i)
	}
	wg.Wait()

	if successCount.Load() == 0 || errorCount.Load() == 0 || panicCount.Load() == 0 {
		t.Logf("success=%d error=%d panic=%d", successCount.Load(), errorCount.Load(), panicCount.Load())
	}
}

// ==================== Logger Interface Tests ====================

func TestLogField_Str(t *testing.T) {
	f := core.Str("key", "value")
	if f.Key != "key" || f.Value != "value" {
		t.Errorf("Str field: %+v", f)
	}
}

func TestLogField_Err(t *testing.T) {
	err := errors.New("test")
	f := core.Err(err)
	if f.Key != "error" || f.Value != err {
		t.Errorf("Err field: %+v", f)
	}
}

func TestLogField_Dur(t *testing.T) {
	f := core.Dur("elapsed", time.Second)
	if f.Key != "elapsed" || f.Value != time.Second {
		t.Errorf("Dur field: %+v", f)
	}
}

func TestLogField_Any(t *testing.T) {
	f := core.Any("data", map[string]int{"a": 1})
	if f.Key != "data" {
		t.Errorf("Any field key = %q, want 'data'", f.Key)
	}
}

func TestLogField_Bytes(t *testing.T) {
	data := []byte("hello")
	f := core.Bytes("payload", data)
	if f.Key != "payload" {
		t.Errorf("Bytes field key = %q, want 'payload'", f.Key)
	}
}

func TestLogField_Int64(t *testing.T) {
	f := core.Int64("count", 100)
	if f.Key != "count" || f.Value != int64(100) {
		t.Errorf("Int64 field: %+v", f)
	}
}

func TestSetLogger(t *testing.T) {
	original := core.GetLogger()
	defer core.SetLogger(original)

	tl := &testLogger{}
	core.SetLogger(tl)

	if core.GetLogger() != tl {
		t.Error("GetLogger should return the set logger")
	}

	// Test With method
	withLogger := tl.With()
	if !tl.withOk {
		t.Error("Logger.With should be called")
	}
	_ = withLogger

	// Test WithContext
	ctx := tl.WithContext(context.Background())
	_ = ctx
	if !tl.ctxOk {
		t.Error("Logger.WithContext should be called")
	}

	// Set nil resets to nop
	core.SetLogger(nil)
	if core.GetLogger() == tl {
		t.Error("GetLogger after nil should not return test logger")
	}
}

func TestLogCtxFunctions(t *testing.T) {
	original := core.GetLogger()
	defer core.SetLogger(original)

	tl := &testLogger{}
	core.SetLogger(tl)

	ctx := core.EnsureTraceID(context.Background())

	core.LogCtxError(ctx, "error msg", core.Str("k", "v"))
	if tl.LogCount() == 0 {
		t.Error("LogCtxError should produce log")
	}
	tl.Reset()

	core.LogCtxWarn(ctx, "warn msg")
	if tl.LogCount() == 0 {
		t.Error("LogCtxWarn should produce log")
	}
	tl.Reset()

	core.LogCtxInfo(ctx, "info msg")
	if tl.LogCount() == 0 {
		t.Error("LogCtxInfo should produce log")
	}
	tl.Reset()

	core.LogCtxDebug(ctx, "debug msg")
	if tl.LogCount() == 0 {
		t.Error("LogCtxDebug should produce log")
	}
}

func TestLogGlobalFunctions(t *testing.T) {
	original := core.GetLogger()
	defer core.SetLogger(original)

	tl := &testLogger{}
	core.SetLogger(tl)

	core.LogError("error")
	if tl.LogCount() == 0 {
		t.Error("LogError should produce log")
	}
	tl.Reset()

	core.LogWarn("warn")
	if tl.LogCount() == 0 {
		t.Error("LogWarn should produce log")
	}

	core.LogInfo("info")
	core.LogDebug("debug")
}

func TestLogTaskFailCtx(t *testing.T) {
	original := core.GetLogger()
	oldLevel := core.GetTaskFailLogLevel()
	defer func() {
		core.SetLogger(original)
		core.SetTaskFailLogLevel(oldLevel)
	}()

	tl := &testLogger{}
	core.SetLogger(tl)

	ctx := context.Background()

	// Error level
	core.SetTaskFailLogLevel(core.LogLevelError)
	core.LogTaskFailCtx(ctx, "fail", core.Err(errors.New("err")))
	if tl.LogCount() == 0 {
		t.Error("LogTaskFailCtx should log at Error level")
	}
	tl.Reset()

	// Warn level
	core.SetTaskFailLogLevel(core.LogLevelWarn)
	core.LogTaskFailCtx(ctx, "fail")
	if tl.LogCount() == 0 {
		t.Error("LogTaskFailCtx should log at Warn level")
	}
	tl.Reset()

	// Silent level
	core.SetTaskFailLogLevel(core.LogLevelSilent)
	core.LogTaskFailCtx(ctx, "fail")
	if tl.LogCount() != 0 {
		t.Error("LogTaskFailCtx should not log at Silent level")
	}
}

func TestLogTaskFail(t *testing.T) {
	original := core.GetLogger()
	defer core.SetLogger(original)

	tl := &testLogger{}
	core.SetLogger(tl)

	ctx := core.EnsureTraceID(context.Background())
	core.LogTaskFail(ctx, errors.New("task error"), "task failed")
	if tl.LogCount() == 0 {
		t.Error("LogTaskFail should produce log")
	}
}

// ==================== AutoScaleConfig Tests ====================

func TestDefaultAutoScaleConfig(t *testing.T) {
	cfg := core.DefaultAutoScaleConfig()
	if cfg.MinWorkers <= 0 {
		t.Errorf("DefaultAutoScaleConfig MinWorkers = %d, want >0", cfg.MinWorkers)
	}
	if cfg.MaxWorkers <= 0 {
		t.Errorf("DefaultAutoScaleConfig MaxWorkers = %d, want >0", cfg.MaxWorkers)
	}
	if cfg.CheckInterval <= 0 {
		t.Errorf("DefaultAutoScaleConfig CheckInterval = %v, want >0", cfg.CheckInterval)
	}
	if cfg.ScaleUpThreshold <= 0 {
		t.Errorf("DefaultAutoScaleConfig ScaleUpThreshold = %f, want >0", cfg.ScaleUpThreshold)
	}
	if cfg.ScaleDownThreshold <= 0 {
		t.Errorf("DefaultAutoScaleConfig ScaleDownThreshold = %f, want >0", cfg.ScaleDownThreshold)
	}
}

func TestAutoScaleConfig_Normalize(t *testing.T) {
	cfg := &core.AutoScaleConfig{}
	cfg.Normalize()
	if cfg.MinWorkers <= 0 {
		t.Errorf("Normalize MinWorkers = %d, want >0", cfg.MinWorkers)
	}
	if cfg.MaxWorkers <= 0 {
		t.Errorf("Normalize MaxWorkers = %d, want >0", cfg.MaxWorkers)
	}
	if cfg.CheckInterval <= 0 {
		t.Errorf("Normalize CheckInterval = %v, want >0", cfg.CheckInterval)
	}
	if cfg.ScaleUpThreshold <= 0 {
		t.Errorf("Normalize ScaleUpThreshold = %f, want >0", cfg.ScaleUpThreshold)
	}
	if cfg.ScaleDownThreshold <= 0 {
		t.Errorf("Normalize ScaleDownThreshold = %f, want >0", cfg.ScaleDownThreshold)
	}
	if cfg.ScaleUpChecks <= 0 {
		t.Errorf("Normalize ScaleUpChecks = %d, want >0", cfg.ScaleUpChecks)
	}
	if cfg.ScaleDownChecks <= 0 {
		t.Errorf("Normalize ScaleDownChecks = %d, want >0", cfg.ScaleDownChecks)
	}
	if cfg.ScaleUpFactor <= 0 {
		t.Errorf("Normalize ScaleUpFactor = %f, want >0", cfg.ScaleUpFactor)
	}
	if cfg.ScaleDownFactor <= 0 {
		t.Errorf("Normalize ScaleDownFactor = %f, want >0", cfg.ScaleDownFactor)
	}
}

func TestAutoScaleConfig_Normalize_PartialZero(t *testing.T) {
	cfg := &core.AutoScaleConfig{
		MinWorkers:    5,
		MaxWorkers:    100,
		ScaleUpFactor: 2.0,
		// Other fields are zero, should be filled
	}
	cfg.Normalize()
	if cfg.MinWorkers != 5 {
		t.Errorf("MinWorkers should stay 5, got %d", cfg.MinWorkers)
	}
	if cfg.MaxWorkers != 100 {
		t.Errorf("MaxWorkers should stay 100, got %d", cfg.MaxWorkers)
	}
	if cfg.ScaleUpFactor != 2.0 {
		t.Errorf("ScaleUpFactor should stay 2.0, got %f", cfg.ScaleUpFactor)
	}
	if cfg.CheckInterval == 0 {
		t.Error("CheckInterval should be normalized")
	}
}

// ==================== OverflowStrategy Tests ====================

func TestOverflowStrategy(t *testing.T) {
	if core.OverflowBlock != 0 {
		t.Errorf("OverflowBlock should be 0, got %v", core.OverflowBlock)
	}
	if core.OverflowDrop != 1 {
		t.Errorf("OverflowDrop should be 1, got %v", core.OverflowDrop)
	}
	if core.OverflowError != 2 {
		t.Errorf("OverflowError should be 2, got %v", core.OverflowError)
	}
}

// ==================== RingBuffer Tests ====================

func TestNewRingBuffer(t *testing.T) {
	rb := core.NewRingBuffer[int](100, core.OverflowDrop)
	if rb.Cap() != 100 {
		t.Errorf("Cap() = %d, want 100", rb.Cap())
	}
	if rb.Len() != 0 {
		t.Errorf("Len() = %d, want 0", rb.Len())
	}
	if rb.Overflow() != core.OverflowDrop {
		t.Errorf("Overflow() = %v, want OverflowDrop", rb.Overflow())
	}
}

func TestNewRingBuffer_DefaultCapacity(t *testing.T) {
	rb := core.NewRingBuffer[int](0, core.OverflowDrop)
	if rb.Cap() != 1024 {
		t.Errorf("Default capacity = %d, want 1024", rb.Cap())
	}
}

func TestRingBuffer_PushPop(t *testing.T) {
	rb := core.NewRingBuffer[int](3, core.OverflowBlock)
	if !rb.Push(1) {
		t.Error("Push(1) should succeed")
	}
	if !rb.Push(2) {
		t.Error("Push(2) should succeed")
	}
	if !rb.Push(3) {
		t.Error("Push(3) should succeed")
	}
	if rb.Push(4) {
		t.Error("Push(4) should fail when full (OverflowBlock)")
	}

	val, ok := rb.Pop()
	if !ok || val != 1 {
		t.Errorf("Pop() = (%d, %v), want (1, true)", val, ok)
	}
	val, ok = rb.Pop()
	if !ok || val != 2 {
		t.Errorf("Pop() = (%d, %v), want (2, true)", val, ok)
	}
	val, ok = rb.Pop()
	if !ok || val != 3 {
		t.Errorf("Pop() = (%d, %v), want (3, true)", val, ok)
	}
	_, ok = rb.Pop()
	if ok {
		t.Error("Pop() on empty should return false")
	}
}

func TestRingBuffer_PushDrop(t *testing.T) {
	rb := core.NewRingBuffer[int](3, core.OverflowDrop)
	rb.Push(1)
	rb.Push(2)
	rb.Push(3)
	// Full, this should drop oldest (1)
	if !rb.Push(4) {
		t.Error("Push(4) should succeed with OverflowDrop")
	}
	if rb.Disposed() != 1 {
		t.Errorf("Disposed() = %d, want 1", rb.Disposed())
	}
	val, ok := rb.Pop()
	if !ok || val != 2 {
		t.Errorf("After drop, Pop() = (%d, %v), want (2, true)", val, ok)
	}
}

func TestRingBuffer_PushError(t *testing.T) {
	rb := core.NewRingBuffer[int](2, core.OverflowError)
	rb.Push(1)
	rb.Push(2)
	if rb.Push(3) {
		t.Error("Push(3) should fail with OverflowError")
	}
	if rb.Len() != 2 {
		t.Errorf("Len after failed push = %d, want 2", rb.Len())
	}
}

func TestRingBuffer_Peek(t *testing.T) {
	rb := core.NewRingBuffer[int](10, core.OverflowBlock)
	rb.Push(1)
	rb.Push(2)

	val, ok := rb.Peek()
	if !ok || val != 1 {
		t.Errorf("Peek() = (%d, %v), want (1, true)", val, ok)
	}
	// Peek should not remove
	if rb.Len() != 2 {
		t.Errorf("Len after Peek = %d, want 2", rb.Len())
	}
}

func TestRingBuffer_PeekEmpty(t *testing.T) {
	rb := core.NewRingBuffer[int](5, core.OverflowBlock)
	_, ok := rb.Peek()
	if ok {
		t.Error("Peek on empty should return false")
	}
}

func TestRingBuffer_IsFull(t *testing.T) {
	rb := core.NewRingBuffer[int](2, core.OverflowBlock)
	if rb.IsFull() {
		t.Error("New buffer should not be full")
	}
	rb.Push(1)
	rb.Push(2)
	if !rb.IsFull() {
		t.Error("Buffer with 2/2 items should be full")
	}
	rb.Pop()
	if rb.IsFull() {
		t.Error("After pop, buffer should not be full")
	}
}

func TestRingBuffer_Flush(t *testing.T) {
	rb := core.NewRingBuffer[int](5, core.OverflowBlock)
	rb.Push(1)
	rb.Push(2)
	rb.Push(3)

	items := rb.Flush()
	if len(items) != 3 {
		t.Errorf("Flush() len = %d, want 3", len(items))
	}
	if rb.Len() != 0 {
		t.Errorf("Len after Flush = %d, want 0", rb.Len())
	}
	for i, v := range items {
		if v != i+1 {
			t.Errorf("Flush()[%d] = %d, want %d", i, v, i+1)
		}
	}
}

func TestRingBuffer_FlushN(t *testing.T) {
	rb := core.NewRingBuffer[int](10, core.OverflowBlock)
	for i := 0; i < 10; i++ {
		rb.Push(i)
	}

	items := rb.FlushN(3)
	if len(items) != 3 {
		t.Errorf("FlushN(3) len = %d, want 3", len(items))
	}
	if rb.Len() != 7 {
		t.Errorf("Len after FlushN(3) = %d, want 7", rb.Len())
	}
	for i, v := range items {
		if v != i {
			t.Errorf("FlushN(3)[%d] = %d, want %d", i, v, i)
		}
	}

	// FlushN with n=0 or n > size should return all
	items = rb.FlushN(0)
	if len(items) != 7 {
		t.Errorf("FlushN(0) len = %d, want 7", len(items))
	}

	// Flush empty
	rb.Push(100)
	items = rb.FlushN(10)
	if len(items) != 1 {
		t.Errorf("FlushN(10) on single item = %d, want 1", len(items))
	}
}

func TestRingBuffer_Dropped(t *testing.T) {
	rb := core.NewRingBuffer[int](2, core.OverflowDrop)
	if rb.Dropped() != 0 {
		t.Errorf("Dropped initially = %d, want 0", rb.Dropped())
	}
	if rb.Disposed() != 0 {
		t.Errorf("Disposed initially = %d, want 0", rb.Disposed())
	}

	rb.Push(1)
	rb.Push(2)
	rb.Push(3) // drops 1
	rb.Push(4) // drops 2

	if rb.Dropped() != 2 {
		t.Errorf("Dropped() = %d, want 2", rb.Dropped())
	}
}

func TestRingBuffer_Reset(t *testing.T) {
	rb := core.NewRingBuffer[int](5, core.OverflowDrop)
	rb.Push(1)
	rb.Push(2)
	rb.Push(3)

	rb.Reset()
	if rb.Len() != 0 {
		t.Errorf("Len after Reset = %d, want 0", rb.Len())
	}
	if rb.Disposed() != 0 {
		t.Errorf("Disposed after Reset = %d, want 0", rb.Disposed())
	}
	_, ok := rb.Pop()
	if ok {
		t.Error("Pop after Reset should return false")
	}

	// Should be able to push again
	rb.Push(10)
	val, ok := rb.Pop()
	if !ok || val != 10 {
		t.Errorf("After Reset+Push, Pop() = (%d, %v), want (10, true)", val, ok)
	}
}

func TestRingBuffer_WrapAround(t *testing.T) {
	rb := core.NewRingBuffer[int](3, core.OverflowBlock)
	rb.Push(1)
	rb.Push(2)
	rb.Pop() // remove 1
	rb.Pop() // remove 2
	rb.Push(3)
	rb.Push(4)
	rb.Push(5)
	// head should have wrapped

	val, ok := rb.Pop()
	if !ok || val != 3 {
		t.Errorf("After wrap, Pop() = (%d, %v), want (3, true)", val, ok)
	}
	val, ok = rb.Pop()
	if !ok || val != 4 {
		t.Errorf("After wrap, Pop() = (%d, %v), want (4, true)", val, ok)
	}
	val, ok = rb.Pop()
	if !ok || val != 5 {
		t.Errorf("After wrap, Pop() = (%d, %v), want (5, true)", val, ok)
	}
}

func TestRingBuffer_Concurrent(t *testing.T) {
	rb := core.NewRingBuffer[int](10000, core.OverflowDrop)
	var wg sync.WaitGroup
	n := 1000

	// Concurrent producers
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				rb.Push(offset*n + i)
			}
		}(g)
	}

	// Concurrent consumers
	var consumed atomic.Int64
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				if _, ok := rb.Pop(); ok {
					consumed.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	// Drain remaining
	for {
		if _, ok := rb.Pop(); !ok {
			break
		}
		consumed.Add(1)
	}

	t.Logf("Consumed: %d, Disposed: %d", consumed.Load(), rb.Disposed())
}

// ==================== WaitTimeoutImpl Tests ====================

func TestWaitTimeoutImpl_NormalCompletion(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	var waited atomic.Bool
	var submitGuard atomic.Bool

	var cancelCount atomic.Int64
	cancel := func() { cancelCount.Add(1) }

	var cancelAllCount atomic.Int64
	cancelAll := func() { cancelAllCount.Add(1) }

	go func() {
		time.Sleep(10 * time.Millisecond)
		wg.Done()
	}()

	results, ok := core.WaitTimeoutImpl(5*time.Second, &wg, cancel, &submitGuard, &waited, cancelAll, func() []core.Result[int] {
		return []core.Result[int]{{Value: 42, Err: nil}}
	}, context.Background())

	if !ok {
		t.Error("WaitTimeoutImpl should return ok=true on normal completion")
	}
	if len(results) != 1 || results[0].Value != 42 {
		t.Errorf("results = %+v, want [{Value:42}]", results)
	}
	if !waited.Load() {
		t.Error("waited should be true after completion")
	}
}

func TestWaitTimeoutImpl_Timeout(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	var waited atomic.Bool
	var submitGuard atomic.Bool

	var cancelCount atomic.Int64
	cancel := func() { cancelCount.Add(1) }

	var cancelAllCount atomic.Int64
	cancelAll := func() { cancelAllCount.Add(1) }

	results, ok := core.WaitTimeoutImpl(10*time.Millisecond, &wg, cancel, &submitGuard, &waited, cancelAll, func() []core.Result[int] {
		return []core.Result[int]{{Value: 1}}
	}, context.Background())

	if ok {
		t.Error("WaitTimeoutImpl should return ok=false on timeout")
	}
	if len(results) != 1 {
		t.Errorf("results len = %d, want 1", len(results))
	}
	if !waited.Load() {
		t.Error("waited should be true even on timeout")
	}
	// cancel should have been called
	time.Sleep(50 * time.Millisecond)
	if cancelCount.Load() == 0 {
		t.Error("cancel should have been called on timeout")
	}
}

// ==================== WaitContextImpl Tests ====================

func TestWaitContextImpl_NormalCompletion(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	var waited atomic.Bool

	var cancelCount2 atomic.Int64
	cancel2 := func() { cancelCount2.Add(1) }

	var cancelAllCount2 atomic.Int64
	cancelAll2 := func() { cancelAllCount2.Add(1) }

	go func() {
		time.Sleep(10 * time.Millisecond)
		wg.Done()
	}()

	results, ok := core.WaitContextImpl(context.Background(), &wg, cancel2, nil, &waited, cancelAll2, func() []core.Result[int] {
		return []core.Result[int]{{Value: 100, Err: nil}}
	}, context.Background(), "Test")

	if !ok {
		t.Error("WaitContextImpl should return ok=true on normal completion")
	}
	if len(results) != 1 || results[0].Value != 100 {
		t.Errorf("results = %+v, want [{Value:100}]", results)
	}
}

func TestWaitContextImpl_ContextCancel(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	var waited atomic.Bool

	var cancelCount atomic.Int64
	cancel := func() { cancelCount.Add(1) }

	var cancelAllCount atomic.Int64
	cancelAll := func() { cancelAllCount.Add(1) }

	ctx, ctxCancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		ctxCancel()
	}()

	results, ok := core.WaitContextImpl(ctx, &wg, cancel, nil, &waited, cancelAll, func() []core.Result[int] {
		return []core.Result[int]{{Value: 1}}
	}, context.Background(), "Test")

	if ok {
		t.Error("WaitContextImpl should return ok=false on ctx cancel")
	}
	if len(results) != 1 {
		t.Errorf("results len = %d, want 1", len(results))
	}
}

// ==================== Sentinel Errors Tests ====================

func TestSentinelErrors(t *testing.T) {
	errs := []error{
		core.ErrPoolClosed,
		core.ErrPoolWaited,
		core.ErrSubmitTimeout,
		core.ErrGroupWaited,
		core.ErrGroupWaiting,
		core.ErrSkipped,
		core.ErrRateLimiterStopped,
		core.ErrRateLimitExceeded,
		core.ErrTimeout,
		core.ErrPoolWaiting,
	}

	for _, err := range errs {
		if err == nil || err.Error() == "" {
			t.Errorf("Sentinel error should not be nil or empty: %v", err)
		}
	}
}

// ==================== Production Scenario Tests ====================

func TestProduction_GlobalSettingsRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping production test in short mode")
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	n := 50

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				core.SetDefaultTimeout(time.Duration(int(time.Now().UnixNano()%1000)) * time.Millisecond)
				_ = core.GetDefaultTimeout()
				core.SetSubmitTimeout(100 * time.Millisecond)
				_ = core.GetSubmitTimeout()
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	close(stopCh)
	wg.Wait()
}

func TestProduction_LoggingRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping production test in short mode")
	}

	original := core.GetLogger()
	defer core.SetLogger(original)

	tl := &testLogger{}
	core.SetLogger(tl)

	oldLevel := core.GetTaskFailLogLevel()
	defer core.SetTaskFailLogLevel(oldLevel)

	var wg sync.WaitGroup
	n := 50
	stopCh := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := core.EnsureTraceID(context.Background())
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				core.LogCtxInfo(ctx, "test", core.Int64("i", int64(i)))
				core.LogTaskFailCtx(ctx, "fail test")
			}
		}(i)
	}

	time.Sleep(300 * time.Millisecond)
	close(stopCh)
	wg.Wait()
}

func TestProduction_RingBufferHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping production test in short mode")
	}

	rb := core.NewRingBuffer[int](100000, core.OverflowDrop)

	var wg sync.WaitGroup
	producers := 20
	consumers := 10
	opsPerWorker := 5000

	var pushTotal, popTotal atomic.Int64

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				rb.Push(offset*opsPerWorker + j)
				pushTotal.Add(1)
			}
		}(i)
	}

	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				if _, ok := rb.Pop(); ok {
					popTotal.Add(1)
				} else {
					j-- // retry
				}
			}
		}()
	}

	wg.Wait()

	// Drain remaining items
	for {
		if _, ok := rb.Pop(); ok {
			popTotal.Add(1)
		} else {
			break
		}
	}

	t.Logf("Pushed: %d, Popped: %d, Disposed: %d", pushTotal.Load(), popTotal.Load(), rb.Disposed())
}

// TestMain ensures cleanup of any global state
func TestMain(m *testing.M) {
	// Save and restore defaults
	oldTimeout := core.GetDefaultTimeout()
	oldSubmit := core.GetSubmitTimeout()
	oldCleanup := core.GetMaxCleanupDuration()
	oldMaxResults := core.GetDefaultMaxResults()
	oldRingCap := core.GetDefaultRingBufferCap()
	oldRingStrat := core.GetDefaultOverflowStrategy()
	oldFailLogLevel := core.GetTaskFailLogLevel()
	oldTraceEnabled := core.GetTraceLogEnabled()

	code := m.Run()

	// Restore
	core.SetDefaultTimeout(oldTimeout)
	core.SetSubmitTimeout(oldSubmit)
	core.SetMaxCleanupDuration(oldCleanup)
	core.SetDefaultMaxResults(oldMaxResults)
	core.SetDefaultRingBuffer(oldRingCap, oldRingStrat)
	core.SetTaskFailLogLevel(oldFailLogLevel)
	core.SetTraceLogEnabled(oldTraceEnabled)

	os.Exit(code)
}
