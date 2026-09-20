package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ==================== Result 测试 ====================

func TestResult_Ok(t *testing.T) {
	r := Result[int]{Value: 42}
	if !r.Ok() {
		t.Fatal("expected Ok=true for result without error")
	}
	r2 := Result[int]{Err: errors.New("fail")}
	if r2.Ok() {
		t.Fatal("expected Ok=false for result with error")
	}
}

func TestResult_IsPanic(t *testing.T) {
	pe := NewPanicError("boom")
	r := Result[int]{Err: pe}
	if !r.IsPanic() {
		t.Fatal("expected IsPanic=true")
	}
	r2 := Result[int]{Err: errors.New("normal error")}
	if r2.IsPanic() {
		t.Fatal("expected IsPanic=false for normal error")
	}
}

func TestResult_String(t *testing.T) {
	r1 := Result[int]{Value: 100}
	if r1.String() != "value=100" {
		t.Fatalf("unexpected string: %s", r1.String())
	}
	r2 := Result[int]{Err: errors.New("fail")}
	if r2.String() != "err=fail" {
		t.Fatalf("unexpected string: %s", r2.String())
	}
}

// ==================== PanicError 测试 ====================

func TestPanicError_Error(t *testing.T) {
	pe := NewPanicError("test panic")
	if pe.Error() != "panic: test panic" {
		t.Fatalf("unexpected: %s", pe.Error())
	}
}

func TestPanicError_Unwrap(t *testing.T) {
	pe := NewPanicError("test")
	if pe.Unwrap() != nil {
		t.Fatal("expected Unwrap to return nil")
	}
}

func TestPanicError_Format(t *testing.T) {
	pe := NewPanicError(42)
	s := fmt.Sprintf("%v", pe)
	if s != "panic: 42" {
		t.Fatalf("unexpected: %s", s)
	}
	s2 := fmt.Sprintf("%+v", pe)
	if len(s2) <= len(s) {
		t.Fatal("expected +v format to include stack trace")
	}
}

func TestPanicError_ConcurrentCreation(t *testing.T) {
	var wg sync.WaitGroup
	n := 10000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			pe := NewPanicError(fmt.Sprintf("panic-%d", i))
			if pe.Value == nil {
				t.Errorf("expected non-nil value")
			}
			if len(pe.Stack) == 0 {
				t.Errorf("expected non-empty stack")
			}
		}(i)
	}
	wg.Wait()
}

// ==================== 全局配置测试 ====================

func TestSetGetDefaultTimeout(t *testing.T) {
	orig := GetDefaultTimeout()
	defer SetDefaultTimeout(orig)

	SetDefaultTimeout(10 * time.Second)
	if got := GetDefaultTimeout(); got != 10*time.Second {
		t.Fatalf("expected 10s, got %v", got)
	}

	SetDefaultTimeout(0)
	if got := GetDefaultTimeout(); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}

func TestSetGetSubmitTimeout(t *testing.T) {
	orig := GetSubmitTimeout()
	defer SetSubmitTimeout(orig)

	SetSubmitTimeout(3 * time.Second)
	if got := GetSubmitTimeout(); got != 3*time.Second {
		t.Fatalf("expected 3s, got %v", got)
	}
}

func TestSetGetMaxCleanupDuration(t *testing.T) {
	orig := GetMaxCleanupDuration()
	defer SetMaxCleanupDuration(orig)

	SetMaxCleanupDuration(5 * time.Minute)
	if got := GetMaxCleanupDuration(); got != 5*time.Minute {
		t.Fatalf("expected 5m, got %v", got)
	}
}

func TestSetGetTaskFailLogLevel(t *testing.T) {
	orig := GetTaskFailLogLevel()
	defer SetTaskFailLogLevel(orig)

	SetTaskFailLogLevel(LogLevelWarn)
	if got := GetTaskFailLogLevel(); got != LogLevelWarn {
		t.Fatalf("expected LogLevelWarn, got %v", got)
	}

	SetTaskFailLogLevel(LogLevelSilent)
	if got := GetTaskFailLogLevel(); got != LogLevelSilent {
		t.Fatalf("expected LogLevelSilent, got %v", got)
	}
}

func TestSetGetTraceLogEnabled(t *testing.T) {
	orig := GetTraceLogEnabled()
	defer SetTraceLogEnabled(orig)

	SetTraceLogEnabled(false)
	if GetTraceLogEnabled() {
		t.Fatal("expected trace log disabled")
	}

	SetTraceLogEnabled(true)
	if !GetTraceLogEnabled() {
		t.Fatal("expected trace log enabled")
	}
}

// ==================== 并发度计算测试 ====================

func TestCPU(t *testing.T) {
	c := CPU()
	if c <= 0 {
		t.Fatalf("expected positive CPU count, got %d", c)
	}
}

func TestIO(t *testing.T) {
	io := IO()
	if io <= 0 {
		t.Fatalf("expected positive IO concurrency, got %d", io)
	}
	if io != CPU()*2 {
		t.Fatalf("expected IO=%d, got %d", CPU()*2, io)
	}
}

func TestIOMulti(t *testing.T) {
	m := IOMulti(4)
	if m != CPU()*4 {
		t.Fatalf("expected IOMulti(4)=%d, got %d", CPU()*4, m)
	}

	m2 := IOMulti(0)
	if m2 != CPU()*2 {
		t.Fatalf("expected IOMulti(0)=IO()=%d, got %d", CPU()*2, m2)
	}

	m3 := IOMulti(-1)
	if m3 != CPU()*2 {
		t.Fatalf("expected IOMulti(-1)=IO()=%d, got %d", CPU()*2, m3)
	}
}

func TestWithConfig(t *testing.T) {
	c1 := WithConfig(0)
	if c1 != IO() {
		t.Fatalf("expected WithConfig(0)=IO()=%d, got %d", IO(), c1)
	}

	c2 := WithConfig(8)
	if c2 != 8 {
		t.Fatalf("expected WithConfig(8)=8, got %d", c2)
	}

	c3 := WithConfig(-1)
	if c3 != IO() {
		t.Fatalf("expected WithConfig(-1)=IO()=%d, got %d", IO(), c3)
	}
}

// ==================== TraceID 测试 ====================

func TestEnsureTraceID(t *testing.T) {
	ctx := context.Background()
	ctx = EnsureTraceID(ctx)
	tid := GetTraceID(ctx)
	if tid == "" {
		t.Fatal("expected non-empty trace_id")
	}
	if len(tid) != 32 {
		t.Fatalf("expected 32 char trace_id, got %d", len(tid))
	}
}

func TestEnsureTraceID_Idempotent(t *testing.T) {
	ctx := context.Background()
	ctx = WithTraceID(ctx, "custom-trace-id")
	ctx = EnsureTraceID(ctx)
	tid := GetTraceID(ctx)
	if tid != "custom-trace-id" {
		t.Fatalf("expected custom-trace-id, got %s", tid)
	}
}

func TestWithTraceID(t *testing.T) {
	ctx := WithTraceID(context.Background(), "my-trace-123")
	if GetTraceID(ctx) != "my-trace-123" {
		t.Fatal("trace_id mismatch")
	}
}

func TestWithTraceID_Empty(t *testing.T) {
	ctx := WithTraceID(context.Background(), "")
	tid := GetTraceID(ctx)
	if tid == "" {
		t.Fatal("expected auto-generated trace_id")
	}
}

func TestNewTraceID(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := NewTraceID()
		if len(id) != 32 {
			t.Fatalf("expected 32 char trace_id, got %d", len(id))
		}
		if ids[id] {
			t.Fatalf("duplicate trace_id generated: %s", id)
		}
		ids[id] = true
	}
}

func TestNewTraceID_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	n := 50000
	ids := sync.Map{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			id := NewTraceID()
			if len(id) != 32 {
				t.Errorf("invalid trace_id length: %d", len(id))
			}
			if _, loaded := ids.LoadOrStore(id, true); loaded {
				t.Errorf("duplicate trace_id: %s", id)
			}
		}()
	}
	wg.Wait()
}

// ==================== SafeCall 测试 ====================

func TestSafeCall_Success(t *testing.T) {
	ctx := context.Background()
	val, err := SafeCall(ctx, 10, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 20 {
		t.Fatalf("expected 20, got %d", val)
	}
}

func TestSafeCall_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	val, err := SafeCall(ctx, 1, func(ctx context.Context, n int) (int, error) {
		panic("oh no")
	})
	if err == nil {
		t.Fatal("expected panic error")
	}
	if val != 0 {
		t.Fatalf("expected zero value, got %d", val)
	}
	if !errors.As(err, new(*PanicError)) {
		t.Fatalf("expected PanicError, got %T", err)
	}
}

func TestSafeCallVoid(t *testing.T) {
	ctx := context.Background()
	err := SafeCallVoid(ctx, 1, func(ctx context.Context, n int) error {
		return errors.New("fail")
	})
	if err == nil || err.Error() != "fail" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSafeCallVoid_Panic(t *testing.T) {
	ctx := context.Background()
	err := SafeCallVoid(ctx, 1, func(ctx context.Context, n int) error {
		panic("boom")
	})
	if err == nil {
		t.Fatal("expected panic error")
	}
}

// ==================== MergeCancel 测试 ====================

func TestMergeCancel(t *testing.T) {
	var oldCancelled, newCancelled atomic.Bool
	oldCancel := func() { oldCancelled.Store(true) }
	newCancel := func() { newCancelled.Store(true) }

	merged := MergeCancel(oldCancel, newCancel)
	merged()

	if !oldCancelled.Load() {
		t.Fatal("expected old cancel to be called")
	}
	if !newCancelled.Load() {
		t.Fatal("expected new cancel to be called")
	}
}

func TestMergeCancel_Nil(t *testing.T) {
	var called atomic.Bool
	newCancel := func() { called.Store(true) }
	merged := MergeCancel(nil, newCancel)
	merged()
	if !called.Load() {
		t.Fatal("expected cancel to be called")
	}
}

// ==================== Logger 测试 ====================

func TestSetGetLogger(t *testing.T) {
	orig := GetLogger()
	defer SetLogger(orig)

	SetLogger(nil)
	if GetLogger() == nil {
		t.Fatal("expected non-nil logger (nop)")
	}
}

func TestLogger_DefaultNop(t *testing.T) {
	orig := GetLogger()
	defer SetLogger(orig)
	SetLogger(nil)

	logger := GetLogger()
	logger.Log(context.Background(), LevelError, "test")
	// Should not panic
}

// ==================== PoolTask 测试 ====================

func TestPoolTask(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	task := PoolTask[int]{
		Ctx:     ctx,
		Cancel:  cancel,
		Fn:      func(ctx context.Context) (int, error) { return 42, nil },
		Timeout: time.Second,
		Index:   0,
	}
	if task.Fn == nil {
		t.Fatal("expected non-nil fn")
	}
	cancel()
}

// ==================== 高并发极限压力测试 ====================

// TestGlobalConfigConcurrent 全局配置并发读写
func TestGlobalConfigConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	n := 50000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			switch i % 5 {
			case 0:
				SetDefaultTimeout(time.Duration(i%100+1) * time.Second)
			case 1:
				GetDefaultTimeout()
			case 2:
				SetSubmitTimeout(time.Duration(i%50+1) * time.Second)
			case 3:
				GetSubmitTimeout()
			case 4:
				SetTaskFailLogLevel(TaskLogLevel(i % 5))
			}
		}(i)
	}
	wg.Wait()
	t.Logf("50K concurrent global config read/write completed")
}

// TestSafeCall_HighConcurrency 高并发 SafeCall
func TestSafeCall_HighConcurrency(t *testing.T) {
	ctx := context.Background()
	var wg sync.WaitGroup
	n := 100000
	var success, panicCaught atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			val, err := SafeCall(ctx, i, func(ctx context.Context, n int) (int, error) {
				if n%100 == 0 {
					panic("periodic panic")
				}
				return n * 2, nil
			})
			if err != nil {
				panicCaught.Add(1)
			} else if val == i*2 {
				success.Add(1)
			}
		}(i)
	}
	wg.Wait()
	expectedPanics := int64(n / 100)
	if n%100 == 0 {
		expectedPanics = int64(n/100) + 1
	}
	if panicCaught.Load() < expectedPanics-5 {
		t.Fatalf("expected ~%d panics caught, got %d", expectedPanics, panicCaught.Load())
	}
	if success.Load() < int64(n)-expectedPanics-5 {
		t.Fatalf("expected ~%d successes, got %d", int64(n)-expectedPanics, success.Load())
	}
	t.Logf("100K SafeCall: success=%d, panic=%d", success.Load(), panicCaught.Load())
}

// TestTraceID_HighConcurrency 高并发 TraceID 生成去重
func TestTraceID_HighConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	n := 200000
	ids := sync.Map{}
	var duplicates atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			id := NewTraceID()
			if _, loaded := ids.LoadOrStore(id, true); loaded {
				duplicates.Add(1)
			}
		}()
	}
	wg.Wait()
	if duplicates.Load() > 0 {
		t.Fatalf("found %d duplicate trace_ids out of %d", duplicates.Load(), n)
	}
	t.Logf("200K unique trace_ids generated, duplicates=%d", duplicates.Load())
}

// TestResult_ConcurrentCreation 高并发 Result 创建
func TestResult_ConcurrentCreation(t *testing.T) {
	var wg sync.WaitGroup
	n := 200000
	results := make([]Result[int], n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				results[idx] = Result[int]{Value: idx}
			} else {
				results[idx] = Result[int]{Err: fmt.Errorf("err-%d", idx)}
			}
		}(i)
	}
	wg.Wait()
	okCount := 0
	for _, r := range results {
		if r.Ok() {
			okCount++
		}
	}
	if okCount != n/2 {
		t.Fatalf("expected %d ok results, got %d", n/2, okCount)
	}
}
