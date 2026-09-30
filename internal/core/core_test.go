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

// ============================================================
// 共享工具：四档数据量（万/十万/百万/千万），short 跳过
// ============================================================

type tier struct {
	name string
	size int
}

var allTiers = []tier{
	{"万级_10K", 10_000},
	{"十万级_100K", 100_000},
	{"百万级_1M", 1_000_000},
	{"千万级_10M", 10_000_000},
}

func skipIfTooLarge(t *testing.T, size int) {
	if testing.Short() && size >= 100_000 {
		t.Skip("short mode: skip large scale test")
	}
}

// ============================================================
// 一、Result 测试
// ============================================================

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

// ============================================================
// 二、PanicError 测试
// ============================================================

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

// ============================================================
// 三、全局配置 Get/Set 测试
// ============================================================

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

// ============================================================
// 四、并发度计算测试
// ============================================================

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

// ============================================================
// 五、TraceID 测试
// ============================================================

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

// ============================================================
// 六、SafeCall 测试
// ============================================================

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

func TestSafeCallVoid_Success(t *testing.T) {
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

// ============================================================
// 七、MergeCancel 测试
// ============================================================

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

// ============================================================
// 八、Logger 测试
// ============================================================

func TestLogField_Constructors(t *testing.T) {
	f1 := Str("key", "value")
	if f1.Key != "key" || f1.Value != "value" {
		t.Fatalf("unexpected Str: %v", f1)
	}
	f2 := Err(errors.New("test error"))
	if f2.Key != "error" {
		t.Fatalf("expected key='error', got %s", f2.Key)
	}
	f3 := Dur("elapsed", 5*time.Second)
	if f3.Key != "elapsed" || f3.Value != 5*time.Second {
		t.Fatalf("unexpected Dur: %v", f3)
	}
	f4 := Any("data", map[string]int{"a": 1})
	if f4.Key != "data" {
		t.Fatalf("unexpected Any key: %s", f4.Key)
	}
	f5 := Int64("count", 100)
	if f5.Key != "count" || f5.Value != int64(100) {
		t.Fatalf("unexpected Int64: %v", f5)
	}
}

func TestNopLogger_AllMethods(t *testing.T) {
	n := &nopLogger{}
	ctx := context.Background()
	n.Log(ctx, LevelError, "error msg", Str("key", "val"))
	n.Log(ctx, LevelWarn, "warn msg")
	n.Log(ctx, LevelInfo, "info msg")
	n.Log(ctx, LevelDebug, "debug msg")

	logger := n.With(Str("a", "b"))
	logger.Log(ctx, LevelError, "test")
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	ctx2 := n.WithContext(ctx)
	if ctx2 != ctx {
		t.Fatal("expected same context from nop logger")
	}
}

func TestSetGetLogger(t *testing.T) {
	orig := GetLogger()
	defer SetLogger(orig)

	SetLogger(nil)
	if GetLogger() == nil {
		t.Fatal("expected non-nil logger (nop)")
	}
}

func TestLogLevels(t *testing.T) {
	if LevelError != 0 {
		t.Fatal("expected LevelError=0")
	}
	if LevelWarn != 1 {
		t.Fatal("expected LevelWarn=1")
	}
}

func TestLogCtx_NoPanic(t *testing.T) {
	ctx := context.Background()
	logCtx(ctx, LevelError, "test message", Str("key", "value"))
}

func TestLogCtx_NilCtx(t *testing.T) {
	logCtx(nil, LevelError, "test nil ctx")
}

func TestLogGlobal(t *testing.T) {
	logGlobal(LevelInfo, "test global log")
}

// ============================================================
// 九、PoolTask 测试
// ============================================================

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

// ============================================================
// 十、RingBuffer 基础测试
// ============================================================

func TestRingBuffer_RollbackRace(t *testing.T) {
	const (
		capacity        = 64
		numWriters      = 8
		writesPerWorker = 5000
	)

	rb := NewRingBuffer[int](capacity, OverflowBlock)

	var writeSuccess atomic.Int64
	var writeFail atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				if rb.Push(workerID*writesPerWorker + i) {
					writeSuccess.Add(1)
				} else {
					writeFail.Add(1)
				}
			}
		}(w)
	}

	var popSuccess atomic.Int64
	popDone := make(chan struct{})
	go func() {
		defer close(popDone)
		for {
			if _, ok := rb.Pop(); ok {
				popSuccess.Add(1)
			} else {
				return
			}
		}
	}()

	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	// drain remaining
	for {
		if _, ok := rb.Pop(); !ok {
			break
		}
		popSuccess.Add(1)
	}

	totalWrites := writeSuccess.Load() + writeFail.Load()
	expectedTotal := int64(numWriters * writesPerWorker)
	if totalWrites != expectedTotal {
		t.Errorf("total writes mismatch: got %d, want %d", totalWrites, expectedTotal)
	}
	if rb.Len() != 0 {
		t.Errorf("expected buffer empty: got %d", rb.Len())
	}
	if writeSuccess.Load() != popSuccess.Load() {
		t.Errorf("Push=%d != Pop=%d", writeSuccess.Load(), popSuccess.Load())
	}
}

func TestRingBuffer_RollbackCAS(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)
	capacity := rb.Cap()
	for i := 0; i < capacity; i++ {
		if !rb.Push(i) {
			t.Fatalf("prefill Push failed: i=%d", i)
		}
	}
	if !rb.IsFull() {
		t.Fatalf("buffer should be full: cap=%d len=%d", capacity, rb.Len())
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rb.Push(999)
		}()
	}
	wg.Wait()

	t.Logf("concurrent push to full buffer: Len=%d (may contain holes)", rb.Len())

	popped := 0
	for {
		if _, ok := rb.Pop(); ok {
			popped++
		} else {
			break
		}
	}
	if popped != capacity {
		t.Errorf("expected Pop %d, got %d", capacity, popped)
	}
	if rb.Len() != 0 {
		t.Errorf("buffer should be empty: got %d", rb.Len())
	}
}

func TestRingBuffer_PeekWithHoles(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)
	for i := 0; i < 10; i++ {
		rb.Push(i)
	}
	for i := 0; i < 5; i++ {
		rb.Pop()
	}
	val, ok := rb.Peek()
	if !ok {
		t.Fatal("Peek should return true")
	}
	if val != 5 {
		t.Errorf("Peek expected 5, got %d", val)
	}
}

func TestRingBuffer_OverflowDrop(t *testing.T) {
	capacity := 16
	rb := NewRingBuffer[int](capacity, OverflowDrop)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				rb.Push(v*1000 + j)
			}
		}(i)
	}
	wg.Wait()

	dropped := rb.Dropped()
	t.Logf("dropped: %d", dropped)

	popped := 0
	for {
		_, ok := rb.Pop()
		if !ok {
			break
		}
		popped++
	}
	maxCapacity := rb.Cap()
	if popped > maxCapacity {
		t.Errorf("Pop should not exceed capacity: got %d, capacity %d", popped, maxCapacity)
	}
}

func TestRingBuffer_ConcurrentPushPop(t *testing.T) {
	capacity := 64
	rb := NewRingBuffer[int](capacity, OverflowBlock)

	var wg sync.WaitGroup
	pushCount := atomic.Int64{}
	popCount := atomic.Int64{}
	stop := atomic.Bool{}

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counter := 0
			for !stop.Load() {
				if rb.Push(counter) {
					pushCount.Add(1)
				}
				counter++
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if _, ok := rb.Pop(); ok {
					popCount.Add(1)
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	for {
		if _, ok := rb.Pop(); ok {
			popCount.Add(1)
		} else {
			break
		}
	}

	remaining := rb.Len()
	totalPushed := pushCount.Load()
	totalPopped := popCount.Load()
	t.Logf("Push=%d Pop=%d remain=%d", totalPushed, totalPopped, remaining)
	if totalPushed != totalPopped+int64(remaining) {
		t.Errorf("data inconsistency: push=%d, pop=%d, remain=%d", totalPushed, totalPopped, remaining)
	}
}

func TestRingBuffer_RollbackRaceStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skip stress test in short mode")
	}
	rb := NewRingBuffer[int](32, OverflowBlock)
	capacity := rb.Cap()
	prefillCount := capacity / 2
	for i := 0; i < prefillCount; i++ {
		rb.Push(i)
	}

	var wg sync.WaitGroup
	successCount := atomic.Int64{}
	failCount := atomic.Int64{}
	const numWriters = 100
	const attempts = 1000

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < attempts; i++ {
				if rb.Push(i) {
					successCount.Add(1)
				} else {
					failCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	t.Logf("success=%d fail=%d (cap=%d)", successCount.Load(), failCount.Load(), capacity)
}

// ============================================================
// 十一、四档并发压力测试（万/十万/百万/千万）
// ============================================================

func TestPanicError_ConcurrentCreation(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func(i int) {
					defer wg.Done()
					pe := NewPanicError(fmt.Sprintf("panic-%d", i))
					if pe.Value == nil || len(pe.Stack) == 0 {
						t.Errorf("invalid PanicError at %d", i)
					}
				}(i)
			}
			wg.Wait()
		})
	}
}

func TestGlobalConfig_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
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
		})
	}
}

func TestSafeCall_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			ctx := context.Background()
			var wg sync.WaitGroup
			var success, panicCaught atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
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
			t.Logf("SafeCall: success=%d panic=%d", success.Load(), panicCaught.Load())
		})
	}
}

func TestTraceID_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			ids := sync.Map{}
			var duplicates atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func() {
					defer wg.Done()
					id := NewTraceID()
					if len(id) != 32 {
						t.Errorf("invalid trace_id length: %d", len(id))
					}
					if _, loaded := ids.LoadOrStore(id, true); loaded {
						duplicates.Add(1)
					}
				}()
			}
			wg.Wait()
			if duplicates.Load() > 0 {
				t.Fatalf("duplicates: %d / %d", duplicates.Load(), tier.size)
			}
		})
	}
}

func TestResult_ConcurrentCreation(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			results := make([]Result[int], tier.size)
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
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
			if okCount != tier.size/2 {
				t.Fatalf("expected %d ok, got %d", tier.size/2, okCount)
			}
		})
	}
}

func TestLogField_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func(i int) {
					defer wg.Done()
					switch i % 5 {
					case 0:
						_ = Str("key", "value")
					case 1:
						_ = Err(errors.New("test"))
					case 2:
						_ = Dur("d", time.Second)
					case 3:
						_ = Any("a", i)
					case 4:
						_ = Int64("c", int64(i))
					}
				}(i)
			}
			wg.Wait()
		})
	}
}

func TestLogCtx_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			ctx := context.Background()
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func() {
					defer wg.Done()
					logCtx(ctx, LevelDebug, "concurrent log", Str("test", "val"))
				}()
			}
			wg.Wait()
		})
	}
}

// ============================================================
// 十二、Race 竞态测试
// ============================================================

func TestRace_GlobalConfig_ReadWrite(t *testing.T) {
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		n := 5000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				SetDefaultTimeout(1 * time.Second)
				GetDefaultTimeout()
				SetSubmitTimeout(2 * time.Second)
				GetSubmitTimeout()
			}()
		}
		wg.Wait()
	}
}

func TestRace_SafeCall_PanicRace(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		var wg sync.WaitGroup
		n := 5000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()
				SafeCall(ctx, i, func(ctx context.Context, n int) (int, error) {
					if n%2 == 0 {
						panic("boom")
					}
					return n, nil
				})
			}(i)
		}
		wg.Wait()
	}
}

func TestRace_TraceID_GenerateRace(t *testing.T) {
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		n := 5000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				id := NewTraceID()
				if len(id) != 32 {
					t.Errorf("invalid trace_id length")
				}
			}()
		}
		wg.Wait()
	}
}

func TestRace_RingBuffer_PushPop(t *testing.T) {
	for round := 0; round < 10; round++ {
		rb := NewRingBuffer[int](64, OverflowBlock)
		var wg sync.WaitGroup
		n := 5000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				rb.Push(1)
				rb.Pop()
			}()
		}
		wg.Wait()
	}
}

func TestRace_MergeCancel_ConcurrentCalls(t *testing.T) {
	for round := 0; round < 10; round++ {
		var counter atomic.Int64
		cancel := func() { counter.Add(1) }
		var wg sync.WaitGroup
		n := 5000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				merged := MergeCancel(cancel, cancel)
				merged()
			}()
		}
		wg.Wait()
		if counter.Load() < int64(n) {
			t.Errorf("counter=%d, expected >=%d", counter.Load(), n)
		}
	}
}
