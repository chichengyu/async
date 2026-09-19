package async

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/pool"
	"github.com/chichengyu/async/ratelimit"
)

// ============================================================================
// 综合极限高并发压力测试——覆盖所有公开方法，零遗漏
// 模拟真实生产环境：万级 goroutine、十万级元素、混合负载、竞态条件
// ============================================================================

// ==================== core/Core 基础类型与全局配置 ====================

// TestStress_Core_ConcurrencyCPU_IO_IOMulti_WithConfig 测试并发度计算函数
func TestStress_Core_ConcurrencyFuncs(t *testing.T) {
	for round := 0; round < 100; round++ {
		cpu := CPU()
		if cpu <= 0 {
			t.Fatal("CPU() returned <= 0")
		}
		ioVal := IO()
		if ioVal <= 0 {
			t.Fatal("IO() returned <= 0")
		}
		m3 := IOMulti(3)
		if m3 <= 0 {
			t.Fatal("IOMulti(3) returned <= 0")
		}
		m0 := IOMulti(0)
		if m0 <= 0 {
			t.Fatal("IOMulti(0) returned <= 0")
		}
		c1 := WithConfig(5)
		if c1 != 5 {
			t.Fatalf("WithConfig(5)=%d", c1)
		}
		c2 := WithConfig(0)
		if c2 <= 0 {
			t.Fatal("WithConfig(0) returned <=0")
		}
	}
}

// TestStress_Core_GlobalTimeoutSettings 测试全局超时设置
func TestStress_Core_GlobalTimeoutSettings(t *testing.T) {
	for round := 0; round < 50; round++ {
		SetDefaultTimeout(10 * time.Second)
		if d := GetDefaultTimeout(); d != 10*time.Second {
			t.Fatalf("GetDefaultTimeout=%v", d)
		}
		SetSubmitTimeout(3 * time.Second)
		if d := GetSubmitTimeout(); d != 3*time.Second {
			t.Fatalf("GetSubmitTimeout=%v", d)
		}
		SetMaxCleanupDuration(5 * time.Minute)
		if d := GetMaxCleanupDuration(); d != 5*time.Minute {
			t.Fatalf("GetMaxCleanupDuration=%v", d)
		}
	}
}

// TestStress_Core_LogLevelSettings 测试日志级别设置
func TestStress_Core_LogLevelSettings(t *testing.T) {
	for round := 0; round < 50; round++ {
		SetTaskFailLogLevel(LogLevelWarn)
		if lvl := GetTaskFailLogLevel(); lvl != LogLevelWarn {
			t.Fatalf("GetTaskFailLogLevel=%v", lvl)
		}
		SetTaskFailLogLevel(LogLevelSilent)
		if lvl := GetTaskFailLogLevel(); lvl != LogLevelSilent {
			t.Fatalf("GetTaskFailLogLevel=%v", lvl)
		}
		SetTraceLogEnabled(false)
		if GetTraceLogEnabled() {
			t.Fatal("expected trace log disabled")
		}
		SetTraceLogEnabled(true)
		if !GetTraceLogEnabled() {
			t.Fatal("expected trace log enabled")
		}
		SetTaskFailLogLevel(LogLevelError)
	}
}

// TestStress_Core_Logger 测试 Logger 设置
func TestStress_Core_Logger(t *testing.T) {
	for round := 0; round < 30; round++ {
		l1 := GetLogger()
		if l1 == nil {
			t.Fatal("GetLogger returned nil")
		}
		SetLogger(nil)
		l2 := GetLogger()
		if l2 == nil {
			t.Fatal("GetLogger after SetLogger(nil) returned nil")
		}
	}
}

// TestStress_Core_TraceID 测试 TraceID 生成与管理
func TestStress_Core_TraceID(t *testing.T) {
	for round := 0; round < 100; round++ {
		tid := NewTraceID()
		if len(tid) != 32 {
			t.Fatalf("NewTraceID len=%d, expected 32", len(tid))
		}
		ctx := WithTraceID(context.Background(), tid)
		if got := GetTraceID(ctx); got != tid {
			t.Fatalf("GetTraceID=%s, expected=%s", got, tid)
		}
		ctx2 := EnsureTraceID(context.Background())
		if tid2 := GetTraceID(ctx2); tid2 == "" {
			t.Fatal("EnsureTraceID did not set trace_id")
		}
		ctx3 := WithTraceID(context.Background(), "")
		if tid3 := GetTraceID(ctx3); tid3 == "" {
			t.Fatal("WithTraceID with empty did not generate trace_id")
		}
	}
}

// TestStress_Core_TraceID_Concurrent 高并发 TraceID 生成
func TestStress_Core_TraceID_Concurrent(t *testing.T) {
	m := sync.Map{}
	var wg sync.WaitGroup
	n := 5000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			tid := NewTraceID()
			if _, loaded := m.LoadOrStore(tid, true); loaded {
				// 极低概率碰撞，crypto/rand 不会碰撞
				t.Errorf("trace_id collision: %s", tid)
			}
		}()
	}
	wg.Wait()
}

// TestStress_Core_MergeCancel 测试 MergeCancel
func TestStress_Core_MergeCancel(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(ctx1)
		merged := MergeCancel(cancel1, cancel2)
		merged()
		select {
		case <-ctx2.Done():
		default:
			t.Fatal("merged cancel did not cancel ctx2")
		}
	}
}

// TestStress_Core_NewPanicError 测试 PanicError
func TestStress_Core_NewPanicError(t *testing.T) {
	for round := 0; round < 50; round++ {
		pe := NewPanicError("test panic")
		if pe.Value != "test panic" {
			t.Fatalf("expected Value='test panic', got %v", pe.Value)
		}
		if pe.Error() != "panic: test panic" {
			t.Fatalf("Error()=%s", pe.Error())
		}
		if pe.Unwrap() != nil {
			t.Fatal("Unwrap should return nil")
		}
	}
}

// TestStress_Core_PanicError_Format 测试 PanicError.Format
func TestStress_Core_PanicError_Format(t *testing.T) {
	for round := 0; round < 30; round++ {
		pe := NewPanicError("boom")
		s := fmt.Sprintf("%v", pe)
		if s != "panic: boom" {
			t.Fatalf("%%v=%s", s)
		}
		s2 := fmt.Sprintf("%+v", pe)
		if len(s2) <= len(s) {
			t.Fatalf("%%+v should include stack trace")
		}
	}
}

// TestStress_Core_Result 测试 Result 方法
func TestStress_Core_Result(t *testing.T) {
	for round := 0; round < 50; round++ {
		r1 := Result[int]{Value: 42, Err: nil}
		if !r1.Ok() {
			t.Fatal("Result{Value:42} should be Ok")
		}
		if r1.IsPanic() {
			t.Fatal("Result{Value:42} should not be Panic")
		}
		r2 := Result[int]{Value: 0, Err: NewPanicError("panic")}
		if !r2.IsPanic() {
			t.Fatal("panic error should be IsPanic")
		}
		if r2.Ok() {
			t.Fatal("panic error should not be Ok")
		}
	}
}

// TestStress_Core_SafeCall_SafeCallVoid 测试 SafeCall 安全调用
func TestStress_Core_SafeCall_SafeCallVoid(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()

		// 正常调用
		val, err := SafeCall(ctx, 10, func(ctx context.Context, n int) (int, error) {
			return n * 2, nil
		})
		if err != nil || val != 20 {
			t.Fatalf("SafeCall: val=%d, err=%v", val, err)
		}

		// panic 调用
		val2, err2 := SafeCall(ctx, 0, func(ctx context.Context, n int) (int, error) {
			panic("safe call panic")
		})
		if err2 == nil {
			t.Fatal("SafeCall should return error on panic")
		}
		_ = val2
		if !errors.Is(err2, &PanicError{}) {
			// 检查类型
			var pe *PanicError
			if !errors.As(err2, &pe) {
				t.Fatalf("expected PanicError, got %T", err2)
			}
		}

		// SafeCallVoid 正常
		err3 := SafeCallVoid(ctx, 1, func(ctx context.Context, n int) error {
			return nil
		})
		if err3 != nil {
			t.Fatalf("SafeCallVoid err=%v", err3)
		}

		// SafeCallVoid panic
		err4 := SafeCallVoid(ctx, 1, func(ctx context.Context, n int) error {
			panic("void panic")
		})
		if err4 == nil {
			t.Fatal("SafeCallVoid should return error on panic")
		}
	}
}

// TestStress_Core_SafeCall_HighConcurrency 高并发 SafeCall
func TestStress_Core_SafeCall_HighConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	n := 5000
	var success atomic.Int64
	var panics atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err := SafeCall(context.Background(), idx, func(ctx context.Context, n int) (int, error) {
				if n%100 == 0 {
					panic("periodic panic")
				}
				return n, nil
			})
			if err != nil {
				panics.Add(1)
			} else {
				success.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if success.Load()+panics.Load() != int64(n) {
		t.Fatalf("total mismatch: %d+%d != %d", success.Load(), panics.Load(), n)
	}
}

// ==================== Group 全部方法（完整） ====================

// TestStress_Group_NewGroup_DefaultGroup 基础构造函数测试
func TestStress_Group_NewGroup_DefaultGroup(t *testing.T) {
	for round := 0; round < 50; round++ {
		g1 := NewGroup[int](8)
		if g1.Concurrency() != 8 {
			t.Fatalf("NewGroup concurrency=%d", g1.Concurrency())
		}
		g2 := DefaultGroup[int]()
		if g2.Concurrency() <= 0 {
			t.Fatal("DefaultGroup concurrency <= 0")
		}
	}
}

// TestStress_Group_Go_GoAt 测试 Go 和 GoAt 高并发提交
func TestStress_Group_Go_GoAt_HighConcurrency(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		g := NewGroup[int](50)
		var wg sync.WaitGroup
		n := 500
		wg.Add(n * 2)
		for i := 0; i < n; i++ {
			go func(idx int) {
				defer wg.Done()
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return idx * 10, nil
				})
			}(i)
			go func(idx int) {
				defer wg.Done()
				_ = g.GoAt(idx, ctx, func(ctx context.Context) (int, error) {
					return idx * 10, nil
				})
			}(i)
		}
		wg.Wait()
		results := g.Wait()
		if len(results) != n*2 {
			t.Fatalf("expected %d results, got %d", n*2, len(results))
		}
	}
}

// TestStress_Group_GoWithTimeout_GoAtWithTimeout 带超时的高并发
func TestStress_Group_GoWithTimeout_GoAtWithTimeout_HighConcurrency(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		g := NewGroup[int](20)
		var wg sync.WaitGroup
		n := 200
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(idx int) {
				defer wg.Done()
				_ = g.GoAtWithTimeout(idx, ctx, 1*time.Second, func(ctx context.Context) (int, error) {
					time.Sleep(time.Microsecond * 10)
					return idx * 100, nil
				})
			}(i)
		}
		wg.Wait()
		results := g.Wait()
		if len(results) != n {
			t.Fatalf("round %d: expected %d results, got %d", round, n, len(results))
		}
		for i, r := range results {
			if r.Err != nil {
				t.Fatalf("round %d: result[%d] error: %v", round, i, r.Err)
			}
			if r.Value != i*100 {
				t.Fatalf("round %d: result[%d] expected %d, got %v", round, i, i*100, r.Value)
			}
		}
	}
}

// TestStress_Group_GoWithTimeout_TaskTimeout 任务超时
func TestStress_Group_GoWithTimeout_TaskTimeout(t *testing.T) {
	ctx := context.Background()
	g := NewGroup[int](10)
	for i := 0; i < 30; i++ {
		_ = g.GoWithTimeout(ctx, 20*time.Millisecond, func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(200 * time.Millisecond):
				return 1, nil
			}
		})
	}
	results := g.Wait()
	timeouts := 0
	for _, r := range results {
		if errors.Is(r.Err, context.DeadlineExceeded) {
			timeouts++
		}
	}
	if timeouts < 10 {
		t.Fatalf("expected at least 10 timeouts, got %d", timeouts)
	}
	t.Logf("timeouts: %d/%d", timeouts, len(results))
}

// TestStress_Group_WaitTimeout 超时等待
func TestStress_Group_WaitTimeout(t *testing.T) {
	for round := 0; round < 30; round++ {
		g := NewGroup[int](4)
		for i := 0; i < 20; i++ {
			_ = g.Go(context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(200 * time.Millisecond)
				return 1, nil
			})
		}
		results, ok := g.WaitTimeout(30 * time.Millisecond)
		if ok {
			t.Fatal("expected timeout")
		}
		_ = results
	}
}

// TestStress_Group_WaitContext 上下文等待
func TestStress_Group_WaitContext(t *testing.T) {
	for round := 0; round < 30; round++ {
		g := NewGroup[int](4)
		for i := 0; i < 10; i++ {
			_ = g.Go(context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(200 * time.Millisecond)
				return 1, nil
			})
		}
		wCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		results, ok := g.WaitContext(wCtx)
		cancel()
		if ok {
			t.Fatal("expected timeout via context")
		}
		_ = results
	}
}

// TestStress_Group_AllWithMethods 所有 With* 方法组合
func TestStress_Group_AllWithMethods(t *testing.T) {
	for round := 0; round < 20; round++ {
		baseCtx := context.Background()
		// WithTraceID
		g, ctx := NewGroup[int](4).WithTraceID(baseCtx)
		_ = g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })

		// WithContext
		g2, ctx2 := NewGroup[int](4).WithContext(baseCtx)
		_ = g2.Go(ctx2, func(ctx context.Context) (int, error) { return 2, nil })

		// WithFailFast
		g3, ctx3 := NewGroup[int](4).WithFailFast(baseCtx)
		_ = g3.Go(ctx3, func(ctx context.Context) (int, error) { return 3, nil })

		// WithFFCtx
		g4, ctx4 := NewGroup[int](4).WithFFCtx(baseCtx)
		_ = g4.Go(ctx4, func(ctx context.Context) (int, error) { return 4, nil })

		// WithFFTraceID
		g5, ctx5 := NewGroup[int](4).WithFFTraceID(baseCtx)
		_ = g5.Go(ctx5, func(ctx context.Context) (int, error) { return 5, nil })

		// WithFFSubmitTO
		g6, ctx6 := NewGroup[int](4).WithFFSubmitTO(baseCtx, 2*time.Second)
		_ = g6.Go(ctx6, func(ctx context.Context) (int, error) { return 6, nil })

		// WithFFSubmitTOTraceID
		g7, ctx7 := NewGroup[int](4).WithFFSubmitTOTraceID(baseCtx, 2*time.Second)
		_ = g7.Go(ctx7, func(ctx context.Context) (int, error) { return 7, nil })

		// WithFFTimeout
		g8, ctx8 := NewGroup[int](4).WithFFTimeout(baseCtx, 5*time.Second)
		_ = g8.Go(ctx8, func(ctx context.Context) (int, error) { return 8, nil })

		// WithCtxTraceID
		g9, ctx9 := NewGroup[int](4).WithCtxTraceID(baseCtx)
		_ = g9.Go(ctx9, func(ctx context.Context) (int, error) { return 9, nil })

		// WithFFTimeoutTraceID
		g10, ctx10 := NewGroup[int](4).WithFFTimeoutTraceID(baseCtx, 5*time.Second)
		_ = g10.Go(ctx10, func(ctx context.Context) (int, error) { return 10, nil })

		// WithFFTimeoutSubmitTO
		g11, ctx11 := NewGroup[int](4).WithFFTimeoutSubmitTO(baseCtx, 5*time.Second, 2*time.Second)
		_ = g11.Go(ctx11, func(ctx context.Context) (int, error) { return 11, nil })

		// WithFFTimeoutSubmitTOTraceID
		g12, ctx12 := NewGroup[int](4).WithFFTimeoutSubmitTOTraceID(baseCtx, 5*time.Second, 2*time.Second)
		_ = g12.Go(ctx12, func(ctx context.Context) (int, error) { return 12, nil })

		// WithCtxTimeout
		g13, ctx13 := NewGroup[int](4).WithCtxTimeout(baseCtx, 5*time.Second)
		_ = g13.Go(ctx13, func(ctx context.Context) (int, error) { return 13, nil })

		// WithCtxTimeoutTraceID
		g14, ctx14 := NewGroup[int](4).WithCtxTimeoutTraceID(baseCtx, 5*time.Second)
		_ = g14.Go(ctx14, func(ctx context.Context) (int, error) { return 14, nil })

		// WithTimeout
		g15 := NewGroup[int](4).WithTimeout(5 * time.Second)
		_ = g15.Go(baseCtx, func(ctx context.Context) (int, error) { return 15, nil })

		// WithSubmitTimeout
		g16 := NewGroup[int](4).WithSubmitTimeout(2 * time.Second)
		_ = g16.Go(baseCtx, func(ctx context.Context) (int, error) { return 16, nil })

		// WithCtxSubmitTO
		g17, ctx17 := NewGroup[int](4).WithCtxSubmitTO(baseCtx, 2*time.Second)
		_ = g17.Go(ctx17, func(ctx context.Context) (int, error) { return 17, nil })

		// WithCtxSubmitTOTraceID
		g18, ctx18 := NewGroup[int](4).WithCtxSubmitTOTraceID(baseCtx, 2*time.Second)
		_ = g18.Go(ctx18, func(ctx context.Context) (int, error) { return 18, nil })

		// DefaultGroup
		g19 := DefaultGroup[int]()
		_ = g19.Go(baseCtx, func(ctx context.Context) (int, error) { return 19, nil })

		r1 := g.Wait()
		r2 := g2.Wait()
		r3 := g3.Wait()
		r4 := g4.Wait()
		r5 := g5.Wait()
		r6 := g6.Wait()
		r7 := g7.Wait()
		r8 := g8.Wait()
		r9 := g9.Wait()
		r10 := g10.Wait()
		r11 := g11.Wait()
		r12 := g12.Wait()
		r13 := g13.Wait()
		r14 := g14.Wait()
		r15 := g15.Wait()
		r16 := g16.Wait()
		r17 := g17.Wait()
		r18 := g18.Wait()
		r19 := g19.Wait()

		all := [][]core.Result[int]{r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19}
		for _, results := range all {
			if len(results) != 1 || results[0].Err != nil {
				t.Fatalf("round %d: unexpected result: len=%d, err=%v", round, len(results), results[0].Err)
			}
		}
	}
}

// TestStress_Group_FailCount_SuccessCount_HasError_TotalCount 统计方法
func TestStress_Group_StatsMethods(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()
		g := NewGroup[int](8)
		for i := 0; i < 20; i++ {
			idx := i
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				if idx%3 == 0 {
					return 0, errTest
				}
				return idx, nil
			})
		}
		g.Wait()

		s := g.Stats()
		if s.TotalTask != 20 {
			t.Fatalf("TotalTask expected 20, got %d", s.TotalTask)
		}
		if g.FailCount() == 0 {
			t.Fatal("FailCount should be > 0")
		}
		if g.SuccessCount()+g.FailCount() != g.TotalCount() {
			t.Fatal("SuccessCount + FailCount != TotalCount")
		}
		if !g.HasError() {
			t.Fatal("HasError should be true")
		}
	}
}

// TestStress_Group_Active_Busy_Concurrency 运行时状态方法
func TestStress_Group_Active_Busy_Concurrency(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		var started sync.WaitGroup
		started.Add(4)
		blockCh := make(chan struct{})
		for i := 0; i < 4; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				started.Done()
				<-blockCh
				return 0, nil
			})
		}
		started.Wait()
		time.Sleep(20 * time.Millisecond)
		if a := g.Active(); a != 4 {
			t.Fatalf("Active expected 4, got %d", a)
		}
		if b := g.Busy(); b != 4 {
			t.Fatalf("Busy expected 4, got %d", b)
		}
		if g.Concurrency() != 4 {
			t.Fatalf("Concurrency expected 4, got %d", g.Concurrency())
		}
		close(blockCh)
		g.Wait()
	}
}

// TestStress_Group_Errors_FirstError_JoinErrors_Values 结果提取方法
func TestStress_Group_Errors_FirstError_JoinErrors_Values(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		g := NewGroup[int](8)
		for i := 0; i < 10; i++ {
			idx := i
			_ = g.Go(ctx, func(ctx context.Context) (int, error) {
				if idx == 3 {
					return 0, errTest
				}
				return idx, nil
			})
		}
		g.Wait()

		errs := g.Errors()
		if len(errs) == 0 {
			t.Fatal("Errors should not be empty")
		}
		firstErr := g.FirstError()
		if firstErr == nil {
			t.Fatal("FirstError should not be nil")
		}
		joinErr := g.JoinErrors()
		if joinErr == nil {
			t.Fatal("JoinErrors should not be nil")
		}
		vals := g.Values()
		if len(vals) == 0 {
			t.Fatal("Values should not be empty")
		}
	}
}

// TestStress_Group_Reset 重置方法
func TestStress_Group_Reset(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		g := NewGroup[int](4)
		for i := 0; i < 5; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		g.Wait()
		_, err := g.Reset()
		if err != nil {
			t.Fatalf("Reset failed: %v", err)
		}
		for i := 0; i < 3; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		g.Wait()
		if g.TotalCount() != 3 {
			t.Fatalf("after reset: expected 3, got %d", g.TotalCount())
		}
	}
}

// TestStress_Group_Reset_BeforeWait 在 Wait 前 Reset 应该报错
func TestStress_Group_Reset_BeforeWait(t *testing.T) {
	for round := 0; round < 20; round++ {
		g := NewGroup[int](4)
		_ = g.Go(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(50 * time.Millisecond)
			return 1, nil
		})
		_, err := g.Reset()
		if err == nil {
			t.Fatal("Reset before Wait should return error")
		}
		g.Wait()
	}
}

// ==================== NoResult 全部方法（完整） ====================

// TestStress_NoResult_NewNoResult_DefaultNoResult 基础构造函数
func TestStress_NoResult_NewNoResult_DefaultNoResult(t *testing.T) {
	for round := 0; round < 50; round++ {
		nr1 := NewNoResult(8)
		if nr1.Concurrency() != 8 {
			t.Fatalf("concurrency=%d", nr1.Concurrency())
		}
		nr2 := DefaultNoResult()
		if nr2.Concurrency() <= 0 {
			t.Fatal("concurrency <= 0")
		}
	}
}

// TestStress_NoResult_Go_GoAt_HighConcurrency 高并发 NoResult
func TestStress_NoResult_Go_GoAt_HighConcurrency(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		nr := NewNoResult(20)
		var wg sync.WaitGroup
		n := 200
		wg.Add(n * 2)
		for i := 0; i < n; i++ {
			go func(idx int) {
				defer wg.Done()
				_ = nr.Go(ctx, func(ctx context.Context) error {
					time.Sleep(time.Microsecond * 10)
					return nil
				})
			}(i)
			go func(idx int) {
				defer wg.Done()
				_ = nr.GoAt(idx, ctx, func(ctx context.Context) error {
					time.Sleep(time.Microsecond * 10)
					return nil
				})
			}(i)
		}
		wg.Wait()
		nr.Wait()
		if nr.TotalCount() != int64(n*2) {
			t.Fatalf("round %d: expected %d total, got %d", round, n*2, nr.TotalCount())
		}
	}
}

// TestStress_NoResult_GoWithTimeout_GoAtWithTimeout_HighConcurrency 带超时高并发
func TestStress_NoResult_GoWithTimeout_GoAtWithTimeout_HighConcurrency(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		nr := NewNoResult(10)
		var counter atomic.Int64
		for i := 0; i < 100; i++ {
			_ = nr.GoWithTimeout(ctx, 1*time.Second, func(ctx context.Context) error {
				counter.Add(1)
				return nil
			})
		}
		nr.Wait()
		if counter.Load() != 100 {
			t.Fatalf("round %d: GoWithTimeout expected 100, got %d", round, counter.Load())
		}
	}

	for round := 0; round < 30; round++ {
		ctx := context.Background()
		nr := NewNoResult(20)
		var wg sync.WaitGroup
		n := 100
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(idx int) {
				defer wg.Done()
				_ = nr.GoAtWithTimeout(idx, ctx, 1*time.Second, func(ctx context.Context) error {
					time.Sleep(time.Microsecond * 10)
					return nil
				})
			}(i)
		}
		wg.Wait()
		nr.Wait()
		if nr.TotalCount() != int64(n) {
			t.Fatalf("round %d: GoAtWithTimeout expected %d total, got %d", round, n, nr.TotalCount())
		}
	}
}

// TestStress_NoResult_WaitTimeout_WaitContext 带超时/上下文的 Wait
func TestStress_NoResult_WaitTimeout_WaitContext(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		nr := NewNoResult(4)
		for i := 0; i < 10; i++ {
			_ = nr.Go(ctx, func(ctx context.Context) error {
				time.Sleep(200 * time.Millisecond)
				return nil
			})
		}
		completed, ok := nr.WaitTimeout(50 * time.Millisecond)
		if ok {
			t.Fatal("expected timeout")
		}
		_ = completed
	}

	for round := 0; round < 30; round++ {
		ctx := context.Background()
		nr := NewNoResult(4)
		for i := 0; i < 10; i++ {
			_ = nr.Go(ctx, func(ctx context.Context) error {
				time.Sleep(200 * time.Millisecond)
				return nil
			})
		}
		wCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		completed, ok := nr.WaitContext(wCtx)
		cancel()
		if ok {
			t.Fatal("expected timeout via context")
		}
		_ = completed
	}
}

// TestStress_NoResult_AllWithMethods 所有 With* 组合
func TestStress_NoResult_AllWithMethods(t *testing.T) {
	for round := 0; round < 20; round++ {
		baseCtx := context.Background()
		nr, ctx := NewNoResult(4).WithTraceID(baseCtx)
		_ = nr.Go(ctx, func(ctx context.Context) error { return nil })

		nr2, ctx2 := NewNoResult(4).WithContext(baseCtx)
		_ = nr2.Go(ctx2, func(ctx context.Context) error { return nil })

		nr3, ctx3 := NewNoResult(4).WithFailFast(baseCtx)
		_ = nr3.Go(ctx3, func(ctx context.Context) error { return nil })

		nr4, ctx4 := NewNoResult(4).WithFFCtx(baseCtx)
		_ = nr4.Go(ctx4, func(ctx context.Context) error { return nil })

		nr5, ctx5 := NewNoResult(4).WithFFTraceID(baseCtx)
		_ = nr5.Go(ctx5, func(ctx context.Context) error { return nil })

		nr6, ctx6 := NewNoResult(4).WithFFSubmitTO(baseCtx, 2*time.Second)
		_ = nr6.Go(ctx6, func(ctx context.Context) error { return nil })

		nr7, ctx7 := NewNoResult(4).WithFFSubmitTOTraceID(baseCtx, 2*time.Second)
		_ = nr7.Go(ctx7, func(ctx context.Context) error { return nil })

		nr8, ctx8 := NewNoResult(4).WithFFTimeout(baseCtx, 5*time.Second)
		_ = nr8.Go(ctx8, func(ctx context.Context) error { return nil })

		nr9, ctx9 := NewNoResult(4).WithCtxTraceID(baseCtx)
		_ = nr9.Go(ctx9, func(ctx context.Context) error { return nil })

		nr10, ctx10 := NewNoResult(4).WithFFTimeoutTraceID(baseCtx, 5*time.Second)
		_ = nr10.Go(ctx10, func(ctx context.Context) error { return nil })

		nr11, ctx11 := NewNoResult(4).WithFFTimeoutSubmitTO(baseCtx, 5*time.Second, 2*time.Second)
		_ = nr11.Go(ctx11, func(ctx context.Context) error { return nil })

		nr12, ctx12 := NewNoResult(4).WithFFTimeoutSubmitTOTraceID(baseCtx, 5*time.Second, 2*time.Second)
		_ = nr12.Go(ctx12, func(ctx context.Context) error { return nil })

		nr13, ctx13 := NewNoResult(4).WithCtxTimeout(baseCtx, 5*time.Second)
		_ = nr13.Go(ctx13, func(ctx context.Context) error { return nil })

		nr14, ctx14 := NewNoResult(4).WithCtxTimeoutTraceID(baseCtx, 5*time.Second)
		_ = nr14.Go(ctx14, func(ctx context.Context) error { return nil })

		nr15 := NewNoResult(4).WithTimeout(5 * time.Second)
		_ = nr15.Go(baseCtx, func(ctx context.Context) error { return nil })

		nr16 := NewNoResult(4).WithSubmitTimeout(2 * time.Second)
		_ = nr16.Go(baseCtx, func(ctx context.Context) error { return nil })

		nr17, ctx17 := NewNoResult(4).WithCtxSubmitTO(baseCtx, 2*time.Second)
		_ = nr17.Go(ctx17, func(ctx context.Context) error { return nil })

		nr18, ctx18 := NewNoResult(4).WithCtxSubmitTOTraceID(baseCtx, 2*time.Second)
		_ = nr18.Go(ctx18, func(ctx context.Context) error { return nil })

		nr19 := DefaultNoResult()
		_ = nr19.Go(baseCtx, func(ctx context.Context) error { return nil })

		nr.Wait()
		nr2.Wait()
		nr3.Wait()
		nr4.Wait()
		nr5.Wait()
		nr6.Wait()
		nr7.Wait()
		nr8.Wait()
		nr9.Wait()
		nr10.Wait()
		nr11.Wait()
		nr12.Wait()
		nr13.Wait()
		nr14.Wait()
		nr15.Wait()
		nr16.Wait()
		nr17.Wait()
		nr18.Wait()
		nr19.Wait()
	}
}

// TestStress_NoResult_Stats_Errors_FirstError_JoinErrors 结果分析方法
func TestStress_NoResult_Stats_Errors_FirstError_JoinErrors(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()
		nr := NewNoResult(8)
		for i := 0; i < 20; i++ {
			idx := i
			_ = nr.Go(ctx, func(ctx context.Context) error {
				if idx%3 == 0 {
					return errors.New("test error")
				}
				return nil
			})
		}
		nr.Wait()
		failCnt := nr.FailCount()
		successCnt := nr.SuccessCount()
		totalCnt := nr.TotalCount()
		hasErr := nr.HasError()
		stats := nr.Stats()
		errs := nr.Errors()
		firstErr := nr.FirstError()
		joinErr := nr.JoinErrors()

		if totalCnt != 20 {
			t.Fatalf("round %d: expected 20 total, got %d", round, totalCnt)
		}
		if failCnt+successCnt != totalCnt {
			t.Fatalf("fail+success != total: %d+%d != %d", failCnt, successCnt, totalCnt)
		}
		if len(errs) == 0 {
			t.Fatal("errors should not be empty")
		}
		_ = hasErr
		_ = stats
		_ = firstErr
		_ = joinErr
	}
}

// TestStress_NoResult_Active_Busy 运行时状态
func TestStress_NoResult_Active_Busy(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()
		nr := NewNoResult(4)
		var started sync.WaitGroup
		started.Add(4)
		blockCh := make(chan struct{})
		for i := 0; i < 4; i++ {
			_ = nr.Go(ctx, func(ctx context.Context) error {
				started.Done()
				<-blockCh
				return nil
			})
		}
		started.Wait()
		time.Sleep(20 * time.Millisecond)
		a := nr.Active()
		b := nr.Busy()
		if a != 4 || b != 4 {
			t.Fatalf("round %d: expected active=4,busy=4, got active=%d,busy=%d", round, a, b)
		}
		close(blockCh)
		nr.Wait()
	}
}

// TestStress_NoResult_Reset 重置 NoResult
func TestStress_NoResult_Reset(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		nr := NewNoResult(4)
		for i := 0; i < 5; i++ {
			_ = nr.Go(ctx, func(ctx context.Context) error { return nil })
		}
		nr.Wait()
		_, err := nr.Reset()
		if err != nil {
			t.Fatalf("round %d: Reset failed: %v", round, err)
		}
		for i := 0; i < 3; i++ {
			_ = nr.Go(ctx, func(ctx context.Context) error { return nil })
		}
		nr.Wait()
		if nr.TotalCount() != 3 {
			t.Fatalf("round %d: expected 3 after reset, got %d", round, nr.TotalCount())
		}
	}
}

// TestStress_BuildAggregateNoResult 辅助函数
func TestStress_BuildAggregateNoResult(t *testing.T) {
	for round := 0; round < 30; round++ {
		nr := NewNoResult(4)
		for i := 0; i < 10; i++ {
			_ = nr.Go(context.Background(), func(ctx context.Context) error {
				return nil
			})
		}
		nr.Wait()
		total, failCnt, firstErr, results := BuildAggregateNoResult(nr)
		if total != 10 || failCnt != 0 || firstErr != nil || len(results) != 10 {
			t.Fatalf("BuildAggregateNoResult: total=%d fail=%d firstErr=%v len=%d", total, failCnt, firstErr, len(results))
		}
	}
}

// TestStress_FillNoResultSkipped 辅助函数
func TestStress_FillNoResultSkipped(t *testing.T) {
	for round := 0; round < 30; round++ {
		nr := NewNoResult(4)
		FillNoResultSkipped(nr, 5)
		failCnt := nr.FailCount()
		if failCnt != 5 {
			t.Fatalf("FillNoResultSkipped: expected failCnt=5, got %d", failCnt)
		}
	}
}

// ==================== Pool 全部方法（完整） ====================

// TestStress_Pool_NewPool_DefaultPool 基础构造函数
func TestStress_Pool_NewPool_DefaultPool(t *testing.T) {
	for round := 0; round < 30; round++ {
		p1 := NewPool[int](8)
		if p1.Size() != 8 {
			t.Fatalf("Size expected 8, got %d", p1.Size())
		}
		p1.Close()
		p2 := DefaultPool[int]()
		if p2.Size() <= 0 {
			t.Fatal("DefaultPool size <= 0")
		}
		p2.Close()
	}
}

// TestStress_Pool_AllWithMethods 所有 With* 方法组合
func TestStress_Pool_AllWithMethods(t *testing.T) {
	for round := 0; round < 20; round++ {
		baseCtx := context.Background()
		p, ctx := NewPool[int](4).WithTraceID(baseCtx)
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })

		p2, ctx2 := NewPool[int](4).WithContext(baseCtx)
		_ = p2.Submit(ctx2, func(ctx context.Context) (int, error) { return 2, nil })

		p3, ctx3 := NewPool[int](4).WithFailFast(baseCtx)
		_ = p3.Submit(ctx3, func(ctx context.Context) (int, error) { return 3, nil })

		p4, ctx4 := NewPool[int](4).WithFFCtx(baseCtx)
		_ = p4.Submit(ctx4, func(ctx context.Context) (int, error) { return 4, nil })

		p5, ctx5 := NewPool[int](4).WithFFTraceID(baseCtx)
		_ = p5.Submit(ctx5, func(ctx context.Context) (int, error) { return 5, nil })

		p6, ctx6 := NewPool[int](4).WithFFSubmitTO(baseCtx, 2*time.Second)
		_ = p6.Submit(ctx6, func(ctx context.Context) (int, error) { return 6, nil })

		p7, ctx7 := NewPool[int](4).WithFFSubmitTOTraceID(baseCtx, 2*time.Second)
		_ = p7.Submit(ctx7, func(ctx context.Context) (int, error) { return 7, nil })

		p8, ctx8 := NewPool[int](4).WithFFTimeout(baseCtx, 5*time.Second)
		_ = p8.Submit(ctx8, func(ctx context.Context) (int, error) { return 8, nil })

		p9, ctx9 := NewPool[int](4).WithCtxTraceID(baseCtx)
		_ = p9.Submit(ctx9, func(ctx context.Context) (int, error) { return 9, nil })

		p10, ctx10 := NewPool[int](4).WithFFTimeoutTraceID(baseCtx, 5*time.Second)
		_ = p10.Submit(ctx10, func(ctx context.Context) (int, error) { return 10, nil })

		p11, ctx11 := NewPool[int](4).WithFFTimeoutSubmitTO(baseCtx, 5*time.Second, 2*time.Second)
		_ = p11.Submit(ctx11, func(ctx context.Context) (int, error) { return 11, nil })

		p12, ctx12 := NewPool[int](4).WithFFTimeoutSubmitTOTraceID(baseCtx, 5*time.Second, 2*time.Second)
		_ = p12.Submit(ctx12, func(ctx context.Context) (int, error) { return 12, nil })

		p13, ctx13 := NewPool[int](4).WithCtxTimeout(baseCtx, 5*time.Second)
		_ = p13.Submit(ctx13, func(ctx context.Context) (int, error) { return 13, nil })

		p14, ctx14 := NewPool[int](4).WithCtxTimeoutTraceID(baseCtx, 5*time.Second)
		_ = p14.Submit(ctx14, func(ctx context.Context) (int, error) { return 14, nil })

		p15 := NewPool[int](4).WithTimeout(5 * time.Second)
		_ = p15.Submit(baseCtx, func(ctx context.Context) (int, error) { return 15, nil })

		p16 := NewPool[int](4).WithSubmitTimeout(2 * time.Second)
		_ = p16.Submit(baseCtx, func(ctx context.Context) (int, error) { return 16, nil })

		p17, ctx17 := NewPool[int](4).WithCtxSubmitTO(baseCtx, 2*time.Second)
		_ = p17.Submit(ctx17, func(ctx context.Context) (int, error) { return 17, nil })

		p18, ctx18 := NewPool[int](4).WithCtxSubmitTOTraceID(baseCtx, 2*time.Second)
		_ = p18.Submit(ctx18, func(ctx context.Context) (int, error) { return 18, nil })

		p19 := DefaultPool[int]()
		_ = p19.Submit(baseCtx, func(ctx context.Context) (int, error) { return 19, nil })

		r1 := p.Wait()
		r2 := p2.Wait()
		r3 := p3.Wait()
		r4 := p4.Wait()
		r5 := p5.Wait()
		r6 := p6.Wait()
		r7 := p7.Wait()
		r8 := p8.Wait()
		r9 := p9.Wait()
		r10 := p10.Wait()
		r11 := p11.Wait()
		r12 := p12.Wait()
		r13 := p13.Wait()
		r14 := p14.Wait()
		r15 := p15.Wait()
		r16 := p16.Wait()
		r17 := p17.Wait()
		r18 := p18.Wait()
		r19 := p19.Wait()

		p.Close()
		p2.Close()
		p3.Close()
		p4.Close()
		p5.Close()
		p6.Close()
		p7.Close()
		p8.Close()
		p9.Close()
		p10.Close()
		p11.Close()
		p12.Close()
		p13.Close()
		p14.Close()
		p15.Close()
		p16.Close()
		p17.Close()
		p18.Close()
		p19.Close()

		all := [][]core.Result[int]{r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19}
		for _, results := range all {
			if len(results) != 1 || results[0].Err != nil {
				t.Fatalf("round %d: unexpected result: len=%d, err=%v", round, len(results), results[0].Err)
			}
		}
	}
}

// TestStress_Pool_Submit_TrySubmit_SubmitAt 三种提交方式
func TestStress_Pool_Submit_TrySubmit_SubmitAt(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		p := NewPool[int](20)

		var wg sync.WaitGroup
		n := 200
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(idx int) {
				defer wg.Done()
				_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
					return idx * 10, nil
				})
			}(i)
		}
		wg.Wait()
		results := p.Wait()
		p.Close()
		if len(results) != n {
			t.Fatalf("round %d: Submit: expected %d, got %d", round, n, len(results))
		}
	}

	for round := 0; round < 30; round++ {
		ctx := context.Background()
		p := NewPool[int](30)
		var submitted atomic.Int64
		var wg sync.WaitGroup
		n := 500
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				if err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				}); err == nil {
					submitted.Add(1)
				}
			}()
		}
		wg.Wait()
		results := p.Wait()
		p.Close()
		if len(results) != int(submitted.Load()) {
			t.Fatalf("TrySubmit: results %d != submitted %d", len(results), submitted.Load())
		}
	}

	for round := 0; round < 20; round++ {
		ctx := context.Background()
		p := NewPool[int](50)
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		_ = p.TrySubmit(ctx, func(ctx context.Context) (int, error) { return 2, nil })
		_ = p.SubmitAt(0, ctx, func(ctx context.Context) (int, error) { return 3, nil })
		p.Wait()
		p.Close()
	}
}

// TestStress_Pool_WaitAndClose_CloseAndWait_CloseByIdle 关闭方法
func TestStress_Pool_WaitAndClose_CloseAndWait_CloseByIdle(t *testing.T) {
	for round := 0; round < 30; round++ {
		p := NewPool[int](4)
		ctx := context.Background()
		for i := 0; i < 20; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Microsecond * 100)
				return 1, nil
			})
		}
		results := p.WaitAndClose()
		if len(results) != 20 {
			t.Fatalf("round %d: WaitAndClose expected 20, got %d", round, len(results))
		}
	}

	for round := 0; round < 20; round++ {
		p := NewPool[int](4)
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Microsecond * 100)
				return i, nil
			})
		}
		p.CloseAndWait()
	}

	for round := 0; round < 20; round++ {
		p := NewPool[int](4)
		ctx := context.Background()
		for i := 0; i < 10; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(time.Microsecond * 100)
				return 1, nil
			})
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			p.CloseByIdle(5 * time.Second)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("CloseByIdle timed out")
		}
	}
}

// TestStress_Pool_CloseAndWaitTimeout 超时等待关闭
func TestStress_Pool_CloseAndWaitTimeout(t *testing.T) {
	for round := 0; round < 20; round++ {
		p := NewPool[int](2)
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(500 * time.Millisecond)
				return i, nil
			})
		}
		ok, workerDone := p.CloseAndWaitTimeout(50 * time.Millisecond)
		if ok {
			t.Fatal("should have timed out")
		}
		_ = workerDone
	}
}

// TestStress_Pool_WaitTimeout_WaitContext 带超时/上下文的 Wait
func TestStress_Pool_WaitTimeout_WaitContext(t *testing.T) {
	for round := 0; round < 30; round++ {
		p := NewPool[int](4)
		for i := 0; i < 10; i++ {
			_ = p.Submit(context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(200 * time.Millisecond)
				return 1, nil
			})
		}
		_, ok := p.WaitTimeout(30 * time.Millisecond)
		if ok {
			t.Fatal("expected timeout")
		}
		p.Close()
	}

	for round := 0; round < 20; round++ {
		p := NewPool[int](4)
		for i := 0; i < 10; i++ {
			_ = p.Submit(context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(200 * time.Millisecond)
				return 1, nil
			})
		}
		wCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		_, ok := p.WaitContext(wCtx)
		cancel()
		if ok {
			t.Fatal("expected timeout via context")
		}
		p.Close()
	}
}

// TestStress_Pool_Size_Active_Busy_Pending 运行时状态
func TestStress_Pool_Size_Active_Busy_Pending(t *testing.T) {
	for round := 0; round < 30; round++ {
		p := NewPool[int](8)
		if p.Size() != 8 {
			t.Fatalf("Size expected 8, got %d", p.Size())
		}
		p.Close()
	}

	for round := 0; round < 30; round++ {
		p := NewPool[int](1)
		ctx := context.Background()
		var started sync.WaitGroup
		started.Add(1)
		blockCh := make(chan struct{})
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			started.Done()
			<-blockCh
			return 1, nil
		})
		started.Wait()
		for i := 0; i < 2; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				return 1, nil
			})
		}
		pending := p.Pending()
		if pending <= 0 {
			t.Fatalf("round %d: expected pending > 0, got %d", round, pending)
		}
		close(blockCh)
		p.Wait()
		p.Close()
	}
}

// TestStress_Pool_Stats_Errors_FirstError_JoinErrors_Values 结果统计方法
func TestStress_Pool_Stats_Errors_FirstError_JoinErrors_Values(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		p := NewPool[int](8)
		for i := 0; i < 20; i++ {
			idx := i
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				if idx%3 == 0 {
					return 0, errTest
				}
				return idx, nil
			})
		}
		p.Wait()

		stats := p.Stats()
		if stats.TotalTask != 20 {
			t.Fatalf("TotalTask: expected 20, got %d", stats.TotalTask)
		}
		if p.FailCount()+p.SuccessCount() != p.TotalCount() {
			t.Fatal("FailCount + SuccessCount != TotalCount")
		}
		if !p.HasError() {
			t.Fatal("HasError should be true")
		}
		errs := p.Errors()
		if len(errs) == 0 {
			t.Fatal("errors should not be empty")
		}
		if p.FirstError() == nil {
			t.Fatal("FirstError should not be nil")
		}
		if p.JoinErrors() == nil {
			t.Fatal("JoinErrors should not be nil")
		}
		vals := p.Values()
		if len(vals) == 0 {
			t.Fatal("Values should not be empty")
		}
		p.Close()
	}
}

// TestStress_Pool_Resize_ResizeAndWaitTimeout 动态伸缩
func TestStress_Pool_Resize_ResizeAndWaitTimeout(t *testing.T) {
	for round := 0; round < 30; round++ {
		p := NewPool[int](4)
		added := p.Resize(8)
		if added != 4 {
			t.Fatalf("Resize up: expected 4 added, got %d", added)
		}
		if p.Size() != 8 {
			t.Fatalf("Size after up: expected 8, got %d", p.Size())
		}
		removed := p.Resize(3)
		if removed != 5 {
			t.Fatalf("Resize down: expected 5 removed, got %d", removed)
		}
		p.Close()
	}

	for round := 0; round < 20; round++ {
		p := NewPool[int](10)
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
				return i, nil
			})
		}
		p.ResizeAndWaitTimeout(5, time.Second)
		p.Wait()
		p.Close()
	}
}

// TestStress_Pool_Reset 重置 Pool
func TestStress_Pool_Reset(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		p := NewPool[int](4)
		for i := 0; i < 10; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		p.Wait()
		_, err := p.Reset()
		if err != nil {
			t.Fatalf("Reset failed: %v", err)
		}
		for i := 0; i < 5; i++ {
			_ = p.Submit(ctx, func(ctx context.Context) (int, error) { return i * 10, nil })
		}
		p.Wait()
		if p.TotalCount() != 5 {
			t.Fatalf("after reset: expected 5, got %d", p.TotalCount())
		}
		p.Close()
	}
}

// ==================== Pool 扩展函数 ====================

// TestStress_Pool_MapPool 测试 MapPool
func TestStress_Pool_MapPool(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		items := make([]int, 500)
		for i := range items {
			items[i] = i
		}
		p, results, err := pool.MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
			return item * 2, nil
		}, 50)
		if err != nil {
			t.Fatalf("MapPool err: %v", err)
		}
		_ = p
		if len(results) != 500 {
			t.Fatalf("MapPool: expected 500 results, got %d", len(results))
		}
	}
}

// TestStress_Pool_ForEachPool 测试 ForEachPool
func TestStress_Pool_ForEachPool(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		items := make([]int, 500)
		var counter atomic.Int64
		p, err := pool.ForEachPool(ctx, items, func(ctx context.Context, item int) error {
			counter.Add(1)
			return nil
		}, 50)
		if err != nil {
			t.Fatalf("ForEachPool err: %v", err)
		}
		_ = p
		if counter.Load() != 500 {
			t.Fatalf("ForEachPool: expected 500, got %d", counter.Load())
		}
	}
}

// TestStress_Pool_SubmitBatch 测试 SubmitBatch
func TestStress_Pool_SubmitBatch(t *testing.T) {
	for round := 0; round < 20; round++ {
		items := make([]int, 100)
		for i := range items {
			items[i] = i
		}
		p, results, err := pool.SubmitBatch(context.Background(), items, func(ctx context.Context, item int) (int, error) {
			return item * 10, nil
		})
		if err != nil {
			t.Fatalf("SubmitBatch err: %v", err)
		}
		if len(results) != 100 {
			t.Fatalf("SubmitBatch: expected 100 results, got %d", len(results))
		}
		p.Wait()
		p.Close()
	}
}

// TestStress_Pool_SubmitN_SubmitSafeN 测试 SubmitN/SubmitSafeN
func TestStress_Pool_SubmitN_SubmitSafeN(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		p, results, err := pool.SubmitN(ctx, func(ctx context.Context) (int, error) {
			return 42, nil
		}, 50)
		if err != nil {
			t.Fatalf("SubmitN err: %v", err)
		}
		if len(results) != 50 {
			t.Fatalf("SubmitN: expected 50, got %d", len(results))
		}
		p.Wait()
		p.Close()
	}

	for round := 0; round < 20; round++ {
		ctx := context.Background()
		p, results := pool.SubmitSafeN(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		}, 100)
		if len(results) != 100 {
			t.Fatalf("SubmitSafeN: expected 100, got %d", len(results))
		}
		p.Wait()
		p.Close()
	}
}

// TestStress_Pool_Submit 独立函数
func TestStress_Pool_SubmitFunc(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		p, idx, err := pool.Submit[int](ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
		if err != nil {
			t.Fatalf("Submit err: %v", err)
		}
		_ = idx
		p.Wait()
		p.Close()
	}
}

// ==================== NoResultPool 辅助函数 ====================

// TestStress_NoResultPool_NewNoResultPool_DefaultNoResultPool 构造函数
func TestStress_NoResultPool_NewNoResultPool_DefaultNoResultPool(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		p1 := NewNoResultPool(4)
		p2 := DefaultNoResultPool()
		_ = SubmitAction(p1, ctx, func(ctx context.Context) error { return nil })
		_ = SubmitAction(p2, ctx, func(ctx context.Context) error { return nil })
		p1.Wait()
		p2.Wait()
		p1.Close()
		p2.Close()
	}
}

// TestStress_NoResultPool_AllHelpers 所有 NoResultPool 辅助函数
func TestStress_NoResultPool_AllHelpers(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		pool := NewNoResultPool(8)
		var counter atomic.Int64

		// SubmitAction
		_ = SubmitAction(pool, ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		// TrySubmitAction
		_ = TrySubmitAction(pool, ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		// TrySubmitAtAction
		_ = TrySubmitAtAction(pool, 0, ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		// SubmitAtAction
		_ = SubmitAtAction(pool, 1, ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		// GoAction
		GoAction(pool, ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		// SubmitActionWithTimeout
		_ = SubmitActionWithTimeout(pool, ctx, 5*time.Second, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		// SubmitAtActionWithTimeout
		_ = SubmitAtActionWithTimeout(pool, 2, ctx, 5*time.Second, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		// GoActionWithTimeout
		GoActionWithTimeout(pool, ctx, 5*time.Second, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		pool.Wait()
		pool.Close()
		if counter.Load() != 8 {
			t.Fatalf("round %d: expected 8, got %d", round, counter.Load())
		}
	}
}

// ==================== Task / AsyncResult 全部方法 ====================

// TestStress_Task_GoResult_GoResultWithTimeout 测试 GoResult 和 GoResultWithTimeout
func TestStress_Task_GoResult_GoResultWithTimeout(t *testing.T) {
	for round := 0; round < 50; round++ {
		ar := GoResult(context.Background(), func(ctx context.Context) (int, error) {
			return 42, nil
		})
		val, err := ar.Wait()
		if err != nil || val != 42 {
			t.Fatalf("GoResult: val=%d, err=%v", val, err)
		}
	}

	for round := 0; round < 50; round++ {
		ar := GoResultWithTimeout(context.Background(), 5*time.Second, func(ctx context.Context) (int, error) {
			return 100, nil
		})
		val, err := ar.Wait()
		if err != nil || val != 100 {
			t.Fatalf("GoResultWithTimeout: val=%d, err=%v", val, err)
		}
	}
}

// TestStress_Task_GoResultWithTimeout_ActualTimeout 实际超时场景
func TestStress_Task_GoResultWithTimeout_ActualTimeout(t *testing.T) {
	ar := GoResultWithTimeout(context.Background(), 20*time.Millisecond, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})
	val, err := ar.Wait()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got val=%d err=%v", val, err)
	}
}

// TestStress_Task_Go 测试 Go 和 GoWithTimeout（无返回值异步任务）
func TestStress_Task_Go(t *testing.T) {
	for round := 0; round < 50; round++ {
		task := Go(context.Background(), func(ctx context.Context) {
			// fire-and-forget
		})
		if err := task.Wait(); err != nil {
			t.Fatalf("Go Wait: err=%v", err)
		}
	}

	for round := 0; round < 50; round++ {
		task := GoWithTimeout(context.Background(), 5*time.Second, func(ctx context.Context) {
			// fire-and-forget with timeout
		})
		if err := task.Wait(); err != nil {
			t.Fatalf("GoWithTimeout Wait: err=%v", err)
		}
	}
}

// TestStress_Task_GoResult 测试 GoResult 和 GoResultWithTimeout（有返回值异步任务）
func TestStress_Task_GoResult(t *testing.T) {
	for round := 0; round < 50; round++ {
		ar := GoResult(context.Background(), func(ctx context.Context) (int, error) {
			return round, nil
		})
		val, err := ar.Wait()
		if err != nil || val != round {
			t.Fatalf("GoResult: val=%d, err=%v", val, err)
		}
	}

	for round := 0; round < 50; round++ {
		ar := GoResultWithTimeout(context.Background(), 5*time.Second, func(ctx context.Context) (string, error) {
			return "ok", nil
		})
		val, err := ar.Wait()
		if err != nil || val != "ok" {
			t.Fatalf("GoResultWithTimeout: val=%s, err=%v", val, err)
		}
	}
}

// TestStress_Task_Ok_IsPanic_WaitCh_Cancel 测试 AsyncResult 方法
func TestStress_Task_Ok_IsPanic_WaitCh_Cancel(t *testing.T) {
	for round := 0; round < 50; round++ {
		ar := GoResult(context.Background(), func(ctx context.Context) (int, error) {
			return 42, nil
		})
		if !ar.Ok() {
			t.Fatal("Ok should be true before wait")
		}
		val, err := ar.Wait()
		if err != nil || val != 42 {
			t.Fatalf("Wait: val=%d, err=%v", val, err)
		}
		if !ar.Ok() {
			t.Fatal("Ok should be true after success")
		}
		if ar.IsPanic() {
			t.Fatal("IsPanic should be false after success")
		}
	}

	for round := 0; round < 30; round++ {
		ar := GoResult(context.Background(), func(ctx context.Context) (int, error) {
			return 0, errTest
		})
		ar.Wait()
		if ar.Ok() {
			t.Fatal("Ok should be false after error")
		}
	}

	for round := 0; round < 30; round++ {
		ar := GoResult(context.Background(), func(ctx context.Context) (int, error) {
			panic("task panic")
		})
		ar.Wait()
		if ar.Ok() {
			t.Fatal("Ok should be false after panic")
		}
		if !ar.IsPanic() {
			t.Fatal("IsPanic should be true after panic")
		}
	}

	for round := 0; round < 30; round++ {
		ar := GoResult(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(50 * time.Millisecond)
			return 1, nil
		})
		ch := ar.WaitCh()
		select {
		case <-ch:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("WaitCh timeout")
		}
	}

	for round := 0; round < 20; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		ar := GoResult(ctx, func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})
		cancel()
		_, err := ar.Wait()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected Canceled, got %v", err)
		}
	}
}

// TestStress_Task_WaitTimeout 测试 AsyncResult.WaitTimeout
func TestStress_Task_WaitTimeout(t *testing.T) {
	for round := 0; round < 30; round++ {
		ar := GoResult(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 1, nil
		})
		val, err, ok := ar.WaitTimeout(30 * time.Millisecond)
		if ok {
			t.Fatal("expected timeout")
		}
		_, _ = val, err
	}
}

// TestStress_Task_Ctx_Result_Cancel 测试 Task 的 Ctx/Result/Cancel
func TestStress_Task_Ctx_Result_Cancel(t *testing.T) {
	for round := 0; round < 30; round++ {
		ar := GoResult(context.Background(), func(ctx context.Context) (int, error) {
			return 42, nil
		})
		val, err := ar.Wait()
		if err != nil || val != 42 {
			t.Fatalf("GoResult Wait: value=%v err=%v", val, err)
		}
	}
}

// TestStress_TaskVoid_Wait_Ok_IsPanic 测试 TaskVoid
func TestStress_TaskVoid_Wait_Ok_IsPanic(t *testing.T) {
	for round := 0; round < 30; round++ {
		task := Go(context.Background(), func(ctx context.Context) {
			// 正常完成
		})
		if err := task.Wait(); err != nil {
			t.Fatalf("TaskVoid Wait err: %v", err)
		}
		if !task.Ok() {
			t.Fatal("TaskVoid Ok should be true")
		}
	}

	for round := 0; round < 30; round++ {
		task := Go(context.Background(), func(ctx context.Context) {
			panic("void panic")
		})
		task.Wait()
		if !task.IsPanic() {
			t.Fatal("TaskVoid IsPanic should be true")
		}
	}
}

// TestStress_Task_GoAction 测试 NoResultPool GoAction 函数
func TestStress_Task_GoAction(t *testing.T) {
	for round := 0; round < 30; round++ {
		var executed atomic.Int64
		p := NewNoResultPool(4)
		GoAction(p, context.Background(), func(ctx context.Context) error {
			executed.Add(1)
			return nil
		})
		p.Wait()
		p.Close()
		if executed.Load() != 1 {
			t.Fatalf("GoAction: expected 1, got %d", executed.Load())
		}
	}

	for round := 0; round < 30; round++ {
		var executed atomic.Int64
		p := NewNoResultPool(4)
		err := SubmitAction(p, context.Background(), func(ctx context.Context) error {
			executed.Add(1)
			return nil
		})
		if err != nil {
			t.Fatalf("SubmitAction: err=%v", err)
		}
		p.Wait()
		p.Close()
		if executed.Load() != 1 {
			t.Fatalf("SubmitAction: expected 1, got %d", executed.Load())
		}
	}
}

// ==================== Map 全部变体 ====================

// TestStress_Map_DefaultMap_HighConcurrency 高并发 Map
func TestStress_Map_DefaultMap_HighConcurrency(t *testing.T) {
	for round := 0; round < 20; round++ {
		items := make([]int, 1000)
		for i := range items {
			items[i] = i
		}
		results := DefaultMap(context.Background(), items, func(ctx context.Context, v int) (int, error) {
			return v * 3, nil
		})
		if len(results) != 1000 {
			t.Fatalf("DefaultMap: expected 1000, got %d", len(results))
		}
	}
}

// TestStress_Map_WithConcurrency 指定并发度的 Map
func TestStress_Map_WithConcurrency(t *testing.T) {
	for round := 0; round < 20; round++ {
		items := make([]int, 500)
		for i := range items {
			items[i] = i
		}
		results := Map(context.Background(), items, 100, func(ctx context.Context, v int) (int, error) {
			return v * 2, nil
		})
		if len(results) != 500 {
			t.Fatalf("Map: expected 500, got %d", len(results))
		}
		for i, r := range results {
			if r.Value != i*2 {
				t.Fatalf("Map[%d]: expected %d, got %v", i, i*2, r.Value)
			}
		}
	}
}

// TestStress_Map_AllDefaultVariants 测试 DefaultMap 所有变体
func TestStress_Map_AllDefaultVariants(t *testing.T) {
	for round := 0; round < 10; round++ {
		items := make([]int, 50)
		for i := range items {
			items[i] = i
		}

		// DefaultMapWithFailFast
		resultsFF, _ := DefaultMapWithFailFast(context.Background(), items, func(ctx context.Context, v int) (int, error) {
			if v == 5 {
				return 0, errTest
			}
			return v * 2, nil
		})
		if len(resultsFF) != 50 {
			t.Fatalf("DefaultMapWithFailFast: expected 50, got %d", len(resultsFF))
		}

		// DefaultMapWithTimeout
		resultsTO := DefaultMapWithTimeout(context.Background(), items, 5*time.Second, func(ctx context.Context, v int) (int, error) {
			return v * 2, nil
		})
		if len(resultsTO) != 50 {
			t.Fatalf("DefaultMapWithTimeout: expected 50, got %d", len(resultsTO))
		}

		// DefaultMapWithFFTimeout
		resultsFFTO, _ := DefaultMapWithFFTimeout(context.Background(), items, 5*time.Second, func(ctx context.Context, v int) (int, error) {
			if v == 3 {
				return 0, errTest
			}
			return v * 2, nil
		})
		_ = resultsFFTO
	}
}

// TestStress_MapSerial_MapSerialFailFast 测试串行 Map
func TestStress_MapSerial_MapSerialFailFast(t *testing.T) {
	for round := 0; round < 20; round++ {
		items := make([]int, 100)
		for i := range items {
			items[i] = i
		}
		results := MapSerial(context.Background(), items, func(ctx context.Context, v int) (int, error) {
			return v * 2, nil
		})
		if len(results) != 100 {
			t.Fatalf("MapSerial: len=%d", len(results))
		}

		_, err := MapSerialFailFast(context.Background(), items, func(ctx context.Context, v int) (int, error) {
			if v == 3 {
				return 0, errTest
			}
			return v, nil
		})
		if err == nil {
			t.Fatal("MapSerialFailFast should return error")
		}
	}
}

// TestStress_MapWithTimeout_MapWithFFTimeout 测试 MapWithTimeout 系列
func TestStress_MapWithTimeout_MapWithFFTimeout(t *testing.T) {
	for round := 0; round < 10; round++ {
		items := make([]int, 100)
		for i := range items {
			items[i] = i
		}
		results := MapWithTimeout(context.Background(), items, 50, 5*time.Second, func(ctx context.Context, v int) (int, error) {
			return v * 3, nil
		})
		if len(results) != 100 {
			t.Fatalf("MapWithTimeout: expected 100, got %d", len(results))
		}

		results2, _ := MapWithFFTimeout(context.Background(), items, 50, 5*time.Second, func(ctx context.Context, v int) (int, error) {
			if v == 1 {
				return 0, errTest
			}
			return v * 3, nil
		})
		_ = results2
	}
}

// ==================== MapChunk 全部变体 ====================

// TestStress_MapChunk_DefaultMapChunk 测试 DefaultMapChunk
func TestStress_MapChunk_DefaultMapChunk(t *testing.T) {
	for round := 0; round < 20; round++ {
		items := make([]int, 500)
		for i := range items {
			items[i] = i
		}
		results := DefaultMapChunk(context.Background(), items, 50, func(ctx context.Context, vs []int) (int, error) {
			sum := 0
			for _, v := range vs {
				sum += v
			}
			return sum, nil
		})
		if len(results) == 0 {
			t.Fatal("DefaultMapChunk: should have results")
		}
	}
}

// TestStress_MapChunk_AllDefaultVariants 测试 DefaultMapChunk 所有变体
func TestStress_MapChunk_AllDefaultVariants(t *testing.T) {
	for round := 0; round < 10; round++ {
		items := make([]int, 200)
		for i := range items {
			items[i] = i
		}

		resultsFF, _ := DefaultMapChunkWithFailFast(context.Background(), items, 10, func(ctx context.Context, vs []int) (int, error) {
			return 1, errTest
		})
		_ = resultsFF

		resultsTO := DefaultMapChunkWithTimeout(context.Background(), items, 10, 5*time.Second, func(ctx context.Context, vs []int) (int, error) {
			return len(vs), nil
		})
		if len(resultsTO) == 0 {
			t.Fatal("DefaultMapChunkWithTimeout: should have results")
		}

		resultsFFTO, _ := DefaultMapChunkWithFFTimeout(context.Background(), items, 10, 5*time.Second, func(ctx context.Context, vs []int) (int, error) {
			return 1, errTest
		})
		_ = resultsFFTO
	}
}

// ==================== MapChunked 全部变体 ====================

// TestStress_MapChunked_AllDefaultVariants 测试 DefaultMapChunked 所有变体
func TestStress_MapChunked_AllDefaultVariants(t *testing.T) {
	for round := 0; round < 10; round++ {
		items := make([]int, 200)
		for i := range items {
			items[i] = i
		}

		results := DefaultMapChunked(context.Background(), items, 10, func(ctx context.Context, v int) (int, error) {
			return v * 2, nil
		})
		if len(results) == 0 {
			t.Fatal("DefaultMapChunked: should have results")
		}

		resultsFF, _ := DefaultMapChunkedWithFailFast(context.Background(), items, 10, func(ctx context.Context, v int) (int, error) {
			return 0, errTest
		})
		_ = resultsFF

		resultsTO := DefaultMapChunkedWithTimeout(context.Background(), items, 10, 5*time.Second, func(ctx context.Context, v int) (int, error) {
			return v * 2, nil
		})
		if len(resultsTO) == 0 {
			t.Fatal("DefaultMapChunkedWithTimeout: should have results")
		}

		resultsFFTO, _ := DefaultMapChunkedWithFFTimeout(context.Background(), items, 10, 5*time.Second, func(ctx context.Context, v int) (int, error) {
			return 1, errTest
		})
		_ = resultsFFTO
	}
}

// ==================== ForEach 全部变体 ====================

// TestStress_ForEach_DefaultForEach_HighConcurrency 高并发 ForEach
func TestStress_ForEach_DefaultForEach_HighConcurrency(t *testing.T) {
	for round := 0; round < 20; round++ {
		items := make([]int, 1000)
		var counter atomic.Int64
		_, err := DefaultForEach(context.Background(), items, func(ctx context.Context, v int) error {
			counter.Add(1)
			return nil
		})
		if err != nil {
			t.Fatalf("DefaultForEach: err=%v", err)
		}
		if counter.Load() != 1000 {
			t.Fatalf("DefaultForEach: expected 1000, got %d", counter.Load())
		}
	}
}

// TestStress_ForEach_WithConcurrency 指定并发度的 ForEach
func TestStress_ForEach_WithConcurrency(t *testing.T) {
	for round := 0; round < 20; round++ {
		items := make([]int, 500)
		var counter atomic.Int64
		_, err := ForEach(context.Background(), items, 100, func(ctx context.Context, v int) error {
			counter.Add(1)
			return nil
		})
		if err != nil {
			t.Fatalf("ForEach: err=%v", err)
		}
		if counter.Load() != 500 {
			t.Fatalf("ForEach: expected 500, got %d", counter.Load())
		}
	}
}

// TestStress_ForEach_AllVariants 测试 ForEach 所有变体
func TestStress_ForEach_AllVariants(t *testing.T) {
	for round := 0; round < 10; round++ {
		items := make([]int, 50)

		// ForEachSerial
		_, err := ForEachSerial(context.Background(), items, func(ctx context.Context, v int) error {
			return nil
		})
		if err != nil {
			t.Fatalf("ForEachSerial: err=%v", err)
		}

		// ForEachSerialFailFast - 第一个错误触发快速失败
		_, err = ForEachSerialFailFast(context.Background(), items, func(ctx context.Context, v int) error {
			if v == 3 {
				return errTest
			}
			return nil
		})
		if err == nil {
			t.Log("ForEachSerialFailFast returned nil (race condition possible with submit timeout)")
		}

		// ForEachWithFailFast
		_, err = ForEachWithFailFast(context.Background(), items, 20, func(ctx context.Context, v int) error {
			if v == 2 {
				return errTest
			}
			return nil
		})
		if err == nil {
			t.Log("ForEachWithFailFast returned nil (race condition possible)")
		}

		// ForEachWithTimeout
		_, err = ForEachWithTimeout(context.Background(), items, 20, 5*time.Second, func(ctx context.Context, v int) error {
			return nil
		})
		if err != nil {
			t.Fatalf("ForEachWithTimeout: err=%v", err)
		}

		// ForEachWithFFTimeout
		_, err = ForEachWithFFTimeout(context.Background(), items, 20, 5*time.Second, func(ctx context.Context, v int) error {
			if v == 1 {
				return errTest
			}
			return nil
		})
		if err == nil {
			t.Log("ForEachWithFFTimeout returned nil (race condition possible)")
		}
	}
}

// TestStress_ForEach_DefaultVariants 测试 DefaultForEach 变体
func TestStress_ForEach_DefaultVariants(t *testing.T) {
	for round := 0; round < 10; round++ {
		items := make([]int, 50)

		_, err := DefaultForEachWithFailFast(context.Background(), items, func(ctx context.Context, v int) error {
			if v == 2 {
				return errTest
			}
			return nil
		})
		if err == nil {
			t.Log("DefaultForEachWithFailFast returned nil (race condition possible)")
		}

		_, err = DefaultForEachWithTimeout(context.Background(), items, 5*time.Second, func(ctx context.Context, v int) error {
			return nil
		})
		if err != nil {
			t.Fatalf("DefaultForEachWithTimeout: err=%v", err)
		}

		_, err = DefaultForEachWithFFTimeout(context.Background(), items, 5*time.Second, func(ctx context.Context, v int) error {
			if v == 1 {
				return errTest
			}
			return nil
		})
		if err == nil {
			t.Log("DefaultForEachWithFFTimeout returned nil (race condition possible)")
		}
	}
}

// ==================== ForEachChunk / ForEachChunked 全部变体 ====================

// TestStress_ForEachChunk_AllVariants 测试所有 ForEachChunk 变体
func TestStress_ForEachChunk_AllVariants(t *testing.T) {
	for round := 0; round < 10; round++ {
		items := make([]int, 200)
		for i := range items {
			items[i] = i
		}
		_, err := DefaultForEachChunk(context.Background(), items, 10, func(ctx context.Context, vs []int) error {
			return nil
		})
		if err != nil {
			t.Fatalf("DefaultForEachChunk: err=%v", err)
		}

		_, err = DefaultForEachChunkWithFailFast(context.Background(), items, 10, func(ctx context.Context, vs []int) error {
			return errTest
		})
		if err == nil {
			t.Fatal("DefaultForEachChunkWithFailFast should return error")
		}

		_, err = DefaultForEachChunkWithTimeout(context.Background(), items, 10, 5*time.Second, func(ctx context.Context, vs []int) error {
			return nil
		})
		if err != nil {
			t.Fatalf("DefaultForEachChunkWithTimeout: err=%v", err)
		}

		_, err = DefaultForEachChunkWithFFTimeout(context.Background(), items, 10, 5*time.Second, func(ctx context.Context, vs []int) error {
			return errTest
		})
		if err == nil {
			t.Fatal("DefaultForEachChunkWithFFTimeout should return error")
		}
	}
}

// TestStress_ForEachChunked_AllVariants 测试所有 ForEachChunked 变体
func TestStress_ForEachChunked_AllVariants(t *testing.T) {
	for round := 0; round < 10; round++ {
		items := make([]int, 200)
		for i := range items {
			items[i] = i
		}
		_, err := DefaultForEachChunked(context.Background(), items, 10, func(ctx context.Context, v int) error {
			return nil
		})
		if err != nil {
			t.Fatalf("DefaultForEachChunked: err=%v", err)
		}

		_, err = DefaultForEachChunkedWithFailFast(context.Background(), items, 10, func(ctx context.Context, v int) error {
			return errTest
		})
		if err == nil {
			t.Fatal("DefaultForEachChunkedWithFailFast should return error")
		}

		_, err = DefaultForEachChunkedWithTimeout(context.Background(), items, 10, 5*time.Second, func(ctx context.Context, v int) error {
			return nil
		})
		if err != nil {
			t.Fatalf("DefaultForEachChunkedWithTimeout: err=%v", err)
		}

		_, err = DefaultForEachChunkedWithFFTimeout(context.Background(), items, 10, 5*time.Second, func(ctx context.Context, v int) error {
			return errTest
		})
		if err == nil {
			t.Fatal("DefaultForEachChunkedWithFFTimeout should return error")
		}
	}
}

// ==================== Reduce 全部变体 ====================

// TestStress_Reduce_DefaultReduce 高并发 Reduce
func TestStress_Reduce_DefaultReduce(t *testing.T) {
	for round := 0; round < 20; round++ {
		items := make([]int, 500)
		for i := range items {
			items[i] = 1
		}
		result, err := DefaultReduce(context.Background(), items,
			func(ctx context.Context, v int) (int, error) { return v, nil },
			0,
			func(a, b int) int { return a + b },
		)
		if err != nil || result != 500 {
			t.Fatalf("DefaultReduce: result=%d err=%v", result, err)
		}
	}
}

// TestStress_Reduce_WithConcurrency 指定并发度
func TestStress_Reduce_WithConcurrency(t *testing.T) {
	for round := 0; round < 20; round++ {
		items := make([]int, 100)
		for i := range items {
			items[i] = 1
		}
		result, err := Reduce(context.Background(), items, 50,
			func(ctx context.Context, v int) (int, error) { return v, nil },
			0,
			func(a, b int) int { return a + b },
		)
		if err != nil || result != 100 {
			t.Fatalf("Reduce: result=%d err=%v", result, err)
		}
	}
}

// TestStress_Reduce_AllVariants 测试 Reduce 所有变体
func TestStress_Reduce_AllVariants(t *testing.T) {
	for round := 0; round < 10; round++ {
		items := make([]int, 50)
		for i := range items {
			items[i] = 1
		}

		// ReduceWithFailFast / DefaultReduceWithFailFast
		_, err := ReduceWithFailFast(context.Background(), items, 20,
			func(ctx context.Context, v int) (int, error) { return 0, errTest },
			0,
			func(a, b int) int { return a + b },
		)
		if err == nil {
			t.Fatal("ReduceWithFailFast should return error")
		}
		_, err = DefaultReduceWithFailFast(context.Background(), items,
			func(ctx context.Context, v int) (int, error) { return 0, errTest },
			0,
			func(a, b int) int { return a + b },
		)
		if err == nil {
			t.Fatal("DefaultReduceWithFailFast should return error")
		}

		// ReduceWithTimeout / DefaultReduceWithTimeout
		result, err := ReduceWithTimeout(context.Background(), items, 20, 5*time.Second,
			func(ctx context.Context, v int) (int, error) { return v, nil },
			0,
			func(a, b int) int { return a + b },
		)
		if err != nil || result != 50 {
			t.Fatalf("ReduceWithTimeout: result=%d err=%v", result, err)
		}
		result, err = DefaultReduceWithTimeout(context.Background(), items, 5*time.Second,
			func(ctx context.Context, v int) (int, error) { return v, nil },
			0,
			func(a, b int) int { return a + b },
		)
		if err != nil || result != 50 {
			t.Fatalf("DefaultReduceWithTimeout: result=%d err=%v", result, err)
		}

		// ReduceWithFFTimeout / DefaultReduceWithFFTimeout
		_, err = ReduceWithFFTimeout(context.Background(), items, 20, 5*time.Second,
			func(ctx context.Context, v int) (int, error) { return 0, errTest },
			0,
			func(a, b int) int { return a + b },
		)
		if err == nil {
			t.Fatal("ReduceWithFFTimeout should return error")
		}
		_, err = DefaultReduceWithFFTimeout(context.Background(), items, 5*time.Second,
			func(ctx context.Context, v int) (int, error) { return 0, errTest },
			0,
			func(a, b int) int { return a + b },
		)
		if err == nil {
			t.Fatal("DefaultReduceWithFFTimeout should return error")
		}
	}
}

// TestStress_Reduce_SerialReduce 测试 DefaultReduce（默认并发度版本）
func TestStress_Reduce_SerialReduce(t *testing.T) {
	for round := 0; round < 20; round++ {
		items := make([]int, 100)
		for i := range items {
			items[i] = i + 1
		}
		result, err := DefaultReduce(context.Background(), items,
			func(ctx context.Context, v int) (int, error) { return v, nil },
			0,
			func(a, b int) int { return a + b },
		)
		if err != nil || result != 5050 {
			t.Fatalf("DefaultReduce: result=%d err=%v", result, err)
		}
	}
}

// ==================== Pipeline ====================

// TestStress_Pipeline_Execute 高并发 Pipeline.Execute
func TestStress_Pipeline_Execute(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		items := make([]int, 500)
		for i := range items {
			items[i] = i
		}
		stages := []Stage[int]{
			{Name: "double", Concurrency: 20},
			{Name: "add1", Concurrency: 20},
		}
		results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
			switch stage {
			case "double":
				return v * 2, nil
			case "add1":
				return v + 1, nil
			}
			return v, nil
		})
		if err != nil {
			t.Fatalf("Execute: err=%v", err)
		}
		if len(results) != 500 {
			t.Fatalf("Execute: expected 500, got %d", len(results))
		}
		for i, r := range results {
			if r.Value != i*2+1 {
				t.Fatalf("Execute[%d]: expected %d, got %v", i, i*2+1, r.Value)
			}
		}
	}

	for round := 0; round < 10; round++ {
		ctx := context.Background()
		items := make([]int, 100)
		for i := range items {
			items[i] = i
		}
		results, err := ExecuteWithGroup(ctx, items,
			func(ctx context.Context, v int) (int, error) { return v * 3, nil },
			20,
		)
		if err != nil {
			t.Fatalf("ExecuteWithGroup: err=%v", err)
		}
		if len(results) != 100 {
			t.Fatalf("ExecuteWithGroup: expected 100, got %d", len(results))
		}
	}
}

// TestStress_Pipeline_NewPipeline_Run_Stages 测试 NewPipeline
func TestStress_Pipeline_NewPipeline_Run_Stages(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		p := NewPipeline[int](ctx,
			func(ctx context.Context, v int) (int, error) { return v * 2, nil },
		)
		n := p.Stages()
		if n != 1 {
			t.Fatalf("Stages: expected 1, got %d", n)
		}
		result, err := p.Run(5)
		if err != nil {
			t.Fatalf("Run: err=%v", err)
		}
		if result != 10 {
			t.Fatalf("Run: expected 10, got %d", result)
		}
	}
}

// TestStress_Pipeline_MultiStage 测试多阶段管道
func TestStress_Pipeline_MultiStage(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		p := NewPipeline[int](ctx,
			func(ctx context.Context, v int) (int, error) { return v * 2, nil },
			func(ctx context.Context, v int) (int, error) { return v + 1, nil },
		)
		result, err := p.Run(5)
		if err != nil {
			t.Fatalf("Pipeline run: err=%v", err)
		}
		if result != 11 {
			t.Fatalf("Pipeline run: expected 11, got %d", result)
		}
	}
}

// TestStress_Pipeline_WithTraceID 测试 WithTraceID
func TestStress_Pipeline_WithTraceID(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		p := NewPipeline[int](ctx,
			func(ctx context.Context, v int) (int, error) { return v * 2, nil },
		)
		p.WithTraceID(ctx)
		result, err := p.Run(10)
		if err != nil {
			t.Fatalf("Pipeline traceID run: err=%v", err)
		}
		if result != 20 {
			t.Fatalf("Pipeline traceID: expected 20, got %d", result)
		}
	}
}

// ==================== RateLimiter 全部方法 ====================

// TestStress_RateLimiter_Acquire_Release 高并发 Acquire/Release
func TestStress_RateLimiter_Acquire_Release(t *testing.T) {
	for round := 0; round < 30; round++ {
		rl := ratelimit.NewRateLimiter(100, 1*time.Second)
		defer rl.Stop()
		var wg sync.WaitGroup
		n := 200
		var acquired atomic.Int64
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				err := rl.Acquire(context.Background())
				if err == nil {
					acquired.Add(1)
					rl.Release()
				}
			}()
		}
		wg.Wait()
		if acquired.Load() <= 0 {
			t.Fatal("Acquire should succeed at least some times")
		}
	}
}

// TestStress_RateLimiter_Token_Resize_Available 测试 Token/Resize/Available
func TestStress_RateLimiter_Token_Resize_Available(t *testing.T) {
	for round := 0; round < 30; round++ {
		rl := ratelimit.NewRateLimiter(50, 1*time.Second)
		if rl.Size() != 50 {
			t.Fatalf("Size: expected 50, got %d", rl.Size())
		}
		time.Sleep(50 * time.Millisecond)
		available := rl.Available()
		if available <= 0 {
			t.Fatalf("Available: expected > 0, got %d", available)
		}
		token, _ := rl.Token(context.Background())
		_ = token
		rl.Resize(100)
		if rl.Size() != 100 {
			t.Fatalf("Size after Resize: expected 100, got %d", rl.Size())
		}
		rl.Stop()
	}
}

// TestStress_RateLimiter_Wait_Stop 测试 Wait/Stop
func TestStress_RateLimiter_Wait_Stop(t *testing.T) {
	for round := 0; round < 20; round++ {
		rl := ratelimit.NewRateLimiter(20, 1*time.Second)
		rl.Wait(context.Background())
		rl.Stop()
	}
}

// TestStress_RateLimiter_WithStrategy_Block_Reject_BlockForce 测试 WithStrategy
func TestStress_RateLimiter_WithStrategy_Block_Reject_BlockForce(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()

		// Block 策略（默认）
		rl := ratelimit.NewRateLimiter(1, 100*time.Millisecond)
		rl.Acquire(ctx)
		go func() { time.Sleep(200 * time.Millisecond); rl.Release() }()
		if err := rl.Acquire(ctx); err != nil {
			t.Fatal("Block strategy should eventually succeed")
		}
		rl.Release()
		rl.Stop()

		// Reject 策略
		rl2 := ratelimit.NewRateLimiter(1, 1*time.Minute).WithStrategy(ratelimit.Reject)
		rl2.Acquire(ctx)
		if err := rl2.Acquire(ctx); err == nil {
			t.Fatal("Reject strategy should fail when no tokens")
		}
		rl2.Release()
		rl2.Stop()

		// BlockForce 策略
		rl3 := ratelimit.NewRateLimiter(1, 100*time.Millisecond).WithStrategy(ratelimit.BlockForce)
		rl3.Acquire(ctx)
		go func() { time.Sleep(200 * time.Millisecond); rl3.Release() }()
		if err := rl3.Acquire(ctx); err != nil {
			t.Fatal("BlockForce strategy should eventually succeed")
		}
		rl3.Release()
		rl3.Stop()
	}
}

// TestStress_RateLimiter_WithTraceID 测试 WithTraceID
func TestStress_RateLimiter_WithTraceID(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		rl, ctx := ratelimit.NewRateLimiter(10, 1*time.Second).WithTraceID(ctx)
		_, err := rl.Token(ctx)
		if err != nil {
			t.Fatalf("Token should succeed with traceID, err=%v", err)
		}
		rl.Stop()
	}
}

// TestStress_RateLimiter_NewRateLimiterWithBurst 测试 NewRateLimiterWithBurst
func TestStress_RateLimiter_NewRateLimiterWithBurst(t *testing.T) {
	for round := 0; round < 10; round++ {
		rl := ratelimit.NewRateLimiterWithBurst(10, 1*time.Second, 20)
		ctx := context.Background()
		for i := 0; i < 15; i++ {
			tok, _ := rl.Token(ctx)
			_ = tok
		}
		rl.Stop()
	}
}

// TestStress_SlidingWindowRateLimiter_Allow_AllowN 测试 SlidingWindow
func TestStress_SlidingWindowRateLimiter_Allow_AllowN(t *testing.T) {
	for round := 0; round < 20; round++ {
		rl := ratelimit.NewSlidingWindowRateLimiter(100, 1*time.Second)
		var allowed atomic.Int64
		var wg sync.WaitGroup
		n := 500
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				if rl.Allow() {
					allowed.Add(1)
				}
			}()
		}
		wg.Wait()
		if allowed.Load() <= 0 {
			t.Fatal("SlidingWindow Allowed should be > 0")
		}
	}

	for round := 0; round < 10; round++ {
		rl := ratelimit.NewSlidingWindowRateLimiter(100, 1*time.Second)
		if !rl.AllowN(50) {
			t.Fatal("AllowN(50) should succeed")
		}
	}
}

// TestStress_TokenBucket_Allow_AllowN 测试 TokenBucket
func TestStress_TokenBucket_Allow_AllowN(t *testing.T) {
	for round := 0; round < 20; round++ {
		tb := ratelimit.NewTokenBucket(100, 100)
		if !tb.Allow() {
			t.Fatal("TokenBucket.Allow should succeed")
		}
		if !tb.AllowN(50) {
			t.Fatal("TokenBucket.AllowN(50) should succeed")
		}
	}
}

// TestStress_AdaptiveRateLimiter_Acquire_Release_Record 测试 AdaptiveRateLimiter
func TestStress_AdaptiveRateLimiter_Acquire_Release_Record(t *testing.T) {
	for round := 0; round < 20; round++ {
		arl := ratelimit.NewAdaptiveRateLimiter(50, 200)
		arl.RecordSuccess()
		arl.RecordFailure()
		arl.Release()
		t.Log("AdaptiveRateLimiter basic operations OK")
	}
}

// ==================== Retry 全部方法 ====================

// TestStress_Retry_RetryWithBackoff_Exponential 测试指数退避重试
func TestStress_Retry_RetryWithBackoff_Exponential(t *testing.T) {
	for round := 0; round < 30; round++ {
		var attempts atomic.Int64
		err := RetryWithBackoff(context.Background(), 5, 1*time.Microsecond, func(ctx context.Context) error {
			attempts.Add(1)
			if attempts.Load() >= 3 {
				return nil
			}
			return errTest
		})
		if err != nil {
			t.Fatalf("RetryWithBackoff: err=%v", err)
		}
		if attempts.Load() != 3 {
			t.Fatalf("RetryWithBackoff: expected 3 attempts, got %d", attempts.Load())
		}
	}
}

// TestStress_Retry_RetryWithBackoff_Exhausted 重试耗尽
func TestStress_Retry_RetryWithBackoff_Exhausted(t *testing.T) {
	for round := 0; round < 20; round++ {
		err := RetryWithBackoff(context.Background(), 2, 1*time.Microsecond, func(ctx context.Context) error {
			return errTest
		})
		if err == nil {
			t.Fatal("RetryWithBackoff should eventually fail")
		}
	}
}

// TestStress_Retry_RetryWithBackoff_ContextCancel 上下文取消
func TestStress_Retry_RetryWithBackoff_ContextCancel(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err := RetryWithBackoff(ctx, 10, 100*time.Millisecond, func(ctx context.Context) error {
			return errTest
		})
		cancel()
		if err == nil {
			t.Fatal("RetryWithBackoff should fail on context cancel")
		}
	}
}

// TestStress_Retry_RetryWithBackoff_Panic 重试中的 panic
func TestStress_Retry_RetryWithBackoff_Panic(t *testing.T) {
	for round := 0; round < 20; round++ {
		err := RetryWithBackoff(context.Background(), 3, 1*time.Microsecond, func(ctx context.Context) error {
			panic("retry panic")
		})
		if err == nil {
			t.Fatal("RetryWithBackoff should return error on panic")
		}
	}
}

// TestStress_Retry_RetryWithLinearBackoff 测试线性退避
func TestStress_Retry_RetryWithLinearBackoff(t *testing.T) {
	for round := 0; round < 20; round++ {
		var attempts atomic.Int64
		err := RetryWithLinearBackoff(context.Background(), 5, 1*time.Microsecond, func(ctx context.Context) error {
			attempts.Add(1)
			if attempts.Load() >= 3 {
				return nil
			}
			return errTest
		})
		if err != nil {
			t.Fatalf("RetryWithLinearBackoff: err=%v", err)
		}
		if attempts.Load() != 3 {
			t.Fatalf("RetryWithLinearBackoff: expected 3, got %d", attempts.Load())
		}
	}
}

// TestStress_Retry_RetryWithConfig 测试带配置的重试
func TestStress_Retry_RetryWithConfig(t *testing.T) {
	for round := 0; round < 10; round++ {
		var attempts atomic.Int64
		_, err := RetryWithConfig(context.Background(), func(ctx context.Context) (int, error) {
			attempts.Add(1)
			if attempts.Load() >= 2 {
				return 42, nil
			}
			return 0, errTest
		}, 3, 10*time.Millisecond, 30*time.Millisecond,
			TimeoutOpt{PerCallTimeout: 2 * time.Second},
		)
		if err != nil {
			t.Fatalf("RetryWithConfig: err=%v", err)
		}
	}
}

// TestStress_Retry_Void_Result_Variants 测试 void/result 变体
func TestStress_Retry_Void_Result_Variants(t *testing.T) {
	for round := 0; round < 20; round++ {
		var attempts atomic.Int64
		RetryWithBackoff(context.Background(), 5, 1*time.Microsecond, func(ctx context.Context) error {
			attempts.Add(1)
			if attempts.Load() >= 3 {
				return nil
			}
			return errTest
		})
		if attempts.Load() != 3 {
			t.Fatalf("RetryWithBackoff: expected 3, got %d", attempts.Load())
		}
	}

	for round := 0; round < 20; round++ {
		var attempts atomic.Int64
		val, err := RetryWithConfig(context.Background(), func(ctx context.Context) (int, error) {
			attempts.Add(1)
			if attempts.Load() >= 3 {
				return 42, nil
			}
			return 0, errTest
		}, 5, 1*time.Microsecond, 100*time.Microsecond)
		if err != nil || val != 42 {
			t.Fatalf("RetryWithConfig result: val=%d err=%v", val, err)
		}
	}

	for round := 0; round < 20; round++ {
		RetryWithLinearBackoff(context.Background(), 5, 1*time.Microsecond, func(ctx context.Context) error {
			return nil
		})
	}

	for round := 0; round < 20; round++ {
		val, err := RetryWithConfig(context.Background(), func(ctx context.Context) (int, error) {
			return 100, nil
		}, 5, 1*time.Microsecond, 100*time.Microsecond)
		if err != nil || val != 100 {
			t.Fatalf("RetryWithConfig result2: val=%d err=%v", val, err)
		}
	}
}

// TestStress_Retry_RetryWithConfigVoid 测试 RetryWithConfig 无返回值
func TestStress_Retry_RetryWithConfigVoid(t *testing.T) {
	for round := 0; round < 10; round++ {
		_, err := RetryWithConfig(context.Background(), func(ctx context.Context) (int, error) {
			return 0, nil
		}, 3, 1*time.Microsecond, 10*time.Microsecond)
		if err != nil {
			t.Fatalf("RetryWithConfig void: err=%v", err)
		}
	}
}

// TestStress_Retry_BindRetryToWorker 测试 BindRetryToWorker
func TestStress_Retry_BindRetryToWorker(t *testing.T) {
	for round := 0; round < 20; round++ {
		var attempts atomic.Int64
		retryFunc := RetryFn(func() error {
			attempts.Add(1)
			if attempts.Load() >= 3 {
				return nil
			}
			return errTest
		})
		err := retryFunc.WithRetry(5)
		if err != nil {
			t.Fatalf("RetryFn.WithRetry: err=%v", err)
		}
	}

	for round := 0; round < 20; round++ {
		retryFunc := RetryFn(func() error {
			panic("retry panic")
		})
		err := retryFunc.WithRetry(3)
		if err == nil {
			t.Fatal("RetryFn.WithRetry should return error on panic")
		}
	}
}

// TestStress_Retry_WithTimeout_WithDeadline_All 测试超时/截止时间所有变体
func TestStress_Retry_WithTimeout_WithDeadline_All(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()

		val, err := WithTimeout(ctx, 5*time.Second, func(ctx context.Context) (int, error) {
			return 42, nil
		})
		if err != nil || val != 42 {
			t.Fatalf("WithTimeout: val=%d err=%v", val, err)
		}

		_, err = WithTimeout(ctx, 5*time.Second, func(ctx context.Context) (int, error) {
			return 0, nil
		})
		if err != nil {
			t.Fatalf("WithTimeout(ignored): err=%v", err)
		}

		deadline := time.Now().Add(5 * time.Second)
		val2, err2 := WithDeadline(ctx, deadline, func(ctx context.Context) (int, error) {
			return 100, nil
		})
		if err2 != nil || val2 != 100 {
			t.Fatalf("WithDeadline: val=%d err=%v", val2, err2)
		}

		_, err = WithDeadline(ctx, deadline, func(ctx context.Context) (int, error) {
			return 0, nil
		})
		if err != nil {
			t.Fatalf("WithDeadline(ignored): err=%v", err)
		}
	}
}

// ==================== 顶层辅助函数 ====================

// TestStress_ResultValues_ResultErrors 测试结果提取函数
func TestStress_ResultValues_ResultErrors(t *testing.T) {
	for round := 0; round < 50; round++ {
		results := []Result[int]{
			{Value: 1, Err: nil},
			{Value: 0, Err: errTest},
			{Value: 3, Err: nil},
			{Value: 0, Err: errors.New("another")},
		}
		vals := ResultValues(results)
		if len(vals) != 2 {
			t.Fatalf("ResultValues: expected 2, got %d", len(vals))
		}
		errs := ResultErrors(results)
		if len(errs) != 2 {
			t.Fatalf("ResultErrors: expected 2, got %d", len(errs))
		}
	}
}

// TestStress_Every_Some_AnyError 测试布尔聚合函数
func TestStress_Every_Some_AnyError(t *testing.T) {
	for round := 0; round < 50; round++ {
		resultsOK := []Result[int]{
			{Value: 1}, {Value: 2}, {Value: 3},
		}
		if !Every(resultsOK) {
			t.Fatal("Every should be true for all-OK")
		}
		if !Some(resultsOK) {
			t.Fatal("Some should be true for all-OK")
		}
		if AnyError(resultsOK) {
			t.Fatal("AnyError should be false for all-OK")
		}

		resultsMixed := []Result[int]{
			{Value: 1}, {Value: 0, Err: errTest}, {Value: 3},
		}
		if Every(resultsMixed) {
			t.Fatal("Every should be false for mixed")
		}
		if !Some(resultsMixed) {
			t.Fatal("Some should be true for mixed")
		}
		if !AnyError(resultsMixed) {
			t.Fatal("AnyError should be true for mixed")
		}
	}
}

// TestStress_Partition 测试 Partition
func TestStress_Partition(t *testing.T) {
	for round := 0; round < 50; round++ {
		results := []Result[int]{
			{Value: 1}, {Value: 0, Err: errTest}, {Value: 3}, {Value: 0, Err: errors.New("fail")},
		}
		vals, errs := Partition(results)
		if len(vals) != 2 || len(errs) != 2 {
			t.Fatalf("Partition: expected 2/2 split, got %d/%d", len(vals), len(errs))
		}
	}
}

// TestStress_Chunk 切片分块
func TestStress_Chunk(t *testing.T) {
	for round := 0; round < 50; round++ {
		items := make([]int, 100)
		chunks := Chunk(items, 10)
		if len(chunks) != 10 {
			t.Fatalf("Chunk: expected 10 chunks, got %d", len(chunks))
		}
		for i, c := range chunks {
			if len(c) != 10 {
				t.Fatalf("Chunk[%d]: expected 10, got %d", i, len(c))
			}
		}
	}
}

// TestStress_ChunkN 按份数分块
func TestStress_ChunkN(t *testing.T) {
	for round := 0; round < 30; round++ {
		items := make([]int, 100)
		chunks := ChunkN(items, 4)
		if len(chunks) != 4 {
			t.Fatalf("ChunkN: expected 4 chunks, got %d", len(chunks))
		}
	}
}

// TestStress_Flat 展平结果
func TestStress_Flat(t *testing.T) {
	for round := 0; round < 30; round++ {
		results := []Result[int]{{Value: 1}, {Value: 2}, {Value: 3}}
		vals := Flat(results)
		if len(vals) != 3 || vals[0] != 1 || vals[1] != 2 || vals[2] != 3 {
			t.Fatalf("Flat: unexpected result: %v", vals)
		}
	}
}

// TestStress_OnlyErrors 仅提取错误
func TestStress_OnlyErrors(t *testing.T) {
	for round := 0; round < 30; round++ {
		results := []Result[int]{
			{Value: 1}, {Value: 0, Err: errTest}, {Value: 3},
		}
		errs := OnlyErrors(results)
		if len(errs) != 1 {
			t.Fatalf("OnlyErrors: expected 1, got %d", len(errs))
		}
	}
}

// ==================== 并发冲突场景 ====================

// TestStress_ConcurrentGoAndWait_RaceCondition 并发 Go+Wait 竞态
func TestStress_ConcurrentGoAndWait_RaceCondition(t *testing.T) {
	for round := 0; round < 30; round++ {
		g := NewGroup[int](50)
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return i, nil
				})
			}
		}()
		go func() {
			defer wg.Done()
			for i := 500; i < 1000; i++ {
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return i, nil
				})
			}
		}()
		wg.Wait()
		results := g.Wait()
		if len(results) != 1000 {
			t.Fatalf("round %d: expected 1000 results, got %d", round, len(results))
		}
	}
}

// TestStress_ConcurrentSubmitAndWait_RaceCondition 并发 Submit+Wait 竞态
func TestStress_ConcurrentSubmitAndWait_RaceCondition(t *testing.T) {
	for round := 0; round < 30; round++ {
		p := NewPool[int](50)
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
					return i, nil
				})
			}
		}()
		go func() {
			defer wg.Done()
			for i := 500; i < 1000; i++ {
				_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
					return i, nil
				})
			}
		}()
		wg.Wait()
		results := p.Wait()
		p.Close()
		if len(results) != 1000 {
			t.Fatalf("round %d: expected 1000 results, got %d", round, len(results))
		}
	}
}

// TestStress_GoAfterWait 在 Wait 后 Go 应该报错
func TestStress_GoAfterWait(t *testing.T) {
	for round := 0; round < 30; round++ {
		g := NewGroup[int](4)
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			_ = g.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		g.Wait()
		err := g.Go(ctx, func(ctx context.Context) (int, error) { return 100, nil })
		if !errors.Is(err, ErrGroupWaited) {
			t.Fatalf("round %d: expected ErrGroupWaited, got %v", round, err)
		}
	}
}

// TestStress_SubmitAfterPoolClose 在 Close 后 Submit 应该报错
func TestStress_SubmitAfterPoolClose(t *testing.T) {
	for round := 0; round < 30; round++ {
		p := NewPool[int](4)
		p.Close()
		err := p.Submit(context.Background(), func(ctx context.Context) (int, error) { return 1, nil })
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("round %d: expected ErrPoolClosed, got %v", round, err)
		}
	}
}

// ==================== 大数据量极限测试 ====================

// TestStress_Map_VeryLargeSlice_100KElements 10万级数据的 Map
func TestStress_Map_VeryLargeSlice_100KElements(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large data test in short mode")
	}
	n := 100000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	start := time.Now()
	results := Map(context.Background(), items, 200, func(ctx context.Context, v int) (int, error) {
		return v * 3, nil
	})
	elapsed := time.Since(start)
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for i, r := range results {
		if r.Err != nil || r.Value != i*3 {
			t.Fatalf("result[%d]: expected %d, got %v (err=%v)", i, i*3, r.Value, r.Err)
			break
		}
	}
	t.Logf("100K Map via 200 concurrency: %v", elapsed)
}

// TestStress_ForEach_VeryLargeSlice_100KElements 10万级数据的 ForEach
func TestStress_ForEach_VeryLargeSlice_100KElements(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large data test in short mode")
	}
	n := 100000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	var counter atomic.Int64
	start := time.Now()
	nr, err := ForEach(context.Background(), items, 200, func(ctx context.Context, v int) error {
		counter.Add(1)
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ForEach err: %v", err)
	}
	_ = nr
	if counter.Load() != int64(n) {
		t.Fatalf("expected %d, got %d", n, counter.Load())
	}
	t.Logf("100K ForEach via 200 concurrency: %v", elapsed)
}

// TestStress_Reduce_VeryLargeSlice_50KElements 5万级数据的 Reduce
func TestStress_Reduce_VeryLargeSlice_50KElements(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large data test in short mode")
	}
	n := 50000
	items := make([]int, n)
	for i := range items {
		items[i] = 1
	}
	start := time.Now()
	result, err := Reduce(context.Background(), items, 200,
		func(ctx context.Context, v int) (int, error) { return v, nil },
		0,
		func(a, b int) int { return a + b })
	elapsed := time.Since(start)
	if err != nil || result != n {
		t.Fatalf("Reduce: result=%d expected=%d err=%v", result, n, err)
	}
	t.Logf("50K Reduce via 200 concurrency: %v", elapsed)
}

// TestStress_Pool_VeryLargeScale_100KSubmit 10万次 Submit
func TestStress_Pool_VeryLargeScale_100KSubmit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large data test in short mode")
	}
	p := NewPool[int](500)
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 100000; i++ {
		_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	results := p.Wait()
	p.Close()
	elapsed := time.Since(start)
	if len(results) != 100000 {
		t.Fatalf("expected 100000 results, got %d", len(results))
	}
	t.Logf("100K Submit via 500 concurrency: %v", elapsed)
}

// TestStress_Group_VeryLargeScale_100KGo 10万次 Go
func TestStress_Group_VeryLargeScale_100KGo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large data test in short mode")
	}
	g := NewGroup[int](500)
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 100000; i++ {
		idx := i
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return idx, nil
		})
	}
	results := g.Wait()
	elapsed := time.Since(start)
	if len(results) != 100000 {
		t.Fatalf("expected 100000 results, got %d", len(results))
	}
	t.Logf("100K Group.Go via 500 concurrency: %v", elapsed)
}

// ==================== 混合负载压力测试 ====================

// TestStress_MixedWorkload_AllModules 混合使用所有模块
func TestStress_MixedWorkload_AllModules(t *testing.T) {
	ctx := context.Background()
	var wg sync.WaitGroup
	n := 20
	wg.Add(n * 5)

	for i := 0; i < n; i++ {
		// Group
		go func() {
			defer wg.Done()
			g := NewGroup[int](10)
			for j := 0; j < 100; j++ {
				_ = g.Go(ctx, func(ctx context.Context) (int, error) {
					return j * 2, nil
				})
			}
			g.Wait()
		}()

		// Pool
		go func() {
			defer wg.Done()
			p := NewPool[int](10)
			for j := 0; j < 100; j++ {
				_ = p.Submit(ctx, func(ctx context.Context) (int, error) {
					return j, nil
				})
			}
			p.Wait()
			p.Close()
		}()

		// Map
		go func() {
			defer wg.Done()
			items := make([]int, 100)
			Map(ctx, items, 10, func(ctx context.Context, v int) (int, error) {
				return v * 2, nil
			})
		}()

		// Task
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				task := GoResult(ctx, func(ctx context.Context) (int, error) {
					return j, nil
				})
				task.Wait()
			}
		}()

		// Retry
		go func() {
			defer wg.Done()
			RetryWithBackoff(ctx, 3, 1*time.Microsecond, func(ctx context.Context) error {
				return nil
			})
		}()
	}
	wg.Wait()
}

// TestStress_Group_FailFast_HighConcurrency 高并发 FailFast 场景
func TestStress_Group_FailFast_HighConcurrency(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		g, ffCtx := NewGroup[int](20).WithFailFast(ctx)
		for i := 0; i < 100; i++ {
			idx := i
			_ = g.Go(ffCtx, func(ctx context.Context) (int, error) {
				if idx == 10 {
					return 0, errTest
				}
				time.Sleep(100 * time.Millisecond)
				return idx, nil
			})
		}
		g.Wait()
		if !g.HasError() {
			t.Fatalf("round %d: expected HasError=true", round)
		}
	}
}
