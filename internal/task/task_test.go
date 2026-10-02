package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/test"
)

var errTask = errors.New("task error")

func genItems(n int) []int {
	sl := make([]int, n)
	for i := range sl {
		sl[i] = i
	}
	return sl
}

// ============================================================
// 一、Go 基础测试
// ============================================================

func TestGo_Success(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 42, nil
	})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestGo_Values(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (string, error) {
		return "hello", nil
	})
	val, err := ar.Values()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got '%s'", val)
	}
}

func TestGo_Error(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGo_ErrorViaError(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	err := ar.Error()
	if err == nil {
		t.Fatal("expected error via Error()")
	}
}

func TestGo_ErrorViaError_Success(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 10, nil
	})
	err := ar.Error()
	if err != nil {
		t.Fatalf("unexpected error via Error(): %v", err)
	}
}

func TestGo_PanicRecovery(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		panic("go panic")
	})
	val, err := ar.Wait()
	if err == nil {
		t.Fatalf("expected panic error, got value=%d", val)
	}
}

func TestGo_WaitTimeout(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		time.Sleep(500 * time.Millisecond)
		return 1, nil
	})
	val, err, ok := ar.WaitTimeout(50 * time.Millisecond)
	if ok {
		t.Fatalf("expected timeout, got value=%d", val)
	}
	if err == nil {
		t.Fatal("expected error from timeout")
	}
}

func TestGo_WaitTimeout_Success(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 99, nil
	})
	val, err, ok := ar.WaitTimeout(time.Second)
	if !ok {
		t.Fatal("expected success")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}

func TestGo_Ok_NotOk(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if !ar.Ok() {
		t.Fatal("expected ok")
	}
	ar2 := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	if ar2.Ok() {
		t.Fatal("expected not ok")
	}
}

func TestGo_IsPanic(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		panic("test panic")
	})
	if !ar.IsPanic() {
		t.Fatal("expected panic")
	}
	ar2 := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if ar2.IsPanic() {
		t.Fatal("expected not panic")
	}
}

func TestGo_WaitCh(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 7, nil
	})
	select {
	case r := <-ar.WaitCh():
		if r.Value != 7 || r.Err != nil {
			t.Fatalf("unexpected result: %v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel")
	}
}

func TestGo_WaitCh_Error(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	select {
	case r := <-ar.WaitCh():
		if r.Err == nil {
			t.Fatal("expected error in WaitCh result")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel")
	}
}

func TestGo_Cancel(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 1, nil
		}
	})
	val, err := ar.Cancel()
	if err != nil {
		t.Logf("cancel returned error: %v (expected before task completion)", err)
	} else {
		t.Logf("cancel returned value: %d (task completed before cancel)", val)
	}
}

func TestGo_Cancel_Completed(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 88, nil
	})
	val, err := ar.Cancel()
	if err != nil {
		t.Fatalf("Cancel on completed task should return result, got: %v", err)
	}
	if val != 88 {
		t.Fatalf("expected 88, got %d", val)
	}
}

func TestGo_MultipleWait(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 55, nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, err := ar.Wait()
			if err != nil || val != 55 {
				t.Errorf("multiple Wait failed: val=%d, err=%v", val, err)
			}
		}()
	}
	wg.Wait()
}

// ============================================================
// 二、GoResult 基础测试
// ============================================================

func TestGoResult_Success(t *testing.T) {
	tk := GoResult(context.Background(), func(ctx context.Context) (string, error) {
		return "hello", nil
	})
	val, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got '%s'", val)
	}
}

func TestGoResult_Error(t *testing.T) {
	tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	err := tk.Error()
	if err == nil {
		t.Fatal("expected error via Task.Error()")
	}
}

func TestGoResult_Cancel(t *testing.T) {
	tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 1, nil
		}
	})
	tk.Cancel()
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected error after cancel")
	}
}

func TestGoResult_Panic(t *testing.T) {
	tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		panic("result panic")
	})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected panic error")
	}
}

func TestGoResult_Ctx(t *testing.T) {
	tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if tk.Ctx == nil {
		t.Fatal("Task.Ctx should not be nil")
	}
}

func TestGoResult_CancelFunc(t *testing.T) {
	tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if tk.Cancel == nil {
		t.Fatal("Task.Cancel should not be nil")
	}
}

func TestGoResult_ResultMultiple(t *testing.T) {
	tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		return 10, nil
	})
	for i := 0; i < 5; i++ {
		val, err := tk.Result()
		if err != nil || val != 10 {
			t.Fatalf("call %d: val=%d err=%v", i, val, err)
		}
	}
}

// ============================================================
// 三、GoAct 基础测试
// ============================================================

func TestGoAct_Success(t *testing.T) {
	var called atomic.Bool
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		called.Store(true)
		return nil
	})
	_, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called.Load() {
		t.Fatal("action not called")
	}
}

func TestGoAct_Values(t *testing.T) {
	var called atomic.Bool
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		called.Store(true)
		return nil
	})
	_, err := ar.Values()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called.Load() {
		t.Fatal("action not called")
	}
}

func TestGoAct_Error(t *testing.T) {
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		return errTask
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGoAct_Panic(t *testing.T) {
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		panic("action panic")
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected panic error")
	}
}

func TestGoAct_WaitTimeout(t *testing.T) {
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		time.Sleep(500 * time.Millisecond)
		return nil
	})
	_, err, ok := ar.WaitTimeout(50 * time.Millisecond)
	if ok {
		t.Fatal("expected timeout")
	}
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGoAct_WaitTimeout_Success(t *testing.T) {
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		return nil
	})
	_, err, ok := ar.WaitTimeout(time.Second)
	if !ok {
		t.Fatal("expected success")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGoAct_Cancel(t *testing.T) {
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		time.Sleep(time.Second)
		return nil
	})
	_, err := ar.Cancel()
	if err == nil {
		t.Log("task completed before cancel")
	}
}

func TestGoAct_IsPanic(t *testing.T) {
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		panic("act panic")
	})
	if !ar.IsPanic() {
		t.Fatal("expected panic")
	}
	ar2 := GoAct(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if ar2.IsPanic() {
		t.Fatal("expected not panic")
	}
}

// ============================================================
// 四、GoResultAct 基础测试
// ============================================================

func TestGoResultAct_Success(t *testing.T) {
	var done atomic.Bool
	tk := GoResultAct(context.Background(), func(ctx context.Context) error {
		done.Store(true)
		return nil
	})
	_, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !done.Load() {
		t.Fatal("action not done")
	}
}

func TestGoResultAct_Error(t *testing.T) {
	tk := GoResultAct(context.Background(), func(ctx context.Context) error {
		return errTask
	})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGoResultAct_Panic(t *testing.T) {
	tk := GoResultAct(context.Background(), func(ctx context.Context) error {
		panic("result act panic")
	})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected panic error")
	}
}

func TestGoResultAct_Cancel(t *testing.T) {
	tk := GoResultAct(context.Background(), func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return nil
		}
	})
	tk.Cancel()
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected error after cancel")
	}
}

func TestGoResultAct_ErrorMethod(t *testing.T) {
	tk := GoResultAct(context.Background(), func(ctx context.Context) error {
		return errTask
	})
	err := tk.Error()
	if err == nil {
		t.Fatal("expected error via Task.Error()")
	}
}

// ============================================================
// 五、Mu 容器测试
// ============================================================

func TestMu_Append_Snapshot(t *testing.T) {
	mu := &Mu[int]{}
	mu.Append(func() int { return 1 })
	mu.Append(func() int { return 2 })
	mu.Append(func() int { return 3 })
	snap := mu.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3, got %d", len(snap))
	}
	if snap[0] != 1 || snap[1] != 2 || snap[2] != 3 {
		t.Fatalf("unexpected: %v", snap)
	}
}

func TestMu_Nil(t *testing.T) {
	var mu *Mu[int]
	result := mu.Snapshot()
	if result != nil {
		t.Fatal("expected nil from nil receiver")
	}
	mu = &Mu[int]{}
	result = mu.Snapshot()
	if result == nil {
		t.Fatal("expected empty slice from non-nil receiver")
	}
	if len(result) != 0 {
		t.Fatalf("expected 0, got %d", len(result))
	}
}

func TestMu_Append_Empty(t *testing.T) {
	mu := &Mu[float64]{}
	snap := mu.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("expected 0, got %d", len(snap))
	}
}

// ============================================================
// 六、NoResult 类型测试
// ============================================================

func TestNoResult_Zero(t *testing.T) {
	var nr NoResult
	_ = nr
}

func TestAsyncResultNoResult_TypeCheck(t *testing.T) {
	var _ *AsyncResultNoResult
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		return nil
	})
	nores, err := ar.Wait()
	if err != nil {
		t.Fatal(err)
	}
	_ = nores
}

func TestTaskNoResult_TypeCheck(t *testing.T) {
	var _ TaskNoResult
	tk := GoResultAct(context.Background(), func(ctx context.Context) error {
		return nil
	})
	_, err := tk.Result()
	if err != nil {
		t.Fatal(err)
	}
}

// ============================================================
// 七、BoundedRunner 基础测试
// ============================================================

func TestNewBoundedRunner(t *testing.T) {
	r := NewBoundedRunner(10)
	if r.Max() != 10 {
		t.Fatalf("expected Max=10, got %d", r.Max())
	}
	if r.Available() != 10 {
		t.Fatalf("expected Available=10, got %d", r.Available())
	}
	if r.Busy() != 0 {
		t.Fatalf("expected Busy=0, got %d", r.Busy())
	}
}

func TestNewBoundedRunner_Default(t *testing.T) {
	r := NewBoundedRunner(0)
	if r.Max() <= 0 {
		t.Fatal("default Max should be > 0")
	}
}

func TestNewDefaultBoundedRunner(t *testing.T) {
	r := NewDefaultBoundedRunner()
	if r.Max() <= 0 {
		t.Fatal("default Max should be > 0")
	}
}

func TestBoundedRunner_Available_Busy(t *testing.T) {
	r := NewBoundedRunner(5)
	if r.Available() != 5 {
		t.Fatalf("expected Available=5, got %d", r.Available())
	}
	if r.Busy() != 0 {
		t.Fatalf("expected Busy=0, got %d", r.Busy())
	}

	var wg sync.WaitGroup
	n := 3
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ar := BoundedGo(r, context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(100 * time.Millisecond)
				return 1, nil
			})
			ar.Wait()
		}()
	}
	time.Sleep(10 * time.Millisecond)
	busy := r.Busy()
	if busy < 1 || busy > n {
		t.Fatalf("expected Busy between 1 and %d, got %d", n, busy)
	}
	avail := r.Available()
	if avail < 0 || avail > r.Max() {
		t.Fatalf("unexpected Available: %d", avail)
	}
	wg.Wait()
	if r.Busy() != 0 {
		t.Fatalf("expected Busy=0 after all done, got %d", r.Busy())
	}
	if r.Available() != r.Max() {
		t.Fatalf("expected Available=%d after all done, got %d", r.Max(), r.Available())
	}
}

func TestBoundedGo_Success(t *testing.T) {
	r := NewBoundedRunner(10)
	n := 100
	var success atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			ar := BoundedGo(r, context.Background(), func(ctx context.Context) (int, error) {
				return idx, nil
			})
			val, err := ar.Wait()
			if err == nil && val == idx {
				success.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if success.Load() != int64(n) {
		t.Fatalf("expected %d successes, got %d", n, success.Load())
	}
}

func TestBoundedGo_Error(t *testing.T) {
	r := NewBoundedRunner(5)
	ar := BoundedGo(r, context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBoundedGo_Panic(t *testing.T) {
	r := NewBoundedRunner(5)
	ar := BoundedGo(r, context.Background(), func(ctx context.Context) (int, error) {
		panic("bounded panic")
	})
	if !ar.IsPanic() {
		t.Fatal("expected panic")
	}
}

func TestBoundedGo_ContextCancel(t *testing.T) {
	r := NewBoundedRunner(1)
	blockCh := make(chan struct{})
	go func() {
		BoundedGo(r, context.Background(), func(ctx context.Context) (int, error) {
			<-blockCh
			return 1, nil
		})
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ar := BoundedGo(r, ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected context cancel error")
	}
	close(blockCh)
}

func TestBoundedGoAct_Success(t *testing.T) {
	r := NewBoundedRunner(10)
	var called atomic.Bool
	ar := BoundedGoAct(r, context.Background(), func(ctx context.Context) error {
		called.Store(true)
		return nil
	})
	_, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called.Load() {
		t.Fatal("action not called")
	}
}

func TestBoundedGoAct_Error(t *testing.T) {
	r := NewBoundedRunner(5)
	ar := BoundedGoAct(r, context.Background(), func(ctx context.Context) error {
		return errTask
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBoundedGoAct_ContextCancel(t *testing.T) {
	r := NewBoundedRunner(1)
	blockCh := make(chan struct{})
	go func() {
		BoundedGoAct(r, context.Background(), func(ctx context.Context) error {
			<-blockCh
			return nil
		})
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ar := BoundedGoAct(r, ctx, func(ctx context.Context) error {
		return nil
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected context cancel error")
	}
	close(blockCh)
}

func TestBoundedGoResult_Success(t *testing.T) {
	r := NewBoundedRunner(10)
	tk := BoundedGoResult(r, context.Background(), func(ctx context.Context) (string, error) {
		return "done", nil
	})
	val, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "done" {
		t.Fatalf("expected 'done', got '%s'", val)
	}
}

func TestBoundedGoResult_Cancel(t *testing.T) {
	r := NewBoundedRunner(5)
	tk := BoundedGoResult(r, context.Background(), func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 1, nil
		}
	})
	tk.Cancel()
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected error after cancel")
	}
}

func TestBoundedGoResult_ContextCancel(t *testing.T) {
	r := NewBoundedRunner(1)
	blockCh := make(chan struct{})
	go func() {
		BoundedGoResult(r, context.Background(), func(ctx context.Context) (int, error) {
			<-blockCh
			return 1, nil
		})
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tk := BoundedGoResult(r, ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected context cancel error")
	}
	close(blockCh)
}

func TestBoundedGoResult_Error(t *testing.T) {
	r := NewBoundedRunner(5)
	tk := BoundedGoResult(r, context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	err := tk.Error()
	if err == nil {
		t.Fatal("expected error via Task.Error()")
	}
}

// ============================================================
// 八、BoundedRunnerBuilder 链式构建测试
// ============================================================

func TestNewBoundedRunnerBuilder(t *testing.T) {
	b := NewBoundedRunnerBuilder()
	r := b.Build()
	if r.Max() <= 0 {
		t.Fatal("default Max should be > 0")
	}
}

func TestBoundedRunnerBuilder_Max(t *testing.T) {
	r := NewBoundedRunnerBuilder().Max(20).Build()
	if r.Max() != 20 {
		t.Fatalf("expected Max=20, got %d", r.Max())
	}
}

func TestBoundedRunnerBuilder_Context(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewBoundedRunnerBuilder().Context(ctx).Max(5).Build()
	ar := BoundedGo(r, ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	_, err := ar.Wait()
	_ = err
}

func TestBoundedRunnerBuilder_Logger(t *testing.T) {
	b := NewBoundedRunnerBuilder().Logger(nil)
	r := b.Build()
	if r == nil {
		t.Fatal("build should not be nil")
	}
}

func TestBoundedRunnerBuilder_DefaultLogger(t *testing.T) {
	b := NewBoundedRunnerBuilder().DefaultLogger()
	r := b.Build()
	if r == nil {
		t.Fatal("build should not be nil")
	}
}

func TestBoundedRunnerBuilder_Chain(t *testing.T) {
	r := NewBoundedRunnerBuilder().
		Context(context.Background()).
		Max(15).
		DefaultLogger().
		Build()
	if r.Max() != 15 {
		t.Fatalf("expected Max=15, got %d", r.Max())
	}
}

// ============================================================
// 九、TaskBuilder 基础测试
// ============================================================

func TestNewTaskBuilder_Go(t *testing.T) {
	b := NewTaskBuilder[int]()
	ar := b.Context(context.Background()).Go(func(ctx context.Context) (int, error) {
		return 100, nil
	})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 100 {
		t.Fatalf("expected 100, got %d", val)
	}
}

func TestNewTaskBuilder_GoResult(t *testing.T) {
	b := NewTaskBuilder[string]()
	tk := b.Context(context.Background()).GoResult(func(ctx context.Context) (string, error) {
		return "builder", nil
	})
	val, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "builder" {
		t.Fatalf("expected 'builder', got '%s'", val)
	}
}

func TestNewTaskBuilder_GoAct(t *testing.T) {
	b := NewTaskBuilder[int]()
	var called atomic.Bool
	ar := b.Context(context.Background()).GoAct(func(ctx context.Context) error {
		called.Store(true)
		return nil
	})
	_, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called.Load() {
		t.Fatal("action not called")
	}
}

func TestNewTaskBuilder_GoResultAct(t *testing.T) {
	b := NewTaskBuilder[int]()
	var called atomic.Bool
	tk := b.Context(context.Background()).GoResultAct(func(ctx context.Context) error {
		called.Store(true)
		return nil
	})
	_, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called.Load() {
		t.Fatal("action not called")
	}
}

func TestTaskBuilder_WithTimeout(t *testing.T) {
	b := NewTaskBuilder[int]()
	ar := b.Context(context.Background()).WithTimeout(50 * time.Millisecond).Go(
		func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(time.Second):
				return 1, nil
			}
		})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestTaskBuilder_WithTimeout_GoResult(t *testing.T) {
	b := NewTaskBuilder[int]()
	tk := b.Context(context.Background()).WithTimeout(50 * time.Millisecond).GoResult(
		func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(time.Second):
				return 1, nil
			}
		})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestTaskBuilder_DefaultTimeout(t *testing.T) {
	b := NewTaskBuilder[int]()
	ar := b.Context(context.Background()).WithTimeout(time.Second).DefaultTimeout().Go(
		func(ctx context.Context) (int, error) {
			return 42, nil
		})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("default timeout should have no effect: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestTaskBuilder_Bounded(t *testing.T) {
	b := NewTaskBuilder[int]()
	ar := b.Context(context.Background()).Bounded(5).Go(
		func(ctx context.Context) (int, error) {
			return 77, nil
		})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 77 {
		t.Fatalf("expected 77, got %d", val)
	}
}

func TestTaskBuilder_Bounded_GoResult(t *testing.T) {
	b := NewTaskBuilder[int]()
	tk := b.Context(context.Background()).Bounded(3).GoResult(
		func(ctx context.Context) (int, error) {
			return 88, nil
		})
	val, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 88 {
		t.Fatalf("expected 88, got %d", val)
	}
}

func TestTaskBuilder_Bounded_GoAct(t *testing.T) {
	b := NewTaskBuilder[int]()
	var called atomic.Bool
	ar := b.Context(context.Background()).Bounded(3).GoAct(
		func(ctx context.Context) error {
			called.Store(true)
			return nil
		})
	_, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called.Load() {
		t.Fatal("action not called")
	}
}

func TestTaskBuilder_Bounded_GoResultAct(t *testing.T) {
	b := NewTaskBuilder[int]()
	var called atomic.Bool
	tk := b.Context(context.Background()).Bounded(3).GoResultAct(
		func(ctx context.Context) error {
			called.Store(true)
			return nil
		})
	_, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called.Load() {
		t.Fatal("action not called")
	}
}

func TestTaskBuilder_DefaultBounded(t *testing.T) {
	b := NewTaskBuilder[int]()
	ar := b.Context(context.Background()).Bounded(1).DefaultBounded().Go(
		func(ctx context.Context) (int, error) {
			return 99, nil
		})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}

func TestTaskBuilder_Logger(t *testing.T) {
	b := NewTaskBuilder[int]().Logger(nil)
	ar := b.Context(context.Background()).Go(func(ctx context.Context) (int, error) {
		return 1, nil
	})
	_, err := ar.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

func TestTaskBuilder_DefaultLogger(t *testing.T) {
	b := NewTaskBuilder[int]().DefaultLogger()
	ar := b.Context(context.Background()).Go(func(ctx context.Context) (int, error) {
		return 1, nil
	})
	_, err := ar.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

func TestTaskBuilder_FullChain(t *testing.T) {
	b := NewTaskBuilder[int]().
		Context(context.Background()).
		DefaultLogger().
		DefaultTimeout().
		DefaultBounded()

	ar := b.Go(func(ctx context.Context) (int, error) {
		return 42, nil
	})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestTaskBuilder_BoundedWithTimeout(t *testing.T) {
	b := NewTaskBuilder[int]()
	ar := b.Context(context.Background()).WithTimeout(time.Second).Bounded(10).Go(
		func(ctx context.Context) (int, error) {
			return 55, nil
		})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 55 {
		t.Fatalf("expected 55, got %d", val)
	}
}

func TestTaskBuilder_GoResult_Error(t *testing.T) {
	b := NewTaskBuilder[int]()
	tk := b.Context(context.Background()).GoResult(func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTaskBuilder_GoAct_Error(t *testing.T) {
	b := NewTaskBuilder[int]()
	ar := b.Context(context.Background()).GoAct(func(ctx context.Context) error {
		return errTask
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTaskBuilder_GoResultAct_Error(t *testing.T) {
	b := NewTaskBuilder[int]()
	tk := b.Context(context.Background()).GoResultAct(func(ctx context.Context) error {
		return errTask
	})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected error")
	}
}

// ============================================================
// 十、四档并发压力测试
// ============================================================

func TestGo_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			var success, fail atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func(i int) {
					defer wg.Done()
					ar := Go(context.Background(), func(ctx context.Context) (int, error) {
						return i, nil
					})
					val, err := ar.Wait()
					if err == nil && val == i {
						success.Add(1)
					} else {
						fail.Add(1)
					}
				}(i)
			}
			wg.Wait()
			if fail.Load() > 0 {
				t.Fatalf("failures: %d / %d", fail.Load(), tier.Size)
			}
		})
	}
}

func TestGo_WaitTimeout_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			var success, timeout atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					ar := Go(context.Background(), func(ctx context.Context) (int, error) {
						return 1, nil
					})
					_, _, ok := ar.WaitTimeout(time.Second)
					if ok {
						success.Add(1)
					} else {
						timeout.Add(1)
					}
				}()
			}
			wg.Wait()
			if timeout.Load() > 0 {
				t.Fatalf("timeouts: %d", timeout.Load())
			}
		})
	}
}

func TestGo_WaitCh_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					ar := Go(context.Background(), func(ctx context.Context) (int, error) {
						return 1, nil
					})
					select {
					case r := <-ar.WaitCh():
						if r.Err == nil && r.Value == 1 {
							success.Add(1)
						}
					case <-time.After(5 * time.Second):
					}
				}()
			}
			wg.Wait()
			if success.Load() < int64(tier.Size)*95/100 {
				t.Fatalf("expected >= 95%% success, got %d / %d", success.Load(), tier.Size)
			}
		})
	}
}

func TestGo_MultipleWait_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			n := 100

			var wg sync.WaitGroup
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					ar := Go(context.Background(), func(ctx context.Context) (int, error) {
						return 42, nil
					})
					var innerWg sync.WaitGroup
					innerWg.Add(10)
					for j := 0; j < 10; j++ {
						go func() {
							defer innerWg.Done()
							val, err := ar.Wait()
							if err != nil || val != 42 {
								t.Errorf("multiple wait failed: val=%d, err=%v", val, err)
							}
						}()
					}
					innerWg.Wait()
				}()
			}
			wg.Wait()
		})
	}
}

func TestGoResult_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
						return 1, nil
					})
					val, err := tk.Result()
					if err == nil && val == 1 {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, success.Load())
			}
		})
	}
}

func TestGoAct_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					ar := GoAct(context.Background(), func(ctx context.Context) error {
						return nil
					})
					_, err := ar.Wait()
					if err == nil {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, success.Load())
			}
		})
	}
}

func TestGoResultAct_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					tk := GoResultAct(context.Background(), func(ctx context.Context) error {
						return nil
					})
					_, err := tk.Result()
					if err == nil {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, success.Load())
			}
		})
	}
}

func TestGo_PanicRecovery_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			var panics, nonpanics atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func(i int) {
					defer wg.Done()
					ar := Go(context.Background(), func(ctx context.Context) (int, error) {
						if i%2 == 0 {
							panic("even panic")
						}
						return i, nil
					})
					if ar.IsPanic() {
						panics.Add(1)
					} else {
						nonpanics.Add(1)
					}
				}(i)
			}
			wg.Wait()
			t.Logf("panics=%d nonpanics=%d", panics.Load(), nonpanics.Load())
		})
	}
}

func TestMu_ConcurrentAppend(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			mu := &Mu[int]{}
			var wg sync.WaitGroup
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func(i int) {
					defer wg.Done()
					mu.Append(func() int { return i })
				}(i)
			}
			wg.Wait()
			snap := mu.Snapshot()
			if len(snap) != tier.Size {
				t.Fatalf("expected %d, got %d", tier.Size, len(snap))
			}
		})
	}
}

func TestBoundedGo_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			r := NewBoundedRunner(100)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func(idx int) {
					defer wg.Done()
					ar := BoundedGo(r, context.Background(), func(ctx context.Context) (int, error) {
						return idx, nil
					})
					_, err := ar.Wait()
					if err == nil {
						success.Add(1)
					}
				}(i)
			}
			wg.Wait()
			if success.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, success.Load())
			}
		})
	}
}

func TestBoundedGoAct_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			r := NewBoundedRunner(100)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					ar := BoundedGoAct(r, context.Background(), func(ctx context.Context) error {
						return nil
					})
					_, err := ar.Wait()
					if err == nil {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, success.Load())
			}
		})
	}
}

func TestBoundedGoResult_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			r := NewBoundedRunner(100)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					tk := BoundedGoResult(r, context.Background(), func(ctx context.Context) (int, error) {
						return 1, nil
					})
					_, err := tk.Result()
					if err == nil {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, success.Load())
			}
		})
	}
}

func TestBoundedRunner_SlotReuse_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			r := NewBoundedRunner(50)

			var wg sync.WaitGroup
			var success atomic.Int64
			n := tier.Size
			if n > 50000 {
				n = 50000
			}
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					ar := BoundedGo(r, context.Background(), func(ctx context.Context) (int, error) {
						return 1, nil
					})
					_, err := ar.Wait()
					if err == nil {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(n) {
				t.Fatalf("expected %d, got %d", n, success.Load())
			}
			if r.Busy() != 0 {
				t.Fatalf("expected Busy=0, got %d", r.Busy())
			}
			if r.Available() != r.Max() {
				t.Fatalf("expected all slots available, got %d/%d", r.Available(), r.Max())
			}
		})
	}
}

func TestTaskBuilder_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					b := NewTaskBuilder[int]().Context(context.Background())
					ar := b.Go(func(ctx context.Context) (int, error) {
						return 1, nil
					})
					_, err := ar.Wait()
					if err == nil {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, success.Load())
			}
		})
	}
}

func TestTaskBuilder_Bounded_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			b := NewTaskBuilder[int]().Context(context.Background()).Bounded(50)

			var wg sync.WaitGroup
			var success atomic.Int64
			n := tier.Size
			if n > 50000 {
				n = 50000
			}
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					ar := b.Go(func(ctx context.Context) (int, error) {
						return 1, nil
					})
					_, err := ar.Wait()
					if err == nil {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(n) {
				t.Fatalf("expected %d, got %d", n, success.Load())
			}
		})
	}
}

// ============================================================
// 十一、Race 竞态测试
// ============================================================

func TestRace_Go_CancelRace(t *testing.T) {
	for round := 0; round < 3; round++ {
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ar := Go(context.Background(), func(ctx context.Context) (int, error) {
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(5 * time.Millisecond):
						return 1, nil
					}
				})
				_, _ = ar.Cancel()
			}()
		}
		wg.Wait()
	}
}

func TestRace_Go_ContextCancellation(t *testing.T) {
	for round := 0; round < 3; round++ {
		var wg sync.WaitGroup
		n := 1000
		var cancelled atomic.Int64
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithCancel(context.Background())
				ar := Go(ctx, func(ctx context.Context) (int, error) {
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(time.Second):
						return 1, nil
					}
				})
				cancel()
				_, err := ar.Wait()
				if err != nil {
					cancelled.Add(1)
				}
			}()
		}
		wg.Wait()
		if cancelled.Load() == 0 {
			t.Fatal("expected some cancellations")
		}
	}
}

func TestRace_Go_WaitCancelRace(t *testing.T) {
	for round := 0; round < 3; round++ {
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ar := Go(context.Background(), func(ctx context.Context) (int, error) {
					time.Sleep(100 * time.Microsecond)
					return 1, nil
				})
				go ar.Cancel()
				ar.Wait()
			}()
		}
		wg.Wait()
	}
}

func TestRace_GoResult_CancelRace(t *testing.T) {
	for round := 0; round < 3; round++ {
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
					time.Sleep(100 * time.Microsecond)
					return 1, nil
				})
				go tk.Cancel()
				tk.Result()
			}()
		}
		wg.Wait()
	}
}

func TestRace_BoundedGo_CancelRace(t *testing.T) {
	for round := 0; round < 3; round++ {
		r := NewBoundedRunner(100)
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ar := BoundedGo(r, context.Background(), func(ctx context.Context) (int, error) {
					time.Sleep(100 * time.Microsecond)
					return 1, nil
				})
				_, _ = ar.Cancel()
			}()
		}
		wg.Wait()
	}
}

func TestRace_BoundedGoResult_CancelRace(t *testing.T) {
	for round := 0; round < 3; round++ {
		r := NewBoundedRunner(100)
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				tk := BoundedGoResult(r, context.Background(), func(ctx context.Context) (int, error) {
					time.Sleep(100 * time.Microsecond)
					return 1, nil
				})
				go tk.Cancel()
				tk.Result()
			}()
		}
		wg.Wait()
	}
}

func TestRace_BoundedRunner_SlotRace(t *testing.T) {
	for round := 0; round < 3; round++ {
		r := NewBoundedRunner(50)
		var wg sync.WaitGroup
		n := 1000
		var success atomic.Int64
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ar := BoundedGo(r, context.Background(), func(ctx context.Context) (int, error) {
					return 1, nil
				})
				_, err := ar.Wait()
				if err == nil {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() != int64(n) {
			t.Fatalf("round %d: expected %d, got %d", round, n, success.Load())
		}
	}
}

func TestRace_TaskBuilder_ConcurrentBuild(t *testing.T) {
	for round := 0; round < 3; round++ {
		var wg sync.WaitGroup
		n := 1000
		var success atomic.Int64
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				b := NewTaskBuilder[int]().Context(context.Background()).Bounded(20)
				ar := b.Go(func(ctx context.Context) (int, error) {
					return 1, nil
				})
				_, err := ar.Wait()
				if err == nil {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() != int64(n) {
			t.Fatalf("round %d: expected %d, got %d", round, n, success.Load())
		}
	}
}

// ==================== BoundedRunner Boundary Tests ====================

func TestNewBoundedRunner_Negative(t *testing.T) {
	r := NewBoundedRunner(-1)
	if r.Max() <= 0 {
		t.Fatal("NewBoundedRunner(-1) should fallback to default IO concurrency > 0")
	}
	t.Logf("NewBoundedRunner(-1) Max=%d", r.Max())
}

func TestNewBoundedRunner_Zero(t *testing.T) {
	r := NewBoundedRunner(0)
	if r.Max() <= 0 {
		t.Fatal("NewBoundedRunner(0) should fallback to default IO concurrency > 0")
	}
	t.Logf("NewBoundedRunner(0) Max=%d", r.Max())
}

func TestBoundedRunnerBuilder_Negative(t *testing.T) {
	r := NewBoundedRunnerBuilder().Max(-1).Build()
	if r.Max() <= 0 {
		t.Fatal("BoundedRunnerBuilder.Max(-1) should fallback to default")
	}
	t.Logf("BoundedRunnerBuilder.Max(-1) Max=%d", r.Max())
}

func TestBoundedRunnerBuilder_Zero(t *testing.T) {
	r := NewBoundedRunnerBuilder().Max(0).Build()
	if r.Max() <= 0 {
		t.Fatal("BoundedRunnerBuilder.Max(0) should fallback to default")
	}
	t.Logf("BoundedRunnerBuilder.Max(0) Max=%d", r.Max())
}
