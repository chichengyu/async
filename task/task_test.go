package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errTask = errors.New("task error")

// ==================== Go 基础测试 ====================

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

func TestGo_Error(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error")
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

func TestGo_WaitTimeoutSuccess(t *testing.T) {
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

func TestGo_Ok(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if !ar.Ok() {
		t.Fatal("expected ok")
	}
}

func TestGo_NotOk(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	if ar.Ok() {
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
}

func TestGo_NotPanic(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if ar.IsPanic() {
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

// ==================== GoResult 测试 ====================

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

// ==================== GoAction 测试 ====================

func TestGoAction_Success(t *testing.T) {
	var called atomic.Bool
	ar := GoAction(context.Background(), func(ctx context.Context) error {
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

func TestGoAction_Error(t *testing.T) {
	ar := GoAction(context.Background(), func(ctx context.Context) error {
		return errTask
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGoAction_Panic(t *testing.T) {
	ar := GoAction(context.Background(), func(ctx context.Context) error {
		panic("action panic")
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected panic error")
	}
}

// ==================== GoResultAction 测试 ====================

func TestGoResultAction_Success(t *testing.T) {
	var done atomic.Bool
	tk := GoResultAction(context.Background(), func(ctx context.Context) error {
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

func TestGoResultAction_Error(t *testing.T) {
	tk := GoResultAction(context.Background(), func(ctx context.Context) error {
		return errTask
	})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected error")
	}
}

// ==================== 高并发极限压力测试 ====================

func TestGo_50K_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	n := 50000
	var success, fail atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
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
		t.Fatalf("failures: %d / %d", fail.Load(), n)
	}
	t.Logf("Go 50K: success=%d, fail=%d", success.Load(), fail.Load())
}

func TestGo_WaitTimeout_10K(t *testing.T) {
	var wg sync.WaitGroup
	n := 10000
	var success, timeout atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
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
}

func TestGo_WaitCh_50K(t *testing.T) {
	var wg sync.WaitGroup
	n := 50000
	var success atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
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
	if success.Load() < int64(n)*95/100 {
		t.Fatalf("expected >= 95%% success, got %d / %d", success.Load(), n)
	}
	t.Logf("Go WaitCh 50K: success=%d/%d", success.Load(), n)
}

func TestGo_MultipleWait_10K(t *testing.T) {
	var wg sync.WaitGroup
	n := 1000
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
}

func TestGoResult_50K(t *testing.T) {
	var wg sync.WaitGroup
	n := 50000
	var success atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
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
	if success.Load() != int64(n) {
		t.Fatalf("expected %d, got %d", n, success.Load())
	}
}

func TestGoAction_50K(t *testing.T) {
	var wg sync.WaitGroup
	n := 50000
	var success atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ar := GoAction(context.Background(), func(ctx context.Context) error {
				return nil
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
}

func TestGoResultAction_10K(t *testing.T) {
	var wg sync.WaitGroup
	n := 10000
	var success atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			tk := GoResultAction(context.Background(), func(ctx context.Context) error {
				return nil
			})
			_, err := tk.Result()
			if err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() != int64(n) {
		t.Fatalf("expected %d, got %d", n, success.Load())
	}
}

func TestGo_CancelRace_10K(t *testing.T) {
	var wg sync.WaitGroup
	n := 10000
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

func TestGo_PanicRecovery_50K(t *testing.T) {
	var wg sync.WaitGroup
	n := 50000
	var panics, nonpanics atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
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
	t.Logf("PanicRecovery 50K: panics=%d, nonpanics=%d", panics.Load(), nonpanics.Load())
}

func TestGo_ContextCancellation_10K(t *testing.T) {
	var wg sync.WaitGroup
	n := 10000
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

func TestMu_ConcurrentAppend_100K(t *testing.T) {
	mu := &Mu[int]{}
	var wg sync.WaitGroup
	n := 100000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			mu.Append(func() int { return i })
		}(i)
	}
	wg.Wait()
	snap := mu.Snapshot()
	if len(snap) != n {
		t.Fatalf("expected %d, got %d", n, len(snap))
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
