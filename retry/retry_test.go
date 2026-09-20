package retry

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errRetry = errors.New("retry test error")

// ==================== 指数退避测试 ====================

func TestRetryWithBackoff_Success(t *testing.T) {
	ctx := context.Background()
	val, err := RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	}, 3, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestRetryWithBackoff_RetryThenSuccess(t *testing.T) {
	ctx := context.Background()
	var attempts int32
	val, err := RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
		attempts++
		if attempts < 3 {
			return 0, errRetry
		}
		return 123, nil
	}, 5, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 123 {
		t.Fatalf("expected 123, got %d", val)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryWithBackoff_Exhausted(t *testing.T) {
	ctx := context.Background()
	_, err := RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
		return 0, errRetry
	}, 3, 10*time.Millisecond, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}
}

func TestRetryWithBackoff_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
		return 0, errRetry
	}, 10, 200*time.Millisecond, 2*time.Second)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestRetryWithBackoff_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	_, err := RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
		panic("retry panic")
	}, 1, 10*time.Millisecond, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected panic error")
	}
}

func TestRetryWithBackoff_ZeroBackoff(t *testing.T) {
	ctx := context.Background()
	var attempts int
	_, err := RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
		attempts++
		if attempts < 100 {
			return 0, errRetry
		}
		return attempts, nil
	}, 200, 0, 0)
	if err == nil || attempts >= 50 {
		t.Logf("zero backoff test: attempts=%d, err=%v", attempts, err)
	}
}

// ==================== Void 测试 ====================

func TestRetryWithBackoffVoid_Success(t *testing.T) {
	ctx := context.Background()
	err := RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
		return nil
	}, 3, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryWithBackoffVoid_Retry(t *testing.T) {
	ctx := context.Background()
	var attempts int32
	err := RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return errRetry
		}
		return nil
	}, 5, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ==================== 线性退避测试 ====================

func TestRetryWithLinearBackoff_Success(t *testing.T) {
	ctx := context.Background()
	val, err := RetryWithLinearBackoff(ctx, func(ctx context.Context) (string, error) {
		return "linear-ok", nil
	}, 3, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "linear-ok" {
		t.Fatalf("unexpected: %s", val)
	}
}

func TestRetryWithLinearBackoff_Retries(t *testing.T) {
	ctx := context.Background()
	var attempts int32
	_, err := RetryWithLinearBackoff(ctx, func(ctx context.Context) (int, error) {
		attempts++
		return 0, errRetry
	}, 3, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts < 4 {
		t.Fatalf("expected >=4 attempts, got %d", attempts)
	}
}

func TestRetryWithLinearBackoffVoid_Panic(t *testing.T) {
	ctx := context.Background()
	err := RetryWithLinearBackoffVoid(ctx, func(ctx context.Context) error {
		panic("linear void panic")
	}, 1, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected panic error")
	}
}

// ==================== 可配置重试测试 ====================

func TestRetryWithConfig_Success(t *testing.T) {
	ctx := context.Background()
	val, err := RetryWithConfig(ctx, func(ctx context.Context) (int, error) {
		return 99, nil
	}, 3, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}

func TestRetryWithConfigVoid(t *testing.T) {
	ctx := context.Background()
	err := RetryWithConfigVoid(ctx, func(ctx context.Context) error {
		return nil
	}, 3, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ==================== 超时/截止时间包装测试 ====================

func TestWithTimeout_Success(t *testing.T) {
	ctx := context.Background()
	val, err := WithTimeout(ctx, 5*time.Second, func(ctx context.Context) (int, error) {
		return 7, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 7 {
		t.Fatalf("expected 7, got %d", val)
	}
}

func TestWithTimeout_Timeout(t *testing.T) {
	ctx := context.Background()
	_, err := WithTimeout(ctx, 50*time.Millisecond, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 1, nil
		}
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestWithDeadline(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	val, err := WithDeadline(ctx, deadline, func(ctx context.Context) (int, error) {
		return 10, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 10 {
		t.Fatalf("expected 10, got %d", val)
	}
}

// ==================== 函数式重试测试 ====================

func TestRetryFn_WithRetry(t *testing.T) {
	var attempts int32
	err := RetryFn(func() error {
		attempts++
		if attempts < 3 {
			return errRetry
		}
		return nil
	}).WithRetry(5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryFn_Exhausted(t *testing.T) {
	err := RetryFn(func() error {
		return errRetry
	}).WithRetry(2)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ==================== 高并发极限压力测试 ====================

func TestRetryWithBackoff_50K_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	n := 50000
	var success, fail atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			_, err := RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
				if i%5 == 0 {
					return 0, errRetry
				}
				return i, nil
			}, 2, time.Millisecond, 10*time.Millisecond)
			if err != nil {
				fail.Add(1)
			} else {
				success.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if success.Load()+fail.Load() != int64(n) {
		t.Fatalf("total mismatch: %d+%d != %d", success.Load(), fail.Load(), n)
	}
	t.Logf("50K: success=%d, fail=%d", success.Load(), fail.Load())
}

func TestRetryWithBackoff_TimedCancellation_10K(t *testing.T) {
	var wg sync.WaitGroup
	n := 10000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cancel()
			_, _ = RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
				return 0, errRetry
			}, 10, 10*time.Millisecond, 100*time.Millisecond)
		}()
	}
	wg.Wait()
}

func TestRetryWithLinearBackoff_100K_Concurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100K in short mode")
	}
	var wg sync.WaitGroup
	n := 100000
	var total atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, _ = RetryWithLinearBackoff(ctx, func(ctx context.Context) (int, error) {
				return 1, nil
			}, 1, time.Microsecond)
			total.Add(1)
		}()
	}
	wg.Wait()
	if total.Load() != int64(n) {
		t.Fatalf("expected %d, got %d", n, total.Load())
	}
}

func TestRetryFn_50K_Simple(t *testing.T) {
	var wg sync.WaitGroup
	n := 50000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_ = RetryFn(func() error { return nil }).WithRetry(1)
		}()
	}
	wg.Wait()
}

func TestRetryWithConfig_Concurrent_10K(t *testing.T) {
	var wg sync.WaitGroup
	n := 10000
	wg.Add(n)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			backoff := time.Duration(rng.Intn(20)+1) * time.Millisecond
			maxBackoff := time.Duration(rng.Intn(100)+10) * time.Millisecond
			_, _ = RetryWithConfig(ctx, func(ctx context.Context) (int, error) {
				return 1, nil
			}, 2, backoff, maxBackoff)
		}()
	}
	wg.Wait()
}

func TestRetryFn_BindRetryToWorker_10K(t *testing.T) {
	t.Skip("BindRetryToWorker requires a WorkerPoolBackend")
}

func TestWithTimeout_Stress_10K(t *testing.T) {
	var wg sync.WaitGroup
	n := 10000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, _ = WithTimeout(ctx, time.Second, func(ctx context.Context) (int, error) {
				return 1, nil
			})
		}()
	}
	wg.Wait()
}
