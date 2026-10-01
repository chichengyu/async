package retry

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/testutil"
)

var errRetry = errors.New("retry test error")

// ==================== RetryWithBackoff ====================

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
	var attempts atomic.Int32
	val, err := RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
		attempts.Add(1)
		if attempts.Load() < 3 {
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
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
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
	var attempts atomic.Int32
	_, err := RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
		attempts.Add(1)
		if attempts.Load() < 100 {
			return 0, errRetry
		}
		return int(attempts.Load()), nil
	}, 150, 0, 0)
	if err != nil {
		t.Logf("zero backoff test: attempts=%d, err=%v", attempts.Load(), err)
	}
}

func TestRetryWithBackoff_MaxRetriesZero(t *testing.T) {
	ctx := context.Background()
	val, err := RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
		return 7, nil
	}, 0, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("maxRetries=0 should still try once: %v", err)
	}
	if val != 7 {
		t.Fatalf("expected 7, got %d", val)
	}
}

// ==================== RetryWithBackoffVoid ====================

func TestRetryWithBackoffVoid_Success(t *testing.T) {
	var attempts atomic.Int32
	ctx := context.Background()
	err := RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
		attempts.Add(1)
		if attempts.Load() < 3 {
			return errRetry
		}
		return nil
	}, 5, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryWithBackoffVoid_Exhausted(t *testing.T) {
	ctx := context.Background()
	err := RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
		return errRetry
	}, 2, 10*time.Millisecond, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetryWithBackoffVoid_Panic(t *testing.T) {
	ctx := context.Background()
	err := RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
		panic("void panic")
	}, 1, 10*time.Millisecond, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected panic error")
	}
}

// ==================== RetryWithBackoffResult ====================

func TestRetryWithBackoffResult_Success(t *testing.T) {
	ctx := context.Background()
	r := RetryWithBackoffResult(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	}, 3, 10*time.Millisecond, 100*time.Millisecond)
	if !r.Ok() {
		t.Fatalf("expected ok: %v", r.Err)
	}
	if r.Value != 42 {
		t.Fatalf("expected 42, got %d", r.Value)
	}
}

func TestRetryWithBackoffResult_Fail(t *testing.T) {
	ctx := context.Background()
	r := RetryWithBackoffResult(ctx, func(ctx context.Context) (int, error) {
		return 0, errRetry
	}, 2, 10*time.Millisecond, 50*time.Millisecond)
	if r.Ok() {
		t.Fatal("expected fail")
	}
}

// ==================== RetryWithLinearBackoff ====================

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
	var attempts atomic.Int32
	_, err := RetryWithLinearBackoff(ctx, func(ctx context.Context) (int, error) {
		attempts.Add(1)
		return 0, errRetry
	}, 3, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts.Load() != 4 {
		t.Fatalf("expected 4 attempts, got %d", attempts.Load())
	}
}

func TestRetryWithLinearBackoffVoid_Success(t *testing.T) {
	ctx := context.Background()
	err := RetryWithLinearBackoffVoid(ctx, func(ctx context.Context) error {
		return nil
	}, 3, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
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

func TestRetryWithLinearBackoffResult_Success(t *testing.T) {
	ctx := context.Background()
	r := RetryWithLinearBackoffResult(ctx, func(ctx context.Context) (int, error) {
		return 88, nil
	}, 3, 10*time.Millisecond)
	if !r.Ok() || r.Value != 88 {
		t.Fatalf("result: %+v", r)
	}
}

func TestRetryWithLinearBackoffResult_Fail(t *testing.T) {
	ctx := context.Background()
	r := RetryWithLinearBackoffResult(ctx, func(ctx context.Context) (int, error) {
		return 0, errRetry
	}, 1, 10*time.Millisecond)
	if r.Ok() {
		t.Fatal("expected fail")
	}
}

// ==================== RetryWithConfig ====================

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

func TestRetryWithConfig_Exhausted(t *testing.T) {
	ctx := context.Background()
	_, err := RetryWithConfig(ctx, func(ctx context.Context) (int, error) {
		return 0, errRetry
	}, 2, 10*time.Millisecond, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetryWithConfig_PerCallTimeout(t *testing.T) {
	ctx := context.Background()
	_, err := RetryWithConfig(ctx, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 1, nil
		}
	}, 2, 10*time.Millisecond, 100*time.Millisecond,
		TimeoutOpt{PerCallTimeout: 50 * time.Millisecond})
	if err == nil {
		t.Fatal("expected error from per-call timeout")
	}
}

func TestRetryWithConfig_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	_, err := RetryWithConfig(ctx, func(ctx context.Context) (int, error) {
		panic("config panic")
	}, 1, 10*time.Millisecond, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected panic error")
	}
}

func TestRetryWithConfig_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := RetryWithConfig(ctx, func(ctx context.Context) (int, error) {
		return 0, errRetry
	}, 5, 200*time.Millisecond, 2*time.Second)
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestRetryWithConfigVoid_Success(t *testing.T) {
	ctx := context.Background()
	err := RetryWithConfigVoid(ctx, func(ctx context.Context) error {
		return nil
	}, 3, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryWithConfigVoid_Exhausted(t *testing.T) {
	ctx := context.Background()
	err := RetryWithConfigVoid(ctx, func(ctx context.Context) error {
		return errRetry
	}, 2, 10*time.Millisecond, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetryWithConfigVoid_PerCallTimeout(t *testing.T) {
	ctx := context.Background()
	err := RetryWithConfigVoid(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return nil
		}
	}, 1, 10*time.Millisecond, 100*time.Millisecond,
		TimeoutOpt{PerCallTimeout: 50 * time.Millisecond})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestRetryWithConfig_DeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := RetryWithConfig(ctx, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 1, nil
		}
	}, 3, 10*time.Millisecond, 100*time.Millisecond,
		TimeoutOpt{PerCallTimeout: 200 * time.Millisecond})
	if err == nil {
		t.Fatal("expected deadline exceeded")
	}
}

// ==================== WithTimeout / WithTimeoutVoid ====================

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

func TestWithTimeoutVoid_Success(t *testing.T) {
	ctx := context.Background()
	err := WithTimeoutVoid(ctx, 5*time.Second, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWithTimeoutVoid_Timeout(t *testing.T) {
	ctx := context.Background()
	err := WithTimeoutVoid(ctx, 50*time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// ==================== WithDeadline / WithDeadlineVoid ====================

func TestWithDeadline_Success(t *testing.T) {
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

func TestWithDeadline_Exceeded(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(50 * time.Millisecond)
	_, err := WithDeadline(ctx, deadline, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 1, nil
		}
	})
	if err == nil {
		t.Fatal("expected deadline exceeded")
	}
}

func TestWithDeadlineVoid_Success(t *testing.T) {
	ctx := context.Background()
	err := WithDeadlineVoid(ctx, time.Now().Add(5*time.Second), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWithDeadlineVoid_Exceeded(t *testing.T) {
	ctx := context.Background()
	err := WithDeadlineVoid(ctx, time.Now().Add(50*time.Millisecond), func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err == nil {
		t.Fatal("expected deadline exceeded")
	}
}

// ==================== RetryFn ====================

func TestRetryFn_WithRetry(t *testing.T) {
	var attempts atomic.Int32
	err := RetryFn(func() error {
		attempts.Add(1)
		if attempts.Load() < 3 {
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

func TestRetryFn_PanicRecovery(t *testing.T) {
	err := RetryFn(func() error {
		panic("retryfn panic")
	}).WithRetry(2)
	if err == nil {
		t.Fatal("expected panic error")
	}
}

func TestRetryFn_ZeroRetry(t *testing.T) {
	err := RetryFn(func() error {
		return nil
	}).WithRetry(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryFn_ZeroRetryFail(t *testing.T) {
	err := RetryFn(func() error {
		return errRetry
	}).WithRetry(0)
	if err == nil {
		t.Fatal("expected error with 0 retries")
	}
}

// ==================== BindRetryToWorker ====================

type mockWorkerBackend struct {
	submitFn func(ctx context.Context, fn func(ctx context.Context) error) error
}

func (m *mockWorkerBackend) Submit(ctx context.Context, fn func(ctx context.Context) error) error {
	return m.submitFn(ctx, fn)
}

func TestBindRetryToWorker_Success(t *testing.T) {
	backend := &mockWorkerBackend{
		submitFn: func(ctx context.Context, fn func(ctx context.Context) error) error {
			return nil
		},
	}
	ctx := context.Background()
	err := BindRetryToWorker(ctx, backend, func(ctx context.Context) error {
		return nil
	}, 3, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBindRetryToWorker_SubmitTimeout(t *testing.T) {
	var callCount atomic.Int32
	backend := &mockWorkerBackend{
		submitFn: func(ctx context.Context, fn func(ctx context.Context) error) error {
			callCount.Add(1)
			if callCount.Load() < 3 {
				return core.ErrSubmitTimeout
			}
			return nil
		},
	}
	ctx := context.Background()
	err := BindRetryToWorker(ctx, backend, func(ctx context.Context) error {
		return nil
	}, 5, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBindRetryToWorker_SubmitTimeoutExhausted(t *testing.T) {
	backend := &mockWorkerBackend{
		submitFn: func(ctx context.Context, fn func(ctx context.Context) error) error {
			return core.ErrSubmitTimeout
		},
	}
	ctx := context.Background()
	err := BindRetryToWorker(ctx, backend, func(ctx context.Context) error {
		return nil
	}, 2, 10*time.Millisecond, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBindRetryToWorker_NonSubmitError(t *testing.T) {
	otherErr := errors.New("other error")
	backend := &mockWorkerBackend{
		submitFn: func(ctx context.Context, fn func(ctx context.Context) error) error {
			return otherErr
		},
	}
	ctx := context.Background()
	err := BindRetryToWorker(ctx, backend, func(ctx context.Context) error {
		return nil
	}, 3, 10*time.Millisecond, 100*time.Millisecond)
	if !errors.Is(err, otherErr) {
		t.Fatalf("expected otherErr, got %v", err)
	}
}

func TestBindRetryToWorker_ContextCancel(t *testing.T) {
	backend := &mockWorkerBackend{
		submitFn: func(ctx context.Context, fn func(ctx context.Context) error) error {
			return core.ErrSubmitTimeout
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := BindRetryToWorker(ctx, backend, func(ctx context.Context) error {
		return nil
	}, 10, 200*time.Millisecond, 2*time.Second)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

// ==================== computeBackoff ====================

func TestComputeBackoff_Zero(t *testing.T) {
	b := computeBackoff(0, 0, 100*time.Millisecond)
	if b != 0 {
		t.Errorf("expected 0, got %v", b)
	}
}

func TestComputeBackoff_Exponential(t *testing.T) {
	b := computeBackoff(2, 100*time.Millisecond, 10*time.Second)
	if b != 400*time.Millisecond {
		t.Errorf("expected 400ms, got %v", b)
	}
}

func TestComputeBackoff_Max(t *testing.T) {
	b := computeBackoff(10, time.Second, 5*time.Second)
	if b != 5*time.Second {
		t.Errorf("expected 5s max, got %v", b)
	}
}

func TestComputeBackoff_MaxZero(t *testing.T) {
	b := computeBackoff(10, 100*time.Millisecond, 0)
	if b != 102400*time.Millisecond {
		t.Errorf("expected 102400ms (no cap), got %v", b)
	}
}

// ==================== RetryChain - Basic ====================

func TestRetryChain_ExponentialExecute(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		Execute(func(ctx context.Context) (int, error) {
			return 42, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestRetryChain_ExponentialExhausted(t *testing.T) {
	ctx := context.Background()
	_, err := New[int](ctx).Exponential().MaxRetries(2).Backoff(10*time.Millisecond, 50*time.Millisecond).
		Execute(func(ctx context.Context) (int, error) {
			return 0, errRetry
		})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetryChain_LinearExecute(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Linear().MaxRetries(3).Backoff(10*time.Millisecond, 0).
		Execute(func(ctx context.Context) (int, error) {
			return 123, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 123 {
		t.Fatalf("expected 123, got %d", val)
	}
}

func TestRetryChain_RetryThenSuccess(t *testing.T) {
	ctx := context.Background()
	var attempts atomic.Int32
	val, err := New[int](ctx).Exponential().MaxRetries(5).Backoff(10*time.Millisecond, 100*time.Millisecond).
		Execute(func(ctx context.Context) (int, error) {
			attempts.Add(1)
			if attempts.Load() < 3 {
				return 0, errRetry
			}
			return 99, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}

func TestRetryChain_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := New[int](ctx).Exponential().MaxRetries(10).Backoff(200*time.Millisecond, 2*time.Second).
		Execute(func(ctx context.Context) (int, error) {
			return 0, errRetry
		})
	if err == nil {
		t.Fatal("expected cancel error")
	}
}

func TestRetryChain_ExecuteVoid(t *testing.T) {
	ctx := context.Background()
	err := NewVoid(ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		ExecuteVoid(func(ctx context.Context) error {
			return nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestRetryChain_ExecuteVoid_Error(t *testing.T) {
	ctx := context.Background()
	err := NewVoid(ctx).Exponential().MaxRetries(2).Backoff(10*time.Millisecond, 50*time.Millisecond).
		ExecuteVoid(func(ctx context.Context) error {
			return errRetry
		})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetryChain_Run(t *testing.T) {
	ctx := context.Background()
	err := NewVoid(ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		Run(func() error {
			return nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestRetryChain_Run_Error(t *testing.T) {
	ctx := context.Background()
	err := NewVoid(ctx).Exponential().MaxRetries(2).Backoff(10*time.Millisecond, 50*time.Millisecond).
		Run(func() error {
			return errRetry
		})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetryChain_RunVoid(t *testing.T) {
	ctx := context.Background()
	err := NewVoid(ctx).Linear().MaxRetries(3).Backoff(10*time.Millisecond, 0).
		RunVoid(func(ctx context.Context) error {
			return nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestRetryChain_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	_, err := New[int](ctx).Exponential().MaxRetries(1).Backoff(10*time.Millisecond, 100*time.Millisecond).
		Execute(func(ctx context.Context) (int, error) {
			panic("chain panic")
		})
	if err == nil {
		t.Fatal("expected panic error")
	}
}

func TestRetryChain_PerCallTimeout(t *testing.T) {
	ctx := context.Background()
	_, err := New[int](ctx).Exponential().MaxRetries(2).Backoff(10*time.Millisecond, 100*time.Millisecond).
		PerCallTimeout(50 * time.Millisecond).
		Execute(func(ctx context.Context) (int, error) {
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

func TestRetryChain_Context(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).Exponential().MaxRetries(1).Backoff(10*time.Millisecond, 100*time.Millisecond)
	c.Context(context.Background())
}

// ==================== RetryChain - Config Methods ====================

func TestRetryChain_DefaultConfig(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Linear().MaxRetries(10).Backoff(time.Second, 0).
		DefaultConfig().MaxRetries(0).
		Execute(func(ctx context.Context) (int, error) {
			return 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestRetryChain_DefaultMaxRetries(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).MaxRetries(10).DefaultMaxRetries()
	if c.maxRetries != 3 {
		t.Errorf("maxRetries = %d, want 3", c.maxRetries)
	}
}

func TestRetryChain_DefaultBackoff(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).Backoff(time.Second, time.Minute).DefaultBackoff()
	if c.initialBackoff != 100*time.Millisecond {
		t.Errorf("initialBackoff = %v, want 100ms", c.initialBackoff)
	}
	if c.maxBackoff != 30*time.Second {
		t.Errorf("maxBackoff = %v, want 30s", c.maxBackoff)
	}
}

func TestRetryChain_DefaultPerCallTimeout(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).PerCallTimeout(time.Second).DefaultPerCallTimeout()
	if c.perCallTimeout != 0 {
		t.Errorf("perCallTimeout = %v, want 0", c.perCallTimeout)
	}
}

func TestRetryChain_NegativeMaxRetries(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).MaxRetries(-1).
		Execute(func(ctx context.Context) (int, error) {
			return 42, nil
		})
	if err != nil {
		t.Fatalf("negative maxRetries: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

// ==================== RetryChain - RateLimiter ====================

func TestRetryChain_RateLimiter(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		RateLimiter().Rate(100).Per(time.Second).
		Execute(func(ctx context.Context) (int, error) {
			return 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestRetryChain_RateLimiterWithBurst(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		RateLimiter().Rate(10).Per(time.Second).Burst(20).
		Execute(func(ctx context.Context) (int, error) {
			return 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestRetryChain_RateLimiterSharded(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		RateLimiter().Rate(100).Per(time.Second).Shards(4).
		Execute(func(ctx context.Context) (int, error) {
			return 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

// ==================== RetryChain - TokenBucket ====================

func TestRetryChain_TokenBucket(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		TokenBucket().Rate(10).Capacity(20).
		Execute(func(ctx context.Context) (int, error) {
			return 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestRetryChain_TokenBucketSharded(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		TokenBucket().Rate(10).Capacity(20).Shards(4).
		Execute(func(ctx context.Context) (int, error) {
			return 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

// ==================== RetryChain - SlidingWindow ====================

func TestRetryChain_SlidingWindow(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		SlidingWindow().Limit(100).Window(time.Second).
		Execute(func(ctx context.Context) (int, error) {
			return 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestRetryChain_SlidingWindowSharded(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		SlidingWindow().Limit(100).Window(time.Second).Shards(4).
		Execute(func(ctx context.Context) (int, error) {
			return 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

// ==================== RetryChain - Adaptive ====================

func TestRetryChain_Adaptive(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		Adaptive().MinWorker(5).MaxWorker(100).
		Execute(func(ctx context.Context) (int, error) {
			return 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestRetryChain_AdaptiveSharded(t *testing.T) {
	ctx := context.Background()
	val, err := New[int](ctx).Exponential().MaxRetries(3).Backoff(10*time.Millisecond, 100*time.Millisecond).
		Adaptive().MinWorker(5).MaxWorker(100).Shards(4).
		Execute(func(ctx context.Context) (int, error) {
			return 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

// ==================== RetryChain - Default Limiter Methods ====================

func TestRetryChain_DefaultRate(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).Rate(50).DefaultRate()
	if c.rlRate != 0 {
		t.Errorf("rlRate = %d, want 0", c.rlRate)
	}
}

func TestRetryChain_DefaultPer(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).Per(time.Minute).DefaultPer()
	if c.rlPer != 0 {
		t.Errorf("rlPer = %v, want 0", c.rlPer)
	}
}

func TestRetryChain_DefaultBurst(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).Burst(100).DefaultBurst()
	if c.rlBurst != 0 {
		t.Errorf("rlBurst = %d, want 0", c.rlBurst)
	}
}

func TestRetryChain_DefaultCapacity(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).Capacity(100).DefaultCapacity()
	if c.tbCapacity != 0 {
		t.Errorf("tbCapacity = %v, want 0", c.tbCapacity)
	}
}

func TestRetryChain_DefaultLimit(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).Limit(200).DefaultLimit()
	if c.swLimit != 0 {
		t.Errorf("swLimit = %d, want 0", c.swLimit)
	}
}

func TestRetryChain_DefaultWindow(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).Window(time.Minute).DefaultWindow()
	if c.swWindow != 0 {
		t.Errorf("swWindow = %v, want 0", c.swWindow)
	}
}

func TestRetryChain_DefaultMinWorker(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).MinWorker(10).DefaultMinWorker()
	if c.adMinRate != 0 {
		t.Errorf("adMinRate = %d, want 0", c.adMinRate)
	}
}

func TestRetryChain_DefaultMaxWorker(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).MaxWorker(200).DefaultMaxWorker()
	if c.adMaxRate != 0 {
		t.Errorf("adMaxRate = %d, want 0", c.adMaxRate)
	}
}

func TestRetryChain_DefaultShards(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).Shards(16).DefaultShards()
	if c.rlShards != 0 || c.swShards != 0 || c.adShards != 0 {
		t.Errorf("shards not zero")
	}
}

// ==================== RetryChain - Logger ====================

func TestRetryChain_Logger(t *testing.T) {
	ctx := context.Background()
	c := New[int](ctx).Logger(nil)
	c.DefaultLogger()
}

// ==================== Concurrent Tests ====================

func TestRetry_Backoff_Concurrent(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			var success, fail atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
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
			total := success.Load() + fail.Load()
			if total != int64(tier.Size) {
				t.Fatalf("total mismatch: %d != %d", total, tier.Size)
			}
		})
	}
}

func TestRetry_LinearBackoff_Concurrent(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			var total atomic.Int64
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
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
			if total.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, total.Load())
			}
		})
	}
}

func TestRetryFn_Concurrent(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					_ = RetryFn(func() error { return nil }).WithRetry(1)
				}()
			}
			wg.Wait()
		})
	}
}

func TestRetry_TimedCancellation_Concurrent(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
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
		})
	}
}

func TestRetry_WithConfig_Concurrent(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					ctx := context.Background()
					_, _ = RetryWithConfig(ctx, func(ctx context.Context) (int, error) {
						return 1, nil
					}, 2, time.Millisecond, 10*time.Millisecond)
				}()
			}
			wg.Wait()
		})
	}
}

func TestRetry_WithTimeout_Concurrent(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)

			var wg sync.WaitGroup
			wg.Add(tier.Size)
			for i := 0; i < tier.Size; i++ {
				go func() {
					defer wg.Done()
					ctx := context.Background()
					_, _ = WithTimeout(ctx, time.Second, func(ctx context.Context) (int, error) {
						return 1, nil
					})
				}()
			}
			wg.Wait()
		})
	}
}

func TestRetryChain_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	var success atomic.Int64
	n := 200
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, err := New[int](ctx).Exponential().MaxRetries(2).Backoff(10*time.Millisecond, 50*time.Millisecond).
				Execute(func(ctx context.Context) (int, error) {
					return 1, nil
				})
			if err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() != int64(n) {
		t.Errorf("success = %d, want %d", success.Load(), n)
	}
}

// ==================== BindRetryToWorker Concurrent ====================

func TestBindRetryToWorker_Concurrent(t *testing.T) {
	backend := &mockWorkerBackend{
		submitFn: func(ctx context.Context, fn func(ctx context.Context) error) error {
			return nil
		},
	}
	var wg sync.WaitGroup
	n := 500
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_ = BindRetryToWorker(ctx, backend, func(ctx context.Context) error {
				return nil
			}, 2, 10*time.Millisecond, 50*time.Millisecond)
		}()
	}
	wg.Wait()
}

// ==================== Race Tests ====================

func TestRace_Retry_Backoff_CancelRace(t *testing.T) {
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(100 * time.Microsecond)
					cancel()
				}()
				_, _ = RetryWithBackoff(ctx, func(ctx context.Context) (int, error) {
					return 0, errRetry
				}, 5, time.Millisecond, 50*time.Millisecond)
			}()
		}
		wg.Wait()
	}
}

func TestRace_RetryWithConfig_BackoffRace(t *testing.T) {
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ctx := context.Background()
				_, _ = RetryWithConfig(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				}, 3, time.Microsecond, time.Millisecond)
			}()
		}
		wg.Wait()
	}
}

func TestRace_RetryChain_CancelRace(t *testing.T) {
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(100 * time.Microsecond)
					cancel()
				}()
				_, _ = New[int](ctx).Exponential().MaxRetries(5).Backoff(time.Millisecond, 50*time.Millisecond).
					Execute(func(ctx context.Context) (int, error) {
						return 0, errRetry
					})
			}()
		}
		wg.Wait()
	}
}

func TestRace_RetryChain_RateLimiterRace(t *testing.T) {
	var wg sync.WaitGroup
	n := 200
	var success atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, err := New[int](ctx).Exponential().MaxRetries(1).Backoff(time.Millisecond, 10*time.Millisecond).
				RateLimiter().Rate(100).Per(time.Second).Shards(8).
				Execute(func(ctx context.Context) (int, error) { return 1, nil })
			if err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	_ = success.Load()
}

func TestRace_RetryChain_TokenBucketRace(t *testing.T) {
	var wg sync.WaitGroup
	n := 200
	var success atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, err := New[int](ctx).Exponential().MaxRetries(1).Backoff(time.Millisecond, 10*time.Millisecond).
				TokenBucket().Rate(100).Capacity(200).Shards(8).
				Execute(func(ctx context.Context) (int, error) { return 1, nil })
			if err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	_ = success.Load()
}

func TestRace_RetryChain_SlidingWindowRace(t *testing.T) {
	var wg sync.WaitGroup
	n := 200
	var success atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, err := New[int](ctx).Exponential().MaxRetries(1).Backoff(time.Millisecond, 10*time.Millisecond).
				SlidingWindow().Limit(200).Window(time.Second).Shards(8).
				Execute(func(ctx context.Context) (int, error) { return 1, nil })
			if err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	_ = success.Load()
}

func TestRace_RetryChain_AdaptiveRace(t *testing.T) {
	var wg sync.WaitGroup
	n := 200
	var success atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, err := New[int](ctx).Exponential().MaxRetries(1).Backoff(time.Millisecond, 10*time.Millisecond).
				Adaptive().MinWorker(5).MaxWorker(100).Shards(8).
				Execute(func(ctx context.Context) (int, error) { return 1, nil })
			if err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	_ = success.Load()
}

func TestRace_WithTimeout_Concurrent(t *testing.T) {
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ctx := context.Background()
				_, _ = WithTimeout(ctx, 500*time.Millisecond, func(ctx context.Context) (int, error) {
					return 1, nil
				})
			}()
		}
		wg.Wait()
	}
}
