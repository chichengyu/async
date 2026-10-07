package retry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ──────────────────────────── RetryFn helpers ────────────────────────────

// RetryFn 函数式重试辅助类型，支持方法链式调用。
// 将普通函数转换为 RetryFn 后直接调用 WithRetry。
//
// 使用示例：
//
//	// 简单重试：最多3次
//	err := retry.RetryFn(func() error {
//	    return doSomething()
//	}).WithRetry(3)
//
//	// 带 panic 保护的重试
//	err := retry.RetryFn(func() error {
//	    return riskyOperation()
//	}).WithRetry(2)
type RetryFn func() error

// WithRetry 执行 fn，最多执行 maxRetries+1 次，自动捕获 panic。
//
// 参数：
//   - maxRetries：最大重试次数
func (r RetryFn) WithRetry(maxRetries int) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := r.safeCall()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("retry exhausted: %w", lastErr)
}

func (r RetryFn) safeCall() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = core.NewPanicError(r)
		}
	}()
	return r()
}

// WorkerPoolBackend 是 Pool/Workers 的最小抽象接口，用于 BindRetryToWorker。
type WorkerPoolBackend interface {
	Submit(ctx context.Context, fn func(ctx context.Context) error) error
}

// BindRetryToWorker 向 worker 池提交任务，并在遇到 ErrSubmitTimeout 时自动重试。
// 适用于高负载场景下提交任务时池满需要重试的情况。
//
// 参数：
//   - ctx：上下文，取消后终止重试
//   - backend：WorkerPoolBackend 接口（Pool 实现此接口）
//   - fn：要提交执行的任务函数
//   - maxRetries：最大重试次数
//   - initialBackoff：初始退避时间
//   - maxBackoff：最大退避时间上限
//
// 使用示例：
//
//	// 向协程池提交任务，提交超时时自动退避重试
//	err := retry.BindRetryToWorker(ctx, pool, func(ctx context.Context) error {
//	    return processItem(ctx, item)
//	}, 3, 10*time.Millisecond, 1*time.Second)
func BindRetryToWorker(
	ctx context.Context,
	backend WorkerPoolBackend,
	fn func(ctx context.Context) error,
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := backend.Submit(ctx, fn)
		if err == nil {
			return nil
		}

		if errors.Is(err, core.ErrSubmitTimeout) {
			lastErr = err
			core.LogTaskFail(ctx, err, fmt.Sprintf("bind retry attempt %d/%d failed", attempt+1, maxRetries+1))
			if backoff := computeBackoff(attempt, initialBackoff, maxBackoff); backoff > 0 {
				timer := time.NewTimer(backoff)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
				}
			}
			continue
		}
		return err
	}
	return fmt.Errorf("bind retry exhausted: %w", lastErr)
}
