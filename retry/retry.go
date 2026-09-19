// Package retry 提供重试策略和超时控制。
//
// 核心功能：
//   - 指数退避重试：RetryWithBackoff / RetryWithBackoffVoid / RetryWithBackoffResult
//   - 线性退避重试：RetryWithLinearBackoff / RetryWithLinearBackoffVoid / RetryWithLinearBackoffResult
//   - 可配置重试（支持每次调用超时）：RetryWithConfig / RetryWithConfigVoid
//   - 超时与截止时间包装：WithTimeout / WithTimeoutVoid / WithDeadline / WithDeadlineVoid
//   - 简单函数式重试：RetryFn.WithRetry(maxRetries)
//   - Worker 绑定重试：BindRetryToWorker
//
// 退避算法：backoff = min(initialBackoff * 2^attempt, maxBackoff)
//
// 使用示例：
//
//	// 指数退避重试：最多重试3次，初始退避100ms，最大退避5s
//	val, err := retry.RetryWithBackoff(ctx, fn, 3, 100*time.Millisecond, 5*time.Second)
//
//	// 带每次调用超时的重试
//	val, err := retry.RetryWithConfig(ctx, fn, 3, 100*time.Millisecond, 5*time.Second,
//	    retry.TimeoutOpt{PerCallTimeout: 2 * time.Second})
//
//	// 简单函数式重试
//	err := retry.RetryFn(func() error { return doSomething() }).WithRetry(3)
//
//	// 给函数加超时
//	val, err := retry.WithTimeout(ctx, 5*time.Second, fn)
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/chichengyu/async/core"
)

// RetryWithBackoff 使用指数退避策略执行 fn，最多执行 maxRetries+1 次。
// 退避时间计算公式：min(initialBackoff * 2^attempt, maxBackoff)。
// 如果 initialBackoff 为 0，则不等待直接重试。
//
// 参数：
//   - ctx：上下文，取消后终止重试
//   - fn：要执行的函数
//   - maxRetries：最大重试次数（总执行次数 = maxRetries + 1）
//   - initialBackoff：初始退避时间
//   - maxBackoff：最大退避时间上限
//
// 使用示例：
//
//	// 调用 RPC，最多重试3次（共4次尝试），退避从100ms开始指数增长到最多5s
//	result, err := retry.RetryWithBackoff(ctx, func(ctx context.Context) (*Response, error) {
//	    return rpcClient.Call(ctx, request)
//	}, 3, 100*time.Millisecond, 5*time.Second)
func RetryWithBackoff[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
) (T, error) {
	var zero T
	for attempt := 0; attempt <= maxRetries; attempt++ {
		val, err := invokeSafely(ctx, fn)
		if err == nil {
			return val, nil
		}
		if attempt == maxRetries {
			return zero, fmt.Errorf("retry exhausted after %d attempts: %w", maxRetries+1, err)
		}
		core.LogTaskFail(ctx, err, fmt.Sprintf("retry attempt %d/%d failed", attempt+1, maxRetries+1))
		if backoff := computeBackoff(attempt, initialBackoff, maxBackoff); backoff > 0 {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	panic("unreachable")
}

// RetryWithBackoffVoid 与 RetryWithBackoff 相同，但 fn 只返回 error。
//
// 使用示例：
//
//	// 重试发送消息
//	err := retry.RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
//	    return kafkaProducer.Send(ctx, msg)
//	}, 3, 100*time.Millisecond, 5*time.Second)
func RetryWithBackoffVoid(
	ctx context.Context,
	fn func(ctx context.Context) error,
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
) error {
	_, err := RetryWithBackoff(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, maxRetries, initialBackoff, maxBackoff)
	return err
}

// RetryWithBackoffResult 与 RetryWithBackoff 相同，但返回 Result[T] 而非两个返回值。
//
// 使用示例：
//
//	// 返回 Result 类型便于统一处理
//	r := retry.RetryWithBackoffResult(ctx, fn, 3, 100*time.Millisecond, 5*time.Second)
//	if !r.Ok() {
//	    log.Printf("重试失败: %v", r.Err)
//	}
func RetryWithBackoffResult[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
) core.Result[T] {
	val, err := RetryWithBackoff(ctx, fn, maxRetries, initialBackoff, maxBackoff)
	return core.Result[T]{Value: val, Err: err}
}

// RetryWithLinearBackoff 使用线性退避策略（固定退避时间）执行 fn。
// 每次重试等待相同的 backoff 时间。
//
// 使用示例：
//
//	// 每次重试等1秒，最多重试5次
//	val, err := retry.RetryWithLinearBackoff(ctx, fn, 5, 1*time.Second)
func RetryWithLinearBackoff[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	backoff time.Duration,
) (T, error) {
	return RetryWithBackoff(ctx, fn, maxRetries, backoff, backoff)
}

// RetryWithLinearBackoffVoid 与 RetryWithLinearBackoff 相同，但 fn 只返回 error。
func RetryWithLinearBackoffVoid(
	ctx context.Context,
	fn func(ctx context.Context) error,
	maxRetries int,
	backoff time.Duration,
) error {
	_, err := RetryWithBackoff(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, maxRetries, backoff, backoff)
	return err
}

// RetryWithLinearBackoffResult 与 RetryWithLinearBackoff 相同，但返回 Result[T]。
func RetryWithLinearBackoffResult[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	backoff time.Duration,
) core.Result[T] {
	val, err := RetryWithLinearBackoff(ctx, fn, maxRetries, backoff)
	return core.Result[T]{Value: val, Err: err}
}

// WithTimeout 包装 fn，使其在指定超时后自动取消。
//
// 使用示例：
//
//	// 单个调用最多3秒
//	val, err := retry.WithTimeout(ctx, 3*time.Second, func(ctx context.Context) (string, error) {
//	    return httpGet(ctx, url)
//	})
func WithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(ctx context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(ctx)
}

// WithTimeoutVoid 与 WithTimeout 相同，但 fn 只返回 error。
func WithTimeoutVoid(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(ctx)
}

// WithDeadline 包装 fn，使其在指定截止时间后自动取消。
//
// 使用示例：
//
//	// 必须在 5 秒后之前完成
//	deadline := time.Now().Add(5 * time.Second)
//	val, err := retry.WithDeadline(ctx, deadline, fn)
func WithDeadline[T any](ctx context.Context, deadline time.Time, fn func(ctx context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return fn(ctx)
}

// WithDeadlineVoid 与 WithDeadline 相同，但 fn 只返回 error。
func WithDeadlineVoid(ctx context.Context, deadline time.Time, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return fn(ctx)
}

// TimeoutOpt 用于 RetryWithConfig 的每次调用超时配置。
//
// 使用示例：
//
//	retry.RetryWithConfig(ctx, fn, 3, 100*time.Millisecond, 5*time.Second,
//	    retry.TimeoutOpt{PerCallTimeout: 2 * time.Second})
type TimeoutOpt struct {
	PerCallTimeout time.Duration // 每次调用（含重试中的每次尝试）的超时时间
}

// RetryWithConfig 支持每次调用超时的指数退避重试。
// 相比 RetryWithBackoff，增加了对每次 fn 调用的超时控制，
// 并且会区分 context.DeadlineExceeded 和 context.Canceled 错误（这两种错误不重试）。
//
// 使用示例：
//
//	// 每次调用最多2秒，最多重试3次，退避100ms到5s
//	result, err := retry.RetryWithConfig(ctx, func(ctx context.Context) (*Data, error) {
//	    return fetchData(ctx, id)
//	}, 3, 100*time.Millisecond, 5*time.Second,
//	    retry.TimeoutOpt{PerCallTimeout: 2 * time.Second})
//	if err != nil {
//	    // 可能的重试耗尽错误
//	}
func RetryWithConfig[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	opts ...TimeoutOpt,
) (T, error) {
	var zero T
	perCallTimeout := time.Duration(0)
	if len(opts) > 0 && opts[0].PerCallTimeout > 0 {
		perCallTimeout = opts[0].PerCallTimeout
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		var val T
		var err error
		if perCallTimeout > 0 {
			var cancel context.CancelFunc
			var timeoutCtx context.Context
			timeoutCtx, cancel = context.WithTimeout(ctx, perCallTimeout)
			val, err = invokeSafely(timeoutCtx, fn)
			cancel()
		} else {
			val, err = invokeSafely(ctx, fn)
		}

		if err == nil {
			return val, nil
		}

		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return zero, err
		}

		if attempt == maxRetries {
			return zero, fmt.Errorf("retry exhausted after %d attempts: %w", maxRetries+1, err)
		}

		core.LogTaskFail(ctx, err, fmt.Sprintf("retry attempt %d/%d failed", attempt+1, maxRetries+1))

		if backoff := computeBackoff(attempt, initialBackoff, maxBackoff); backoff > 0 {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	panic("unreachable")
}

// RetryWithConfigVoid 与 RetryWithConfig 相同，但 fn 只返回 error。
func RetryWithConfigVoid(
	ctx context.Context,
	fn func(ctx context.Context) error,
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	opts ...TimeoutOpt,
) error {
	_, err := RetryWithConfig(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, maxRetries, initialBackoff, maxBackoff, opts...)
	return err
}

// ──────────────────────────── helpers ────────────────────────────

func invokeSafely[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) (val T, err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := core.NewPanicError(r)
			core.LogCtxError(ctx, "async retry panic recovered",
				core.Any("panic", r),
				core.Bytes("stack", pe.Stack))
			err = pe
		}
	}()
	return fn(ctx)
}

var recoverSafely = invokeSafely[struct{}]

func computeBackoff(attempt int, initialBackoff time.Duration, maxBackoff time.Duration) time.Duration {
	if initialBackoff <= 0 {
		return 0
	}
	mul := math.Pow(2, float64(attempt))
	backoff := time.Duration(mul) * initialBackoff
	if maxBackoff > 0 && backoff > maxBackoff {
		backoff = maxBackoff
	}
	return backoff
}

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
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
			}
			continue
		}
		return err
	}
	return fmt.Errorf("bind retry exhausted: %w", lastErr)
}
