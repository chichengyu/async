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
	"fmt"
	"math"
	"time"

	"github.com/chichengyu/async/internal/core"
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
// 参数：
//   - ctx：上下文，取消后终止重试
//   - fn：要执行的函数（只返回 error）
//   - maxRetries：最大重试次数
//   - initialBackoff：初始退避时间
//   - maxBackoff：最大退避时间上限
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
// 参数：
//   - ctx：上下文
//   - fn：要执行的函数
//   - maxRetries：最大重试次数
//   - initialBackoff：初始退避时间
//   - maxBackoff：最大退避时间上限
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
// 参数：
//   - ctx：上下文，取消后终止重试
//   - fn：要执行的函数
//   - maxRetries：最大重试次数
//   - backoff：固定退避时间
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
//
// 参数：
//   - ctx：上下文
//   - fn：要执行的函数（只返回 error）
//   - maxRetries：最大重试次数
//   - backoff：固定退避时间
//
// 使用示例：
//
//	// 每次重试等500ms，最多重试3次
//	err := retry.RetryWithLinearBackoffVoid(ctx, func(ctx context.Context) error {
//	    return sendEmail(ctx, to, body)
//	}, 3, 500*time.Millisecond)
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
//
// 参数：
//   - ctx：上下文
//   - fn：要执行的函数
//   - maxRetries：最大重试次数
//   - backoff：固定退避时间
//
// 使用示例：
//
//	// 每次等1s，返回 Result 便于链式处理
//	r := retry.RetryWithLinearBackoffResult(ctx, fn, 5, 1*time.Second)
//	if r.Ok() {
//	    process(r.Value)
//	}
func RetryWithLinearBackoffResult[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	backoff time.Duration,
) core.Result[T] {
	val, err := RetryWithLinearBackoff(ctx, fn, maxRetries, backoff)
	return core.Result[T]{Value: val, Err: err}
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
