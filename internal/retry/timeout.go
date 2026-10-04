package retry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// WithTimeout 包装 fn，使其在指定超时后自动取消。
//
// 参数：
//   - ctx：父上下文
//   - timeout：超时时间
//   - fn：要执行的函数
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
//
// 参数：
//   - ctx：父上下文
//   - timeout：超时时间
//   - fn：要执行的函数（只返回 error）
//
// 使用示例：
//
//	// 发送请求最多5秒
//	err := retry.WithTimeoutVoid(ctx, 5*time.Second, func(ctx context.Context) error {
//	    return kafkaProducer.Send(ctx, msg)
//	})
func WithTimeoutVoid(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(ctx)
}

// WithDeadline 包装 fn，使其在指定截止时间后自动取消。
//
// 参数：
//   - ctx：父上下文
//   - deadline：截止时间
//   - fn：要执行的函数
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
//
// 参数：
//   - ctx：父上下文
//   - deadline：截止时间
//   - fn：要执行的函数（只返回 error）
//
// 使用示例：
//
//	// 必须在截止时间前完成
//	err := retry.WithDeadlineVoid(ctx, time.Now().Add(30*time.Second), func(ctx context.Context) error {
//	    return batchProcess(ctx, items)
//	})
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
// 参数：
//   - ctx：上下文，取消后终止重试
//   - fn：要执行的函数
//   - maxRetries：最大重试次数
//   - initialBackoff：初始退避时间
//   - maxBackoff：最大退避时间上限
//   - opts：可选的 TimeoutOpt{PerCallTimeout}，设置每次调用的超时
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
			if ctx.Err() != nil {
				return zero, err
			}
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
//
// 参数：
//   - ctx：上下文
//   - fn：要执行的函数（只返回 error）
//   - maxRetries：最大重试次数
//   - initialBackoff：初始退避时间
//   - maxBackoff：最大退避时间上限
//   - opts：可选的 TimeoutOpt{PerCallTimeout}
//
// 使用示例：
//
//	// 带每次调用超时的重试
//	err := retry.RetryWithConfigVoid(ctx, func(ctx context.Context) error {
//	    return callExternalAPI(ctx, req)
//	}, 3, 100*time.Millisecond, 5*time.Second,
//	    retry.TimeoutOpt{PerCallTimeout: 2 * time.Second})
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
