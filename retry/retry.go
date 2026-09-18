// Package retry 提供重试策略和超时控制。
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"time"

	"github.com/jxue/async/core"
	"github.com/rs/zerolog/log"
)

// RetryWithBackoff executes fn with exponential backoff, respecting maxRetries+1 total attempts.
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

// RetryWithBackoffVoid is RetryWithBackoff for fn returning only error.
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

// RetryWithBackoffResult is RetryWithBackoff returning Result[T].
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

// RetryWithLinearBackoff executes fn with linear backoff.
func RetryWithLinearBackoff[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	backoff time.Duration,
) (T, error) {
	return RetryWithBackoff(ctx, fn, maxRetries, backoff, backoff)
}

// RetryWithLinearBackoffVoid is RetryWithLinearBackoff for fn returning only error.
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

// RetryWithLinearBackoffResult is RetryWithLinearBackoff returning Result[T].
func RetryWithLinearBackoffResult[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	backoff time.Duration,
) core.Result[T] {
	val, err := RetryWithLinearBackoff(ctx, fn, maxRetries, backoff)
	return core.Result[T]{Value: val, Err: err}
}

// WithTimeout wraps fn with a context timeout.
func WithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(ctx context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(ctx)
}

// WithTimeoutVoid is WithTimeout for fn returning only error.
func WithTimeoutVoid(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(ctx)
}

// WithDeadline wraps fn with a context deadline.
func WithDeadline[T any](ctx context.Context, deadline time.Time, fn func(ctx context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return fn(ctx)
}

// WithDeadlineVoid is WithDeadline for fn returning only error.
func WithDeadlineVoid(ctx context.Context, deadline time.Time, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return fn(ctx)
}

// TimeoutOpt allows configuring per-call timeout within retry.
type TimeoutOpt struct {
	PerCallTimeout time.Duration
}

// RetryWithConfig supports per-call timeouts and configurable error hooks.
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

// RetryWithConfigVoid is RetryWithConfig for fn returning only error.
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
			log.Ctx(ctx).Error().
				Interface("panic", r).
				Bytes("stack", pe.Stack).
				Msg("async retry panic recovered")
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

// RetryFn calls fn up to maxRetries+1 times; panics are recovered and wrapped.
type RetryFn func() error

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
			err = &core.PanicError{
				Value: r,
				Stack: debug.Stack(),
			}
		}
	}()
	return r()
}

// WorkerPoolBackend is a minimal interface to abstract Pool/Workers for bindings.
type WorkerPoolBackend interface {
	Submit(ctx context.Context, fn func(ctx context.Context) error) error
}

// BindRetryToWorker submits fn to pool with retry on ErrSubmitTimeout.
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

func init() {
	debug.SetTraceback("system")
}
