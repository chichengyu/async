package task

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// TaskBuilder 泛型异步任务链式构建器，统一入口为 async.Task[T]()。
// 支持链式配置上下文、超时、BoundedRunner 限流，终端方法启动异步任务。
//
// 使用示例：
//
//	// 基础异步任务
//	ar := async.Task[int]().Context(ctx).Go(func(ctx context.Context) (int, error) {
//	    return compute(ctx)
//	})
//	val, err := ar.Wait()
//
//	// 带超时
//	ar := async.Task[int]().Context(ctx).WithTimeout(5*time.Second).Go(fn)
//
//	// 带限流
//	ar := async.Task[int]().Context(ctx).Bounded(1000).Go(fn)
//
//	// 可取消任务
//	t := async.Task[int]().Context(ctx).GoResult(func(ctx context.Context) (int, error) {
//	    return longRunning(ctx)
//	})
//	t.Cancel()
//	val, err := t.Result()
type TaskBuilder[T any] struct {
	ctx     context.Context
	timeout time.Duration
	bounded *BoundedRunner
}

// NewTaskBuilder 创建任务构建器，默认使用 context.Background()。
// 通过 .Context(ctx) 链式设置上下文。
func NewTaskBuilder[T any]() *TaskBuilder[T] {
	return &TaskBuilder[T]{ctx: context.Background()}
}

// Context 链式设置上下文，自动注入 trace_id。
func (b *TaskBuilder[T]) Context(ctx context.Context) *TaskBuilder[T] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

// WithTimeout 设置任务超时。超时 context 会注入到 fn 的 ctx 参数中。
func (b *TaskBuilder[T]) WithTimeout(d time.Duration) *TaskBuilder[T] {
	b.timeout = d
	return b
}

// DefaultTimeout 清除超时设置。
func (b *TaskBuilder[T]) DefaultTimeout() *TaskBuilder[T] {
	b.timeout = 0
	return b
}

// Bounded 使用 BoundedRunner 限制并发 goroutine 数。
// max 为最大并发数，<=0 时使用默认 IO 并发度。
func (b *TaskBuilder[T]) Bounded(max int) *TaskBuilder[T] {
	b.bounded = NewBoundedRunner(max)
	return b
}

// DefaultBounded 清除 BoundedRunner 限制。
func (b *TaskBuilder[T]) DefaultBounded() *TaskBuilder[T] {
	b.bounded = nil
	return b
}

// Logger 注入自定义日志实现，全局生效。
func (b *TaskBuilder[T]) Logger(l core.Logger) *TaskBuilder[T] { core.SetLogger(l); return b }

// DefaultLogger 重置为默认日志实现。
func (b *TaskBuilder[T]) DefaultLogger() *TaskBuilder[T] { core.SetLogger(nil); return b }

// ── 终端方法 ──

// Go 启动异步任务，返回 AsyncResult[T]。
// 通过 Wait() 获取结果，不支持外部取消。
func (b *TaskBuilder[T]) Go(fn func(context.Context) (T, error)) *AsyncResult[T] {
	fn = b.wrapTimeout(fn)
	if b.bounded != nil {
		return BoundedGo(b.bounded, b.ctx, fn)
	}
	return Go(b.ctx, fn)
}

// GoResult 启动可取消的异步任务，返回 Task[T]。
// 通过 Task.Cancel() 可主动取消，通过 Task.Result() 获取结果。
func (b *TaskBuilder[T]) GoResult(fn func(context.Context) (T, error)) Task[T] {
	fn = b.wrapTimeout(fn)
	if b.bounded != nil {
		return BoundedGoResult(b.bounded, b.ctx, fn)
	}
	return GoResult(b.ctx, fn)
}

// GoAct 启动无返回值异步任务，返回 AsyncResult[NoResult]。
// fn 签名为 func(ctx) error。
func (b *TaskBuilder[T]) GoAct(fn func(context.Context) error) *AsyncResult[NoResult] {
	fn = b.wrapTimeoutAct(fn)
	if b.bounded != nil {
		return BoundedGoAct(b.bounded, b.ctx, fn)
	}
	return GoAct(b.ctx, fn)
}

// GoResultAct 启动无返回值的可取消异步任务，返回 Task[NoResult]。
// fn 签名为 func(ctx) error，可通过 Task.Cancel() 主动取消。
func (b *TaskBuilder[T]) GoResultAct(fn func(context.Context) error) Task[NoResult] {
	fn = b.wrapTimeoutAct(fn)
	if b.bounded != nil {
		return boundedGoResultAct(b.bounded, b.ctx, fn)
	}
	return GoResultAct(b.ctx, fn)
}

// ── 内部辅助方法 ──

// wrapTimeout 将超时包裹到 fn 闭包内部，确保超时 context 生命周期与 fn 执行周期一致。
func (b *TaskBuilder[T]) wrapTimeout(fn func(context.Context) (T, error)) func(context.Context) (T, error) {
	if b.timeout <= 0 {
		return fn
	}
	timeout := b.timeout
	return func(ctx context.Context) (T, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return fn(ctx)
	}
}

// wrapTimeoutAct 无返回值版本的超时包裹。
func (b *TaskBuilder[T]) wrapTimeoutAct(fn func(context.Context) error) func(context.Context) error {
	if b.timeout <= 0 {
		return fn
	}
	timeout := b.timeout
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return fn(ctx)
	}
}

// boundedGoResultAct 带限流的无返回值可取消异步任务。
func boundedGoResultAct(r *BoundedRunner, ctx context.Context, fn func(context.Context) error) Task[NoResult] {
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		var zero NoResult
		ctx, cancel := context.WithCancel(ctx)
		return Task[NoResult]{
			Ctx:    ctx,
			Cancel: cancel,
			Result: func() (NoResult, error) { return zero, ctx.Err() },
		}
	}

	return GoResultAct(ctx, func(ctx context.Context) error {
		defer func() { <-r.sem }()
		return fn(ctx)
	})
}
