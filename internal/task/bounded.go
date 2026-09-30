package task

import (
	"context"

	"github.com/chichengyu/async/internal/core"
)

// ──────────────────────────── BoundedRunner ────────────────────────────

// BoundedRunner 限制并发 goroutine 数的异步任务执行器。
// 通过内部信号量控制最大并发 goroutine 数，用 task.Go() 的 API 体验扛千万级高并发。
//
// 与裸 task.Go() 的区别：
//   - task.Go()：每次调用创建一个新 goroutine，1 千万调用 = 1 千万 goroutine
//   - BoundedRunner.Go()：最多同时运行 max 个 goroutine，其余阻塞等待，公平调度
//
// 使用示例：
//
//	// 直接创建
//	runner := task.NewBoundedRunner(1000)
//
//	// 链式创建
//	runner := task.NewBoundedRunnerBuilder().Max(1000).Build()
//
//	for i := 0; i < 10_000_000; i++ {
//	    idx := i
//	    task.BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
//	        return processData(ctx, idx)
//	    })
//	}
type BoundedRunner struct {
	sem chan struct{}
}

// BoundedRunnerBuilder 链式构建 BoundedRunner。
// 与 TaskBuilder.Bounded(max) 等价，提供独立入口用于提前创建复用。
//
// 使用示例：
//
//	runner := async.NewBoundedRunner().Max(1000).Build()
type BoundedRunnerBuilder struct {
	max int
}

// NewBoundedRunnerBuilder 创建 BoundedRunner 链式构建器。
func NewBoundedRunnerBuilder() *BoundedRunnerBuilder {
	return &BoundedRunnerBuilder{max: core.IO()}
}

// Max 设置最大并发 goroutine 数。<=0 使用默认 IO 并发度。
func (b *BoundedRunnerBuilder) Max(n int) *BoundedRunnerBuilder {
	b.max = n
	return b
}

// Build 创建 BoundedRunner 实例。
func (b *BoundedRunnerBuilder) Build() *BoundedRunner {
	return NewBoundedRunner(b.max)
}

// NewBoundedRunner 创建一个限制并发 goroutine 数的执行器。
// max <= 0 时使用默认 IO 并发度。
func NewBoundedRunner(max int) *BoundedRunner {
	if max <= 0 {
		max = core.IO()
	}
	return &BoundedRunner{sem: make(chan struct{}, max)}
}

// NewDefaultBoundedRunner 使用默认 IO 并发度创建执行器。
func NewDefaultBoundedRunner() *BoundedRunner {
	return NewBoundedRunner(core.IO())
}

// Max 返回最大并发 goroutine 数。
func (r *BoundedRunner) Max() int {
	return cap(r.sem)
}

// Available 返回当前可用的并发槽位数。
func (r *BoundedRunner) Available() int {
	return cap(r.sem) - len(r.sem)
}

// Busy 返回当前正在执行任务的 goroutine 数。
func (r *BoundedRunner) Busy() int {
	return len(r.sem)
}

// BoundedGo 通过信号量限流后启动异步任务，返回 AsyncResult[T]。
// 当并发 goroutine 数已达上限时，阻塞等待 slot 释放或 ctx 取消。
//
// 重要：fn 必须正确响应 ctx.Done() 以按时释放信号量槽位。
// 如果 fn 忽略 ctx 取消而持续阻塞（例如未在 HTTP 请求中使用 ctx），
// 对应的信号量槽位将永久占用，最终耗尽所有槽位导致 BoundedRunner 完全阻塞。
// 建议在 fn 内部所有 I/O 操作中传递 ctx，或设置合理的业务超时。
func BoundedGo[T any](r *BoundedRunner, ctx context.Context, fn func(context.Context) (T, error)) *AsyncResult[T] {
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		var zero T
		ar := &AsyncResult[T]{ready: make(chan struct{})}
		ar.result = core.Result[T]{Value: zero, Err: ctx.Err()}
		close(ar.ready)
		return ar
	}

	return Go[T](ctx, func(ctx context.Context) (T, error) {
		defer func() { <-r.sem }()
		return fn(ctx)
	})
}

// BoundedGoAct 带限流的无返回值异步任务，返回 AsyncResultNoResult。
// 关于 ctx 响应的注意事项与 BoundedGo 相同。
func BoundedGoAct(r *BoundedRunner, ctx context.Context, fn func(context.Context) error) *AsyncResultNoResult {
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		ar := &AsyncResultNoResult{ready: make(chan struct{})}
		ar.result = core.Result[NoResult]{Value: NoResult{}, Err: ctx.Err()}
		close(ar.ready)
		return ar
	}

	return GoAct(ctx, func(ctx context.Context) error {
		defer func() { <-r.sem }()
		return fn(ctx)
	})
}

// BoundedGoResult 带限流的可取消异步任务，返回 Task[T]。
// 关于 ctx 响应的注意事项与 BoundedGo 相同。
func BoundedGoResult[T any](r *BoundedRunner, ctx context.Context, fn func(context.Context) (T, error)) Task[T] {
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		var zero T
		ctx, cancel := context.WithCancel(ctx)
		return Task[T]{
			Ctx:    ctx,
			Cancel: cancel,
			Result: func() (T, error) { return zero, ctx.Err() },
		}
	}

	t := GoResult[T](ctx, fn)
	orig := t.Result
	t.Result = func() (T, error) {
		defer func() { <-r.sem }()
		return orig()
	}
	return t
}
