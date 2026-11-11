package task

import (
	"context"

	"github.com/chichengyu/async/internal/core"
)

// ──────────────────────────── BoundedRunner ────────────────────────────

// BoundedRunner 限制同时占用许可的业务回调数。
// 通过内部信号量控制最大并发任务数，提交方在槽位耗尽时等待。
// 不限制提交方创建的等待 goroutine、业务子 goroutine 或结果投递收尾。
//
// 与裸 task.Go() 的区别：
//   - task.Go()：每次调用创建一个新 goroutine，同时存活数量取决于任务执行时间
//   - BoundedGo()：最多 max 个回调持有许可，其余调用阻塞等待槽位或上下文取消
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
	sem       chan struct{}
	loggerCtx context.Context
}

// BoundedRunnerBuilder 链式构建 BoundedRunner。
// 使用与 TaskBuilder.Bounded(max) 相同的并发槽位机制，Build 创建独立执行器。
// 公开的 TaskBuilder 接受并发数，不接受已有的 BoundedRunner。
//
// 使用示例：
//
//	runner := async.NewBoundedRunnerBuilder().Context(ctx).Max(1000).Build()
type BoundedRunnerBuilder struct {
	ctx context.Context
	max int
}

// NewBoundedRunnerBuilder 创建 BoundedRunner 链式构建器。
func NewBoundedRunnerBuilder() *BoundedRunnerBuilder {
	return &BoundedRunnerBuilder{
		ctx: context.Background(),
		max: core.IO(),
	}
}

// Context 设置构建器上下文并注入 trace_id；Build 仅保留局部日志设置，不继承其取消和期限。
func (b *BoundedRunnerBuilder) Context(ctx context.Context) *BoundedRunnerBuilder {
	b.ctx = core.EnsureTraceID(core.CopyLogger(b.ctx, ctx))
	return b
}

// Max 设置业务许可容量。<=0 使用默认 IO 并发度。
func (b *BoundedRunnerBuilder) Max(n int) *BoundedRunnerBuilder {
	b.max = n
	return b
}

// Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。
func (b *BoundedRunnerBuilder) Logger(l core.Logger) *BoundedRunnerBuilder {
	b.ctx = core.WithLogger(b.ctx, l)
	return b
}

// DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。
func (b *BoundedRunnerBuilder) DefaultLogger() *BoundedRunnerBuilder {
	b.ctx = core.WithLogger(b.ctx, nil)
	return b
}

// Build 创建 BoundedRunner 实例。
func (b *BoundedRunnerBuilder) Build() *BoundedRunner {
	r := NewBoundedRunner(b.max)
	r.loggerCtx = core.CopyLogger(b.ctx, context.Background())
	return r
}

// NewBoundedRunner 创建一个限制同时持有许可的业务回调数的执行器。
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

// Max 返回业务许可容量，不是进程 goroutine 数上限。
func (r *BoundedRunner) Max() int {
	return cap(r.sem)
}

// Available 返回当前可用的并发槽位数。
func (r *BoundedRunner) Available() int {
	return cap(r.sem) - len(r.sem)
}

// Busy 返回当前已占用槽位数，包含取得槽位后尚未开始执行的任务。
// 与 Available 分别读取，不保证两次调用形成同一时刻的快照。
func (r *BoundedRunner) Busy() int {
	return len(r.sem)
}

// BoundedGo 通过信号量限流后启动异步任务，返回 AsyncResult[T]。
// 当业务许可已耗尽时，在调用方阻塞等待 slot 释放或 ctx 取消。
//
// 重要：fn 必须正确响应 ctx.Done() 以按时释放信号量槽位。
// 如果 fn 忽略 ctx 取消而持续阻塞（例如未在 HTTP 请求中使用 ctx），
// 对应的信号量槽位将永久占用，最终耗尽所有槽位导致 BoundedRunner 完全阻塞。
// 建议在 fn 内部所有 I/O 操作中传递 ctx，或设置合理的业务超时。
func BoundedGo[T any](r *BoundedRunner, ctx context.Context, fn func(context.Context) (T, error)) *AsyncResult[T] {
	ctx = core.CopyLogger(r.loggerCtx, ctx)
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		var zero T
		ar := &AsyncResult[T]{ready: make(chan struct{})}
		ar.result = core.Result[T]{Value: zero, Err: ctx.Err()}
		ar.readyOnce.Do(func() { close(ar.ready) })
		return ar
	}

	// Until Go returns, the submitting goroutine owns the acquired slot.
	// Trace injection can panic or Goexit before the worker is launched.
	started := false
	defer func() {
		if !started {
			<-r.sem
		}
	}()
	result := Go[T](ctx, func(ctx context.Context) (T, error) {
		defer func() { <-r.sem }()
		return fn(ctx)
	})
	started = true
	return result
}

// BoundedGoAct 带限流的无返回值异步任务，返回 AsyncResultNoResult。
// 关于 ctx 响应的注意事项与 BoundedGo 相同。
func BoundedGoAct(r *BoundedRunner, ctx context.Context, fn func(context.Context) error) *AsyncResultNoResult {
	ctx = core.CopyLogger(r.loggerCtx, ctx)
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		ar := &AsyncResultNoResult{ready: make(chan struct{})}
		ar.result = core.Result[NoResult]{Value: NoResult{}, Err: ctx.Err()}
		ar.readyOnce.Do(func() { close(ar.ready) })
		return ar
	}

	started := false
	defer func() {
		if !started {
			<-r.sem
		}
	}()
	result := GoAct(ctx, func(ctx context.Context) error {
		defer func() { <-r.sem }()
		return fn(ctx)
	})
	started = true
	return result
}

// BoundedGoResult 带限流的可取消异步任务，返回 Task[T]。
// 关于 ctx 响应的注意事项与 BoundedGo 相同。
func BoundedGoResult[T any](r *BoundedRunner, ctx context.Context, fn func(context.Context) (T, error)) Task[T] {
	ctx = core.CopyLogger(r.loggerCtx, ctx)
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

	started := false
	defer func() {
		if !started {
			<-r.sem
		}
	}()
	result := GoResult[T](ctx, func(ctx context.Context) (T, error) {
		defer func() { <-r.sem }()
		return fn(ctx)
	})
	started = true
	return result
}
