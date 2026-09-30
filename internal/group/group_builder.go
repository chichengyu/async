package group

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// ──────────────────────────── GroupBuilder ────────────────────────────
//
// GroupBuilder 泛型任务组链式构造器，统一入口为 async.Group[T]()。
// 支持 Context、Concurrency、Timeout、SubmitTimeout、FailFast、Streaming、ResultCallback。
//
// 两种终端方法：
//   - Build()：返回 *Group[T]，调用方自行管理生命周期（需手动 Wait / Close）
//   - Run(fn)：自动创建→执行→Close，fn 结束后清理资源
//
// 示例：
//
//	// Build 模式
//	g := async.Group[string]().Context(ctx).Worker(10).Timeout(5 * time.Second).Build()
//	defer g.Close()
//	g.Go(ctx, fn)
//	g.Wait()
//
//	// Run 模式
//	async.Group[string]().Context(ctx).Worker(10).Timeout(5 * time.Second).Run(func(ctx context.Context, g *group.Group[string]) error {
//	    for _, item := range items {
//	        g.Go(ctx, fn)
//	    }
//	    return nil
//	})
type GroupBuilder[T any] struct {
	ctx           context.Context       // 请求上下文，自动注入 trace_id
	concurrency   int                   // 最大并发数
	timeout       time.Duration         // 单任务超时
	submitTimeout time.Duration         // 获取并发槽位的等待超时
	failFast      bool                  // 快速失败：任一失败取消所有
	streaming     int                   // 流式结果通道缓冲大小
	resultCb      func(core.Result[T])  // 结果回调
	autoScale     *core.AutoScaleConfig // 自动扩缩容配置，nil 表示禁用
	logger        core.Logger           // 自定义日志
	extPool       *pool.Pool[T]         // 外部注入协程池
}

// NewGroupBuilder 创建任务组构造器，默认使用 context.Background()。
// 通过 .Context(ctx) 链式设置上下文。
func NewGroupBuilder[T any]() *GroupBuilder[T] {
	return &GroupBuilder[T]{
		ctx:         context.Background(),
		concurrency: core.IO(),
	}
}

// Context 链式设置上下文，自动注入 trace_id。
func (b *GroupBuilder[T]) Context(ctx context.Context) *GroupBuilder[T] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

// ── 链式配置方法 ──

// Worker 设置最大并发数。
func (b *GroupBuilder[T]) Worker(n int) *GroupBuilder[T] {
	b.concurrency = n
	return b
}

// DefaultWorker 使用默认并发数（core.IO()）。
func (b *GroupBuilder[T]) DefaultWorker() *GroupBuilder[T] {
	b.concurrency = core.IO()
	return b
}

// Timeout 设置每个任务的超时时间。
func (b *GroupBuilder[T]) Timeout(d time.Duration) *GroupBuilder[T] {
	b.timeout = d
	return b
}

// DefaultTimeout 使用默认超时（core.GetDefaultTimeout()）。
func (b *GroupBuilder[T]) DefaultTimeout() *GroupBuilder[T] {
	b.timeout = core.GetDefaultTimeout()
	return b
}

// SubmitTimeout 设置获取并发槽位的等待超时。
func (b *GroupBuilder[T]) SubmitTimeout(d time.Duration) *GroupBuilder[T] {
	b.submitTimeout = d
	return b
}

// DefaultSubmitTimeout 使用默认提交超时。
func (b *GroupBuilder[T]) DefaultSubmitTimeout() *GroupBuilder[T] {
	b.submitTimeout = 0
	return b
}

// FailFast 启用快速失败模式。
func (b *GroupBuilder[T]) FailFast() *GroupBuilder[T] {
	b.failFast = true
	return b
}

// Streaming 设置流式结果通道缓冲大小，0 使用 concurrency*2 的默认值。
func (b *GroupBuilder[T]) Streaming(buf int) *GroupBuilder[T] {
	b.streaming = buf
	return b
}

// DefaultStreaming 使用默认流式缓冲。
func (b *GroupBuilder[T]) DefaultStreaming() *GroupBuilder[T] {
	b.streaming = 0
	return b
}

// ResultCallback 设置结果回调，每个任务完成时同步调用。
func (b *GroupBuilder[T]) ResultCallback(fn func(core.Result[T])) *GroupBuilder[T] {
	b.resultCb = fn
	return b
}

// DefaultResultCallback 取消结果回调。
func (b *GroupBuilder[T]) DefaultResultCallback() *GroupBuilder[T] {
	b.resultCb = nil
	return b
}

// AutoScale 启用自动扩缩容，config 为 nil 时使用 DefaultAutoScaleConfig()。
func (b *GroupBuilder[T]) AutoScale(config *core.AutoScaleConfig) *GroupBuilder[T] {
	b.autoScale = config
	return b
}

// DefaultAutoScale 使用默认配置启用自动扩缩容。
func (b *GroupBuilder[T]) DefaultAutoScale() *GroupBuilder[T] {
	b.autoScale = core.DefaultAutoScaleConfig()
	return b
}

// Logger 注入自定义日志实现，全局生效。
func (b *GroupBuilder[T]) Logger(l core.Logger) *GroupBuilder[T] { core.SetLogger(l); return b }

// DefaultLogger 重置为默认日志实现。
func (b *GroupBuilder[T]) DefaultLogger() *GroupBuilder[T] { core.SetLogger(nil); return b }

// Pool 注入外部协程池，构建的 Group 将使用该池执行任务。
func (b *GroupBuilder[T]) Pool(p *pool.Pool[T]) *GroupBuilder[T] { b.extPool = p; return b }

// DefaultPool 恢复为内部自动创建协程池（默认行为）。
func (b *GroupBuilder[T]) DefaultPool() *GroupBuilder[T] { b.extPool = nil; return b }

// ── 终端方法 ──

// Build 创建并配置 Group，返回后调用方需自行管理生命周期（手动 Close / Wait）。
func (b *GroupBuilder[T]) Build() *Group[T] {
	g := NewGroup[T](b.concurrency)

	if b.timeout > 0 {
		g.WithTimeout(b.timeout)
	}
	if b.submitTimeout > 0 {
		g.WithSubmitTimeout(b.submitTimeout)
	}
	if b.failFast {
		g.WithFailFast(b.ctx)
	} else {
		g.WithContext(b.ctx)
	}

	if b.streaming > 0 {
		g.WithStreaming(b.streaming)
	}
	if b.resultCb != nil {
		g.WithResultCallback(b.resultCb)
	}
	if b.autoScale != nil {
		g.EnableAutoScale(b.autoScale)
	}
	if b.extPool != nil {
		g.WithPool(b.extPool)
	}

	return g
}

// Run 创建→执行→关闭。
// 内部流程：Build() → 调用 fn → Close()（Wait + 清理）。
// 使用前必须设置 Concurrency（或使用 DefaultWorker）。
//
// 示例：
//
//	async.Group[string](ctx).
//	    Worker(10).
//	    Timeout(5 * time.Second).
//	    Run(func(ctx context.Context, g *group.Group[string]) error {
//	        for _, item := range items {
//	            g.Go(ctx, func(ctx context.Context) (string, error) {
//	                return process(item)
//	            })
//	        }
//	        return nil
//	    })
func (b *GroupBuilder[T]) Run(fn func(ctx context.Context, g *Group[T]) error) error {
	g := b.Build()
	defer g.Close()
	return fn(b.ctx, g)
}
