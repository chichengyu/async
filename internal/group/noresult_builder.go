package group

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ──────────────────────────── GroupNoResultBuilder ────────────────────────────
//
// GroupNoResultBuilder 无返回值任务组链式构造器，统一入口为 async.GroupVoid()。
//
// 示例：
//
//	// Build 模式
//	nr := async.GroupVoid().Context(ctx).Worker(10).Timeout(5 * time.Second).Build()
//	defer nr.Close()
//	nr.Go(ctx, fn)
//	nr.Wait()
//
//	// Run 模式
//	async.GroupVoid().Context(ctx).Worker(10).Run(func(ctx context.Context, nr *NoResult) error {
//	    for _, item := range items {
//	        nr.Go(ctx, func(ctx context.Context) error { return process(item) })
//	    }
//	    return nil
//	})
type GroupNoResultBuilder struct {
	ctx           context.Context       // 请求上下文，自动注入 trace_id
	concurrency   int                   // 最大并发数
	timeout       time.Duration         // 单任务超时
	submitTimeout time.Duration         // 获取并发槽位的等待超时
	failFast      bool                  // 快速失败：任一失败取消所有
	streaming     int                   // 流式结果通道缓冲大小
	autoScale     *core.AutoScaleConfig // 自动扩缩容配置，nil 表示禁用
}

// NewGroupNoResultBuilder 创建无返回值任务组构造器，默认使用 context.Background()。
// 通过 .Context(ctx) 链式设置上下文。
func NewGroupNoResultBuilder() *GroupNoResultBuilder {
	return &GroupNoResultBuilder{
		ctx:         context.Background(),
		concurrency: core.IO(),
	}
}

// Context 链式设置上下文，自动注入 trace_id。
func (b *GroupNoResultBuilder) Context(ctx context.Context) *GroupNoResultBuilder {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

// ── 链式配置方法 ──

// Worker 设置最大并发数。
func (b *GroupNoResultBuilder) Worker(n int) *GroupNoResultBuilder {
	b.concurrency = n
	return b
}

// DefaultWorker 使用默认并发数（core.IO()）。
func (b *GroupNoResultBuilder) DefaultWorker() *GroupNoResultBuilder {
	b.concurrency = core.IO()
	return b
}

// Timeout 设置每个任务的超时时间。
func (b *GroupNoResultBuilder) Timeout(d time.Duration) *GroupNoResultBuilder {
	b.timeout = d
	return b
}

// DefaultTimeout 使用默认超时。
func (b *GroupNoResultBuilder) DefaultTimeout() *GroupNoResultBuilder {
	b.timeout = core.GetDefaultTimeout()
	return b
}

// SubmitTimeout 设置获取并发槽位的等待超时。
func (b *GroupNoResultBuilder) SubmitTimeout(d time.Duration) *GroupNoResultBuilder {
	b.submitTimeout = d
	return b
}

// DefaultSubmitTimeout 使用默认提交超时。
func (b *GroupNoResultBuilder) DefaultSubmitTimeout() *GroupNoResultBuilder {
	b.submitTimeout = 0
	return b
}

// FailFast 启用快速失败模式。
func (b *GroupNoResultBuilder) FailFast() *GroupNoResultBuilder {
	b.failFast = true
	return b
}

// Streaming 设置流式结果通道缓冲大小。
func (b *GroupNoResultBuilder) Streaming(buf int) *GroupNoResultBuilder {
	b.streaming = buf
	return b
}

// DefaultStreaming 使用默认流式缓冲。
func (b *GroupNoResultBuilder) DefaultStreaming() *GroupNoResultBuilder {
	b.streaming = 0
	return b
}

// AutoScale 启用自动扩缩容，config 为 nil 时使用 DefaultAutoScaleConfig()。
func (b *GroupNoResultBuilder) AutoScale(config *core.AutoScaleConfig) *GroupNoResultBuilder {
	b.autoScale = config
	return b
}

// DefaultAutoScale 使用默认配置启用自动扩缩容。
func (b *GroupNoResultBuilder) DefaultAutoScale() *GroupNoResultBuilder {
	b.autoScale = core.DefaultAutoScaleConfig()
	return b
}

// ── 终端方法 ──

// Build 创建并配置 NoResult，返回后调用方需自行管理生命周期。
func (b *GroupNoResultBuilder) Build() *NoResult {
	g := NewGroup[struct{}](b.concurrency)
	nr := (*NoResult)(g)

	if b.timeout > 0 {
		nr.WithTimeout(b.timeout)
	}
	if b.submitTimeout > 0 {
		nr.WithSubmitTimeout(b.submitTimeout)
	}
	if b.failFast {
		nr.WithFailFast(b.ctx)
	} else {
		nr.WithContext(b.ctx)
	}

	if b.streaming > 0 {
		nr.WithStreaming(b.streaming)
	}
	if b.autoScale != nil {
		nr.EnableAutoScale(b.autoScale)
	}

	return nr
}

// Run 创建→执行→关闭。
// 内部流程：Build() → 调用 fn → Close()（Wait + 清理）。
func (b *GroupNoResultBuilder) Run(fn func(ctx context.Context, nr *NoResult) error) error {
	nr := b.Build()
	defer nr.Close()
	return fn(b.ctx, nr)
}
