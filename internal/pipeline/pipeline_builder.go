package pipeline

import (
	"context"

	"github.com/chichengyu/async/internal/core"
)

// PipelineBuilder 多阶段管道链式构建器，统一入口为 async.Pipeline[T](items)。
// 支持链式追加 Stage，终端方法执行管道。
// 通过 Serial()/Parallel() 切换执行模式。
//
// 使用示例：
//
//	// 基础用法（100% 向后兼容）
//	results, err := async.Pipeline[Data](items).Context(ctx).
//	    Stage("parse", 4).
//	    Stage("enrich", 8).
//	    Execute(fn)
//
//	// 串行模式
//	async.Pipeline(items).Stage("parse", 4).Serial().Run(fn)
//
//	// 并行模式 + 分片 + 协程池
//	async.Pipeline(items).Stage("parse", 4).
//	    Parallel().Shard(8).Pool(myPool).Run(fn)
//
//	// 公共配置继承到模式切换
//	async.Pipeline(items).
//	    Timeout(10*time.Second).FailFast().OnResult(cb).
//	    Stage("parse", 4).
//	    Parallel().Shard(8).Run(fn)
type PipelineBuilder[T any] struct {
	*pipelineBase[T, *PipelineBuilder[T]]
}

// NewPipelineBuilder 创建管道构建器，默认使用 context.Background()。
func NewPipelineBuilder[T any](items []T) *PipelineBuilder[T] {
	pb := &PipelineBuilder[T]{}
	pb.pipelineBase = newPipelineBase(context.Background(), items, pb)
	return pb
}

// Serial 切换到串行模式，返回 SerialChain。
// 继承 PipelineBuilder 上已配置的所有公共参数（Timeout/FailFast/Logger/OnResult/Stage）。
func (b *PipelineBuilder[T]) Serial() *SerialChain[T] {
	return b.spawnSerial()
}

// Parallel 切换到并行模式，返回 ParallelChain。
// 继承 PipelineBuilder 上已配置的所有公共参数，并暴露 Pool/Shard/AutoScale/Worker 方法。
func (b *PipelineBuilder[T]) Parallel() *ParallelChain[T] {
	return b.spawnParallel()
}

// ── 终端方法（向后兼容：直接调用原核心函数，100% 行为不变）──

// Execute 依次执行各阶段，前一阶段输出作为后一阶段输入。
// 保持向后兼容：直接代理到核心 Execute 函数。
func (b *PipelineBuilder[T]) Execute(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return Execute(b.ctx, b.stages, b.items, fn)
}

// Run 是 Execute 的别名。
func (b *PipelineBuilder[T]) Run(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return Execute(b.ctx, b.stages, b.items, fn)
}

// ExecuteWithMeta 与 Execute 相同，但返回带阶段元信息的结果。
func (b *PipelineBuilder[T]) ExecuteWithMeta(fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T] {
	return ExecuteWithMeta(b.ctx, b.stages, b.items, fn)
}

// ExecuteStream 执行管道，通过 channel 流式返回最终阶段结果。
func (b *PipelineBuilder[T]) ExecuteStream(fn func(context.Context, string, T) (T, error), bufSize int) <-chan core.Result[T] {
	return ExecuteStream(b.ctx, b.stages, b.items, fn, bufSize)
}

// ── 流式模式 ──

// Stream 切换为流式管道模式。
// 返回 PipelineStreamBuilder，通过 .Buf(n).Run(fn).Receive() 获取结果 channel。
func (b *PipelineBuilder[T]) Stream() *PipelineStreamBuilder[T] {
	return &PipelineStreamBuilder[T]{
		ctx:    b.ctx,
		items:  b.items,
		stages: b.stages,
	}
}

// PipelineStreamBuilder 流式管道构建器，由 PipelineBuilder.Stream() 创建。
type PipelineStreamBuilder[T any] struct {
	ctx    context.Context
	items  []T
	stages []Stage[T]
	buf    int
}

// Buf 设置 channel 缓冲大小（<=0 使用默认值）。
func (b *PipelineStreamBuilder[T]) Buf(n int) *PipelineStreamBuilder[T] {
	b.buf = n
	return b
}

// DefaultBuf 重置 channel 缓冲大小为默认值（自动计算）。
func (b *PipelineStreamBuilder[T]) DefaultBuf() *PipelineStreamBuilder[T] {
	b.buf = 0
	return b
}

// Run 启动流式管道，返回句柄。
func (b *PipelineStreamBuilder[T]) Run(fn func(context.Context, string, T) (T, error)) *PipelineStream[T] {
	ch := ExecuteStream(b.ctx, b.stages, b.items, fn, b.buf)
	return &PipelineStream[T]{ch: ch}
}

// PipelineStream 流式管道结果句柄。
type PipelineStream[T any] struct {
	ch <-chan core.Result[T]
}

// Receive 返回流式结果 channel。
func (s *PipelineStream[T]) Receive() <-chan core.Result[T] {
	return s.ch
}

// Drain 通过回调逐条消费流式结果。
func (s *PipelineStream[T]) Drain(fn func(core.Result[T])) {
	for r := range s.ch {
		fn(r)
	}
}
