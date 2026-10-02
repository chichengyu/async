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

// Stream 切换到一次性流水线模式，返回 StreamChain。
// 所有阶段同时运转，元素流经阶段间 channel（无阶段 barrier）。
// 继承 PipelineBuilder 上已配置的所有公共参数，不暴露 Pool/Shard/AutoScale/Worker。
func (b *PipelineBuilder[T]) Stream() *StreamChain[T] {
	return b.spawnStream()
}

// Flow 切换到常驻管道模式，返回 FlowBuilder。
// 用于持续高频数据流处理场景，goroutine 常驻，通过 Build() 构建后 Submit() 使用。
// PipelineBuilder 上已配置的 ctx 和 stages 会传递给 FlowBuilder。
// Timeout/FailFast/OnResult 等一次性执行专属配置在 Flow 模式下被忽略。
func (b *PipelineBuilder[T]) Flow() *FlowBuilder[T] {
	return newFlowBuilder(b.ctx, b.stages)
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

// ExecutePipe 执行管道，通过 channel 返回最终阶段结果。
func (b *PipelineBuilder[T]) ExecutePipe(fn func(context.Context, string, T) (T, error), bufSize int) <-chan core.Result[T] {
	return ExecutePipe(b.ctx, b.stages, b.items, fn, bufSize)
}

// ── Pipe 模式 ──

// Pipe 切换为管道输出模式。
// 返回 PipelinePipeBuilder，通过 .Buf(n).Run(fn).Receive() 获取结果 channel。
func (b *PipelineBuilder[T]) Pipe() *PipelinePipeBuilder[T] {
	return &PipelinePipeBuilder[T]{
		ctx:    b.ctx,
		items:  b.items,
		stages: b.stages,
	}
}

// PipelinePipeBuilder 管道输出构建器，由 PipelineBuilder.Pipe() 创建。
type PipelinePipeBuilder[T any] struct {
	ctx    context.Context
	items  []T
	stages []Stage[T]
	buf    int
}

// Buf 设置 channel 缓冲大小（<=0 使用默认值）。
func (b *PipelinePipeBuilder[T]) Buf(n int) *PipelinePipeBuilder[T] {
	b.buf = n
	return b
}

// DefaultBuf 重置 channel 缓冲大小为默认值（自动计算）。
func (b *PipelinePipeBuilder[T]) DefaultBuf() *PipelinePipeBuilder[T] {
	b.buf = 0
	return b
}

// Logger 注入自定义日志实现，全局生效。
func (b *PipelinePipeBuilder[T]) Logger(l core.Logger) *PipelinePipeBuilder[T] {
	core.SetLogger(l)
	return b
}

// DefaultLogger 重置为默认日志实现。
func (b *PipelinePipeBuilder[T]) DefaultLogger() *PipelinePipeBuilder[T] {
	core.SetLogger(nil)
	return b
}

// Run 启动管道，返回句柄。
func (b *PipelinePipeBuilder[T]) Run(fn func(context.Context, string, T) (T, error)) *PipelinePipe[T] {
	ch := ExecutePipe(b.ctx, b.stages, b.items, fn, b.buf)
	return &PipelinePipe[T]{ch: ch}
}

// PipelinePipe 管道输出结果句柄。
type PipelinePipe[T any] struct {
	ch <-chan core.Result[T]
}

// Receive 返回结果 channel。
func (s *PipelinePipe[T]) Receive() <-chan core.Result[T] {
	return s.ch
}

// Drain 通过回调逐条消费结果。
func (s *PipelinePipe[T]) Drain(fn func(core.Result[T])) {
	for r := range s.ch {
		fn(r)
	}
}
