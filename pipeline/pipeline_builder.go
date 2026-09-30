package pipeline

import (
	"context"

	"github.com/chichengyu/async/internal/core"
)

// PipelineBuilder 多阶段管道链式构建器，统一入口为 async.Pipeline[T](items)。
// 支持链式追加 Stage，终端方法执行管道。
//
// 使用示例：
//
//	// 多阶段管道
//	results, err := async.Pipeline[Data](items).Context(ctx).
//	    Stage("parse", 4).
//	    Stage("enrich", 8).
//	    Stage("validate", 2).
//	    Execute(func(ctx context.Context, stage string, item Data) (Data, error) {
//	        switch stage {
//	        case "parse":
//	            return parseData(ctx, item)
//	        case "enrich":
//	            return enrichData(ctx, item)
//	        case "validate":
//	            return validateData(ctx, item)
//	        }
//	        return item, nil
//	    })
//
//	// 流式管道
//	ch := async.Pipeline[Data](items).Context(ctx).
//	    Stage("parse", 4).
//	    Stage("validate", 2).
//	    ExecuteStream(fn, 1024)
//	for r := range ch {
//	    if r.Ok() {
//	        saveToDB(r.Value)
//	    }
//	}
type PipelineBuilder[T any] struct {
	ctx    context.Context
	items  []T
	stages []Stage[T]
}

// NewPipelineBuilder 创建管道构建器，默认使用 context.Background()。
// 通过 .Context(ctx) 链式设置上下文。
func NewPipelineBuilder[T any](items []T) *PipelineBuilder[T] {
	return &PipelineBuilder[T]{
		ctx:   context.Background(),
		items: items,
	}
}

// Context 链式设置上下文，自动注入 trace_id。
func (b *PipelineBuilder[T]) Context(ctx context.Context) *PipelineBuilder[T] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

// Stage 追加一个处理阶段。
// name 为阶段名称，concurrency 为该阶段的并发度（<=0 使用默认 IO 并发度）。
func (b *PipelineBuilder[T]) Stage(name string, concurrency int) *PipelineBuilder[T] {
	b.stages = append(b.stages, Stage[T]{Name: name, Concurrency: concurrency})
	return b
}

// DefaultStage 使用默认 IO 并发度追加处理阶段，等价于 Stage(name, core.IO())。
func (b *PipelineBuilder[T]) DefaultStage(name string) *PipelineBuilder[T] {
	return b.Stage(name, core.IO())
}

// ── 终端方法 ──

// Execute 依次执行各阶段，前一阶段输出作为后一阶段输入。
// 返回每个元素在所有阶段执行后的最终结果。
func (b *PipelineBuilder[T]) Execute(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return Execute(b.ctx, b.stages, b.items, fn)
}

// Run 是 Execute 的别名，统一命名。
func (b *PipelineBuilder[T]) Run(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return Execute(b.ctx, b.stages, b.items, fn)
}

// ExecuteWithMeta 与 Execute 相同，但返回带阶段元信息的结果。
// 可追踪每个元素在每个阶段的处理情况。
func (b *PipelineBuilder[T]) ExecuteWithMeta(fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T] {
	return ExecuteWithMeta(b.ctx, b.stages, b.items, fn)
}

// ExecuteStream 执行管道，通过 channel 流式返回最终阶段结果。
// 非最终阶段与 Execute 行为一致，最终阶段结果逐条实时发送到 channel。
// bufSize <= 0 时自动计算缓冲区大小。
func (b *PipelineBuilder[T]) ExecuteStream(fn func(context.Context, string, T) (T, error), bufSize int) <-chan core.Result[T] {
	return ExecuteStream(b.ctx, b.stages, b.items, fn, bufSize)
}

// ── 流式模式 ──

// Stream 切换为流式管道模式，bufSize 为 channel 缓冲大小（<=0 使用默认值）。
// 返回 PipelineStreamBuilder，通过 .Run(fn).Receive() 获取结果 channel。
//
// 使用示例：
//
//	ch := async.Pipeline[Data](items).Context(ctx).
//	    Stage("parse", 4).Stage("validate", 2).
//	    Stream(1024).Run(fn).Receive()
//	for r := range ch {
//	    if r.Ok() { saveToDB(r.Value) }
//	}
func (b *PipelineBuilder[T]) Stream(bufSize int) *PipelineStreamBuilder[T] {
	return &PipelineStreamBuilder[T]{
		ctx:    b.ctx,
		items:  b.items,
		stages: b.stages,
		buf:    bufSize,
	}
}

// PipelineStreamBuilder 流式管道构建器，由 PipelineBuilder.Stream() 创建。
// 通过 .Run(fn).Receive() 启动管道并获取结果 channel。
type PipelineStreamBuilder[T any] struct {
	ctx    context.Context
	items  []T
	stages []Stage[T]
	buf    int
}

// BufSize 设置 channel 缓冲大小（<=0 使用默认值）。
func (b *PipelineStreamBuilder[T]) BufSize(n int) *PipelineStreamBuilder[T] {
	b.buf = n
	return b
}

// Run 启动流式管道，返回句柄。
func (b *PipelineStreamBuilder[T]) Run(fn func(context.Context, string, T) (T, error)) *PipelineStream[T] {
	ch := ExecuteStream(b.ctx, b.stages, b.items, fn, b.buf)
	return &PipelineStream[T]{ch: ch}
}

// PipelineStream 流式管道结果句柄，由 PipelineStreamBuilder.Run() 返回。
//
// 两种消费方式：
//
//	// 方式一：channel 消费（适合 select/range）
//	ch := stream.Receive()
//	for r := range ch { ... }
//
//	// 方式二：回调消费（一边执行一边消费）
//	stream.ForEach(func(r core.Result[Data]) {
//	    if r.Ok() { saveToDB(r.Value) }
//	})
type PipelineStream[T any] struct {
	ch <-chan core.Result[T]
}

// Receive 返回流式结果 channel，通过 range 逐条消费。
func (s *PipelineStream[T]) Receive() <-chan core.Result[T] {
	return s.ch
}

// ForEach 通过回调函数逐条消费流式结果，阻塞直到管道执行完毕。
// fn 在管道执行过程中被调用，实现一边执行一边消费。
func (s *PipelineStream[T]) ForEach(fn func(core.Result[T])) {
	for r := range s.ch {
		fn(r)
	}
}
