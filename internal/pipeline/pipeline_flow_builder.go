package pipeline

import (
	"context"

	"github.com/chichengyu/async/internal/core"
)

// FlowBuilder Flow 构建器，由 PipelineBuilder.Flow() 创建。
//
// 与 SerialChain/ParallelChain/StreamChain 不同：
//   - 不需要 items，Build() 返回 *Flow 而非执行结果
//   - 不暴露 Timeout/FailFast/OnResult 等一次性执行专属配置
//   - 不暴露 Pool/Shard/AutoScale/Worker 等并行配置
//   - 仅暴露 Stage/DefaultStage/BufSize/Context/Logger/Build
//
// 使用示例：
//
//	fl := async.Pipeline[int]().
//	    Stage("parse", 4).Stage("enrich", 8).
//	    BufSize(1024).
//	    Flow().
//	    Build(ctx, fn)
//	defer fl.Close()
type FlowBuilder[T any] struct {
	ctx     context.Context
	stages  []Stage[T]
	bufSize int
}

// newFlowBuilder 创建 Flow 构建器。
func newFlowBuilder[T any](ctx context.Context, stages []Stage[T]) *FlowBuilder[T] {
	return &FlowBuilder[T]{
		ctx:    ctx,
		stages: append([]Stage[T]{}, stages...),
	}
}

// Context 设置上下文，自动注入 trace_id。
func (fb *FlowBuilder[T]) Context(ctx context.Context) *FlowBuilder[T] {
	fb.ctx = core.EnsureTraceID(ctx)
	return fb
}

// Stage 追加一个处理阶段。
func (fb *FlowBuilder[T]) Stage(name string, concurrency int) *FlowBuilder[T] {
	fb.stages = append(fb.stages, Stage[T]{Name: name, Concurrency: concurrency})
	return fb
}

// DefaultStage 使用默认 IO 并发度追加处理阶段。
func (fb *FlowBuilder[T]) DefaultStage(name string) *FlowBuilder[T] {
	return fb.Stage(name, core.IO())
}

// BufSize 设置阶段间 channel 缓冲大小（<=0 使用默认值 256）。
func (fb *FlowBuilder[T]) BufSize(n int) *FlowBuilder[T] {
	fb.bufSize = n
	return fb
}

// Logger 注入自定义日志实现，全局生效。
func (fb *FlowBuilder[T]) Logger(l core.Logger) *FlowBuilder[T] {
	core.SetLogger(l)
	return fb
}

// Run 构建 Flow 并启动常驻 worker，使用 builder 内部 ctx。
// 等价于 Build(fb.ctx, fn)，推荐配合 defer fl.Close() 使用。
//
// 使用示例：
//
//	fl := async.Pipeline[int]().
//	    Stage("parse", 4).Stage("enrich", 8).
//	    Flow().
//	    Run(fn)
//	defer fl.Close()
func (fb *FlowBuilder[T]) Run(
	fn func(ctx context.Context, stage string, item T) (T, error),
) *Flow[T] {
	return fb.Build(fb.ctx, fn)
}

// Build 构建 Flow，启动常驻 worker goroutine。
// stages 为空时会 panic（与 NewFlow 行为一致）。
func (fb *FlowBuilder[T]) Build(
	ctx context.Context,
	fn func(ctx context.Context, stage string, item T) (T, error),
) *Flow[T] {
	cfg := FlowConfig[T]{
		Stages:  fb.stages,
		BufSize: fb.bufSize,
	}
	return NewFlow(ctx, cfg, fn)
}
