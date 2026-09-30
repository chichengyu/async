package pipeline

import (
	"context"

	"github.com/chichengyu/async/internal/core"
)

// SerialChain 串行管道构建器，由 PipelineBuilder.Serial() 创建。
// 所有阶段串行执行，不暴露 Pool/Shard/AutoScale/Worker 等并行配置方法。
type SerialChain[T any] struct {
	*pipelineBase[T, *SerialChain[T]]
}

// ── 终端方法 ──

// Execute 串行执行所有阶段，前一阶段输出作为后一阶段输入。
func (s *SerialChain[T]) Execute(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return s.execSerial(fn)
}

// Run 是 Execute 的别名。
func (s *SerialChain[T]) Run(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return s.execSerial(fn)
}

// ExecuteWithMeta 串行执行并返回带阶段元信息的结果。
func (s *SerialChain[T]) ExecuteWithMeta(fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T] {
	return s.execSerialWithMeta(fn)
}

// ExecuteStream 串行执行管道，通过 channel 流式返回最终阶段结果。
func (s *SerialChain[T]) ExecuteStream(fn func(context.Context, string, T) (T, error), bufSize int) <-chan core.Result[T] {
	return s.execSerialStream(fn, bufSize)
}

// Stream 切换为流式管道模式（串行版）。
func (s *SerialChain[T]) Stream() *PipelineStreamBuilder[T] {
	return &PipelineStreamBuilder[T]{
		ctx:    s.ctx,
		items:  s.items,
		stages: s.stages,
	}
}
