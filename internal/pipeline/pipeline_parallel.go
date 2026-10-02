package pipeline

import (
	"context"
	"runtime"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// ParallelChain 并行管道构建器，由 PipelineBuilder.Parallel() 创建。
// 在公共方法基础上额外暴露 Pool/Shard/AutoScale/Worker 等并行专属配置。
type ParallelChain[T any] struct {
	*pipelineBase[T, *ParallelChain[T]]
}

// Pool 注入外部协程池，启用池化执行。
func (b *ParallelChain[T]) Pool(p *pool.Pool[T]) *ParallelChain[T] {
	b.pool = p
	b.poolAuto = false
	return b
}

// PoolAuto 注入外部协程池，终端方法执行完成后自动 Close。
func (b *ParallelChain[T]) PoolAuto(p *pool.Pool[T]) *ParallelChain[T] {
	b.pool = p
	b.poolAuto = true
	return b
}

// DefaultPool 创建默认并发度的协程池并自动管理生命周期。
func (b *ParallelChain[T]) DefaultPool() *ParallelChain[T] {
	b.pool = nil
	b.poolAuto = true
	return b
}

// Shard 设置水平分片数，shards <= 1 不启用分片。
func (b *ParallelChain[T]) Shard(shards int) *ParallelChain[T] {
	b.shards = shards
	return b
}

// DefaultShard 使用默认分片数（GOMAXPROCS，最少 2）启用分片。
func (b *ParallelChain[T]) DefaultShard() *ParallelChain[T] {
	n := runtime.GOMAXPROCS(0)
	if n < 2 {
		n = 2
	}
	b.shards = n
	return b
}

// AutoScale 启用自动扩缩容。
func (b *ParallelChain[T]) AutoScale(config *core.AutoScaleConfig) *ParallelChain[T] {
	b.autoScaleCfg = config
	return b
}

// DefaultAutoScale 使用默认配置启用自动扩缩容。
func (b *ParallelChain[T]) DefaultAutoScale() *ParallelChain[T] {
	b.autoScaleCfg = core.DefaultAutoScaleConfig()
	return b
}

// Worker 覆盖所有阶段的并发度。
func (b *ParallelChain[T]) Worker(n int) *ParallelChain[T] {
	b.concurrency = n
	return b
}

// DefaultWorker 使用 Stage 自带的并发度。
func (b *ParallelChain[T]) DefaultWorker() *ParallelChain[T] {
	b.concurrency = 0
	return b
}

// Execute 并行执行所有阶段。
func (b *ParallelChain[T]) Execute(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return b.execParallel(fn)
}

// Run 是 Execute 的别名。
func (b *ParallelChain[T]) Run(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return b.execParallel(fn)
}

// ExecuteWithMeta 并行执行并返回带阶段元信息的结果。
func (b *ParallelChain[T]) ExecuteWithMeta(fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T] {
	return b.execParallelWithMeta(fn)
}

// ExecutePipe 并行执行管道，通过 channel 返回最终阶段结果。
func (b *ParallelChain[T]) ExecutePipe(fn func(context.Context, string, T) (T, error), bufSize int) <-chan core.Result[T] {
	return b.execParallelPipe(fn, bufSize)
}

// Pipe 切换为管道输出模式。
func (b *ParallelChain[T]) Pipe() *PipelinePipeBuilder[T] {
	return &PipelinePipeBuilder[T]{
		ctx:    b.ctx,
		items:  b.items,
		stages: b.stages,
	}
}
