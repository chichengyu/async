package async

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/group"
)

// ──────────────────────────── MultiGroup 分片任务组 ────────────────────────────

// MultiGroupBuilder 分片任务组构造器，统一入口为 MultiGroup[T](ctx)。
// 内部创建 Group → 水平分片为 N 份，支持极限高并发。
// 支持链式配置，Run 自动创建→执行→Close 所有分片。
//
// 示例：
//
//	async.MultiGroup[string](ctx).
//	    Shards(8).
//	    Concurrency(64).
//	    FailFast().
//	    Run(func(ctx context.Context, mg *group.MultiGroup[string]) error {
//	        for _, item := range items {
//	            mg.Go(ctx, func(ctx context.Context) (string, error) {
//	                return process(item)
//	            })
//	        }
//	        results, firstErr := mg.Wait()
//	        if firstErr != nil { return firstErr }
//	        _ = results
//	        return nil
//	    })
type MultiGroupBuilder[T any] struct {
	ctx           context.Context
	concurrency   int
	shards        int
	timeout       time.Duration
	submitTimeout time.Duration
	failFast      bool
}

// MultiGroup 创建分片任务组构造器，内部自动注入 trace_id。
func MultiGroup[T any](ctx context.Context) *MultiGroupBuilder[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return &MultiGroupBuilder[T]{
		ctx: core.EnsureTraceID(ctx),
	}
}

// MultiGroupBG 无上下文快捷构造，内部使用 context.Background()。
func MultiGroupBG[T any]() *MultiGroupBuilder[T] {
	return MultiGroup[T](context.Background())
}

// Context 链式设置上下文
func (b *MultiGroupBuilder[T]) Context(ctx context.Context) *MultiGroupBuilder[T] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

// Shards 设置分片数
func (b *MultiGroupBuilder[T]) Shards(n int) *MultiGroupBuilder[T] {
	b.shards = n
	return b
}

// Concurrency 设置并发度
func (b *MultiGroupBuilder[T]) Concurrency(n int) *MultiGroupBuilder[T] {
	b.concurrency = n
	return b
}

// Timeout 设置任务超时
func (b *MultiGroupBuilder[T]) Timeout(d time.Duration) *MultiGroupBuilder[T] {
	b.timeout = d
	return b
}

// SubmitTimeout 设置提交超时
func (b *MultiGroupBuilder[T]) SubmitTimeout(d time.Duration) *MultiGroupBuilder[T] {
	b.submitTimeout = d
	return b
}

// FailFast 启用快速失败模式
func (b *MultiGroupBuilder[T]) FailFast() *MultiGroupBuilder[T] {
	b.failFast = true
	return b
}

// Run 终端方法：创建 Group → 应用配置 → 分片 → 执行 fn → Close 所有分片。
func (b *MultiGroupBuilder[T]) Run(fn func(ctx context.Context, mg *group.MultiGroup[T]) error) error {
	g := group.NewGroup[T](b.concurrency)

	if b.timeout > 0 {
		g.WithTimeout(b.timeout)
	}
	if b.submitTimeout > 0 {
		g.WithSubmitTimeout(b.submitTimeout)
	}
	if b.failFast {
		g.WithFailFast(b.ctx)
	}

	mg := g.Shard(b.shards)
	defer mg.Close()
	return fn(b.ctx, mg)
}
