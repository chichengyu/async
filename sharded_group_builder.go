package async

import (
	"context"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/shard"
)

// ──────────────────────────── ShardedGroup 分片 Group ────────────────────────────

// ShardedGroupBuilder 分片 Group 构造器，统一入口为 ShardedGroup[T](ctx)。
// 将任务分发到 N 个 Group 实例，支持按 Key 亲和或 RoundRobin。
// 支持链式配置，Run 自动创建→执行→Close 所有分片 Group。
//
// 示例：
//
//	async.ShardedGroup[string](ctx).
//	    Shards(8).
//	    Concurrency(16).
//	    Distribution(async.RoundRobin).
//	    Run(func(ctx context.Context, sg *shard.ShardedGroup[string]) error {
//	        for _, item := range items {
//	            sg.Go(ctx, func(ctx context.Context) (string, error) {
//	                return process(item)
//	            })
//	        }
//	        results := sg.Wait()
//	        return check(results)
//	    })
type ShardedGroupBuilder[T any] struct {
	ctx context.Context
	cfg shard.ShardGroupConfig[T]
}

// ShardedGroup 创建分片 Group 构造器，内部自动注入 trace_id。
func ShardedGroup[T any](ctx context.Context) *ShardedGroupBuilder[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return &ShardedGroupBuilder[T]{
		ctx: core.EnsureTraceID(ctx),
		cfg: shard.ShardGroupConfig[T]{},
	}
}

// ShardedGroupBG 无上下文快捷构造，内部使用 context.Background()。
func ShardedGroupBG[T any]() *ShardedGroupBuilder[T] {
	return ShardedGroup[T](context.Background())
}

// Context 链式设置上下文
func (b *ShardedGroupBuilder[T]) Context(ctx context.Context) *ShardedGroupBuilder[T] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

// DefaultShardedGroupConfig 重置为默认配置
func (b *ShardedGroupBuilder[T]) DefaultShardedGroupConfig() *ShardedGroupBuilder[T] {
	b.cfg = shard.ShardGroupConfig[T]{}
	return b
}

// Config 函数式配置
func (b *ShardedGroupBuilder[T]) Config(fn func(shard.ShardGroupConfig[T]) shard.ShardGroupConfig[T]) *ShardedGroupBuilder[T] {
	b.cfg = fn(b.cfg)
	return b
}

// Shards 设置分片数
func (b *ShardedGroupBuilder[T]) Shards(n int) *ShardedGroupBuilder[T] {
	b.cfg.Shards = n
	return b
}

// Concurrency 设置每分片并发度
func (b *ShardedGroupBuilder[T]) Concurrency(n int) *ShardedGroupBuilder[T] {
	b.cfg.ConcurrencyPerShard = n
	return b
}

// Distribution 设置分发策略
func (b *ShardedGroupBuilder[T]) Distribution(d Distribution) *ShardedGroupBuilder[T] {
	b.cfg.Distribution = d
	return b
}

// Run 终端方法：创建 ShardedGroup → 执行 fn。
// 调用方应在 fn 内调用 sg.Wait() / sg.WaitTimeout() / sg.WaitContext() 等待结果。
func (b *ShardedGroupBuilder[T]) Run(fn func(ctx context.Context, sg *shard.ShardedGroup[T]) error) error {
	sg := shard.NewShardedGroup(b.cfg)
	return fn(b.ctx, sg)
}
