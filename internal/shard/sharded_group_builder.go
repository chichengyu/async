package shard

import (
	"context"

	"github.com/chichengyu/async/internal/core"
)

// ShardedGroupBuilder 分片 Group 构造器，统一入口为 async.GroupSharded[T]()。
// 将任务分发到 N 个 Group 实例，支持按 Key 亲和或 RoundRobin。
// 支持链式配置，Run 自动创建→执行→Close 所有分片 Group。
type ShardedGroupBuilder[T any] struct {
	ctx context.Context     // 请求上下文，自动注入 trace_id
	cfg ShardGroupConfig[T] // 分片 Group 配置
}

// NewGroupBuilder 创建分片 Group 构造器，默认使用 context.Background()。
// 通过 .Context(ctx) 链式设置上下文。
func NewGroupBuilder[T any]() *ShardedGroupBuilder[T] {
	return &ShardedGroupBuilder[T]{
		ctx: context.Background(),
		cfg: ShardGroupConfig[T]{},
	}
}

// Context 链式设置上下文
func (b *ShardedGroupBuilder[T]) Context(ctx context.Context) *ShardedGroupBuilder[T] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

// DefaultShardedGroupConfig 重置为默认配置
func (b *ShardedGroupBuilder[T]) DefaultShardedGroupConfig() *ShardedGroupBuilder[T] {
	b.cfg = ShardGroupConfig[T]{}
	return b
}

// DefaultShards 使用默认分片数（4）。
func (b *ShardedGroupBuilder[T]) DefaultShards() *ShardedGroupBuilder[T] {
	b.cfg.Shards = DefaultShardCount
	return b
}

// DefaultConcurrency 使用默认每分片并发度（GOMAXPROCS）。
func (b *ShardedGroupBuilder[T]) DefaultConcurrency() *ShardedGroupBuilder[T] {
	b.cfg.ConcurrencyPerShard = DefaultConcurrencyShard
	return b
}

// DefaultDistribution 使用默认分发策略（RoundRobin）。
func (b *ShardedGroupBuilder[T]) DefaultDistribution() *ShardedGroupBuilder[T] {
	b.cfg.Distribution = DefaultDistribution
	return b
}

// Config 函数式配置
func (b *ShardedGroupBuilder[T]) Config(fn func(ShardGroupConfig[T]) ShardGroupConfig[T]) *ShardedGroupBuilder[T] {
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

// Run 终端方法：创建 ShardedGroup → 执行 fn → 关闭所有分片。
// 调用方应在 fn 内调用 sg.Wait() / sg.WaitTimeout() / sg.WaitContext() 等待结果。
func (b *ShardedGroupBuilder[T]) Run(fn func(ctx context.Context, sg *ShardedGroup[T]) error) error {
	sg := NewShardedGroup(b.cfg)
	defer sg.Close()
	return fn(b.ctx, sg)
}
