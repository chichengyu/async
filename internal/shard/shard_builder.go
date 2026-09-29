package shard

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ShardPoolBuilder 分片池构造器，统一入口为 async.ShardedPool[T](ctx)。
// 支持链式配置和函数式配置，Run 自动创建分片池→执行→Close。
type ShardPoolBuilder[T any] struct {
	ctx context.Context
	cfg ShardPoolConfig[T]
}

// NewPoolBuilder 创建分片池构造器，内部自动注入 trace_id。
func NewPoolBuilder[T any](ctx context.Context) *ShardPoolBuilder[T] {
	return &ShardPoolBuilder[T]{
		ctx: core.EnsureTraceID(ctx),
		cfg: DefaultShardConfig[T](),
	}
}

// DefaultShardPoolConfig 重置为默认配置
func (b *ShardPoolBuilder[T]) DefaultShardPoolConfig() *ShardPoolBuilder[T] {
	b.cfg = DefaultShardConfig[T]()
	return b
}

// Config 函数式配置，允许通过闭包修改 ShardPoolConfig
func (b *ShardPoolBuilder[T]) Config(fn func(ShardPoolConfig[T]) ShardPoolConfig[T]) *ShardPoolBuilder[T] {
	b.cfg = fn(b.cfg)
	return b
}

// Shards 设置分片数
func (b *ShardPoolBuilder[T]) Shards(n int) *ShardPoolBuilder[T] {
	b.cfg.Shards = n
	return b
}

// Worker 设置每个分片的 worker 数量
func (b *ShardPoolBuilder[T]) Worker(n int) *ShardPoolBuilder[T] {
	b.cfg.SizePerShard = n
	return b
}

// Distribution 设置分发策略
func (b *ShardPoolBuilder[T]) Distribution(d Distribution) *ShardPoolBuilder[T] {
	b.cfg.Distribution = d
	return b
}

// KeyFn 设置分片 key 函数（Hash 模式下使用）
func (b *ShardPoolBuilder[T]) KeyFn(fn func(T) uint64) *ShardPoolBuilder[T] {
	b.cfg.KeyFn = fn
	return b
}

// Timeout 设置任务超时
func (b *ShardPoolBuilder[T]) Timeout(d time.Duration) *ShardPoolBuilder[T] {
	b.cfg.PoolCfg.Timeout = d
	return b
}

// FailFast 启用快速失败模式
func (b *ShardPoolBuilder[T]) FailFast() *ShardPoolBuilder[T] {
	b.cfg.PoolCfg.FailFast = true
	return b
}

// MaxPending 设置最大待处理数
func (b *ShardPoolBuilder[T]) MaxPending(n int) *ShardPoolBuilder[T] {
	b.cfg.PoolCfg.MaxPending = n
	return b
}

// Run 终端方法：创建分片池，执行 fn，fn 返回后自动 Close 所有分片。
func (b *ShardPoolBuilder[T]) Run(fn func(ctx context.Context, sp *ShardedPool[T]) error) error {
	return WithShardCfg[T](b.ctx, b.cfg, func(sp *ShardedPool[T]) error {
		return fn(b.ctx, sp)
	})
}
