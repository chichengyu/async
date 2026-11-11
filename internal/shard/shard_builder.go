package shard

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ShardPoolBuilder 分片池构造器，统一入口为 async.PoolSharded[T]()。
// 支持链式配置和函数式配置，Run 自动创建分片池→执行→Close。
type ShardPoolBuilder[T any] struct {
	ctx context.Context    // 请求上下文，自动注入 trace_id
	cfg ShardPoolConfig[T] // 分片池配置
}

// NewPoolBuilder 创建分片池构造器，默认使用 context.Background()。
// 通过 .Context(ctx) 链式设置上下文。
func NewPoolBuilder[T any]() *ShardPoolBuilder[T] {
	return &ShardPoolBuilder[T]{
		ctx: context.Background(),
		cfg: DefaultShardConfig[T](),
	}
}

// Context 链式设置上下文，自动注入 trace_id。
func (b *ShardPoolBuilder[T]) Context(ctx context.Context) *ShardPoolBuilder[T] {
	b.ctx = core.EnsureTraceID(core.CopyLogger(b.ctx, ctx))
	return b
}

// DefaultShardPoolConfig 重置为默认配置
func (b *ShardPoolBuilder[T]) DefaultShardPoolConfig() *ShardPoolBuilder[T] {
	b.cfg = DefaultShardConfig[T]()
	return b
}

// DefaultShards 使用默认分片数（4）。
func (b *ShardPoolBuilder[T]) DefaultShards() *ShardPoolBuilder[T] {
	b.cfg.Shards = DefaultShardCount
	return b
}

// DefaultWorker 清除每片 worker 覆盖，创建时使用 core.IO()（NumCPU×2）。
func (b *ShardPoolBuilder[T]) DefaultWorker() *ShardPoolBuilder[T] {
	b.cfg.SizePerShard = DefaultSizePerShard
	return b
}

// DefaultDistribution 使用默认分发策略（RoundRobin）。
func (b *ShardPoolBuilder[T]) DefaultDistribution() *ShardPoolBuilder[T] {
	b.cfg.Distribution = DefaultDistribution
	return b
}

// DefaultTimeout 清除分片配置中的任务超时覆盖，保留创建各池时的全局默认任务超时。
func (b *ShardPoolBuilder[T]) DefaultTimeout() *ShardPoolBuilder[T] {
	b.cfg.PoolCfg.Timeout = 0
	return b
}

// DefaultFailFast 关闭快速失败模式（默认关闭）。
func (b *ShardPoolBuilder[T]) DefaultFailFast() *ShardPoolBuilder[T] {
	b.cfg.PoolCfg.FailFast = false
	return b
}

// DefaultMaxPending 使用默认最大待处理数（0=无限制）。
func (b *ShardPoolBuilder[T]) DefaultMaxPending() *ShardPoolBuilder[T] {
	b.cfg.PoolCfg.MaxPending = 0
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

// KeyFn 设置 Hash 模式下批量提交的元素路由函数。
// Submit 不接收元素，Hash 模式下使用第 0 片；单项路由可使用 SubmitKeyed。
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

// MaxResults 设置结果存储上限（0=无限）
func (b *ShardPoolBuilder[T]) MaxResults(n int) *ShardPoolBuilder[T] {
	b.cfg.PoolCfg.MaxResults = n
	return b
}

// DefaultMaxResults 使用默认最大结果数（无限）
func (b *ShardPoolBuilder[T]) DefaultMaxResults() *ShardPoolBuilder[T] {
	b.cfg.PoolCfg.MaxResults = -1
	return b
}

// Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。
func (b *ShardPoolBuilder[T]) Logger(l core.Logger) *ShardPoolBuilder[T] {
	b.ctx = core.WithLogger(b.ctx, l)
	return b
}

// DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。
func (b *ShardPoolBuilder[T]) DefaultLogger() *ShardPoolBuilder[T] {
	b.ctx = core.WithLogger(b.ctx, nil)
	return b
}

// Run 终端方法：创建分片池，执行 fn，fn 返回后自动 Close 所有分片。
// FailFast 时 fn 接收跨片共享 context；各片正常 Wait 不提前取消它，整体收尾后释放。
func (b *ShardPoolBuilder[T]) Run(fn func(ctx context.Context, sp *ShardedPool[T]) error) error {
	return WithShardCfg[T](b.ctx, b.cfg, func(sp *ShardedPool[T]) error {
		ctx := b.ctx
		if b.cfg.PoolCfg.FailFast {
			ctx = sp.ffCtx
		}
		return fn(ctx, sp)
	})
}
