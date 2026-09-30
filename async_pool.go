package async

import (
	"context"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/shard"
)

// ──────────────────────────── Pool 协程池 ────────────────────────────
//
// Pool 相关 API 统一使用 "Pool" 前缀：
//
//	async.Pool[int]()                   → 协程池构建器（链式）
//	async.PoolNew[int](8)               → 直接创建协程池
//	async.PoolDefault[int]()            → 默认 IO 并发度协程池
//	async.PoolVoid(8)                   → 无返回值协程池
//	async.PoolAutoScale[int](4, nil)     → 自动扩缩容协程池
//	async.PoolSharded[int]()            → 分片协程池构建器
//	async.PoolMulti[int]()              → 分片协程池构建器（MultiPool 风格）
//
// 示例：
//
//	// 构建器模式
//	async.Pool[int]().Context(ctx).Worker(64).Run(fn)
//
//	// 直接创建
//	p := async.PoolNew[int](8)
//	defer p.Close()
//
//	// 无返回值
//	vp := async.PoolVoid(10)
//	defer vp.Close()

// Pool 创建协程池构建器。
// 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。
func Pool[T any]() *PoolBuilder[T] {
	return pool.NewBuilder[T]()
}

// PoolNew 创建泛型协程池。
func PoolNew[T any](size int) *pool.Pool[T] {
	return pool.NewPool[T](size)
}

// PoolDefault 使用默认 IO 并发度创建协程池。
func PoolDefault[T any]() *pool.Pool[T] {
	return pool.DefaultPool[T]()
}

// PoolAutoScale 创建带自动扩缩容的协程池。
func PoolAutoScale[T any](initialSize int, config *core.AutoScaleConfig) *pool.Pool[T] {
	p := pool.NewPool[T](initialSize)
	p.EnableAutoScale(config)
	return p
}

// PoolVoid 创建无返回值协程池。
func PoolVoid(size int) *NoResultPool {
	return pool.NewPool[struct{}](size)
}

// PoolDefaultVoid 使用默认 IO 并发度创建无返回值协程池。
func PoolDefaultVoid() *NoResultPool {
	return pool.DefaultPool[struct{}]()
}

// PoolMulti 创建分片协程池构造器。
// 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。
func PoolMulti[T any]() *MultiPoolBuilder[T] {
	return pool.NewMultiBuilder[T]()
}

// PoolSharded 创建分片池构造器。
// 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。
func PoolSharded[T any]() *ShardPoolBuilder[T] {
	return shard.NewPoolBuilder[T]()
}

// PoolNewSharded 创建分片池。
func PoolNewSharded[T any](cfg ShardPoolConfig[T]) *shard.ShardedPool[T] {
	return shard.NewShardedPool(cfg)
}

// PoolDefaultSharded 使用默认配置创建分片池。
func PoolDefaultSharded[T any]() *shard.ShardedPool[T] {
	return shard.DefaultShardedPool[T]()
}

// PoolNewShardedSimple 用简单参数创建分片池。
func PoolNewShardedSimple[T any](shards int, sizePerShard int) *shard.ShardedPool[T] {
	return shard.NewShardedPoolSimple[T](shards, sizePerShard)
}

// PoolDefaultShardedWith 用自定义分片数创建分片池。
func PoolDefaultShardedWith[T any](shards int) *shard.ShardedPool[T] {
	return shard.DefaultShardedPoolWith[T](shards)
}

// PoolNewAutoScaleSharded 创建带自动扩缩容的分片池。
func PoolNewAutoScaleSharded[T any](shards int, initialSizePerShard int, config *AutoScaleConfig) *shard.ShardedPool[T] {
	return shard.NewAutoScaleShardedPool[T](shards, initialSizePerShard, config)
}

// PoolWith 创建协程池并执行 fn，fn 返回后自动 Close。
func PoolWith[T any](size int, fn func(p *pool.Pool[T]) error) error {
	return pool.WithPool[T](size, fn)
}

// PoolWithCfg 使用 PoolConfig 创建协程池并执行 fn。
func PoolWithCfg[T any](ctx context.Context, cfg PoolConfig, fn func(p *pool.Pool[T]) error) error {
	return pool.WithCfg[T](ctx, cfg, fn)
}

// PoolWithMulti 创建分片协程池并执行 fn。
func PoolWithMulti[T any](size int, shards int, fn func(mp *pool.MultiPool[T]) error) error {
	return pool.WithMultiPool[T](size, shards, fn)
}

// PoolWithSharded 创建分片池并执行 fn。
func PoolWithSharded[T any](shards int, sizePerShard int, fn func(sp *shard.ShardedPool[T]) error) error {
	return shard.WithShardedPool[T](shards, sizePerShard, fn)
}

// PoolWithShardedCfg 使用 ShardPoolConfig 创建分片池并执行 fn。
func PoolWithShardedCfg[T any](ctx context.Context, cfg ShardPoolConfig[T], fn func(sp *shard.ShardedPool[T]) error) error {
	return shard.WithShardCfg[T](ctx, cfg, fn)
}

// PoolShard 将 Pool 水平分片为 N 个实例。
func PoolShard[T any](p *pool.Pool[T], shards int) *pool.MultiPool[T] {
	return p.Shard(shards)
}

// PoolDefaultShard 使用默认分片数对 Pool 进行水平分片。
func PoolDefaultShard[T any](p *pool.Pool[T]) *pool.MultiPool[T] {
	return p.DefaultShard()
}
