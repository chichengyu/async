package async

import (
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/shard"
)

// ──────────────────────────── Pool 协程池 ────────────────────────────
//
// Pool 相关 API 统一使用 "Pool" 前缀链式调用：
//
//	async.Pool[int]()                   → 协程池构建器（链式）
//	async.PoolSharded[int]()            → 分片协程池构建器
//	async.PoolMulti[int]()              → 分片协程池构建器（MultiPool 风格）
//
// 示例：
//
//	async.Pool[int]().Context(ctx).Worker(64).Run(fn)

// Pool 创建协程池构建器。
// 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。
func Pool[T any]() *PoolBuilder[T] {
	return pool.NewBuilder[T]()
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
