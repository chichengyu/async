package async

import (
	"github.com/chichengyu/async/internal/group"
	"github.com/chichengyu/async/internal/shard"
)

// ──────────────────────────── Group 任务组 ────────────────────────────
//
// Group 相关 API 统一使用 "Group" 前缀：
//
//	async.Group[int]()                    → 带返回值构建器
//	async.GroupVoid()                     → 无返回值构建器
//	async.GroupSharded[int]()             → 分片构建器
//	async.GroupMulti[int]()               → 分片构建器（MultiPool 风格）
//
// 示例：
//
//	// 带返回值
//	g := async.Group[int]().Context(ctx).Concurrency(10).Build()
//
//	// 无返回值
//	nr := async.GroupVoid().Context(ctx).Concurrency(10).Build()
//
//	// 分片
//	sh := async.GroupSharded[int]().Context(ctx).Shards(8).Build()

// Group 创建带返回值的任务组构造器。
// 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。
func Group[T any]() *GroupBuilder[T] {
	return group.NewGroupBuilder[T]()
}

// GroupVoid 创建无返回值任务组构造器。
// 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。
func GroupVoid() *GroupNoResultBuilder {
	return group.NewGroupNoResultBuilder()
}

// GroupSharded 创建分片任务组构造器。
// 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。
func GroupSharded[T any]() *ShardedGroupBuilder[T] {
	return shard.NewGroupBuilder[T]()
}

// GroupMulti 创建分片任务组构造器（MultiPool 风格）。
// 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。
func GroupMulti[T any]() *MultiGroupBuilder[T] {
	return group.NewMultiBuilder[T]()
}

// GroupNewSharded 创建分片 Group。
func GroupNewSharded[T any](cfg ShardGroupConfig[T]) *shard.ShardedGroup[T] {
	return shard.NewShardedGroup(cfg)
}

// GroupDefaultSharded 使用默认配置创建分片 Group。
func GroupDefaultSharded[T any]() *shard.ShardedGroup[T] {
	return shard.DefaultShardedGroup[T]()
}
