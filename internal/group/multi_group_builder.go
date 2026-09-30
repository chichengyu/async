package group

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// MultiGroupBuilder 分片任务组构造器，统一入口为 async.GroupMulti[T]()。
// 内部创建 Group → 水平分片为 N 份，支持极限高并发。
// 支持链式配置，Run 自动创建→执行→Close 所有分片。
type MultiGroupBuilder[T any] struct {
	ctx           context.Context
	concurrency   int
	shards        int
	timeout       time.Duration
	submitTimeout time.Duration
	failFast      bool
}

// NewMultiBuilder 创建分片任务组构造器，默认使用 context.Background()。
// 通过 .Context(ctx) 链式设置上下文。
func NewMultiBuilder[T any]() *MultiGroupBuilder[T] {
	return &MultiGroupBuilder[T]{
		ctx: context.Background(),
	}
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

// Worker 设置并发度
func (b *MultiGroupBuilder[T]) Worker(n int) *MultiGroupBuilder[T] {
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
func (b *MultiGroupBuilder[T]) Run(fn func(ctx context.Context, mg *MultiGroup[T]) error) error {
	g := NewGroup[T](b.concurrency)
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
