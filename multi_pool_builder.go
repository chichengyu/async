package async

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// ──────────────────────────── MultiPool 分片协程池 ────────────────────────────

// MultiPoolBuilder 分片协程池构造器，统一入口为 MultiPool[T](ctx)。
// 内部创建 Pool → 水平分片为 N 份，支持极限高并发（百万~千万 QPS）。
// 支持链式配置，Run 自动创建→执行→Close 所有分片。
//
// 示例：
//
//	async.MultiPool[string](ctx).
//	    Shards(8).
//	    Worker(64).
//	    FailFast().
//	    Timeout(5 * time.Second).
//	    Run(func(ctx context.Context, mp *pool.MultiPool[string]) error {
//	        for _, item := range items {
//	            mp.Submit(ctx, func(ctx context.Context) (string, error) {
//	                return process(item)
//	            })
//	        }
//	        results := mp.Wait()
//	        return check(results)
//	    })
type MultiPoolBuilder[T any] struct {
	ctx    context.Context
	cfg    pool.Config
	shards int
}

// MultiPool 创建分片协程池构造器，内部自动注入 trace_id。
func MultiPool[T any](ctx context.Context) *MultiPoolBuilder[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return &MultiPoolBuilder[T]{
		ctx: core.EnsureTraceID(ctx),
		cfg: pool.DefaultConfig(),
	}
}

// MultiPoolBG 无上下文快捷构造，内部使用 context.Background()。
func MultiPoolBG[T any]() *MultiPoolBuilder[T] {
	return MultiPool[T](context.Background())
}

// Context 链式设置上下文
func (b *MultiPoolBuilder[T]) Context(ctx context.Context) *MultiPoolBuilder[T] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

// DefaultMultiPoolConfig 重置为默认配置
func (b *MultiPoolBuilder[T]) DefaultMultiPoolConfig() *MultiPoolBuilder[T] {
	b.cfg = pool.DefaultConfig()
	return b
}

// Config 函数式配置，允许通过闭包修改 PoolConfig
func (b *MultiPoolBuilder[T]) Config(fn func(PoolConfig) PoolConfig) *MultiPoolBuilder[T] {
	b.cfg = fn(b.cfg)
	return b
}

// Shards 设置分片数
func (b *MultiPoolBuilder[T]) Shards(n int) *MultiPoolBuilder[T] {
	b.shards = n
	return b
}

// Worker 设置 worker 数量
func (b *MultiPoolBuilder[T]) Worker(n int) *MultiPoolBuilder[T] {
	b.cfg.Size = n
	return b
}

// Timeout 设置任务超时
func (b *MultiPoolBuilder[T]) Timeout(d time.Duration) *MultiPoolBuilder[T] {
	b.cfg.Timeout = d
	return b
}

// SubmitTimeout 设置提交超时
func (b *MultiPoolBuilder[T]) SubmitTimeout(d time.Duration) *MultiPoolBuilder[T] {
	b.cfg.SubmitTimeout = d
	return b
}

// FailFast 启用快速失败模式
func (b *MultiPoolBuilder[T]) FailFast() *MultiPoolBuilder[T] {
	b.cfg.FailFast = true
	return b
}

// MaxPending 设置最大待处理数
func (b *MultiPoolBuilder[T]) MaxPending(n int) *MultiPoolBuilder[T] {
	b.cfg.MaxPending = n
	return b
}

// Overflow 设置队列溢出策略
func (b *MultiPoolBuilder[T]) Overflow(s OverflowStrategy) *MultiPoolBuilder[T] {
	b.cfg.Overflow = s
	return b
}

// RingBuf 设置环形缓冲容量
func (b *MultiPoolBuilder[T]) RingBuf(cap int) *MultiPoolBuilder[T] {
	b.cfg.RingBufCap = cap
	return b
}

// MaxResults 设置最大结果数
func (b *MultiPoolBuilder[T]) MaxResults(n int) *MultiPoolBuilder[T] {
	b.cfg.MaxResults = n
	return b
}

// Streaming 设置流式 buf 大小
func (b *MultiPoolBuilder[T]) Streaming(buf int) *MultiPoolBuilder[T] {
	b.cfg.Streaming = buf
	return b
}

// Run 终端方法：创建 Pool → 应用配置 → 分片 → 执行 fn → Close 所有分片。
func (b *MultiPoolBuilder[T]) Run(fn func(ctx context.Context, mp *pool.MultiPool[T]) error) error {
	p := pool.NewPool[T](b.cfg.Size)

	if b.cfg.Timeout > 0 {
		p.WithTimeout(b.cfg.Timeout)
	}
	if b.cfg.FailFast {
		p.WithFailFast(b.ctx)
	}
	if b.cfg.SubmitTimeout > 0 {
		p.WithSubmitTimeout(b.cfg.SubmitTimeout)
	}
	if b.cfg.MaxPending > 0 {
		p.WithMaxPending(b.cfg.MaxPending)
	}
	if b.cfg.Overflow != 0 {
		p.WithOverflow(b.cfg.Overflow)
	}
	if b.cfg.RingBufCap > 0 {
		p.WithRingBuffer(b.cfg.RingBufCap, core.GetDefaultOverflowStrategy())
	}
	if b.cfg.MaxResults >= 0 {
		p.WithMaxResults(b.cfg.MaxResults)
	}
	if b.cfg.Streaming > 0 {
		p.WithStreaming(b.cfg.Streaming)
	}

	mp := p.Shard(b.shards)
	defer mp.Close()
	return fn(b.ctx, mp)
}
