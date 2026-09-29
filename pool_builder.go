package async

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// ──────────────────────────── Pool 协程池 ────────────────────────────

// PoolBuilder 协程池构造器，统一入口为 Pool[T](ctx)。
// 支持链式配置和函数式配置，Run 自动创建池→执行→Close。
//
// 示例：
//
//	async.Pool[string](ctx).
//	    Worker(64).
//	    FailFast().
//	    Timeout(5 * time.Second).
//	    Run(func(ctx context.Context, p *pool.Pool[string]) error {
//	        for _, item := range items {
//	            p.Submit(ctx, func(ctx context.Context) (string, error) {
//	                return process(ctx, item)
//	            })
//	        }
//	        results := p.Wait()
//	        return check(results)
//	    })
type PoolBuilder[T any] struct {
	ctx context.Context
	cfg pool.Config
}

// Pool 创建协程池构造器，内部自动注入 trace_id。
// 通过链式方法配置后调用 Run 执行。
func Pool[T any](ctx context.Context) *PoolBuilder[T] {
	return &PoolBuilder[T]{ctx: core.EnsureTraceID(ctx), cfg: pool.DefaultConfig()}
}

// DefaultPoolConfig 重置为默认配置
func (b *PoolBuilder[T]) DefaultPoolConfig() *PoolBuilder[T] {
	b.cfg = pool.DefaultConfig()
	return b
}

// Config 函数式配置，允许通过闭包修改 PoolConfig
func (b *PoolBuilder[T]) Config(fn func(PoolConfig) PoolConfig) *PoolBuilder[T] {
	b.cfg = fn(b.cfg)
	return b
}

// Worker 设置 worker 数量
func (b *PoolBuilder[T]) Worker(n int) *PoolBuilder[T] {
	b.cfg.Size = n
	return b
}

// Timeout 设置任务超时
func (b *PoolBuilder[T]) Timeout(d time.Duration) *PoolBuilder[T] {
	b.cfg.Timeout = d
	return b
}

// SubmitTimeout 设置提交超时
func (b *PoolBuilder[T]) SubmitTimeout(d time.Duration) *PoolBuilder[T] {
	b.cfg.SubmitTimeout = d
	return b
}

// FailFast 启用快速失败模式
func (b *PoolBuilder[T]) FailFast() *PoolBuilder[T] {
	b.cfg.FailFast = true
	return b
}

// MaxPending 设置最大待处理数
func (b *PoolBuilder[T]) MaxPending(n int) *PoolBuilder[T] {
	b.cfg.MaxPending = n
	return b
}

// Overflow 设置队列溢出策略
func (b *PoolBuilder[T]) Overflow(s OverflowStrategy) *PoolBuilder[T] {
	b.cfg.Overflow = s
	return b
}

// RingBuf 设置环形缓冲容量
func (b *PoolBuilder[T]) RingBuf(cap int) *PoolBuilder[T] {
	b.cfg.RingBufCap = cap
	return b
}

// MaxResults 设置最大结果数
func (b *PoolBuilder[T]) MaxResults(n int) *PoolBuilder[T] {
	b.cfg.MaxResults = n
	return b
}

// Streaming 设置流式 buf 大小
func (b *PoolBuilder[T]) Streaming(buf int) *PoolBuilder[T] {
	b.cfg.Streaming = buf
	return b
}

// Run 终端方法：创建协程池，执行 fn，fn 返回后自动 Close。
func (b *PoolBuilder[T]) Run(fn func(ctx context.Context, p *pool.Pool[T]) error) error {
	return pool.WithCfg[T](b.ctx, b.cfg, func(p *pool.Pool[T]) error {
		return fn(b.ctx, p)
	})
}
