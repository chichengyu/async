package pool

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// PoolBuilder 协程池构造器，统一入口为 async.Pool[T]()。
// 支持链式配置和函数式配置，Run 自动创建池→执行→Close。
type PoolBuilder[T any] struct {
	ctx    context.Context // 请求上下文，自动注入 trace_id
	cfg    Config          // 协程池配置
	logger core.Logger     // 自定义日志
}

// NewBuilder 创建协程池构造器，默认使用 context.Background()。
// 通过 .Context(ctx) 链式设置上下文。
func NewBuilder[T any]() *PoolBuilder[T] {
	return &PoolBuilder[T]{ctx: context.Background(), cfg: DefaultConfig()}
}

// Context 链式设置上下文，自动注入 trace_id。
func (b *PoolBuilder[T]) Context(ctx context.Context) *PoolBuilder[T] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

// DefaultPoolConfig 重置为默认配置
func (b *PoolBuilder[T]) DefaultPoolConfig() *PoolBuilder[T] {
	b.cfg = DefaultConfig()
	return b
}

// DefaultWorker 使用默认 worker 数量（GOMAXPROCS）。
func (b *PoolBuilder[T]) DefaultWorker() *PoolBuilder[T] {
	b.cfg.Size = core.IO()
	return b
}

// DefaultTimeout 使用默认任务超时（不限时）。
func (b *PoolBuilder[T]) DefaultTimeout() *PoolBuilder[T] {
	b.cfg.Timeout = 0
	return b
}

// DefaultSubmitTimeout 使用默认提交超时（不限时）。
func (b *PoolBuilder[T]) DefaultSubmitTimeout() *PoolBuilder[T] {
	b.cfg.SubmitTimeout = 0
	return b
}

// DefaultFailFast 关闭快速失败模式（默认关闭）。
func (b *PoolBuilder[T]) DefaultFailFast() *PoolBuilder[T] {
	b.cfg.FailFast = false
	return b
}

// DefaultMaxPending 使用默认最大待处理数（0=无限制）。
func (b *PoolBuilder[T]) DefaultMaxPending() *PoolBuilder[T] {
	b.cfg.MaxPending = 0
	return b
}

// DefaultOverflow 使用默认溢出策略（OverflowDrop）。
func (b *PoolBuilder[T]) DefaultOverflow() *PoolBuilder[T] {
	b.cfg.Overflow = core.OverflowDrop
	return b
}

// OverflowDrop 队列满时丢弃新任务（默认策略）。
func (b *PoolBuilder[T]) OverflowDrop() *PoolBuilder[T] {
	b.cfg.Overflow = core.OverflowDrop
	return b
}

// OverflowBlock 队列满时阻塞等待空位。
func (b *PoolBuilder[T]) OverflowBlock() *PoolBuilder[T] {
	b.cfg.Overflow = core.OverflowBlock
	return b
}

// OverflowError 队列满时返回错误。
func (b *PoolBuilder[T]) OverflowError() *PoolBuilder[T] {
	b.cfg.Overflow = core.OverflowError
	return b
}

// DefaultRingBuf 使用默认环形缓冲容量（0=不启用）。
func (b *PoolBuilder[T]) DefaultRingBuf() *PoolBuilder[T] {
	b.cfg.RingBufCap = 0
	return b
}

// DefaultMaxResults 使用默认最大结果数（100_000）。
func (b *PoolBuilder[T]) DefaultMaxResults() *PoolBuilder[T] {
	b.cfg.MaxResults = -1
	return b
}

// DefaultStreaming 使用默认流式 buf 大小（0=无缓冲）。
func (b *PoolBuilder[T]) DefaultStreaming() *PoolBuilder[T] {
	b.cfg.Streaming = 0
	return b
}

// Config 函数式配置，允许通过闭包修改 Config
func (b *PoolBuilder[T]) Config(fn func(Config) Config) *PoolBuilder[T] {
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
func (b *PoolBuilder[T]) Overflow(s core.OverflowStrategy) *PoolBuilder[T] {
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

// Logger 注入自定义日志实现，全局生效。
func (b *PoolBuilder[T]) Logger(l core.Logger) *PoolBuilder[T] { core.SetLogger(l); return b }

// DefaultLogger 重置为默认日志实现。
func (b *PoolBuilder[T]) DefaultLogger() *PoolBuilder[T] { core.SetLogger(nil); return b }

// Run 终端方法：创建协程池，执行 fn，fn 返回后自动 Close。
func (b *PoolBuilder[T]) Run(fn func(ctx context.Context, p *Pool[T]) error) error {
	return WithCfg[T](b.ctx, b.cfg, func(p *Pool[T]) error {
		return fn(b.ctx, p)
	})
}
