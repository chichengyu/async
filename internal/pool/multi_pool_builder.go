package pool

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// MultiPoolBuilder 分片协程池构造器，统一入口为 async.PoolMulti[T]()。
// 内部创建 Pool → 水平分片为 N 份，支持极限高并发（百万~千万 QPS）。
// 支持链式配置，Run 自动创建→执行→Close 所有分片。
type MultiPoolBuilder[T any] struct {
	ctx    context.Context // 请求上下文，自动注入 trace_id
	cfg    Config          // 协程池配置
	shards int             // 水平分片数
	extP   *Pool[T]        // 外部注入的协程池（非 nil 时跳过内部创建）
}

// NewMultiBuilder 创建分片协程池构造器，默认使用 context.Background()。
// 通过 .Context(ctx) 链式设置上下文。
func NewMultiBuilder[T any]() *MultiPoolBuilder[T] {
	return &MultiPoolBuilder[T]{
		ctx: context.Background(),
		cfg: DefaultConfig(),
	}
}

// Context 链式设置上下文
func (b *MultiPoolBuilder[T]) Context(ctx context.Context) *MultiPoolBuilder[T] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

// DefaultMultiPoolConfig 重置为默认配置
func (b *MultiPoolBuilder[T]) DefaultMultiPoolConfig() *MultiPoolBuilder[T] {
	b.cfg = DefaultConfig()
	return b
}

// DefaultShards 使用默认分片数（GOMAXPROCS，最少 2）。
func (b *MultiPoolBuilder[T]) DefaultShards() *MultiPoolBuilder[T] {
	b.shards = 0
	return b
}

// DefaultWorker 使用默认 worker 数量（GOMAXPROCS）。
func (b *MultiPoolBuilder[T]) DefaultWorker() *MultiPoolBuilder[T] {
	b.cfg.Size = core.IO()
	return b
}

// DefaultTimeout 使用默认任务超时（不限时）。
func (b *MultiPoolBuilder[T]) DefaultTimeout() *MultiPoolBuilder[T] {
	b.cfg.Timeout = 0
	return b
}

// DefaultSubmitTimeout 使用默认提交超时（不限时）。
func (b *MultiPoolBuilder[T]) DefaultSubmitTimeout() *MultiPoolBuilder[T] {
	b.cfg.SubmitTimeout = 0
	return b
}

// DefaultFailFast 关闭快速失败模式（默认关闭）。
func (b *MultiPoolBuilder[T]) DefaultFailFast() *MultiPoolBuilder[T] {
	b.cfg.FailFast = false
	return b
}

// DefaultMaxPending 使用默认最大待处理数（0=无限制）。
func (b *MultiPoolBuilder[T]) DefaultMaxPending() *MultiPoolBuilder[T] {
	b.cfg.MaxPending = 0
	return b
}

// DefaultOverflow 使用默认溢出策略（OverflowDrop）。
func (b *MultiPoolBuilder[T]) DefaultOverflow() *MultiPoolBuilder[T] {
	b.cfg.Overflow = core.OverflowDrop
	return b
}

// OverflowDrop 队列满时丢弃新任务（默认策略）。
func (b *MultiPoolBuilder[T]) OverflowDrop() *MultiPoolBuilder[T] {
	b.cfg.Overflow = core.OverflowDrop
	return b
}

// OverflowBlock 队列满时阻塞等待空位。
func (b *MultiPoolBuilder[T]) OverflowBlock() *MultiPoolBuilder[T] {
	b.cfg.Overflow = core.OverflowBlock
	return b
}

// OverflowError 队列满时返回错误。
func (b *MultiPoolBuilder[T]) OverflowError() *MultiPoolBuilder[T] {
	b.cfg.Overflow = core.OverflowError
	return b
}

// DefaultRingBuf 使用默认环形缓冲容量（0=不启用）。
func (b *MultiPoolBuilder[T]) DefaultRingBuf() *MultiPoolBuilder[T] {
	b.cfg.RingBufCap = 0
	return b
}

// DefaultMaxResults 使用默认最大结果数（0=无限制）。
func (b *MultiPoolBuilder[T]) DefaultMaxResults() *MultiPoolBuilder[T] {
	b.cfg.MaxResults = 0
	return b
}

// DefaultStreaming 使用默认流式 buf 大小（0=无缓冲）。
func (b *MultiPoolBuilder[T]) DefaultStreaming() *MultiPoolBuilder[T] {
	b.cfg.Streaming = 0
	return b
}

// Config 函数式配置，允许通过闭包修改 Config
func (b *MultiPoolBuilder[T]) Config(fn func(Config) Config) *MultiPoolBuilder[T] {
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
func (b *MultiPoolBuilder[T]) Overflow(s core.OverflowStrategy) *MultiPoolBuilder[T] {
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

// Pool 注入外部协程池，MultiPool 使用该池进行分片。
// 注入后 Conc/Timeout 等配置方法不再生效。
// 外部池生命周期由 Run 接管：外部池成为 shard[0]，defer mp.Close() 会同时关闭它。
// 调用方不得在 Run 之后再使用该池。
//
// 示例：
//
//	myPool := pool.New[string](8).WithTimeout(5 * time.Second)
//	async.MultiPool[string](ctx).Pool(myPool).Shards(4).Run(func(ctx context.Context, mp *pool.MultiPool[string]) error {
//	    mp.Submit(ctx, fn)
//	    return nil
//	})
func (b *MultiPoolBuilder[T]) Pool(p *Pool[T]) *MultiPoolBuilder[T] {
	b.extP = p
	return b
}

// DefaultPool 恢复为内部自动创建协程池（默认行为）。
func (b *MultiPoolBuilder[T]) DefaultPool() *MultiPoolBuilder[T] {
	b.extP = nil
	return b
}

// Run 终端方法：创建 Pool（或使用外部注入的 Pool）→ 应用配置 → 分片 → 执行 fn → Close 所有分片。
func (b *MultiPoolBuilder[T]) Run(fn func(ctx context.Context, mp *MultiPool[T]) error) error {
	var p *Pool[T]
	if b.extP != nil {
		p = b.extP
	} else {
		p = NewPool[T](b.cfg.Size)
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
	}
	mp := p.Shard(b.shards)
	defer mp.Close()
	return fn(b.ctx, mp)
}
