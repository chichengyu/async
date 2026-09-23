package async

import (
	"context"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/group"
	"github.com/chichengyu/async/pool"
	"github.com/chichengyu/async/ratelimit"
	"github.com/chichengyu/async/task"
)

// ============================================================
// PoolBuilder —— 协程池构建器
// ============================================================

// PoolBuilder 链式构建 Pool，嵌入底层 Pool 的所有方法。
//
// 使用示例：
//
//	p := NewPoolBuilder[string](8).
//	    WithTimeoutCh(5 * time.Second).
//	    WithStreamingCh(256)
//
//	p.Submit(ctx, fn)
//	results := p.Wait()
type PoolBuilder[T any] struct {
	*Pool[T]
	errs []error
}

// NewPoolBuilder 创建协程池构建器。
// size <= 0 使用默认 IO 并发度。
func NewPoolBuilder[T any](size int) *PoolBuilder[T] {
	if size <= 0 {
		size = core.IO()
	}
	return &PoolBuilder[T]{Pool: NewPool[T](size)}
}

// DefaultPoolBuilder 使用默认 IO 并发度创建。
func DefaultPoolBuilder[T any]() *PoolBuilder[T] {
	return &PoolBuilder[T]{Pool: DefaultPool[T]()}
}

// NewAutoScalePoolBuilder 创建带自动扩缩容的协程池构建器。
// initialSize <= 0 使用默认 IO 并发度，cfg 为 nil 时使用 DefaultAutoScaleConfig。
func NewAutoScalePoolBuilder[T any](initialSize int, cfg *core.AutoScaleConfig) *PoolBuilder[T] {
	if initialSize <= 0 {
		initialSize = core.IO()
	}
	if cfg == nil {
		cfg = core.DefaultAutoScaleConfig()
	}
	return &PoolBuilder[T]{Pool: NewAutoScalePool[T](initialSize, cfg)}
}

// DefaultAutoScalePoolBuilder 使用默认配置创建自动扩缩容池（IO 并发度 + DefaultAutoScaleConfig）。
func DefaultAutoScalePoolBuilder[T any]() *PoolBuilder[T] {
	return NewAutoScalePoolBuilder[T](core.IO(), core.DefaultAutoScaleConfig())
}

// ── Pool 配置链式方法（覆盖嵌入方法，返回 *PoolBuilder 以保持链式） ──

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
//
//	logger := myZerologAdapter{}
//	p := NewPoolBuilder[int](8).WithLoggerCh(logger).WithTimeoutCh(5*time.Second)
//
// 传入 nil 恢复默认静默日志。
func (p *PoolBuilder[T]) WithLoggerCh(l core.Logger) *PoolBuilder[T] {
	core.SetLogger(l)
	return p
}

// WithTimeoutCh 设置任务总超时，返回构建器以支持链式调用。
func (p *PoolBuilder[T]) WithTimeoutCh(d time.Duration) *PoolBuilder[T] {
	p.Pool.WithTimeout(d)
	return p
}

// WithTimeoutChDefault 使用默认超时（30s）设置任务超时。
func (p *PoolBuilder[T]) WithTimeoutChDefault() *PoolBuilder[T] {
	p.Pool.WithTimeout(30 * time.Second)
	return p
}

// WithSubmitTimeoutCh 设置 Submit 等待空闲 worker 的超时，返回构建器。
func (p *PoolBuilder[T]) WithSubmitTimeoutCh(d time.Duration) *PoolBuilder[T] {
	p.Pool.WithSubmitTimeout(d)
	return p
}

// WithSubmitTimeoutChDefault 使用默认提交超时（5s）。
func (p *PoolBuilder[T]) WithSubmitTimeoutChDefault() *PoolBuilder[T] {
	p.Pool.WithSubmitTimeout(5 * time.Second)
	return p
}

// WithStreamingCh 启用流式结果消费，返回构建器。
func (p *PoolBuilder[T]) WithStreamingCh(bufSize int) *PoolBuilder[T] {
	p.Pool.WithStreaming(bufSize)
	return p
}

// WithStreamingChDefault 用默认缓冲大小（concurrency*2）启用流式结果消费。
func (p *PoolBuilder[T]) WithStreamingChDefault() *PoolBuilder[T] {
	p.Pool.WithStreaming(0)
	return p
}

// WithResultCallbackCh 设置结果回调，返回构建器。
func (p *PoolBuilder[T]) WithResultCallbackCh(fn func(core.Result[T])) *PoolBuilder[T] {
	p.Pool.WithResultCallback(fn)
	return p
}

// WithRingBufferCh 设置环形缓冲区，返回构建器。
func (p *PoolBuilder[T]) WithRingBufferCh(capacity int, overflow core.OverflowStrategy) *PoolBuilder[T] {
	p.Pool.WithRingBuffer(capacity, overflow)
	return p
}

// WithRingBufferChDefault 使用默认环形缓冲区设置（4096, DropOldest）。
func (p *PoolBuilder[T]) WithRingBufferChDefault() *PoolBuilder[T] {
	p.Pool.WithRingBuffer(4096, core.OverflowDrop)
	return p
}

// WithMaxResultsCh 设置最大结果数，返回构建器。
func (p *PoolBuilder[T]) WithMaxResultsCh(maxResults int) *PoolBuilder[T] {
	p.Pool.WithMaxResults(maxResults)
	return p
}

// WithMaxPendingCh 设置最大待处理任务数，返回构建器。
func (p *PoolBuilder[T]) WithMaxPendingCh(maxPending int) *PoolBuilder[T] {
	p.Pool.WithMaxPending(maxPending)
	return p
}

// WithOverflowCh 设置溢出策略，返回构建器。
func (p *PoolBuilder[T]) WithOverflowCh(strategy core.OverflowStrategy) *PoolBuilder[T] {
	p.Pool.WithOverflow(strategy)
	return p
}

// ── Pool 上下文配置链式方法 ──

// WithContextCh 设置上下文并返回 (构建器, 新上下文) 以支持链式。
func (p *PoolBuilder[T]) WithContextCh(ctx context.Context) (*PoolBuilder[T], context.Context) {
	p.Pool.WithContext(ctx)
	return p, ctx
}

// WithFailFastCh 启用 FailFast 模式并返回 (构建器, failFast上下文)。
func (p *PoolBuilder[T]) WithFailFastCh(ctx context.Context) (*PoolBuilder[T], context.Context) {
	p.Pool.WithFailFast(ctx)
	return p, ctx
}

// WithFFCtxCh 启用 FailFast（仅返回构建器上下文对）。
func (p *PoolBuilder[T]) WithFFCtxCh(ctx context.Context) (*PoolBuilder[T], context.Context) {
	p.Pool.WithFFCtx(ctx)
	return p, ctx
}

// WithFFSubmitTOCh 启用 FailFast 并设置提交超时。
func (p *PoolBuilder[T]) WithFFSubmitTOCh(ctx context.Context, submitTimeout time.Duration) (*PoolBuilder[T], context.Context) {
	p.Pool.WithFFSubmitTO(ctx, submitTimeout)
	return p, ctx
}

// WithFFTimeoutCh 启用 FailFast 并设置任务超时。
func (p *PoolBuilder[T]) WithFFTimeoutCh(ctx context.Context, timeout time.Duration) (*PoolBuilder[T], context.Context) {
	p.Pool.WithFFTimeout(ctx, timeout)
	return p, ctx
}

// WithCtxTimeoutCh 设置上下文并启用任务超时。
func (p *PoolBuilder[T]) WithCtxTimeoutCh(ctx context.Context, timeout time.Duration) (*PoolBuilder[T], context.Context) {
	p.Pool.WithCtxTimeout(ctx, timeout)
	return p, ctx
}

// WithCtxSubmitTOCh 设置上下文并启用提交超时。
func (p *PoolBuilder[T]) WithCtxSubmitTOCh(ctx context.Context, submitTimeout time.Duration) (*PoolBuilder[T], context.Context) {
	p.Pool.WithCtxSubmitTO(ctx, submitTimeout)
	return p, ctx
}

// WithFFTimeoutSubmitTOCh 启用 FailFast + 任务超时 + 提交超时。
func (p *PoolBuilder[T]) WithFFTimeoutSubmitTOCh(ctx context.Context, timeout, submitTimeout time.Duration) (*PoolBuilder[T], context.Context) {
	p.Pool.WithFFTimeoutSubmitTO(ctx, timeout, submitTimeout)
	return p, ctx
}

// WithTraceIDCh 设置 trace_id 上下文。
func (p *PoolBuilder[T]) WithTraceIDCh(ctx context.Context) (*PoolBuilder[T], context.Context) {
	p.Pool.WithTraceID(ctx)
	return p, ctx
}

// WithFFTraceIDCh 启用 FailFast + trace_id。
func (p *PoolBuilder[T]) WithFFTraceIDCh(ctx context.Context) (*PoolBuilder[T], context.Context) {
	p.Pool.WithFFTraceID(ctx)
	return p, ctx
}

// WithFFSubmitTOTraceIDCh 启用 FailFast + 提交超时 + trace_id。
func (p *PoolBuilder[T]) WithFFSubmitTOTraceIDCh(ctx context.Context, submitTimeout time.Duration) (*PoolBuilder[T], context.Context) {
	p.Pool.WithFFSubmitTOTraceID(ctx, submitTimeout)
	return p, ctx
}

// WithCtxTraceIDCh 设置上下文 + trace_id。
func (p *PoolBuilder[T]) WithCtxTraceIDCh(ctx context.Context) (*PoolBuilder[T], context.Context) {
	p.Pool.WithCtxTraceID(ctx)
	return p, ctx
}

// WithFFTimeoutTraceIDCh 启用 FailFast + 超时 + trace_id。
func (p *PoolBuilder[T]) WithFFTimeoutTraceIDCh(ctx context.Context, timeout time.Duration) (*PoolBuilder[T], context.Context) {
	p.Pool.WithFFTimeoutTraceID(ctx, timeout)
	return p, ctx
}

// WithFFTimeoutSubmitTOTraceIDCh 启用 FailFast + 超时 + 提交超时 + trace_id。
func (p *PoolBuilder[T]) WithFFTimeoutSubmitTOTraceIDCh(ctx context.Context, timeout, submitTimeout time.Duration) (*PoolBuilder[T], context.Context) {
	p.Pool.WithFFTimeoutSubmitTOTraceID(ctx, timeout, submitTimeout)
	return p, ctx
}

// WithCtxTimeoutTraceIDCh 设置上下文 + 超时 + trace_id。
func (p *PoolBuilder[T]) WithCtxTimeoutTraceIDCh(ctx context.Context, timeout time.Duration) (*PoolBuilder[T], context.Context) {
	p.Pool.WithCtxTimeoutTraceID(ctx, timeout)
	return p, ctx
}

// WithCtxSubmitTOTraceIDCh 设置上下文 + 提交超时 + trace_id。
func (p *PoolBuilder[T]) WithCtxSubmitTOTraceIDCh(ctx context.Context, submitTimeout time.Duration) (*PoolBuilder[T], context.Context) {
	p.Pool.WithCtxSubmitTOTraceID(ctx, submitTimeout)
	return p, ctx
}

// ── PoolBuilder 错误累积与链式 Action ──

// Error 返回 Submit 过程中累积的第一个错误，无错误返回 nil。
// 链式 Submit 不会因错误中断，需调用此方法检查。
func (p *PoolBuilder[T]) Error() error {
	if len(p.errs) == 0 {
		return nil
	}
	return p.errs[0]
}

// Errors 返回所有累积的错误。
func (p *PoolBuilder[T]) Errors() []error {
	return p.errs
}

// SubmitCh 提交任务并捕获错误，返回构建器以支持链式调用。
//
//	results := NewPoolBuilder[string](8).
//	    SubmitCh(ctx, fn1).
//	    SubmitCh(ctx, fn2).
//	    Wait()
//	if err := poolBuilder.Error(); err != nil { ... }
func (p *PoolBuilder[T]) SubmitCh(ctx context.Context, fn func(context.Context) (T, error)) *PoolBuilder[T] {
	if err := p.Pool.Submit(ctx, fn); err != nil {
		p.errs = append(p.errs, err)
	}
	return p
}

// SubmitAtCh 在指定索引位置提交任务，捕获错误，返回构建器。
func (p *PoolBuilder[T]) SubmitAtCh(index int, ctx context.Context, fn func(context.Context) (T, error)) *PoolBuilder[T] {
	if err := p.Pool.SubmitAt(index, ctx, fn); err != nil {
		p.errs = append(p.errs, err)
	}
	return p
}

// TrySubmitCh 非阻塞提交任务，失败时捕获错误，返回构建器。
func (p *PoolBuilder[T]) TrySubmitCh(ctx context.Context, fn func(context.Context) (T, error)) *PoolBuilder[T] {
	if err := p.Pool.TrySubmit(ctx, fn); err != nil {
		p.errs = append(p.errs, err)
	}
	return p
}

// ============================================================
// GroupBuilder —— 任务组构建器
// ============================================================

// GroupBuilder 链式构建 Group，嵌入底层 Group 的所有方法。
//
// 使用示例：
//
//	g := NewGroupBuilder[string](10).
//	    WithTimeoutCh(5 * time.Second).
//	    WithStreamingCh(128)
//
//	g.Go(ctx, fn)
//	results := g.Wait()
type GroupBuilder[T any] struct {
	*Group[T]
	errs []error
}

// NewGroupBuilder 创建任务组构建器。
// concurrency <= 0 时默认 1。
func NewGroupBuilder[T any](concurrency int) *GroupBuilder[T] {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &GroupBuilder[T]{Group: NewGroup[T](concurrency)}
}

// DefaultGroupBuilder 使用默认 IO 并发度创建。
func DefaultGroupBuilder[T any]() *GroupBuilder[T] {
	return &GroupBuilder[T]{Group: DefaultGroup[T]()}
}

// ── Group 配置链式方法 ──

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (g *GroupBuilder[T]) WithLoggerCh(l core.Logger) *GroupBuilder[T] {
	core.SetLogger(l)
	return g
}

// WithTimeoutCh 设置任务总超时。设置任务总超时，返回构建器。
func (g *GroupBuilder[T]) WithTimeoutCh(d time.Duration) *GroupBuilder[T] {
	g.Group.WithTimeout(d)
	return g
}

// WithTimeoutChDefault 使用默认超时（30s）设置任务超时。
func (g *GroupBuilder[T]) WithTimeoutChDefault() *GroupBuilder[T] {
	g.Group.WithTimeout(30 * time.Second)
	return g
}

// WithSubmitTimeoutCh 设置 Submit 等待空闲 worker 的超时。
func (g *GroupBuilder[T]) WithSubmitTimeoutCh(d time.Duration) *GroupBuilder[T] {
	g.Group.WithSubmitTimeout(d)
	return g
}

// WithSubmitTimeoutChDefault 使用默认提交超时（5s）。
func (g *GroupBuilder[T]) WithSubmitTimeoutChDefault() *GroupBuilder[T] {
	g.Group.WithSubmitTimeout(5 * time.Second)
	return g
}

// WithStreamingCh 启用流式结果消费。
func (g *GroupBuilder[T]) WithStreamingCh(bufSize int) *GroupBuilder[T] {
	g.Group.WithStreaming(bufSize)
	return g
}

// WithStreamingChDefault 用默认缓冲大小（concurrency*2）启用流式结果消费。
func (g *GroupBuilder[T]) WithStreamingChDefault() *GroupBuilder[T] {
	g.Group.WithStreaming(0)
	return g
}

// WithResultCallbackCh 设置结果回调。
func (g *GroupBuilder[T]) WithResultCallbackCh(fn func(core.Result[T])) *GroupBuilder[T] {
	g.Group.WithResultCallback(fn)
	return g
}

// WithContextCh 设置上下文并返回 (构建器, 新上下文)。
func (g *GroupBuilder[T]) WithContextCh(ctx context.Context) (*GroupBuilder[T], context.Context) {
	g.Group.WithContext(ctx)
	return g, ctx
}

// WithFailFastCh 启用 FailFast 模式。
func (g *GroupBuilder[T]) WithFailFastCh(ctx context.Context) (*GroupBuilder[T], context.Context) {
	g.Group.WithFailFast(ctx)
	return g, ctx
}

// WithFFCtxCh 启用 FailFast（仅返回构建器上下文对）。
func (g *GroupBuilder[T]) WithFFCtxCh(ctx context.Context) (*GroupBuilder[T], context.Context) {
	g.Group.WithFFCtx(ctx)
	return g, ctx
}

// WithFFSubmitTOCh 启用 FailFast 并设置提交超时。
func (g *GroupBuilder[T]) WithFFSubmitTOCh(ctx context.Context, submitTimeout time.Duration) (*GroupBuilder[T], context.Context) {
	g.Group.WithFFSubmitTO(ctx, submitTimeout)
	return g, ctx
}

// WithFFTimeoutCh 启用 FailFast 并设置任务超时。
func (g *GroupBuilder[T]) WithFFTimeoutCh(ctx context.Context, timeout time.Duration) (*GroupBuilder[T], context.Context) {
	g.Group.WithFFTimeout(ctx, timeout)
	return g, ctx
}

// WithCtxTimeoutCh 设置上下文并启用任务超时。
func (g *GroupBuilder[T]) WithCtxTimeoutCh(ctx context.Context, timeout time.Duration) (*GroupBuilder[T], context.Context) {
	g.Group.WithCtxTimeout(ctx, timeout)
	return g, ctx
}

// WithCtxSubmitTOCh 设置上下文并启用提交超时。
func (g *GroupBuilder[T]) WithCtxSubmitTOCh(ctx context.Context, submitTimeout time.Duration) (*GroupBuilder[T], context.Context) {
	g.Group.WithCtxSubmitTO(ctx, submitTimeout)
	return g, ctx
}

// WithFFTimeoutSubmitTOCh 启用 FailFast + 任务超时 + 提交超时。
func (g *GroupBuilder[T]) WithFFTimeoutSubmitTOCh(ctx context.Context, timeout, submitTimeout time.Duration) (*GroupBuilder[T], context.Context) {
	g.Group.WithFFTimeoutSubmitTO(ctx, timeout, submitTimeout)
	return g, ctx
}

// WithTraceIDCh 设置 trace_id 上下文。
func (g *GroupBuilder[T]) WithTraceIDCh(ctx context.Context) (*GroupBuilder[T], context.Context) {
	g.Group.WithTraceID(ctx)
	return g, ctx
}

// WithFFTraceIDCh 启用 FailFast + trace_id。
func (g *GroupBuilder[T]) WithFFTraceIDCh(ctx context.Context) (*GroupBuilder[T], context.Context) {
	g.Group.WithFFTraceID(ctx)
	return g, ctx
}

// WithFFSubmitTOTraceIDCh 启用 FailFast + 提交超时 + trace_id。
func (g *GroupBuilder[T]) WithFFSubmitTOTraceIDCh(ctx context.Context, submitTimeout time.Duration) (*GroupBuilder[T], context.Context) {
	g.Group.WithFFSubmitTOTraceID(ctx, submitTimeout)
	return g, ctx
}

// WithCtxTraceIDCh 设置上下文 + trace_id。
func (g *GroupBuilder[T]) WithCtxTraceIDCh(ctx context.Context) (*GroupBuilder[T], context.Context) {
	g.Group.WithCtxTraceID(ctx)
	return g, ctx
}

// WithFFTimeoutTraceIDCh 启用 FailFast + 超时 + trace_id。
func (g *GroupBuilder[T]) WithFFTimeoutTraceIDCh(ctx context.Context, timeout time.Duration) (*GroupBuilder[T], context.Context) {
	g.Group.WithFFTimeoutTraceID(ctx, timeout)
	return g, ctx
}

// WithFFTimeoutSubmitTOTraceIDCh 启用 FailFast + 超时 + 提交超时 + trace_id。
func (g *GroupBuilder[T]) WithFFTimeoutSubmitTOTraceIDCh(ctx context.Context, timeout, submitTimeout time.Duration) (*GroupBuilder[T], context.Context) {
	g.Group.WithFFTimeoutSubmitTOTraceID(ctx, timeout, submitTimeout)
	return g, ctx
}

// WithCtxTimeoutTraceIDCh 设置上下文 + 超时 + trace_id。
func (g *GroupBuilder[T]) WithCtxTimeoutTraceIDCh(ctx context.Context, timeout time.Duration) (*GroupBuilder[T], context.Context) {
	g.Group.WithCtxTimeoutTraceID(ctx, timeout)
	return g, ctx
}

// WithCtxSubmitTOTraceIDCh 设置上下文 + 提交超时 + trace_id。
func (g *GroupBuilder[T]) WithCtxSubmitTOTraceIDCh(ctx context.Context, submitTimeout time.Duration) (*GroupBuilder[T], context.Context) {
	g.Group.WithCtxSubmitTOTraceID(ctx, submitTimeout)
	return g, ctx
}

// ── GroupBuilder 错误累积与链式 Action ──

// Error 返回 Go 过程中累积的第一个错误，无错误返回 nil。
func (g *GroupBuilder[T]) Error() error {
	if len(g.errs) == 0 {
		return nil
	}
	return g.errs[0]
}

// Errors 返回所有累积的错误。
func (g *GroupBuilder[T]) Errors() []error {
	return g.errs
}

// GoCh 并发执行任务并捕获错误，返回构建器以支持链式调用。
func (g *GroupBuilder[T]) GoCh(ctx context.Context, fn func(context.Context) (T, error)) *GroupBuilder[T] {
	if err := g.Group.Go(ctx, fn); err != nil {
		g.errs = append(g.errs, err)
	}
	return g
}

// GoWithTimeoutCh 带超时并发执行任务，捕获错误，返回构建器。
func (g *GroupBuilder[T]) GoWithTimeoutCh(ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) *GroupBuilder[T] {
	if err := g.Group.GoWithTimeout(ctx, timeout, fn); err != nil {
		g.errs = append(g.errs, err)
	}
	return g
}

// GoAtCh 在指定索引位置执行任务，捕获错误，返回构建器。
func (g *GroupBuilder[T]) GoAtCh(index int, ctx context.Context, fn func(context.Context) (T, error)) *GroupBuilder[T] {
	if err := g.Group.GoAt(index, ctx, fn); err != nil {
		g.errs = append(g.errs, err)
	}
	return g
}

// GoAtWithTimeoutCh 在指定索引位置带超时执行任务，捕获错误。
func (g *GroupBuilder[T]) GoAtWithTimeoutCh(index int, ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) *GroupBuilder[T] {
	if err := g.Group.GoAtWithTimeout(index, ctx, timeout, fn); err != nil {
		g.errs = append(g.errs, err)
	}
	return g
}

// ============================================================
// ShardPoolBuilder —— 分片池构建器
// ============================================================

// ShardPoolBuilder 链式构建分片池，嵌入底层 ShardedPool 的所有方法。
//
// 使用示例：
//
//	sp := NewShardPoolBuilder[string](8, 4).
//	    WithTimeoutCh(5 * time.Second).
//	    WithStreamingCh(1024)
//
//	sp.Submit(ctx, fn)
//	results := sp.WaitAndClose()
type ShardPoolBuilder[T any] struct {
	*ShardedPool[T]
	errs         []error
	batchResults []SubmitBatchResult
}

// NewShardPoolBuilder 创建分片池构建器。
// shards 分片数（<=0 默认 4），sizePerShard 每个分片的 worker 数（<=0 使用 IO 并发度）。
func NewShardPoolBuilder[T any](shards, sizePerShard int) *ShardPoolBuilder[T] {
	return &ShardPoolBuilder[T]{ShardedPool: NewShardedPoolSimple[T](shards, sizePerShard)}
}

// DefaultShardPoolBuilder 使用默认配置创建（4 分片，IO 并发度）。
func DefaultShardPoolBuilder[T any]() *ShardPoolBuilder[T] {
	return &ShardPoolBuilder[T]{ShardedPool: DefaultShardedPool[T]()}
}

// NewAutoScaleShardPoolBuilder 创建带自动扩缩容的分片池构建器。
// shards/initialSizePerShard <= 0 使用默认值，cfg 为 nil 时使用 DefaultAutoScaleConfig。
func NewAutoScaleShardPoolBuilder[T any](shards, initialSizePerShard int, cfg *core.AutoScaleConfig) *ShardPoolBuilder[T] {
	if shards <= 0 {
		shards = 4
	}
	if initialSizePerShard <= 0 {
		initialSizePerShard = core.IO()
	}
	if cfg == nil {
		cfg = core.DefaultAutoScaleConfig()
	}
	return &ShardPoolBuilder[T]{ShardedPool: NewAutoScaleShardedPool[T](shards, initialSizePerShard, cfg)}
}

// DefaultAutoScaleShardPoolBuilder 使用默认配置创建自动扩缩容分片池（4 分片 + IO 并发度）。
func DefaultAutoScaleShardPoolBuilder[T any]() *ShardPoolBuilder[T] {
	return NewAutoScaleShardPoolBuilder[T](4, core.IO(), core.DefaultAutoScaleConfig())
}

// ── ShardedPool 配置链式方法 ──

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (sp *ShardPoolBuilder[T]) WithLoggerCh(l core.Logger) *ShardPoolBuilder[T] {
	core.SetLogger(l)
	return sp
}

// WithTimeoutCh 设置任务总超时，返回构建器。
func (sp *ShardPoolBuilder[T]) WithTimeoutCh(d time.Duration) *ShardPoolBuilder[T] {
	sp.ShardedPool.WithTimeout(d)
	return sp
}

// WithTimeoutChDefault 使用默认超时（30s）设置任务超时。
func (sp *ShardPoolBuilder[T]) WithTimeoutChDefault() *ShardPoolBuilder[T] {
	sp.ShardedPool.WithTimeout(30 * time.Second)
	return sp
}

// WithSubmitTimeoutCh 设置 Submit 等待空闲 worker 的超时。
func (sp *ShardPoolBuilder[T]) WithSubmitTimeoutCh(d time.Duration) *ShardPoolBuilder[T] {
	sp.ShardedPool.WithSubmitTimeout(d)
	return sp
}

// WithSubmitTimeoutChDefault 使用默认提交超时（5s）。
func (sp *ShardPoolBuilder[T]) WithSubmitTimeoutChDefault() *ShardPoolBuilder[T] {
	sp.ShardedPool.WithSubmitTimeout(5 * time.Second)
	return sp
}

// WithStreamingCh 启用流式结果消费。
func (sp *ShardPoolBuilder[T]) WithStreamingCh(bufSize int) *ShardPoolBuilder[T] {
	sp.ShardedPool.WithStreaming(bufSize)
	return sp
}

// WithStreamingChDefault 用默认缓冲大小（concurrency*2）启用流式结果消费。
func (sp *ShardPoolBuilder[T]) WithStreamingChDefault() *ShardPoolBuilder[T] {
	sp.ShardedPool.WithStreaming(0)
	return sp
}

// WithResultCallbackCh 设置结果回调。
func (sp *ShardPoolBuilder[T]) WithResultCallbackCh(fn func(core.Result[T])) *ShardPoolBuilder[T] {
	sp.ShardedPool.WithResultCallback(fn)
	return sp
}

// WithRingBufferCh 设置环形缓冲区。
func (sp *ShardPoolBuilder[T]) WithRingBufferCh(capacity int, overflow core.OverflowStrategy) *ShardPoolBuilder[T] {
	sp.ShardedPool.WithRingBuffer(capacity, overflow)
	return sp
}

// WithRingBufferChDefault 使用默认环形缓冲区设置（4096, DropOldest）。
func (sp *ShardPoolBuilder[T]) WithRingBufferChDefault() *ShardPoolBuilder[T] {
	sp.ShardedPool.WithRingBuffer(4096, core.OverflowDrop)
	return sp
}

// WithMaxPendingCh 设置最大待处理任务数。
func (sp *ShardPoolBuilder[T]) WithMaxPendingCh(n int) *ShardPoolBuilder[T] {
	sp.ShardedPool.WithMaxPending(n)
	return sp
}

// WithOverflowCh 设置溢出策略。
func (sp *ShardPoolBuilder[T]) WithOverflowCh(strategy core.OverflowStrategy) *ShardPoolBuilder[T] {
	sp.ShardedPool.WithOverflow(strategy)
	return sp
}

// ResizePerShardCh 调整每个分片的 worker 数量，返回构建器。
func (sp *ShardPoolBuilder[T]) ResizePerShardCh(newSize int) *ShardPoolBuilder[T] {
	sp.ShardedPool.ResizePerShard(newSize)
	return sp
}

// WithFailFastCh 启用 FailFast 模式。
func (sp *ShardPoolBuilder[T]) WithFailFastCh(ctx context.Context) (*ShardPoolBuilder[T], context.Context) {
	sp.ShardedPool.WithFailFast(ctx)
	return sp, ctx
}

// ── ShardPoolBuilder 错误累积与链式 Action ──

// Error 返回累积的第一个错误。
func (sp *ShardPoolBuilder[T]) Error() error {
	if len(sp.errs) == 0 {
		return nil
	}
	return sp.errs[0]
}

// Errors 返回所有累积的错误。
func (sp *ShardPoolBuilder[T]) Errors() []error {
	return sp.errs
}

// BatchResults 返回批量提交累积的所有结果。
func (sp *ShardPoolBuilder[T]) BatchResults() []SubmitBatchResult {
	return sp.batchResults
}

// SubmitCh 提交任务并捕获错误，返回构建器。
func (sp *ShardPoolBuilder[T]) SubmitCh(ctx context.Context, fn func(context.Context) (T, error)) *ShardPoolBuilder[T] {
	if err := sp.ShardedPool.Submit(ctx, fn); err != nil {
		sp.errs = append(sp.errs, err)
	}
	return sp
}

// SubmitAtCh 在指定分片和索引位置提交任务，捕获错误，返回构建器。
func (sp *ShardPoolBuilder[T]) SubmitAtCh(shardIdx, index int, ctx context.Context, fn func(context.Context) (T, error)) *ShardPoolBuilder[T] {
	if err := sp.ShardedPool.SubmitAt(shardIdx, index, ctx, fn); err != nil {
		sp.errs = append(sp.errs, err)
	}
	return sp
}

// TrySubmitCh 非阻塞提交任务，失败时捕获错误，返回构建器。
func (sp *ShardPoolBuilder[T]) TrySubmitCh(ctx context.Context, fn func(context.Context) (T, error)) *ShardPoolBuilder[T] {
	if err := sp.ShardedPool.TrySubmit(ctx, fn); err != nil {
		sp.errs = append(sp.errs, err)
	}
	return sp
}

// SubmitKeyedCh 按键哈希提交任务，捕获错误，返回构建器。
func (sp *ShardPoolBuilder[T]) SubmitKeyedCh(key string, ctx context.Context, fn func(context.Context) (T, error)) *ShardPoolBuilder[T] {
	if err := sp.ShardedPool.SubmitKeyed(key, ctx, fn); err != nil {
		sp.errs = append(sp.errs, err)
	}
	return sp
}

// TrySubmitKeyedCh 按键哈希非阻塞提交任务，捕获错误，返回构建器。
func (sp *ShardPoolBuilder[T]) TrySubmitKeyedCh(key string, ctx context.Context, fn func(context.Context) (T, error)) *ShardPoolBuilder[T] {
	if err := sp.ShardedPool.TrySubmitKeyed(key, ctx, fn); err != nil {
		sp.errs = append(sp.errs, err)
	}
	return sp
}

// SubmitBatchCh 批量提交任务，捕获错误并累积结果，返回构建器。
func (sp *ShardPoolBuilder[T]) SubmitBatchCh(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) *ShardPoolBuilder[T] {
	results, err := sp.ShardedPool.SubmitBatch(ctx, items, fn)
	if err != nil {
		sp.errs = append(sp.errs, err)
	}
	sp.batchResults = append(sp.batchResults, results...)
	return sp
}

// TrySubmitBatchCh 非阻塞批量提交任务，捕获错误并累积结果，返回构建器。
func (sp *ShardPoolBuilder[T]) TrySubmitBatchCh(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) *ShardPoolBuilder[T] {
	results, err := sp.ShardedPool.TrySubmitBatch(ctx, items, fn)
	if err != nil {
		sp.errs = append(sp.errs, err)
	}
	sp.batchResults = append(sp.batchResults, results...)
	return sp
}

// ============================================================
// ShardGroupBuilder —— 分片任务组构建器
// ============================================================

// ShardGroupBuilder 链式构建分片任务组，嵌入底层 ShardedGroup 的所有方法。
//
// 使用示例：
//
//	sg := NewShardGroupBuilder[string](8, 4).
//	    WithTimeoutCh(5 * time.Second).
//	    WithStreamingCh(128)
//
//	sg.Go(ctx, fn)
//	results := sg.Wait()
type ShardGroupBuilder[T any] struct {
	*ShardedGroup[T]
	errs         []error
	batchResults []GoBatchResult
}

// NewShardGroupBuilder 创建分片任务组构建器。
// shards 分片数（<=0 默认 4），concurrencyPerShard 每个分片的并发度（<=0 使用 IO 并发度）。
func NewShardGroupBuilder[T any](shards, concurrencyPerShard int) *ShardGroupBuilder[T] {
	cfg := ShardGroupConfig[T]{
		Shards:              shards,
		ConcurrencyPerShard: concurrencyPerShard,
	}
	return &ShardGroupBuilder[T]{ShardedGroup: NewShardedGroup[T](cfg)}
}

// DefaultShardGroupBuilder 使用默认配置创建（4 分片，IO 并发度）。
func DefaultShardGroupBuilder[T any]() *ShardGroupBuilder[T] {
	return &ShardGroupBuilder[T]{ShardedGroup: DefaultShardedGroup[T]()}
}

// ── ShardGroup 配置链式方法 ──

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (sg *ShardGroupBuilder[T]) WithLoggerCh(l core.Logger) *ShardGroupBuilder[T] {
	core.SetLogger(l)
	return sg
}

// WithTimeoutCh 设置任务总超时。
func (sg *ShardGroupBuilder[T]) WithTimeoutCh(d time.Duration) *ShardGroupBuilder[T] {
	sg.ShardedGroup.WithTimeout(d)
	return sg
}

// WithTimeoutChDefault 使用默认超时（30s）设置任务超时。
func (sg *ShardGroupBuilder[T]) WithTimeoutChDefault() *ShardGroupBuilder[T] {
	sg.ShardedGroup.WithTimeout(30 * time.Second)
	return sg
}

// WithSubmitTimeoutCh 设置提交超时。
func (sg *ShardGroupBuilder[T]) WithSubmitTimeoutCh(d time.Duration) *ShardGroupBuilder[T] {
	sg.ShardedGroup.WithSubmitTimeout(d)
	return sg
}

// WithSubmitTimeoutChDefault 使用默认提交超时（5s）。
func (sg *ShardGroupBuilder[T]) WithSubmitTimeoutChDefault() *ShardGroupBuilder[T] {
	sg.ShardedGroup.WithSubmitTimeout(5 * time.Second)
	return sg
}

// WithStreamingCh 启用流式结果消费。
func (sg *ShardGroupBuilder[T]) WithStreamingCh(bufSize int) *ShardGroupBuilder[T] {
	sg.ShardedGroup.WithStreaming(bufSize)
	return sg
}

// WithStreamingChDefault 用默认缓冲大小（concurrency*2）启用流式结果消费。
func (sg *ShardGroupBuilder[T]) WithStreamingChDefault() *ShardGroupBuilder[T] {
	sg.ShardedGroup.WithStreaming(0)
	return sg
}

// WithResultCallbackCh 设置结果回调。
func (sg *ShardGroupBuilder[T]) WithResultCallbackCh(fn func(core.Result[T])) *ShardGroupBuilder[T] {
	sg.ShardedGroup.WithResultCallback(fn)
	return sg
}

// WithFailFastCh 启用 FailFast 模式。
func (sg *ShardGroupBuilder[T]) WithFailFastCh(ctx context.Context) (*ShardGroupBuilder[T], context.Context) {
	sg.ShardedGroup.WithFailFast(ctx)
	return sg, ctx
}

// WithFFCtxCh 启用 FailFast（仅返回构建器上下文对）。
func (sg *ShardGroupBuilder[T]) WithFFCtxCh(ctx context.Context) (*ShardGroupBuilder[T], context.Context) {
	sg.ShardedGroup.WithFFCtx(ctx)
	return sg, ctx
}

// ── ShardGroupBuilder 错误累积与链式 Action ──

// Error 返回累积的第一个错误。
func (sg *ShardGroupBuilder[T]) Error() error {
	if len(sg.errs) == 0 {
		return nil
	}
	return sg.errs[0]
}

// Errors 返回所有累积的错误。
func (sg *ShardGroupBuilder[T]) Errors() []error {
	return sg.errs
}

// BatchResults 返回批量分发累积的所有结果。
func (sg *ShardGroupBuilder[T]) BatchResults() []GoBatchResult {
	return sg.batchResults
}

// GoCh 并发执行任务并捕获错误，返回构建器。
func (sg *ShardGroupBuilder[T]) GoCh(ctx context.Context, fn func(context.Context) (T, error)) *ShardGroupBuilder[T] {
	if err := sg.ShardedGroup.Go(ctx, fn); err != nil {
		sg.errs = append(sg.errs, err)
	}
	return sg
}

// GoAtCh 在指定分片和索引位置执行任务，捕获错误，返回构建器。
func (sg *ShardGroupBuilder[T]) GoAtCh(shardIdx, index int, ctx context.Context, fn func(context.Context) (T, error)) *ShardGroupBuilder[T] {
	if err := sg.ShardedGroup.GoAt(shardIdx, index, ctx, fn); err != nil {
		sg.errs = append(sg.errs, err)
	}
	return sg
}

// GoKeyedCh 按键哈希执行任务，捕获错误，返回构建器。
func (sg *ShardGroupBuilder[T]) GoKeyedCh(key string, ctx context.Context, fn func(context.Context) (T, error)) *ShardGroupBuilder[T] {
	if err := sg.ShardedGroup.GoKeyed(key, ctx, fn); err != nil {
		sg.errs = append(sg.errs, err)
	}
	return sg
}

// GoBatchCh 批量分发任务，捕获错误并累积结果，返回构建器。
func (sg *ShardGroupBuilder[T]) GoBatchCh(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) *ShardGroupBuilder[T] {
	results, err := sg.ShardedGroup.GoBatch(ctx, items, fn)
	if err != nil {
		sg.errs = append(sg.errs, err)
	}
	sg.batchResults = append(sg.batchResults, results...)
	return sg
}

// ============================================================
// MultiPoolBuilder —— 多实例协程池构建器（水平分片）
// ============================================================

// MultiPoolBuilder 链式构建 MultiPool，嵌入底层 MultiPool 的所有方法。
//
// 使用示例：
//
//	mp := NewMultiPoolBuilder(poolBuilder.Pool, 8).
//	    WithTimeoutCh(10 * time.Second).
//	    WithStreamingCh(1024)
//
//	mp.Submit(ctx, fn)
//	results := mp.WaitAndClose()
type MultiPoolBuilder[T any] struct {
	*MultiPool[T]
	errs []error
}

// NewMultiPoolBuilder 从现有 Pool 创建分片池构建器。
func NewMultiPoolBuilder[T any](p *Pool[T], shards int) *MultiPoolBuilder[T] {
	return &MultiPoolBuilder[T]{MultiPool: ShardPool(p, shards)}
}

// DefaultMultiPoolBuilder 使用默认分片数创建。
func DefaultMultiPoolBuilder[T any](p *Pool[T]) *MultiPoolBuilder[T] {
	return &MultiPoolBuilder[T]{MultiPool: DefaultShardPool(p)}
}

// ── MultiPool 配置链式方法 ──

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (mp *MultiPoolBuilder[T]) WithLoggerCh(l core.Logger) *MultiPoolBuilder[T] {
	core.SetLogger(l)
	return mp
}

// WithTimeoutCh 设置任务总超时。
func (mp *MultiPoolBuilder[T]) WithTimeoutCh(d time.Duration) *MultiPoolBuilder[T] {
	mp.MultiPool.WithTimeout(d)
	return mp
}

// WithTimeoutChDefault 使用默认超时（30s）设置任务超时。
func (mp *MultiPoolBuilder[T]) WithTimeoutChDefault() *MultiPoolBuilder[T] {
	mp.MultiPool.WithTimeout(30 * time.Second)
	return mp
}

// WithSubmitTimeoutCh 设置提交超时。
func (mp *MultiPoolBuilder[T]) WithSubmitTimeoutCh(d time.Duration) *MultiPoolBuilder[T] {
	mp.MultiPool.WithSubmitTimeout(d)
	return mp
}

// WithSubmitTimeoutChDefault 使用默认提交超时（5s）。
func (mp *MultiPoolBuilder[T]) WithSubmitTimeoutChDefault() *MultiPoolBuilder[T] {
	mp.MultiPool.WithSubmitTimeout(5 * time.Second)
	return mp
}

// WithStreamingCh 启用流式结果消费。
func (mp *MultiPoolBuilder[T]) WithStreamingCh(bufSize int) *MultiPoolBuilder[T] {
	mp.MultiPool.WithStreaming(bufSize)
	return mp
}

// WithStreamingChDefault 用默认缓冲大小（concurrency*2）启用流式结果消费。
func (mp *MultiPoolBuilder[T]) WithStreamingChDefault() *MultiPoolBuilder[T] {
	mp.MultiPool.WithStreaming(0)
	return mp
}

// WithResultCallbackCh 设置结果回调。
func (mp *MultiPoolBuilder[T]) WithResultCallbackCh(fn func(core.Result[T])) *MultiPoolBuilder[T] {
	mp.MultiPool.WithResultCallback(fn)
	return mp
}

// WithRingBufferCh 设置环形缓冲区。
func (mp *MultiPoolBuilder[T]) WithRingBufferCh(capacity int, overflow core.OverflowStrategy) *MultiPoolBuilder[T] {
	mp.MultiPool.WithRingBuffer(capacity, overflow)
	return mp
}

// WithRingBufferChDefault 使用默认环形缓冲区设置（4096, DropOldest）。
func (mp *MultiPoolBuilder[T]) WithRingBufferChDefault() *MultiPoolBuilder[T] {
	mp.MultiPool.WithRingBuffer(4096, core.OverflowDrop)
	return mp
}

// WithMaxPendingCh 设置最大待处理任务数。
func (mp *MultiPoolBuilder[T]) WithMaxPendingCh(n int) *MultiPoolBuilder[T] {
	mp.MultiPool.WithMaxPending(n)
	return mp
}

// WithOverflowCh 设置溢出策略。
func (mp *MultiPoolBuilder[T]) WithOverflowCh(strategy core.OverflowStrategy) *MultiPoolBuilder[T] {
	mp.MultiPool.WithOverflow(strategy)
	return mp
}

// WithMaxResultsCh 设置最大结果数。
func (mp *MultiPoolBuilder[T]) WithMaxResultsCh(n int) *MultiPoolBuilder[T] {
	mp.MultiPool.WithMaxResults(n)
	return mp
}

// ── MultiPoolBuilder 错误累积与链式 Action ──

// Error 返回累积的第一个错误。
func (mp *MultiPoolBuilder[T]) Error() error {
	if len(mp.errs) == 0 {
		return nil
	}
	return mp.errs[0]
}

// Errors 返回所有累积的错误。
func (mp *MultiPoolBuilder[T]) Errors() []error {
	return mp.errs
}

// SubmitCh 提交任务并捕获错误，返回构建器。
func (mp *MultiPoolBuilder[T]) SubmitCh(ctx context.Context, fn func(context.Context) (T, error)) *MultiPoolBuilder[T] {
	if err := mp.MultiPool.Submit(ctx, fn); err != nil {
		mp.errs = append(mp.errs, err)
	}
	return mp
}

// TrySubmitCh 非阻塞提交任务，失败时捕获错误。
func (mp *MultiPoolBuilder[T]) TrySubmitCh(ctx context.Context, fn func(context.Context) (T, error)) *MultiPoolBuilder[T] {
	if err := mp.MultiPool.TrySubmit(ctx, fn); err != nil {
		mp.errs = append(mp.errs, err)
	}
	return mp
}

// SubmitKeyedCh 按键哈希提交任务，捕获错误。
func (mp *MultiPoolBuilder[T]) SubmitKeyedCh(key uint64, ctx context.Context, fn func(context.Context) (T, error)) *MultiPoolBuilder[T] {
	if err := mp.MultiPool.SubmitKeyed(key, ctx, fn); err != nil {
		mp.errs = append(mp.errs, err)
	}
	return mp
}

// TrySubmitKeyedCh 按键哈希非阻塞提交任务，捕获错误。
func (mp *MultiPoolBuilder[T]) TrySubmitKeyedCh(key uint64, ctx context.Context, fn func(context.Context) (T, error)) *MultiPoolBuilder[T] {
	if err := mp.MultiPool.TrySubmitKeyed(key, ctx, fn); err != nil {
		mp.errs = append(mp.errs, err)
	}
	return mp
}

// SubmitBatchCh 批量提交任务，捕获错误，返回构建器。
func (mp *MultiPoolBuilder[T]) SubmitBatchCh(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) *MultiPoolBuilder[T] {
	results := mp.MultiPool.SubmitBatch(ctx, items, fn)
	for _, r := range results {
		if r.Err != nil {
			mp.errs = append(mp.errs, r.Err)
		}
	}
	return mp
}

// ============================================================
// MultiGroupBuilder —— 多实例任务组构建器（水平分片）
// ============================================================

// MultiGroupBuilder 链式构建 MultiGroup，嵌入底层 MultiGroup 的所有方法。
//
// 使用示例：
//
//	mg := NewMultiGroupBuilder(groupBuilder.Group, 8).
//	    WithTimeoutCh(10 * time.Second)
//
//	mg.Go(ctx, fn)
//	results := mg.Wait()
type MultiGroupBuilder[T any] struct {
	*MultiGroup[T]
	errs []error
}

// NewMultiGroupBuilder 从现有 Group 创建分片任务组构建器。
func NewMultiGroupBuilder[T any](g *Group[T], shards int) *MultiGroupBuilder[T] {
	return &MultiGroupBuilder[T]{MultiGroup: ShardGroup(g, shards)}
}

// DefaultMultiGroupBuilder 使用默认分片数创建。
func DefaultMultiGroupBuilder[T any](g *Group[T]) *MultiGroupBuilder[T] {
	return &MultiGroupBuilder[T]{MultiGroup: DefaultShardGroup(g)}
}

// ── MultiGroup 配置链式方法 ──

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (mg *MultiGroupBuilder[T]) WithLoggerCh(l core.Logger) *MultiGroupBuilder[T] {
	core.SetLogger(l)
	return mg
}

// WithTimeoutCh 设置任务总超时。
func (mg *MultiGroupBuilder[T]) WithTimeoutCh(d time.Duration) *MultiGroupBuilder[T] {
	mg.MultiGroup.WithTimeout(d)
	return mg
}

// WithTimeoutChDefault 使用默认超时（30s）设置任务超时。
func (mg *MultiGroupBuilder[T]) WithTimeoutChDefault() *MultiGroupBuilder[T] {
	mg.MultiGroup.WithTimeout(30 * time.Second)
	return mg
}

// WithStreamingCh 启用流式结果消费。
func (mg *MultiGroupBuilder[T]) WithStreamingCh(bufSize int) *MultiGroupBuilder[T] {
	mg.MultiGroup.WithStreaming(bufSize)
	return mg
}

// WithStreamingChDefault 用默认缓冲大小启用流式结果消费（concurrency*2）。
func (mg *MultiGroupBuilder[T]) WithStreamingChDefault() *MultiGroupBuilder[T] {
	mg.MultiGroup.WithStreaming(0)
	return mg
}

// WithResultCallbackCh 设置结果回调，每个任务完成时同步调用。
func (mg *MultiGroupBuilder[T]) WithResultCallbackCh(fn func(core.Result[T])) *MultiGroupBuilder[T] {
	mg.MultiGroup.WithResultCallback(fn)
	return mg
}

// ── MultiGroupBuilder 错误累积与链式 Action ──

// Error 返回累积的第一个错误。
func (mg *MultiGroupBuilder[T]) Error() error {
	if len(mg.errs) == 0 {
		return nil
	}
	return mg.errs[0]
}

// Errors 返回所有累积的错误。
func (mg *MultiGroupBuilder[T]) Errors() []error {
	return mg.errs
}

// GoCh 并发执行任务并捕获错误，返回构建器。
func (mg *MultiGroupBuilder[T]) GoCh(ctx context.Context, fn func(context.Context) (T, error)) *MultiGroupBuilder[T] {
	if err := mg.MultiGroup.Go(ctx, fn); err != nil {
		mg.errs = append(mg.errs, err)
	}
	return mg
}

// GoKeyedCh 按键哈希执行任务，捕获错误，返回构建器。
func (mg *MultiGroupBuilder[T]) GoKeyedCh(key uint64, ctx context.Context, fn func(context.Context) (T, error)) *MultiGroupBuilder[T] {
	if err := mg.MultiGroup.GoKeyed(key, ctx, fn); err != nil {
		mg.errs = append(mg.errs, err)
	}
	return mg
}

// ============================================================
// NoResultPoolBuilder —— 无返回值协程池构建器
// ============================================================

// NoResultPoolBuilder 链式构建无返回值协程池，嵌入底层 NoResultPool 的所有方法。
//
// 使用示例：
//
//	p := NewNoResultPoolBuilder(8).
//	    WithTimeoutCh(5 * time.Second).
//	    WithStreamingCh(128)
//
//	GoAction(p.Pool, ctx, fn)
//	p.Wait()
type NoResultPoolBuilder struct {
	*NoResultPool
	errs []error
}

// NewNoResultPoolBuilder 创建无返回值协程池构建器。
// size <= 0 使用默认 IO 并发度。
func NewNoResultPoolBuilder(size int) *NoResultPoolBuilder {
	if size <= 0 {
		size = core.IO()
	}
	return &NoResultPoolBuilder{NoResultPool: NewNoResultPool(size)}
}

// DefaultNoResultPoolBuilder 使用默认 IO 并发度创建。
func DefaultNoResultPoolBuilder() *NoResultPoolBuilder {
	return &NoResultPoolBuilder{NoResultPool: DefaultNoResultPool()}
}

// ── NoResultPool 配置链式方法 ──

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (p *NoResultPoolBuilder) WithLoggerCh(l core.Logger) *NoResultPoolBuilder {
	core.SetLogger(l)
	return p
}

// WithTimeoutCh 设置任务总超时。
func (p *NoResultPoolBuilder) WithTimeoutCh(d time.Duration) *NoResultPoolBuilder {
	p.NoResultPool.WithTimeout(d)
	return p
}

// WithTimeoutChDefault 使用默认超时（30s）设置任务超时。
func (p *NoResultPoolBuilder) WithTimeoutChDefault() *NoResultPoolBuilder {
	p.NoResultPool.WithTimeout(30 * time.Second)
	return p
}

// WithSubmitTimeoutCh 设置提交超时。
func (p *NoResultPoolBuilder) WithSubmitTimeoutCh(d time.Duration) *NoResultPoolBuilder {
	p.NoResultPool.WithSubmitTimeout(d)
	return p
}

// WithSubmitTimeoutChDefault 使用默认提交超时（5s）。
func (p *NoResultPoolBuilder) WithSubmitTimeoutChDefault() *NoResultPoolBuilder {
	p.NoResultPool.WithSubmitTimeout(5 * time.Second)
	return p
}

// WithStreamingCh 启用流式结果消费。
func (p *NoResultPoolBuilder) WithStreamingCh(bufSize int) *NoResultPoolBuilder {
	p.NoResultPool.WithStreaming(bufSize)
	return p
}

// WithStreamingChDefault 用默认缓冲大小（concurrency*2）启用流式结果消费。
func (p *NoResultPoolBuilder) WithStreamingChDefault() *NoResultPoolBuilder {
	p.NoResultPool.WithStreaming(0)
	return p
}

// WithRingBufferCh 设置环形缓冲区。
func (p *NoResultPoolBuilder) WithRingBufferCh(capacity int, overflow core.OverflowStrategy) *NoResultPoolBuilder {
	p.NoResultPool.WithRingBuffer(capacity, overflow)
	return p
}

// WithRingBufferChDefault 使用默认环形缓冲区设置（4096, DropOldest）。
func (p *NoResultPoolBuilder) WithRingBufferChDefault() *NoResultPoolBuilder {
	p.NoResultPool.WithRingBuffer(4096, core.OverflowDrop)
	return p
}

// WithMaxPendingCh 设置最大待处理任务数。
func (p *NoResultPoolBuilder) WithMaxPendingCh(maxPending int) *NoResultPoolBuilder {
	p.NoResultPool.WithMaxPending(maxPending)
	return p
}

// WithOverflowCh 设置溢出策略。
func (p *NoResultPoolBuilder) WithOverflowCh(strategy core.OverflowStrategy) *NoResultPoolBuilder {
	p.NoResultPool.WithOverflow(strategy)
	return p
}

// WithContextCh 设置上下文。
func (p *NoResultPoolBuilder) WithContextCh(ctx context.Context) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithContext(ctx)
	return p, ctx
}

// WithFailFastCh 启用 FailFast 模式。
func (p *NoResultPoolBuilder) WithFailFastCh(ctx context.Context) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithFailFast(ctx)
	return p, ctx
}

// WithCtxTimeoutCh 设置上下文并启用任务超时。
func (p *NoResultPoolBuilder) WithCtxTimeoutCh(ctx context.Context, timeout time.Duration) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithCtxTimeout(ctx, timeout)
	return p, ctx
}

// WithCtxSubmitTOCh 设置上下文并启用提交超时。
func (p *NoResultPoolBuilder) WithCtxSubmitTOCh(ctx context.Context, submitTimeout time.Duration) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithCtxSubmitTO(ctx, submitTimeout)
	return p, ctx
}

// WithResultCallbackCh 设置流式结果回调。
func (p *NoResultPoolBuilder) WithResultCallbackCh(fn func(core.Result[struct{}])) *NoResultPoolBuilder {
	p.NoResultPool.WithResultCallback(fn)
	return p
}

// WithMaxResultsCh 设置最大结果数。
func (p *NoResultPoolBuilder) WithMaxResultsCh(maxResults int) *NoResultPoolBuilder {
	p.NoResultPool.WithMaxResults(maxResults)
	return p
}

// WithFFCtxCh 启用 FailFast。
func (p *NoResultPoolBuilder) WithFFCtxCh(ctx context.Context) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithFFCtx(ctx)
	return p, ctx
}

// WithFFSubmitTOCh 启用 FailFast + 提交超时。
func (p *NoResultPoolBuilder) WithFFSubmitTOCh(ctx context.Context, submitTimeout time.Duration) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithFFSubmitTO(ctx, submitTimeout)
	return p, ctx
}

// WithFFTimeoutCh 启用 FailFast + 任务超时。
func (p *NoResultPoolBuilder) WithFFTimeoutCh(ctx context.Context, timeout time.Duration) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithFFTimeout(ctx, timeout)
	return p, ctx
}

// WithFFTimeoutSubmitTOCh 启用 FailFast + 超时 + 提交超时。
func (p *NoResultPoolBuilder) WithFFTimeoutSubmitTOCh(ctx context.Context, timeout, submitTimeout time.Duration) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithFFTimeoutSubmitTO(ctx, timeout, submitTimeout)
	return p, ctx
}

// WithTraceIDCh 设置 trace_id。
func (p *NoResultPoolBuilder) WithTraceIDCh(ctx context.Context) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithTraceID(ctx)
	return p, ctx
}

// WithFFTraceIDCh 启用 FailFast + trace_id。
func (p *NoResultPoolBuilder) WithFFTraceIDCh(ctx context.Context) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithFFTraceID(ctx)
	return p, ctx
}

// WithFFSubmitTOTraceIDCh 启用 FailFast + 提交超时 + trace_id。
func (p *NoResultPoolBuilder) WithFFSubmitTOTraceIDCh(ctx context.Context, submitTimeout time.Duration) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithFFSubmitTOTraceID(ctx, submitTimeout)
	return p, ctx
}

// WithCtxTraceIDCh 设置上下文 + trace_id。
func (p *NoResultPoolBuilder) WithCtxTraceIDCh(ctx context.Context) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithCtxTraceID(ctx)
	return p, ctx
}

// WithFFTimeoutTraceIDCh 启用 FailFast + 超时 + trace_id。
func (p *NoResultPoolBuilder) WithFFTimeoutTraceIDCh(ctx context.Context, timeout time.Duration) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithFFTimeoutTraceID(ctx, timeout)
	return p, ctx
}

// WithFFTimeoutSubmitTOTraceIDCh 启用 FailFast + 超时 + 提交超时 + trace_id。
func (p *NoResultPoolBuilder) WithFFTimeoutSubmitTOTraceIDCh(ctx context.Context, timeout, submitTimeout time.Duration) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithFFTimeoutSubmitTOTraceID(ctx, timeout, submitTimeout)
	return p, ctx
}

// WithCtxTimeoutTraceIDCh 设置上下文 + 超时 + trace_id。
func (p *NoResultPoolBuilder) WithCtxTimeoutTraceIDCh(ctx context.Context, timeout time.Duration) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithCtxTimeoutTraceID(ctx, timeout)
	return p, ctx
}

// WithCtxSubmitTOTraceIDCh 设置上下文 + 提交超时 + trace_id。
func (p *NoResultPoolBuilder) WithCtxSubmitTOTraceIDCh(ctx context.Context, submitTimeout time.Duration) (*NoResultPoolBuilder, context.Context) {
	p.NoResultPool.WithCtxSubmitTOTraceID(ctx, submitTimeout)
	return p, ctx
}

// ── NoResultPoolBuilder 错误累积与链式 Action ──

// Error 返回累积的第一个错误。
func (p *NoResultPoolBuilder) Error() error {
	if len(p.errs) == 0 {
		return nil
	}
	return p.errs[0]
}

// Errors 返回所有累积的错误。
func (p *NoResultPoolBuilder) Errors() []error {
	return p.errs
}

// SubmitCh 提交无返回值任务并捕获错误，返回构建器。
func (p *NoResultPoolBuilder) SubmitCh(ctx context.Context, fn func(context.Context) error) *NoResultPoolBuilder {
	fnWrap := func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}
	if err := p.NoResultPool.Submit(ctx, fnWrap); err != nil {
		p.errs = append(p.errs, err)
	}
	return p
}

// TrySubmitCh 非阻塞提交无返回值任务，失败时捕获错误。
func (p *NoResultPoolBuilder) TrySubmitCh(ctx context.Context, fn func(context.Context) error) *NoResultPoolBuilder {
	fnWrap := func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}
	if err := p.NoResultPool.TrySubmit(ctx, fnWrap); err != nil {
		p.errs = append(p.errs, err)
	}
	return p
}

// SubmitAtCh 在指定索引位置提交无返回值任务，捕获错误。
func (p *NoResultPoolBuilder) SubmitAtCh(index int, ctx context.Context, fn func(context.Context) error) *NoResultPoolBuilder {
	fnWrap := func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}
	if err := p.NoResultPool.SubmitAt(index, ctx, fnWrap); err != nil {
		p.errs = append(p.errs, err)
	}
	return p
}

// ============================================================
// NoResultGroupBuilder —— 无返回值任务组构建器
// ============================================================

// NoResultGroupBuilder 链式构建无返回值任务组，嵌入底层 NoResult 的所有方法。
//
// 使用示例：
//
//	nr := NewNoResultGroupBuilder(8).
//	    WithTimeoutCh(5 * time.Second)
//
//	nr.Go(ctx, fn)
//	nr.Wait()
type NoResultGroupBuilder struct {
	*NoResult
	errs []error
}

// NewNoResultGroupBuilder 创建无返回值任务组构建器。
// concurrency <= 0 时默认 1。
func NewNoResultGroupBuilder(concurrency int) *NoResultGroupBuilder {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &NoResultGroupBuilder{NoResult: NewNoResult(concurrency)}
}

// DefaultNoResultGroupBuilder 使用默认 IO 并发度创建。
func DefaultNoResultGroupBuilder() *NoResultGroupBuilder {
	return &NoResultGroupBuilder{NoResult: DefaultNoResult()}
}

// ── NoResult 配置链式方法 ──

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (nr *NoResultGroupBuilder) WithLoggerCh(l core.Logger) *NoResultGroupBuilder {
	core.SetLogger(l)
	return nr
}

// WithTimeoutCh 设置任务总超时。
func (nr *NoResultGroupBuilder) WithTimeoutCh(d time.Duration) *NoResultGroupBuilder {
	nr.NoResult.WithTimeout(d)
	return nr
}

// WithTimeoutChDefault 使用默认超时（30s）设置任务超时。
func (nr *NoResultGroupBuilder) WithTimeoutChDefault() *NoResultGroupBuilder {
	nr.NoResult.WithTimeout(30 * time.Second)
	return nr
}

// WithSubmitTimeoutCh 设置提交超时。
func (nr *NoResultGroupBuilder) WithSubmitTimeoutCh(d time.Duration) *NoResultGroupBuilder {
	nr.NoResult.WithSubmitTimeout(d)
	return nr
}

// WithSubmitTimeoutChDefault 使用默认提交超时（5s）。
func (nr *NoResultGroupBuilder) WithSubmitTimeoutChDefault() *NoResultGroupBuilder {
	nr.NoResult.WithSubmitTimeout(5 * time.Second)
	return nr
}

// WithContextCh 设置上下文。
func (nr *NoResultGroupBuilder) WithContextCh(ctx context.Context) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithContext(ctx)
	return nr, ctx
}

// WithFailFastCh 启用 FailFast 模式。
func (nr *NoResultGroupBuilder) WithFailFastCh(ctx context.Context) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithFailFast(ctx)
	return nr, ctx
}

// WithFFCtxCh 启用 FailFast。
func (nr *NoResultGroupBuilder) WithFFCtxCh(ctx context.Context) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithFFCtx(ctx)
	return nr, ctx
}

// WithFFSubmitTOCh 启用 FailFast + 提交超时。
func (nr *NoResultGroupBuilder) WithFFSubmitTOCh(ctx context.Context, submitTimeout time.Duration) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithFFSubmitTO(ctx, submitTimeout)
	return nr, ctx
}

// WithFFTimeoutCh 启用 FailFast + 任务超时。
func (nr *NoResultGroupBuilder) WithFFTimeoutCh(ctx context.Context, timeout time.Duration) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithFFTimeout(ctx, timeout)
	return nr, ctx
}

// WithCtxTimeoutCh 设置上下文 + 任务超时。
func (nr *NoResultGroupBuilder) WithCtxTimeoutCh(ctx context.Context, timeout time.Duration) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithCtxTimeout(ctx, timeout)
	return nr, ctx
}

// WithCtxSubmitTOCh 设置上下文 + 提交超时。
func (nr *NoResultGroupBuilder) WithCtxSubmitTOCh(ctx context.Context, submitTimeout time.Duration) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithCtxSubmitTO(ctx, submitTimeout)
	return nr, ctx
}

// WithFFTimeoutSubmitTOCh 启用 FailFast + 超时 + 提交超时。
func (nr *NoResultGroupBuilder) WithFFTimeoutSubmitTOCh(ctx context.Context, timeout, submitTimeout time.Duration) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithFFTimeoutSubmitTO(ctx, timeout, submitTimeout)
	return nr, ctx
}

// WithTraceIDCh 设置 trace_id。
func (nr *NoResultGroupBuilder) WithTraceIDCh(ctx context.Context) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithTraceID(ctx)
	return nr, ctx
}

// WithFFTraceIDCh 启用 FailFast + trace_id。
func (nr *NoResultGroupBuilder) WithFFTraceIDCh(ctx context.Context) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithFFTraceID(ctx)
	return nr, ctx
}

// WithFFSubmitTOTraceIDCh 启用 FailFast + 提交超时 + trace_id。
func (nr *NoResultGroupBuilder) WithFFSubmitTOTraceIDCh(ctx context.Context, submitTimeout time.Duration) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithFFSubmitTOTraceID(ctx, submitTimeout)
	return nr, ctx
}

// WithCtxTraceIDCh 设置上下文 + trace_id。
func (nr *NoResultGroupBuilder) WithCtxTraceIDCh(ctx context.Context) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithCtxTraceID(ctx)
	return nr, ctx
}

// WithFFTimeoutTraceIDCh 启用 FailFast + 超时 + trace_id。
func (nr *NoResultGroupBuilder) WithFFTimeoutTraceIDCh(ctx context.Context, timeout time.Duration) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithFFTimeoutTraceID(ctx, timeout)
	return nr, ctx
}

// WithCtxTimeoutTraceIDCh 设置上下文 + 超时 + trace_id。
func (nr *NoResultGroupBuilder) WithCtxTimeoutTraceIDCh(ctx context.Context, timeout time.Duration) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithCtxTimeoutTraceID(ctx, timeout)
	return nr, ctx
}

// WithFFTimeoutSubmitTOTraceIDCh 启用 FailFast + 超时 + 提交超时 + trace_id。
func (nr *NoResultGroupBuilder) WithFFTimeoutSubmitTOTraceIDCh(ctx context.Context, timeout, submitTimeout time.Duration) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithFFTimeoutSubmitTOTraceID(ctx, timeout, submitTimeout)
	return nr, ctx
}

// WithCtxSubmitTOTraceIDCh 设置上下文 + 提交超时 + trace_id。
func (nr *NoResultGroupBuilder) WithCtxSubmitTOTraceIDCh(ctx context.Context, submitTimeout time.Duration) (*NoResultGroupBuilder, context.Context) {
	nr.NoResult.WithCtxSubmitTOTraceID(ctx, submitTimeout)
	return nr, ctx
}

// WithStreamingCh 启用流式结果消费。
func (nr *NoResultGroupBuilder) WithStreamingCh(bufSize int) *NoResultGroupBuilder {
	nr.NoResult.WithStreaming(bufSize)
	return nr
}

// WithStreamingChDefault 用默认缓冲大小启用流式结果消费（concurrency*2）。
func (nr *NoResultGroupBuilder) WithStreamingChDefault() *NoResultGroupBuilder {
	nr.NoResult.WithStreaming(0)
	return nr
}

// WithResultCallbackCh 设置结果回调，每个任务完成时同步调用。
func (nr *NoResultGroupBuilder) WithResultCallbackCh(fn func(core.Result[struct{}])) *NoResultGroupBuilder {
	nr.NoResult.WithResultCallback(fn)
	return nr
}

// ── NoResultGroupBuilder 错误累积与链式 Action ──

// Error 返回累积的第一个错误。
func (nr *NoResultGroupBuilder) Error() error {
	if len(nr.errs) == 0 {
		return nil
	}
	return nr.errs[0]
}

// Errors 返回所有累积的错误。
func (nr *NoResultGroupBuilder) Errors() []error {
	return nr.errs
}

// GoCh 并发执行无返回值任务并捕获错误，返回构建器。
func (nr *NoResultGroupBuilder) GoCh(ctx context.Context, fn func(context.Context) error) *NoResultGroupBuilder {
	if err := nr.NoResult.Go(ctx, fn); err != nil {
		nr.errs = append(nr.errs, err)
	}
	return nr
}

// GoAtCh 在指定索引位置执行无返回值任务，捕获错误。
func (nr *NoResultGroupBuilder) GoAtCh(index int, ctx context.Context, fn func(context.Context) error) *NoResultGroupBuilder {
	if err := nr.NoResult.GoAt(index, ctx, fn); err != nil {
		nr.errs = append(nr.errs, err)
	}
	return nr
}

// GoWithTimeoutCh 带超时执行无返回值任务，捕获错误。
func (nr *NoResultGroupBuilder) GoWithTimeoutCh(ctx context.Context, timeout time.Duration, fn func(context.Context) error) *NoResultGroupBuilder {
	if err := nr.NoResult.GoWithTimeout(ctx, timeout, fn); err != nil {
		nr.errs = append(nr.errs, err)
	}
	return nr
}

// GoAtWithTimeoutCh 在指定索引带超时执行无返回值任务，捕获错误。
func (nr *NoResultGroupBuilder) GoAtWithTimeoutCh(index int, ctx context.Context, timeout time.Duration, fn func(context.Context) error) *NoResultGroupBuilder {
	if err := nr.NoResult.GoAtWithTimeout(index, ctx, timeout, fn); err != nil {
		nr.errs = append(nr.errs, err)
	}
	return nr
}

// ============================================================
// PipelineBuilder —— 管道构建器
// ============================================================

// PipelineBuilder 链式构建管道（基于 async.go 的 Pipeline 类型）。
//
// 使用示例：
//
//	p := NewPipelineBuilder[int](ctx).
//	    Add(func(ctx context.Context, n int) (int, error) { return n * 2, nil }).
//	    Add(func(ctx context.Context, n int) (int, error) { return n + 1, nil })
//
//	result, err := p.Run(5) // (5*2)+1 = 11
type PipelineBuilder[T any] struct {
	*Pipeline[T]
}

// NewPipelineBuilder 创建管道构建器。
func NewPipelineBuilder[T any](ctx context.Context) *PipelineBuilder[T] {
	return &PipelineBuilder[T]{Pipeline: NewPipeline[T](ctx)}
}

// DefaultPipelineBuilder 使用 context.Background() 创建管道构建器。
func DefaultPipelineBuilder[T any]() *PipelineBuilder[T] {
	return &PipelineBuilder[T]{Pipeline: NewPipeline[T](context.Background())}
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (p *PipelineBuilder[T]) WithLoggerCh(l core.Logger) *PipelineBuilder[T] {
	core.SetLogger(l)
	return p
}

// Add 追加一个阶段函数，返回自身以支持链式调用。
func (p *PipelineBuilder[T]) Add(fn func(context.Context, T) (T, error)) *PipelineBuilder[T] {
	p.stages = append(p.stages, fn)
	return p
}

// WithTraceIDCh 设置带 trace_id 的上下文，返回构建器。
func (p *PipelineBuilder[T]) WithTraceIDCh(ctx context.Context) *PipelineBuilder[T] {
	p.Pipeline.WithTraceID(ctx)
	return p
}

// ============================================================
// RateLimitBuilder —— 限流器构建器
// ============================================================

// RateLimitBuilder 链式构建限流器。
//
// 使用示例：
//
//	rl := NewRateLimitBuilder(100, time.Second).
//	    WithBurst(200).
//	    WithStrategy(ratelimit.Reject).
//	    Build()
//
//	defer rl.Close()
//	rl.Wait(ctx)
type RateLimitBuilder struct {
	rate        int
	perDuration time.Duration
	burst       int
	strategy    ratelimit.Strategy
}

// NewRateLimitBuilder 创建限流器构建器。
// rate 每 perDuration 允许的操作次数，rate <= 0 时使用 IO 并发度。
func NewRateLimitBuilder(rate int, perDuration time.Duration) *RateLimitBuilder {
	if rate <= 0 {
		rate = core.IO()
	}
	return &RateLimitBuilder{
		rate:        rate,
		perDuration: perDuration,
		strategy:    ratelimit.Block,
	}
}

// DefaultRateLimitBuilder 使用默认的 IO 并发度和 1 秒间隔。
func DefaultRateLimitBuilder() *RateLimitBuilder {
	return NewRateLimitBuilder(core.IO(), time.Second)
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (b *RateLimitBuilder) WithLoggerCh(l core.Logger) *RateLimitBuilder {
	core.SetLogger(l)
	return b
}

// WithBurst 设置突发容量（允许瞬时超过 rate 速率的请求数）。
// burst < rate 时自动调整为 rate。
func (b *RateLimitBuilder) WithBurst(burst int) *RateLimitBuilder {
	b.burst = burst
	return b
}

// WithBurstDefault 使用默认突发容量（1）。
func (b *RateLimitBuilder) WithBurstDefault() *RateLimitBuilder {
	b.burst = 1
	return b
}

// WithStrategy 设置令牌耗尽时的处理策略。
func (b *RateLimitBuilder) WithStrategy(s ratelimit.Strategy) *RateLimitBuilder {
	b.strategy = s
	return b
}

// WithStrategyDefault 使用默认策略（Block 阻塞等待）。
func (b *RateLimitBuilder) WithStrategyDefault() *RateLimitBuilder {
	b.strategy = ratelimit.Block
	return b
}

// Build 构建限流器实例。
func (b *RateLimitBuilder) Build() *RateLimiter {
	return NewRateLimiter(b.rate, b.perDuration)
}

// BuildWithBurst 构建带突发容量的限流器实例（若 WithBurst 未设置则等同 Build）。
func (b *RateLimitBuilder) BuildWithBurst() *RateLimiter {
	if b.burst > 0 {
		return NewRateLimiterWithBurst(b.rate, b.perDuration, b.burst)
	}
	return NewRateLimiter(b.rate, b.perDuration)
}

// ============================================================
// SlidingWindowBuilder —— 滑动窗口限流构建器
// ============================================================

// SlidingWindowBuilder 链式构建滑动窗口限流器。
//
// 使用示例：
//
//	sw := NewSlidingWindowBuilder(100, 10*time.Second).Build()
//	if sw.Allow() { doRequest() }
type SlidingWindowBuilder struct {
	limit  int
	window time.Duration
}

// NewSlidingWindowBuilder 创建滑动窗口构建器。
// limit <= 0 时使用 IO 并发度，window <= 0 时使用 1 秒。
func NewSlidingWindowBuilder(limit int, window time.Duration) *SlidingWindowBuilder {
	if limit <= 0 {
		limit = core.IO()
	}
	if window <= 0 {
		window = time.Second
	}
	return &SlidingWindowBuilder{limit: limit, window: window}
}

// DefaultSlidingWindowBuilder 使用默认的 IO 并发度和 1 秒窗口。
func DefaultSlidingWindowBuilder() *SlidingWindowBuilder {
	return NewSlidingWindowBuilder(core.IO(), time.Second)
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (b *SlidingWindowBuilder) WithLoggerCh(l core.Logger) *SlidingWindowBuilder {
	core.SetLogger(l)
	return b
}

// Build 构建滑动窗口限流器。
func (b *SlidingWindowBuilder) Build() *SlidingWindowRateLimiter {
	return NewSlidingWindowRateLimiter(b.limit, b.window)
}

// ============================================================
// TokenBucketBuilder —— 令牌桶构建器
// ============================================================

// TokenBucketBuilder 链式构建令牌桶。
//
// 使用示例：
//
//	tb := NewTokenBucketBuilder(10, 20).Build()
//	if tb.Allow() { doRequest() }
type TokenBucketBuilder struct {
	rate     float64
	capacity float64
}

// NewTokenBucketBuilder 创建令牌桶构建器。
// rate <= 0 时使用 IO 并发度，capacity <= 0 时使用 2*rate。
func NewTokenBucketBuilder(rate, capacity float64) *TokenBucketBuilder {
	if rate <= 0 {
		rate = float64(core.IO())
	}
	if capacity <= 0 {
		capacity = rate * 2
	}
	return &TokenBucketBuilder{rate: rate, capacity: capacity}
}

// DefaultTokenBucketBuilder 使用默认的 IO 并发度和 2x 容量。
func DefaultTokenBucketBuilder() *TokenBucketBuilder {
	return NewTokenBucketBuilder(float64(core.IO()), 0)
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (b *TokenBucketBuilder) WithLoggerCh(l core.Logger) *TokenBucketBuilder {
	core.SetLogger(l)
	return b
}

// Build 构建令牌桶。
func (b *TokenBucketBuilder) Build() *TokenBucket {
	return NewTokenBucket(b.rate, b.capacity)
}

// ============================================================
// AdaptiveRateLimitBuilder —— 自适应限流构建器
// ============================================================

// AdaptiveRateLimitBuilder 链式构建自适应限流器。
//
// 使用示例：
//
//	al := NewAdaptiveRateLimitBuilder(5, 100).Build()
//	al.Acquire(ctx)
//	defer al.Release()
type AdaptiveRateLimitBuilder struct {
	minRate int
	maxRate int
}

// NewAdaptiveRateLimitBuilder 创建自适应限流构建器。
// minRate <= 0 时使用 1，maxRate <= 0 时使用 minRate*10。
func NewAdaptiveRateLimitBuilder(minRate, maxRate int) *AdaptiveRateLimitBuilder {
	if minRate <= 0 {
		minRate = 1
	}
	if maxRate <= 0 {
		maxRate = minRate * 10
	}
	return &AdaptiveRateLimitBuilder{minRate: minRate, maxRate: maxRate}
}

// DefaultAdaptiveRateLimitBuilder 使用默认范围 5～100。
func DefaultAdaptiveRateLimitBuilder() *AdaptiveRateLimitBuilder {
	return NewAdaptiveRateLimitBuilder(5, 100)
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (b *AdaptiveRateLimitBuilder) WithLoggerCh(l core.Logger) *AdaptiveRateLimitBuilder {
	core.SetLogger(l)
	return b
}

// Build 构建自适应限流器。
func (b *AdaptiveRateLimitBuilder) Build() *AdaptiveRateLimiter {
	return NewAdaptiveRateLimiter(b.minRate, b.maxRate)
}

// ============================================================
// TaskBuilder —— 异步任务构建器
// ============================================================

// TaskBuilder 链式构建异步任务，一个 Builder 覆盖 Go / GoResult / GoAction / GoCancel 等所有变体。
//
// 使用示例：
//
//	ar := NewTaskBuilder[int](ctx).
//	    WithTimeout(5 * time.Second).
//	    Run(func(ctx context.Context) (int, error) {
//	        return doWork(ctx)
//	    })
//
//	val, err := ar.Wait()
type TaskBuilder[T any] struct {
	ctx     context.Context
	timeout time.Duration
}

// NewTaskBuilder 创建异步任务构建器。
func NewTaskBuilder[T any](ctx context.Context) *TaskBuilder[T] {
	return &TaskBuilder[T]{ctx: core.EnsureTraceID(ctx)}
}

// DefaultTaskBuilder 使用 context.Background() 创建任务构建器。
func DefaultTaskBuilder[T any]() *TaskBuilder[T] {
	return NewTaskBuilder[T](context.Background())
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (b *TaskBuilder[T]) WithLoggerCh(l core.Logger) *TaskBuilder[T] {
	core.SetLogger(l)
	return b
}

// WithTimeout 设置任务超时。
func (b *TaskBuilder[T]) WithTimeout(d time.Duration) *TaskBuilder[T] {
	b.timeout = d
	return b
}

// WithTimeoutDefault 使用默认任务超时（30s）。
func (b *TaskBuilder[T]) WithTimeoutDefault() *TaskBuilder[T] {
	b.timeout = 30 * time.Second
	return b
}

// Run 启动异步任务，返回 AsyncResult[T]。
func (b *TaskBuilder[T]) Run(fn func(context.Context) (T, error)) *AsyncResult[T] {
	if b.timeout > 0 {
		return GoResultWithTimeout(b.ctx, b.timeout, fn)
	}
	return GoResult(b.ctx, fn)
}

// RunCancel 启动可取消的异步任务，返回 Task[T]。
func (b *TaskBuilder[T]) RunCancel(fn func(context.Context) (T, error)) Task[T] {
	return GoCancel(b.ctx, fn)
}

// RunAction 启动无返回值的异步任务，返回 *AsyncErr。
func (b *TaskBuilder[T]) RunAction(fn func(context.Context) error) *AsyncErr {
	return GoErr(b.ctx, fn)
}

// RunActionCancel 启动无返回值的可取消异步任务，返回 TaskErr。
func (b *TaskBuilder[T]) RunActionCancel(fn func(context.Context) error) TaskErr {
	return GoCancelErr(b.ctx, fn)
}

// RunVoid 启动无返回值的异步任务（fire-and-forget），返回 *TaskVoid。
func (b *TaskBuilder[T]) RunVoid(fn func(context.Context)) *TaskVoid {
	if b.timeout > 0 {
		return GoWithTimeout(b.ctx, b.timeout, fn)
	}
	return Go(b.ctx, fn)
}

// ============================================================
// BoundedRunnerBuilder —— 有界并发执行器构建器
// ============================================================

// BoundedRunnerBuilder 链式构建有界并发执行器。
//
// 使用示例：
//
//	runner := NewBoundedRunnerBuilder(1000).Build()
//
//	for i := 0; i < 1_000_000; i++ {
//	    ar := runner.Go(ctx, fn)
//	}
type BoundedRunnerBuilder struct {
	max int
}

// NewBoundedRunnerBuilder 创建有界并发执行器构建器。
// max <= 0 时使用 IO 并发度。
func NewBoundedRunnerBuilder(max int) *BoundedRunnerBuilder {
	if max <= 0 {
		max = core.IO()
	}
	return &BoundedRunnerBuilder{max: max}
}

// DefaultBoundedRunnerBuilder 使用默认的 IO 并发度。
func DefaultBoundedRunnerBuilder() *BoundedRunnerBuilder {
	return NewBoundedRunnerBuilder(core.IO())
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (b *BoundedRunnerBuilder) WithLoggerCh(l core.Logger) *BoundedRunnerBuilder {
	core.SetLogger(l)
	return b
}

// Build 构建有界并发执行器。
func (b *BoundedRunnerBuilder) Build() *BoundedRunner {
	return NewBoundedRunner(b.max)
}

// ============================================================
// 仅导出 async.go 中未直接导出的子包类型
// ============================================================

// TaskT 可取消的异步任务（从 task 子包导出）。
// 注意：async.go 中已有 AsyncResult 和 TaskVoid。
type TaskT[T any] = task.Task[T]

// PoolT 对 pool.Pool 的显式导出（防止未来命名冲突）。
type PoolT[T any] = pool.Pool[T]

// GroupT 对 group.Group 的显式导出。
type GroupT[T any] = group.Group[T]
