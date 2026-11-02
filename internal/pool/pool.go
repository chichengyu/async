// Package pool 提供泛型协程池 Pool[T]，适合高频小任务的协程复用场景。
// 池的生命周期：NewPool → worker goroutines 启动 → Submit → Wait → Close。
// 支持超时控制、快速失败、提交超时等选项。
//
// 基本使用示例：
//
//	p := pool.NewPool[string](4)
//	defer p.Close()
//	p.Submit(ctx, func(ctx context.Context) (string, error) {
//	    return fetchData(ctx)
//	})
//	results := p.Wait()
//	for _, r := range results {
//	    if r.Ok() {
//	        fmt.Println(r.Value)
//	    }
//	}
package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// poolShardCount 结果存储分片数，降低锁竞争到 1/32。
const poolShardCount = 32

// resultShard 单个结果存储分片，有独立的锁。
type resultShard[T any] struct {
	mu      sync.Mutex
	results []core.Result[T]
	cancels map[*poolCancellation]struct{}
}

type poolCancellation struct{ cancel context.CancelFunc }

// Pool 泛型协程池，复用固定数量的 goroutine 处理高频并发任务。
// 适合长期运行、反复提交任务的场景。
//
// 结果存储使用 32 路分片锁设计，即使百万 QPS 提交也能保持极低锁竞争。
//
// 示例：
//
//	p := pool.NewPool[string](8)
//	p.Submit(ctx, func(ctx context.Context) (string, error) {
//	    return process(ctx), nil
//	})
//	results := p.Wait()
//	p.Close()
type Pool[T any] struct {
	taskCh        chan core.PoolTask[T] // 任务队列（缓冲 channel）
	wg            sync.WaitGroup        // 等待所有已提交任务完成
	taskWaitMu    sync.Mutex
	taskWait      chan struct{}
	taskCount     int64
	workerWg      sync.WaitGroup                 // 等待所有 worker 退出
	resizeMu      sync.Mutex                     // 串行化扩缩容与关闭
	shards        [poolShardCount]resultShard[T] // 分片结果存储
	submitIdx     atomic.Int64                   // 原子提交序号（用于分片路由）
	totalCnt      atomic.Int64                   // 已记录的提交次数
	submitGuard   atomic.Bool                    // 提交保护：Wait期间禁止新提交
	resizeGuards  atomic.Int64                   // 调整大小等待期间的提交保护引用数
	addInFlight   atomic.Int64                   // 处于 precheck 通过后、wg.Add(1) 前的协程数，用于与 Wait 同步
	errCnt        atomic.Int64                   // 失败任务计数
	okCnt         atomic.Int64                   // 成功完成的任务数
	cancel        context.CancelFunc             // 全局取消函数
	failCancel    context.CancelFunc             // 仅在任务失败时传播给其他池
	cancelMu      sync.RWMutex                   // 同步分片时的取消函数切换
	failFast      atomic.Bool                    // 是否启用 FailFast 模式
	timeout       time.Duration                  // 全局任务超时时间（0=无限制）
	submitTimeout time.Duration                  // 任务提交超时时间
	closed        atomic.Bool                    // 是否已关闭
	done          chan struct{}                  // 关闭广播信号
	closeDone     chan struct{}                  // 关闭清理完成信号，由 resizeMu 保护
	size          atomic.Int32                   // worker 数量
	active        atomic.Int32                   // 当前活跃任务数
	busy          atomic.Int32                   // 当前忙碌任务数
	pending       atomic.Int64                   // 等待中的任务数
	waited        atomic.Bool                    // 是否已完成 Wait
	waitInvoked   atomic.Bool                    // Wait 幂等保护：确保 Wait/WaitTimeout/WaitContext 只执行一次
	waitCleanup   chan struct{}                  // Closed after producers and stream cleanup finish
	waitCanceled  atomic.Bool                    // Cancel tasks registered after a timed wait ends
	quitting      atomic.Int32                   // 正在退出的 worker 计数
	quitCh        chan struct{}                  // 缩容广播信号，close 通知空闲 worker 检查 quitting
	quitMu        sync.RWMutex                   // 保护 quitCh 读写的并发安全
	ctx           context.Context                // 池级别的上下文

	// ──────── 自动扩缩容（可选，默认关闭）────────
	autoScaleMu      sync.Mutex
	autoScale        *core.AutoScaleConfig // 扩缩容配置（nil=未启用）
	autoScaleStop    chan struct{}         // 停止扩缩容 goroutine
	autoScaleEnabled atomic.Bool           // 是否已启用自动扩缩容

	// ──────── 结果流式消费 ────────
	streamCh      chan core.Result[T]              // 流式结果 channel
	streamOnce    sync.Once                        // 确保 streamCh 只关闭一次
	streamDropped atomic.Int64                     // 因 stream channel 满被丢弃的结果数
	resultCb      func(core.Result[T])             // 结果回调
	ringBuf       *core.RingBuffer[core.Result[T]] // 环形缓冲（替代无限 results 切片）
	ringBufFlag   atomic.Bool                      // 是否启用环形缓冲

	// ──────── 背压控制 ────────
	maxPending    int64                 // 最大等待任务数（0=无限制）
	overflowStrat core.OverflowStrategy // 溢出策略
	pendingMu     sync.Mutex
	pendingChange chan struct{}
	maxResults    atomic.Int64 // 最大结果数（0=无限制），防止 results 无界增长
}

// SubmitResult 封装 Submit 便捷函数的返回结果，包含提交索引和可能发生的错误。
//
// 示例：
//
//	p, results, err := pool.SubmitBatch(ctx, items, fn)
//	for _, r := range results {
//	    if r.Err != nil {
//	        log.Printf("提交位置 %d 失败: %v", r.Index, r.Err)
//	    }
//	}
type SubmitResult struct {
	Index int   // 任务在结果切片中的索引位置
	Err   error // 提交错误（如超时、池已关闭等）
}

// NewPool 创建指定大小的泛型协程池，立即启动 worker goroutine。
// size<=0 时自动使用 IO 并发度（CPU 核数×2）。
//
// 参数：
//   - size：worker 数量，<=0 时使用默认 IO 并发度
//
// 示例：
//
//	// 创建 8 个 worker 的协程池
//	p := pool.NewPool[string](8)
//	defer p.Close()
//
//	// 使用 CPU 密集型并发度
//	p := pool.NewPool[int](pool.CPU())
func NewPool[T any](size int) *Pool[T] {
	if size <= 0 {
		size = core.IO()
	}
	p := &Pool[T]{
		taskCh:        make(chan core.PoolTask[T], size*2),
		timeout:       core.GetDefaultTimeout(),
		ctx:           context.Background(),
		done:          make(chan struct{}),
		closeDone:     make(chan struct{}),
		quitCh:        make(chan struct{}),
		pendingChange: make(chan struct{}),
	}
	p.maxResults.Store(int64(core.GetDefaultMaxResults()))
	if cap := core.GetDefaultRingBufferCap(); cap > 0 {
		p.ringBuf = core.NewRingBuffer[core.Result[T]](cap, core.GetDefaultOverflowStrategy())
		p.ringBufFlag.Store(true)
	}
	p.size.Store(int32(size))
	p.workerWg.Add(size)
	for i := 0; i < size; i++ {
		go p.worker()
	}
	return p
}

// DefaultPool 使用默认 IO 并发度创建协程池。
// 等价于 NewPool[T](core.IO())。
//
// 示例：
//
//	p := pool.DefaultPool[string]()
//	defer p.Close()
func DefaultPool[T any]() *Pool[T] {
	return NewPool[T](core.IO())
}

// ──────────────────────────── Config：Pool 配置结构体与链式设置 ────────────────────────────

// Config 汇集 Pool 的所有可配置项，支持一次性传入或链式设置。
// 零值字段表示"使用默认值"，不影响已有行为。
//
// 使用方式一：完整传入
//
//	err := pool.WithCfg[string](ctx, pool.Config{
//	    Size: 64, Timeout: 5 * time.Second, FailFast: true,
//	}, func(p *Pool[string]) error { ... })
//
// 使用方式二：链式设置
//
//	cfg := pool.DefaultConfig().
//	    WithSize(64).
//	    WithTimeout(5 * time.Second).
//	    WithFailFast()
//	err := pool.WithCfg[string](ctx, cfg, func(p *Pool[string]) error { ... })
type Config struct {
	Size          int
	Timeout       time.Duration
	SubmitTimeout time.Duration
	FailFast      bool
	MaxPending    int
	Overflow      core.OverflowStrategy
	RingBufCap    int
	MaxResults    int
	Streaming     int
}

// DefaultConfig 返回使用默认 Size（core.IO()）的 Config。
// MaxResults 默认值为 -1，表示不覆盖 Pool 的内置默认值（100_000）。
// 调用链式方法 .MaxResults(n) 可显式覆盖（n=0 恢复无限，n>0 设置上限）。
func DefaultConfig() Config {
	return Config{Size: core.IO(), MaxResults: -1}
}

// WithSize 设置 worker 数量。
func (c Config) WithSize(n int) Config { c.Size = n; return c }

// WithTimeout 设置每个任务的超时时间。
func (c Config) WithTimeout(d time.Duration) Config { c.Timeout = d; return c }

// WithSubmitTimeout 设置任务提交超时时间。
func (c Config) WithSubmitTimeout(d time.Duration) Config { c.SubmitTimeout = d; return c }

// WithFailFast 启用 FailFast 模式（任一任务失败即取消其余任务）。
func (c Config) WithFailFast() Config { c.FailFast = true; return c }

// WithMaxPending 设置最大等待任务数（背压控制）。
func (c Config) WithMaxPending(n int) Config { c.MaxPending = n; return c }

// WithOverflow 设置队列溢出策略。
func (c Config) WithOverflow(s core.OverflowStrategy) Config { c.Overflow = s; return c }

// WithRingBuf 启用环形缓冲区，capacity 为容量。
// 溢出策略使用默认值 core.GetDefaultOverflowStrategy()。
func (c Config) WithRingBuf(capacity int) Config { c.RingBufCap = capacity; return c }

// WithMaxResults 设置结果切片的容量上限（0=无限）。
func (c Config) WithMaxResults(n int) Config { c.MaxResults = n; return c }

// WithStreaming 启用流式结果消费，bufSize 为 channel 缓冲大小。
func (c Config) WithStreaming(bufSize int) Config { c.Streaming = bufSize; return c }

// ──────────────────────────── WithPool / WithCfg：自动 Close 便捷包装函数 ────────────────────────────

// WithPool 创建协程池并执行 fn，fn 返回后自动 Close 释放资源。
// size <= 0 时使用 core.IO() 作为默认并发度。
//
// 示例：
//
//	err := pool.WithPool(64, func(p *Pool[int]) error {
//	    for _, item := range items {
//	        p.Submit(ctx, func(ctx context.Context) (int, error) {
//	            return process(item)
//	        })
//	    }
//	    results := p.Wait()
//	    for _, r := range results {
//	        if r.Err != nil { return r.Err }
//	    }
//	    return nil
//	})
func WithPool[T any](size int, fn func(p *Pool[T]) error) error {
	p := NewPool[T](size)
	defer p.Close()
	return fn(p)
}

// WithCfg 使用 Config 创建协程池并执行 fn，fn 返回后自动 Close 释放资源。
// 支持完整 Config 传入或链式设置后传入。
//
// 使用方式一：完整传入
//
//	err := pool.WithCfg[string](ctx, pool.Config{
//	    Size: 64, Timeout: 5 * time.Second, FailFast: true,
//	}, func(p *Pool[string]) error {
//	    for _, item := range items {
//	        p.Submit(p.Ctx(), func(ctx context.Context) (string, error) {
//	            return process(item)
//	        })
//	    }
//	    results := p.Wait()
//	    return check(results)
//	})
//
// 使用方式二：链式设置
//
//	cfg := pool.DefaultConfig().WithSize(64).WithTimeout(5 * time.Second).WithFailFast()
//	err := pool.WithCfg[string](ctx, cfg, func(p *Pool[string]) error { ... })
func WithCfg[T any](ctx context.Context, cfg Config, fn func(p *Pool[T]) error) error {
	p := NewPool[T](cfg.Size)
	defer p.Close()
	p.applyConfig(ctx, cfg)
	return fn(p)
}

// WithTraceID 设置池的上下文并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//
// 示例：
//
//	p, ctx := pool.NewPool[string](4).WithTraceID(ctx)
//	p.Submit(ctx, fn) // 使用带 trace_id 的 ctx 提交
func (p *Pool[T]) WithTraceID(ctx context.Context) (*Pool[T], context.Context) {
	ctx = core.EnsureTraceID(ctx)
	p.cancelMu.Lock()
	p.ctx = ctx
	p.cancelMu.Unlock()
	return p, ctx
}

// WithContext 设置池的上下文，基于传入 ctx 创建可取消的子 context。
//
// 参数：
//   - ctx：原始上下文
//
// 示例：
//
//	p, ctx := pool.NewPool[string](4).WithContext(ctx)
//	// ctx 是可取消的子 context
func (p *Pool[T]) WithContext(ctx context.Context) (*Pool[T], context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancelMu.Lock()
	p.cancel = core.MergeCancel(p.cancel, cancel)
	p.ctx = ctx
	p.cancelMu.Unlock()
	return p, ctx
}

// Ctx 返回 Pool 内部存储的上下文。
// 当使用 WithFailFast / WithContext 后，此上下文为可取消的子 context；
// 当使用 Config 的 FailFast: true 后，通过此方法获取 ffCtx 以传递给 Submit。
func (p *Pool[T]) Ctx() context.Context {
	p.cancelMu.RLock()
	ctx := p.ctx
	p.cancelMu.RUnlock()
	return ctx
}

// applyConfig 将 Config 中的非零值配置应用到 Pool 上。
// 全部复用已有的 WithXxx 方法，不引入新逻辑。
func (p *Pool[T]) applyConfig(ctx context.Context, cfg Config) {
	if cfg.Timeout > 0 {
		p.WithTimeout(cfg.Timeout)
	}
	if cfg.FailFast {
		p.WithFailFast(ctx)
	}
	if cfg.SubmitTimeout > 0 {
		p.WithSubmitTimeout(cfg.SubmitTimeout)
	}
	if cfg.MaxPending > 0 {
		p.WithMaxPending(cfg.MaxPending)
	}
	if cfg.Overflow != 0 {
		p.WithOverflow(cfg.Overflow)
	}
	if cfg.RingBufCap > 0 {
		p.WithRingBuffer(cfg.RingBufCap, core.GetDefaultOverflowStrategy())
	}
	if cfg.MaxResults >= 0 {
		p.WithMaxResults(cfg.MaxResults)
	}
	if cfg.Streaming > 0 {
		p.WithStreaming(cfg.Streaming)
	}
}

// WithFailFast 启用 FailFast 模式：任一任务失败后立即取消其余任务。
// 失败时自动调用 cancel 触发 ctx 的 Done 信号。
//
// 参数：
//   - ctx：原始上下文
//
// 示例：
//
//	p, ffCtx := pool.NewPool[string](4).WithFailFast(ctx)
//	for _, item := range items {
//	    p.Submit(ffCtx, func(ctx context.Context) (string, error) {
//	        return validate(ctx, item)
//	    })
//	}
//	results := p.Wait()
func (p *Pool[T]) WithFailFast(ctx context.Context) (*Pool[T], context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancelMu.Lock()
	p.cancel = core.MergeCancel(p.cancel, cancel)
	p.failFast.Store(true)
	p.ctx = ctx
	p.cancelMu.Unlock()
	return p, ctx
}

// MergeFailFastCancel 将外部 cancel 函数合并到当前 failFast 取消链中。
// 用于 ShardedPool 等上层组件在多个 Pool 间共享 failFast 取消信号，
// 确保任一池的任务失败能取消所有池。
func (p *Pool[T]) MergeFailFastCancel(cancel context.CancelFunc) *Pool[T] {
	p.cancelMu.Lock()
	p.failCancel = core.MergeCancel(p.failCancel, cancel)
	p.cancelMu.Unlock()
	return p
}

// WithFFCtx 等价于 WithFailFast，是 FailFast + Context 的缩写形式。
//
// 参数：
//   - ctx：原始上下文
func (p *Pool[T]) WithFFCtx(ctx context.Context) (*Pool[T], context.Context) {
	return p.WithFailFast(ctx)
}

// WithFFTraceID 启用 FailFast 并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
func (p *Pool[T]) WithFFTraceID(ctx context.Context) (*Pool[T], context.Context) {
	p, ctx = p.WithFailFast(ctx)
	return p.WithTraceID(ctx)
}

// WithFFSto 启用 FailFast 并设置提交超时（Sto = SubmitTimeout）。
// submitTimeout 控制任务入队的最大等待时间。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
//
// 示例：
//
//	p, ffCtx := pool.NewPool[string](4).WithFFSto(ctx, 5*time.Second)
func (p *Pool[T]) WithFFSto(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFailFast(ctx)
	p.WithSubmitTimeout(submitTimeout)
	return p, ctx
}

// WithFFStoTID 启用 FailFast、设置提交超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
func (p *Pool[T]) WithFFStoTID(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFSto(ctx, submitTimeout)
	return p.WithTraceID(ctx)
}

// WithFFTimeout 启用 FailFast 并设置每个任务的超时时间。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
//
// 示例：
//
//	p, ffCtx := pool.NewPool[string](4).WithFFTimeout(ctx, 10*time.Second)
//	p.Submit(ffCtx, longRunningTask)
func (p *Pool[T]) WithFFTimeout(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFailFast(ctx)
	p.WithTimeout(timeout)
	return p, ctx
}

// WithCtxTraceID 设置 Context 并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
func (p *Pool[T]) WithCtxTraceID(ctx context.Context) (*Pool[T], context.Context) {
	p, ctx = p.WithContext(ctx)
	return p.WithTraceID(ctx)
}

// WithFFTOTID 启用 FailFast、设置超时并注入 trace_id（TO=Timeout, TID=TraceID）。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
func (p *Pool[T]) WithFFTOTID(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFTimeout(ctx, timeout)
	return p.WithTraceID(ctx)
}

// WithFFTOSto 启用 FailFast、设置任务超时和提交超时（TO=Timeout, Sto=SubmitTimeout）。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
//   - submitTimeout：提交超时时间
func (p *Pool[T]) WithFFTOSto(ctx context.Context, timeout, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFTimeout(ctx, timeout)
	p.WithSubmitTimeout(submitTimeout)
	return p, ctx
}

// WithFFTOStoTID 启用 FailFast、设置超时、提交超时并注入 trace_id（TO=Timeout, Sto=SubmitTimeout, TID=TraceID）。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
//   - submitTimeout：提交超时时间
func (p *Pool[T]) WithFFTOStoTID(ctx context.Context, timeout, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFTOSto(ctx, timeout, submitTimeout)
	return p.WithTraceID(ctx)
}

// WithCtxTimeout 设置 Context 和任务超时。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
func (p *Pool[T]) WithCtxTimeout(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithContext(ctx)
	p.WithTimeout(timeout)
	return p, ctx
}

// WithCtxTOTID 设置 Context、任务超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
func (p *Pool[T]) WithCtxTOTID(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithCtxTimeout(ctx, timeout)
	return p.WithTraceID(ctx)
}

// WithTimeout 设置每个任务的超时时间。
//
// 参数：
//   - d：任务超时时间
//
// 示例：
//
//	p := pool.NewPool[string](4).WithTimeout(10 * time.Second)
func (p *Pool[T]) WithTimeout(d time.Duration) *Pool[T] {
	p.timeout = d
	return p
}

// WithSubmitTimeout 设置任务提交超时。超过此时间任务未能入队则丢弃。
//
// 参数：
//   - d：提交超时时间
//
// 示例：
//
//	p := pool.NewPool[string](4).WithSubmitTimeout(3 * time.Second)
func (p *Pool[T]) WithSubmitTimeout(d time.Duration) *Pool[T] {
	p.submitTimeout = d
	return p
}

// WithCtxSto 设置 Context 和提交超时（Sto = SubmitTimeout）。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
func (p *Pool[T]) WithCtxSto(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithContext(ctx)
	p.WithSubmitTimeout(submitTimeout)
	return p, ctx
}

// WithCtxStoTID 设置 Context、提交超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
func (p *Pool[T]) WithCtxStoTID(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithCtxSto(ctx, submitTimeout)
	return p.WithTraceID(ctx)
}

// WithStreaming 启用流式结果消费，结果通过 channel 实时发送。
// bufSize 控制 channel 缓冲大小，0 使用 size*2 的默认值。
// 调用 StreamResults() 获取只读 channel，在 Wait() 后自动关闭。
//
// 示例：
//
//	p := pool.NewPool[string](8).WithStreaming(256)
//	stream := p.StreamResults()
//	go func() {
//	    for r := range stream {
//	        if r.Ok() {
//	            fmt.Println(r.Value)
//	        }
//	    }
//	}()
//	// ... submit tasks ...
//	p.Wait() // 关闭 stream channel
func (p *Pool[T]) WithStreaming(bufSize int) *Pool[T] {
	if bufSize <= 0 {
		bufSize = p.Size() * 2
	}
	p.streamCh = make(chan core.Result[T], bufSize)
	return p
}

// WithResultCallback 设置结果回调，每个任务完成时同步调用。
// 回调同步执行，可能并发调用；任务未执行就被丢弃时也可能由提交或关闭池的 goroutine 调用。
// 回调应尽量轻量，不能同步调用同一池的 Wait、Close 或 CloseAndWait，否则可能死锁。
//
// 示例：
//
//	p := pool.NewPool[string](8).WithResultCallback(func(r core.Result[string]) {
//	    if r.Ok() {
//	        metrics.RecordOk()
//	    }
//	})
func (p *Pool[T]) WithResultCallback(fn func(core.Result[T])) *Pool[T] {
	p.resultCb = fn
	return p
}

// StreamResults 返回流式结果的只读 channel，必须在 WithStreaming 之后调用。
// channel 在 Wait() 完成后自动关闭。
// 不启用流式消费时返回 nil。
func (p *Pool[T]) StreamResults() <-chan core.Result[T] {
	return p.streamCh
}

// StreamDropped 返回因 stream channel 满而被丢弃的结果数。
// 仅在启用了 WithStreaming 时有效。
// 如果此值持续增长，说明消费者速度跟不上生产者，应增大 channel 缓冲或加速消费。
func (p *Pool[T]) StreamDropped() int64 {
	return p.streamDropped.Load()
}

// WithRingBuffer 启用环形缓冲区；它不会单独限制 results 切片的增长。
// capacity 为缓冲区容量，overflow 为满时策略：
//
//	core.OverflowDrop：覆盖最旧结果
//	core.OverflowBlock：返回 false（不阻塞）
//	core.OverflowError：返回 false
//
// 未限制 results 时，完成的结果同时写入 results 和环形缓冲。
// 设置正数 WithMaxResults 后，环形缓冲仅保存超出该上限的结果；0 表示无限制。
//
// 示例：
//
//	p := pool.NewPool[string](8).WithRingBuffer(10000, core.OverflowDrop)
//	// 提交大量任务...
//	batch := p.Flush(5000) // 取出一批结果
func (p *Pool[T]) WithRingBuffer(capacity int, overflow core.OverflowStrategy) *Pool[T] {
	p.ringBuf = core.NewRingBuffer[core.Result[T]](capacity, overflow)
	p.ringBufFlag.Store(true)
	return p
}

// Flush 从环形缓冲区中取出最多 maxCount 个结果。
// 未启用环形缓冲时返回 nil。
// maxCount <= 0 取出全部。
func (p *Pool[T]) Flush(maxCount int) []core.Result[T] {
	if p.ringBuf == nil {
		return nil
	}
	return p.ringBuf.FlushN(maxCount)
}

// RingBufDropped 返回环形缓冲区因 OverflowDrop 覆盖丢弃的元素数。
func (p *Pool[T]) RingBufDropped() int64 {
	if p.ringBuf == nil {
		return 0
	}
	return p.ringBuf.Dropped()
}

// WithMaxResults 设置 results 切片的容量上限（0=无限；默认使用全局配置）。
// 当提交数达到上限后，后续任务的结果只通过流式或环形缓冲区消费。
// 这是防止长期运行的池内存持续增长的关键配置。
//
// 示例：
//
//	// 最多保留 100 万个结果在内存中
//	p := pool.NewPool[string](8).WithMaxResults(1_000_000)
func (p *Pool[T]) WithMaxResults(maxResults int) *Pool[T] {
	if maxResults >= 0 {
		p.maxResults.Store(int64(maxResults))
	}
	return p
}

// WithMaxPending 设置最大等待任务数，配合 WithOverflow 使用。
// maxPending <= 0 表示无限制。
// 当 Pending() >= maxPending 时，新提交根据溢出策略处理。
//
// 示例：
//
//	p := pool.NewPool[string](8).
//	    WithMaxPending(1000).
//	    WithOverflow(core.OverflowError)
func (p *Pool[T]) WithMaxPending(maxPending int) *Pool[T] {
	if maxPending > 0 {
		p.maxPending = int64(maxPending)
	}
	return p
}

// WithOverflow 设置队列溢出策略。
//
//	core.OverflowBlock：阻塞等待（默认行为）
//	core.OverflowDrop：静默丢弃
//	core.OverflowError：返回 ErrQueueOverflow
//
// 示例：
//
//	p := pool.NewPool[string](8).
//	    WithMaxPending(2000).
//	    WithOverflow(core.OverflowDrop)
func (p *Pool[T]) WithOverflow(strategy core.OverflowStrategy) *Pool[T] {
	p.overflowStrat = strategy
	return p
}

// QueueDepth 返回当前队列深度（等待中的任务数）。
func (p *Pool[T]) QueueDepth() int {
	return p.Pending()
}

func (p *Pool[T]) recordToStream(r core.Result[T]) {
	if p.streamCh != nil {
		select {
		case p.streamCh <- r:
		default:
			p.streamDropped.Add(1)
		}
	}
	if p.resultCb != nil {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					pe := core.NewPanicError(recovered)
					core.LogCtxError(p.Ctx(), "async pool result callback panic recovered", core.Any("panic", recovered), core.Bytes("stack", pe.Stack))
				}
			}()
			p.resultCb(r)
		}()
	}
}

// drainStreaming 在 Wait 后关闭流式 channel 并排空 ringBuf。
func (p *Pool[T]) drainStreaming() {
	p.streamOnce.Do(func() {
		if p.streamCh != nil {
			close(p.streamCh)
		}
	})
}

// tryQuit 原子尝试获取一个退出名额。返回 true 表示本 worker 应退出。
// 所有 worker 退出路径统一使用此方法，消除 Quit task 的 Add(-1) 与 quitCh 的 CAS
// 之间因非原子 load-then-decrement 导致的 TOCTOU 竞态（quitting 计数器会越过零点变负）。
func (p *Pool[T]) tryQuit() bool {
	for {
		v := p.quitting.Load()
		if v <= 0 {
			return false
		}
		if p.quitting.CompareAndSwap(v, v-1) {
			return true
		}
	}
}

func (p *Pool[T]) worker() {
	defer p.workerWg.Done()
	p.quitMu.RLock()
	quitCh := p.quitCh
	p.quitMu.RUnlock()
	for {
		select {
		case task, ok := <-p.taskCh:
			if !ok {
				return
			}
			if task.Quit {
				if p.tryQuit() {
					return
				}
				continue
			}
			p.active.Add(1)
			p.processTask(task)
			p.active.Add(-1)
			if p.tryQuit() {
				return
			}
		case <-quitCh:
			if p.tryQuit() {
				return
			}
			p.quitMu.RLock()
			quitCh = p.quitCh
			p.quitMu.RUnlock()
		case <-p.done:
			for {
				select {
				case task, ok := <-p.taskCh:
					if !ok {
						return
					}
					if task.Quit {
						p.tryQuit()
						continue
					}
					p.active.Add(1)
					p.processTask(task)
					p.active.Add(-1)
				default:
					return
				}
			}
		}
	}
}

func (p *Pool[T]) processTask(task core.PoolTask[T]) {
	taskCtx := task.Ctx
	taskCancel := task.Cancel
	timeout := task.Timeout
	failFast := task.FailFast
	idx := task.Index
	record := task.Record
	defer p.finishTask()
	defer taskCancel()

	defer func() {
		if r := recover(); r != nil {
			core.LogCtxError(taskCtx, "async pool panic recovered",
				core.Any("panic", r),
				core.Bytes("stack", core.NewPanicError(r).Stack))
			var zero T
			record(core.Result[T]{Value: zero, Err: core.NewPanicError(r), Occupied: true}, idx)
			p.errCnt.Add(1)
			if failFast {
				p.cancelOnFailure()
			}
		}
	}()

	p.releasePendingSlot()
	p.busy.Add(1)
	defer p.busy.Add(-1)

	// 出队后、执行前重检 ctx，消除 blockSend select 随机性带来的竞态窗口
	select {
	case <-taskCtx.Done():
		var zero T
		record(core.Result[T]{Value: zero, Err: taskCtx.Err(), Occupied: true}, idx)
		p.errCnt.Add(1)
		return
	default:
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(taskCtx, timeout)
		defer cancel()
	}

	val, err := task.Fn(taskCtx)
	if err != nil {
		if !(failFast && errors.Is(err, context.Canceled)) {
			core.LogTaskFail(taskCtx, err, "async pool task failed")
		}
		p.errCnt.Add(1)
		if failFast {
			p.cancelOnFailure()
		}
	}

	r := core.Result[T]{Value: val, Err: err, Occupied: true}
	record(r, idx)
	if err == nil {
		p.okCnt.Add(1)
	}

}

func (p *Pool[T]) cancelOnFailure() {
	p.cancelMu.RLock()
	cancel, failCancel := p.cancel, p.failCancel
	p.cancelMu.RUnlock()
	if cancel != nil {
		cancel()
	}
	if failCancel != nil {
		failCancel()
	}
}

func (p *Pool[T]) cancelContext() {
	p.cancelMu.RLock()
	cancel := p.cancel
	p.cancelMu.RUnlock()
	if cancel != nil {
		cancel()
	}
}

func (p *Pool[T]) poolPrecheck(ctx context.Context, caller string) error {
	if p.closed.Load() {
		return core.ErrPoolClosed
	}
	if p.waited.Load() {
		return core.ErrPoolWaited
	}
	if p.submitGuard.Load() || p.resizeGuards.Load() > 0 {
		return core.ErrPoolWaiting
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}

// NoResultPool 无返回值协程池，是 Pool[struct{}] 的类型别名。
// 适合批量写入、通知发送、文件下载等只关心错误的场景。
//
// 示例：
//
//	p := pool.NewPool[struct{}](8)
//	defer p.Close()
//	p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
//	    return struct{}{}, sendNotification(ctx, msg)
//	})
//	p.Wait()
type NoResultPool = Pool[struct{}]

// Submit 向协程池提交一个任务，返回提交结果（无索引）。
//
// 参数：
//   - ctx：上下文，用于取消和 TraceID
//   - fn：任务执行函数，接收派生 context，返回结果和错误
//
// 示例：
//
//	err := p.Submit(ctx, func(ctx context.Context) (string, error) {
//	    return fetchData(ctx)
//	})
//	if err != nil {
//	    log.Printf("提交失败: %v", err)
//	}
func (p *Pool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error {
	_, err := p.submitIndexed(ctx, fn)
	return err
}

// SubmitWithCompletion notifies internal adapters of every terminal outcome,
// including rejection, overflow and cancellation before execution. The callback
// runs synchronously and must not wait for or close this pool.
func (p *Pool[T]) SubmitWithCompletion(ctx context.Context, fn func(context.Context) (T, error), complete func(core.Result[T])) error {
	return p.submitWithCompletion(ctx, fn, complete, 0)
}

// SubmitWithCompletionTimeout overrides the timeout for this task when positive.
func (p *Pool[T]) SubmitWithCompletionTimeout(ctx context.Context, fn func(context.Context) (T, error), complete func(core.Result[T]), timeout time.Duration) error {
	return p.submitWithCompletion(ctx, fn, complete, timeout)
}

func (p *Pool[T]) submitWithCompletion(ctx context.Context, fn func(context.Context) (T, error), complete func(core.Result[T]), timeout time.Duration) error {
	var once sync.Once
	finish := func(r core.Result[T]) {
		once.Do(func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					pe := core.NewPanicError(recovered)
					core.LogCtxError(ctx, "async pool completion callback panic recovered", core.Any("panic", recovered), core.Bytes("stack", pe.Stack))
				}
			}()
			complete(r)
		})
	}
	idx, err := p.submitIndexedWithTimeout(ctx, fn, timeout, finish)
	if err != nil || idx < 0 {
		cause := err
		if cause == nil {
			cause = core.ErrQueueOverflow
		}
		finish(core.Result[T]{Err: cause, Occupied: true})
	}
	return err
}

// resultsAppend 向分片存储追加一个结果槽位，返回全局索引 idx。
// 如果设置了 maxResults 且已达上限，仅增加序号不追加存储。
func (p *Pool[T]) resultsAppend() int64 {
	idx := p.submitIdx.Add(1) - 1

	maxR := int(p.maxResults.Load())
	if maxR > 0 && idx >= int64(maxR) {
		// 超出上限：不存储到分片，仅推进序号
		return idx
	}

	shardIdx := int(idx % poolShardCount)
	s := &p.shards[shardIdx]
	s.mu.Lock()
	for int(idx/poolShardCount) >= len(s.results) {
		s.results = append(s.results, core.Result[T]{})
	}
	s.mu.Unlock()
	return idx
}

// rejectedIndex preserves an existing result slot when a submission is rejected
// for an index that has already been allocated.
func (p *Pool[T]) rejectedIndex(index int64) int64 {
	for {
		current := p.submitIdx.Load()
		if index < current {
			return p.submitIdx.Add(1) - 1
		}
		if p.submitIdx.CompareAndSwap(current, index+1) {
			return index
		}
	}
}

// resultsSet 向分片存储写入指定索引的结果。
// 若分片槽位不足则自动扩展（兼容 submitIndexed/SubmitAt 失败路径中跳过 resultsAppend 的场景）。
func (p *Pool[T]) resultsSet(idx int64, r core.Result[T]) {
	maxR := int(p.maxResults.Load())
	if maxR > 0 && idx >= int64(maxR) {
		return
	}

	shardIdx := int(idx % poolShardCount)
	localIdx := int(idx / poolShardCount)
	s := &p.shards[shardIdx]
	s.mu.Lock()
	for localIdx >= len(s.results) {
		s.results = append(s.results, core.Result[T]{})
	}
	s.results[localIdx] = r
	s.mu.Unlock()
}

func (p *Pool[T]) recordRejectedResult(idx int64, err error) {
	r := core.Result[T]{Err: err, Occupied: true}
	p.resultsSet(idx, r)
	if maxR := p.maxResults.Load(); maxR > 0 && idx >= maxR && p.ringBufFlag.Load() && p.ringBuf != nil {
		p.ringBuf.Push(r)
	}
}

// resultsCollect 收集所有分片的结果，按提交顺序合并。
//
// 优化：不再对每个元素逐一加锁（lock-per-element），
// 改为对每个分片加锁一次并批量拷贝（lock-per-shard）。
// 锁次数从 O(N) 降到 O(poolShardCount)，百万任务下从百万次降到 32 次。
func (p *Pool[T]) resultsCollect() []core.Result[T] {
	total := int(p.submitIdx.Load())
	maxR := int(p.maxResults.Load())
	if maxR > 0 && total > maxR {
		total = maxR
	}
	results := make([]core.Result[T], total)

	// 按分片批量拷贝：每个分片锁定一次，将所有结果写入交错排列的位置
	for shardIdx := 0; shardIdx < poolShardCount; shardIdx++ {
		s := &p.shards[shardIdx]
		s.mu.Lock()
		for localIdx := range s.results {
			globalIdx := localIdx*poolShardCount + shardIdx
			if globalIdx < total {
				results[globalIdx] = s.results[localIdx]
			}
		}
		s.mu.Unlock()
	}
	return results
}

// cancelAllShards 取消所有分片中的 cancel 函数，并清空。
func (p *Pool[T]) cancelAllShards() {
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.Lock()
		cancels := make([]context.CancelFunc, 0, len(s.cancels))
		for c := range s.cancels {
			cancels = append(cancels, c.cancel)
		}
		s.mu.Unlock()
		for _, c := range cancels {
			if c != nil {
				c()
			}
		}
	}
}

func (p *Pool[T]) trackCancel(idx int64, cancel context.CancelFunc) context.CancelFunc {
	s := &p.shards[idx%poolShardCount]
	entry := &poolCancellation{cancel: cancel}
	s.mu.Lock()
	if p.waitCanceled.Load() {
		s.mu.Unlock()
		cancel()
		return cancel
	}
	if s.cancels == nil {
		s.cancels = make(map[*poolCancellation]struct{})
	}
	s.cancels[entry] = struct{}{}
	s.mu.Unlock()
	return func() {
		cancel()
		s.mu.Lock()
		delete(s.cancels, entry)
		s.mu.Unlock()
	}
}

// totalResultsCount 返回所有分片中已存储的结果总数。
func (p *Pool[T]) totalResultsCount() int64 {
	var n int64
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.Lock()
		n += int64(len(s.results))
		s.mu.Unlock()
	}
	return n
}

func (p *Pool[T]) submitIndexed(ctx context.Context, fn func(context.Context) (T, error), completion ...func(core.Result[T])) (index int, err error) {
	return p.submitIndexedWithTimeout(ctx, fn, 0, completion...)
}

func (p *Pool[T]) submitIndexedWithTimeout(ctx context.Context, fn func(context.Context) (T, error), timeout time.Duration, completion ...func(core.Result[T])) (index int, err error) {
	// 进入临界区：标记本协程正在 precheck 与 wg.Add(1) 之间，
	// 防止 Wait 系列方法在 submitGuard 设置后立即调用 wg.Wait() 产生竞态。
	p.addInFlight.Add(1)

	if err := p.poolPrecheck(ctx, "Submit"); err != nil {
		p.totalCnt.Add(1)
		p.errCnt.Add(1)
		idx := p.submitIdx.Add(1) - 1
		p.recordRejectedResult(idx, err)
		p.addInFlight.Add(-1)
		return -1, err
	}

	// 二次检查：防止 poolPrecheck 与 wg.Add 之间 Close() 被调用导致泄漏。
	// 必须在 CAS 背压检查之前执行，否则若 CAS 已递增 pending 后再检测到 Close，
	// 需要在返回路径中回退 pending，增加复杂度且降低性能。
	if p.closed.Load() {
		p.totalCnt.Add(1)
		p.errCnt.Add(1)
		idx := p.submitIdx.Add(1) - 1
		p.recordRejectedResult(idx, core.ErrPoolClosed)
		p.addInFlight.Add(-1)
		return -1, core.ErrPoolClosed
	}

	// Drop/Error reserve pending capacity here. Block reserves it after task
	// registration, so Close and Wait can observe and unblock the submission.
	var pendingAcquired bool
	if mp := p.maxPending; mp > 0 {
		switch p.overflowStrat {
		case core.OverflowDrop:
			if !p.tryAcquirePendingSlot(mp) {
				p.totalCnt.Add(1)
				idx := p.submitIdx.Add(1) - 1
				p.recordRejectedResult(idx, core.ErrQueueOverflow)
				p.errCnt.Add(1)
				p.addInFlight.Add(-1)
				return -1, nil
			}
			pendingAcquired = true
		case core.OverflowError:
			if !p.tryAcquirePendingSlot(mp) {
				p.totalCnt.Add(1)
				idx := p.submitIdx.Add(1) - 1
				p.recordRejectedResult(idx, core.ErrQueueOverflow)
				p.errCnt.Add(1)
				p.addInFlight.Add(-1)
				return -1, core.ErrQueueOverflow
			}
			pendingAcquired = true
		}
	}

	taskCtx, taskCancel := context.WithCancel(ctx)

	p.totalCnt.Add(1)
	idx := p.resultsAppend()
	taskCancel = p.trackCancel(idx, taskCancel)

	if !pendingAcquired && !(p.maxPending > 0 && p.overflowStrat == core.OverflowBlock) {
		p.pending.Add(1)
	}
	p.registerTask()
	// wg.Add(1) 已完成，退出临界区
	p.addInFlight.Add(-1)

	maxR := int(p.maxResults.Load())
	record := func(r core.Result[T], _ int) {
		if len(completion) > 0 {
			defer completion[0](r)
		}
		p.resultsSet(idx, r)
		p.recordToStream(r)
		if p.ringBufFlag.Load() && p.ringBuf != nil {
			// maxR<=0 时全量镜像写入；maxR>0 时仅写入溢出部分，避免与分片存储重复
			if maxR <= 0 || int(idx) >= maxR {
				p.ringBuf.Push(r)
			}
		}
	}

	if timeout <= 0 {
		timeout = p.timeout
	}
	task := core.PoolTask[T]{
		Ctx:      taskCtx,
		Cancel:   taskCancel,
		Fn:       fn,
		Timeout:  timeout,
		FailFast: p.failFast.Load(),
		Index:    int(idx),
		Record:   record,
	}

	if err := p.enqueueTask(ctx, taskCtx, taskCancel, task, record, int(idx)); err != nil {
		return int(idx), err
	}

	return int(idx), nil
}

// SubmitAt 在指定索引位置提交任务，保证结果的有序性。
//
// 参数：
//   - index：任务在结果切片中的索引
//   - ctx：上下文
//   - fn：任务执行函数
//
// 示例：
//
//	for i, item := range items {
//	    idx := i
//	    p.SubmitAt(idx, ctx, func(ctx context.Context) (string, error) {
//	        return process(ctx, items[idx])
//	    })
//	}
func (p *Pool[T]) SubmitAt(index int, ctx context.Context, fn func(context.Context) (T, error)) error {
	return p.submitAt(index, ctx, fn, nil, nil)
}

// SubmitAtWithCompletion reports each terminal submission outcome to an internal adapter.
// The callback runs synchronously and must not wait for or close this pool.
func (p *Pool[T]) SubmitAtWithCompletion(index int, ctx context.Context, fn func(context.Context) (T, error), complete func(core.Result[T])) error {
	var once sync.Once
	finish := func(r core.Result[T]) {
		once.Do(func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					pe := core.NewPanicError(recovered)
					core.LogCtxError(ctx, "async pool completion callback panic recovered", core.Any("panic", recovered), core.Bytes("stack", pe.Stack))
				}
			}()
			complete(r)
		})
	}
	err := p.submitAt(index, ctx, fn, finish, nil)
	if err != nil {
		finish(core.Result[T]{Err: err, Occupied: true})
	}
	return err
}

// SubmitAtWithResultTransform lets an internal adapter transform a result before
// it is recorded. The transform is synchronous and must not wait for or close the pool.
// A transform panic is converted to a result error.
func (p *Pool[T]) SubmitAtWithResultTransform(index int, ctx context.Context, fn func(context.Context) (T, error), transform func(core.Result[T]) core.Result[T]) error {
	if transform == nil {
		return p.submitAt(index, ctx, fn, nil, nil)
	}
	safeTransform := func(r core.Result[T]) (result core.Result[T]) {
		result = r
		defer func() {
			if recovered := recover(); recovered != nil {
				result.Err = core.NewPanicError(recovered)
			}
		}()
		return transform(r)
	}
	return p.submitAt(index, ctx, fn, nil, safeTransform)
}

func (p *Pool[T]) submitAt(index int, ctx context.Context, fn func(context.Context) (T, error), complete func(core.Result[T]), transform func(core.Result[T]) core.Result[T]) error {
	if index < 0 || index > core.MaxPoolIndex {
		return core.ErrInvalidIndex
	}
	p.totalCnt.Add(1)
	recordRejected := func(err error) {
		resultIndex := p.rejectedIndex(int64(index))
		r := core.Result[T]{Err: err, Occupied: true}
		if transform != nil {
			r = transform(r)
		}
		p.resultsSet(resultIndex, r)
		if maxR := p.maxResults.Load(); maxR > 0 && resultIndex >= maxR && p.ringBufFlag.Load() && p.ringBuf != nil {
			p.ringBuf.Push(r)
		}
	}

	p.addInFlight.Add(1)
	if err := p.poolPrecheck(ctx, "SubmitAt"); err != nil {
		p.errCnt.Add(1)
		recordRejected(err)
		p.addInFlight.Add(-1)
		if complete != nil {
			complete(core.Result[T]{Err: err, Occupied: true})
		}
		return err
	}

	if p.closed.Load() {
		p.errCnt.Add(1)
		recordRejected(core.ErrPoolClosed)
		p.addInFlight.Add(-1)
		if complete != nil {
			complete(core.Result[T]{Err: core.ErrPoolClosed, Occupied: true})
		}
		return core.ErrPoolClosed
	}

	// 原子化背压检查：与 submitIndexed 一致的 maxPending + overflowStrat 管控，
	// 消除 SubmitAt 绕过背压限制的安全隐患。
	var pendingAcquired bool
	if mp := p.maxPending; mp > 0 {
		switch p.overflowStrat {
		case core.OverflowDrop:
			if !p.tryAcquirePendingSlot(mp) {
				recordRejected(core.ErrQueueOverflow)
				p.errCnt.Add(1)
				p.addInFlight.Add(-1)
				if complete != nil {
					complete(core.Result[T]{Err: core.ErrQueueOverflow, Occupied: true})
				}
				return nil
			}
			pendingAcquired = true
		case core.OverflowError:
			if !p.tryAcquirePendingSlot(mp) {
				recordRejected(core.ErrQueueOverflow)
				p.errCnt.Add(1)
				p.addInFlight.Add(-1)
				if complete != nil {
					complete(core.Result[T]{Err: core.ErrQueueOverflow, Occupied: true})
				}
				return core.ErrQueueOverflow
			}
			pendingAcquired = true
		}
	}

	taskCtx, taskCancel := context.WithCancel(ctx)

	// Claim a new index before enqueueing so a later rejection can keep it.
	freshIndex := false
	for current := p.submitIdx.Load(); int64(index) >= current; current = p.submitIdx.Load() {
		if p.submitIdx.CompareAndSwap(current, int64(index)+1) {
			freshIndex = true
			break
		}
	}
	taskCancel = p.trackCancel(int64(index), taskCancel)

	if !pendingAcquired && !(p.maxPending > 0 && p.overflowStrat == core.OverflowBlock) {
		p.pending.Add(1)
	}
	p.registerTask()
	p.addInFlight.Add(-1)

	maxR := int(p.maxResults.Load())
	record := func(r core.Result[T], idx int) {
		if complete != nil {
			defer complete(r)
		}
		if transform != nil {
			r = transform(r)
		}
		i := int64(idx)
		if i >= 0 {
			p.resultsSet(i, r)
		}
		p.recordToStream(r)
		if p.ringBufFlag.Load() && p.ringBuf != nil {
			if maxR <= 0 || int(i) >= maxR {
				p.ringBuf.Push(r)
			}
		}
	}

	task := core.PoolTask[T]{
		Ctx:      taskCtx,
		Cancel:   taskCancel,
		Fn:       fn,
		Timeout:  p.timeout,
		FailFast: p.failFast.Load(),
		Index:    index,
		Record:   record,
	}

	discardRecord := func(r core.Result[T], _ int) {
		resultIndex := int64(index)
		if !freshIndex {
			resultIndex = p.rejectedIndex(resultIndex)
		}
		record(r, int(resultIndex))
	}
	return p.enqueueTask(ctx, taskCtx, taskCancel, task, discardRecord, index)
}

// TrySubmit 非阻塞向协程池提交任务。如果任务队列已满则立即返回错误。
//
// 参数：
//   - ctx：上下文
//   - fn：任务执行函数
//
// 示例：
//
//	err := p.TrySubmit(ctx, fn)
//	if errors.Is(err, core.ErrSubmitTimeout) {
//	    log.Println("队列已满，任务被丢弃")
//	}
func (p *Pool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error {
	p.addInFlight.Add(1)
	if p.closed.Load() {
		p.addInFlight.Add(-1)
		return core.ErrPoolClosed
	}
	if p.waited.Load() {
		p.addInFlight.Add(-1)
		return core.ErrPoolWaited
	}
	if p.submitGuard.Load() || p.resizeGuards.Load() > 0 {
		p.addInFlight.Add(-1)
		return core.ErrPoolWaiting
	}
	if err := ctx.Err(); err != nil {
		p.totalCnt.Add(1)
		p.errCnt.Add(1)
		idx := p.submitIdx.Add(1) - 1
		p.recordRejectedResult(idx, err)
		p.addInFlight.Add(-1)
		return err
	}

	// 原子化背压检查：与 submitIndexed 一致的 maxPending + overflowStrat 管控，
	// 消除 TrySubmit 绕过背压限制的安全隐患。
	var pendingAcquired bool
	if mp := p.maxPending; mp > 0 {
		switch p.overflowStrat {
		case core.OverflowBlock:
			if !p.tryAcquirePendingSlot(mp) {
				p.errCnt.Add(1)
				p.totalCnt.Add(1)
				idx := p.submitIdx.Add(1) - 1
				p.recordRejectedResult(idx, core.ErrSubmitTimeout)
				p.addInFlight.Add(-1)
				return core.ErrSubmitTimeout
			}
			pendingAcquired = true
		case core.OverflowDrop:
			if !p.tryAcquirePendingSlot(mp) {
				p.errCnt.Add(1)
				p.totalCnt.Add(1)
				idx := p.submitIdx.Add(1) - 1
				p.recordRejectedResult(idx, core.ErrQueueOverflow)
				p.addInFlight.Add(-1)
				return nil
			}
			pendingAcquired = true
		case core.OverflowError:
			if !p.tryAcquirePendingSlot(mp) {
				p.errCnt.Add(1)
				p.totalCnt.Add(1)
				idx := p.submitIdx.Add(1) - 1
				p.recordRejectedResult(idx, core.ErrQueueOverflow)
				p.addInFlight.Add(-1)
				return core.ErrQueueOverflow
			}
			pendingAcquired = true
		}
	}

	taskCtx, taskCancel := context.WithCancel(ctx)

	// 二次检查：防止检查通过后 Close() 被调用导致 wg 泄漏
	if p.closed.Load() {
		p.addInFlight.Add(-1)
		taskCancel()
		if pendingAcquired {
			p.releasePendingSlot()
		}
		return core.ErrPoolClosed
	}

	p.totalCnt.Add(1)
	idx := p.resultsAppend()
	taskCancel = p.trackCancel(idx, taskCancel)

	if !pendingAcquired {
		p.pending.Add(1)
	}
	p.registerTask()
	p.addInFlight.Add(-1)

	maxR := int(p.maxResults.Load())
	record := func(r core.Result[T], _ int) {
		p.resultsSet(idx, r)
		p.recordToStream(r)
		if p.ringBufFlag.Load() && p.ringBuf != nil {
			if maxR <= 0 || int(idx) >= maxR {
				p.ringBuf.Push(r)
			}
		}
	}

	task := core.PoolTask[T]{
		Ctx:      taskCtx,
		Cancel:   taskCancel,
		Fn:       fn,
		Timeout:  p.timeout,
		FailFast: p.failFast.Load(),
		Index:    int(idx),
		Record:   record,
	}

	if err := taskCtx.Err(); err != nil {
		p.discardTask(record, int(idx), taskCancel, err)
		return err
	}
	sent, err := p.trySend(task)
	if !sent {
		if err == nil {
			err = core.ErrSubmitTimeout
		}
		p.discardTask(record, int(idx), taskCancel, err)
		return err
	}
	return nil
}

// ──────────────────────────── Resize ────────────────────────────

// Resize 动态调整 worker 数量，返回新增或移除的 worker 数量。
// newSize 小于等于零时使用 core.IO()。
// 扩容时立即启动新 worker，缩容时向退出的 worker 发送退出信号。
//
// 参数：
//   - newSize：目标 worker 数量
//
// 示例：
//
//	added := p.Resize(8)  // 扩容到 8 个 worker
//	fmt.Printf("新增 %d 个 worker\n", added)
//	removed := p.Resize(2) // 缩容到 2 个 worker
//	fmt.Printf("移除 %d 个 worker\n", removed)
func (p *Pool[T]) Resize(newSize int) int {
	p.resizeMu.Lock()
	defer p.resizeMu.Unlock()
	return p.resizeLocked(newSize)
}

func (p *Pool[T]) resizeLocked(newSize int) int {
	if p.closed.Load() {
		return 0
	}
	if newSize <= 0 {
		newSize = core.IO()
	}
	// 更新 AutoScale 边界，保持手动 resize 后的范围合理
	p.autoScaleMu.Lock()
	if p.IsAutoScaleEnabled() && p.autoScale != nil {
		if newSize < p.autoScale.MinWorkers {
			p.autoScale.MinWorkers = newSize
		}
		if newSize > p.autoScale.MaxWorkers {
			p.autoScale.MaxWorkers = newSize
		}
	}
	p.autoScaleMu.Unlock()

	current := int(p.size.Load())
	if newSize > current {
		added := newSize - current
		p.size.Store(int32(newSize))
		for i := 0; i < added; i++ {
			p.workerWg.Add(1)
			go p.worker()
		}
		return added
	} else if newSize < current {
		quit := current - newSize
		p.size.Store(int32(newSize))
		p.quitting.Add(int32(quit))
		if !p.closed.Load() {
			p.quitMu.Lock()
			close(p.quitCh)
			p.quitCh = make(chan struct{})
			p.quitMu.Unlock()
			for i := 0; i < quit; i++ {
				p.sendQuitSignal()
			}
		}
		return quit
	}
	return 0
}

// sendQuitSignal 向 taskCh 发送退出信号（尽力而为）。
// 如果 taskCh 已满则静默丢弃，因为 worker 处理完任务后会通过 quitting 计数器自行退出。
// taskCh 可能在 Close() 中被关闭，用 recover 防止 panic。
func (p *Pool[T]) sendQuitSignal() {
	defer func() { recover() }()
	select {
	case p.taskCh <- core.PoolTask[T]{Quit: true}:
	default:
	}
}

// ResizeAndWaitTimeout 调整 worker 数量并等待任务完成（最多等待 timeout）。
//
// 参数：
//   - newSize：目标 worker 数量
//   - timeout：等待已执行任务完成的最大时长
//
// 示例：
//
//	// 缩容到 5 个 worker，最多等待 10 秒让正在执行的任务完成
//	p.ResizeAndWaitTimeout(5, 10*time.Second)
func (p *Pool[T]) ResizeAndWaitTimeout(newSize int, timeout time.Duration) {
	p.Resize(newSize)
	p.resizeGuards.Add(1)
	defer p.resizeGuards.Add(-1)
	p.waitAddInFlight()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	p.taskWaitMu.Lock()
	done := p.taskWait
	p.taskWaitMu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-timer.C:
	}
}

func (p *Pool[T]) registerTask() {
	p.taskWaitMu.Lock()
	if p.taskCount == 0 {
		p.taskWait = make(chan struct{})
	}
	p.taskCount++
	p.wg.Add(1)
	p.taskWaitMu.Unlock()
}

func (p *Pool[T]) finishTask() {
	p.taskWaitMu.Lock()
	p.wg.Done()
	p.taskCount--
	if p.taskCount == 0 {
		close(p.taskWait)
		p.taskWait = nil
	}
	p.taskWaitMu.Unlock()
}

func (p *Pool[T]) enqueueTask(ctx context.Context, taskCtx context.Context, taskCancel context.CancelFunc, task core.PoolTask[T], record core.PoolRecordFunc[T], idx int) error {
	var timer *time.Timer
	var timerC <-chan time.Time
	if p.submitTimeout > 0 {
		timer = time.NewTimer(p.submitTimeout)
		defer timer.Stop()
		timerC = timer.C
	}
	if p.maxPending > 0 && p.overflowStrat == core.OverflowBlock {
		for {
			p.pendingMu.Lock()
			changed := p.pendingChange
			acquired := p.tryAcquirePendingSlot(p.maxPending)
			p.pendingMu.Unlock()
			if acquired {
				break
			}
			var err error
			select {
			case <-p.done:
				err = core.ErrPoolClosed
			case <-taskCtx.Done():
				err = taskCtx.Err()
			case <-timerC:
				err = core.ErrSubmitTimeout
			case <-changed:
				continue
			}
			var zero T
			record(core.Result[T]{Value: zero, Err: err, Occupied: true}, idx)
			p.errCnt.Add(1)
			taskCancel()
			p.finishTask()
			return err
		}
	}
	if timer != nil {
		sent, err := p.blockSend(task, taskCtx, timerC)
		if sent {
			return nil
		}
		p.discardTask(record, idx, taskCancel, err)
		return err
	}

	// 快速路径：先检查 done/taskCtx，再非阻塞发送（trySend 内置 recover 防 close panic）
	select {
	case <-p.done:
		p.discardTask(record, idx, taskCancel, core.ErrPoolClosed)
		return core.ErrPoolClosed
	case <-taskCtx.Done():
		p.discardTask(record, idx, taskCancel, taskCtx.Err())
		return taskCtx.Err()
	default:
	}
	if sent, err := p.trySend(task); sent {
		return nil
	} else if err != nil {
		p.discardTask(record, idx, taskCancel, err)
		return err
	}

	// 慢路径：channel 满，阻塞等待，带指数退避防止 CPU 空转。
	// blockSend 复用 submitTimeout timer，退避复用 backoffTimer 避免每次循环分配新 Timer。
	warnTicker := time.NewTicker(core.SlotAcquireWarnTimeout)
	defer warnTicker.Stop()

	backoffTimer := time.NewTimer(0)
	if !backoffTimer.Stop() {
		<-backoffTimer.C
	}
	defer backoffTimer.Stop()

	backoff := 50 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for {
		if sent, err := p.blockSend(task, taskCtx, warnTicker.C); sent {
			return nil
		} else if errors.Is(err, core.ErrPoolClosed) {
			p.discardTask(record, idx, taskCancel, err)
			return err
		} else if errors.Is(err, core.ErrSubmitTimeout) {
			core.LogCtxWarn(ctx, "async: Pool.Submit blocking on task queue, consider setting WithSubmitTimeout",
				core.Dur("elapsed", core.SlotAcquireWarnTimeout))
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			backoffTimer.Reset(backoff/2 + jitter)
			select {
			case <-backoffTimer.C:
			case <-p.done:
				p.discardTask(record, idx, taskCancel, core.ErrPoolClosed)
				return core.ErrPoolClosed
			case <-taskCtx.Done():
				p.discardTask(record, idx, taskCancel, taskCtx.Err())
				return taskCtx.Err()
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		// taskCtx.Done() fired
		p.discardTask(record, idx, taskCancel, taskCtx.Err())
		return taskCtx.Err()
	}
}

// blockSend 安全地阻塞发送任务到 taskCh。
// 返回 (true, nil) 表示发送成功，
// 返回 (false, error) 表示发送失败（可能是通道关闭、context 取消或超时）。
func (p *Pool[T]) blockSend(task core.PoolTask[T], taskCtx context.Context, timerC <-chan time.Time) (sent bool, err error) {
	if p.closed.Load() {
		return false, core.ErrPoolClosed
	}

	// 先检查取消，避免 select 随机选到 taskCh 而忽略取消信号
	select {
	case <-p.done:
		return false, core.ErrPoolClosed
	case <-taskCtx.Done():
		return false, taskCtx.Err()
	default:
	}

	defer func() {
		if r := recover(); r != nil {
			sent = false
			err = core.ErrPoolClosed
		}
	}()
	select {
	case p.taskCh <- task:
		return true, nil
	case <-p.done:
		return false, core.ErrPoolClosed
	case <-taskCtx.Done():
		return false, taskCtx.Err()
	case <-timerC:
		return false, core.ErrSubmitTimeout
	}
}

// trySend 非阻塞安全发送到 taskCh，用于 TrySubmit。
// 通道关闭时通过 recover 安全返回 (false, ErrPoolClosed)。
func (p *Pool[T]) trySend(task core.PoolTask[T]) (sent bool, err error) {
	if p.closed.Load() {
		return false, core.ErrPoolClosed
	}

	// 先检查 done，避免 select 随机选到 taskCh 而忽略关闭信号
	select {
	case <-p.done:
		return false, core.ErrPoolClosed
	default:
	}

	defer func() {
		if r := recover(); r != nil {
			sent = false
			err = core.ErrPoolClosed
		}
	}()
	select {
	case p.taskCh <- task:
		return true, nil
	default:
		return false, nil
	}
}

func (p *Pool[T]) discardTask(record core.PoolRecordFunc[T], idx int, taskCancel context.CancelFunc, err error) {
	var zero T
	record(core.Result[T]{Value: zero, Err: err, Occupied: true}, idx)
	p.errCnt.Add(1)
	taskCancel()
	p.releasePendingSlot()
	p.finishTask()
}

func (p *Pool[T]) releasePendingSlot() {
	if p.maxPending <= 0 || p.overflowStrat != core.OverflowBlock {
		p.pending.Add(-1)
		return
	}
	p.pendingMu.Lock()
	p.pending.Add(-1)
	close(p.pendingChange)
	p.pendingChange = make(chan struct{})
	p.pendingMu.Unlock()
}

// tryAcquirePendingSlot 原子地检查并获取一个等待槽位。
// 使用 CAS 循环保证"检查上限→递增计数"的原子性，消除传统"先读后写"带来的 TOCTOU 竞态窗口。
// 返回 true 表示获取成功（pending 已递增），false 表示已达上限。
func (p *Pool[T]) tryAcquirePendingSlot(limit int64) bool {
	for i := 0; ; i++ {
		cur := p.pending.Load()
		if cur >= limit {
			return false
		}
		if p.pending.CompareAndSwap(cur, cur+1) {
			return true
		}
		if i >= 3 {
			runtime.Gosched()
		}
	}
}

// waitAddInFlight 等待所有处于 precheck 与 wg.Add(1) 之间的协程完成 wg.Add(1)。
// 必须在 submitGuard 设为 true 之后、wg.Wait() 之前调用，保证二者之间的同步。
//
// 采用渐进退避：绝大多数场景下 addInFlight 在微秒级降为 0，
// 前几次迭代用 Gosched 快速轮询，超时后逐步放大等待间隔，
// 避免极端高并发下 CPU 空转。
func (p *Pool[T]) waitAddInFlight() {
	for i := 0; p.addInFlight.Load() > 0; i++ {
		switch {
		case i < 4:
			runtime.Gosched()
		case i < 32:
			time.Sleep(time.Microsecond)
		default:
			time.Sleep(100 * time.Microsecond)
		}
	}
}

// Wait 阻塞等待所有已提交的任务完成。主结果按提交顺序排列；
// 配置 MaxResults 和环形缓冲时，剩余溢出结果按写入缓冲的顺序追加，可能因缓冲满而丢失。
// Wait 只能调用一次，多次调用返回空切片。
//
// 示例：
//
//	results := p.Wait()
//	for _, r := range results {
//	    if r.Ok() {
//	        fmt.Printf("成功: %v\n", r.Value)
//	    } else {
//	        log.Printf("失败: %v\n", r.Err)
//	    }
//	}
func (p *Pool[T]) Wait() []core.Result[T] {
	if !p.waitInvoked.CompareAndSwap(false, true) {
		core.LogCtxWarn(p.Ctx(), "async: Pool.Wait called multiple times, returning nil")
		return nil
	}
	// 设置提交保护，禁止新 Submit
	p.submitGuard.Store(true)
	// 等待所有已通过 precheck 的协程完成 wg.Add(1)，避免与 wg.Wait() 竞态
	p.waitAddInFlight()
	p.wg.Wait()
	p.waited.Store(true)
	p.submitGuard.Store(false)

	p.cancelAllShards()

	results := p.resultsCollect()

	// 环形缓冲作为溢出区时，将尚未取出且未丢失的结果追加到主结果后。
	if p.ringBufFlag.Load() && p.ringBuf != nil && p.maxResults.Load() > 0 {
		overflow := p.ringBuf.Flush()
		results = append(results, overflow...)
	}

	p.cancelContext()
	p.drainStreaming()
	return results
}

// WaitAndClose 等待所有任务完成后关闭池，返回结果。
// 等价于先调用 Wait() 再调用 Close()。
//
// 示例：
//
//	results := p.WaitAndClose()
func (p *Pool[T]) WaitAndClose() []core.Result[T] {
	results := p.Wait()
	p.Close()
	return results
}

// ResetWait 轻量复位池的 Wait/submit 状态以支持复用。
// 不会重启 worker，仅清零计数器与结果存储。
// 必须在 Wait() 之后、下次 Submit() 之前调用。
// After a timed wait it joins unfinished tasks before resetting their state.
// ResetWait 不重建 WithContext 或 WithFailFast 创建的上下文；Wait 后它们已取消，
// 如需在下一批使用池上下文，须使用新的父上下文重新调用相应配置方法。
//
// 示例：
//
//	p.Submit(ctx, task1)
//	r1 := p.Wait()
//	p.ResetWait()
//	p.Submit(ctx, task2)
//	r2 := p.Wait()
func (p *Pool[T]) ResetWait() *Pool[T] {
	if !p.waited.Load() {
		return p
	}
	if p.waitCleanup != nil {
		<-p.waitCleanup
	}
	p.wg.Wait()
	if p.streamCh != nil {
		p.streamCh = make(chan core.Result[T], cap(p.streamCh))
		p.streamOnce = sync.Once{}
		p.streamDropped.Store(0)
	}
	p.waitInvoked.Store(false)
	p.waited.Store(false)
	p.waitCanceled.Store(false)
	p.submitIdx.Store(0)
	p.totalCnt.Store(0)
	p.errCnt.Store(0)
	p.okCnt.Store(0)
	p.submitGuard.Store(false)
	p.addInFlight.Store(0)
	for i := range p.shards {
		p.shards[i].mu.Lock()
		p.shards[i].results = nil
		p.shards[i].cancels = nil
		p.shards[i].mu.Unlock()
	}
	if p.ringBuf != nil {
		p.ringBuf.Reset()
	}
	return p
}

// Close 关闭协程池，等待所有 worker 退出后返回。
// 关闭后不能再提交新任务。
//
// 示例：
//
//	p.Close()
func (p *Pool[T]) Close() {
	p.resizeMu.Lock()
	if !p.closed.CompareAndSwap(false, true) {
		done := p.closeDone
		p.resizeMu.Unlock()
		<-done
		return
	}
	closeDone := p.closeDone
	p.resizeMu.Unlock()
	defer close(closeDone)
	p.submitGuard.Store(true)
	p.waitAddInFlight()
	close(p.done)
	p.workerWg.Wait()

	p.drainOrphanTasks()

	close(p.taskCh)
	for task := range p.taskCh {
		if task.Quit || task.Record == nil {
			continue
		}
		p.discardTask(task.Record, task.Index, task.Cancel, core.ErrPoolClosed)
	}

	p.wg.Wait()
	p.drainStreaming()
}

// drainOrphanTasks 排空孤儿任务 —— 这些任务在 worker 退出后才被发送到 taskCh，
// 没有 worker 会处理它们，需要手动清理避免 Wait() 永久阻塞。
func (p *Pool[T]) drainOrphanTasks() {
	for {
		select {
		case task := <-p.taskCh:
			if task.Quit || task.Record == nil {
				continue
			}
			p.discardTask(task.Record, task.Index, task.Cancel, core.ErrPoolClosed)
		default:
			return
		}
	}
}

// CloseAndWait 关闭池并等待所有任务完成。
// 等价于先调用 Close() 再调用 Wait()。
//
// 示例：
//
//	p.CloseAndWait()
func (p *Pool[T]) CloseAndWait() {
	p.Close()
	p.Wait()
}

// CloseAndWaitTimeout 关闭池并等待，最多等待 timeout 后返回。
// ok 为 true 表示在超时内正常完成，workerDone channel 可用于异步等待 worker 退出。
// 首次关闭超时返回的 workerDone 关闭时，队列清理可能仍在进行；调用 Close 可等待完整清理。
// 若关闭已在进行，等待同一次关闭；超时时返回的通道在关闭清理完成后关闭。
//
// 示例：
//
//	ok, workerDone := p.CloseAndWaitTimeout(10 * time.Second)
//	if !ok {
//	    go func() {
//	        <-workerDone
//	        log.Println("worker 已全部退出")
//	    }()
//	}
func (p *Pool[T]) CloseAndWaitTimeout(timeout time.Duration) (ok bool, workerDone <-chan struct{}) {
	p.resizeMu.Lock()
	if !p.closed.CompareAndSwap(false, true) {
		done := p.closeDone
		p.resizeMu.Unlock()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-done:
			return true, nil
		case <-timer.C:
			return false, done
		}
	}
	closeDone := p.closeDone
	p.resizeMu.Unlock()
	p.submitGuard.Store(true)
	workerDoneCh := make(chan struct{})
	go func() {
		defer close(closeDone)
		p.waitAddInFlight()
		close(p.done)
		p.workerWg.Wait()
		close(workerDoneCh)
		p.drainOrphanTasks()
		close(p.taskCh)
		for task := range p.taskCh {
			if task.Quit || task.Record == nil {
				continue
			}
			p.discardTask(task.Record, task.Index, task.Cancel, core.ErrPoolClosed)
		}

		p.wg.Wait()
		p.drainStreaming()
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-closeDone:
		return true, nil
	case <-timer.C:
		return false, workerDoneCh
	}
}

// CloseByIdle 最多等待 timeout 使池进入空闲状态，然后同步关闭池。
// 空闲指 Active()==0 且 Busy()==0。关闭会等待 worker 退出，
// 因此任务未结束时总耗时可能超过 timeout。
//
// 参数：
//   - timeout：等待空闲的时间上限，不限制关闭耗时
//
// 示例：
//
//	// 最多等待 30 秒让池进入空闲状态
//	p.CloseByIdle(30 * time.Second)
func (p *Pool[T]) CloseByIdle(timeout time.Duration) {
	waitStart := time.Now()
	for {
		if p.Active() == 0 && p.Busy() == 0 {
			p.Close()
			return
		}
		if time.Since(waitStart) >= timeout {
			p.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// FailCount 返回执行失败的任务数。
func (p *Pool[T]) FailCount() int64 {
	return p.errCnt.Load()
}

// OkCount 返回执行成功的任务数。
func (p *Pool[T]) OkCount() int64 {
	return p.okCnt.Load()
}

// HasError 返回是否有任务失败。
//
// 示例：
//
//	if p.HasError() {
//	    log.Printf("有 %d 个任务失败", p.FailCount())
//	}
func (p *Pool[T]) HasError() bool {
	return p.FailCount() > 0
}

// TotalCount 返回已提交的任务总数。
func (p *Pool[T]) TotalCount() int64 {
	return p.totalCnt.Load()
}

// Size 返回当前 worker 数量。
func (p *Pool[T]) Size() int {
	return int(p.size.Load())
}

// Active 返回当前活跃（取得任务）的 worker 数量。
func (p *Pool[T]) Active() int {
	return int(p.active.Load())
}

// Busy 返回当前正在执行任务的 worker 数量。
func (p *Pool[T]) Busy() int {
	return int(p.busy.Load())
}

// Pending 返回排队中的任务数量。
func (p *Pool[T]) Pending() int {
	return int(p.pending.Load())
}

// PoolStats 池统计信息。
type PoolStats struct {
	Size      int           // worker 数量
	Active    int           // 当前活跃任务数
	Busy      int           // 当前忙碌任务数（正在执行 fn）
	Pending   int           // 等待中的任务数
	FailFast  bool          // 是否启用 FailFast
	Timeout   time.Duration // 全局任务超时时间
	TotalTask int64         // 历史提交任务总数
	OkTask    int64         // 历史成功任务数
	FailTask  int64         // 历史失败任务数
}

func (s PoolStats) String() string {
	return fmt.Sprintf("PoolStats{size=%d, active=%d, busy=%d, pending=%d, failFast=%v, timeout=%v, total=%d, success=%d, fail=%d}",
		s.Size, s.Active, s.Busy, s.Pending, s.FailFast, s.Timeout, s.TotalTask, s.OkTask, s.FailTask)
}

// Stats 返回池的完整统计信息。
//
// 示例：
//
//	stats := p.Stats()
//	fmt.Printf("并发度: %d, 成功: %d, 失败: %d\n",
//	    stats.Size, stats.OkTask, stats.FailTask)
func (p *Pool[T]) Stats() PoolStats {
	return PoolStats{
		Size:      p.Size(),
		Active:    p.Active(),
		Busy:      p.Busy(),
		Pending:   p.Pending(),
		FailFast:  p.failFast.Load(),
		Timeout:   p.timeout,
		TotalTask: p.TotalCount(),
		OkTask:    p.OkCount(),
		FailTask:  p.FailCount(),
	}
}

// Errors 返回主结果存储中的非 nil 错误（建议在 Wait 后调用）。
// 超出 MaxResults 的溢出结果不包含在内，应从 Wait 或 Flush 返回值中处理。
//
// 示例：
//
//	p.Wait()
//	for _, err := range p.Errors() {
//	    log.Printf("任务错误: %v", err)
//	}
func (p *Pool[T]) Errors() []error {
	if !p.waited.Load() {
		core.LogCtxWarn(p.Ctx(), "async: Pool.Errors called before Wait, results may be incomplete", core.Str("type", "Pool"))
	}
	results := p.resultsCollect()
	capacity := min(int64(len(results)), p.FailCount())
	errs := make([]error, 0, int(capacity))
	for _, r := range results {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errs
}

// FirstError 返回主结果存储中的首个任务错误（建议在 Wait 后调用）。
// 超出 MaxResults 的溢出结果不包含在内。
//
// 示例：
//
//	p.Wait()
//	if firstErr := p.FirstError(); firstErr != nil {
//	    log.Printf("首个失败: %v", firstErr)
//	}
func (p *Pool[T]) FirstError() error {
	if !p.waited.Load() {
		core.LogCtxWarn(p.Ctx(), "async: Pool.FirstError called before Wait, results may be incomplete", core.Str("type", "Pool"))
	}
	results := p.resultsCollect()
	for _, r := range results {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// JoinErrors 合并主结果存储中的错误（使用 errors.Join）。
// 超出 MaxResults 的溢出结果不包含在内。
//
// 示例：
//
//	p.Wait()
//	if err := p.JoinErrors(); err != nil {
//	    if errors.Is(err, core.ErrTimeout) {
//	        log.Println("存在超时错误")
//	    }
//	}
func (p *Pool[T]) JoinErrors() error {
	return errors.Join(p.Errors()...)
}

// WaitTimeout 带超时的任务等待，ok 为 true 表示在超时前完成。
//
// 参数：
//   - d：最长等待时间
//
// 示例：
//
//	results, ok := p.WaitTimeout(5 * time.Second)
//	if !ok {
//	    log.Println("等待超时")
//	}
func (p *Pool[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return p.WaitContext(ctx)
}

// WaitContext 使用 Context 控制等待，ctx 取消或超时后返回。
//
// 参数：
//   - ctx：上下文，取消后返回部分结果
//
// 示例：
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	results, ok := p.WaitContext(ctx)
func (p *Pool[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool) {
	if !p.waitInvoked.CompareAndSwap(false, true) {
		core.LogCtxWarn(p.Ctx(), "async: Pool.WaitContext called after a Wait variant already completed")
		return nil, false
	}
	p.submitGuard.Store(true)
	done := make(chan struct{})
	p.waitCleanup = done
	go func() {
		p.waitAddInFlight()
		p.wg.Wait()
		p.drainStreaming()
		close(done)
	}()
	ok := false
	select {
	case <-done:
		ok = true
	case <-ctx.Done():
		p.waitCanceled.Store(true)
	}
	p.cancelContext()
	p.cancelAllShards()
	results := p.resultsCollect()
	if p.ringBufFlag.Load() && p.ringBuf != nil && p.maxResults.Load() > 0 {
		overflow := p.ringBuf.Flush()
		results = append(results, overflow...)
	}
	p.waited.Store(true)
	p.submitGuard.Store(false)
	return results, ok
}

// Values 提取主结果存储中成功的值（建议在 Wait 后调用）。
// 超出 MaxResults 的溢出结果不包含在内。
//
// 示例：
//
//	p.Wait()
//	values := p.Values()
//	fmt.Printf("成功值: %v\n", values)
func (p *Pool[T]) Values() []T {
	results := p.resultsCollect()
	vals := make([]T, 0, len(results))
	for _, r := range results {
		if r.Occupied && r.Err == nil {
			vals = append(vals, r.Value)
		}
	}
	return vals
}

// Reset 关闭旧池并创建新池，保留并发度等配置。
// 必须在 Wait 之后调用，否则返回错误。
//
// 使用示例：
//
//	p.Wait()
//	newP, err := p.Reset()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer newP.Close()
func (p *Pool[T]) Reset() (*Pool[T], error) {
	if !p.waited.Load() {
		return p, fmt.Errorf("async: Pool.Reset called before Wait")
	}
	if p.waitCleanup != nil {
		<-p.waitCleanup
	}
	p.resizeMu.Lock()
	if p.closed.Load() {
		done := p.closeDone
		p.resizeMu.Unlock()
		<-done
		p.resizeMu.Lock()
	}
	defer p.resizeMu.Unlock()
	if !p.waited.Load() {
		return p, fmt.Errorf("async: Pool.Reset called before Wait")
	}
	p.autoScaleMu.Lock()
	if p.autoScaleEnabled.Load() && p.autoScaleStop != nil {
		close(p.autoScaleStop)
		p.autoScaleStop = nil
	}
	p.autoScaleMu.Unlock()
	p.wg.Wait()
	oldTaskCh := p.taskCh

	// 清空分片存储
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.Lock()
		s.results = nil
		s.cancels = nil
		s.mu.Unlock()
	}

	p.submitIdx.Store(0)
	p.totalCnt.Store(0)
	savedTimeout := p.timeout
	savedSubmitTimeout := p.submitTimeout
	savedMaxPending := p.maxPending
	savedOverflowStrat := p.overflowStrat
	savedMaxResults := p.maxResults.Load()
	savedRingBuf := p.ringBuf
	savedStreamCh := p.streamCh
	savedResultCb := p.resultCb
	savedFailFast := p.failFast.Load()
	p.cancelMu.Lock()
	p.ctx = context.Background()
	p.failCancel = nil
	p.errCnt.Store(0)
	p.okCnt.Store(0)
	p.failFast.Store(savedFailFast)
	if savedFailFast {
		ctx, cancel := context.WithCancel(context.Background())
		p.ctx = ctx
		p.cancel = cancel
	} else {
		p.cancel = nil
	}
	p.cancelMu.Unlock()
	p.quitting.Store(0)
	p.streamDropped.Store(0)
	if savedStreamCh != nil {
		p.streamOnce = sync.Once{}
	}
	newSize := int(p.size.Load())

	func() {
		defer func() { recover() }()
		close(oldTaskCh)
	}()

	// 等待旧 worker 全部退出后再替换 taskCh，避免新旧 worker 竞争
	p.workerWg.Wait()

	// 重建 done channel：Close() 后 done 已关闭，Reset 必须重建，否则新 worker 会立即退出。
	p.done = make(chan struct{})
	p.closeDone = make(chan struct{})

	newTaskCh := make(chan core.PoolTask[T], newSize*2)
	p.taskCh = newTaskCh

	// 先替换 taskCh 再开放 Submit 入口，防止 Submit 向已关闭的 oldTaskCh 发送导致任务丢失
	p.submitGuard.Store(false)
	p.waited.Store(false)
	p.waitInvoked.Store(false)
	p.waitCanceled.Store(false)
	p.closed.Store(false)
	p.quitMu.Lock()
	p.quitCh = make(chan struct{})
	p.quitMu.Unlock()
	p.timeout = savedTimeout
	p.submitTimeout = savedSubmitTimeout
	p.maxPending = savedMaxPending
	p.overflowStrat = savedOverflowStrat
	p.maxResults.Store(savedMaxResults)
	if savedStreamCh != nil {
		p.streamCh = make(chan core.Result[T], cap(savedStreamCh))
	}
	p.resultCb = savedResultCb
	if savedRingBuf != nil {
		p.ringBuf = core.NewRingBuffer[core.Result[T]](savedRingBuf.Cap(), savedRingBuf.Overflow())
		p.ringBufFlag.Store(true)
	} else {
		p.ringBufFlag.Store(false)
	}

	// 重启自动扩缩容 goroutine：Close() 关闭 done 后 autoScale goroutine 已退出，
	// Reset 后若 autoScale 仍启用，需重建 stopCh 并重新启动后台检测循环。
	p.autoScaleMu.Lock()
	if p.autoScaleEnabled.Load() && p.autoScale != nil {
		stopCh := make(chan struct{})
		p.autoScaleStop = stopCh
		configCopy := *p.autoScale
		config := &configCopy
		p.autoScale = config
		done := p.done
		go func() {
			p.autoScaleLoop(config, stopCh, done)
		}()
	}
	p.autoScaleMu.Unlock()

	p.workerWg.Add(newSize)
	for i := 0; i < newSize; i++ {
		go p.worker()
	}
	return p, nil
}

// ──────────────────────────── 自动扩缩容 ────────────────────────────

// EnableAutoScale 启用自动扩缩容。基于 busy/size 比率周期性检测负载：
// - busy/size > ScaleUpThreshold 持续 ScaleUpChecks 次 → 扩容（乘以 ScaleUpFactor，上限 MaxWorkers）
// - busy/size < ScaleDownThreshold 持续 ScaleDownChecks 次 → 缩容（乘以 ScaleDownFactor，下限 MinWorkers）
//
// config 为 nil 时使用 DefaultAutoScaleConfig()（CPU*2 ~ CPU*100，每 5s 检测）。
// 默认扩容因子 1.5（增加 50%），缩容因子 0.75（保留 75%），比翻倍/减半更加平滑。
//
// 重复调用是安全的（幂等）。调用后启动后台 goroutine 进行负载检测。
//
// 示例：
//
//	// 使用默认配置
//	p.EnableAutoScale(nil)
//
//	// 自定义配置：10~500 worker，每次扩容 20%，每 3 秒检测
//	p.EnableAutoScale(&core.AutoScaleConfig{
//	    MinWorkers: 10,
//	    MaxWorkers: 500,
//	    CheckInterval: 3 * time.Second,
//	    ScaleUpFactor: 1.2,
//	})
func (p *Pool[T]) EnableAutoScale(config *core.AutoScaleConfig) {
	p.resizeMu.Lock()
	defer p.resizeMu.Unlock()
	p.autoScaleMu.Lock()
	defer p.autoScaleMu.Unlock()
	if p.autoScaleEnabled.Load() {
		return
	}
	if config == nil {
		config = core.DefaultAutoScaleConfig()
	}
	configCopy := *config
	config = &configCopy
	config.Normalize()

	p.autoScale = config
	if p.autoScaleEnabled.CompareAndSwap(false, true) {
		stopCh := make(chan struct{})
		p.autoScaleStop = stopCh
		done := p.done
		go func() {
			p.autoScaleLoop(config, stopCh, done)
		}()
	}
}

// DisableAutoScale 停止自动扩缩容，将 worker 数量恢复到初始值。
//
// 停止后仍可通过 Resize 手动调整。如果自动扩缩容未启用，调用无效果。
func (p *Pool[T]) DisableAutoScale() {
	p.resizeMu.Lock()
	defer p.resizeMu.Unlock()
	p.autoScaleMu.Lock()
	minWorkers := 0
	if p.autoScaleEnabled.CompareAndSwap(true, false) {
		close(p.autoScaleStop)

		if p.autoScale != nil {
			minWorkers = p.autoScale.MinWorkers
		}
	}
	p.autoScaleMu.Unlock()
	if minWorkers > 0 && p.Size() != minWorkers {
		p.resizeLocked(minWorkers)
	}
}

// IsAutoScaleEnabled 返回是否已启用自动扩缩容。
func (p *Pool[T]) IsAutoScaleEnabled() bool {
	return p.autoScaleEnabled.Load()
}

// autoScaleLoop 自动扩缩容后台检测循环。
func (p *Pool[T]) autoScaleLoop(config *core.AutoScaleConfig, stopCh, done <-chan struct{}) {
	ticker := time.NewTicker(config.CheckInterval)
	defer ticker.Stop()

	var scaleUpCount, scaleDownCount int

	for {
		select {
		case <-done:
			return
		case <-stopCh:
			return
		case <-ticker.C:
			p.performAutoScaleCheck(config, &scaleUpCount, &scaleDownCount)
		}
	}
}

func (p *Pool[T]) performAutoScaleCheck(config *core.AutoScaleConfig, scaleUpCount, scaleDownCount *int) {
	activeConfig := config
	p.autoScaleMu.Lock()
	snapshot := *config
	p.autoScaleMu.Unlock()
	config = &snapshot
	if p.closed.Load() || p.submitGuard.Load() || p.resizeGuards.Load() > 0 {
		return
	}

	size := p.Size()
	busy := p.Busy()
	if size == 0 {
		return
	}

	busyRatio := float64(busy) / float64(size)

	if busyRatio > config.ScaleUpThreshold {
		*scaleDownCount = 0
		*scaleUpCount++
		if *scaleUpCount >= config.ScaleUpChecks {
			newSize := int(float64(size) * config.ScaleUpFactor)
			if newSize <= size {
				newSize = size + 1
			}
			if newSize > config.MaxWorkers {
				newSize = config.MaxWorkers
			}
			if newSize > size {
				p.resizeAutoScale(activeConfig, newSize)
			}
			*scaleUpCount = 0
		}
	} else if busyRatio < config.ScaleDownThreshold {
		*scaleUpCount = 0
		*scaleDownCount++
		if *scaleDownCount >= config.ScaleDownChecks {
			newSize := int(float64(size) * config.ScaleDownFactor)
			if newSize >= size {
				newSize = size - 1
			}
			if newSize < config.MinWorkers {
				newSize = config.MinWorkers
			}
			if newSize < size {
				p.resizeAutoScale(activeConfig, newSize)
			}
			*scaleDownCount = 0
		}
	} else {
		*scaleUpCount = 0
		*scaleDownCount = 0
	}
}

func (p *Pool[T]) resizeAutoScale(config *core.AutoScaleConfig, newSize int) {
	p.resizeMu.Lock()
	defer p.resizeMu.Unlock()
	p.autoScaleMu.Lock()
	valid := p.autoScaleEnabled.Load() && p.autoScale == config
	p.autoScaleMu.Unlock()
	if valid {
		p.resizeLocked(newSize)
	}
}
