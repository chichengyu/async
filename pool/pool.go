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

	"github.com/chichengyu/async/core"
)

// poolShardCount 结果存储分片数，降低锁竞争到 1/32。
const poolShardCount = 32

// resultShard 单个结果存储分片，有独立的锁。
type resultShard[T any] struct {
	mu      sync.Mutex
	results []core.Result[T]
	cancels []context.CancelFunc
}

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
	taskCh        chan core.PoolTask[T]          // 任务队列（缓冲 channel）
	wg            sync.WaitGroup                 // 等待所有已提交任务完成
	workerWg      sync.WaitGroup                 // 等待所有 worker 退出
	shards        [poolShardCount]resultShard[T] // 分片结果存储
	submitIdx     atomic.Int64                   // 原子提交序号（用于分片路由）
	submitGuard   atomic.Bool                    // 提交保护：Wait期间禁止新提交
	addInFlight   atomic.Int64                   // 处于 precheck 通过后、wg.Add(1) 前的协程数，用于与 Wait 同步
	errCnt        int64                          // 失败任务计数（atomic 原子操作）
	cancel        context.CancelFunc             // 全局取消函数
	failFast      atomic.Bool                    // 是否启用 FailFast 模式
	timeout       time.Duration                  // 全局任务超时时间（0=无限制）
	submitTimeout time.Duration                  // 任务提交超时时间
	closed        atomic.Bool                    // 是否已关闭
	done          chan struct{}                  // 关闭广播信号
	size          atomic.Int32                   // worker 数量
	active        atomic.Int32                   // 当前活跃任务数
	busy          atomic.Int32                   // 当前忙碌任务数
	pending       atomic.Int32                   // 等待中的任务数
	waited        atomic.Bool                    // 是否已完成 Wait
	waitInvoked   atomic.Bool                    // Wait 幂等保护：确保 Wait/WaitTimeout/WaitContext 只执行一次
	quitting      atomic.Int32                   // 正在退出的 worker 计数
	quitCh        chan struct{}                  // 缩容广播信号，close 通知空闲 worker 检查 quitting
	quitMu        sync.RWMutex                   // 保护 quitCh 读写的并发安全
	ctx           context.Context                // 池级别的上下文

	// ──────── 自动扩缩容（可选，默认关闭）────────
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
	maxPending    int32                 // 最大等待任务数（0=无限制）
	overflowStrat core.OverflowStrategy // 溢出策略
	maxResults    int32                 // 最大结果数（0=无限制），防止 results 无界增长
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
		taskCh:  make(chan core.PoolTask[T], size*2),
		timeout: core.GetDefaultTimeout(),
		ctx:     context.Background(),
		done:    make(chan struct{}),
		quitCh:  make(chan struct{}),
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
	p.ctx = ctx
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
	p.cancel = core.MergeCancel(p.cancel, cancel)
	p.ctx = ctx
	return p, ctx
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
	p.cancel = core.MergeCancel(p.cancel, cancel)
	p.failFast.Store(true)
	p.ctx = ctx
	return p, ctx
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

// WithFFSubmitTO 启用 FailFast 并设置提交超时。
// submitTimeout 控制任务入队的最大等待时间。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
//
// 示例：
//
//	p, ffCtx := pool.NewPool[string](4).WithFFSubmitTO(ctx, 5*time.Second)
func (p *Pool[T]) WithFFSubmitTO(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFailFast(ctx)
	p.WithSubmitTimeout(submitTimeout)
	return p, ctx
}

// WithFFSubmitTOTraceID 启用 FailFast、设置提交超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
func (p *Pool[T]) WithFFSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFSubmitTO(ctx, submitTimeout)
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

// WithFFTimeoutTraceID 启用 FailFast、设置超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
func (p *Pool[T]) WithFFTimeoutTraceID(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFTimeout(ctx, timeout)
	return p.WithTraceID(ctx)
}

// WithFFTimeoutSubmitTO 启用 FailFast、设置任务超时和提交超时。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
//   - submitTimeout：提交超时时间
func (p *Pool[T]) WithFFTimeoutSubmitTO(ctx context.Context, timeout, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFTimeout(ctx, timeout)
	p.WithSubmitTimeout(submitTimeout)
	return p, ctx
}

// WithFFTimeoutSubmitTOTraceID 启用 FailFast、设置超时、提交超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
//   - submitTimeout：提交超时时间
func (p *Pool[T]) WithFFTimeoutSubmitTOTraceID(ctx context.Context, timeout, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFTimeoutSubmitTO(ctx, timeout, submitTimeout)
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

// WithCtxTimeoutTraceID 设置 Context、任务超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
func (p *Pool[T]) WithCtxTimeoutTraceID(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context) {
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

// WithCtxSubmitTO 设置 Context 和提交超时。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
func (p *Pool[T]) WithCtxSubmitTO(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithContext(ctx)
	p.WithSubmitTimeout(submitTimeout)
	return p, ctx
}

// WithCtxSubmitTOTraceID 设置 Context、提交超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
func (p *Pool[T]) WithCtxSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithCtxSubmitTO(ctx, submitTimeout)
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
// 回调在 worker goroutine 中执行，应尽量轻量。
//
// 示例：
//
//	p := pool.NewPool[string](8).WithResultCallback(func(r core.Result[string]) {
//	    if r.Ok() {
//	        metrics.RecordSuccess()
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

// WithRingBuffer 启用环形缓冲区替代无限增长的 results 切片。
// capacity 为缓冲区容量，overflow 为满时策略：
//
//	core.OverflowDrop：覆盖最旧结果
//	core.OverflowBlock：返回 false（不阻塞）
//	core.OverflowError：返回 false
//
// 启用后结果会同时写入 results 切片和环形缓冲。
// 要完全避免 results 增长，请配合 WithMaxResults(0) 使用。
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

// WithMaxResults 设置 results 切片的容量上限（0=无限，默认）。
// 当提交数达到上限后，后续任务的结果只通过流式或环形缓冲区消费。
// 这是防止长期运行的池内存持续增长的关键配置。
//
// 示例：
//
//	// 最多保留 100 万个结果在内存中
//	p := pool.NewPool[string](8).WithMaxResults(1_000_000)
func (p *Pool[T]) WithMaxResults(maxResults int) *Pool[T] {
	if maxResults >= 0 {
		p.maxResults = int32(maxResults)
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
		p.maxPending = int32(maxPending)
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
		p.resultCb(r)
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
				if p.quitting.Load() <= 0 {
					continue
				}
				p.quitting.Add(-1)
				return
			}
			p.active.Add(1)
			p.processTask(task)
			p.active.Add(-1)
			if p.quitting.Load() > 0 {
				for {
					v := p.quitting.Load()
					if v <= 0 {
						break
					}
					if p.quitting.CompareAndSwap(v, v-1) {
						return
					}
				}
			}
		case <-quitCh:
			if p.quitting.Load() > 0 {
				for {
					v := p.quitting.Load()
					if v <= 0 {
						break
					}
					if p.quitting.CompareAndSwap(v, v-1) {
						return
					}
				}
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
						if p.quitting.Load() > 0 {
							p.quitting.Add(-1)
						}
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

	defer func() {
		if r := recover(); r != nil {
			core.LogCtxError(taskCtx, "async pool panic recovered",
				core.Any("panic", r),
				core.Bytes("stack", core.NewPanicError(r).Stack))
			var zero T
			record(core.Result[T]{Value: zero, Err: core.NewPanicError(r), Occupied: true}, idx)
			atomic.AddInt64(&p.errCnt, 1)
			if failFast {
				if cancel := p.cancel; cancel != nil {
					cancel()
				}
			}
			p.wg.Done()
			taskCancel()
		}
	}()

	p.pending.Add(-1)
	p.busy.Add(1)
	defer p.busy.Add(-1)

	// 出队后、执行前重检 ctx，消除 blockSend select 随机性带来的竞态窗口
	select {
	case <-taskCtx.Done():
		var zero T
		record(core.Result[T]{Value: zero, Err: taskCtx.Err(), Occupied: true}, idx)
		atomic.AddInt64(&p.errCnt, 1)
		p.wg.Done()
		taskCancel()
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
			atomic.AddInt64(&p.errCnt, 1)
		}
		if failFast {
			if cancel := p.cancel; cancel != nil {
				cancel()
			}
		}
	}

	r := core.Result[T]{Value: val, Err: err, Occupied: true}
	record(r, idx)

	p.wg.Done()
	taskCancel()
}

func (p *Pool[T]) poolPrecheck(ctx context.Context, caller string) error {
	if p.closed.Load() {
		return core.ErrPoolClosed
	}
	if p.waited.Load() {
		return core.ErrPoolWaited
	}
	if p.submitGuard.Load() {
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

// resultsAppend 向分片存储追加一个结果槽位，返回全局索引 idx。
// 如果设置了 maxResults 且已达上限，仅增加序号不追加存储。
func (p *Pool[T]) resultsAppend(taskCancel context.CancelFunc) int64 {
	idx := p.submitIdx.Add(1) - 1

	maxR := int(p.maxResults)
	if maxR > 0 && idx >= int64(maxR) {
		// 超出上限：不存储到分片，仅推进序号
		return idx
	}

	shardIdx := int(idx % poolShardCount)
	s := &p.shards[shardIdx]
	s.mu.Lock()
	s.results = append(s.results, core.Result[T]{})
	s.cancels = append(s.cancels, taskCancel)
	s.mu.Unlock()
	return idx
}

// resultsSet 向分片存储写入指定索引的结果。
func (p *Pool[T]) resultsSet(idx int64, r core.Result[T]) {
	maxR := int(p.maxResults)
	if maxR > 0 && idx >= int64(maxR) {
		return
	}

	shardIdx := int(idx % poolShardCount)
	localIdx := int(idx / poolShardCount)
	s := &p.shards[shardIdx]
	s.mu.Lock()
	if localIdx < len(s.results) {
		s.results[localIdx] = r
	}
	s.mu.Unlock()
}

// resultsPrecheckSet 在 precheck 失败时写入错误结果（如果需要）。
func (p *Pool[T]) resultsPrecheckSet(idx int64, r core.Result[T]) {
	if idx < 0 {
		// append 模式
		idx = p.submitIdx.Add(1) - 1
	}
	p.resultsSet(idx, r)
}

// resultsCollect 收集所有分片的结果，按提交顺序合并。
//
// 优化：不再对每个元素逐一加锁（lock-per-element），
// 改为对每个分片加锁一次并批量拷贝（lock-per-shard）。
// 锁次数从 O(N) 降到 O(poolShardCount)，百万任务下从百万次降到 32 次。
func (p *Pool[T]) resultsCollect() []core.Result[T] {
	total := int(p.submitIdx.Load())
	maxR := int(p.maxResults)
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
		cancels := make([]context.CancelFunc, len(s.cancels))
		copy(cancels, s.cancels)
		s.cancels = nil
		s.mu.Unlock()
		for _, c := range cancels {
			c()
		}
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

func (p *Pool[T]) submitIndexed(ctx context.Context, fn func(context.Context) (T, error)) (index int, err error) {
	// 进入临界区：标记本协程正在 precheck 与 wg.Add(1) 之间，
	// 防止 Wait 系列方法在 submitGuard 设置后立即调用 wg.Wait() 产生竞态。
	p.addInFlight.Add(1)

	if err := p.poolPrecheck(ctx, "Submit"); err != nil {
		p.addInFlight.Add(-1)
		idx := p.submitIdx.Add(1) - 1
		p.resultsSet(idx, core.Result[T]{Err: err, Occupied: true})
		return -1, err
	}

	// 二次检查：防止 poolPrecheck 与 wg.Add 之间 Close() 被调用导致泄漏。
	// 必须在 CAS 背压检查之前执行，否则若 CAS 已递增 pending 后再检测到 Close，
	// 需要在返回路径中回退 pending，增加复杂度且降低性能。
	if p.closed.Load() {
		p.addInFlight.Add(-1)
		idx := p.submitIdx.Add(1) - 1
		p.resultsSet(idx, core.Result[T]{Err: core.ErrPoolClosed, Occupied: true})
		return -1, core.ErrPoolClosed
	}

	// 原子化背压检查：对 Drop/Error 策略使用 CAS 保证"检查上限→递增计数"的原子性，
	// 消除传统"先读后写"带来的 TOCTOU 竞态窗口。
	// Block 策略或未设置限制时，TOCTOU 无害（任务本身就会排队等待），保持简单递增。
	var pendingAcquired bool
	if mp := p.maxPending; mp > 0 {
		switch p.overflowStrat {
		case core.OverflowDrop:
			if !p.tryAcquirePendingSlot(mp) {
				p.addInFlight.Add(-1)
				idx := p.submitIdx.Add(1) - 1
				p.resultsSet(idx, core.Result[T]{Err: core.ErrQueueOverflow, Occupied: true})
				atomic.AddInt64(&p.errCnt, 1)
				return -1, nil
			}
			pendingAcquired = true
		case core.OverflowError:
			if !p.tryAcquirePendingSlot(mp) {
				p.addInFlight.Add(-1)
				idx := p.submitIdx.Add(1) - 1
				p.resultsSet(idx, core.Result[T]{Err: core.ErrQueueOverflow, Occupied: true})
				atomic.AddInt64(&p.errCnt, 1)
				return -1, core.ErrQueueOverflow
			}
			pendingAcquired = true
		}
	}

	taskCtx, taskCancel := context.WithCancel(ctx)

	idx := p.resultsAppend(taskCancel)

	if !pendingAcquired {
		p.pending.Add(1)
	}
	p.wg.Add(1)
	// wg.Add(1) 已完成，退出临界区
	p.addInFlight.Add(-1)

	maxR := int(p.maxResults)
	record := func(r core.Result[T], _ int) {
		p.resultsSet(idx, r)
		p.recordToStream(r)
		if p.ringBufFlag.Load() && p.ringBuf != nil {
			// maxR<=0 时全量镜像写入；maxR>0 时仅写入溢出部分，避免与分片存储重复
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
	p.addInFlight.Add(1)
	if err := p.poolPrecheck(ctx, "SubmitAt"); err != nil {
		p.addInFlight.Add(-1)
		if int64(index) >= p.submitIdx.Load() {
			p.submitIdx.Store(int64(index) + 1)
		}
		p.resultsSet(int64(index), core.Result[T]{Err: err, Occupied: true})
		return err
	}
	taskCtx, taskCancel := context.WithCancel(ctx)

	// 确保分片存储能容纳此索引
	for int64(index) >= p.submitIdx.Load() {
		p.resultsAppend(taskCancel)
	}

	p.pending.Add(1)
	p.wg.Add(1)
	p.addInFlight.Add(-1)

	maxR := int(p.maxResults)
	record := func(r core.Result[T], idx int) {
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

	return p.enqueueTask(ctx, taskCtx, taskCancel, task, record, index)
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
	if p.submitGuard.Load() || p.waited.Load() {
		p.addInFlight.Add(-1)
		return core.ErrPoolWaiting
	}
	taskCtx, taskCancel := context.WithCancel(ctx)

	// 二次检查：防止检查通过后 Close() 被调用导致 wg 泄漏
	if p.closed.Load() {
		p.addInFlight.Add(-1)
		taskCancel()
		return core.ErrPoolClosed
	}

	idx := p.resultsAppend(taskCancel)

	p.pending.Add(1)
	p.wg.Add(1)
	p.addInFlight.Add(-1)

	maxR := int(p.maxResults)
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

	sent, err := p.trySend(task)
	if !sent {
		err = core.ErrSubmitTimeout
		p.discardTask(record, int(idx), taskCancel, err)
		return err
	}
	return nil
}

// ──────────────────────────── Resize ────────────────────────────

// Resize 动态调整 worker 数量，返回变更数（正数=新增，负数=移除）。"
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
	// 更新 AutoScale 边界，保持手动 resize 后的范围合理
	if p.IsAutoScaleEnabled() && p.autoScale != nil {
		if newSize < p.autoScale.MinWorkers {
			p.autoScale.MinWorkers = newSize
		}
		if newSize > p.autoScale.MaxWorkers {
			p.autoScale.MaxWorkers = newSize
		}
	}

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
func (p *Pool[T]) sendQuitSignal() {
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
	p.submitGuard.Store(true)
	defer p.submitGuard.Store(false)
	p.waitAddInFlight()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

func (p *Pool[T]) enqueueTask(ctx context.Context, taskCtx context.Context, taskCancel context.CancelFunc, task core.PoolTask[T], record core.PoolRecordFunc[T], idx int) error {
	if p.submitTimeout > 0 {
		timer := time.NewTimer(p.submitTimeout)
		defer timer.Stop()
		sent, err := p.blockSend(task, taskCtx, timer)
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

	// 慢路径：channel 满，分配 Timer 阻塞等待，带指数退避防止 CPU 空转
	timer := time.NewTimer(core.SlotAcquireWarnTimeout)
	defer timer.Stop()

	backoff := 50 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for {
		if sent, err := p.blockSend(task, taskCtx, timer); sent {
			return nil
		} else if errors.Is(err, core.ErrPoolClosed) {
			p.discardTask(record, idx, taskCancel, err)
			return err
		} else if errors.Is(err, core.ErrSubmitTimeout) {
			core.LogCtxWarn(ctx, "async: Pool.Submit blocking on task queue, consider setting WithSubmitTimeout",
				core.Dur("elapsed", core.SlotAcquireWarnTimeout))
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			select {
			case <-time.After(backoff/2 + jitter):
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
			timer.Reset(core.SlotAcquireWarnTimeout)
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
func (p *Pool[T]) blockSend(task core.PoolTask[T], taskCtx context.Context, timer *time.Timer) (sent bool, err error) {
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
	case <-timer.C:
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
	atomic.AddInt64(&p.errCnt, 1)
	taskCancel()
	p.pending.Add(-1)
	p.wg.Done()
}

// tryAcquirePendingSlot 原子地检查并获取一个等待槽位。
// 使用 CAS 循环保证"检查上限→递增计数"的原子性，消除传统"先读后写"带来的 TOCTOU 竞态窗口。
// 返回 true 表示获取成功（pending 已递增），false 表示已达上限。
func (p *Pool[T]) tryAcquirePendingSlot(limit int32) bool {
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

// Wait 阻塞等待所有已提交的任务完成，返回按提交顺序排列的结果。
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
		core.LogCtxWarn(p.ctx, "async: Pool.Wait called multiple times, returning nil")
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

	// 环形缓冲作为溢出区时（maxResults>0），将溢出结果也纳入 Wait 返回，
	// 保证调用方通过 Wait() 拿到完整结果集。
	if p.ringBufFlag.Load() && p.ringBuf != nil && p.maxResults > 0 {
		overflow := p.ringBuf.Flush()
		results = append(results, overflow...)
	}

	if cancel := p.cancel; cancel != nil {
		cancel()
	}
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

// Close 关闭协程池，等待所有 worker 退出后返回。
// 关闭后不能再提交新任务。
//
// 示例：
//
//	p.Close()
func (p *Pool[T]) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	close(p.done)
	p.workerWg.Wait()

	p.drainOrphanTasks()

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
	if !p.closed.CompareAndSwap(false, true) {
		return false, nil
	}
	close(p.done)

	done := make(chan struct{})
	go func() {
		p.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.wg.Wait()
		p.drainOrphanTasks()
		return true, nil
	case <-time.After(timeout):
		return false, done
	}
}

// CloseByIdle 等待池空闲后关闭，最多等待 timeout。
// 空闲指 Active()==0 且 Busy()==0。
//
// 参数：
//   - timeout：最长等待时间
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
	return atomic.LoadInt64(&p.errCnt)
}

// SuccessCount 返回执行成功的任务数。
func (p *Pool[T]) SuccessCount() int64 {
	return p.TotalCount() - p.FailCount()
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
	return p.submitIdx.Load()
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
	Size        int           // worker 数量
	Active      int           // 当前活跃任务数
	Busy        int           // 当前忙碌任务数（正在执行 fn）
	Pending     int           // 等待中的任务数
	FailFast    bool          // 是否启用 FailFast
	Timeout     time.Duration // 全局任务超时时间
	TotalTask   int64         // 历史提交任务总数
	SuccessTask int64         // 历史成功任务数
	FailTask    int64         // 历史失败任务数
}

func (s PoolStats) String() string {
	return fmt.Sprintf("PoolStats{size=%d, active=%d, busy=%d, pending=%d, failFast=%v, timeout=%v, total=%d, success=%d, fail=%d}",
		s.Size, s.Active, s.Busy, s.Pending, s.FailFast, s.Timeout, s.TotalTask, s.SuccessTask, s.FailTask)
}

// Stats 返回池的完整统计信息。
//
// 示例：
//
//	stats := p.Stats()
//	fmt.Printf("并发度: %d, 成功: %d, 失败: %d\n",
//	    stats.Size, stats.SuccessTask, stats.FailTask)
func (p *Pool[T]) Stats() PoolStats {
	return PoolStats{
		Size:        p.Size(),
		Active:      p.Active(),
		Busy:        p.Busy(),
		Pending:     p.Pending(),
		FailFast:    p.failFast.Load(),
		Timeout:     p.timeout,
		TotalTask:   p.TotalCount(),
		SuccessTask: p.SuccessCount(),
		FailTask:    p.FailCount(),
	}
}

// Errors 返回所有任务的非 nil 错误（建议在 Wait 后调用）。
//
// 示例：
//
//	p.Wait()
//	for _, err := range p.Errors() {
//	    log.Printf("任务错误: %v", err)
//	}
func (p *Pool[T]) Errors() []error {
	if !p.waited.Load() {
		core.LogCtxWarn(p.ctx, "async: Pool.Errors called before Wait, results may be incomplete", core.Str("type", "Pool"))
	}
	results := p.resultsCollect()
	errs := make([]error, 0, p.FailCount())
	for _, r := range results {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errs
}

// FirstError 返回首个任务错误（建议在 Wait 后调用）。
//
// 示例：
//
//	p.Wait()
//	if firstErr := p.FirstError(); firstErr != nil {
//	    log.Printf("首个失败: %v", firstErr)
//	}
func (p *Pool[T]) FirstError() error {
	if !p.waited.Load() {
		core.LogCtxWarn(p.ctx, "async: Pool.FirstError called before Wait, results may be incomplete", core.Str("type", "Pool"))
	}
	results := p.resultsCollect()
	for _, r := range results {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// JoinErrors 合并所有错误为一个错误（使用 errors.Join）。
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
	if !p.waitInvoked.CompareAndSwap(false, true) {
		core.LogCtxWarn(p.ctx, "async: Pool.WaitTimeout called after a Wait variant already completed")
		return nil, false
	}
	p.submitGuard.Store(true)
	p.waitAddInFlight()
	results, ok := core.WaitTimeoutImpl(d, &p.wg, p.cancel, &p.submitGuard, &p.waited, func() {
		p.cancelAllShards()
	}, func() []core.Result[T] {
		return p.resultsCollect()
	}, p.ctx)
	return results, ok
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
		core.LogCtxWarn(p.ctx, "async: Pool.WaitContext called after a Wait variant already completed")
		return nil, false
	}
	p.submitGuard.Store(true)
	p.waitAddInFlight()
	return core.WaitContextImpl(ctx, &p.wg, p.cancel, &p.submitGuard, &p.waited, func() {
		p.cancelAllShards()
	}, func() []core.Result[T] {
		return p.resultsCollect()
	}, p.ctx, "Pool")
}

func checkErr(err error, name string) {
	if err != nil {
		var buf [4096]byte
		n := runtime.Stack(buf[:], false)
		core.LogCtxError(context.Background(), "async pool fatal",
			core.Err(err),
			core.Str("stack", string(buf[:n])),
			core.Str("name", name))
	}
}

// Submit 创建默认 Pool 并提交单个任务，返回池实例、索引和错误。
// 适用于快速的单次提交场景，使用完后需要调用 p.Close()。
//
// 参数：
//   - ctx：上下文
//   - fn：任务执行函数
//
// 示例：
//
//	p, idx, err := pool.Submit(ctx, func(ctx context.Context) (string, error) {
//	    return processData(ctx)
//	})
//	defer p.Close()
//	results := p.Wait()
func Submit[T any](ctx context.Context, fn func(context.Context) (T, error)) (*Pool[T], int, error) {
	p := DefaultPool[T]()
	p.ctx = ctx
	idx, err := p.submitIndexed(ctx, fn)
	return p, idx, err
}

// SubmitN 创建默认 Pool 并提交 N 个相同任务，返回提交结果列表。
//
// 参数：
//   - ctx：上下文
//   - fn：任务执行函数
//   - n：提交次数
//
// 示例：
//
//	p, results, err := pool.SubmitN(ctx, fn, 100)
//	defer p.Close()
//	fmt.Printf("提交失败数: %d\n", countFailed(results))
func SubmitN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult, error) {
	p := DefaultPool[T]()
	p.ctx = ctx
	results := make([]SubmitResult, n)
	for i := 0; i < n; i++ {
		idx, err := p.submitIndexed(ctx, fn)
		results[i] = SubmitResult{Index: idx, Err: err}
	}
	return p, results, nil
}

// SubmitSafeN 创建默认 Pool 并提交 N 个相同任务，忽略提交失败（仅记录日志）。
//
// 参数：
//   - ctx：上下文
//   - fn：任务执行函数
//   - n：提交次数
//
// 示例：
//
//	p, results := pool.SubmitSafeN(ctx, fn, 100)
//	defer p.Close()
func SubmitSafeN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult) {
	pool, results, err := SubmitN(ctx, fn, n)
	for _, r := range results {
		checkErr(r.Err, "SubmitSafeN")
	}
	checkErr(err, "SubmitSafeN")
	return pool, results
}

// SubmitBatch 批量提交切片元素，每个元素调用一次 fn。
// S 约束为 ~[]E，支持 []E 及其派生类型。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，接收 ctx 和元素，返回结果和错误
//
// 示例：
//
//	items := []string{"a", "b", "c"}
//	p, results, err := pool.SubmitBatch(ctx, items, func(ctx context.Context, s string) (int, error) {
//	    return len(s), nil
//	})
//	defer p.Close()
func SubmitBatch[T any, S ~[]E, E any](ctx context.Context, items S, fn func(context.Context, E) (T, error)) (*Pool[T], []SubmitResult, error) {
	p := DefaultPool[T]()
	p.ctx = ctx
	n := len(items)
	results := make([]SubmitResult, n)
	for i, item := range items {
		item := item
		idx, err := p.submitIndexed(ctx, func(ctx context.Context) (T, error) {
			return fn(ctx, item)
		})
		results[i] = SubmitResult{Index: idx, Err: err}
	}
	return p, results, nil
}

// MapPool 使用协程池并发映射，返回池实例和结果列表。
// concurrency<=0 时使用 IO 并发度。MapPool 内部自动 Wait+Close 池。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，接收 ctx 和元素，返回结果和错误
//   - concurrency：并发度
//
// 示例：
//
//	urls := []string{"http://a.com", "http://b.com", "http://c.com"}
//	p, results, err := pool.MapPool(ctx, urls, func(ctx context.Context, url string) (string, error) {
//	    return httpGet(ctx, url)
//	}, 4)
//	fmt.Printf("成功: %d, 失败: %d\n", p.SuccessCount(), p.FailCount())
func MapPool[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) (*Pool[R], []core.Result[R], error) {
	if concurrency <= 0 {
		concurrency = core.IO()
	}
	if concurrency > len(items) {
		concurrency = len(items)
	}
	p := NewPool[R](concurrency)
	p.ctx = ctx
	for _, item := range items {
		item := item
		p.Submit(ctx, func(ctx context.Context) (R, error) {
			return fn(ctx, item)
		})
	}
	results := p.Wait()
	p.Close()
	return p, results, nil
}

// ForEachPool 使用协程池并发遍历，只返回错误。
// concurrency<=0 时使用 IO 并发度。ForEachPool 内部自动 Wait+Close 池。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，接收 ctx 和元素，只返回 error
//   - concurrency：并发度
//
// 示例：
//
//	urls := []string{"http://a.com", "http://b.com"}
//	p, err := pool.ForEachPool(ctx, urls, func(ctx context.Context, url string) error {
//	    return downloadFile(ctx, url)
//	}, 4)
//	if err != nil {
//	    log.Printf("下载失败: %v", err)
//	}
func ForEachPool[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (*NoResultPool, error) {
	if concurrency <= 0 {
		concurrency = core.IO()
	}
	p := NewPool[struct{}](concurrency)
	p.ctx = ctx
	for _, item := range items {
		item := item
		p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
			return struct{}{}, fn(ctx, item)
		})
	}
	p.Wait()
	p.Close()
	return p, p.FirstError()
}

// Values 提取所有成功的值（建议在 Wait 后调用）。
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
		if r.Err == nil {
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
	savedTimeout := p.timeout
	savedSubmitTimeout := p.submitTimeout
	savedMaxPending := p.maxPending
	savedOverflowStrat := p.overflowStrat
	savedMaxResults := p.maxResults
	savedRingBuf := p.ringBuf
	savedStreamCh := p.streamCh
	savedResultCb := p.resultCb
	savedFailFast := p.failFast.Load()
	p.ctx = context.Background()
	p.errCnt = 0
	p.failFast.Store(savedFailFast)
	if savedFailFast {
		ctx, cancel := context.WithCancel(context.Background())
		p.ctx = ctx
		p.cancel = cancel
	} else {
		p.cancel = nil
	}
	p.quitting.Store(0)
	p.streamDropped.Store(0)
	if savedStreamCh != nil {
		p.streamOnce = sync.Once{}
	}
	if p.Size() != int(p.size.Load()) {
		p.size.Store(int32(p.Size()))
	}
	newSize := int(p.size.Load())

	close(oldTaskCh)

	// 等待旧 worker 全部退出后再替换 taskCh，避免新旧 worker 竞争
	p.workerWg.Wait()

	// 重建 done channel：Close() 后 done 已关闭，Reset 必须重建，否则新 worker 会立即退出。
	p.done = make(chan struct{})

	newTaskCh := make(chan core.PoolTask[T], newSize*2)
	p.taskCh = newTaskCh

	// 先替换 taskCh 再开放 Submit 入口，防止 Submit 向已关闭的 oldTaskCh 发送导致任务丢失
	p.submitGuard.Store(false)
	p.waited.Store(false)
	p.waitInvoked.Store(false)
	p.closed.Store(false)
	p.quitMu.Lock()
	p.quitCh = make(chan struct{})
	p.quitMu.Unlock()
	p.timeout = savedTimeout
	p.submitTimeout = savedSubmitTimeout
	p.maxPending = savedMaxPending
	p.overflowStrat = savedOverflowStrat
	p.maxResults = savedMaxResults
	if savedStreamCh != nil {
		p.streamCh = make(chan core.Result[T], cap(savedStreamCh))
	}
	p.resultCb = savedResultCb
	if savedRingBuf != nil {
		p.ringBuf = core.NewRingBuffer[core.Result[T]](savedRingBuf.Cap(), savedOverflowStrat)
		p.ringBufFlag.Store(true)
	} else {
		p.ringBufFlag.Store(false)
	}

	// 重启自动扩缩容 goroutine：Close() 关闭 done 后 autoScale goroutine 已退出，
	// Reset 后若 autoScale 仍启用，需重建 stopCh 并重新启动后台检测循环。
	if p.autoScaleEnabled.Load() && p.autoScale != nil {
		stopCh := make(chan struct{})
		p.autoScaleStop = stopCh
		go func() {
			p.autoScaleLoop(p.autoScale, stopCh)
		}()
	}

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
	if config == nil {
		config = core.DefaultAutoScaleConfig()
	}
	config.Normalize()

	p.autoScale = config
	if p.autoScaleEnabled.CompareAndSwap(false, true) {
		stopCh := make(chan struct{})
		p.autoScaleStop = stopCh
		go func() {
			p.autoScaleLoop(config, stopCh)
		}()
	}
}

// DisableAutoScale 停止自动扩缩容，将 worker 数量恢复到初始值。
//
// 停止后仍可通过 Resize 手动调整。如果自动扩缩容未启用，调用无效果。
func (p *Pool[T]) DisableAutoScale() {
	if p.autoScaleEnabled.CompareAndSwap(true, false) {
		close(p.autoScaleStop)

		if p.autoScale != nil {
			minWorkers := p.autoScale.MinWorkers
			if minWorkers > 0 && p.Size() != minWorkers {
				p.Resize(minWorkers)
			}
		}
	}
}

// IsAutoScaleEnabled 返回是否已启用自动扩缩容。
func (p *Pool[T]) IsAutoScaleEnabled() bool {
	return p.autoScaleEnabled.Load()
}

// autoScaleLoop 自动扩缩容后台检测循环。
func (p *Pool[T]) autoScaleLoop(config *core.AutoScaleConfig, stopCh chan struct{}) {
	ticker := time.NewTicker(config.CheckInterval)
	defer ticker.Stop()

	var scaleUpCount, scaleDownCount int

	for {
		select {
		case <-p.done:
			return
		case <-stopCh:
			return
		case <-ticker.C:
			p.performAutoScaleCheck(config, &scaleUpCount, &scaleDownCount)
		}
	}
}

// performAutoScaleCheck 执行一次扩缩容检测。
// ──────────────────────────── MultiPool 分片协程池 ────────────────────────────

// MultiPool 将任务分发到 N 个 Pool 实例，实现水平扩展，支持极限高并发（百万~千万 QPS）。
// 每个分片池独立运行，互不影响，通过 round-robin 分发任务。
//
// 通过 Pool.Shard() 创建，无需直接构造：
//
//	mp := pool.NewPool[int](100).Shard(8) // 8 个分片，每个 100 worker
//	defer mp.Close()
//	mp.Submit(ctx, fn)
//	results := mp.Wait()
type MultiPool[T any] struct {
	pools   []*Pool[T]
	nextIdx atomic.Uint64
}

// ShardCount 返回分片数。
func (mp *MultiPool[T]) ShardCount() int {
	return len(mp.pools)
}

// GetShard 获取指定分片的 Pool 实例，用于直接调用 Pool 的方法。
// idx 越界返回 nil。
func (mp *MultiPool[T]) GetShard(idx int) *Pool[T] {
	if idx < 0 || idx >= len(mp.pools) {
		return nil
	}
	return mp.pools[idx]
}

// Submit 将任务以 round-robin 方式分发到某个分片提交。
func (mp *MultiPool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := int(mp.nextIdx.Add(1)-1) % len(mp.pools)
	return mp.pools[idx].Submit(ctx, fn)
}

// TrySubmit 非阻塞分发提交，不等待 worker 空闲。
func (mp *MultiPool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := int(mp.nextIdx.Add(1)-1) % len(mp.pools)
	return mp.pools[idx].TrySubmit(ctx, fn)
}

// SubmitKeyed 按 key 哈希分发到固定分片，保证同一 key 的任务落到同一分片。
func (mp *MultiPool[T]) SubmitKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := int(key % uint64(len(mp.pools)))
	return mp.pools[idx].Submit(ctx, fn)
}

// TrySubmitKeyed 按 key 哈希非阻塞分发。
func (mp *MultiPool[T]) TrySubmitKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := int(key % uint64(len(mp.pools)))
	return mp.pools[idx].TrySubmit(ctx, fn)
}

// SubmitBatch 批量提交，每个 item 分发到不同分片。
// 返回每个元素的提交结果，包含分片索引和错误。
func (mp *MultiPool[T]) SubmitBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) []SubmitResult {
	results := make([]SubmitResult, len(items))
	for i, item := range items {
		idx := int(mp.nextIdx.Add(1)-1) % len(mp.pools)
		err := mp.pools[idx].Submit(ctx, func(ctx context.Context) (T, error) {
			return fn(ctx, item)
		})
		results[i] = SubmitResult{Index: i, Err: err}
	}
	return results
}

// Wait 等待所有分片完成，合并结果（保持各分片内部顺序，分片间不保证全局顺序）。
func (mp *MultiPool[T]) Wait() []core.Result[T] {
	var total int
	for _, p := range mp.pools {
		total += int(p.totalResultsCount())
	}
	all := make([]core.Result[T], 0, total)
	for _, p := range mp.pools {
		all = append(all, p.Wait()...)
	}
	return all
}

// WaitAndClose 等待所有分片完成后关闭。
func (mp *MultiPool[T]) WaitAndClose() []core.Result[T] {
	var total int
	for _, p := range mp.pools {
		total += int(p.totalResultsCount())
	}
	all := make([]core.Result[T], 0, total)
	for _, p := range mp.pools {
		all = append(all, p.WaitAndClose()...)
	}
	return all
}

// Close 关闭所有分片。
func (mp *MultiPool[T]) Close() {
	for _, p := range mp.pools {
		p.Close()
	}
}

// ── 代理配置方法 ──

// WithTimeout 为所有分片设置任务超时。
func (mp *MultiPool[T]) WithTimeout(d time.Duration) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithTimeout(d)
	}
	return mp
}

// WithSubmitTimeout 为所有分片设置提交超时。
func (mp *MultiPool[T]) WithSubmitTimeout(d time.Duration) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithSubmitTimeout(d)
	}
	return mp
}

// WithStreaming 为所有分片启用流式结果消费。
// 注意：目前各分片的流式 channel 是独立的，如需统一消费请使用 resultCb。
func (mp *MultiPool[T]) WithStreaming(bufSize int) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithStreaming(bufSize)
	}
	return mp
}

// WithResultCallback 为所有分片设置结果回调。
func (mp *MultiPool[T]) WithResultCallback(fn func(core.Result[T])) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithResultCallback(fn)
	}
	return mp
}

// StreamResults 返回合并所有分片流式结果的只读 channel。
// 每个分片的流式结果被 fan-in 到此 channel，所有分片 channel 关闭后自动关闭此 channel。
func (mp *MultiPool[T]) StreamResults() <-chan core.Result[T] {
	merged := make(chan core.Result[T], len(mp.pools)*256)
	var wg sync.WaitGroup
	for _, p := range mp.pools {
		ch := p.StreamResults()
		if ch != nil {
			wg.Add(1)
			go func(c <-chan core.Result[T]) {
				defer wg.Done()
				for r := range c {
					merged <- r
				}
			}(ch)
		}
	}
	go func() {
		wg.Wait()
		close(merged)
	}()
	return merged
}

// WithRingBuffer 为所有分片启用环形缓冲区。
func (mp *MultiPool[T]) WithRingBuffer(capacity int, overflow core.OverflowStrategy) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithRingBuffer(capacity, overflow)
	}
	return mp
}

// WithMaxPending 为所有分片设置最大等待任务数（背压控制）。
func (mp *MultiPool[T]) WithMaxPending(n int) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithMaxPending(n)
	}
	return mp
}

// WithOverflow 为所有分片设置溢出策略。
func (mp *MultiPool[T]) WithOverflow(strategy core.OverflowStrategy) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithOverflow(strategy)
	}
	return mp
}

// WithMaxResults 为所有分片设置最大结果数。
func (mp *MultiPool[T]) WithMaxResults(n int) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithMaxResults(n)
	}
	return mp
}

// ── 统计聚合 ──

// TotalActive 汇总所有分片当前活跃任务数。
func (mp *MultiPool[T]) TotalActive() int {
	var total int
	for _, p := range mp.pools {
		total += p.Active()
	}
	return total
}

// TotalBusy 汇总所有分片当前忙碌任务数。
func (mp *MultiPool[T]) TotalBusy() int {
	var total int
	for _, p := range mp.pools {
		total += p.Busy()
	}
	return total
}

// TotalPending 汇总所有分片等待中任务数。
func (mp *MultiPool[T]) TotalPending() int {
	var total int
	for _, p := range mp.pools {
		total += p.Pending()
	}
	return total
}

// TotalWorkerCount 汇总所有分片的 worker 总数。
func (mp *MultiPool[T]) TotalWorkerCount() int {
	var total int
	for _, p := range mp.pools {
		total += p.Size()
	}
	return total
}

// TotalFailCount 汇总所有分片的失败数。
func (mp *MultiPool[T]) TotalFailCount() int64 {
	var total int64
	for _, p := range mp.pools {
		total += p.FailCount()
	}
	return total
}

// TotalSuccessCount 汇总所有分片的成功数。
func (mp *MultiPool[T]) TotalSuccessCount() int64 {
	var total int64
	for _, p := range mp.pools {
		total += p.SuccessCount()
	}
	return total
}

// TotalCount 汇总所有分片的任务总数。
func (mp *MultiPool[T]) TotalCount() int64 {
	var total int64
	for _, p := range mp.pools {
		total += p.TotalCount()
	}
	return total
}

// Flush 从所有分片环形缓冲区排空结果（需提前启用 WithRingBuffer）。
func (mp *MultiPool[T]) Flush(maxPerShard int) []core.Result[T] {
	var all []core.Result[T]
	for _, p := range mp.pools {
		all = append(all, p.Flush(maxPerShard)...)
	}
	return all
}

// ──────────────────────────── Pool.Shard ────────────────────────────

// Shard 将当前 Pool 水平扩展为 N 个分片，支持极限高并发（百万~千万 QPS）。
// 第一个分片复用当前 Pool，剩余分片创建相同配置的新 Pool。
//
// 适用场景：单 Pool 的 channel 缓冲区成为瓶颈时，通过分片降低单 channel 竞争。
// 例如：一个 Pool(100 worker) 吞吐约 37万/s，Shard(8) 后理论可达 ~300万/s。
//
// shards <= 1 时返回只有当前 Pool 的单分片 MultiPool。
//
// 使用示例：
//
//	// 基本用法：8 分片 × 每个 100 worker = 800 worker
//	mp := pool.NewPool[int](100).Shard(8)
//	defer mp.Close()
//	for i := 0; i < 10_000_000; i++ {
//	    mp.Submit(ctx, func(ctx context.Context) (int, error) { return i * 2, nil })
//	}
//	results := mp.Wait()
//
//	// 配合自动扩缩容：每个分片独立扩缩
//	mp := pool.NewPool[int](4).
//	    WithMaxPending(10000).
//	    Shard(4)
//	for _, p := range mp.GetShard(i) { ... } // 手动为每个分片开启 AutoScale
//
//	// 配合背压控制
//	mp := pool.NewPool[int](50).
//	    WithMaxPending(5000).
//	    WithOverflow(core.OverflowDrop).
//	    Shard(16)
func (p *Pool[T]) Shard(shards int) *MultiPool[T] {
	if shards <= 1 {
		return &MultiPool[T]{pools: []*Pool[T]{p}}
	}

	templateSize := p.Size()

	pools := make([]*Pool[T], shards)
	pools[0] = p // 第一个分片复用当前 Pool

	for i := 1; i < shards; i++ {
		clone := NewPool[T](templateSize)
		// 复制所有配置
		clone.timeout = p.timeout
		clone.submitTimeout = p.submitTimeout
		clone.maxPending = p.maxPending
		clone.overflowStrat = p.overflowStrat
		clone.maxResults = p.maxResults
		clone.ctx = p.ctx

		// FailFast
		if p.failFast.Load() {
			clone.failFast.Store(true)
			clone.cancel = p.cancel
		}

		// RingBuffer
		if p.ringBufFlag.Load() && p.ringBuf != nil {
			clone.ringBuf = core.NewRingBuffer[core.Result[T]](p.ringBuf.Cap(), p.ringBuf.OverflowStrategy())
			clone.ringBufFlag.Store(true)
		}

		// Stream channel
		if p.streamCh != nil {
			clone.streamCh = make(chan core.Result[T], cap(p.streamCh))
		}

		// Result callback
		clone.resultCb = p.resultCb

		// AutoScale
		if p.IsAutoScaleEnabled() && p.autoScale != nil {
			cfgCopy := *p.autoScale
			clone.EnableAutoScale(&cfgCopy)
		}

		pools[i] = clone
	}

	return &MultiPool[T]{pools: pools}
}

// DefaultShard 使用默认分片数（runtime.GOMAXPROCS(0)，最少 2）对当前 Pool 进行水平分片。
// 等效于 p.Shard(max(2, runtime.GOMAXPROCS(0)))，无需手动指定分片数。
//
// 示例：
//
//	// 自动按 CPU 核心数分片
//	mp := async.NewPool[int](100).DefaultShard()
//	defer mp.Close()
func (p *Pool[T]) DefaultShard() *MultiPool[T] {
	shards := runtime.GOMAXPROCS(0)
	if shards < 2 {
		shards = 2
	}
	return p.Shard(shards)
}

func (p *Pool[T]) performAutoScaleCheck(config *core.AutoScaleConfig, scaleUpCount, scaleDownCount *int) {
	if p.closed.Load() || p.submitGuard.Load() {
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
				p.Resize(newSize)
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
				p.Resize(newSize)
			}
			*scaleDownCount = 0
		}
	} else {
		*scaleUpCount = 0
		*scaleDownCount = 0
	}
}
