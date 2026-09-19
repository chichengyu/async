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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/core"
)

// Pool 泛型协程池，复用固定数量的 goroutine 处理高频并发任务。
// 适合长期运行、反复提交任务的场景。
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
	workerWg      sync.WaitGroup        // 等待所有 worker 退出
	mu            sync.Mutex            // 保护 results/cancels/waited
	results       []core.Result[T]      // 任务结果切片（按提交顺序）
	cancels       []context.CancelFunc  // 所有任务取消函数（Wait 后批量调用）
	errCnt        int64                 // 失败任务计数（atomic 原子操作）
	cancel        context.CancelFunc    // 全局取消函数
	failFast      atomic.Bool           // 是否启用 FailFast 模式
	timeout       time.Duration         // 全局任务超时时间（0=无限制）
	submitTimeout time.Duration         // 任务提交超时时间
	closed        atomic.Bool           // 是否已关闭
	size          atomic.Int32          // worker 数量
	active        atomic.Int32          // 当前活跃任务数
	busy          atomic.Int32          // 当前忙碌任务数
	pending       atomic.Int32          // 等待中的任务数
	waiting       atomic.Bool           // 是否正在 Wait 等待中
	waited        bool                  // 是否已完成 Wait
	quitting      atomic.Int32          // 是否正在退出中
	ctx           context.Context       // 池级别的上下文
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

func (p *Pool[T]) worker() {
	defer p.workerWg.Done()
	for task := range p.taskCh {
		if task.Quit {
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
	}
}

func (p *Pool[T]) processTask(task core.PoolTask[T]) {
	taskCtx := task.Ctx
	taskCancel := task.Cancel
	timeout := task.Timeout
	failFast := task.FailFast
	index := task.Index
	record := task.Record

	defer func() {
		if r := recover(); r != nil {
			core.LogCtxError(taskCtx, "async pool panic recovered",
				core.Any("panic", r),
				core.Bytes("stack", core.NewPanicError(r).Stack))
			var zero T
			if index >= 0 {
				record(core.Result[T]{Value: zero, Err: core.NewPanicError(r), Occupied: true}, index)
			} else {
				record(core.Result[T]{Value: zero, Err: core.NewPanicError(r), Occupied: true}, -1)
			}
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
	if index >= 0 {
		record(r, index)
	} else {
		record(r, -1)
	}

	p.wg.Done()
	taskCancel()
}

func (p *Pool[T]) poolPrecheck(ctx context.Context, caller string) error {
	if p.closed.Load() {
		return core.ErrPoolClosed
	}
	if p.waiting.Load() {
		return core.ErrPoolWaiting
	}
	p.mu.Lock()
	waited := p.waited
	p.mu.Unlock()
	if waited {
		return core.ErrPoolWaited
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

func (p *Pool[T]) submitIndexed(ctx context.Context, fn func(context.Context) (T, error)) (index int, err error) {
	if err := p.poolPrecheck(ctx, "Submit"); err != nil {
		p.mu.Lock()
		p.results = append(p.results, core.Result[T]{Err: err, Occupied: true})
		p.mu.Unlock()
		return -1, err
	}
	taskCtx, taskCancel := context.WithCancel(ctx)

	p.mu.Lock()
	idx := len(p.results)
	p.results = append(p.results, core.Result[T]{})
	p.cancels = append(p.cancels, taskCancel)
	p.mu.Unlock()

	p.pending.Add(1)
	p.wg.Add(1)

	record := func(r core.Result[T], _ int) {
		p.mu.Lock()
		if idx < len(p.results) {
			p.results[idx] = r
		}
		p.mu.Unlock()
	}

	task := core.PoolTask[T]{
		Ctx:      taskCtx,
		Cancel:   taskCancel,
		Fn:       fn,
		Timeout:  p.timeout,
		FailFast: p.failFast.Load(),
		Index:    -1,
		Record:   record,
	}

	if err := p.enqueueTask(ctx, taskCtx, taskCancel, task, record, idx); err != nil {
		return idx, err
	}

	return idx, nil
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
	if err := p.poolPrecheck(ctx, "SubmitAt"); err != nil {
		p.mu.Lock()
		for len(p.results) <= index {
			p.results = append(p.results, core.Result[T]{})
		}
		if index < len(p.results) {
			p.results[index] = core.Result[T]{Err: err, Occupied: true}
		}
		p.mu.Unlock()
		return err
	}
	taskCtx, taskCancel := context.WithCancel(ctx)

	p.mu.Lock()
	for len(p.results) <= index {
		p.results = append(p.results, core.Result[T]{})
	}
	p.cancels = append(p.cancels, taskCancel)
	p.mu.Unlock()

	p.pending.Add(1)
	p.wg.Add(1)

	record := func(r core.Result[T], idx int) {
		p.mu.Lock()
		if idx >= 0 && idx < len(p.results) {
			p.results[idx] = r
		}
		p.mu.Unlock()
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
	if p.closed.Load() {
		return core.ErrPoolClosed
	}
	if p.waiting.Load() || p.waited {
		return core.ErrPoolWaiting
	}
	taskCtx, taskCancel := context.WithCancel(ctx)

	p.mu.Lock()
	idx := len(p.results)
	p.results = append(p.results, core.Result[T]{})
	p.cancels = append(p.cancels, taskCancel)
	p.mu.Unlock()

	p.pending.Add(1)
	p.wg.Add(1)

	record := func(r core.Result[T], _ int) {
		p.mu.Lock()
		if idx < len(p.results) {
			p.results[idx] = r
		}
		p.mu.Unlock()
	}

	task := core.PoolTask[T]{
		Ctx:      taskCtx,
		Cancel:   taskCancel,
		Fn:       fn,
		Timeout:  p.timeout,
		FailFast: p.failFast.Load(),
		Index:    -1,
		Record:   record,
	}

	sent, err := p.trySend(task)
	if !sent {
		err = core.ErrSubmitTimeout
		p.discardTask(record, idx, taskCancel, err)
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
	p.mu.Lock()
	defer p.mu.Unlock()
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
			for i := 0; i < quit; i++ {
				select {
				case p.taskCh <- core.PoolTask[T]{Quit: true}:
				default:
				}
			}
		}
		return quit
	}
	return 0
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

	timer := time.NewTimer(core.SlotAcquireWarnTimeout)
	defer timer.Stop()

	for {
		if sent, err := p.blockSend(task, taskCtx, timer); sent {
			return nil
		} else if errors.Is(err, core.ErrPoolClosed) {
			p.discardTask(record, idx, taskCancel, err)
			return err
		} else if errors.Is(err, core.ErrSubmitTimeout) {
			core.LogCtxWarn(ctx, "async: Pool.Submit blocking on task queue, consider setting WithSubmitTimeout",
				core.Dur("elapsed", core.SlotAcquireWarnTimeout))
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
	defer func() {
		if r := recover(); r != nil {
			sent = false
			err = core.ErrPoolClosed
		}
	}()
	select {
	case p.taskCh <- task:
		return true, nil
	case <-taskCtx.Done():
		return false, taskCtx.Err()
	case <-timer.C:
		return false, core.ErrSubmitTimeout
	}
}

// trySend 非阻塞安全发送到 taskCh，用于 TrySubmit。
// 通道关闭时通过 recover 安全返回 (false, ErrPoolClosed)。
func (p *Pool[T]) trySend(task core.PoolTask[T]) (sent bool, err error) {
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
	if idx >= 0 {
		record(core.Result[T]{Value: zero, Err: err, Occupied: true}, idx)
	} else {
		record(core.Result[T]{Value: zero, Err: err, Occupied: true}, -1)
	}
	atomic.AddInt64(&p.errCnt, 1)
	taskCancel()
	p.wg.Done()
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
	p.waiting.Store(true)
	p.wg.Wait()
	p.mu.Lock()
	p.waited = true
	p.waiting.Store(false)
	for _, c := range p.cancels {
		c()
	}
	p.cancels = nil
	results := make([]core.Result[T], len(p.results))
	copy(results, p.results)
	cancel := p.cancel
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
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
	p.mu.Lock()
	if !p.closed.CompareAndSwap(false, true) {
		p.mu.Unlock()
		return
	}
	close(p.taskCh)
	p.mu.Unlock()
	p.workerWg.Wait()
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
	p.mu.Lock()
	if !p.closed.CompareAndSwap(false, true) {
		p.mu.Unlock()
		return false, nil
	}
	close(p.taskCh)
	p.mu.Unlock()
	done := make(chan struct{})
	go func() {
		p.workerWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		p.wg.Wait()
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
	p.mu.Lock()
	n := len(p.results)
	p.mu.Unlock()
	return int64(n)
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.waited {
		core.LogCtxWarn(p.ctx, "async: Pool.Errors called before Wait, results may be incomplete", core.Str("type", "Pool"))
	}
	errs := make([]error, 0, p.FailCount())
	for _, r := range p.results {
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.waited {
		core.LogCtxWarn(p.ctx, "async: Pool.FirstError called before Wait, results may be incomplete", core.Str("type", "Pool"))
	}
	for _, r := range p.results {
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
	p.waiting.Store(true)
	results, ok := core.WaitTimeoutImpl(d, &p.wg, p.cancel, &p.mu, &p.waited, &p.waiting, func() {
		for _, c := range p.cancels {
			c()
		}
		p.cancels = nil
	}, func() []core.Result[T] {
		results := make([]core.Result[T], len(p.results))
		copy(results, p.results)
		return results
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
	return core.WaitContextImpl(ctx, &p.wg, p.cancel, &p.mu, &p.waited, &p.waiting, func() {
		for _, c := range p.cancels {
			c()
		}
		p.cancels = nil
	}, func() []core.Result[T] {
		results := make([]core.Result[T], len(p.results))
		copy(results, p.results)
		return results
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
	p.mu.Lock()
	defer p.mu.Unlock()
	vals := make([]T, 0, len(p.results))
	for _, r := range p.results {
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
	p.mu.Lock()
	if !p.waited {
		p.mu.Unlock()
		return p, fmt.Errorf("async: Pool.Reset called before Wait")
	}
	oldTaskCh := p.taskCh
	p.results = nil
	p.cancels = nil
	p.waited = false
	savedTimeout := p.timeout
	savedSubmitTimeout := p.submitTimeout
	p.ctx = context.Background()
	p.errCnt = 0
	p.failFast.Store(false)
	p.cancel = nil
	p.closed.Store(false)
	p.waiting.Store(false)
	p.quitting.Store(0)
	if p.Size() != int(p.size.Load()) {
		p.size.Store(int32(p.Size()))
	}
	newSize := int(p.size.Load())
	p.mu.Unlock()

	close(oldTaskCh)

	newTaskCh := make(chan core.PoolTask[T], newSize*2)
	p.mu.Lock()
	p.taskCh = newTaskCh
	p.timeout = savedTimeout
	p.submitTimeout = savedSubmitTimeout
	p.mu.Unlock()

	p.workerWg.Add(newSize)
	for i := 0; i < newSize; i++ {
		go p.worker()
	}
	return p, nil
}
