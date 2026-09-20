// Package group 提供泛型并发任务组 Group[T] 和无返回值任务组 NoResult。
// Group 适合一次性批量任务，每次 Go 新建 goroutine，任务完成后销毁。
package group

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/core"
)

// ──────────────────────────── Group ────────────────────────────
//
// With* 链式配置命名约定：
//   - FF（FailFast）：任一任务失败立即取消所有其他任务
//   - Ctx（Context）：通过 context.WithCancel 管理生命周期
//   - Timeout：单个任务执行超时
//   - SubmitTO（SubmitTimeout）：获取并发槽位的等待超时
//   - TraceID：自动注入 TraceID 到 context
//
// 组合示例：
//
//	g, ctx := group.NewGroup[string](10).
//	    WithFFTimeoutTraceID(ctx, 5*time.Second)

type Group[T any] struct {
	limit         chan struct{}        // 并发控制信号量
	concurrency   int                  // 最大并发数
	wg            sync.WaitGroup       // 等待所有任务完成
	addMu         sync.Mutex           // 保护 wg.Add 和 Wait 的竞态，避免 data race
	mu            sync.Mutex           // 保护 results/cancels/waited
	results       []core.Result[T]     // 任务结果切片（按提交顺序）
	cancels       []context.CancelFunc // 所有任务的取消函数（Wait 后批量调用）
	errCnt        int64                // 失败任务计数（atomic 原子操作）
	active        atomic.Int32         // 当前活跃任务数
	busy          atomic.Int32         // 当前忙碌任务数
	waiting       atomic.Bool          // 是否正在 Wait 等待中
	cancel        context.CancelFunc   // 全局取消函数
	timeout       time.Duration        // 全局任务超时时间（0=无限制）
	submitTimeout time.Duration        // 任务提交超时时间
	failFast      atomic.Bool          // 是否启用 FailFast 模式
	waited        bool                 // 是否已完成 Wait
	ctx           context.Context      // 组级别的上下文
}

// GroupRecordFunc 记录结果的函数类型，抽象 addResult（Go）和 setResultAt（GoAt）。
type GroupRecordFunc[T any] func(core.Result[T])

// NewGroup 创建一个新的任务组，限制最多 concurrency 个 goroutine 同时执行。
// concurrency <= 0 时自动设为 1。
//
// 参数：
//   - concurrency：最大并发数，<=0 时默认 1
//
// 使用示例：
//
//	g := group.NewGroup[string](5) // 最多 5 个任务并发
//	g.WithTimeout(10 * time.Second)
//	for _, url := range urls {
//	    g.Go(ctx, func(ctx context.Context) (string, error) {
//	        return httpGet(ctx, url)
//	    })
//	}
//	results := g.Wait()
func NewGroup[T any](concurrency int) *Group[T] {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &Group[T]{
		limit:       make(chan struct{}, concurrency),
		concurrency: concurrency,
		timeout:     core.GetDefaultTimeout(),
		ctx:         context.Background(),
	}
}

// DefaultGroup 使用 core.IO() 作为默认并发数创建 Group。
//
// 使用示例：
//
//	g := group.DefaultGroup[string]()
//	g.Go(ctx, myTask)
func DefaultGroup[T any]() *Group[T] {
	return NewGroup[T](core.IO())
}

// WithTraceID 确保 context 中包含 TraceID，便于链路追踪。
//
// 参数：
//   - ctx：原始上下文（没有 TraceID 时自动生成）
//
// 使用示例：
//
//	g, ctx := group.DefaultGroup[string]().WithTraceID(ctx)
func (g *Group[T]) WithTraceID(ctx context.Context) (*Group[T], context.Context) {
	ctx = core.EnsureTraceID(ctx)
	g.ctx = ctx
	return g, ctx
}

// WithContext 绑定 context，支持通过 cancel 统一取消所有任务。
// 返回的 ctx 是派生的可取消 context。
//
// 参数：
//   - ctx：原始上下文
//
// 使用示例：
//
//	g, ctx := group.DefaultGroup[string]().WithContext(ctx)
//	defer cancel() // 由调用方管理原始 ctx 的取消
func (g *Group[T]) WithContext(ctx context.Context) (*Group[T], context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	g.cancel = core.MergeCancel(g.cancel, cancel)
	g.ctx = ctx
	return g, ctx
}

// WithFailFast 启用快速失败模式：任一任务出错时取消所有其他任务。
//
// 参数：
//   - ctx：原始上下文
//
// 使用示例：
//
//	g, ctx := group.DefaultGroup[string]().WithFailFast(ctx)
func (g *Group[T]) WithFailFast(ctx context.Context) (*Group[T], context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	g.cancel = core.MergeCancel(g.cancel, cancel)
	g.failFast.Store(true)
	g.ctx = ctx
	return g, ctx
}

// WithFFCtx 等同 WithFailFast。提供更短的别名。
//
// 参数：
//   - ctx：原始上下文
func (g *Group[T]) WithFFCtx(ctx context.Context) (*Group[T], context.Context) {
	return g.WithFailFast(ctx)
}

// ─── 组合配置方法（FF=快速失败, Ctx=Context管理, Timeout=任务超时, SubmitTO=提交超时, TraceID=链路追踪）───
//
// 以下方法均为 WithXxx 的组合封装，命名格式为 With[FF][Ctx|Timeout|SubmitTO|TraceID]*：
//   - WithFFTraceID：快速失败 + TraceID
//   - WithFFSubmitTO：快速失败 + 提交超时
//   - WithFFSubmitTOTraceID：快速失败 + 提交超时 + TraceID
//   - WithFFTimeout：快速失败 + 任务超时
//   - WithFFTimeoutTraceID：快速失败 + 任务超时 + TraceID
//   - WithFFTimeoutSubmitTO：快速失败 + 任务超时 + 提交超时
//   - WithFFTimeoutSubmitTOTraceID：快速失败 + 任务超时 + 提交超时 + TraceID
//   - WithCtxTraceID：Context + TraceID
//   - WithCtxTimeout：Context + 任务超时
//   - WithCtxTimeoutTraceID：Context + 任务超时 + TraceID
//   - WithCtxSubmitTO：Context + 提交超时
//   - WithCtxSubmitTOTraceID：Context + 提交超时 + TraceID

// WithFFTraceID 启用 FailFast 并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
func (g *Group[T]) WithFFTraceID(ctx context.Context) (*Group[T], context.Context) {
	g, ctx = g.WithFailFast(ctx)
	return g.WithTraceID(ctx)
}

// WithFFSubmitTO 启用 FailFast 并设置提交超时。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
func (g *Group[T]) WithFFSubmitTO(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFailFast(ctx)
	g.WithSubmitTimeout(submitTimeout)
	return g, ctx
}

// WithFFSubmitTOTraceID 启用 FailFast、设置提交超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
func (g *Group[T]) WithFFSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFFSubmitTO(ctx, submitTimeout)
	return g.WithTraceID(ctx)
}

// WithFFTimeout 启用 FailFast 并设置任务超时。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
func (g *Group[T]) WithFFTimeout(ctx context.Context, timeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFailFast(ctx)
	g.WithTimeout(timeout)
	return g, ctx
}

// WithCtxTraceID 设置 Context 并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
func (g *Group[T]) WithCtxTraceID(ctx context.Context) (*Group[T], context.Context) {
	g, ctx = g.WithContext(ctx)
	return g.WithTraceID(ctx)
}

// WithFFTimeoutTraceID 启用 FailFast、设置超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
func (g *Group[T]) WithFFTimeoutTraceID(ctx context.Context, timeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFFTimeout(ctx, timeout)
	return g.WithTraceID(ctx)
}

// WithFFTimeoutSubmitTO 启用 FailFast、设置任务超时和提交超时。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
//   - submitTimeout：提交超时时间
func (g *Group[T]) WithFFTimeoutSubmitTO(ctx context.Context, timeout time.Duration, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFFTimeout(ctx, timeout)
	g.WithSubmitTimeout(submitTimeout)
	return g, ctx
}

// WithFFTimeoutSubmitTOTraceID 启用 FailFast、设置超时、提交超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
//   - submitTimeout：提交超时时间
func (g *Group[T]) WithFFTimeoutSubmitTOTraceID(ctx context.Context, timeout time.Duration, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFFTimeoutSubmitTO(ctx, timeout, submitTimeout)
	return g.WithTraceID(ctx)
}

// WithCtxTimeout 设置 Context 和任务超时。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
func (g *Group[T]) WithCtxTimeout(ctx context.Context, timeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithContext(ctx)
	g.WithTimeout(timeout)
	return g, ctx
}

// WithCtxTimeoutTraceID 设置 Context、任务超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - timeout：任务超时时间
func (g *Group[T]) WithCtxTimeoutTraceID(ctx context.Context, timeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithCtxTimeout(ctx, timeout)
	return g.WithTraceID(ctx)
}

// WithTimeout 设置每个任务的执行超时时间（默认 core.GetDefaultTimeout()）。
//
// 参数：
//   - d：任务超时时间，0 表示无限等待
//
// 使用示例：
//
//	g := group.DefaultGroup[string]().WithTimeout(3 * time.Second)
func (g *Group[T]) WithTimeout(d time.Duration) *Group[T] {
	g.timeout = d
	return g
}

// WithSubmitTimeout 设置获取并发槽位的等待超时。0 表示无限等待。
// 高负载时建议设置以避免长时间阻塞提交 goroutine。
//
// 参数：
//   - d：提交超时时间，0 表示无限等待
//
// 使用示例：
//
//	g := group.DefaultGroup[string]().WithSubmitTimeout(500 * time.Millisecond)
func (g *Group[T]) WithSubmitTimeout(d time.Duration) *Group[T] {
	g.submitTimeout = d
	return g
}

// WithCtxSubmitTO 设置 Context 和提交超时。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
func (g *Group[T]) WithCtxSubmitTO(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithContext(ctx)
	g.WithSubmitTimeout(submitTimeout)
	return g, ctx
}

// WithCtxSubmitTOTraceID 设置 Context、提交超时并注入 trace_id。
//
// 参数：
//   - ctx：原始上下文
//   - submitTimeout：提交超时时间
func (g *Group[T]) WithCtxSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithCtxSubmitTO(ctx, submitTimeout)
	return g.WithTraceID(ctx)
}

// ──────────────────────────── Group 内部方法 ────────────────────────────

func (g *Group[T]) groupPrecheck(ctx context.Context, record GroupRecordFunc[T], index int, caller string) (context.Context, context.CancelFunc, error) {
	g.mu.Lock()
	if g.waited {
		if index >= 0 {
			for len(g.results) <= index {
				g.results = append(g.results, core.Result[T]{})
			}
			g.results[index] = core.Result[T]{Err: core.ErrGroupWaited, Occupied: true}
		} else {
			g.results = append(g.results, core.Result[T]{Err: core.ErrGroupWaited, Occupied: true})
		}
		g.mu.Unlock()
		core.LogCtxError(ctx, fmt.Sprintf("async: Group.%s called after Group.Wait, task discarded", caller))
		atomic.AddInt64(&g.errCnt, 1)
		var cancel context.CancelFunc
		return ctx, cancel, core.ErrGroupWaited
	}
	if index >= 0 {
		for len(g.results) <= index {
			g.results = append(g.results, core.Result[T]{})
		}
	}
	g.mu.Unlock()

	select {
	case <-ctx.Done():
		if index >= 0 {
			record(core.Result[T]{Err: ctx.Err()})
		} else {
			g.addResult(core.Result[T]{Err: ctx.Err()})
		}
		atomic.AddInt64(&g.errCnt, 1)
		var cancel context.CancelFunc
		return ctx, cancel, ctx.Err()
	default:
	}

	g.addMu.Lock()
	if g.waiting.Load() {
		g.addMu.Unlock()
		core.LogCtxError(ctx, fmt.Sprintf("async: Group.%s called while Group.Wait is in progress, task discarded", caller))
		record(core.Result[T]{Err: core.ErrGroupWaiting})
		atomic.AddInt64(&g.errCnt, 1)
		var cancel context.CancelFunc
		return ctx, cancel, core.ErrGroupWaiting
	}
	g.wg.Add(1)
	g.addMu.Unlock()
	g.active.Add(1)
	taskCtx, taskCancel := context.WithCancel(ctx)

	return taskCtx, taskCancel, nil
}

func (g *Group[T]) groupAcquireSlot(ctx context.Context, taskCtx context.Context, taskCancel context.CancelFunc, record GroupRecordFunc[T]) error {
	if g.submitTimeout > 0 {
		timer := time.NewTimer(g.submitTimeout)
		defer timer.Stop()
		select {
		case g.limit <- struct{}{}:
			return nil
		case <-taskCtx.Done():
			g.discardTask(record, taskCancel, taskCtx.Err())
			return taskCtx.Err()
		case <-timer.C:
			err := core.ErrSubmitTimeout
			core.LogCtxWarn(ctx, "async: Group submit timeout, concurrency slot unavailable", core.Err(err))
			g.discardTask(record, taskCancel, err)
			return err
		}
	}

	timer := time.NewTimer(core.SlotAcquireWarnTimeout)
	defer timer.Stop()

	for {
		select {
		case g.limit <- struct{}{}:
			return nil
		case <-taskCtx.Done():
			g.discardTask(record, taskCancel, taskCtx.Err())
			return taskCtx.Err()
		case <-timer.C:
			core.LogCtxWarn(ctx, "async: Group.Go blocking on concurrency slot, consider setting WithSubmitTimeout",
				core.Dur("elapsed", core.SlotAcquireWarnTimeout))
			timer.Reset(core.SlotAcquireWarnTimeout)
		}
	}
}

func (g *Group[T]) groupRecordCancel(taskCancel context.CancelFunc) {
	g.mu.Lock()
	g.cancels = append(g.cancels, taskCancel)
	g.mu.Unlock()
}

func (g *Group[T]) discardTask(record GroupRecordFunc[T], taskCancel context.CancelFunc, err error) {
	record(core.Result[T]{Err: err})
	atomic.AddInt64(&g.errCnt, 1)
	taskCancel()
	g.active.Add(-1)
	g.wg.Done()
}

// Go 提交一个任务到 Group 中执行。fn 通过 context 支持取消和超时。
// 返回 error 表示提交失败（如槽位满超时、ctx 已取消等），不代表任务执行失败。
//
// 参数：
//   - ctx：上下文，用于取消和 TraceID 追踪
//   - fn：任务执行函数，接收派生 context，返回结果和错误
//
// 使用示例：
//
//	g := group.DefaultGroup[int]()
//	g.WithTimeout(5 * time.Second)
//	for _, id := range ids {
//	    g.Go(ctx, func(ctx context.Context) (int, error) {
//	        return fetchScore(ctx, id)
//	    })
//	}
//	results := g.Wait()
func (g *Group[T]) Go(ctx context.Context, fn func(context.Context) (T, error)) error {
	taskCtx, taskCancel, err := g.groupPrecheck(ctx, g.addResult, -1, "Go")
	if err != nil {
		return err
	}

	if err := g.groupAcquireSlot(ctx, taskCtx, taskCancel, g.addResult); err != nil {
		return err
	}

	g.groupRecordCancel(taskCancel)
	go g.runTaskImpl(taskCtx, taskCancel, g.addResult, g.timeout, g.failFast.Load(), g.cancel, fn)

	return nil
}

// GoWithTimeout 提交任务并单独设置该任务的超时时间（覆盖 Group 级别的 timeout）。
//
// 参数：
//   - ctx：上下文
//   - timeout：该任务的超时时间（覆盖 Group 级别默认值）
//   - fn：任务执行函数
//
// 使用示例：
//
//	// 组默认 5s 超时，但这个特定任务只有 1s
//	g.GoWithTimeout(ctx, 1*time.Second, func(ctx context.Context) (string, error) {
//	    return quickAPI(ctx)
//	})
func (g *Group[T]) GoWithTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) error {
	taskCtx, taskCancel, err := g.groupPrecheck(ctx, g.addResult, -1, "GoWithTimeout")
	if err != nil {
		return err
	}

	if err := g.groupAcquireSlot(ctx, taskCtx, taskCancel, g.addResult); err != nil {
		return err
	}

	g.groupRecordCancel(taskCancel)
	go g.runTaskImpl(taskCtx, taskCancel, g.addResult, timeout, g.failFast.Load(), g.cancel, fn)

	return nil
}

// GoAtWithTimeout 提交任务到指定索引并单独设置超时时间。
//
// 参数：
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - timeout：该任务的超时时间
//   - fn：任务执行函数
//
// 使用示例：
//
//	// 第 3 个任务只需 500ms
//	g.GoAtWithTimeout(2, ctx, 500*time.Millisecond, fn)
func (g *Group[T]) GoAtWithTimeout(index int, ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) error {
	record := func(r core.Result[T]) { g.setResultAt(index, r) }
	taskCtx, taskCancel, err := g.groupPrecheck(ctx, record, index, "GoAtWithTimeout")
	if err != nil {
		return err
	}

	if err := g.groupAcquireSlot(ctx, taskCtx, taskCancel, record); err != nil {
		return err
	}

	g.groupRecordCancel(taskCancel)
	go g.runTaskImpl(taskCtx, taskCancel, record, timeout, g.failFast.Load(), g.cancel, fn)

	return nil
}

func (g *Group[T]) runTaskImpl(taskCtx context.Context, taskCancel context.CancelFunc, record GroupRecordFunc[T], timeout time.Duration, failFast bool, failCancel context.CancelFunc, fn func(context.Context) (T, error)) {
	defer func() {
		<-g.limit
		g.busy.Add(-1)
		g.active.Add(-1)
		g.wg.Done()
	}()
	defer taskCancel()

	g.busy.Add(1)

	defer func() {
		if r := recover(); r != nil {
			core.LogCtxError(taskCtx, "async group panic recovered",
				core.Any("panic", r),
				core.Bytes("stack", core.NewPanicError(r).Stack))
			var zero T
			record(core.Result[T]{Value: zero, Err: core.NewPanicError(r)})
			atomic.AddInt64(&g.errCnt, 1)
			if failFast && failCancel != nil {
				failCancel()
			}
		}
	}()

	if timeout > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(taskCtx, timeout)
		defer cancel()
	}

	val, err := fn(taskCtx)
	if err != nil {
		if !(failFast && errors.Is(err, context.Canceled)) {
			core.LogTaskFail(taskCtx, err, "async task failed")
			atomic.AddInt64(&g.errCnt, 1)
		}
		if failFast && failCancel != nil {
			failCancel()
		}
	}
	record(core.Result[T]{Value: val, Err: err})
}

func (g *Group[T]) addResult(r core.Result[T]) {
	r.Occupied = true
	g.mu.Lock()
	for i := range g.results {
		if !g.results[i].Occupied {
			g.results[i] = r
			g.mu.Unlock()
			return
		}
	}
	g.results = append(g.results, r)
	g.mu.Unlock()
}

// GoAt 提交任务到指定索引位置，结果会写入 results[index]。
// 适用于需要按索引排列结果的场景。
//
// 参数：
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - fn：任务执行函数
//
// 使用示例：
//
//	// 按原始顺序排列结果
//	for i, url := range urls {
//	    g.GoAt(i, ctx, func(ctx context.Context) (string, error) {
//	        return httpGet(ctx, url)
//	    })
//	}
func (g *Group[T]) GoAt(index int, ctx context.Context, fn func(context.Context) (T, error)) error {
	record := func(r core.Result[T]) { g.setResultAt(index, r) }
	taskCtx, taskCancel, err := g.groupPrecheck(ctx, record, index, "GoAt")
	if err != nil {
		return err
	}

	if err := g.groupAcquireSlot(ctx, taskCtx, taskCancel, record); err != nil {
		return err
	}

	g.groupRecordCancel(taskCancel)
	go g.runTaskImpl(taskCtx, taskCancel, record, g.timeout, g.failFast.Load(), g.cancel, fn)

	return nil
}

func (g *Group[T]) setResultAt(index int, r core.Result[T]) {
	r.Occupied = true
	g.mu.Lock()
	if g.results[index].Occupied {
		existing := g.results[index]
		g.results[index] = r
		for i := range g.results {
			if !g.results[i].Occupied {
				g.results[i] = existing
				g.mu.Unlock()
				return
			}
		}
		g.results = append(g.results, existing)
	} else {
		g.results[index] = r
	}
	g.mu.Unlock()
}

// Wait 等待所有任务完成并返回结果切片。调用后 Group 进入 waited 状态，不能再次提交任务。
// 应先调用 WithTimeout / WithContext 等配置方法，再调用 Go/GoAt，最后调用 Wait。
//
// 使用示例：
//
//	results := g.Wait()
//	for _, r := range results {
//	    if r.Ok() {
//	        fmt.Println(r.Value)
//	    }
//	}
func (g *Group[T]) Wait() []core.Result[T] {
	g.addMu.Lock()
	g.waiting.Store(true)
	g.addMu.Unlock()
	g.wg.Wait()
	g.mu.Lock()
	g.waited = true
	g.waiting.Store(false)
	g.cancelAll()
	results := make([]core.Result[T], len(g.results))
	copy(results, g.results)
	cancel := g.cancel
	g.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return results
}

func (g *Group[T]) cancelAll() {
	for _, c := range g.cancels {
		c()
	}
	g.cancels = nil
}

// FailCount 返回失败任务数（线程安全，可在 Wait 前调用）。
func (g *Group[T]) FailCount() int64 {
	return atomic.LoadInt64(&g.errCnt)
}

// SuccessCount 返回成功任务数。
func (g *Group[T]) SuccessCount() int64 {
	return g.TotalCount() - g.FailCount()
}

// HasError 检查是否有任务失败。
func (g *Group[T]) HasError() bool {
	return g.FailCount() > 0
}

// TotalCount 返回已提交任务总数（包含成功和失败）。
func (g *Group[T]) TotalCount() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	var n int64
	for _, r := range g.results {
		if r.Occupied {
			n++
		}
	}
	return n
}

// Concurrency 返回最大并发数。
func (g *Group[T]) Concurrency() int {
	return g.concurrency
}

// Active 返回当前活跃（已启动未结束）的 goroutine 数。
func (g *Group[T]) Active() int {
	return int(g.active.Load())
}

// Busy 返回当前正在执行任务的 goroutine 数。
func (g *Group[T]) Busy() int {
	return int(g.busy.Load())
}

// GroupStats 记录 Group 的运行时统计信息。
type GroupStats struct {
	Concurrency int           // 最大并发数
	Active      int           // 当前活跃任务数
	Busy        int           // 当前忙碌任务数（正在执行 fn）
	FailFast    bool          // 是否启用 FailFast
	Timeout     time.Duration // 全局任务超时时间
	TotalTask   int64         // 历史提交任务总数
	SuccessTask int64         // 历史成功任务数
	FailTask    int64         // 历史失败任务数
}

func (s GroupStats) String() string {
	return fmt.Sprintf("GroupStats{concurrency=%d, active=%d, busy=%d, failFast=%v, timeout=%v, total=%d, success=%d, fail=%d}",
		s.Concurrency, s.Active, s.Busy, s.FailFast, s.Timeout, s.TotalTask, s.SuccessTask, s.FailTask)
}

// Stats 获取当前 Group 的运行时统计快照。
//
// 使用示例：
//
//	stats := g.Stats()
//	fmt.Printf("活跃: %d, 繁忙: %d, 成功: %d, 失败: %d\n",
//	    stats.Active, stats.Busy, stats.SuccessTask, stats.FailTask)
func (g *Group[T]) Stats() GroupStats {
	return GroupStats{
		Concurrency: g.concurrency,
		Active:      g.Active(),
		Busy:        g.Busy(),
		FailFast:    g.failFast.Load(),
		Timeout:     g.timeout,
		TotalTask:   g.TotalCount(),
		SuccessTask: g.SuccessCount(),
		FailTask:    g.FailCount(),
	}
}

// Errors 返回所有任务的错误切片。建议在 Wait 后调用以获取完整结果。
//
// 使用示例：
//
//	results := g.Wait()
//	for _, err := range g.Errors() {
//	    log.Println(err)
//	}
func (g *Group[T]) Errors() []error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.waited {
		core.LogCtxWarn(g.ctx, "async: Group.Errors called before Wait, results may be incomplete", core.Str("type", "Group"))
	}
	errs := make([]error, 0, g.FailCount())
	for _, r := range g.results {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errs
}

// FirstError 返回第一个错误（按 results 顺序）。无错误返回 nil。
func (g *Group[T]) FirstError() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.waited {
		core.LogCtxWarn(g.ctx, "async: Group.FirstError called before Wait, results may be incomplete", core.Str("type", "Group"))
	}
	for _, r := range g.results {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// Values 返回所有成功任务的结果值切片。建议在 Wait 后调用。
//
// 使用示例：
//
//	g.Wait()
//	for _, val := range g.Values() {
//	    fmt.Println(val)
//	}
func (g *Group[T]) Values() []T {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.waited {
		core.LogCtxWarn(g.ctx, "async: Group.Values called before Wait, results may be incomplete", core.Str("type", "Group"))
	}
	vals := make([]T, 0, len(g.results))
	for _, r := range g.results {
		if r.Err == nil {
			vals = append(vals, r.Value)
		}
	}
	return vals
}

// JoinErrors 使用 errors.Join 合并所有错误。
//
// 使用示例：
//
//	g.Wait()
//	if err := g.JoinErrors(); err != nil {
//	    log.Printf("有任务失败: %v", err)
//	}
func (g *Group[T]) JoinErrors() error {
	return errors.Join(g.Errors()...)
}

// Reset 重置 Group 状态以便复用。保留 timeout、submitTimeout、concurrency 配置。
// 必须在 Wait 之后且无活跃任务时调用。
//
// 使用示例：
//
//	g.Wait()                  // 等待第一批完成
//	g.Reset()                 // 重置状态
//	for _, item := range batch2 {
//	    g.Go(ctx, process(item))
//	}
//	g.Wait()
func (g *Group[T]) Reset() (*Group[T], error) {
	g.mu.Lock()
	if !g.waited {
		g.mu.Unlock()
		return g, errors.New("async: Group.Reset called before Wait, concurrent unsafe")
	}
	if g.active.Load() != 0 {
		g.mu.Unlock()
		return g, fmt.Errorf("async: Group.Reset called with %d active tasks, wait for all tasks to complete first", g.active.Load())
	}
	g.results = nil
	g.cancels = nil
	g.cancel = nil
	g.failFast.Store(false)
	g.waited = false
	savedTimeout := g.timeout
	savedSubmitTimeout := g.submitTimeout
	savedConcurrency := g.concurrency
	g.ctx = context.Background()
	g.mu.Unlock()
	atomic.StoreInt64(&g.errCnt, 0)
	g.active.Store(0)
	g.busy.Store(0)
	g.waiting.Store(false)
	g.limit = make(chan struct{}, savedConcurrency)
	core.LogCtxDebug(context.Background(), "async: Group.Reset completed, above config preserved across reset",
		core.Dur("timeout", savedTimeout),
		core.Dur("submit_timeout", savedSubmitTimeout),
		core.Int64("concurrency", int64(savedConcurrency)))
	return g, nil
}

// WaitTimeout 等待所有任务完成，最多等待 d 时长。返回 (结果, true) 或 (部分结果, false)。
//
// 参数：
//   - d：最长等待时间
//
// 使用示例：
//
//	results, ok := g.WaitTimeout(10 * time.Second)
//	if !ok {
//	    log.Println("部分任务未完成")
//	}
func (g *Group[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool) {
	g.waiting.Store(true)
	results, ok := core.WaitTimeoutImpl(d, &g.wg, g.cancel, &g.mu, &g.waited, &g.waiting, g.cancelAll, func() []core.Result[T] {
		results := make([]core.Result[T], len(g.results))
		copy(results, g.results)
		return results
	}, g.ctx)
	return results, ok
}

// WaitContext 等待所有任务完成或 ctx 取消。返回 (结果, true) 或 (部分结果, false)。
//
// 参数：
//   - ctx：上下文，取消后立即返回部分结果
//
// 使用示例：
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	results, ok := g.WaitContext(ctx)
func (g *Group[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool) {
	return core.WaitContextImpl(ctx, &g.wg, g.cancel, &g.mu, &g.waited, &g.waiting, g.cancelAll, func() []core.Result[T] {
		results := make([]core.Result[T], len(g.results))
		copy(results, g.results)
		return results
	}, g.ctx, "Group")
}

// ──────────────────────────── NoResult ────────────────────────────
//
// NoResult 是 Group[struct{}] 的类型别名，适用于只需要关注成功/失败、不需要返回值的场景。
// Wait() 方法无返回值（void）。
//
// 使用示例：
//
//	nr := group.NewNoResult(10)
//	nr.WithTimeout(5 * time.Second)
//	for _, item := range items {
//	    nr.Go(ctx, func(ctx context.Context) error {
//	        return sendMessage(ctx, item)
//	    })
//	}
//	nr.Wait() // 无返回值，通过 FailCount/HasError 检查
//	if nr.HasError() {
//	    log.Printf("失败 %d 个任务", nr.FailCount())
//	}

type NoResult Group[struct{}]

// NewNoResult 创建 NoResult 类型任务组。
//
// 参数：
//   - concurrency：最大并发数，<=0 时默认 1
func NewNoResult(concurrency int) *NoResult {
	return (*NoResult)(NewGroup[struct{}](concurrency))
}

// DefaultNoResult 使用默认并发数创建 NoResult。
func DefaultNoResult() *NoResult {
	return NewNoResult(core.IO())
}

func (nr *NoResult) WithTraceID(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithTraceID(ctx)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithContext(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithContext(ctx)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFailFast(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFailFast(ctx)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFCtx(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFCtx(ctx)
	return (*NoResult)(g), ctx
}

// WithTimeout 设置每个任务超时时间。
func (nr *NoResult) WithTimeout(d time.Duration) *NoResult {
	_ = (*Group[struct{}])(nr).WithTimeout(d)
	return nr
}

// WithSubmitTimeout 设置获取并发槽位的等待超时。
func (nr *NoResult) WithSubmitTimeout(d time.Duration) *NoResult {
	_ = (*Group[struct{}])(nr).WithSubmitTimeout(d)
	return nr
}

func (nr *NoResult) WithFFTraceID(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFTraceID(ctx)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFSubmitTO(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFSubmitTO(ctx, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFTimeout(ctx context.Context, timeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFTimeout(ctx, timeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFSubmitTOTraceID(ctx, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFTimeoutTraceID(ctx context.Context, timeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFTimeoutTraceID(ctx, timeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFTimeoutSubmitTO(ctx context.Context, timeout, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFTimeoutSubmitTO(ctx, timeout, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFTimeoutSubmitTOTraceID(ctx context.Context, timeout, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFTimeoutSubmitTOTraceID(ctx, timeout, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithCtxTraceID(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithCtxTraceID(ctx)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithCtxSubmitTO(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithCtxSubmitTO(ctx, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithCtxSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithCtxSubmitTOTraceID(ctx, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithCtxTimeout(ctx context.Context, timeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithCtxTimeout(ctx, timeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithCtxTimeoutTraceID(ctx context.Context, timeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithCtxTimeoutTraceID(ctx, timeout)
	return (*NoResult)(g), ctx
}

// Go 提交无返回值任务。
//
// 参数：
//   - ctx：上下文
//   - fn：任务执行函数，只返回 error
//
// 使用示例：
//
//	nr := group.DefaultNoResult()
//	nr.Go(ctx, func(ctx context.Context) error {
//	    return db.Insert(ctx, record)
//	})
func (nr *NoResult) Go(ctx context.Context, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).Go(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// GoWithTimeout 提交无返回值任务并设置超时。
//
// 参数：
//   - ctx：上下文
//   - timeout：任务超时时间
//   - fn：任务执行函数
func (nr *NoResult) GoWithTimeout(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).GoWithTimeout(ctx, timeout, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// GoAt 提交无返回值任务到指定索引。
//
// 参数：
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - fn：任务执行函数
func (nr *NoResult) GoAt(index int, ctx context.Context, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).GoAt(index, ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// GoAtWithTimeout 提交无返回值任务到指定索引并设置超时。
//
// 参数：
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - timeout：任务超时时间
//   - fn：任务执行函数
func (nr *NoResult) GoAtWithTimeout(index int, ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).GoAtWithTimeout(index, ctx, timeout, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// Wait 等待所有无返回值任务完成。
func (nr *NoResult) Wait() {
	(*Group[struct{}])(nr).Wait()
}

// WaitTimeout 等待任务完成，最多等待 d。
func (nr *NoResult) WaitTimeout(d time.Duration) (completed int64, ok bool) {
	results, ok := (*Group[struct{}])(nr).WaitTimeout(d)
	if ok {
		return int64(len(results)), true
	}
	return int64(len(results)), false
}

// WaitContext 等待任务完成或 ctx 取消。
func (nr *NoResult) WaitContext(ctx context.Context) (completed int64, ok bool) {
	results, ok := (*Group[struct{}])(nr).WaitContext(ctx)
	if ok {
		return int64(len(results)), true
	}
	return int64(len(results)), false
}

// FailCount 返回失败任务数。
func (nr *NoResult) FailCount() int64 {
	return (*Group[struct{}])(nr).FailCount()
}

// SuccessCount 返回成功任务数。
func (nr *NoResult) SuccessCount() int64 {
	return (*Group[struct{}])(nr).SuccessCount()
}

// HasError 检查是否有任务失败。
func (nr *NoResult) HasError() bool {
	return (*Group[struct{}])(nr).HasError()
}

// TotalCount 返回已提交任务总数。
func (nr *NoResult) TotalCount() int64 {
	return (*Group[struct{}])(nr).TotalCount()
}

// Concurrency 返回最大并发数。
func (nr *NoResult) Concurrency() int {
	return (*Group[struct{}])(nr).Concurrency()
}

// Active 返回当前活跃 goroutine 数。
func (nr *NoResult) Active() int {
	return (*Group[struct{}])(nr).Active()
}

// Busy 返回当前执行任务的 goroutine 数。
func (nr *NoResult) Busy() int {
	return (*Group[struct{}])(nr).Busy()
}

// Stats 获取运行时统计快照。
func (nr *NoResult) Stats() GroupStats {
	return (*Group[struct{}])(nr).Stats()
}

// Errors 返回所有错误。
func (nr *NoResult) Errors() []error {
	return (*Group[struct{}])(nr).Errors()
}

// FirstError 返回第一个错误。
func (nr *NoResult) FirstError() error {
	return (*Group[struct{}])(nr).FirstError()
}

// JoinErrors 使用 errors.Join 合并所有错误。
func (nr *NoResult) JoinErrors() error {
	return (*Group[struct{}])(nr).JoinErrors()
}

// Reset 重置 NoResult 状态以便复用。
func (nr *NoResult) Reset() (*NoResult, error) {
	_, err := (*Group[struct{}])(nr).Reset()
	return nr, err
}

// ──────────────────────────── NoResult helpers ────────────────────────────

// BuildAggregateNoResult 从已 Wait 的 NoResult 中构建聚合结果，用于 ForEach/ForEachWithFailFast 等无返回值场景。
//
// 参数：
//   - nr：已 Wait 的 NoResult 实例
func BuildAggregateNoResult(nr *NoResult) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	g := (*Group[struct{}])(nr)
	g.mu.Lock()
	results = make([]core.Result[struct{}], len(g.results))
	copy(results, g.results)
	g.mu.Unlock()
	total = int64(len(results))
	for _, r := range results {
		if r.Err != nil {
			failCnt++
			if firstErr == nil {
				firstErr = r.Err
			}
		}
	}
	return
}

// FillNoResultSkipped 在 nr.Wait() 之后补齐缺失的结果槽位，确保 TotalCount() == total。
//
// 参数：
//   - nr：已 Wait 的 NoResult 实例
//   - total：期望的总任务数
func FillNoResultSkipped(nr *NoResult, total int) {
	g := (*Group[struct{}])(nr)
	if need := total - len(g.results); need > 0 {
		g.mu.Lock()
		for i := 0; i < need; i++ {
			g.results = append(g.results, core.Result[struct{}]{Err: core.ErrSkipped})
		}
		g.mu.Unlock()
		atomic.AddInt64(&g.errCnt, int64(need))
	}
}
