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

	"github.com/jxue/async/core"
)

// ──────────────────────────── Group ────────────────────────────

type Group[T any] struct {
	limit         chan struct{}
	concurrency   int
	wg            sync.WaitGroup
	mu            sync.Mutex
	results       []core.Result[T]
	cancels       []context.CancelFunc
	errCnt        int64
	active        atomic.Int32
	busy          atomic.Int32
	waiting       atomic.Bool
	cancel        context.CancelFunc
	timeout       time.Duration
	submitTimeout time.Duration
	failFast      atomic.Bool
	waited        bool
	ctx           context.Context
}

// GroupRecordFunc 记录结果的函数类型，抽象 addResult（Go）和 setResultAt（GoAt）。
type GroupRecordFunc[T any] func(core.Result[T])

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

func DefaultGroup[T any]() *Group[T] {
	return NewGroup[T](core.IO())
}

func (g *Group[T]) WithTraceID(ctx context.Context) (*Group[T], context.Context) {
	ctx = core.EnsureTraceID(ctx)
	g.ctx = ctx
	return g, ctx
}

func (g *Group[T]) WithContext(ctx context.Context) (*Group[T], context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	g.cancel = core.MergeCancel(g.cancel, cancel)
	g.ctx = ctx
	return g, ctx
}

func (g *Group[T]) WithFailFast(ctx context.Context) (*Group[T], context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	g.cancel = core.MergeCancel(g.cancel, cancel)
	g.failFast.Store(true)
	g.ctx = ctx
	return g, ctx
}

func (g *Group[T]) WithFFCtx(ctx context.Context) (*Group[T], context.Context) {
	return g.WithFailFast(ctx)
}

func (g *Group[T]) WithFFTraceID(ctx context.Context) (*Group[T], context.Context) {
	g, ctx = g.WithFailFast(ctx)
	return g.WithTraceID(ctx)
}

func (g *Group[T]) WithFFSubmitTO(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFailFast(ctx)
	g.WithSubmitTimeout(submitTimeout)
	return g, ctx
}

func (g *Group[T]) WithFFSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFFSubmitTO(ctx, submitTimeout)
	return g.WithTraceID(ctx)
}

func (g *Group[T]) WithFFTimeout(ctx context.Context, timeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFailFast(ctx)
	g.WithTimeout(timeout)
	return g, ctx
}

func (g *Group[T]) WithCtxTraceID(ctx context.Context) (*Group[T], context.Context) {
	g, ctx = g.WithContext(ctx)
	return g.WithTraceID(ctx)
}

func (g *Group[T]) WithFFTimeoutTraceID(ctx context.Context, timeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFFTimeout(ctx, timeout)
	return g.WithTraceID(ctx)
}

func (g *Group[T]) WithFFTimeoutSubmitTO(ctx context.Context, timeout time.Duration, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFFTimeout(ctx, timeout)
	g.WithSubmitTimeout(submitTimeout)
	return g, ctx
}

func (g *Group[T]) WithFFTimeoutSubmitTOTraceID(ctx context.Context, timeout time.Duration, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithFFTimeoutSubmitTO(ctx, timeout, submitTimeout)
	return g.WithTraceID(ctx)
}

func (g *Group[T]) WithCtxTimeout(ctx context.Context, timeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithContext(ctx)
	g.WithTimeout(timeout)
	return g, ctx
}

func (g *Group[T]) WithCtxTimeoutTraceID(ctx context.Context, timeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithCtxTimeout(ctx, timeout)
	return g.WithTraceID(ctx)
}

func (g *Group[T]) WithTimeout(d time.Duration) *Group[T] {
	g.timeout = d
	return g
}

func (g *Group[T]) WithSubmitTimeout(d time.Duration) *Group[T] {
	g.submitTimeout = d
	return g
}

func (g *Group[T]) WithCtxSubmitTO(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithContext(ctx)
	g.WithSubmitTimeout(submitTimeout)
	return g, ctx
}

func (g *Group[T]) WithCtxSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context) {
	g, ctx = g.WithCtxSubmitTO(ctx, submitTimeout)
	return g.WithTraceID(ctx)
}

// ──────────────────────────── Group 内部方法 ────────────────────────────

func (g *Group[T]) groupPrecheck(ctx context.Context, record GroupRecordFunc[T], index int, caller string) (context.Context, context.CancelFunc, error) {
	if g.waiting.Load() {
		core.LogCtxError(ctx, fmt.Sprintf("async: Group.%s called while Group.Wait is in progress, task discarded", caller))
		record(core.Result[T]{Err: core.ErrGroupWaiting})
		atomic.AddInt64(&g.errCnt, 1)
		var cancel context.CancelFunc
		return ctx, cancel, core.ErrGroupWaiting
	}
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

	g.wg.Add(1)
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
		core.LogTaskFail(taskCtx, err, "async task failed")
		atomic.AddInt64(&g.errCnt, 1)
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

func (g *Group[T]) Wait() []core.Result[T] {
	g.waiting.Store(true)
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

func (g *Group[T]) FailCount() int64 {
	return atomic.LoadInt64(&g.errCnt)
}

func (g *Group[T]) SuccessCount() int64 {
	return g.TotalCount() - g.FailCount()
}

func (g *Group[T]) HasError() bool {
	return g.FailCount() > 0
}

func (g *Group[T]) TotalCount() int64 {
	g.mu.Lock()
	n := len(g.results)
	g.mu.Unlock()
	return int64(n)
}

func (g *Group[T]) Concurrency() int {
	return g.concurrency
}

func (g *Group[T]) Active() int {
	return int(g.active.Load())
}

func (g *Group[T]) Busy() int {
	return int(g.busy.Load())
}

type GroupStats struct {
	Concurrency int
	Active      int
	Busy        int
	FailFast    bool
	Timeout     time.Duration
	TotalTask   int64
	SuccessTask int64
	FailTask    int64
}

func (s GroupStats) String() string {
	return fmt.Sprintf("GroupStats{concurrency=%d, active=%d, busy=%d, failFast=%v, timeout=%v, total=%d, success=%d, fail=%d}",
		s.Concurrency, s.Active, s.Busy, s.FailFast, s.Timeout, s.TotalTask, s.SuccessTask, s.FailTask)
}

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

func (g *Group[T]) JoinErrors() error {
	return errors.Join(g.Errors()...)
}

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
	core.LogCtxDebug(context.Background(), "async: Group.Reset completed, above config preserved across reset",
		core.Dur("timeout", savedTimeout),
		core.Dur("submit_timeout", savedSubmitTimeout),
		core.Int64("concurrency", int64(savedConcurrency)))
	return g, nil
}

func (g *Group[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool) {
	g.waiting.Store(true)
	results, ok := core.WaitTimeoutImpl(d, &g.wg, g.cancel, &g.mu, &g.waited, &g.waiting, g.cancelAll, func() []core.Result[T] {
		results := make([]core.Result[T], len(g.results))
		copy(results, g.results)
		return results
	}, g.ctx)
	return results, ok
}

func (g *Group[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool) {
	return core.WaitContextImpl(ctx, &g.wg, g.cancel, &g.mu, &g.waited, &g.waiting, g.cancelAll, func() []core.Result[T] {
		results := make([]core.Result[T], len(g.results))
		copy(results, g.results)
		return results
	}, g.ctx, "Group")
}

// ──────────────────────────── NoResult ────────────────────────────

type NoResult Group[struct{}]

func NewNoResult(concurrency int) *NoResult {
	return (*NoResult)(NewGroup[struct{}](concurrency))
}

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

func (nr *NoResult) WithTimeout(d time.Duration) *NoResult {
	_ = (*Group[struct{}])(nr).WithTimeout(d)
	return nr
}

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

func (nr *NoResult) Go(ctx context.Context, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).Go(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

func (nr *NoResult) GoWithTimeout(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).GoWithTimeout(ctx, timeout, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

func (nr *NoResult) GoAt(index int, ctx context.Context, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).GoAt(index, ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

func (nr *NoResult) GoAtWithTimeout(index int, ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).GoAtWithTimeout(index, ctx, timeout, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

func (nr *NoResult) Wait() {
	(*Group[struct{}])(nr).Wait()
}

func (nr *NoResult) WaitTimeout(d time.Duration) (completed int64, ok bool) {
	results, ok := (*Group[struct{}])(nr).WaitTimeout(d)
	if ok {
		return int64(len(results)), true
	}
	return int64(len(results)), false
}

func (nr *NoResult) WaitContext(ctx context.Context) (completed int64, ok bool) {
	results, ok := (*Group[struct{}])(nr).WaitContext(ctx)
	if ok {
		return int64(len(results)), true
	}
	return int64(len(results)), false
}

func (nr *NoResult) FailCount() int64 {
	return (*Group[struct{}])(nr).FailCount()
}

func (nr *NoResult) SuccessCount() int64 {
	return (*Group[struct{}])(nr).SuccessCount()
}

func (nr *NoResult) HasError() bool {
	return (*Group[struct{}])(nr).HasError()
}

func (nr *NoResult) TotalCount() int64 {
	return (*Group[struct{}])(nr).TotalCount()
}

func (nr *NoResult) Concurrency() int {
	return (*Group[struct{}])(nr).Concurrency()
}

func (nr *NoResult) Active() int {
	return (*Group[struct{}])(nr).Active()
}

func (nr *NoResult) Busy() int {
	return (*Group[struct{}])(nr).Busy()
}

func (nr *NoResult) Stats() GroupStats {
	return (*Group[struct{}])(nr).Stats()
}

func (nr *NoResult) Errors() []error {
	return (*Group[struct{}])(nr).Errors()
}

func (nr *NoResult) FirstError() error {
	return (*Group[struct{}])(nr).FirstError()
}

func (nr *NoResult) JoinErrors() error {
	return (*Group[struct{}])(nr).JoinErrors()
}

func (nr *NoResult) Reset() (*NoResult, error) {
	_, err := (*Group[struct{}])(nr).Reset()
	return nr, err
}

// ──────────────────────────── NoResult helpers ────────────────────────────

// BuildAggregateNoResult 从已 Wait 的 NoResult 中构建聚合结果，用于 ForEach/ForEachWithFailFast 等无返回值场景。
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
