// Package pool 提供泛型协程池 Pool[T]，适合高频小任务的协程复用场景。
// 池的生命周期：NewPool → worker goroutines 启动 → Submit → Wait → Close。
// 支持超时控制、快速失败、提交超时等选项。
package pool

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jxue/async/core"
	"github.com/rs/zerolog/log"
)

// Pool 泛型协程池，复用 goroutine 处理高频并发任务。
type Pool[T any] struct {
	taskCh        chan core.PoolTask[T]
	wg            sync.WaitGroup
	workerWg      sync.WaitGroup
	mu            sync.Mutex
	results       []core.Result[T]
	cancels       []context.CancelFunc
	errCnt        int64
	cancel        context.CancelFunc
	failFast      atomic.Bool
	timeout       time.Duration
	submitTimeout time.Duration
	closed        atomic.Bool
	size          atomic.Int32
	active        atomic.Int32
	busy          atomic.Int32
	pending       atomic.Int32
	waiting       atomic.Bool
	waited        bool
	ctx           context.Context
}

// SubmitResult 封装 Submit 的返回结果。
type SubmitResult struct {
	Index int
	Err   error
}

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

func DefaultPool[T any]() *Pool[T] {
	return NewPool[T](core.IO())
}

func (p *Pool[T]) WithTraceID(ctx context.Context) (*Pool[T], context.Context) {
	ctx = core.EnsureTraceID(ctx)
	p.ctx = ctx
	return p, ctx
}

func (p *Pool[T]) WithContext(ctx context.Context) (*Pool[T], context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = core.MergeCancel(p.cancel, cancel)
	p.ctx = ctx
	return p, ctx
}

func (p *Pool[T]) WithFailFast(ctx context.Context) (*Pool[T], context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = core.MergeCancel(p.cancel, cancel)
	p.failFast.Store(true)
	p.ctx = ctx
	return p, ctx
}

func (p *Pool[T]) WithFFCtx(ctx context.Context) (*Pool[T], context.Context) {
	return p.WithFailFast(ctx)
}

func (p *Pool[T]) WithFFTraceID(ctx context.Context) (*Pool[T], context.Context) {
	p, ctx = p.WithFailFast(ctx)
	return p.WithTraceID(ctx)
}

func (p *Pool[T]) WithFFSubmitTO(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFailFast(ctx)
	p.WithSubmitTimeout(submitTimeout)
	return p, ctx
}

func (p *Pool[T]) WithFFSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFSubmitTO(ctx, submitTimeout)
	return p.WithTraceID(ctx)
}

func (p *Pool[T]) WithFFTimeout(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFailFast(ctx)
	p.WithTimeout(timeout)
	return p, ctx
}

func (p *Pool[T]) WithCtxTraceID(ctx context.Context) (*Pool[T], context.Context) {
	p, ctx = p.WithContext(ctx)
	return p.WithTraceID(ctx)
}

func (p *Pool[T]) WithFFTimeoutTraceID(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFTimeout(ctx, timeout)
	return p.WithTraceID(ctx)
}

func (p *Pool[T]) WithFFTimeoutSubmitTO(ctx context.Context, timeout, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFTimeout(ctx, timeout)
	p.WithSubmitTimeout(submitTimeout)
	return p, ctx
}

func (p *Pool[T]) WithFFTimeoutSubmitTOTraceID(ctx context.Context, timeout, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithFFTimeoutSubmitTO(ctx, timeout, submitTimeout)
	return p.WithTraceID(ctx)
}

func (p *Pool[T]) WithCtxTimeout(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithContext(ctx)
	p.WithTimeout(timeout)
	return p, ctx
}

func (p *Pool[T]) WithCtxTimeoutTraceID(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithCtxTimeout(ctx, timeout)
	return p.WithTraceID(ctx)
}

func (p *Pool[T]) WithTimeout(d time.Duration) *Pool[T] {
	p.timeout = d
	return p
}

func (p *Pool[T]) WithSubmitTimeout(d time.Duration) *Pool[T] {
	p.submitTimeout = d
	return p
}

func (p *Pool[T]) WithCtxSubmitTO(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithContext(ctx)
	p.WithSubmitTimeout(submitTimeout)
	return p, ctx
}

func (p *Pool[T]) WithCtxSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context) {
	p, ctx = p.WithCtxSubmitTO(ctx, submitTimeout)
	return p.WithTraceID(ctx)
}

func (p *Pool[T]) worker() {
	defer p.workerWg.Done()
	for task := range p.taskCh {
		p.active.Add(1)
		p.processTask(task)
		p.active.Add(-1)
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
			log.Ctx(taskCtx).Error().
				Interface("panic", r).
				Bytes("stack", core.NewPanicError(r).Stack).
				Msg("async pool panic recovered")
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
		core.LogTaskFail(taskCtx, err, "async pool task failed")
		atomic.AddInt64(&p.errCnt, 1)
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

// NoResultPool type alias for Pool of empty structs.
type NoResultPool = Pool[struct{}]

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

	select {
	case p.taskCh <- task:
		return nil
	default:
		p.discardTask(record, idx, taskCancel, core.ErrSubmitTimeout)
		return core.ErrSubmitTimeout
	}
}

func (p *Pool[T]) Resize(newSize int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	current := int(p.size.Load())
	if newSize > current {
		added := newSize - current
		for i := 0; i < added; i++ {
			p.workerWg.Add(1)
			go p.worker()
		}
		p.size.Store(int32(newSize))
		p.taskCh = make(chan core.PoolTask[T], newSize)
		return added
	} else if newSize < current {
		quit := current - newSize
		p.size.Store(int32(newSize))
		return quit
	}
	return 0
}

func (p *Pool[T]) ResizeAndWaitTimeout(newSize int, timeout time.Duration) {
	p.Resize(newSize)
	time.Sleep(timeout)
	p.wg.Wait()
}

func (p *Pool[T]) enqueueTask(ctx context.Context, taskCtx context.Context, taskCancel context.CancelFunc, task core.PoolTask[T], record core.PoolRecordFunc[T], idx int) error {
	if p.submitTimeout > 0 {
		timer := time.NewTimer(p.submitTimeout)
		defer timer.Stop()
		select {
		case p.taskCh <- task:
			return nil
		case <-taskCtx.Done():
			p.discardTask(record, idx, taskCancel, taskCtx.Err())
			return taskCtx.Err()
		case <-timer.C:
			err := core.ErrSubmitTimeout
			log.Ctx(ctx).Warn().Err(err).Msg("async: Pool submit timeout, task queue full")
			p.discardTask(record, idx, taskCancel, err)
			return err
		}
	}

	timer := time.NewTimer(core.SlotAcquireWarnTimeout)
	defer timer.Stop()

	for {
		select {
		case p.taskCh <- task:
			return nil
		case <-taskCtx.Done():
			p.discardTask(record, idx, taskCancel, taskCtx.Err())
			return taskCtx.Err()
		case <-timer.C:
			log.Ctx(ctx).Warn().Dur("elapsed", core.SlotAcquireWarnTimeout).Msg("async: Pool.Submit blocking on task queue, consider setting WithSubmitTimeout")
			timer.Reset(core.SlotAcquireWarnTimeout)
		}
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

func (p *Pool[T]) WaitAndClose() []core.Result[T] {
	results := p.Wait()
	p.Close()
	return results
}

func (p *Pool[T]) Close() {
	if p.closed.Load() {
		return
	}
	p.closed.Store(true)
	close(p.taskCh)
	p.workerWg.Wait()
}

func (p *Pool[T]) CloseAndWait() {
	p.Close()
	p.Wait()
}

func (p *Pool[T]) CloseAndWaitTimeout(timeout time.Duration) (ok bool, workerDone <-chan struct{}) {
	p.Close()
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

func (p *Pool[T]) FailCount() int64 {
	return atomic.LoadInt64(&p.errCnt)
}

func (p *Pool[T]) SuccessCount() int64 {
	return p.TotalCount() - p.FailCount()
}

func (p *Pool[T]) HasError() bool {
	return p.FailCount() > 0
}

func (p *Pool[T]) TotalCount() int64 {
	p.mu.Lock()
	n := len(p.results)
	p.mu.Unlock()
	return int64(n)
}

func (p *Pool[T]) Size() int {
	return int(p.size.Load())
}

func (p *Pool[T]) Active() int {
	return int(p.active.Load())
}

func (p *Pool[T]) Busy() int {
	return int(p.busy.Load())
}

func (p *Pool[T]) Pending() int {
	return int(p.pending.Load())
}

type PoolStats struct {
	Size        int
	Active      int
	Busy        int
	Pending     int
	FailFast    bool
	Timeout     time.Duration
	TotalTask   int64
	SuccessTask int64
	FailTask    int64
}

func (s PoolStats) String() string {
	return fmt.Sprintf("PoolStats{size=%d, active=%d, busy=%d, pending=%d, failFast=%v, timeout=%v, total=%d, success=%d, fail=%d}",
		s.Size, s.Active, s.Busy, s.Pending, s.FailFast, s.Timeout, s.TotalTask, s.SuccessTask, s.FailTask)
}

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

func (p *Pool[T]) Errors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.waited {
		log.Ctx(p.ctx).Warn().Str("type", "Pool").Msg("async: Pool.Errors called before Wait, results may be incomplete")
	}
	errs := make([]error, 0, p.FailCount())
	for _, r := range p.results {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errs
}

func (p *Pool[T]) FirstError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.waited {
		log.Ctx(p.ctx).Warn().Str("type", "Pool").Msg("async: Pool.FirstError called before Wait, results may be incomplete")
	}
	for _, r := range p.results {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

func (p *Pool[T]) JoinErrors() error {
	return errors.Join(p.Errors()...)
}

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
		log.Fatal().Err(err).Str("stack", string(buf[:n])).
			Str("name", name).
			Msg("async pool fatal")
	}
}

// ──────────────────────────── 便捷函数 ────────────────────────────

func Submit[T any](ctx context.Context, fn func(context.Context) (T, error)) (*Pool[T], int, error) {
	p := DefaultPool[T]()
	p.ctx = ctx
	idx, err := p.submitIndexed(ctx, fn)
	return p, idx, err
}

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

func SubmitSafeN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult) {
	pool, results, err := SubmitN(ctx, fn, n)
	for _, r := range results {
		checkErr(r.Err, "SubmitSafeN")
	}
	checkErr(err, "SubmitSafeN")
	return pool, results
}

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

func (p *Pool[T]) Values() []T {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.waited {
		_ = debug.Stack()
	}
	vals := make([]T, 0, len(p.results))
	for _, r := range p.results {
		if r.Err == nil {
			vals = append(vals, r.Value)
		}
	}
	return vals
}

func (p *Pool[T]) Reset() (*Pool[T], error) {
	p.mu.Lock()
	if !p.waited {
		p.mu.Unlock()
		return p, fmt.Errorf("async: Pool.Reset called before Wait")
	}
	p.results = nil
	p.cancels = nil
	p.waited = false
	savedTimeout := p.timeout
	savedSubmitTimeout := p.submitTimeout
	p.ctx = context.Background()
	p.mu.Unlock()
	atomic.StoreInt64(&p.errCnt, 0)
	p.active.Store(0)
	p.busy.Store(0)
	p.pending.Store(0)
	p.waiting.Store(false)
	p.failFast.Store(false)
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	log.Ctx(context.Background()).Debug().
		Dur("timeout", savedTimeout).
		Dur("submit_timeout", savedSubmitTimeout).
		Msg("async: Pool.Reset completed")
	return p, nil
}
