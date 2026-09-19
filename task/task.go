// Package task 提供单个异步任务和带 cancel 功能的 AsyncResult。
package task

import (
	"context"
	"sync"
	"time"

	"github.com/chichengyu/async/core"
)

// Task 表示一个可取消的异步任务。
type Task[T any] struct {
	Ctx    context.Context
	Cancel context.CancelFunc
	Result func() (T, error)
}

// AsyncResult 持有一个 results chan（只读），通过它等待并获取任务结果。
type AsyncResult[T any] struct {
	results <-chan core.Result[T]
	mu      sync.Mutex
	ready   chan struct{}
	result  core.Result[T]
}

// startReader 启动后台 goroutine 从 results channel 读取结果并缓存，
// 然后在 ready channel 上广播。保证多个并发 Wait 调用安全。
func (ar *AsyncResult[T]) startReader() {
	go func() {
		r := <-ar.results
		ar.mu.Lock()
		ar.result = r
		ar.mu.Unlock()
		close(ar.ready)
	}()
}

func (ar *AsyncResult[T]) getResult() core.Result[T] {
	<-ar.ready
	ar.mu.Lock()
	r := ar.result
	ar.mu.Unlock()
	return r
}

func (ar *AsyncResult[T]) Wait() (T, error) {
	r := ar.getResult()
	return r.Value, r.Err
}

func (ar *AsyncResult[T]) WaitCh() <-chan core.Result[T] {
	return ar.results
}

func (ar *AsyncResult[T]) Cancel() (T, error) {
	select {
	case <-ar.ready:
		ar.mu.Lock()
		r := ar.result
		ar.mu.Unlock()
		return r.Value, r.Err
	default:
		var zero T
		return zero, context.Canceled
	}
}

func (ar *AsyncResult[T]) Ok() bool {
	r := ar.getResult()
	return r.Err == nil
}

func (ar *AsyncResult[T]) IsPanic() bool {
	r := ar.getResult()
	return r.IsPanic()
}

func (ar *AsyncResult[T]) WaitTimeout(timeout time.Duration) (T, error, bool) {
	select {
	case <-ar.ready:
		ar.mu.Lock()
		r := ar.result
		ar.mu.Unlock()
		return r.Value, r.Err, true
	case <-time.After(timeout):
		var zero T
		return zero, core.ErrTimeout, false
	}
}

func Go[T any](ctx context.Context, fn func(context.Context) (T, error)) *AsyncResult[T] {
	ctx = core.EnsureTraceID(ctx)
	results := make(chan core.Result[T], 1)
	ar := &AsyncResult[T]{results: results, ready: make(chan struct{})}
	ar.startReader()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				var zero T
				results <- core.Result[T]{Value: zero, Err: core.NewPanicError(r)}
			}
			close(results)
		}()
		val, err := fn(ctx)
		results <- core.Result[T]{Value: val, Err: err}
	}()
	return ar
}

func GoResult[T any](ctx context.Context, fn func(context.Context) (T, error)) Task[T] {
	ctx = core.EnsureTraceID(ctx)
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan core.Result[T], 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				var zero T
				results <- core.Result[T]{Value: zero, Err: core.NewPanicError(r)}
			}
			close(results)
		}()
		val, err := fn(ctx)
		results <- core.Result[T]{Value: val, Err: err}
	}()
	return Task[T]{
		Ctx:    ctx,
		Cancel: cancel,
		Result: func() (T, error) {
			r := <-results
			return r.Value, r.Err
		},
	}
}

// ──────────────────────────── NoResult variants ────────────────────────────

type NoResult struct{}

type TaskNoResult = Task[NoResult]

type AsyncResultNoResult = AsyncResult[NoResult]

func GoAction(ctx context.Context, fn func(context.Context) error) *AsyncResult[NoResult] {
	ctx = core.EnsureTraceID(ctx)
	results := make(chan core.Result[NoResult], 1)
	ar := &AsyncResult[NoResult]{results: results, ready: make(chan struct{})}
	ar.startReader()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				results <- core.Result[NoResult]{Value: NoResult{}, Err: core.NewPanicError(r)}
			}
			close(results)
		}()
		err := fn(ctx)
		results <- core.Result[NoResult]{Value: NoResult{}, Err: err}
	}()
	return ar
}

func GoResultAction(ctx context.Context, fn func(context.Context) error) Task[NoResult] {
	ctx = core.EnsureTraceID(ctx)
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan core.Result[NoResult], 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				results <- core.Result[NoResult]{Value: NoResult{}, Err: core.NewPanicError(r)}
			}
			close(results)
		}()
		err := fn(ctx)
		results <- core.Result[NoResult]{Value: NoResult{}, Err: err}
	}()
	return Task[NoResult]{
		Ctx:    ctx,
		Cancel: cancel,
		Result: func() (NoResult, error) {
			r := <-results
			return r.Value, r.Err
		},
	}
}

// ──────────────────────────── append-only mutex ────────────────────────────

type Mu[T any] struct {
	mu sync.Mutex
	ts []T
}

func (m *Mu[T]) Append(add func() T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ts = append(m.ts, add())
}

func (m *Mu[T]) Snapshot() []T {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]T, len(m.ts))
	copy(out, m.ts)
	return out
}
