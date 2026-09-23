package async

import (
	"context"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/retry"
)

// ──────────────────────────── MapBuilder ────────────────────────────

// MapBuilder 链式构建 Map 操作，一个 Builder 覆盖所有 Map 变体。
// 通过 NewMapBuilder 创建，链式配置并发度/超时/FailFast，最后调用 Run/RunSerial/RunChunk/RunChunked/RunPool 执行。
//
// 使用示例：
//
//	results := async.NewMapBuilder[int, string](ctx, items).
//	    WithConcurrency(4).
//	    WithFailFast().
//	    WithTimeout(5 * time.Second).
//	    Run(func(ctx context.Context, n int) (string, error) {
//	        return strconv.Itoa(n * 2), nil
//	    })
type MapBuilder[T any, R any] struct {
	ctx         context.Context
	items       []T
	concurrency int
	timeout     time.Duration
	failFast    bool
}

// NewMapBuilder 创建 Map 操作构建器。
func NewMapBuilder[T any, R any](ctx context.Context, items []T) *MapBuilder[T, R] {
	return &MapBuilder[T, R]{
		ctx:         core.EnsureTraceID(ctx),
		items:       items,
		concurrency: core.IO(),
	}
}

// DefaultMapBuilder 使用 context.Background() 创建 Map 构建器。
func DefaultMapBuilder[T any, R any](items []T) *MapBuilder[T, R] {
	return NewMapBuilder[T, R](context.Background(), items)
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (m *MapBuilder[T, R]) WithLoggerCh(l core.Logger) *MapBuilder[T, R] {
	core.SetLogger(l)
	return m
}

// WithConcurrency 设置并发度，n <= 0 使用默认 IO 并发度。
func (m *MapBuilder[T, R]) WithConcurrency(n int) *MapBuilder[T, R] {
	if n > 0 {
		m.concurrency = n
	}
	return m
}

// WithConcurrencyDefault 使用默认 IO 并发度。
func (m *MapBuilder[T, R]) WithConcurrencyDefault() *MapBuilder[T, R] {
	m.concurrency = core.IO()
	return m
}

// WithTimeout 设置总超时时间。
func (m *MapBuilder[T, R]) WithTimeout(d time.Duration) *MapBuilder[T, R] {
	m.timeout = d
	return m
}

// WithFailFast 启用 FailFast 模式：任一任务失败即终止。
func (m *MapBuilder[T, R]) WithFailFast() *MapBuilder[T, R] {
	m.failFast = true
	return m
}

// Run 并发执行 Map，返回结果切片（顺序保持）。
func (m *MapBuilder[T, R]) Run(fn func(context.Context, T) (R, error)) []Result[R] {
	switch {
	case m.timeout > 0 && m.failFast:
		r, _ := MapWithFFTimeout(m.ctx, m.items, m.concurrency, m.timeout, fn)
		return r
	case m.timeout > 0:
		return MapWithTimeout(m.ctx, m.items, m.concurrency, m.timeout, fn)
	case m.failFast:
		r, _ := MapWithFailFast(m.ctx, m.items, m.concurrency, fn)
		return r
	default:
		return Map(m.ctx, m.items, m.concurrency, fn)
	}
}

// RunWithError 并发执行 Map，返回结果切片和 FailFast 第一个错误。
func (m *MapBuilder[T, R]) RunWithError(fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	switch {
	case m.timeout > 0 && m.failFast:
		return MapWithFFTimeout(m.ctx, m.items, m.concurrency, m.timeout, fn)
	case m.timeout > 0:
		return MapWithTimeout(m.ctx, m.items, m.concurrency, m.timeout, fn), nil
	case m.failFast:
		return MapWithFailFast(m.ctx, m.items, m.concurrency, fn)
	default:
		return Map(m.ctx, m.items, m.concurrency, fn), nil
	}
}

// RunSerial 串行映射每个元素（不并发），等价于 MapSerial / MapSerialFailFast。
func (m *MapBuilder[T, R]) RunSerial(fn func(context.Context, T) (R, error)) []Result[R] {
	if m.failFast {
		r, _ := MapSerialFailFast(m.ctx, m.items, fn)
		return r
	}
	return MapSerial(m.ctx, m.items, fn)
}

// RunChunk 分块并发 Map（fn 接收整块），等价于 MapChunk 系列。
func (m *MapBuilder[T, R]) RunChunk(batchSize int, fn func(context.Context, []T) (R, error)) []Result[R] {
	switch {
	case m.timeout > 0 && m.failFast:
		r, _ := MapChunkWithFFTimeout(m.ctx, m.items, m.concurrency, batchSize, m.timeout, fn)
		return r
	case m.timeout > 0:
		return MapChunkWithTimeout(m.ctx, m.items, m.concurrency, batchSize, m.timeout, fn)
	case m.failFast:
		r, _ := MapChunkWithFailFast(m.ctx, m.items, m.concurrency, batchSize, fn)
		return r
	default:
		return MapChunk(m.ctx, m.items, m.concurrency, batchSize, fn)
	}
}

// RunChunked 分块并发 Map（fn 接收单个元素），等价于 MapChunked 系列。
func (m *MapBuilder[T, R]) RunChunked(batchSize int, fn func(context.Context, T) (R, error)) []Result[R] {
	switch {
	case m.timeout > 0 && m.failFast:
		r, _ := MapChunkedWithFFTimeout(m.ctx, m.items, m.concurrency, batchSize, m.timeout, fn)
		return r
	case m.timeout > 0:
		return MapChunkedWithTimeout(m.ctx, m.items, m.concurrency, batchSize, m.timeout, fn)
	case m.failFast:
		r, _ := MapChunkedWithFailFast(m.ctx, m.items, m.concurrency, batchSize, fn)
		return r
	default:
		return MapChunked(m.ctx, m.items, m.concurrency, batchSize, fn)
	}
}

// RunPool 使用协程池执行 Map，等价于 MapPool + ResultValues。
func (m *MapBuilder[T, R]) RunPool(fn func(context.Context, T) (R, error)) []Result[R] {
	_, results, _ := MapPool(m.ctx, m.items, fn, m.concurrency)
	return results
}

// RunPoolWithError 使用协程池执行 Map，返回池、结果和错误。
func (m *MapBuilder[T, R]) RunPoolWithError(fn func(context.Context, T) (R, error)) (*Pool[R], []core.Result[R], error) {
	return MapPool(m.ctx, m.items, fn, m.concurrency)
}

// ──────────────────────────── ForEachBuilder ────────────────────────────

// ForEachBuilder 链式构建 ForEach 操作，一个 Builder 覆盖所有 ForEach 变体。
// 通过 NewForEachBuilder 创建，链式配置并发度/超时/FailFast，最后调用 Run/RunSerial/RunChunk/RunChunked/RunPool 执行。
//
// 使用示例：
//
//	_, err := async.NewForEachBuilder[int](ctx, items).
//	    WithConcurrency(4).
//	    WithFailFast().
//	    Run(func(ctx context.Context, n int) error {
//	        return saveItem(ctx, n)
//	    })
type ForEachBuilder[T any] struct {
	ctx         context.Context
	items       []T
	concurrency int
	timeout     time.Duration
	failFast    bool
}

// NewForEachBuilder 创建 ForEach 操作构建器。
func NewForEachBuilder[T any](ctx context.Context, items []T) *ForEachBuilder[T] {
	return &ForEachBuilder[T]{
		ctx:         core.EnsureTraceID(ctx),
		items:       items,
		concurrency: core.IO(),
	}
}

// DefaultForEachBuilder 使用 context.Background() 创建 ForEach 构建器。
func DefaultForEachBuilder[T any](items []T) *ForEachBuilder[T] {
	return NewForEachBuilder[T](context.Background(), items)
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (f *ForEachBuilder[T]) WithLoggerCh(l core.Logger) *ForEachBuilder[T] {
	core.SetLogger(l)
	return f
}

// WithConcurrency 设置并发度，n <= 0 使用默认 IO 并发度。
func (f *ForEachBuilder[T]) WithConcurrency(n int) *ForEachBuilder[T] {
	if n > 0 {
		f.concurrency = n
	}
	return f
}

// WithConcurrencyDefault 使用默认 IO 并发度。
func (f *ForEachBuilder[T]) WithConcurrencyDefault() *ForEachBuilder[T] {
	f.concurrency = core.IO()
	return f
}

// WithTimeout 设置总超时时间。
func (f *ForEachBuilder[T]) WithTimeout(d time.Duration) *ForEachBuilder[T] {
	f.timeout = d
	return f
}

// WithFailFast 启用 FailFast 模式：任一任务失败即终止。
func (f *ForEachBuilder[T]) WithFailFast() *ForEachBuilder[T] {
	f.failFast = true
	return f
}

// Run 并发遍历每个元素，返回 NoResult 和可能的错误。
func (f *ForEachBuilder[T]) Run(fn func(context.Context, T) error) (*NoResult, error) {
	switch {
	case f.timeout > 0 && f.failFast:
		return ForEachWithFFTimeout(f.ctx, f.items, f.concurrency, f.timeout, fn)
	case f.timeout > 0:
		return ForEachWithTimeout(f.ctx, f.items, f.concurrency, f.timeout, fn)
	case f.failFast:
		return ForEachWithFailFast(f.ctx, f.items, f.concurrency, fn)
	default:
		return ForEach(f.ctx, f.items, f.concurrency, fn)
	}
}

// RunSerial 串行遍历每个元素。
func (f *ForEachBuilder[T]) RunSerial(fn func(context.Context, T) error) (*NoResult, error) {
	if f.failFast {
		return ForEachSerialFailFast(f.ctx, f.items, fn)
	}
	return ForEachSerial(f.ctx, f.items, fn)
}

// RunChunk 分块并发遍历（fn 接收整块）。
func (f *ForEachBuilder[T]) RunChunk(batchSize int, fn func(context.Context, []T) error) (*NoResult, error) {
	switch {
	case f.timeout > 0 && f.failFast:
		return ForEachChunkWithFFTimeout(f.ctx, f.items, f.concurrency, batchSize, f.timeout, fn)
	case f.timeout > 0:
		return ForEachChunkWithTimeout(f.ctx, f.items, f.concurrency, batchSize, f.timeout, fn)
	case f.failFast:
		return ForEachChunkWithFailFast(f.ctx, f.items, f.concurrency, batchSize, fn)
	default:
		return ForEachChunk(f.ctx, f.items, f.concurrency, batchSize, fn)
	}
}

// RunChunked 分块并发遍历（fn 接收单个元素）。
func (f *ForEachBuilder[T]) RunChunked(batchSize int, fn func(context.Context, T) error) (*NoResult, error) {
	switch {
	case f.timeout > 0 && f.failFast:
		return ForEachChunkedWithFFTimeout(f.ctx, f.items, f.concurrency, batchSize, f.timeout, fn)
	case f.timeout > 0:
		return ForEachChunkedWithTimeout(f.ctx, f.items, f.concurrency, batchSize, f.timeout, fn)
	case f.failFast:
		return ForEachChunkedWithFailFast(f.ctx, f.items, f.concurrency, batchSize, fn)
	default:
		return ForEachChunked(f.ctx, f.items, f.concurrency, batchSize, fn)
	}
}

// RunPool 使用协程池执行 ForEach，返回 NoResultPool 和可能的错误。
func (f *ForEachBuilder[T]) RunPool(fn func(context.Context, T) error) (*NoResultPool, error) {
	return ForEachPool(f.ctx, f.items, fn, f.concurrency)
}

// ──────────────────────────── RetryBuilder ────────────────────────────

// RetryBuilder 链式构建重试操作，一个 Builder 覆盖所有 Retry 变体。
// 通过 NewRetryBuilder 创建，链式配置重试次数、退避策略、超时，最后调用 Run/RunToResult/RunSimple 执行。
//
// 使用示例：
//
//	result, err := async.NewRetryBuilder[string](ctx).
//	    WithMaxRetries(5).
//	    WithExponentialBackoff(100*time.Millisecond, 5*time.Second).
//	    WithPerCallTimeout(2 * time.Second).
//	    Run(func(ctx context.Context) (string, error) {
//	        return callExternalAPI(ctx)
//	    })
type RetryBuilder[T any] struct {
	ctx            context.Context
	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	linearBackoff  time.Duration
	perCallTimeout time.Duration
	deadline       time.Time
	useLinear      bool
}

// NewRetryBuilder 创建重试操作构建器，默认重试 3 次。
func NewRetryBuilder[T any](ctx context.Context) *RetryBuilder[T] {
	return &RetryBuilder[T]{
		ctx:        core.EnsureTraceID(ctx),
		maxRetries: 3,
	}
}

// DefaultRetryBuilder 使用 context.Background() 创建重试构建器。
func DefaultRetryBuilder[T any]() *RetryBuilder[T] {
	return NewRetryBuilder[T](context.Background())
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (r *RetryBuilder[T]) WithLoggerCh(l core.Logger) *RetryBuilder[T] {
	core.SetLogger(l)
	return r
}

// WithMaxRetries 设置最大重试次数（总执行次数 = maxRetries + 1）。
func (r *RetryBuilder[T]) WithMaxRetries(n int) *RetryBuilder[T] {
	if n >= 0 {
		r.maxRetries = n
	}
	return r
}

// WithMaxRetriesDefault 使用默认最大重试次数（3）。
func (r *RetryBuilder[T]) WithMaxRetriesDefault() *RetryBuilder[T] {
	r.maxRetries = 3
	return r
}

// WithExponentialBackoff 设置指数退避：每次失败后等待 initial * 2^attempt，上限 max。
func (r *RetryBuilder[T]) WithExponentialBackoff(initial, max time.Duration) *RetryBuilder[T] {
	r.initialBackoff = initial
	r.maxBackoff = max
	r.useLinear = false
	return r
}

// WithExponentialBackoffDefault 使用默认指数退避（100ms 起步，10s 上限）。
func (r *RetryBuilder[T]) WithExponentialBackoffDefault() *RetryBuilder[T] {
	r.initialBackoff = 100 * time.Millisecond
	r.maxBackoff = 10 * time.Second
	r.useLinear = false
	return r
}

// WithLinearBackoff 设置线性退避：每次失败后等待固定时间。
func (r *RetryBuilder[T]) WithLinearBackoff(d time.Duration) *RetryBuilder[T] {
	r.linearBackoff = d
	r.useLinear = true
	return r
}

// WithLinearBackoffDefault 使用默认线性退避（1s）。
func (r *RetryBuilder[T]) WithLinearBackoffDefault() *RetryBuilder[T] {
	r.linearBackoff = 1 * time.Second
	r.useLinear = true
	return r
}

// WithPerCallTimeout 设置每次调用的超时时间。
func (r *RetryBuilder[T]) WithPerCallTimeout(d time.Duration) *RetryBuilder[T] {
	r.perCallTimeout = d
	return r
}

// WithPerCallTimeoutDefault 使用默认每次调用超时（5s）。
func (r *RetryBuilder[T]) WithPerCallTimeoutDefault() *RetryBuilder[T] {
	r.perCallTimeout = 5 * time.Second
	return r
}

// WithDeadline 设置整个重试过程的截止时间。
func (r *RetryBuilder[T]) WithDeadline(d time.Time) *RetryBuilder[T] {
	r.deadline = d
	return r
}

// Run 执行重试，返回值和可能的错误。聚合了 RetryWithConfig / RetryWithBackoff / RetryWithLinearBackoff。
func (r *RetryBuilder[T]) Run(fn func(context.Context) (T, error)) (T, error) {
	fnWrapped := r.wrapFunc(fn)

	if r.useLinear {
		return retry.RetryWithLinearBackoff(r.ctx, fnWrapped, r.maxRetries, r.linearBackoff)
	}
	return retry.RetryWithBackoff(r.ctx, fnWrapped, r.maxRetries, r.initialBackoff, r.maxBackoff)
}

// RunToResult 执行重试，返回 core.Result[T]（错误不抛，存在 Result 中）。
func (r *RetryBuilder[T]) RunToResult(fn func(context.Context) (T, error)) core.Result[T] {
	if r.useLinear {
		return retry.RetryWithLinearBackoffResult(r.ctx, fn, r.maxRetries, r.linearBackoff)
	}
	return retry.RetryWithBackoffResult(r.ctx, fn, r.maxRetries, r.initialBackoff, r.maxBackoff)
}

// RunSimple 执行简单重试（无返回值，只关心错误）。
func (r *RetryBuilder[T]) RunSimple(fn func(context.Context) error) error {
	if r.useLinear {
		return RetryWithLinearBackoff(r.ctx, r.maxRetries, r.linearBackoff, fn)
	}
	if r.initialBackoff > 0 || r.maxBackoff > 0 {
		return RetryWithBackoff(r.ctx, r.maxRetries, r.initialBackoff, fn)
	}
	return Retry(r.ctx, r.maxRetries, fn)
}

// wrapFunc 包装 fn，叠加 perCallTimeout 和 deadline。
func (r *RetryBuilder[T]) wrapFunc(fn func(context.Context) (T, error)) func(context.Context) (T, error) {
	wrapped := fn
	if r.perCallTimeout > 0 {
		inner := wrapped
		wrapped = func(ctx context.Context) (T, error) {
			return retry.WithTimeout(ctx, r.perCallTimeout, inner)
		}
	}
	if !r.deadline.IsZero() {
		inner := wrapped
		wrapped = func(ctx context.Context) (T, error) {
			return retry.WithDeadline(ctx, r.deadline, inner)
		}
	}
	return wrapped
}

// ──────────────────────────── ReduceBuilder ────────────────────────────

// ReduceBuilder 链式构建 Reduce 操作，一个 Builder 覆盖所有 Reduce 变体。
// 通过 NewReduceBuilder 创建，链式配置并发度/超时/FailFast，最后调用 Run 执行。
//
// 使用示例：
//
//	sum, err := async.NewReduceBuilder[int, int](ctx, items).
//	    WithConcurrency(4).
//	    WithFailFast().
//	    Run(
//	        func(ctx context.Context, n int) (int, error) { return n * n, nil },
//	        0,
//	        func(acc, val int) int { return acc + val },
//	    )
type ReduceBuilder[T any, R any] struct {
	ctx         context.Context
	items       []T
	concurrency int
	timeout     time.Duration
	failFast    bool
}

// NewReduceBuilder 创建 Reduce 操作构建器。
func NewReduceBuilder[T any, R any](ctx context.Context, items []T) *ReduceBuilder[T, R] {
	return &ReduceBuilder[T, R]{
		ctx:         core.EnsureTraceID(ctx),
		items:       items,
		concurrency: core.IO(),
	}
}

// DefaultReduceBuilder 使用 context.Background() 创建 Reduce 构建器。
func DefaultReduceBuilder[T any, R any](items []T) *ReduceBuilder[T, R] {
	return NewReduceBuilder[T, R](context.Background(), items)
}

// WithLoggerCh 注入自定义日志实现，全局生效，返回构建器。
func (r *ReduceBuilder[T, R]) WithLoggerCh(l core.Logger) *ReduceBuilder[T, R] {
	core.SetLogger(l)
	return r
}

// WithConcurrency 设置 Map 阶段的并发度，n <= 0 使用默认 IO 并发度。
func (r *ReduceBuilder[T, R]) WithConcurrency(n int) *ReduceBuilder[T, R] {
	if n > 0 {
		r.concurrency = n
	}
	return r
}

// WithConcurrencyDefault 使用默认 IO 并发度。
func (r *ReduceBuilder[T, R]) WithConcurrencyDefault() *ReduceBuilder[T, R] {
	r.concurrency = core.IO()
	return r
}

// WithTimeout 设置总超时时间。
func (r *ReduceBuilder[T, R]) WithTimeout(d time.Duration) *ReduceBuilder[T, R] {
	r.timeout = d
	return r
}

// WithFailFast 启用 FailFast 模式：任一任务失败即终止。
func (r *ReduceBuilder[T, R]) WithFailFast() *ReduceBuilder[T, R] {
	r.failFast = true
	return r
}

// Run 先并发 Map 再聚合，等价于 Reduce / ReduceWithFailFast / ReduceWithTimeout / ReduceWithFFTimeout。
func (r *ReduceBuilder[T, R]) Run(mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	switch {
	case r.timeout > 0 && r.failFast:
		return ReduceWithFFTimeout(r.ctx, r.items, r.concurrency, r.timeout, mapFn, initial, reduceFn)
	case r.timeout > 0:
		return ReduceWithTimeout(r.ctx, r.items, r.concurrency, r.timeout, mapFn, initial, reduceFn)
	case r.failFast:
		return ReduceWithFailFast(r.ctx, r.items, r.concurrency, mapFn, initial, reduceFn)
	default:
		return Reduce(r.ctx, r.items, r.concurrency, mapFn, initial, reduceFn)
	}
}
