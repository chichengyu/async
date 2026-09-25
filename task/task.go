// Package task 提供单个异步任务和带 cancel 功能的 AsyncResult。
//
// 核心类型：
//   - AsyncResult[T]：异步结果句柄，支持 Wait/WaitTimeout/Cancel/Ok/IsPanic
//   - Task[T]：可取消的异步任务，包含 Ctx、Cancel、Result
//   - TaskVoid：无返回值异步任务（在 async 层实现）
//
// 创建异步任务：
//   - Go[T](ctx, fn)：启动异步任务，返回 AsyncResult[T]
//   - GoResult[T](ctx, fn)：启动异步任务，返回 Task[T]（可取消）
//   - GoAction(ctx, fn)：无返回值异步任务
//   - GoResultAction(ctx, fn)：无返回值可取消异步任务
//
// 并发安全工具：
//   - Mu[T]：带锁的泛型切片，支持 Append/Snapshot
//
// 使用示例：
//
//	// 启动异步任务并等待结果
//	ar := task.Go(ctx, func(ctx context.Context) (string, error) {
//	    return fetchData(ctx, url)
//	})
//	// 做其他事情...
//	result, err := ar.Wait()
//
//	// 带超时等待
//	result, err, ok := ar.WaitTimeout(5 * time.Second)
//	if !ok {
//	    log.Println("任务超时")
//	}
package task

import (
	"context"
	"sync"
	"time"

	"github.com/chichengyu/async/core"
)

// Task 表示一个可取消的异步任务。
// 与 AsyncResult 不同，Task 提供 Cancel 方法可主动取消任务。
//
// 使用示例：
//
//	t := task.GoResult(ctx, func(ctx context.Context) (*Data, error) {
//	    return longRunningTask(ctx)
//	})
//	// 超时后取消
//	time.Sleep(5 * time.Second)
//	t.Cancel()
//	result, err := t.Result()
type Task[T any] struct {
	Ctx    context.Context    // 任务上下文（可取消）
	Cancel context.CancelFunc // 取消函数
	Result func() (T, error)  // 获取结果的函数
}

// AsyncResult 持有一个 results chan（只读），通过它等待并获取任务结果。
// 内部有缓存机制，多次调用 Wait 安全且不会重复读取 channel。
//
// 使用示例：
//
//	ar := task.Go(ctx, func(ctx context.Context) (int, error) {
//	    return doWork(ctx)
//	})
//	// 并发安全地多次等待
//	go func() { v1, _ := ar.Wait() }()
//	go func() { v2, _ := ar.Wait() }()
type AsyncResult[T any] struct {
	results <-chan core.Result[T] // 结果 channel（只读）
	mu      sync.Mutex            // 保护 result 字段
	ready   chan struct{}         // 结果就绪信号
	result  core.Result[T]        // 缓存的结果
	once    sync.Once             // 保证只从 channel 读取一次，消除 startReader goroutine
}

func (ar *AsyncResult[T]) getResult() core.Result[T] {
	ar.once.Do(func() {
		if ar.results != nil {
			r := <-ar.results
			ar.mu.Lock()
			ar.result = r
			ar.mu.Unlock()
		}
		select {
		case <-ar.ready:
		default:
			close(ar.ready)
		}
	})
	<-ar.ready
	ar.mu.Lock()
	r := ar.result
	ar.mu.Unlock()
	return r
}

// Wait 阻塞等待任务完成，返回值和错误。
// 多个 goroutine 可同时调用 Wait，结果是安全的。
//
// 使用示例：
//
//	ar := task.Go(ctx, fn)
//	value, err := ar.Wait()
//	if err != nil {
//	    log.Printf("任务失败: %v", err)
//	}
func (ar *AsyncResult[T]) Wait() (T, error) {
	r := ar.getResult()
	return r.Value, r.Err
}

// WaitCh 返回只读结果 channel，可用于 select 多路复用。
//
// 使用示例：
//
//	select {
//	case r := <-ar.WaitCh():
//	    fmt.Println("任务完成:", r.Value)
//	case <-ctx.Done():
//	    fmt.Println("上下文取消")
//	}
func (ar *AsyncResult[T]) WaitCh() <-chan core.Result[T] {
	ch := make(chan core.Result[T], 1)
	go func() {
		r := ar.getResult()
		ch <- r
		close(ch)
	}()
	return ch
}

// Cancel 尝试取消等待：如果结果已就绪则返回结果，否则返回 context.Canceled 错误。
//
// 使用示例：
//
//	ar := task.Go(ctx, slowFn)
//	time.Sleep(100 * time.Millisecond)
//	if val, err := ar.Cancel(); err != nil {
//	    fmt.Println("任务被取消")
//	}
func (ar *AsyncResult[T]) Cancel() (T, error) {
	go ar.getResult()
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

// Ok 阻塞等待并返回任务是否成功（无错误无 panic）。
func (ar *AsyncResult[T]) Ok() bool {
	r := ar.getResult()
	return r.Err == nil
}

// IsPanic 阻塞等待并返回错误是否为 panic 导致。
func (ar *AsyncResult[T]) IsPanic() bool {
	r := ar.getResult()
	return r.IsPanic()
}

// WaitTimeout 带超时等待，返回 (值, 错误, 是否在超时前完成)。
//
// 参数：
//   - timeout：最长等待时间
//
// 使用示例：
//
//	ar := task.Go(ctx, fn)
//	val, err, ok := ar.WaitTimeout(3 * time.Second)
//	if !ok {
//	    fmt.Println("任务超时，已丢弃结果")
//	    return
//	}
func (ar *AsyncResult[T]) WaitTimeout(timeout time.Duration) (T, error, bool) {
	go ar.getResult()
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

// Go 启动一个异步任务，返回 AsyncResult[T]。
// 自动捕获 panic 并包装为 PanicError。
// ctx 会自动注入 trace_id（通过 EnsureTraceID）。
//
// 参数：
//   - ctx：上下文，自动注入 trace_id
//   - fn：异步执行的函数
//
// 使用示例：
//
//	// 启动异步 HTTP 请求
//	ar := task.Go(ctx, func(ctx context.Context) (*http.Response, error) {
//	    req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
//	    return http.DefaultClient.Do(req)
//	})
//	// 等待结果
//	resp, err := ar.Wait()
func Go[T any](ctx context.Context, fn func(context.Context) (T, error)) *AsyncResult[T] {
	ctx = core.EnsureTraceID(ctx)
	results := make(chan core.Result[T], 1)
	ar := &AsyncResult[T]{results: results, ready: make(chan struct{})}
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

// GoResult 启动一个可取消的异步任务，返回 Task[T]。
// 与 Go 的区别：返回 Task 包含 Cancel 方法，可主动取消。
//
// 参数：
//   - ctx：上下文，自动注入 trace_id
//   - fn：异步执行的函数
//
// 使用示例：
//
//	// 启动可取消的长任务
//	t := task.GoResult(ctx, func(ctx context.Context) ([]Item, error) {
//	    return fetchItems(ctx)
//	})
//	// 5秒后取消
//	time.AfterFunc(5*time.Second, t.Cancel)
//	items, err := t.Result()
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

// NoResult 空结构体，用于无返回值的异步任务。
type NoResult struct{}

// TaskNoResult 无返回值 Task 的类型别名。
type TaskNoResult = Task[NoResult]

// AsyncResultNoResult 无返回值 AsyncResult 的类型别名。
type AsyncResultNoResult = AsyncResult[NoResult]

// GoAction 启动一个无返回值的异步任务，返回 AsyncResult[NoResult]。
//
// 参数：
//   - ctx：上下文，自动注入 trace_id
//   - fn：异步执行的函数，只返回 error
//
// 使用示例：
//
//	ar := task.GoAction(ctx, func(ctx context.Context) error {
//	    return sendNotification(ctx, userID, msg)
//	})
//	_, err := ar.Wait() // 忽略 NoResult 值，只关心 error
func GoAction(ctx context.Context, fn func(context.Context) error) *AsyncResult[NoResult] {
	ctx = core.EnsureTraceID(ctx)
	results := make(chan core.Result[NoResult], 1)
	ar := &AsyncResult[NoResult]{results: results, ready: make(chan struct{})}
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

// GoResultAction 启动一个无返回值的可取消异步任务，返回 Task[NoResult]。
//
// 参数：
//   - ctx：上下文，自动注入 trace_id
//   - fn：异步执行的函数，只返回 error
//
// 使用示例：
//
//	// 启动可取消的后台任务
//	t := task.GoResultAction(ctx, func(ctx context.Context) error {
//	    return uploadFile(ctx, filepath)
//	})
//	// 可随时取消
//	time.AfterFunc(10*time.Second, t.Cancel)
//	_, err := t.Result()
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

// ──────────────────────────── Mu - 并发安全切片 ────────────────────────────

// Mu 泛型并发安全切片，支持并发追加和快照。
// 适用于多个 goroutine 并发收集结果的场景。
//
// 使用示例：
//
//	var results task.Mu[string]
//	var wg sync.WaitGroup
//	for i := 0; i < 10; i++ {
//	    wg.Add(1)
//	    go func(n int) {
//	        defer wg.Done()
//	        results.Append(func() string { return fmt.Sprintf("result-%d", n) })
//	    }(i)
//	}
//	wg.Wait()
//	all := results.Snapshot() // 获取所有结果的副本
type Mu[T any] struct {
	mu sync.Mutex // 保护 ts 的互斥锁
	ts []T        // 内部切片
}

// Append 线程安全地追加元素。add 函数在锁内执行，保证原子性。
//
// 参数：
//   - add：生成要追加元素的函数，在锁内执行
//
// 使用示例：
//
//	var mu task.Mu[int]
//	mu.Append(func() int { return 42 })
//	mu.Append(func() int { return computeSomething() })
func (m *Mu[T]) Append(add func() T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ts = append(m.ts, add())
}

// Snapshot 返回当前所有元素的副本，线程安全。
//
// 使用示例：
//
//	items := mu.Snapshot()
//	for _, item := range items {
//	    fmt.Println(item)
//	}
func (m *Mu[T]) Snapshot() []T {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]T, len(m.ts))
	copy(out, m.ts)
	return out
}

// ──────────────────────────── BoundedRunner ────────────────────────────

// BoundedRunner 限制并发 goroutine 数的异步任务执行器。
// 通过内部信号量控制最大并发 goroutine 数，用 task.Go() 的 API 体验扛千万级高并发。
//
// 与裸 task.Go() 的区别：
//   - task.Go()：每次调用创建一个新 goroutine，1 千万调用 = 1 千万 goroutine
//   - BoundedRunner.Go()：最多同时运行 max 个 goroutine，其余阻塞等待，公平调度
//
// 使用示例：
//
//	// 限制最多 1000 个并发 goroutine
//	runner := task.NewBoundedRunner(1000)
//
//	for i := 0; i < 10_000_000; i++ {
//	    idx := i
//	    task.BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
//	        return processData(ctx, idx)
//	    })
//	}
type BoundedRunner struct {
	sem chan struct{}
}

// NewBoundedRunner 创建一个限制并发 goroutine 数的执行器。
// max <= 0 时使用默认 IO 并发度。
func NewBoundedRunner(max int) *BoundedRunner {
	if max <= 0 {
		max = core.IO()
	}
	return &BoundedRunner{sem: make(chan struct{}, max)}
}

// NewDefaultBoundedRunner 使用默认 IO 并发度创建执行器。
func NewDefaultBoundedRunner() *BoundedRunner {
	return NewBoundedRunner(core.IO())
}

// Max 返回最大并发 goroutine 数。
func (r *BoundedRunner) Max() int {
	return cap(r.sem)
}

// Available 返回当前可用的并发槽位数。
func (r *BoundedRunner) Available() int {
	return cap(r.sem) - len(r.sem)
}

// Busy 返回当前正在执行任务的 goroutine 数。
func (r *BoundedRunner) Busy() int {
	return len(r.sem)
}

// BoundedGo 通过信号量限流后启动异步任务，返回 AsyncResult[T]。
// 当并发 goroutine 数已达上限时，阻塞等待 slot 释放或 ctx 取消。
//
// 重要：fn 必须正确响应 ctx.Done() 以按时释放信号量槽位。
// 如果 fn 忽略 ctx 取消而持续阻塞（例如未在 HTTP 请求中使用 ctx），
// 对应的信号量槽位将永久占用，最终耗尽所有槽位导致 BoundedRunner 完全阻塞。
// 建议在 fn 内部所有 I/O 操作中传递 ctx，或设置合理的业务超时。
func BoundedGo[T any](r *BoundedRunner, ctx context.Context, fn func(context.Context) (T, error)) *AsyncResult[T] {
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		var zero T
		ar := &AsyncResult[T]{ready: make(chan struct{})}
		ar.result = core.Result[T]{Value: zero, Err: ctx.Err()}
		close(ar.ready)
		return ar
	}

	return Go[T](ctx, func(ctx context.Context) (T, error) {
		defer func() { <-r.sem }()
		return fn(ctx)
	})
}

// BoundedGoAction 带限流的无返回值异步任务，返回 AsyncResultNoResult。
// 关于 ctx 响应的注意事项与 BoundedGo 相同。
func BoundedGoAction(r *BoundedRunner, ctx context.Context, fn func(context.Context) error) *AsyncResultNoResult {
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		ar := &AsyncResultNoResult{ready: make(chan struct{})}
		ar.result = core.Result[NoResult]{Value: NoResult{}, Err: ctx.Err()}
		close(ar.ready)
		return ar
	}

	return GoAction(ctx, func(ctx context.Context) error {
		defer func() { <-r.sem }()
		return fn(ctx)
	})
}

// BoundedGoResult 带限流的可取消异步任务，返回 Task[T]。
// 关于 ctx 响应的注意事项与 BoundedGo 相同。
func BoundedGoResult[T any](r *BoundedRunner, ctx context.Context, fn func(context.Context) (T, error)) Task[T] {
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		var zero T
		ctx, cancel := context.WithCancel(ctx)
		return Task[T]{
			Ctx:    ctx,
			Cancel: cancel,
			Result: func() (T, error) { return zero, ctx.Err() },
		}
	}

	t := GoResult[T](ctx, fn)
	orig := t.Result
	t.Result = func() (T, error) {
		defer func() { <-r.sem }()
		return orig()
	}
	return t
}
