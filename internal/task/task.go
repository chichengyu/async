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
//   - GoAct(ctx, fn)：无返回值异步任务
//   - GoResultAct(ctx, fn)：无返回值可取消异步任务
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

	"github.com/chichengyu/async/internal/core"
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

// Error 阻塞等待任务完成，只返回错误（成功时为 nil）。
func (t Task[T]) Error() error {
	_, err := t.Result()
	return err
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
	results   <-chan core.Result[T] // 结果 channel（只读）
	mu        sync.Mutex            // 保护 result / populated 字段
	ready     chan struct{}         // 结果就绪信号
	result    core.Result[T]        // 缓存的结果
	once      sync.Once             // 保证只从 channel 读取一次
	populated bool                  // result 是否已从 channel 成功读取
}

func (ar *AsyncResult[T]) getResult() core.Result[T] {
	ar.once.Do(func() {
		ar.mu.Lock()
		if !ar.populated && ar.results != nil {
			ar.mu.Unlock()
			r := <-ar.results
			ar.mu.Lock()
			ar.result = r
			ar.populated = true
			ar.mu.Unlock()
		} else {
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

// Values 是 Wait 的别名，统一命名风格。
// 阻塞等待任务完成，返回值和错误。
func (ar *AsyncResult[T]) Values() (T, error) {
	return ar.Wait()
}

// Error 阻塞等待任务完成，只返回错误（成功时为 nil）。
func (ar *AsyncResult[T]) Error() error {
	r := ar.getResult()
	return r.Err
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
	// 步骤 1：快速路径 — 检查 ready 是否已关闭（结果已缓存）
	select {
	case <-ar.ready:
		ar.mu.Lock()
		r := ar.result
		ar.mu.Unlock()
		return r.Value, r.Err
	default:
	}

	// 步骤 2：检查 populated — Cancel 或 WaitTimeout 之前已经非阻塞读到了结果
	ar.mu.Lock()
	if ar.populated {
		r := ar.result
		ar.mu.Unlock()
		return r.Value, r.Err
	}

	// 步骤 3：非阻塞尝试从 channel 读取结果（绕过 sync.Once）
	if ar.results != nil {
		select {
		case r, ok := <-ar.results:
			if ok {
				ar.result = r
				ar.populated = true
				ar.mu.Unlock()
				// 关闭 ready，通知所有 Wait 调用者结果已就绪
				select {
				case <-ar.ready:
				default:
					close(ar.ready)
				}
				return r.Value, r.Err
			}
		default:
		}
	}
	ar.mu.Unlock()

	// 步骤 4：短暂等待（1ms），给 goroutine 一个完成窗口再判定
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	go ar.getResult()
	select {
	case <-ar.ready:
		ar.mu.Lock()
		r := ar.result
		ar.mu.Unlock()
		return r.Value, r.Err
	case <-timer.C:
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
	// 步骤 1：快速路径 — ready 已关闭，结果已缓存
	select {
	case <-ar.ready:
		ar.mu.Lock()
		r := ar.result
		ar.mu.Unlock()
		return r.Value, r.Err, true
	default:
	}

	// 步骤 2：检查 populated — 之前 Cancel/WaitTimeout 已经读到结果
	ar.mu.Lock()
	if ar.populated {
		r := ar.result
		ar.mu.Unlock()
		return r.Value, r.Err, true
	}

	// 步骤 3：非阻塞尝试从 channel 读取
	if ar.results != nil {
		select {
		case r, ok := <-ar.results:
			if ok {
				ar.result = r
				ar.populated = true
				ar.mu.Unlock()
				select {
				case <-ar.ready:
				default:
					close(ar.ready)
				}
				return r.Value, r.Err, true
			}
		default:
		}
	}
	ar.mu.Unlock()

	// 步骤 4：阻塞等待结果或超时
	go ar.getResult()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ar.ready:
		ar.mu.Lock()
		r := ar.result
		ar.mu.Unlock()
		return r.Value, r.Err, true
	case <-timer.C:
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
				r := core.Result[T]{Value: zero, Err: core.NewPanicError(r)}
				ar.mu.Lock()
				ar.result = r
				ar.populated = true
				ar.mu.Unlock()
				results <- r
			}
			close(results)
		}()
		val, err := fn(ctx)
		r := core.Result[T]{Value: val, Err: err}
		ar.mu.Lock()
		ar.result = r
		ar.populated = true
		ar.mu.Unlock()
		results <- r
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
	var once sync.Once
	var cachedVal T
	var cachedErr error
	return Task[T]{
		Ctx:    ctx,
		Cancel: cancel,
		Result: func() (T, error) {
			once.Do(func() {
				r := <-results
				cachedVal = r.Value
				cachedErr = r.Err
			})
			return cachedVal, cachedErr
		},
	}
}
