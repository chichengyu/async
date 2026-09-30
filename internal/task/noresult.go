package task

import (
	"context"

	"github.com/chichengyu/async/internal/core"
)

// ──────────────────────────── NoResult variants ────────────────────────────

// NoResult 空结构体，用于无返回值的异步任务。
type NoResult struct{}

// TaskNoResult 无返回值 Task 的类型别名。
type TaskNoResult = Task[NoResult]

// AsyncResultNoResult 无返回值 AsyncResult 的类型别名。
type AsyncResultNoResult = AsyncResult[NoResult]

// GoAct 启动一个无返回值的异步任务，返回 AsyncResult[NoResult]。
//
// 参数：
//   - ctx：上下文，自动注入 trace_id
//   - fn：异步执行的函数，只返回 error
//
// 使用示例：
//
//	ar := task.GoAct(ctx, func(ctx context.Context) error {
//	    return sendNotification(ctx, userID, msg)
//	})
//	_, err := ar.Wait() // 忽略 NoResult 值，只关心 error
func GoAct(ctx context.Context, fn func(context.Context) error) *AsyncResult[NoResult] {
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

// GoResultAct 启动一个无返回值的可取消异步任务，返回 Task[NoResult]。
//
// 参数：
//   - ctx：上下文，自动注入 trace_id
//   - fn：异步执行的函数，只返回 error
//
// 使用示例：
//
//	// 启动可取消的后台任务
//	t := task.GoResultAct(ctx, func(ctx context.Context) error {
//	    return uploadFile(ctx, filepath)
//	})
//	// 可随时取消
//	time.AfterFunc(10*time.Second, t.Cancel)
//	_, err := t.Result()
func GoResultAct(ctx context.Context, fn func(context.Context) error) Task[NoResult] {
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
