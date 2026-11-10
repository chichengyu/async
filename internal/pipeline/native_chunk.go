package pipeline

import (
	"context"
	"sync"

	"github.com/chichengyu/async/internal/core"
)

// callObservedStage publishes the stage's terminal outcome even when business
// code exits its goroutine. The outer Group still owns task completion and
// Goexit recovery; this adapter owns only the pipeline stage notification.
func callObservedStage[T any](ctx context.Context, fn func(context.Context, string, T) (T, error), stage string, item T, observer func(string, core.Result[T])) (value T, err error) {
	finishNativeStage(func() (T, error) { return safeCall(ctx, fn, stage, item) }, func(v T, e error) {
		result := core.Result[T]{Value: v, Err: e, Occupied: true}
		invokeResultCallback(observer, stage, &result)
		value, err = result.Value, result.Err
	})
	return
}

// startNativeChunk transfers its existing wait-group registration when a task
// terminates its goroutine. Ordinary tasks keep using the same chunk goroutine.
func startNativeChunk(wg *sync.WaitGroup, start, end int, run func(int)) {
	go func() {
		next := start
		defer func() {
			if next < end {
				startNativeChunk(wg, next, end, run)
			} else {
				wg.Done()
			}
		}()
		for next < end {
			j := next
			next++
			run(j)
		}
	}()
}

// finishNativeStage publishes a business Goexit before the executing goroutine
// exits. safeCall inside run retains the existing panic conversion semantics.
// Marking a returned call before finish avoids repeating a result callback
// when that callback itself terminates its goroutine.
func finishNativeStage[T any](run func() (T, error), finish func(T, error)) {
	var value T
	var err error
	returned := false
	defer func() {
		if !returned {
			err = core.ErrTaskGoexit
		}
		finish(value, err)
	}()
	value, err = run()
	returned = true
}
