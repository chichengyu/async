package maps

import (
	"sync"

	"github.com/chichengyu/async/internal/core"
)

// Each cursor belongs to one worker registration and survives a business Goexit.
func mapShardCursor(shardCh <-chan int, base, extra int) func() (int, bool) {
	next, end := 0, 0
	return func() (int, bool) {
		if next == end {
			shard, ok := <-shardCh
			if !ok {
				return 0, false
			}
			next = shard*base + min(shard, extra)
			end = next + base
			if shard < extra {
				end++
			}
		}
		j := next
		next++
		return j, true
	}
}

// startMapWorker keeps ordinary tasks on the same goroutine. A Goexit hands
// the existing wait-group registration and cursor to a successor. A panic
// outside the business protection retains the caller's previous handler.
func startMapWorker(wg *sync.WaitGroup, next func() (int, bool), run func(int) bool, onPanic func(any)) {
	go func() {
		finished := false
		defer func() {
			if recovered := recover(); recovered != nil {
				defer wg.Done()
				if onPanic == nil {
					panic(recovered)
				}
				onPanic(recovered)
				return
			}
			if finished {
				wg.Done()
			} else {
				startMapWorker(wg, next, run, onPanic)
			}
		}()
		for {
			j, ok := next()
			if !ok || !run(j) {
				finished = true
				return
			}
		}
	}()
}

func finishMapTask[R any](run func() (R, error), finish func(R, error)) {
	var value R
	var err error
	returned := false
	defer func() {
		if recovered := recover(); recovered != nil {
			panic(recovered)
		}
		if !returned {
			err = core.ErrTaskGoexit
		}
		finish(value, err)
	}()
	value, err = run()
	returned = true
}
