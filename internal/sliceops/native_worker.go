package sliceops

import (
	"sync"

	"github.com/chichengyu/async/internal/core"
)

// startNativeWorker hands the existing registration to a successor if a task
// exits the goroutine. next must advance its cursor before returning a task.
// Ordinary tasks continue on the same goroutine.
func startNativeWorker(wg *sync.WaitGroup, next func() (int, bool), run func(int)) {
	go func() {
		finished := false
		defer func() {
			if finished {
				wg.Done()
			} else {
				startNativeWorker(wg, next, run)
			}
		}()
		for {
			j, ok := next()
			if !ok {
				finished = true
				return
			}
			run(j)
		}
	}()
}

// shardCursor preserves the current shard when its worker must be replaced.
func shardCursor(nextShard func() int, shards, base, extra int) func() (int, bool) {
	next, end := 0, 0
	return func() (int, bool) {
		if next == end {
			shard := nextShard()
			if shard >= shards {
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

// finishNativeTask publishes a business Goexit while its goroutine unwinds.
// run retains each caller's existing panic conversion and timeout semantics.
func finishNativeTask[R any](run func() (R, error), finish func(R, error)) {
	var value R
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
