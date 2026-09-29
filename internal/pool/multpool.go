package pool

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/internal/core"
)

type MultiPool[T any] struct {
	pools   []*Pool[T]
	nextIdx atomic.Uint64
}

func (mp *MultiPool[T]) ShardCount() int {
	return len(mp.pools)
}

func (mp *MultiPool[T]) GetShard(idx int) *Pool[T] {
	if idx < 0 || idx >= len(mp.pools) {
		return nil
	}
	return mp.pools[idx]
}

func (mp *MultiPool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := int(mp.nextIdx.Add(1)-1) % len(mp.pools)
	return mp.pools[idx].Submit(ctx, fn)
}

func (mp *MultiPool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := int(mp.nextIdx.Add(1)-1) % len(mp.pools)
	return mp.pools[idx].TrySubmit(ctx, fn)
}

func (mp *MultiPool[T]) SubmitKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := int(key % uint64(len(mp.pools)))
	return mp.pools[idx].Submit(ctx, fn)
}

func (mp *MultiPool[T]) TrySubmitKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := int(key % uint64(len(mp.pools)))
	return mp.pools[idx].TrySubmit(ctx, fn)
}

func (mp *MultiPool[T]) SubmitBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) []SubmitResult {
	results := make([]SubmitResult, len(items))
	for i, item := range items {
		idx := int(mp.nextIdx.Add(1)-1) % len(mp.pools)
		err := mp.pools[idx].Submit(ctx, func(ctx context.Context) (T, error) {
			return fn(ctx, item)
		})
		results[i] = SubmitResult{Index: i, Err: err}
	}
	return results
}

func (mp *MultiPool[T]) Wait() []core.Result[T] {
	var total int
	for _, p := range mp.pools {
		total += int(p.totalResultsCount())
	}
	all := make([]core.Result[T], 0, total)
	for _, p := range mp.pools {
		all = append(all, p.Wait()...)
	}
	return all
}

func (mp *MultiPool[T]) WaitAndClose() []core.Result[T] {
	var total int
	for _, p := range mp.pools {
		total += int(p.totalResultsCount())
	}
	all := make([]core.Result[T], 0, total)
	for _, p := range mp.pools {
		all = append(all, p.WaitAndClose()...)
	}
	return all
}

func (mp *MultiPool[T]) Close() {
	for _, p := range mp.pools {
		p.Close()
	}
}

func (mp *MultiPool[T]) WithTimeout(d time.Duration) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithTimeout(d)
	}
	return mp
}

func (mp *MultiPool[T]) WithSubmitTimeout(d time.Duration) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithSubmitTimeout(d)
	}
	return mp
}

func (mp *MultiPool[T]) WithStreaming(bufSize int) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithStreaming(bufSize)
	}
	return mp
}

func (mp *MultiPool[T]) WithResultCallback(fn func(core.Result[T])) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithResultCallback(fn)
	}
	return mp
}

func (mp *MultiPool[T]) StreamResults() <-chan core.Result[T] {
	merged := make(chan core.Result[T], len(mp.pools)*256)
	var wg sync.WaitGroup
	for _, p := range mp.pools {
		ch := p.StreamResults()
		if ch != nil {
			wg.Add(1)
			go func(c <-chan core.Result[T]) {
				defer wg.Done()
				for r := range c {
					merged <- r
				}
			}(ch)
		}
	}
	go func() {
		wg.Wait()
		close(merged)
	}()
	return merged
}

func (mp *MultiPool[T]) WithRingBuffer(capacity int, overflow core.OverflowStrategy) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithRingBuffer(capacity, overflow)
	}
	return mp
}

func (mp *MultiPool[T]) WithMaxPending(n int) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithMaxPending(n)
	}
	return mp
}

func (mp *MultiPool[T]) WithOverflow(strategy core.OverflowStrategy) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithOverflow(strategy)
	}
	return mp
}

func (mp *MultiPool[T]) WithMaxResults(n int) *MultiPool[T] {
	for _, p := range mp.pools {
		p.WithMaxResults(n)
	}
	return mp
}

func (mp *MultiPool[T]) TotalActive() int {
	var total int
	for _, p := range mp.pools {
		total += p.Active()
	}
	return total
}

func (mp *MultiPool[T]) TotalBusy() int {
	var total int
	for _, p := range mp.pools {
		total += p.Busy()
	}
	return total
}

func (mp *MultiPool[T]) TotalPending() int {
	var total int
	for _, p := range mp.pools {
		total += p.Pending()
	}
	return total
}

func (mp *MultiPool[T]) TotalWorkerCount() int {
	var total int
	for _, p := range mp.pools {
		total += p.Size()
	}
	return total
}

func (mp *MultiPool[T]) TotalFailCount() int64 {
	var total int64
	for _, p := range mp.pools {
		total += p.FailCount()
	}
	return total
}

func (mp *MultiPool[T]) TotalSuccessCount() int64 {
	var total int64
	for _, p := range mp.pools {
		total += p.SuccessCount()
	}
	return total
}

func (mp *MultiPool[T]) TotalCount() int64 {
	var total int64
	for _, p := range mp.pools {
		total += p.TotalCount()
	}
	return total
}

func (mp *MultiPool[T]) Flush(maxPerShard int) []core.Result[T] {
	var all []core.Result[T]
	for _, p := range mp.pools {
		all = append(all, p.Flush(maxPerShard)...)
	}
	return all
}

func (p *Pool[T]) Shard(shards int) *MultiPool[T] {
	if shards <= 1 {
		return &MultiPool[T]{pools: []*Pool[T]{p}}
	}

	templateSize := p.Size()

	pools := make([]*Pool[T], shards)
	pools[0] = p

	for i := 1; i < shards; i++ {
		clone := NewPool[T](templateSize)
		clone.timeout = p.timeout
		clone.submitTimeout = p.submitTimeout
		clone.maxPending = p.maxPending
		clone.overflowStrat = p.overflowStrat
		clone.maxResults.Store(p.maxResults.Load())
		clone.ctx = p.ctx

		if p.failFast.Load() {
			clone.failFast.Store(true)
			clone.cancel = p.cancel
		}

		if p.ringBufFlag.Load() && p.ringBuf != nil {
			clone.ringBuf = core.NewRingBuffer[core.Result[T]](p.ringBuf.Cap(), p.ringBuf.OverflowStrategy())
			clone.ringBufFlag.Store(true)
		}

		if p.streamCh != nil {
			clone.streamCh = make(chan core.Result[T], cap(p.streamCh))
		}

		clone.resultCb = p.resultCb

		if p.IsAutoScaleEnabled() && p.autoScale != nil {
			cfgCopy := *p.autoScale
			clone.EnableAutoScale(&cfgCopy)
		}

		pools[i] = clone
	}

	return &MultiPool[T]{pools: pools}
}

func (p *Pool[T]) DefaultShard() *MultiPool[T] {
	shards := runtime.GOMAXPROCS(0)
	if shards < 2 {
		shards = 2
	}
	return p.Shard(shards)
}

func WithMultiPool[T any](size int, shards int, fn func(mp *MultiPool[T]) error) error {
	p := NewPool[T](size)
	mp := p.Shard(shards)
	defer mp.Close()
	return fn(mp)
}
