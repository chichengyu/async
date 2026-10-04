package maps

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/sliceops"
)

// MapRunner is the unified executor for concurrent map operations.
type MapRunner[K comparable, V any, R any] struct {
	ctx    context.Context
	data   map[K]V
	policy sliceops.Policy
}

func NewMapRunner[K comparable, V any, R any](ctx context.Context, data map[K]V, policy sliceops.Policy) *MapRunner[K, V, R] {
	return &MapRunner[K, V, R]{ctx: core.EnsureTraceID(ctx), data: data, policy: policy}
}

// ForEach executes fn concurrently for each key-value pair.
func (r *MapRunner[K, V, R]) ForEach(fn func(context.Context, K, V) error) (total int64, failCnt int64, firstErr error) {
	if len(r.data) == 0 {
		return 0, 0, nil
	}
	entries := make([]MapEntry[K, V], 0, len(r.data))
	for k, v := range r.data {
		entries = append(entries, MapEntry[K, V]{Key: k, Val: v})
	}
	if r.policy.IsSerial() {
		for _, e := range entries {
			atomic.AddInt64(&total, 1)
			if err := fn(r.ctx, e.Key, e.Val); err != nil {
				atomic.AddInt64(&failCnt, 1)
				if firstErr == nil {
					firstErr = err
				}
				if r.policy.IsFailFast() {
					return total, failCnt, firstErr
				}
			}
		}
		return total, failCnt, firstErr
	}
	workers := r.policy.GetWorker()
	if workers <= 0 {
		workers = core.IO()
	}
	if len(entries) < workers {
		workers = len(entries)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	failFastTriggered := false
	ch := make(chan MapEntry[K, V], len(entries))
	for _, e := range entries {
		ch <- e
	}
	close(ch)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt64(&failCnt, 1)
					mu.Lock()
					if firstErr == nil {
						firstErr = core.NewPanicError(r)
					}
					failFastTriggered = true
					mu.Unlock()
				}
			}()
			for e := range ch {
				mu.Lock()
				if failFastTriggered {
					mu.Unlock()
					return
				}
				mu.Unlock()
				atomic.AddInt64(&total, 1)
				if err := fn(r.ctx, e.Key, e.Val); err != nil {
					atomic.AddInt64(&failCnt, 1)
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					if r.policy.IsFailFast() {
						failFastTriggered = true
					}
					mu.Unlock()
					if failFastTriggered {
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	return total, failCnt, firstErr
}

// Map transforms each key-value pair and returns *MapResult with embedded error.
// Mirrors SliceBuilder.Map: error is inside the result, access via .Error() / .Err().
func (r *MapRunner[K, V, R]) Map(fn func(context.Context, K, V) (R, error)) *MapResult[K, R] {
	result := make(map[K]R, len(r.data))
	if len(r.data) == 0 {
		return NewMapResult(result, nil)
	}
	entries := make([]MapEntry[K, V], 0, len(r.data))
	for k, v := range r.data {
		entries = append(entries, MapEntry[K, V]{Key: k, Val: v})
	}

	if r.policy.IsSerial() {
		var firstErr error
		for _, e := range entries {
			val, err := fn(r.ctx, e.Key, e.Val)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				if r.policy.IsFailFast() {
					return NewMapResult(result, firstErr)
				}
				continue
			}
			result[e.Key] = val
		}
		return NewMapResult(result, firstErr)
	}

	workers := r.policy.GetWorker()
	if workers <= 0 {
		workers = core.IO()
	}
	if len(entries) < workers {
		workers = len(entries)
	}
	var mu sync.Mutex
	var firstErr error
	ch := make(chan MapEntry[K, V], len(entries))
	for _, e := range entries {
		ch <- e
	}
	close(ch)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = core.NewPanicError(r)
					}
					mu.Unlock()
				}
			}()
			for e := range ch {
				mu.Lock()
				if firstErr != nil && r.policy.IsFailFast() {
					mu.Unlock()
					return
				}
				mu.Unlock()
				val, err := fn(r.ctx, e.Key, e.Val)
				mu.Lock()
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
				} else {
					result[e.Key] = val
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return NewMapResult(result, firstErr)
}

// MapTo transforms each key-value pair and returns a new map with separate error.
// DEPRECATED: use Map() which embeds error in MapResult, matching SliceResult.
func (r *MapRunner[K, V, R]) MapTo(fn func(context.Context, K, V) (R, error)) (*MapResult[K, R], error) {
	res := r.Map(fn)
	if res.err != nil {
		return res, res.err
	}
	return res, nil
}

// Filter keep entries where fn returns true.
func (r *MapRunner[K, V, R]) Filter(fn func(context.Context, K, V) bool) (*Map[K, V], error) {
	result := New[K, V](len(r.data))
	if len(r.data) == 0 {
		return result, nil
	}
	for k, v := range r.data {
		if fn(r.ctx, k, v) {
			result.data[k] = v
		}
	}
	return result, nil
}

// Stream streams Map results through a channel.
func (r *MapRunner[K, V, R]) Stream(fn func(context.Context, K, V) (R, error), bufSize int) <-chan core.Result[R] {
	if bufSize <= 0 {
		bufSize = r.policy.GetBuf()
		if bufSize <= 0 {
			bufSize = len(r.data)
		}
	}
	ch := make(chan core.Result[R], bufSize)
	go func() {
		defer close(ch)
		if len(r.data) == 0 {
			return
		}
		entries := make([]MapEntry[K, V], 0, len(r.data))
		for k, v := range r.data {
			entries = append(entries, MapEntry[K, V]{Key: k, Val: v})
		}
		if r.policy.IsSerial() {
			for _, e := range entries {
				val, err := fn(r.ctx, e.Key, e.Val)
				ch <- core.Result[R]{Value: val, Err: err}
				if err != nil && r.policy.IsFailFast() {
					return
				}
			}
			return
		}
		workers := r.policy.GetWorker()
		if workers <= 0 {
			workers = core.IO()
		}
		if len(entries) < workers {
			workers = len(entries)
		}
		taskCh := make(chan MapEntry[K, V], len(entries))
		for _, e := range entries {
			taskCh <- e
		}
		close(taskCh)
		var wg sync.WaitGroup
		failFastDone := false
		var mu sync.Mutex
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						mu.Lock()
						failFastDone = true
						mu.Unlock()
					}
				}()
				for e := range taskCh {
					mu.Lock()
					if failFastDone {
						mu.Unlock()
						return
					}
					mu.Unlock()
					val, err := fn(r.ctx, e.Key, e.Val)
					ch <- core.Result[R]{Value: val, Err: err}
					if err != nil && r.policy.IsFailFast() {
						mu.Lock()
						failFastDone = true
						mu.Unlock()
						return
					}
				}
			}()
		}
		wg.Wait()
	}()
	return ch
}

// Reduce aggregates all key-value pairs.
func (r *MapRunner[K, V, R]) Reduce(initial R, fn func(context.Context, R, K, V) (R, error)) (R, error) {
	acc := initial
	for k, v := range r.data {
		var err error
		acc, err = fn(r.ctx, acc, k, v)
		if err != nil {
			return acc, err
		}
	}
	return acc, nil
}

// MapBatch chunks map entries and applies fn to each chunk.
func (r *MapRunner[K, V, R]) MapBatch(fn func(context.Context, []MapEntry[K, V]) (R, error)) *MapResult[K, R] {
	result := make(map[K]R, len(r.data))
	if len(r.data) == 0 {
		return NewMapResult(result, nil)
	}
	entries := make([]MapEntry[K, V], 0, len(r.data))
	for k, v := range r.data {
		entries = append(entries, MapEntry[K, V]{Key: k, Val: v})
	}
	chunkSize := r.policy.GetChunkSize()
	if chunkSize <= 0 {
		chunkSize = 1
	}
	chunks := chunkMapEntries(entries, chunkSize)

	if r.policy.IsSerial() {
		var firstErr error
		for _, chunk := range chunks {
			val, err := fn(r.ctx, chunk)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				if r.policy.IsFailFast() {
					return NewMapResult(result, firstErr)
				}
				continue
			}
			result[chunk[0].Key] = val
		}
		return NewMapResult(result, firstErr)
	}

	workers := r.policy.GetWorker()
	if workers <= 0 {
		workers = core.IO()
	}
	if len(chunks) < workers {
		workers = len(chunks)
	}
	var mu sync.Mutex
	var firstErr error
	ch := make(chan []MapEntry[K, V], len(chunks))
	for _, c := range chunks {
		ch <- c
	}
	close(ch)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = core.NewPanicError(r)
					}
					mu.Unlock()
				}
			}()
			for chunk := range ch {
				mu.Lock()
				if firstErr != nil && r.policy.IsFailFast() {
					mu.Unlock()
					return
				}
				mu.Unlock()
				val, err := fn(r.ctx, chunk)
				mu.Lock()
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
				} else {
					result[chunk[0].Key] = val
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return NewMapResult(result, firstErr)
}

// ForEachBatch chunks map entries and applies fn to each chunk.
func (r *MapRunner[K, V, R]) ForEachBatch(fn func(context.Context, []MapEntry[K, V]) error) (total int64, failCnt int64, firstErr error) {
	if len(r.data) == 0 {
		return 0, 0, nil
	}
	entries := make([]MapEntry[K, V], 0, len(r.data))
	for k, v := range r.data {
		entries = append(entries, MapEntry[K, V]{Key: k, Val: v})
	}
	chunkSize := r.policy.GetChunkSize()
	if chunkSize <= 0 {
		chunkSize = 1
	}
	chunks := chunkMapEntries(entries, chunkSize)

	if r.policy.IsSerial() {
		for _, chunk := range chunks {
			atomic.AddInt64(&total, int64(len(chunk)))
			if err := fn(r.ctx, chunk); err != nil {
				atomic.AddInt64(&failCnt, int64(len(chunk)))
				if firstErr == nil {
					firstErr = err
				}
				if r.policy.IsFailFast() {
					return total, failCnt, firstErr
				}
			}
		}
		return total, failCnt, firstErr
	}

	workers := r.policy.GetWorker()
	if workers <= 0 {
		workers = core.IO()
	}
	if len(chunks) < workers {
		workers = len(chunks)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	failFastTriggered := false
	ch := make(chan []MapEntry[K, V], len(chunks))
	for _, c := range chunks {
		ch <- c
	}
	close(ch)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt64(&failCnt, 1)
					mu.Lock()
					if firstErr == nil {
						firstErr = core.NewPanicError(r)
					}
					failFastTriggered = true
					mu.Unlock()
				}
			}()
			for chunk := range ch {
				mu.Lock()
				if failFastTriggered {
					mu.Unlock()
					return
				}
				mu.Unlock()
				atomic.AddInt64(&total, int64(len(chunk)))
				if err := fn(r.ctx, chunk); err != nil {
					atomic.AddInt64(&failCnt, int64(len(chunk)))
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					if r.policy.IsFailFast() {
						failFastTriggered = true
					}
					mu.Unlock()
					if failFastTriggered {
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	return total, failCnt, firstErr
}

// chunkMapEntries splits entries into chunks of the given size.
func chunkMapEntries[K comparable, V any](entries []MapEntry[K, V], size int) [][]MapEntry[K, V] {
	if size <= 0 || len(entries) == 0 {
		return nil
	}
	chunks := make([][]MapEntry[K, V], 0, (len(entries)+size-1)/size)
	for i := 0; i < len(entries); i += size {
		end := i + size
		if end > len(entries) {
			end = len(entries)
		}
		chunks = append(chunks, entries[i:end])
	}
	return chunks
}

type MapEntry[K comparable, V any] struct {
	Key K
	Val V
}
