package maps

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
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
	if r.policy.Pool() != nil || r.policy.IsPoolOwned() {
		entries := make([]MapEntry[K, V], 0, len(r.data))
		for k, v := range r.data {
			entries = append(entries, MapEntry[K, V]{Key: k, Val: v})
		}
		total, failCnt, firstErr, _ = sliceops.NewRunner[MapEntry[K, V], R](r.ctx, entries, r.policy).Each(
			func(ctx context.Context, entry MapEntry[K, V]) error { return fn(ctx, entry.Key, entry.Val) })
		return
	}
	if d := r.policy.GetTimeout(); d > 0 {
		callback := fn
		fn = func(ctx context.Context, k K, v V) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return callback(ctx, k, v)
		}
	}
	if len(r.data) == 0 {
		return 0, 0, nil
	}
	callback := fn
	fn = func(ctx context.Context, k K, v V) error {
		return core.SafeCallVoid(ctx, MapEntry[K, V]{Key: k, Val: v}, func(ctx context.Context, e MapEntry[K, V]) error { return callback(ctx, e.Key, e.Val) })
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
	shardCount := len(entries)
	if shards := r.policy.GetShards(); shards > 0 && shards < shardCount {
		shardCount = shards
	}
	base, extra := len(entries)/shardCount, len(entries)%shardCount
	if workers > shardCount {
		workers = shardCount
	}
	ch := make(chan int, shardCount)
	for shard := 0; shard < shardCount; shard++ {
		ch <- shard
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
			for shard := range ch {
				start := shard*base + min(shard, extra)
				end := start + base
				if shard < extra {
					end++
				}
				for _, e := range entries[start:end] {
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
						stop := failFastTriggered
						mu.Unlock()
						if stop {
							return
						}
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
	if r.policy.Pool() != nil || r.policy.IsPoolOwned() {
		entries := make([]MapEntry[K, V], 0, len(r.data))
		for k, v := range r.data {
			entries = append(entries, MapEntry[K, V]{Key: k, Val: v})
		}
		results, firstErr := sliceops.NewRunner[MapEntry[K, V], R](r.ctx, entries, r.policy).Map(
			func(ctx context.Context, entry MapEntry[K, V]) (R, error) { return fn(ctx, entry.Key, entry.Val) })
		values := make(map[K]R, len(entries))
		for i, result := range results {
			if result.Err != nil {
				if firstErr == nil {
					firstErr = result.Err
				}
			} else {
				values[entries[i].Key] = result.Value
			}
		}
		return NewMapResult(values, firstErr)
	}
	fn = r.timedMap(fn)
	callback := fn
	fn = func(ctx context.Context, k K, v V) (R, error) {
		return core.SafeCall(ctx, MapEntry[K, V]{Key: k, Val: v}, func(ctx context.Context, e MapEntry[K, V]) (R, error) { return callback(ctx, e.Key, e.Val) })
	}
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
	shardCount := len(entries)
	if shards := r.policy.GetShards(); shards > 0 && shards < shardCount {
		shardCount = shards
	}
	base, extra := len(entries)/shardCount, len(entries)%shardCount
	if workers > shardCount {
		workers = shardCount
	}
	ch := make(chan int, shardCount)
	for shard := 0; shard < shardCount; shard++ {
		ch <- shard
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
			for shard := range ch {
				start := shard*base + min(shard, extra)
				end := start + base
				if shard < extra {
					end++
				}
				for _, e := range entries[start:end] {
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
	if pl, ok := r.policy.Pool().(*pool.Pool[R]); ok && pl != nil && r.policy.IsPoolOwned() {
		defer pl.Close()
	}
	if d := r.policy.GetTimeout(); d > 0 {
		callback := fn
		fn = func(ctx context.Context, k K, v V) bool {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return callback(ctx, k, v)
		}
	}
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
	if r.policy.Pool() != nil || r.policy.IsPoolOwned() {
		entries := make([]MapEntry[K, V], 0, len(r.data))
		for k, v := range r.data {
			entries = append(entries, MapEntry[K, V]{Key: k, Val: v})
		}
		return sliceops.NewRunner[MapEntry[K, V], R](r.ctx, entries, r.policy).Stream(
			func(ctx context.Context, entry MapEntry[K, V]) (R, error) { return fn(ctx, entry.Key, entry.Val) }, bufSize)
	}
	fn = r.timedMap(fn)
	callback := fn
	fn = func(ctx context.Context, k K, v V) (R, error) {
		return core.SafeCall(ctx, MapEntry[K, V]{Key: k, Val: v}, func(ctx context.Context, e MapEntry[K, V]) (R, error) { return callback(ctx, e.Key, e.Val) })
	}
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
				if r.ctx.Err() != nil {
					return
				}
				val, err := fn(r.ctx, e.Key, e.Val)
				select {
				case ch <- core.Result[R]{Value: val, Err: err}:
				case <-r.ctx.Done():
					return
				}
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
		shardCount := len(entries)
		if shards := r.policy.GetShards(); shards > 0 && shards < shardCount {
			shardCount = shards
		}
		base, extra := len(entries)/shardCount, len(entries)%shardCount
		if workers > shardCount {
			workers = shardCount
		}
		taskCh := make(chan int, shardCount)
		for shard := 0; shard < shardCount; shard++ {
			taskCh <- shard
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
				for shard := range taskCh {
					start := shard*base + min(shard, extra)
					end := start + base
					if shard < extra {
						end++
					}
					for _, e := range entries[start:end] {
						if r.ctx.Err() != nil {
							return
						}
						mu.Lock()
						if failFastDone {
							mu.Unlock()
							return
						}
						mu.Unlock()
						val, err := fn(r.ctx, e.Key, e.Val)
						select {
						case ch <- core.Result[R]{Value: val, Err: err}:
						case <-r.ctx.Done():
							return
						}
						if err != nil && r.policy.IsFailFast() {
							mu.Lock()
							failFastDone = true
							mu.Unlock()
							return
						}
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
	if pl, ok := r.policy.Pool().(*pool.Pool[R]); ok && pl != nil && r.policy.IsPoolOwned() {
		defer pl.Close()
	}
	if d := r.policy.GetTimeout(); d > 0 {
		callback := fn
		fn = func(ctx context.Context, acc R, k K, v V) (R, error) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return callback(ctx, acc, k, v)
		}
	}
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
	if r.policy.Pool() != nil || r.policy.IsPoolOwned() {
		entries := make([]MapEntry[K, V], 0, len(r.data))
		for k, v := range r.data {
			entries = append(entries, MapEntry[K, V]{Key: k, Val: v})
		}
		size := r.policy.GetChunkSize()
		if size <= 0 {
			size = 1
		}
		chunks := chunkMapEntries(entries, size)
		results, firstErr := sliceops.NewRunner[[]MapEntry[K, V], R](r.ctx, chunks, r.policy).Map(fn)
		values := make(map[K]R, len(chunks))
		for i, result := range results {
			if result.Err != nil {
				if firstErr == nil {
					firstErr = result.Err
				}
			} else {
				values[chunks[i][0].Key] = result.Value
			}
		}
		return NewMapResult(values, firstErr)
	}
	if d := r.policy.GetTimeout(); d > 0 {
		callback := fn
		fn = func(ctx context.Context, entries []MapEntry[K, V]) (R, error) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return callback(ctx, entries)
		}
	}
	callback := fn
	fn = func(ctx context.Context, entries []MapEntry[K, V]) (R, error) {
		return core.SafeCall(ctx, entries, callback)
	}
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
	shardCount := len(chunks)
	if shards := r.policy.GetShards(); shards > 0 && shards < shardCount {
		shardCount = shards
	}
	base, extra := len(chunks)/shardCount, len(chunks)%shardCount
	if workers > shardCount {
		workers = shardCount
	}
	ch := make(chan int, shardCount)
	for shard := 0; shard < shardCount; shard++ {
		ch <- shard
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
			for shard := range ch {
				start := shard*base + min(shard, extra)
				end := start + base
				if shard < extra {
					end++
				}
				for _, chunk := range chunks[start:end] {
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
			}
		}()
	}
	wg.Wait()
	return NewMapResult(result, firstErr)
}

// ForEachBatch chunks map entries and applies fn to each chunk.
func (r *MapRunner[K, V, R]) ForEachBatch(fn func(context.Context, []MapEntry[K, V]) error) (total int64, failCnt int64, firstErr error) {
	if r.policy.Pool() != nil || r.policy.IsPoolOwned() {
		entries := make([]MapEntry[K, V], 0, len(r.data))
		for k, v := range r.data {
			entries = append(entries, MapEntry[K, V]{Key: k, Val: v})
		}
		size := r.policy.GetChunkSize()
		if size <= 0 {
			size = 1
		}
		chunks := chunkMapEntries(entries, size)
		results, err := sliceops.NewRunner[[]MapEntry[K, V], R](r.ctx, chunks, r.policy).Map(
			func(ctx context.Context, chunk []MapEntry[K, V]) (R, error) { var zero R; return zero, fn(ctx, chunk) })
		firstErr = err
		for i, result := range results {
			total += int64(len(chunks[i]))
			if result.Err != nil {
				failCnt += int64(len(chunks[i]))
				if firstErr == nil {
					firstErr = result.Err
				}
			}
		}
		return
	}
	if d := r.policy.GetTimeout(); d > 0 {
		callback := fn
		fn = func(ctx context.Context, entries []MapEntry[K, V]) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return callback(ctx, entries)
		}
	}
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
			if err := core.SafeCallVoid(r.ctx, chunk, fn); err != nil {
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
	shardCount := len(chunks)
	if shards := r.policy.GetShards(); shards > 0 && shards < shardCount {
		shardCount = shards
	}
	base, extra := len(chunks)/shardCount, len(chunks)%shardCount
	if workers > shardCount {
		workers = shardCount
	}
	ch := make(chan int, shardCount)
	for shard := 0; shard < shardCount; shard++ {
		ch <- shard
	}
	close(ch)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for shard := range ch {
				start := shard*base + min(shard, extra)
				end := start + base
				if shard < extra {
					end++
				}
				for _, chunk := range chunks[start:end] {
					mu.Lock()
					if failFastTriggered {
						mu.Unlock()
						return
					}
					mu.Unlock()
					atomic.AddInt64(&total, int64(len(chunk)))
					err := func() (err error) {
						defer func() {
							if recovered := recover(); recovered != nil {
								err = core.NewPanicError(recovered)
							}
						}()
						return fn(r.ctx, chunk)
					}()
					if err != nil {
						atomic.AddInt64(&failCnt, int64(len(chunk)))
						mu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						if r.policy.IsFailFast() {
							failFastTriggered = true
						}
						stop := failFastTriggered
						mu.Unlock()
						if stop {
							return
						}
					}
				}
			}
		}()
	}
	wg.Wait()
	return total, failCnt, firstErr
}

func (r *MapRunner[K, V, R]) timedMap(fn func(context.Context, K, V) (R, error)) func(context.Context, K, V) (R, error) {
	if d := r.policy.GetTimeout(); d > 0 {
		return func(ctx context.Context, k K, v V) (R, error) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return fn(ctx, k, v)
		}
	}
	return fn
}

// chunkMapEntries splits entries into chunks of the given size.
func chunkMapEntries[K comparable, V any](entries []MapEntry[K, V], size int) [][]MapEntry[K, V] {
	return sliceops.Chunk(entries, size)
}

type MapEntry[K comparable, V any] struct {
	Key K
	Val V
}
