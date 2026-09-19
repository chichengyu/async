// Package mapreduce 的扩展函数：Map/ForEach/Reduce 超时、FailFast、Chunk、Chunked 变体。
package mapreduce

import (
	"context"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/group"
)

// ──────────────────────────── Map 扩展 ────────────────────────────

func DefaultMap[T any, R any](ctx context.Context, items []T, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	results, _ := Map(ctx, items, fn, core.IO())
	return results
}

func DefaultMapWithFailFast[T any, R any](ctx context.Context, items []T, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	return MapWithFailFast(ctx, items, fn, core.IO())
}

func MapWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if n == 0 {
		return nil
	}
	concurrency = core.WithConfig(concurrency)
	if concurrency > n {
		concurrency = n
	}
	if concurrency <= 0 {
		concurrency = 1
	}

	g := group.NewGroup[R](concurrency)
	g.WithTimeout(timeout)
	for idx := range items {
		g.GoAt(idx, ctx, func(ctx context.Context) (R, error) {
			return fn(ctx, items[idx])
		})
	}
	return g.Wait()
}

func DefaultMapWithTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	return MapWithTimeout(ctx, items, core.IO(), timeout, fn)
}

func MapWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if n == 0 {
		return nil, nil
	}
	concurrency = core.WithConfig(concurrency)
	if concurrency > n {
		concurrency = n
	}
	if concurrency <= 0 {
		concurrency = 1
	}

	g := group.NewGroup[R](concurrency)
	ffCtx, ffCancel := context.WithCancel(ctx)
	defer ffCancel()
	g.WithFailFast(ffCtx)
	g.WithTimeout(timeout)
	for idx := range items {
		g.GoAt(idx, ffCtx, func(ctx context.Context) (R, error) {
			return fn(ctx, items[idx])
		})
	}
	results := g.Wait()
	return results, g.FirstError()
}

func DefaultMapWithFFTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	return MapWithFFTimeout(ctx, items, core.IO(), timeout, fn)
}

// ──────────────────────────── ForEach 扩展 ────────────────────────────

func DefaultForEach[T any](ctx context.Context, items []T, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEach(ctx, items, fn, core.IO())
}

func DefaultForEachWithFailFast[T any](ctx context.Context, items []T, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachWithFailFast(ctx, items, fn, core.IO())
}

func ForEachWithTimeout[T any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if n == 0 {
		return 0, 0, nil, nil
	}
	concurrency = core.WithConfig(concurrency)
	if concurrency > n {
		concurrency = n
	}

	g := group.NewNoResult(concurrency)
	g.WithTimeout(timeout)
	for i := range items {
		g.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, items[i])
		})
	}
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

func DefaultForEachWithTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachWithTimeout(ctx, items, core.IO(), timeout, fn)
}

func ForEachWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if n == 0 {
		return 0, 0, nil, nil
	}
	concurrency = core.WithConfig(concurrency)
	if concurrency > n {
		concurrency = n
	}

	ffCtx, ffCancel := context.WithCancel(ctx)
	defer ffCancel()

	g := group.NewNoResult(concurrency)
	g.WithFailFast(ffCtx)
	g.WithTimeout(timeout)
	for i := range items {
		g.Go(ffCtx, func(ctx context.Context) error {
			return fn(ctx, items[i])
		})
	}
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

func DefaultForEachWithFFTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachWithFFTimeout(ctx, items, core.IO(), timeout, fn)
}

// ──────────────────────────── Result helpers ────────────────────────────

func ResultValues[T any](results []core.Result[T]) []T {
	vals := make([]T, 0, len(results))
	for _, r := range results {
		if r.Err == nil {
			vals = append(vals, r.Value)
		}
	}
	return vals
}

func ResultErrors[T any](results []core.Result[T]) []error {
	errs := make([]error, 0)
	for _, r := range results {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errs
}

func Every[T any](results []core.Result[T]) bool {
	for _, r := range results {
		if r.Err != nil {
			return false
		}
	}
	return true
}

func Some[T any](results []core.Result[T]) bool {
	for _, r := range results {
		if r.Err == nil {
			return true
		}
	}
	return false
}

func AnyError[T any](results []core.Result[T]) bool {
	for _, r := range results {
		if r.Err != nil {
			return true
		}
	}
	return false
}

func PartitionResults[T any](results []core.Result[T]) (successes []T, failures []error) {
	successes = make([]T, 0, len(results))
	failures = make([]error, 0)
	for _, r := range results {
		if r.Err == nil {
			successes = append(successes, r.Value)
		} else {
			failures = append(failures, r.Err)
		}
	}
	return
}

// ──────────────────────────── MapChunk ────────────────────────────

func MapChunk[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R] {
	chunks := Chunk(items, batchSize)
	results, _ := Map(ctx, chunks, fn, concurrency)
	return results
}

func DefaultMapChunk[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R] {
	return MapChunk(ctx, items, core.IO(), batchSize, fn)
}

func MapChunkWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error) {
	chunks := Chunk(items, batchSize)
	return MapWithFailFast(ctx, chunks, fn, concurrency)
}

func DefaultMapChunkWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error) {
	return MapChunkWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

func MapChunkWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R] {
	chunks := Chunk(items, batchSize)
	return MapWithTimeout(ctx, chunks, concurrency, timeout, fn)
}

func DefaultMapChunkWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R] {
	return MapChunkWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

func MapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error) {
	chunks := Chunk(items, batchSize)
	return MapWithFFTimeout(ctx, chunks, concurrency, timeout, fn)
}

func DefaultMapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error) {
	return MapChunkWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ──────────────────────────── MapChunked ────────────────────────────

func MapChunked[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	n := len(items)
	if n == 0 {
		return nil
	}
	chunks := Chunk(items, batchSize)
	g := group.NewGroup[R](concurrency)
	idx := 0
	for _, chunk := range chunks {
		for _, item := range chunk {
			item := item
			g.GoAt(idx, ctx, func(ctx context.Context) (R, error) {
				return fn(ctx, item)
			})
			idx++
		}
	}
	return g.Wait()
}

func DefaultMapChunked[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	return MapChunked(ctx, items, core.IO(), batchSize, fn)
}

func MapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	n := len(items)
	if n == 0 {
		return nil, nil
	}
	chunks := Chunk(items, batchSize)
	ffCtx, ffCancel := context.WithCancel(ctx)
	defer ffCancel()
	g := group.NewGroup[R](concurrency)
	g.WithFailFast(ffCtx)
	idx := 0
	for _, chunk := range chunks {
		for _, item := range chunk {
			item := item
			g.GoAt(idx, ffCtx, func(ctx context.Context) (R, error) {
				return fn(ctx, item)
			})
			idx++
		}
	}
	return g.Wait(), g.FirstError()
}

func DefaultMapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	return MapChunkedWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

func MapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	chunks := Chunk(items, batchSize)
	g := group.NewGroup[R](concurrency)
	g.WithTimeout(timeout)
	idx := 0
	for _, chunk := range chunks {
		for _, item := range chunk {
			item := item
			g.GoAt(idx, ctx, func(ctx context.Context) (R, error) {
				return fn(ctx, item)
			})
			idx++
		}
	}
	return g.Wait()
}

func DefaultMapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	return MapChunkedWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

func MapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	chunks := Chunk(items, batchSize)
	ffCtx, ffCancel := context.WithCancel(ctx)
	defer ffCancel()
	g := group.NewGroup[R](concurrency)
	g.WithFailFast(ffCtx)
	g.WithTimeout(timeout)
	idx := 0
	for _, chunk := range chunks {
		for _, item := range chunk {
			item := item
			g.GoAt(idx, ffCtx, func(ctx context.Context) (R, error) {
				return fn(ctx, item)
			})
			idx++
		}
	}
	return g.Wait(), g.FirstError()
}

func DefaultMapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	return MapChunkedWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ──────────────────────────── ForEachChunk ────────────────────────────

func ForEachChunk[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	chunks := Chunk(items, batchSize)
	return ForEach(ctx, chunks, func(ctx context.Context, chunk []T) error {
		return fn(ctx, chunk)
	}, concurrency)
}

func DefaultForEachChunk[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunk(ctx, items, core.IO(), batchSize, fn)
}

func ForEachChunkWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	chunks := Chunk(items, batchSize)
	return ForEachWithFailFast(ctx, chunks, func(ctx context.Context, chunk []T) error {
		return fn(ctx, chunk)
	}, concurrency)
}

func DefaultForEachChunkWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

func ForEachChunkWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	chunks := Chunk(items, batchSize)
	return ForEachWithTimeout(ctx, chunks, concurrency, timeout, func(ctx context.Context, chunk []T) error {
		return fn(ctx, chunk)
	})
}

func DefaultForEachChunkWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

func ForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	chunks := Chunk(items, batchSize)
	return ForEachWithFFTimeout(ctx, chunks, concurrency, timeout, func(ctx context.Context, chunk []T) error {
		return fn(ctx, chunk)
	})
}

func DefaultForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ──────────────────────────── ForEachChunked ────────────────────────────

func ForEachChunked[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunk(ctx, items, concurrency, batchSize, func(ctx context.Context, chunk []T) error {
		_, _, _, _ = ForEach(ctx, chunk, fn, core.WithConfig(concurrency))
		return nil
	})
}

func DefaultForEachChunked[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunked(ctx, items, core.IO(), batchSize, fn)
}

func ForEachChunkedWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithFailFast(ctx, items, concurrency, batchSize, func(ctx context.Context, chunk []T) error {
		_, _, _, _ = ForEachWithFailFast(ctx, chunk, fn, core.WithConfig(concurrency))
		return nil
	})
}

func DefaultForEachChunkedWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkedWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

func ForEachChunkedWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithTimeout(ctx, items, concurrency, batchSize, timeout, func(ctx context.Context, chunk []T) error {
		_, _, _, _ = ForEach(ctx, chunk, fn, core.WithConfig(concurrency))
		return nil
	})
}

func DefaultForEachChunkedWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkedWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

func ForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithFFTimeout(ctx, items, concurrency, batchSize, timeout, func(ctx context.Context, chunk []T) error {
		_, _, _, _ = ForEachWithFailFast(ctx, chunk, fn, core.WithConfig(concurrency))
		return nil
	})
}

func DefaultForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkedWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}
