// Package mapreduce 的扩展函数。
//
// 扩展函数命名约定：
//
//	Default*           - 使用 core.IO() 作为默认并发度
//	*WithTimeout       - 支持单任务超时
//	*WithFailFast      - 支持快速失败
//	*WithFFTimeout     - 既快速失败又带超时
//	*Chunk             - 分块处理（fn 接收整个 chunk）
//	*Chunked           - 分块后每个元素单独调用 fn
//
// 使用示例：
//
//	// Default* 便捷方法
//	results := mapreduce.DefaultMap(ctx, ids, fetchFunc)
//
//	// MapChunk：分块批量处理
//	results := mapreduce.MapChunk(ctx, items, 4, 100, func(ctx context.Context, batch []Item) ([]Result, error) {
//	    return batchProcess(ctx, batch)
//	})
//
//	// MapChunked：分块后逐元素处理（适合元素数量大、单次处理轻量的场景）
//	results := mapreduce.MapChunked(ctx, items, 8, 100, func(ctx context.Context, item Item) (Result, error) {
//	    return process(ctx, item)
//	})
//
//	// Result 辅助函数
//	vals := mapreduce.ResultValues(results)   // 提取成功值
//	errs := mapreduce.ResultErrors(results)   // 提取错误
//	allOk := mapreduce.Every(results)         // 是否全部成功
//	hasErr := mapreduce.AnyError(results)     // 是否有失败
package mapreduce

import (
	"context"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/group"
)

// ──────────────────────────── Map 扩展 ────────────────────────────

// DefaultMap 使用 core.IO() 作为默认并发度的 Map。
// 忽略 Map 返回的 error（因为 Map 非 FailFast 不会返回错误）。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，接收 ctx 和元素，返回结果和错误
//
// 使用示例：
//
//	results := mapreduce.DefaultMap(ctx, ids, func(ctx context.Context, id int) (*User, error) {
//	    return userRepo.FindByID(ctx, id)
//	})
func DefaultMap[T any, R any](ctx context.Context, items []T, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	results, _ := Map(ctx, items, fn, core.IO())
	return results
}

// DefaultMapWithFailFast 使用默认并发度的 FailFast Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，接收 ctx 和元素，返回结果和错误
//
// 使用示例：
//
//	results, err := mapreduce.DefaultMapWithFailFast(ctx, ids, func(ctx context.Context, id int) (*User, error) {
//	    return userRepo.FindByID(ctx, id)
//	})
func DefaultMapWithFailFast[T any, R any](ctx context.Context, items []T, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	return MapWithFailFast(ctx, items, fn, core.IO())
}

// MapWithTimeout 带单任务超时的并发 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度，<=0 使用 core.IO()
//   - timeout：单任务超时时间
//   - fn：处理函数，接收 ctx 和元素，返回结果和错误
//
// 使用示例：
//
//	results := mapreduce.MapWithTimeout(ctx, urls, 8, 5*time.Second, func(ctx context.Context, url string) (*Body, error) {
//	    return httpGet(ctx, url)
//	})
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

// DefaultMapWithTimeout 使用默认并发度的带超时 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - timeout：单任务超时时间
//   - fn：处理函数
//
// 使用示例：
//
//	results := mapreduce.DefaultMapWithTimeout(ctx, ids, 3*time.Second, fn)
func DefaultMapWithTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	return MapWithTimeout(ctx, items, core.IO(), timeout, fn)
}

// MapWithFFTimeout 带 FailFast 和单任务超时的并发 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度，<=0 使用 core.IO()
//   - timeout：单任务超时时间
//   - fn：处理函数，接收 ctx 和元素，返回结果和错误
//
// 使用示例：
//
//	results, err := mapreduce.MapWithFFTimeout(ctx, urls, 8, 3*time.Second, func(ctx context.Context, url string) (*Body, error) {
//	    return httpGet(ctx, url)
//	})
//	if err != nil {
//	    log.Printf("首个失败: %v", err)
//	}
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

// DefaultMapWithFFTimeout 使用默认并发度的 FailFast + 超时 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - timeout：单任务超时时间
//   - fn：处理函数
//
// 使用示例：
//
//	results, err := mapreduce.DefaultMapWithFFTimeout(ctx, ids, 3*time.Second, fn)
func DefaultMapWithFFTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	return MapWithFFTimeout(ctx, items, core.IO(), timeout, fn)
}

// ──────────────────────────── ForEach 扩展 ────────────────────────────

// DefaultForEach 使用默认并发度的 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，只返回 error
//
// 使用示例：
//
//	total, fail, firstErr, _ := mapreduce.DefaultForEach(ctx, msgs, func(ctx context.Context, msg string) error {
//	    return send(ctx, msg)
//	})
func DefaultForEach[T any](ctx context.Context, items []T, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEach(ctx, items, fn, core.IO())
}

// DefaultForEachWithFailFast 使用默认并发度的 FailFast ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，只返回 error
//
// 使用示例：
//
//	total, fail, firstErr, _ := mapreduce.DefaultForEachWithFailFast(ctx, tasks, fn)
func DefaultForEachWithFailFast[T any](ctx context.Context, items []T, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachWithFailFast(ctx, items, fn, core.IO())
}

// ForEachWithTimeout 带单任务超时的 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - timeout：单任务超时时间
//   - fn：处理函数，只返回 error
//
// 使用示例：
//
//	total, fail, firstErr, _ := mapreduce.ForEachWithTimeout(ctx, msgs, 10, 2*time.Second, sendFn)
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

// DefaultForEachWithTimeout 使用默认并发度的带超时 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - timeout：单任务超时时间
//   - fn：处理函数，只返回 error
//
// 使用示例：
//
//	total, fail, firstErr, _ := mapreduce.DefaultForEachWithTimeout(ctx, msgs, 2*time.Second, sendFn)
func DefaultForEachWithTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachWithTimeout(ctx, items, core.IO(), timeout, fn)
}

// ForEachWithFFTimeout 带 FailFast 和单任务超时的 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - timeout：单任务超时时间
//   - fn：处理函数，只返回 error
//
// 使用示例：
//
//	total, fail, firstErr, _ := mapreduce.ForEachWithFFTimeout(ctx, tasks, 8, 3*time.Second, fn)
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

// DefaultForEachWithFFTimeout 使用默认并发度的 FailFast + 超时 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - timeout：单任务超时时间
//   - fn：处理函数，只返回 error
//
// 使用示例：
//
//	total, fail, firstErr, _ := mapreduce.DefaultForEachWithFFTimeout(ctx, tasks, 3*time.Second, fn)
func DefaultForEachWithFFTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachWithFFTimeout(ctx, items, core.IO(), timeout, fn)
}

// ──────────────────────────── Result helpers ────────────────────────────

// ResultValues 从 Result 切片中提取所有成功值（跳过错误结果）。
func ResultValues[T any](results []core.Result[T]) []T {
	vals := make([]T, 0, len(results))
	for _, r := range results {
		if r.Err == nil {
			vals = append(vals, r.Value)
		}
	}
	return vals
}

// ResultErrors 从 Result 切片中提取所有错误（跳过成功结果）。
func ResultErrors[T any](results []core.Result[T]) []error {
	errs := make([]error, 0)
	for _, r := range results {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errs
}

// Every 检查所有 Result 是否都成功。
func Every[T any](results []core.Result[T]) bool {
	for _, r := range results {
		if r.Err != nil {
			return false
		}
	}
	return true
}

// Some 检查是否至少有一个 Result 成功。
func Some[T any](results []core.Result[T]) bool {
	for _, r := range results {
		if r.Err == nil {
			return true
		}
	}
	return false
}

// AnyError 检查是否有任何失败。
//
// 参数：
//   - results：Result 切片
func AnyError[T any](results []core.Result[T]) bool {
	for _, r := range results {
		if r.Err != nil {
			return true
		}
	}
	return false
}

// PartitionResults 将 Result 切片拆分为成功值和失败错误两个切片。
//
// 参数：
//   - results：Result 切片
//
// 使用示例：
//
//	successes, failures := mapreduce.PartitionResults(results)
//	fmt.Printf("成功: %d, 失败: %d\n", len(successes), len(failures))
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

// MapChunk 先按 batchSize 分块，再并发处理每个块。fn 接收整个 chunk。
// 适合批量 RPC 调用、batch DB 操作等场景。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - fn：处理函数，接收 ctx 和整个 chunk
func MapChunk[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R] {
	chunks := Chunk(items, batchSize)
	results, _ := Map(ctx, chunks, fn, concurrency)
	return results
}

// DefaultMapChunk 使用默认并发度的分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - fn：处理函数，接收 ctx 和整个 chunk
func DefaultMapChunk[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R] {
	return MapChunk(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkWithFailFast 带 FailFast 的分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - fn：处理函数，接收 ctx 和整个 chunk
func MapChunkWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error) {
	chunks := Chunk(items, batchSize)
	return MapWithFailFast(ctx, chunks, fn, concurrency)
}

// DefaultMapChunkWithFailFast 使用默认并发度的 FailFast 分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - fn：处理函数
func DefaultMapChunkWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error) {
	return MapChunkWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkWithTimeout 带超时的分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数，接收 ctx 和整个 chunk
func MapChunkWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R] {
	chunks := Chunk(items, batchSize)
	return MapWithTimeout(ctx, chunks, concurrency, timeout, fn)
}

// DefaultMapChunkWithTimeout 使用默认并发度的带超时分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func DefaultMapChunkWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R] {
	return MapChunkWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// MapChunkWithFFTimeout 带 FailFast 和超时的分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数，接收 ctx 和整个 chunk
func MapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error) {
	chunks := Chunk(items, batchSize)
	return MapWithFFTimeout(ctx, chunks, concurrency, timeout, fn)
}

// DefaultMapChunkWithFFTimeout 使用默认并发度的 FailFast + 超时分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func DefaultMapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error) {
	return MapChunkWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ──────────────────────────── MapChunked ────────────────────────────

// MapChunked 先分块，再对每个元素并发调用 fn。
// 与 MapChunk 的区别：fn 接收单个元素，适合逐元素处理的大数据量场景。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - fn：处理函数，接收 ctx 和单个元素
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

// DefaultMapChunked 使用默认并发度的逐元素分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - fn：处理函数，接收 ctx 和单个元素
func DefaultMapChunked[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	return MapChunked(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkedWithFailFast 带 FailFast 的逐元素分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - fn：处理函数，接收 ctx 和单个元素
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

// DefaultMapChunkedWithFailFast 使用默认并发度的 FailFast 逐元素分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - fn：处理函数
func DefaultMapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	return MapChunkedWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkedWithTimeout 带超时的逐元素分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数，接收 ctx 和单个元素
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

// DefaultMapChunkedWithTimeout 使用默认并发度的带超时逐元素分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func DefaultMapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) []core.Result[R] {
	return MapChunkedWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// MapChunkedWithFFTimeout 带 FailFast 和超时的逐元素分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数，接收 ctx 和单个元素
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

// DefaultMapChunkedWithFFTimeout 使用默认并发度的 FailFast + 超时逐元素分块 Map。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func DefaultMapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error) {
	return MapChunkedWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ──────────────────────────── ForEachChunk ────────────────────────────

// ForEachChunk 先分块，再对每块并发执行只返回 error 的操作。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - fn：处理函数，接收 ctx 和整个 chunk，只返回 error
func ForEachChunk[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	chunks := Chunk(items, batchSize)
	return ForEach(ctx, chunks, func(ctx context.Context, chunk []T) error {
		return fn(ctx, chunk)
	}, concurrency)
}

// DefaultForEachChunk 使用默认并发度的分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - fn：处理函数
func DefaultForEachChunk[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunk(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkWithFailFast 带 FailFast 的分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - fn：处理函数
func ForEachChunkWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	chunks := Chunk(items, batchSize)
	return ForEachWithFailFast(ctx, chunks, func(ctx context.Context, chunk []T) error {
		return fn(ctx, chunk)
	}, concurrency)
}

// DefaultForEachChunkWithFailFast 使用默认并发度的 FailFast 分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - fn：处理函数
func DefaultForEachChunkWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkWithTimeout 带超时的分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func ForEachChunkWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	chunks := Chunk(items, batchSize)
	return ForEachWithTimeout(ctx, chunks, concurrency, timeout, func(ctx context.Context, chunk []T) error {
		return fn(ctx, chunk)
	})
}

// DefaultForEachChunkWithTimeout 使用默认并发度的带超时分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func DefaultForEachChunkWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ForEachChunkWithFFTimeout 带 FailFast 和超时的分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func ForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	chunks := Chunk(items, batchSize)
	return ForEachWithFFTimeout(ctx, chunks, concurrency, timeout, func(ctx context.Context, chunk []T) error {
		return fn(ctx, chunk)
	})
}

// DefaultForEachChunkWithFFTimeout 使用默认并发度的 FailFast + 超时分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func DefaultForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ──────────────────────────── ForEachChunked ────────────────────────────

// ForEachChunked 先分块，再对每个元素并发执行只返回 error 的操作。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - fn：处理函数，接收 ctx 和单个元素，只返回 error
func ForEachChunked[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	total, failCnt, firstErr, results = ForEachChunk(ctx, items, concurrency, batchSize, func(ctx context.Context, chunk []T) error {
		_, _, fe, _ := ForEach(ctx, chunk, fn, core.WithConfig(concurrency))
		return fe
	})
	return
}

// DefaultForEachChunked 使用默认并发度的逐元素分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - fn：处理函数
func DefaultForEachChunked[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunked(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkedWithFailFast 带 FailFast 的逐元素分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - fn：处理函数
func ForEachChunkedWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	total, failCnt, firstErr, results = ForEachChunkWithFailFast(ctx, items, concurrency, batchSize, func(ctx context.Context, chunk []T) error {
		_, _, fe, _ := ForEachWithFailFast(ctx, chunk, fn, core.WithConfig(concurrency))
		return fe
	})
	return
}

// DefaultForEachChunkedWithFailFast 使用默认并发度的 FailFast 逐元素分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - fn：处理函数
func DefaultForEachChunkedWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkedWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkedWithTimeout 带超时的逐元素分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func ForEachChunkedWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	total, failCnt, firstErr, results = ForEachChunkWithTimeout(ctx, items, concurrency, batchSize, timeout, func(ctx context.Context, chunk []T) error {
		_, _, fe, _ := ForEach(ctx, chunk, fn, core.WithConfig(concurrency))
		return fe
	})
	return
}

// DefaultForEachChunkedWithTimeout 使用默认并发度的带超时逐元素分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func DefaultForEachChunkedWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkedWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ForEachChunkedWithFFTimeout 带 FailFast 和超时的逐元素分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - concurrency：并发度
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func ForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	total, failCnt, firstErr, results = ForEachChunkWithFFTimeout(ctx, items, concurrency, batchSize, timeout, func(ctx context.Context, chunk []T) error {
		_, _, fe, _ := ForEachWithFailFast(ctx, chunk, fn, core.WithConfig(concurrency))
		return fe
	})
	return
}

// DefaultForEachChunkedWithFFTimeout 使用默认并发度的 FailFast + 超时逐元素分块 ForEach。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - batchSize：每块大小
//   - timeout：单任务超时时间
//   - fn：处理函数
func DefaultForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkedWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}
