// Package mapreduce 提供并发 Map、ForEach、Reduce、Chunk 等数据并行操作。
//
// 核心函数：
//   - Map：并发处理切片元素，返回 Result 切片
//   - MapWithFailFast：带快速失败的并发 Map
//   - MapSerial / MapSerialFailFast：串行 Map
//   - ForEach / ForEachWithFailFast：只关心错误的并发遍历
//   - Reduce：先 Map 后聚合
//   - Chunk / ChunkN：切片分块
//   - Must / Partition / Flat / OnlyErrors：辅助函数
//
// 对于有返回值的高阶操作，推荐使用 async.Map / async.ForEach / async.Reduce 等便捷封装，
// 它们提供了 Default* 和 WithTimeout 等变体。
//
// 使用示例：
//
//	// Map：并发调用 RPC
//	results, err := mapreduce.Map(ctx, ids, func(ctx context.Context, id int) (string, error) {
//	    return rpcCall(ctx, id)
//	}, mapreduce.IO())
//
//	// ForEach：并发发送消息
//	total, fail, firstErr, _ := mapreduce.ForEach(ctx, msgs, func(ctx context.Context, msg string) error {
//	    return send(ctx, msg)
//	}, mapreduce.IO())
package mapreduce

import (
	"context"
	"runtime"
	"sync"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/group"
)

// ──────────────────────────── Map ────────────────────────────

// Map 并发处理切片中的每个元素，返回 Result 切片。
// concurrency <= 0 时使用 core.IO()。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，接收 ctx 和元素
//   - concurrency：并发度
//
// 使用示例：
//
//	results, err := mapreduce.Map(ctx, urls, func(ctx context.Context, url string) (*http.Response, error) {
//	    return httpGet(ctx, url)
//	}, 8)
func Map[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) ([]core.Result[R], error) {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if concurrency <= 0 {
		concurrency = core.IO()
	}
	if concurrency > n {
		concurrency = n
	}
	return mapImpl[T, R](ctx, items, fn, concurrency, false)
}

// MapWithFailFast 与 Map 相同，但第一个任务失败时取消其余任务。返回第一个错误。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数
//   - concurrency：并发度
func MapWithFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) ([]core.Result[R], error) {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if concurrency <= 0 {
		concurrency = core.IO()
	}
	if concurrency > n {
		concurrency = n
	}
	return mapImpl[T, R](ctx, items, fn, concurrency, true)
}

// ──────────────────────────── MapStream ────────────────────────────

// MapStream 并发处理切片元素，通过 channel 流式返回结果，实现边执行边消费。
// 返回的 channel 在所有任务完成后自动关闭。
//
// 适用场景：需要实时处理大量数据的场景，如批量请求结果逐条处理、流式 ETL。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数
//   - concurrency：并发度
//   - bufSize：channel 缓冲区大小，<=0 时使用 len(items)/concurrency
//
// 使用示例：
//
//	ch := mapreduce.MapStream(ctx, urls, func(ctx context.Context, url string) (*http.Response, error) {
//	    return httpGet(ctx, url)
//	}, 8, 1024)
//	for r := range ch {
//	    if r.Ok() {
//	        processResponse(r.Value)
//	    } else {
//	        log.Printf("请求失败: %v", r.Err)
//	    }
//	}
func MapStream[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int, bufSize int) <-chan core.Result[R] {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if concurrency <= 0 {
		concurrency = core.IO()
	}
	if concurrency > n && n > 0 {
		concurrency = n
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if bufSize <= 0 {
		if n <= 16384 {
			bufSize = n
		} else {
			bufSize = 16384
		}
	}

	outCh := make(chan core.Result[R], bufSize)

	g := group.NewGroup[R](concurrency)
	g.WithResultCallback(func(r core.Result[R]) {
		outCh <- r
	})

	go func() {
		for i := range items {
			idx := i
			g.Go(ctx, func(ctx context.Context) (R, error) {
				return fn(ctx, items[idx])
			})
		}
		g.Wait()
		close(outCh)
	}()

	return outCh
}

// MapStreamWithFailFast 与 MapStream 相同，但第一个任务失败时取消其余任务。
func MapStreamWithFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int, bufSize int) <-chan core.Result[R] {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if concurrency <= 0 {
		concurrency = core.IO()
	}
	if concurrency > n && n > 0 {
		concurrency = n
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if bufSize <= 0 {
		if n <= 16384 {
			bufSize = n
		} else {
			bufSize = 16384
		}
	}

	ffCtx, ffCancel := context.WithCancel(ctx)

	outCh := make(chan core.Result[R], bufSize)

	g := group.NewGroup[R](concurrency)
	g.WithResultCallback(func(r core.Result[R]) {
		outCh <- r
	})

	go func() {
		for i := range items {
			idx := i
			g.Go(ffCtx, func(ctx context.Context) (R, error) {
				val, err := fn(ctx, items[idx])
				if err != nil {
					ffCancel()
				}
				return val, err
			})
		}
		g.Wait()
		ffCancel()
		close(outCh)
	}()

	return outCh
}

func mapImpl[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int, failFast bool) ([]core.Result[R], error) {
	n := len(items)
	if n == 0 {
		return nil, nil
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > n {
		concurrency = n
	}

	concurrency = core.WithConfig(concurrency)

	results := make([]core.Result[R], n)
	var wg sync.WaitGroup
	chunkSize := (n + concurrency - 1) / concurrency

	var cancel context.CancelFunc
	if failFast {
		ctx, cancel = context.WithCancel(ctx)
	}

	if failFast {
		mapParallelFailFast(ctx, cancel, items, fn, results, concurrency, chunkSize, &wg)
	} else {
		mapParallel(ctx, items, fn, results, concurrency, chunkSize, &wg)
	}

	wg.Wait()
	if cancel != nil {
		cancel()
	}

	if failFast {
		for _, r := range results {
			if r.Err != nil {
				return results, r.Err
			}
		}
	}

	return results, nil
}

func mapParallel[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), results []core.Result[R], concurrency int, chunkSize int, w *sync.WaitGroup) {
	for i := 0; i < concurrency; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(items) {
			break
		}
		if end > len(items) {
			end = len(items)
		}
		w.Add(1)
		go func(start, end int) {
			defer w.Done()
			defer func() {
				if r := recover(); r != nil {
					for j := start; j < end; j++ {
						results[j] = core.Result[R]{Err: core.NewPanicError(r)}
					}
				}
			}()
			for j := start; j < end; j++ {
				val, err := fn(ctx, items[j])
				results[j] = core.Result[R]{Value: val, Err: err}
			}
		}(start, end)
	}
}

func mapParallelFailFast[T any, R any](ctx context.Context, cancel context.CancelFunc, items []T, fn func(context.Context, T) (R, error), results []core.Result[R], concurrency int, chunkSize int, w *sync.WaitGroup) {
	for i := 0; i < concurrency; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(items) {
			break
		}
		if end > len(items) {
			end = len(items)
		}
		w.Add(1)
		go func(start, end int) {
			defer w.Done()
			for j := start; j < end; j++ {
				select {
				case <-ctx.Done():
					results[j] = core.Result[R]{Err: ctx.Err()}
					continue
				default:
				}
				val, err := fn(ctx, items[j])
				results[j] = core.Result[R]{Value: val, Err: err}
				if err != nil {
					cancel()
				}
			}
		}(start, end)
	}
}

// ──────────────────────────── MapSerial ────────────────────────────

// MapSerial 串行处理切片元素，返回 Result 切片。适合数据量小或需保证顺序的场景。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数
func MapSerial[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	ctx = core.EnsureTraceID(ctx)
	return mapSerial(ctx, items, fn, false)
}

// MapSerialFailFast 串行带快速失败的 Map，首个失败立即返回。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数
func MapSerialFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	ctx = core.EnsureTraceID(ctx)
	return mapSerial(ctx, items, fn, true)
}

func mapSerial[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), failFast bool) ([]core.Result[R], error) {
	n := len(items)
	results := make([]core.Result[R], n)
	if failFast {
		ctx2, cancel := context.WithCancel(ctx)
		defer cancel()
		for i, item := range items {
			val, err := core.SafeCall(ctx2, item, fn)
			results[i] = core.Result[R]{Value: val, Err: err}
			if err != nil {
				return results, err
			}
		}
		return results, nil
	}
	for i, item := range items {
		val, err := core.SafeCall(ctx, item, fn)
		results[i] = core.Result[R]{Value: val, Err: err}
	}
	return results, nil
}

// ──────────────────────────── ForEach ────────────────────────────

// ForEach 并发遍历切片，执行只返回 error 的函数。
// 返回值：总数、失败数、首个错误、各任务结果。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，只返回 error
//   - concurrency：并发度
//
// 使用示例：
//
//	total, fail, firstErr, _ := mapreduce.ForEach(ctx, msgs, func(ctx context.Context, msg string) error {
//	    return send(ctx, msg)
//	}, 10)
func ForEach[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if n == 0 {
		return 0, 0, nil, nil
	}
	concurrency = core.WithConfig(concurrency)
	if concurrency > n {
		concurrency = n
	}

	if concurrency <= 1 {
		g, _ := forEachSerialImpl(ctx, items, fn, false)
		g.Wait()
		return group.BuildAggregateNoResult(g)
	}

	g := group.NewNoResult(concurrency)
	for i := range items {
		g.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, items[i])
		})
	}
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

// ForEachSerial 串行遍历切片，逐个执行并收集错误。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，只返回 error
func ForEachSerial[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	ctx = core.EnsureTraceID(ctx)
	g, _ := forEachSerialImpl(ctx, items, fn, false)
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

// ForEachSerialFailFast 串行带快速失败的 ForEach，首个错误立即返回。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，只返回 error
func ForEachSerialFailFast[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	ctx = core.EnsureTraceID(ctx)
	g, _ := forEachSerialImpl(ctx, items, fn, true)
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

func forEachSerialImpl[T any](ctx context.Context, items []T, fn func(context.Context, T) error, failFast bool) (g *group.NoResult, firstErr error) {
	g = group.NewNoResult(1)
	g.WithTimeout(0)

	if failFast {
		ctx2, cancel := context.WithCancel(ctx)
		defer cancel()
		for _, item := range items {
			if err := core.SafeCallVoid(ctx2, item, fn); err != nil {
				g.Go(ctx2, func(ctx context.Context) error { return err })
				return g, err
			}
			g.Go(ctx2, func(ctx context.Context) error { return nil })
		}
		return g, nil
	}
	for _, item := range items {
		if err := core.SafeCallVoid(ctx, item, fn); err != nil {
			g.Go(ctx, func(ctx context.Context) error { return err })
		} else {
			g.Go(ctx, func(ctx context.Context) error { return nil })
		}
	}
	return g, nil
}

// ForEachWithFailFast 与 ForEach 相同，但第一个任务失败时取消其余任务。
//
// 参数：
//   - ctx：上下文
//   - items：待处理的元素切片
//   - fn：处理函数，只返回 error
//   - concurrency：并发度
func ForEachWithFailFast[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
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

	if concurrency <= 1 {
		g, _ := forEachSerialImpl(ffCtx, items, fn, true)
		g.Wait()
		return group.BuildAggregateNoResult(g)
	}

	g := group.NewNoResult(concurrency)
	for i := range items {
		g.Go(ffCtx, func(ctx context.Context) error {
			return fn(ctx, items[i])
		})
	}
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

// ──────────────────────────── ForEachStream ────────────────────────────

// ForEachStream 并发遍历元素，通过 channel 流式返回每个元素的错误结果，实现边执行边消费。
// 返回的 Result[struct{}] 中 Err 为 nil 表示成功，非 nil 表示失败。
//
// 适用场景：需要实时处理批量操作结果的场景，如逐条记录日志、实时告警。
//
// 参数：
//   - ctx：上下文
//   - items：输入元素切片
//   - fn：处理函数，只返回 error
//   - concurrency：并发度
//   - bufSize：channel 缓冲区大小，<=0 时自动计算
//
// 使用示例：
//
//	ch := mapreduce.ForEachStream(ctx, records, func(ctx context.Context, r Record) error {
//	    return saveToDB(ctx, r)
//	}, 64, 1024)
//	for res := range ch {
//	    if res.Err != nil {
//	        log.Printf("处理失败: %v", res.Err)
//	    }
//	}
func ForEachStream[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int, bufSize int) <-chan core.Result[struct{}] {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if concurrency <= 0 {
		concurrency = core.IO()
	}
	if concurrency > n && n > 0 {
		concurrency = n
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if bufSize <= 0 {
		if n <= 16384 {
			bufSize = n
		} else {
			bufSize = 16384
		}
	}

	outCh := make(chan core.Result[struct{}], bufSize)

	nr := group.NewNoResult(concurrency)
	nr.WithResultCallback(func(r core.Result[struct{}]) {
		outCh <- r
	})

	go func() {
		for i := range items {
			idx := i
			nr.Go(ctx, func(ctx context.Context) error {
				return fn(ctx, items[idx])
			})
		}
		nr.Wait()
		close(outCh)
	}()

	return outCh
}

// ForEachStreamWithFailFast 带 FailFast 的流式 ForEach：首个错误立即取消其余任务。
func ForEachStreamWithFailFast[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int, bufSize int) <-chan core.Result[struct{}] {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if concurrency <= 0 {
		concurrency = core.IO()
	}
	if concurrency > n && n > 0 {
		concurrency = n
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if bufSize <= 0 {
		if n <= 16384 {
			bufSize = n
		} else {
			bufSize = 16384
		}
	}

	ffCtx, ffCancel := context.WithCancel(ctx)

	outCh := make(chan core.Result[struct{}], bufSize)

	nr := group.NewNoResult(concurrency)
	nr.WithResultCallback(func(r core.Result[struct{}]) {
		outCh <- r
	})

	go func() {
		for i := range items {
			idx := i
			nr.Go(ffCtx, func(ctx context.Context) error {
				err := fn(ctx, items[idx])
				if err != nil {
					ffCancel()
				}
				return err
			})
		}
		nr.Wait()
		ffCancel()
		close(outCh)
	}()

	return outCh
}

// ──────────────────────────── Reduce ────────────────────────────

// Reduce 串行聚合：对每个元素调用 fn(acc, item)，累积结果。
//
// 参数：
//   - ctx：上下文
//   - items：待聚合的元素切片
//   - initial：初始累加值
//   - fn：聚合函数，接收 ctx、累加值和当前元素
//
// 使用示例：
//
//	sum, err := mapreduce.Reduce(ctx, nums, 0, func(ctx context.Context, acc int, n int) (int, error) {
//	    return acc + n, nil
//	})
func Reduce[T any, R any](ctx context.Context, items []T, initial R, fn func(context.Context, R, T) (R, error)) (R, error) {
	ctx = core.EnsureTraceID(ctx)
	acc := initial
	for _, item := range items {
		var err error
		acc, err = fn(ctx, acc, item)
		if err != nil {
			return acc, err
		}
	}
	return acc, nil
}

// ──────────────────────────── Chunk ────────────────────────────

// Chunk 将切片按大小均分。适用于将大数据集拆分为并发处理的小块。
//
// 参数：
//   - items：待分割的元素切片
//   - chunkSize：每块大小
//
// 使用示例：
//
//	chunks := mapreduce.Chunk(bigSlice, 100)
//	for _, chunk := range chunks {
//	    g.Go(ctx, ...)
//	}
func Chunk[T any](items []T, chunkSize int) [][]T {
	if chunkSize <= 0 || len(items) == 0 {
		return nil
	}
	chunks := make([][]T, 0, (len(items)+chunkSize-1)/chunkSize)
	for i := 0; i < len(items); i += chunkSize {
		end := i + chunkSize
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[i:end])
	}
	return chunks
}

// ──────────────────────────── internal shared helpers ────────────────────────────

func logMapSerialPanic(r any, pe *core.PanicError) {
	var pcs [64]uintptr
	n := runtime.Callers(0, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var fnName string
	frame, more := frames.Next()
	if more {
		fnName = frame.Function
	}
	core.LogError("async serial path panic recovered",
		core.Str("fn", fnName),
		core.Any("panic", r),
		core.Bytes("stack", pe.Stack))
}

func SafeCallWithResult[T any, R any](ctx context.Context, item T, fn func(context.Context, T) (R, error)) (val R, err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := core.NewPanicError(r)
			logMapSerialPanic(r, pe)
			err = pe
		}
	}()
	return fn(ctx, item)
}

// ChunkN 将切片按份数均分。n 为目标份数。
//
// 参数：
//   - items：待分割的元素切片
//   - n：目标份数
//
// 使用示例：
//
//	// 将切片均分为并发数份
//	chunks := mapreduce.ChunkN(items, runtime.NumCPU())
func ChunkN[T any](items []T, n int) [][]T {
	if n <= 0 || len(items) == 0 {
		return nil
	}
	chunkSize := (len(items) + n - 1) / n
	return Chunk(items, chunkSize)
}

// Partition 按条件将切片拆分为匹配和不匹配两个切片。
//
// 使用示例：
//
//	evens, odds := mapreduce.Partition(nums, func(n int) bool { return n%2 == 0 })
func Partition[T any](items []T, pred func(T) bool) (matched []T, unmatched []T) {
	for _, item := range items {
		if pred(item) {
			matched = append(matched, item)
		} else {
			unmatched = append(unmatched, item)
		}
	}
	return
}

// Must 错误时 panic，适用于测试或初始化场景。
//
// 参数：
//   - val：结果值
//   - err：可能发生的错误
func Must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

// Flat 从 Result 切片中提取所有值，丢弃错误。
//
// 参数：
//   - results：Result 切片
func Flat[T any](results []core.Result[T]) []T {
	out := make([]T, 0, len(results))
	for _, r := range results {
		out = append(out, r.Value)
	}
	return out
}

// OnlyErrors 从 Result 切片中提取所有非 nil 错误。
//
// 参数：
//   - results：Result 切片
func OnlyErrors[T any](results []core.Result[T]) []error {
	out := make([]error, 0)
	for _, r := range results {
		if r.Err != nil {
			out = append(out, r.Err)
		}
	}
	return out
}
