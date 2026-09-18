// Package mapreduce 提供并发 Map、ForEach、Reduce、Chunk 等数据并行操作。
package mapreduce

import (
	"context"
	"runtime"
	"sync"

	"github.com/jxue/async/core"
	"github.com/jxue/async/group"
	"github.com/rs/zerolog/log"
)

// ──────────────────────────── Map ────────────────────────────

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

	if failFast {
		mapParallelFailFast(ctx, items, fn, results, concurrency, chunkSize, &wg)
	} else {
		mapParallel(ctx, items, fn, results, concurrency, chunkSize, &wg)
	}

	wg.Wait()

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
			for j := start; j < end; j++ {
				val, err := fn(ctx, items[j])
				results[j] = core.Result[R]{Value: val, Err: err}
			}
		}(start, end)
	}
}

func mapParallelFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), results []core.Result[R], concurrency int, chunkSize int, w *sync.WaitGroup) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

func MapSerial[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	ctx = core.EnsureTraceID(ctx)
	return mapSerial(ctx, items, fn, false)
}

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

func ForEachSerial[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	ctx = core.EnsureTraceID(ctx)
	g, _ := forEachSerialImpl(ctx, items, fn, false)
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

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

// ──────────────────────────── Reduce ────────────────────────────

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
	log.Error().
		Str("fn", fnName).
		Interface("panic", r).
		Bytes("stack", pe.Stack).
		Msg("async serial path panic recovered")
}

func logMapSerialError(err error, fnName string) {
	log.Error().
		Str("fn", fnName).
		Err(err).
		Msg("async serial path error")
}

func logMapSerialTaskFail(err error, fnName string) {
	level := core.GetTaskFailLogLevel()
	switch level {
	case core.LogLevelError:
		log.Error().Str("fn", fnName).Err(err).Msg("async serial task failed")
	case core.LogLevelWarn:
		log.Warn().Str("fn", fnName).Err(err).Msg("async serial task failed")
	case core.LogLevelInfo:
		log.Info().Str("fn", fnName).Err(err).Msg("async serial task failed")
	case core.LogLevelDebug:
		log.Debug().Str("fn", fnName).Err(err).Msg("async serial task failed")
	}
}

func safeCallNoResultFn[T any](ctx context.Context, item T, fn func(context.Context, T) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := core.NewPanicError(r)
			logMapSerialPanic(r, pe)
			err = pe
		}
	}()
	return fn(ctx, item)
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

// ChunkN splits items into exactly n chunks (ceil division).
func ChunkN[T any](items []T, n int) [][]T {
	if n <= 0 || len(items) == 0 {
		return nil
	}
	chunkSize := (len(items) + n - 1) / n
	return Chunk(items, chunkSize)
}

// Partition splits items according to pred. In-order returns two slices: matched and unmatched.
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

// Must is a helper that panics if err != nil, useful in tests.
func Must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

// Flat is a shorthand that extracts all values from Results, discarding errors.
func Flat[T any](results []core.Result[T]) []T {
	out := make([]T, 0, len(results))
	for _, r := range results {
		out = append(out, r.Value)
	}
	return out
}

// OnlyErrors returns errors-only slice from results (nil errors are skipped).
func OnlyErrors[T any](results []core.Result[T]) []error {
	out := make([]error, 0)
	for _, r := range results {
		if r.Err != nil {
			out = append(out, r.Err)
		}
	}
	return out
}

// ──────────────────────────── Wait helpers ────────────────────────────

var waitMu sync.Mutex
var waitBuffers = sync.Pool{
	New: func() any {
		b := make([]int64, 0, 64)
		return &b
	},
}
