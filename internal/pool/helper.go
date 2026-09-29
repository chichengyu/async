package pool

import (
	"context"
	"runtime"

	"github.com/chichengyu/async/internal/core"
)

func checkErr(err error, name string) {
	if err != nil {
		var buf [4096]byte
		n := runtime.Stack(buf[:], false)
		core.LogCtxError(context.Background(), "async pool fatal",
			core.Err(err),
			core.Str("stack", string(buf[:n])),
			core.Str("name", name))
	}
}

func Submit[T any](ctx context.Context, fn func(context.Context) (T, error)) (*Pool[T], int, error) {
	p := DefaultPool[T]()
	p.ctx = ctx
	idx, err := p.submitIndexed(ctx, fn)
	return p, idx, err
}

func SubmitN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult, error) {
	p := DefaultPool[T]()
	p.ctx = ctx
	results := make([]SubmitResult, n)
	for i := 0; i < n; i++ {
		idx, err := p.submitIndexed(ctx, fn)
		results[i] = SubmitResult{Index: idx, Err: err}
	}
	return p, results, nil
}

func SubmitSafeN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult) {
	pool, results, err := SubmitN(ctx, fn, n)
	for _, r := range results {
		checkErr(r.Err, "SubmitSafeN")
	}
	checkErr(err, "SubmitSafeN")
	return pool, results
}

func SubmitBatch[T any, S ~[]E, E any](ctx context.Context, items S, fn func(context.Context, E) (T, error)) (*Pool[T], []SubmitResult, error) {
	p := DefaultPool[T]()
	p.ctx = ctx
	n := len(items)
	results := make([]SubmitResult, n)
	for i, item := range items {
		item := item
		idx, err := p.submitIndexed(ctx, func(ctx context.Context) (T, error) {
			return fn(ctx, item)
		})
		results[i] = SubmitResult{Index: idx, Err: err}
	}
	return p, results, nil
}

func MapPool[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) (*Pool[R], []core.Result[R], error) {
	if concurrency <= 0 {
		concurrency = core.IO()
	}
	if concurrency > len(items) {
		concurrency = len(items)
	}
	p := NewPool[R](concurrency)
	p.ctx = ctx
	for _, item := range items {
		item := item
		p.Submit(ctx, func(ctx context.Context) (R, error) {
			return fn(ctx, item)
		})
	}
	results := p.Wait()
	p.Close()
	return p, results, nil
}

func ForEachPool[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (*NoResultPool, error) {
	if concurrency <= 0 {
		concurrency = core.IO()
	}
	p := NewPool[struct{}](concurrency)
	p.ctx = ctx
	for _, item := range items {
		item := item
		p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
			return struct{}{}, fn(ctx, item)
		})
	}
	p.Wait()
	p.Close()
	return p, p.FirstError()
}
