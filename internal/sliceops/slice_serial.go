package sliceops

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
)

type SerialSlice[T any, R any] struct {
	*sliceBase[T, *SerialSlice[T, R]]
}

func (b *SerialSlice[T, R]) ToParallel() *ParallelSlice[T, R] {
	ps := &ParallelSlice[T, R]{}
	ps.sliceBase = newSliceBase(b.ctx, b.items, DefPar(), ps)
	return ps
}

func (b *SerialSlice[T, R]) FailFast() *SerialSlice[T, R] {
	b.policy = b.policy.FF()
	return b
}

func (b *SerialSlice[T, R]) NoFailFast() *SerialSlice[T, R] {
	b.policy = b.policy.NoFF()
	return b
}

func (b *SerialSlice[T, R]) Timeout(d time.Duration) *SerialSlice[T, R] {
	b.policy = b.policy.TO(d)
	return b
}

func (b *SerialSlice[T, R]) DefaultTimeout() *SerialSlice[T, R] {
	b.policy = b.policy.TO(defaultTimeout)
	return b
}

func (b *SerialSlice[T, R]) Logger(l core.Logger) *SerialSlice[T, R] {
	core.SetLogger(l)
	return b
}

func (b *SerialSlice[T, R]) Map(fn func(context.Context, T) (R, error)) *SliceResult[R] {
	results, err := NewRunner[T, R](b.ctx, b.items, b.policy).Map(fn)
	return &SliceResult[R]{results: results, err: err, failValues: collectFailValues(b.items, results)}
}

func (b *SerialSlice[T, R]) ForEach(fn func(context.Context, T) error) *ForEachResult {
	total, failCnt, firstErr, _ := NewRunner[T, R](b.ctx, b.items, b.policy).Each(fn)
	return &ForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

func (b *SerialSlice[T, R]) Reduce(initial R, fn func(context.Context, R, T) (R, error)) (R, error) {
	return Reduce(b.ctx, b.items, initial, fn)
}

func (b *SerialSlice[T, R]) Stream(fn func(context.Context, T) (R, error), bufSize int) <-chan core.Result[R] {
	return NewRunner[T, R](b.ctx, b.items, b.policy).Stream(fn, bufSize)
}
