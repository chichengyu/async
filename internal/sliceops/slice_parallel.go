package sliceops

import (
	"context"
	"runtime"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

type ParallelSlice[T any, R any] struct {
	*sliceBase[T, *ParallelSlice[T, R]]
}

func (b *ParallelSlice[T, R]) ToSerial() *SerialSlice[T, R] {
	ss := &SerialSlice[T, R]{}
	ss.sliceBase = newSliceBase(b.ctx, b.items, Seq(), ss)
	return ss
}

func (b *ParallelSlice[T, R]) Concurrency(n int) *ParallelSlice[T, R] {
	b.policy = Par(n)
	return b
}

func (b *ParallelSlice[T, R]) DefaultConcurrency() *ParallelSlice[T, R] {
	b.policy = DefPar()
	return b
}

func (b *ParallelSlice[T, R]) Pool(p *pool.Pool[R]) *ParallelSlice[T, R] {
	b.policy = b.policy.WithPool(p)
	return b
}

func (b *ParallelSlice[T, R]) PoolAuto(p *pool.Pool[R]) *ParallelSlice[T, R] {
	b.policy = b.policy.WithOwnedPool(p)
	return b
}

func (b *ParallelSlice[T, R]) DefaultPool() *ParallelSlice[T, R] {
	b.policy = b.policy.WithDefaultPool()
	return b
}

func (b *ParallelSlice[T, R]) FailFast() *ParallelSlice[T, R] {
	b.policy = b.policy.FF()
	return b
}

func (b *ParallelSlice[T, R]) NoFailFast() *ParallelSlice[T, R] {
	b.policy = b.policy.NoFF()
	return b
}

func (b *ParallelSlice[T, R]) Timeout(d time.Duration) *ParallelSlice[T, R] {
	b.policy = b.policy.TO(d)
	return b
}

func (b *ParallelSlice[T, R]) DefaultTimeout() *ParallelSlice[T, R] {
	b.policy = b.policy.TO(defaultTimeout)
	return b
}

func (b *ParallelSlice[T, R]) Shards(n int) *ParallelSlice[T, R] {
	b.policy = b.policy.Shard(n)
	return b
}

func (b *ParallelSlice[T, R]) DefaultShard() *ParallelSlice[T, R] {
	n := runtime.GOMAXPROCS(0)
	if n < 2 {
		n = 2
	}
	b.policy = b.policy.Shard(n)
	return b
}

func (b *ParallelSlice[T, R]) Chunk(size int) *ParallelSlice[T, R] {
	b.policy = b.policy.Chunk(size)
	return b
}

func (b *ParallelSlice[T, R]) DefaultChunk() *ParallelSlice[T, R] {
	b.policy = b.policy.Chunk(defaultBatchSize)
	return b
}

func (b *ParallelSlice[T, R]) Logger(l core.Logger) *ParallelSlice[T, R] {
	core.SetLogger(l)
	return b
}

func (b *ParallelSlice[T, R]) Map(fn func(context.Context, T) (R, error)) *SliceResult[R] {
	results, err := NewRunner[T, R](b.ctx, b.items, b.policy).Map(fn)
	return &SliceResult[R]{results: results, err: err, failValues: collectFailValues(b.items, results)}
}

func (b *ParallelSlice[T, R]) MapBatch(fn func(context.Context, []T) (R, error)) *SliceResult[R] {
	results, err := NewRunner[T, R](b.ctx, b.items, b.policy).MapBatch(fn)
	return &SliceResult[R]{results: results, err: err, failValues: collectFailValues(b.items, results)}
}

func (b *ParallelSlice[T, R]) ForEach(fn func(context.Context, T) error) *ForEachResult {
	total, failCnt, firstErr, _ := NewRunner[T, R](b.ctx, b.items, b.policy).Each(fn)
	return &ForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

func (b *ParallelSlice[T, R]) ForEachBatch(fn func(context.Context, []T) error) *ForEachResult {
	total, failCnt, firstErr, _ := NewRunner[T, R](b.ctx, b.items, b.policy).EachBatch(fn)
	return &ForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

func (b *ParallelSlice[T, R]) Stream(fn func(context.Context, T) (R, error), bufSize int) <-chan core.Result[R] {
	return NewRunner[T, R](b.ctx, b.items, b.policy).Stream(fn, bufSize)
}
