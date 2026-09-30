package maps

import (
	"context"
	"runtime"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/sliceops"
)

// MapParallel is the parallel map chain builder.
type MapParallel[K comparable, V any, R any] struct {
	*MapBase[K, V, *MapParallel[K, V, R]]
}

func (b *MapParallel[K, V, R]) Context(ctx context.Context) *MapParallel[K, V, R] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

func (b *MapParallel[K, V, R]) ToSerial() *MapSerial[K, V, R] {
	s := &MapSerial[K, V, R]{}
	s.MapBase = newMapBase(b.ctx, b.data, sliceops.Seq(), s)
	return s
}

func (b *MapParallel[K, V, R]) Worker(n int) *MapParallel[K, V, R] {
	b.policy = sliceops.Par(n)
	return b
}
func (b *MapParallel[K, V, R]) DefaultWorker() *MapParallel[K, V, R] {
	b.policy = sliceops.DefPar()
	return b
}
func (b *MapParallel[K, V, R]) Pool(p *pool.Pool[R]) *MapParallel[K, V, R] {
	b.policy = b.policy.WithPool(p)
	return b
}
func (b *MapParallel[K, V, R]) PoolAuto(p *pool.Pool[R]) *MapParallel[K, V, R] {
	b.policy = b.policy.WithOwnedPool(p)
	return b
}
func (b *MapParallel[K, V, R]) DefaultPool() *MapParallel[K, V, R] {
	b.policy = b.policy.WithDefaultPool()
	return b
}
func (b *MapParallel[K, V, R]) FailFast() *MapParallel[K, V, R] { b.policy = b.policy.FF(); return b }
func (b *MapParallel[K, V, R]) NoFailFast() *MapParallel[K, V, R] {
	b.policy = b.policy.NoFF()
	return b
}
func (b *MapParallel[K, V, R]) DefaultFailFast() *MapParallel[K, V, R] {
	b.policy = b.policy.NoFF()
	return b
}
func (b *MapParallel[K, V, R]) Timeout(d time.Duration) *MapParallel[K, V, R] {
	b.policy = b.policy.TO(d)
	return b
}
func (b *MapParallel[K, V, R]) DefaultTimeout() *MapParallel[K, V, R] {
	b.policy = b.policy.TO(defaultMapTimeout)
	return b
}
func (b *MapParallel[K, V, R]) Shards(n int) *MapParallel[K, V, R] {
	b.policy = b.policy.Shard(n)
	return b
}
func (b *MapParallel[K, V, R]) DefaultShard() *MapParallel[K, V, R] {
	n := runtime.GOMAXPROCS(0)
	if n < 2 {
		n = 2
	}
	b.policy = b.policy.Shard(n)
	return b
}
func (b *MapParallel[K, V, R]) Buf(size int) *MapParallel[K, V, R] {
	b.policy = b.policy.Buf(size)
	return b
}
func (b *MapParallel[K, V, R]) DefaultBuf() *MapParallel[K, V, R] {
	b.policy = b.policy.Buf(0)
	return b
}
func (b *MapParallel[K, V, R]) Chunk(size int) *MapParallel[K, V, R] {
	b.policy = b.policy.Chunk(size)
	return b
}
func (b *MapParallel[K, V, R]) DefaultChunk() *MapParallel[K, V, R] {
	b.policy = b.policy.Chunk(100)
	return b
}
func (b *MapParallel[K, V, R]) Logger(l core.Logger) *MapParallel[K, V, R] {
	core.SetLogger(l)
	return b
}

func (b *MapParallel[K, V, R]) ForEach(fn func(context.Context, K, V) error) *MapForEachResult {
	total, failCnt, firstErr := NewMapRunner[K, V, R](b.ctx, b.data, b.policy).ForEach(fn)
	return &MapForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

// Map transforms values in parallel mode, returning *MapResult with embedded error.
func (b *MapParallel[K, V, R]) Map(fn func(context.Context, K, V) (R, error)) *MapResult[K, R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Map(fn)
}

// MapToFn backward compatible.
func (b *MapParallel[K, V, R]) MapToFn(fn func(context.Context, K, V) (R, error)) *MapResult[K, R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Map(fn)
}

func (b *MapParallel[K, V, R]) Filter(fn func(context.Context, K, V) bool) (*Map[K, V], error) {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Filter(fn)
}
func (b *MapParallel[K, V, R]) Stream(fn func(context.Context, K, V) (R, error), bufSize int) <-chan core.Result[R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Stream(fn, bufSize)
}
func (b *MapParallel[K, V, R]) Reduce(initial R, fn func(context.Context, R, K, V) (R, error)) (R, error) {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Reduce(initial, fn)
}
func (b *MapParallel[K, V, R]) MapBatch(fn func(context.Context, []MapEntry[K, V]) (R, error)) *MapResult[K, R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).MapBatch(fn)
}
func (b *MapParallel[K, V, R]) ForEachBatch(fn func(context.Context, []MapEntry[K, V]) error) *MapForEachResult {
	total, failCnt, firstErr := NewMapRunner[K, V, R](b.ctx, b.data, b.policy).ForEachBatch(fn)
	return &MapForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}
