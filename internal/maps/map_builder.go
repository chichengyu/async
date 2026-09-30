package maps

import (
	"context"
	"runtime"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/sliceops"
)

const defaultMapTimeout = 30 * time.Second

// MapChain is the generic map concurrent operation chain builder.
// Default: parallel mode (DefPar concurrency). Use Serial() for serial.
type MapChain[K comparable, V any, R any] struct {
	*MapBase[K, V, *MapChain[K, V, R]]
}

func NewMapChain[K comparable, V any](ctx context.Context, data map[K]V) *MapChain[K, V, V] {
	mc := &MapChain[K, V, V]{}
	mc.MapBase = newMapBase(core.EnsureTraceID(ctx), data, sliceops.DefPar(), mc)
	return mc
}

func NewMapChainWith[K comparable, V any, R any](ctx context.Context, data map[K]V) *MapChain[K, V, R] {
	mc := &MapChain[K, V, R]{}
	mc.MapBase = newMapBase(core.EnsureTraceID(ctx), data, sliceops.DefPar(), mc)
	return mc
}

func (b *MapChain[K, V, R]) Context(ctx context.Context) *MapChain[K, V, R] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

func (b *MapChain[K, V, R]) Serial() *MapSerial[K, V, R] {
	s := &MapSerial[K, V, R]{}
	s.MapBase = newMapBase(b.ctx, b.data, sliceops.Seq(), s)
	return s
}

func (b *MapChain[K, V, R]) Parallel() *MapParallel[K, V, R] {
	p := &MapParallel[K, V, R]{}
	p.MapBase = newMapBase(b.ctx, b.data, sliceops.DefPar(), p)
	return p
}

func (b *MapChain[K, V, R]) Logger(l core.Logger) *MapChain[K, V, R] { core.SetLogger(l); return b }
func (b *MapChain[K, V, R]) DefaultLogger() *MapChain[K, V, R]       { core.SetLogger(nil); return b }
func (b *MapChain[K, V, R]) Worker(n int) *MapChain[K, V, R]         { b.policy = sliceops.Par(n); return b }
func (b *MapChain[K, V, R]) DefaultWorker() *MapChain[K, V, R] {
	b.policy = sliceops.DefPar()
	return b
}
func (b *MapChain[K, V, R]) DefaultPool() *MapChain[K, V, R] {
	b.policy = b.policy.WithDefaultPool()
	return b
}
func (b *MapChain[K, V, R]) Pool(p *pool.Pool[R]) *MapChain[K, V, R] {
	b.policy = b.policy.WithPool(p)
	return b
}
func (b *MapChain[K, V, R]) PoolAuto(p *pool.Pool[R]) *MapChain[K, V, R] {
	b.policy = b.policy.WithOwnedPool(p)
	return b
}
func (b *MapChain[K, V, R]) Timeout(d time.Duration) *MapChain[K, V, R] {
	b.policy = b.policy.TO(d)
	return b
}
func (b *MapChain[K, V, R]) DefaultTimeout() *MapChain[K, V, R] {
	b.policy = b.policy.TO(defaultMapTimeout)
	return b
}
func (b *MapChain[K, V, R]) FailFast() *MapChain[K, V, R] { b.policy = b.policy.FF(); return b }
func (b *MapChain[K, V, R]) DefaultFailFast() *MapChain[K, V, R] {
	b.policy = b.policy.NoFF()
	return b
}
func (b *MapChain[K, V, R]) Shards(n int) *MapChain[K, V, R] { b.policy = b.policy.Shard(n); return b }
func (b *MapChain[K, V, R]) DefaultShard() *MapChain[K, V, R] {
	n := runtime.GOMAXPROCS(0)
	if n < 2 {
		n = 2
	}
	b.policy = b.policy.Shard(n)
	return b
}
func (b *MapChain[K, V, R]) Buf(size int) *MapChain[K, V, R] { b.policy = b.policy.Buf(size); return b }
func (b *MapChain[K, V, R]) DefaultBuf() *MapChain[K, V, R]  { b.policy = b.policy.Buf(0); return b }

func (b *MapChain[K, V, R]) Chunk(size int) *MapChain[K, V, R] {
	b.policy = b.policy.Chunk(size)
	return b
}
func (b *MapChain[K, V, R]) DefaultChunk() *MapChain[K, V, R] {
	b.policy = b.policy.Chunk(100)
	return b
}

func (b *MapChain[K, V, R]) ForEach(fn func(context.Context, K, V) error) *MapForEachResult {
	total, failCnt, firstErr := NewMapRunner[K, V, R](b.ctx, b.data, b.policy).ForEach(fn)
	return &MapForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

// Map transforms values, returning *MapResult with embedded error. Mirrors SliceBuilder.Map.
func (b *MapChain[K, V, R]) Map(fn func(context.Context, K, V) (R, error)) *MapResult[K, R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Map(fn)
}

// MapToFn transforms values, returning (*MapResult, error) separately. Backward compatible.
func (b *MapChain[K, V, R]) MapToFn(fn func(context.Context, K, V) (R, error)) *MapResult[K, R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Map(fn)
}

func (b *MapChain[K, V, R]) Filter(fn func(context.Context, K, V) bool) (*Map[K, V], error) {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Filter(fn)
}
func (b *MapChain[K, V, R]) Stream(fn func(context.Context, K, V) (R, error), bufSize int) <-chan core.Result[R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Stream(fn, bufSize)
}
func (b *MapChain[K, V, R]) Reduce(initial R, fn func(context.Context, R, K, V) (R, error)) (R, error) {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Reduce(initial, fn)
}
func (b *MapChain[K, V, R]) MapBatch(fn func(context.Context, []MapEntry[K, V]) (R, error)) *MapResult[K, R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).MapBatch(fn)
}
func (b *MapChain[K, V, R]) ForEachBatch(fn func(context.Context, []MapEntry[K, V]) error) *MapForEachResult {
	total, failCnt, firstErr := NewMapRunner[K, V, R](b.ctx, b.data, b.policy).ForEachBatch(fn)
	return &MapForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

// Explicit MapBase delegates help gopls resolve generic types on embedded methods.

func (b *MapChain[K, V, R]) Has(key K) bool      { return b.MapBase.Has(key) }
func (b *MapChain[K, V, R]) Get(key K) (V, bool) { return b.MapBase.Get(key) }
func (b *MapChain[K, V, R]) Set(key K, value V) *MapChain[K, V, R] {
	b.MapBase.Set(key, value)
	return b
}
func (b *MapChain[K, V, R]) SetAll(entries map[K]V) *MapChain[K, V, R] {
	b.MapBase.SetAll(entries)
	return b
}
func (b *MapChain[K, V, R]) Delete(keys ...K) *MapChain[K, V, R] {
	b.MapBase.Delete(keys...)
	return b
}
func (b *MapChain[K, V, R]) Clear() *MapChain[K, V, R] { b.MapBase.Clear(); return b }
func (b *MapChain[K, V, R]) Find(fn func(K, V) bool) (K, V, bool) {
	return b.MapBase.Find(fn)
}
func (b *MapChain[K, V, R]) All(fn func(K, V) bool) bool  { return b.MapBase.All(fn) }
func (b *MapChain[K, V, R]) Any(fn func(K, V) bool) bool  { return b.MapBase.Any(fn) }
func (b *MapChain[K, V, R]) Count(fn func(K, V) bool) int { return b.MapBase.Count(fn) }
func (b *MapChain[K, V, R]) Range(fn func(K, V) bool) *MapChain[K, V, R] {
	b.MapBase.Range(fn)
	return b
}
func (b *MapChain[K, V, R]) FilterKV(fn func(K, V) bool) *MapChain[K, V, R] {
	b.MapBase.Filter(fn)
	return b
}
func (b *MapChain[K, V, R]) Reject(fn func(K, V) bool) *MapChain[K, V, R] {
	b.MapBase.Reject(fn)
	return b
}
func (b *MapChain[K, V, R]) MapValues(fn func(K, V) V) *MapChain[K, V, R] {
	b.MapBase.MapValues(fn)
	return b
}
func (b *MapChain[K, V, R]) Merge(other map[K]V) *MapChain[K, V, R] {
	b.MapBase.Merge(other)
	return b
}
func (b *MapChain[K, V, R]) MergeMap(m *Map[K, V]) *MapChain[K, V, R] {
	b.MapBase.MergeMap(m)
	return b
}
func (b *MapChain[K, V, R]) MergeWithDefault(other map[K]V) *MapChain[K, V, R] {
	b.MapBase.MergeWithDefault(other)
	return b
}
