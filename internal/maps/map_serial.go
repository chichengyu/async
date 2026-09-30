package maps

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/sliceops"
)

// MapSerial is the serial map chain builder.
type MapSerial[K comparable, V any, R any] struct {
	*MapBase[K, V, *MapSerial[K, V, R]]
}

func (b *MapSerial[K, V, R]) Context(ctx context.Context) *MapSerial[K, V, R] {
	b.ctx = core.EnsureTraceID(ctx)
	return b
}

func (b *MapSerial[K, V, R]) ToParallel() *MapParallel[K, V, R] {
	p := &MapParallel[K, V, R]{}
	p.MapBase = newMapBase(b.ctx, b.data, sliceops.DefPar(), p)
	return p
}

func (b *MapSerial[K, V, R]) FailFast() *MapSerial[K, V, R]   { b.policy = b.policy.FF(); return b }
func (b *MapSerial[K, V, R]) NoFailFast() *MapSerial[K, V, R] { b.policy = b.policy.NoFF(); return b }
func (b *MapSerial[K, V, R]) Timeout(d time.Duration) *MapSerial[K, V, R] {
	b.policy = b.policy.TO(d)
	return b
}
func (b *MapSerial[K, V, R]) DefaultTimeout() *MapSerial[K, V, R] {
	b.policy = b.policy.TO(defaultMapTimeout)
	return b
}
func (b *MapSerial[K, V, R]) Logger(l core.Logger) *MapSerial[K, V, R] { core.SetLogger(l); return b }

func (b *MapSerial[K, V, R]) ForEach(fn func(context.Context, K, V) error) *MapForEachResult {
	total, failCnt, firstErr := NewMapRunner[K, V, R](b.ctx, b.data, b.policy).ForEach(fn)
	return &MapForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

// Map transforms values in serial mode, returning *MapResult with embedded error.
func (b *MapSerial[K, V, R]) Map(fn func(context.Context, K, V) (R, error)) *MapResult[K, R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Map(fn)
}

// MapToFn backward compatible.
func (b *MapSerial[K, V, R]) MapToFn(fn func(context.Context, K, V) (R, error)) *MapResult[K, R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Map(fn)
}

func (b *MapSerial[K, V, R]) Filter(fn func(context.Context, K, V) bool) (*Map[K, V], error) {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Filter(fn)
}
func (b *MapSerial[K, V, R]) Stream(fn func(context.Context, K, V) (R, error), bufSize int) <-chan core.Result[R] {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Stream(fn, bufSize)
}
func (b *MapSerial[K, V, R]) Reduce(initial R, fn func(context.Context, R, K, V) (R, error)) (R, error) {
	return NewMapRunner[K, V, R](b.ctx, b.data, b.policy).Reduce(initial, fn)
}
