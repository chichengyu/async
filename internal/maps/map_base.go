package maps

import (
	"context"

	"github.com/chichengyu/async/internal/sliceops"
)

// MapBase is the shared base for all map chain builders.
// Methods returning S enable chainable data operations across all builder variants.
type MapBase[K comparable, V any, S any] struct {
	ctx    context.Context
	data   map[K]V
	policy sliceops.Policy
	self   S
}

func newMapBase[K comparable, V any, S any](ctx context.Context, data map[K]V, policy sliceops.Policy, self S) *MapBase[K, V, S] {
	return &MapBase[K, V, S]{ctx: ctx, data: data, policy: policy, self: self}
}

func (b *MapBase[K, V, S]) Len() int      { return len(b.data) }
func (b *MapBase[K, V, S]) IsEmpty() bool { return len(b.data) == 0 }

func (b *MapBase[K, V, S]) Keys() []K {
	keys := make([]K, 0, len(b.data))
	for k := range b.data {
		keys = append(keys, k)
	}
	return keys
}

func (b *MapBase[K, V, S]) Values() []V {
	vals := make([]V, 0, len(b.data))
	for _, v := range b.data {
		vals = append(vals, v)
	}
	return vals
}

func (b *MapBase[K, V, S]) AsMap() map[K]V { return b.data }

func (b *MapBase[K, V, S]) Has(key K) bool {
	_, ok := b.data[key]
	return ok
}

func (b *MapBase[K, V, S]) Get(key K) (V, bool) {
	v, ok := b.data[key]
	return v, ok
}

func (b *MapBase[K, V, S]) Set(key K, value V) S {
	b.data[key] = value
	return b.self
}

func (b *MapBase[K, V, S]) SetAll(entries map[K]V) S {
	for k, v := range entries {
		b.data[k] = v
	}
	return b.self
}

func (b *MapBase[K, V, S]) Delete(keys ...K) S {
	for _, k := range keys {
		delete(b.data, k)
	}
	return b.self
}

func (b *MapBase[K, V, S]) Clear() S {
	clear(b.data)
	return b.self
}

func (b *MapBase[K, V, S]) Range(fn func(K, V) bool) S {
	for k, v := range b.data {
		if !fn(k, v) {
			break
		}
	}
	return b.self
}

func (b *MapBase[K, V, S]) Filter(fn func(K, V) bool) S {
	for k, v := range b.data {
		if !fn(k, v) {
			delete(b.data, k)
		}
	}
	return b.self
}

func (b *MapBase[K, V, S]) Reject(fn func(K, V) bool) S {
	return b.Filter(func(k K, v V) bool { return !fn(k, v) })
}

func (b *MapBase[K, V, S]) MapValues(fn func(K, V) V) S {
	for k, v := range b.data {
		b.data[k] = fn(k, v)
	}
	return b.self
}

func (b *MapBase[K, V, S]) Merge(other map[K]V) S {
	for k, v := range other {
		b.data[k] = v
	}
	return b.self
}

func (b *MapBase[K, V, S]) MergeMap(other *Map[K, V]) S {
	if other == nil {
		return b.self
	}
	return b.Merge(other.data)
}

func (b *MapBase[K, V, S]) MergeWithDefault(other map[K]V) S {
	for k, v := range other {
		if _, ok := b.data[k]; !ok {
			b.data[k] = v
		}
	}
	return b.self
}

func (b *MapBase[K, V, S]) Find(fn func(K, V) bool) (key K, val V, found bool) {
	for k, v := range b.data {
		if fn(k, v) {
			return k, v, true
		}
	}
	return
}

func (b *MapBase[K, V, S]) All(fn func(K, V) bool) bool {
	for k, v := range b.data {
		if !fn(k, v) {
			return false
		}
	}
	return true
}

func (b *MapBase[K, V, S]) Any(fn func(K, V) bool) bool {
	for k, v := range b.data {
		if fn(k, v) {
			return true
		}
	}
	return false
}

func (b *MapBase[K, V, S]) Count(fn func(K, V) bool) int {
	n := 0
	for k, v := range b.data {
		if fn(k, v) {
			n++
		}
	}
	return n
}
