// Package maps 提供泛型 key-value 领域对象 Map[K, V]，
// 将 Go 原生 map 的所有操作统一挂载到 Map 对象上，支持链式调用。
//
// 设计理念：
//   - K 为 comparable 类型的键
//   - V 为任意类型的值
//   - 所有方法返回 *Map[K, V] 自身，支持链式调用
//   - 完全对齐 Go 标准库 map 的所有操作语义
//
// 使用示例：
//
//	m := maps.New[string, int](8)
//	m.Set("a", 1).Set("b", 2).Set("c", 3)
//	m.Filter(func(k string, v int) bool { return v > 1 }).Values() // [2, 3]
//	m.Range(func(k string, v int) bool { fmt.Println(k, v); return true })
package maps

import stdmaps "maps"

// Map 泛型 key-value 容器。
// K：键类型，必须为 comparable。
// V：值类型，任意类型。
type Map[K comparable, V any] struct {
	data map[K]V
}

// New 创建空 Map，预分配 capacity 大小的空间。
func New[K comparable, V any](capacity int) *Map[K, V] {
	return &Map[K, V]{data: make(map[K]V, capacity)}
}

// NewMap 创建空 Map，不预分配空间。
func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{data: make(map[K]V)}
}

// From 从原生 map 创建 Map，会深拷贝数据。
func From[K comparable, V any](m map[K]V) *Map[K, V] {
	if m == nil {
		return &Map[K, V]{data: make(map[K]V)}
	}
	return &Map[K, V]{data: stdmaps.Clone(m)}
}

// FromRef 从原生 map 创建 Map，直接持有引用（不拷贝，外部修改会影响 Map 内部数据）。
func FromRef[K comparable, V any](m map[K]V) *Map[K, V] {
	if m == nil {
		return &Map[K, V]{data: make(map[K]V)}
	}
	return &Map[K, V]{data: m}
}

// ──────────────────────────── 基础读写 ────────────────────────────

// Get 获取键对应的值，key 不存在时返回零值和 false。
func (m *Map[K, V]) Get(key K) (V, bool) {
	v, ok := m.data[key]
	return v, ok
}

// Set 设置键值对，覆盖已有值，返回自身以支持链式调用。
func (m *Map[K, V]) Set(key K, value V) *Map[K, V] {
	m.data[key] = value
	return m
}

// SetAll 批量设置键值对，覆盖已有值，返回自身。
func (m *Map[K, V]) SetAll(entries map[K]V) *Map[K, V] {
	for k, v := range entries {
		m.data[k] = v
	}
	return m
}

// Delete 删除一个或多个键，不存在时无操作，返回自身。
func (m *Map[K, V]) Delete(keys ...K) *Map[K, V] {
	for _, k := range keys {
		delete(m.data, k)
	}
	return m
}

// GetAndDelete 获取键值后删除，返回值和是否存在的标志。
func (m *Map[K, V]) GetAndDelete(key K) (V, bool) {
	v, ok := m.data[key]
	if ok {
		delete(m.data, key)
	}
	return v, ok
}

// GetOrDefault 获取键值，不存在时返回默认值。
func (m *Map[K, V]) GetOrDefault(key K, defaultVal V) V {
	if v, ok := m.data[key]; ok {
		return v
	}
	return defaultVal
}

// GetOrSet 获取已有值，不存在时设置默认值并返回。
// 第二个返回值 true 表示 key 之前不存在，已设置新值。
func (m *Map[K, V]) GetOrSet(key K, defaultVal V) (V, bool) {
	if v, ok := m.data[key]; ok {
		return v, false
	}
	m.data[key] = defaultVal
	return defaultVal, true
}

// ──────────────────────────── 基础查询 ────────────────────────────

// Has 检查键是否存在。
func (m *Map[K, V]) Has(key K) bool {
	_, ok := m.data[key]
	return ok
}

// Len 返回键值对数量。
func (m *Map[K, V]) Len() int {
	return len(m.data)
}

// IsEmpty 返回 Map 是否为空。
func (m *Map[K, V]) IsEmpty() bool {
	return len(m.data) == 0
}

// Clear 清空所有键值对，返回自身。
func (m *Map[K, V]) Clear() *Map[K, V] {
	clear(m.data)
	return m
}

// ──────────────────────────── 遍历 ────────────────────────────

// Range 遍历所有键值对，fn 返回 false 时提前终止，返回自身。
func (m *Map[K, V]) Range(fn func(K, V) bool) *Map[K, V] {
	for k, v := range m.data {
		if !fn(k, v) {
			break
		}
	}
	return m
}

// ForEach 遍历所有键值对（不可提前终止），返回自身。
func (m *Map[K, V]) ForEach(fn func(K, V)) *Map[K, V] {
	for k, v := range m.data {
		fn(k, v)
	}
	return m
}

// ──────────────────────────── 提取 ────────────────────────────

// Keys 返回所有键的切片，顺序不确定。
func (m *Map[K, V]) Keys() []K {
	keys := make([]K, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

// Values 返回所有值的切片，顺序不确定。
func (m *Map[K, V]) Values() []V {
	vals := make([]V, 0, len(m.data))
	for _, v := range m.data {
		vals = append(vals, v)
	}
	return vals
}

// Clone 返回底层原生 map 的浅拷贝。
func (m *Map[K, V]) Clone() map[K]V {
	return stdmaps.Clone(m.data)
}

// AsMap 返回底层原生 map 的直接引用（不拷贝，外部修改会影响内部数据）。
func (m *Map[K, V]) AsMap() map[K]V {
	return m.data
}

// ──────────────────────────── 过滤与变换 ────────────────────────────

// Filter 过滤键值对，保留 fn 返回 true 的条目，原地操作后返回自身。
func (m *Map[K, V]) Filter(fn func(K, V) bool) *Map[K, V] {
	for k, v := range m.data {
		if !fn(k, v) {
			delete(m.data, k)
		}
	}
	return m
}

// FilterKeys 过滤键，保留 fn 返回 true 的条目，原地操作后返回自身。
func (m *Map[K, V]) FilterKeys(fn func(K) bool) *Map[K, V] {
	return m.Filter(func(k K, _ V) bool { return fn(k) })
}

// FilterValues 过滤值，保留 fn 返回 true 的条目，原地操作后返回自身。
func (m *Map[K, V]) FilterValues(fn func(V) bool) *Map[K, V] {
	return m.Filter(func(_ K, v V) bool { return fn(v) })
}

// Reject 删除 fn 返回 true 的条目（与 Filter 语义相反），原地操作后返回自身。
func (m *Map[K, V]) Reject(fn func(K, V) bool) *Map[K, V] {
	return m.Filter(func(k K, v V) bool { return !fn(k, v) })
}

// MapValues 原地变换所有值，fn 接收 key 和 value 返回新 value，返回自身。
func (m *Map[K, V]) MapValues(fn func(K, V) V) *Map[K, V] {
	for k, v := range m.data {
		m.data[k] = fn(k, v)
	}
	return m
}

// ──────────────────────────── 合并 ────────────────────────────

// Merge 将原生 map 的键值对合并到当前 Map，同名键会被覆盖，返回自身。
func (m *Map[K, V]) Merge(other map[K]V) *Map[K, V] {
	for k, v := range other {
		m.data[k] = v
	}
	return m
}

// MergeMap 将另一个 Map 合并到当前 Map，同名键会被覆盖，返回自身。
func (m *Map[K, V]) MergeMap(other *Map[K, V]) *Map[K, V] {
	if other == nil {
		return m
	}
	return m.Merge(other.data)
}

// MergeWithDefault 将原生 map 合并到当前 Map，同名键不会覆盖已有值，返回自身。
func (m *Map[K, V]) MergeWithDefault(other map[K]V) *Map[K, V] {
	for k, v := range other {
		if _, ok := m.data[k]; !ok {
			m.data[k] = v
		}
	}
	return m
}

// ──────────────────────────── 集合运算 ────────────────────────────

// Intersect 返回当前 Map 与 other 的交集，返回新 Map。
func (m *Map[K, V]) Intersect(other *Map[K, V]) *Map[K, V] {
	result := New[K, V](min(m.Len(), other.Len()))
	for k, v := range m.data {
		if other.Has(k) {
			result.data[k] = v
		}
	}
	return result
}

// Difference 返回当前 Map 中有而 other 中没有的键值对，返回新 Map。
func (m *Map[K, V]) Difference(other *Map[K, V]) *Map[K, V] {
	result := New[K, V](m.Len())
	for k, v := range m.data {
		if !other.Has(k) {
			result.data[k] = v
		}
	}
	return result
}

// Union 返回当前 Map 与 other 的并集，同名键以 other 的值覆盖，返回新 Map。
func (m *Map[K, V]) Union(other *Map[K, V]) *Map[K, V] {
	result := New[K, V](m.Len() + other.Len())
	for k, v := range m.data {
		result.data[k] = v
	}
	for k, v := range other.data {
		result.data[k] = v
	}
	return result
}

// ──────────────────────────── 条件查询 ────────────────────────────

// Find 查找第一个满足 fn 的键值对。
func (m *Map[K, V]) Find(fn func(K, V) bool) (key K, val V, found bool) {
	for k, v := range m.data {
		if fn(k, v) {
			return k, v, true
		}
	}
	return
}

// All 检查是否所有键值对都满足 fn，空 Map 返回 true。
func (m *Map[K, V]) All(fn func(K, V) bool) bool {
	for k, v := range m.data {
		if !fn(k, v) {
			return false
		}
	}
	return true
}

// Any 检查是否存在任意键值对满足 fn，空 Map 返回 false。
func (m *Map[K, V]) Any(fn func(K, V) bool) bool {
	for k, v := range m.data {
		if fn(k, v) {
			return true
		}
	}
	return false
}

// Count 统计满足 fn 的键值对数量。
func (m *Map[K, V]) Count(fn func(K, V) bool) int {
	n := 0
	for k, v := range m.data {
		if fn(k, v) {
			n++
		}
	}
	return n
}

// ──────────────────────────── 类型变换（包级函数）────────────────────────────

// RemapValues 将值类型从 V 变换为 V2，返回新类型的 *Map[K, V2]。
// 原 Map 不受影响。
func RemapValues[K comparable, V any, V2 any](m *Map[K, V], fn func(K, V) V2) *Map[K, V2] {
	result := New[K, V2](m.Len())
	for k, v := range m.data {
		result.data[k] = fn(k, v)
	}
	return result
}

// RemapKeys 将键类型从 K 变换为 K2，返回新类型的 *Map[K2, V]。
// 如果多个键映射到同一个新键，后面的值覆盖前面的。原 Map 不受影响。
func RemapKeys[K comparable, V any, K2 comparable](m *Map[K, V], fn func(K, V) K2) *Map[K2, V] {
	result := New[K2, V](m.Len())
	for k, v := range m.data {
		result.data[fn(k, v)] = v
	}
	return result
}

// ToSlice 将 Map 转换为切片，fn 将每个键值对映射为一个元素。
func ToSlice[K comparable, V any, R any](m *Map[K, V], fn func(K, V) R) []R {
	result := make([]R, 0, m.Len())
	for k, v := range m.data {
		result = append(result, fn(k, v))
	}
	return result
}

// Invert 翻转键值对，原值作为新键，原键作为新值。
// 多个键映射到同一新键时，后面的值覆盖前面的。
func Invert[K comparable, V comparable](m *Map[K, V]) *Map[V, K] {
	result := New[V, K](m.Len())
	for k, v := range m.data {
		result.data[v] = k
	}
	return result
}
