// Package sliceops 提供泛型切片领域对象 Slice[T, R]，
// 将所有切片操作（纯数据操作 + MapReduce 并发操作）统一挂载到 Slice 对象上。
//
// 设计理念：
//   - T 为当前切片元素类型
//   - R 为 Map/Reduce 操作的目标输出类型
//   - R 提升到 struct 级别，使所有方法无需引入新的类型参数
//
// 使用示例：
//
//	// 纯数据操作
//	s := sliceops.New[int]([]int{3, 1, 2})
//	s.SortFunc(func(a, b int) int { return a - b }).Filter(func(i int) bool { return i > 0 })
//
//	// 声明输出类型的 MapReduce
//	s := sliceops.NewWithResult[int, string]([]int{1, 2, 3})
//	results := s.Map(ctx, 8, func(ctx context.Context, i int) (string, error) {
//	    return strconv.Itoa(i), nil
//	})
package sliceops

import (
	"math/rand"
	"sort"
)

// Slice 泛型切片领域对象。
// T：当前切片元素类型
// R：Map/Reduce 操作的目标输出类型
type Slice[T any, R any] struct {
	items []T
}

// New 创建 Slice 对象，默认 R=T（适用于纯切片操作，无需关注 R）。
func New[T any](items []T) *Slice[T, T] {
	if items == nil {
		items = []T{}
	}
	return &Slice[T, T]{items: items}
}

// NewWithResult 创建 Slice 对象并显式声明 R 类型（适用于 T→R 的 Map/Reduce 操作）。
func NewWithResult[T any, R any](items []T) *Slice[T, R] {
	if items == nil {
		items = []T{}
	}
	return &Slice[T, R]{items: items}
}

// ──────────────────────────── 基础查询 ────────────────────────────

// Values 返回所有元素的切片副本。
func (s *Slice[T, R]) Values() []T {
	out := make([]T, len(s.items))
	copy(out, s.items)
	return out
}

// Items 返回内部切片引用（不复制，修改会反映到 Slice 内部）。
func (s *Slice[T, R]) Items() []T {
	return s.items
}

// Len 返回元素个数。
func (s *Slice[T, R]) Len() int {
	return len(s.items)
}

// IsEmpty 返回切片是否为空。
func (s *Slice[T, R]) IsEmpty() bool {
	return len(s.items) == 0
}

// First 返回第一个元素，空切片则返回零值和 false。
func (s *Slice[T, R]) First() (val T, found bool) {
	if len(s.items) == 0 {
		return
	}
	return s.items[0], true
}

// Last 返回最后一个元素，空切片则返回零值和 false。
func (s *Slice[T, R]) Last() (val T, found bool) {
	if len(s.items) == 0 {
		return
	}
	return s.items[len(s.items)-1], true
}

// ──────────────────────────── 排序 ────────────────────────────

// SortFunc 使用 cmp 对切片进行原地排序，返回自身。
func (s *Slice[T, R]) SortFunc(cmp func(a, b T) int) *Slice[T, R] {
	sort.Slice(s.items, func(i, j int) bool {
		return cmp(s.items[i], s.items[j]) < 0
	})
	return s
}

// StableSortFunc 使用 cmp 对切片进行原地稳定排序，返回自身。
func (s *Slice[T, R]) StableSortFunc(cmp func(a, b T) int) *Slice[T, R] {
	sort.SliceStable(s.items, func(i, j int) bool {
		return cmp(s.items[i], s.items[j]) < 0
	})
	return s
}

// IsSortedFunc 检查切片是否已按 cmp 有序。
func (s *Slice[T, R]) IsSortedFunc(cmp func(a, b T) int) bool {
	return sort.SliceIsSorted(s.items, func(i, j int) bool {
		return cmp(s.items[i], s.items[j]) < 0
	})
}

// Reverse 原地反转切片，返回自身。
func (s *Slice[T, R]) Reverse() *Slice[T, R] {
	for i, j := 0, len(s.items)-1; i < j; i, j = i+1, j-1 {
		s.items[i], s.items[j] = s.items[j], s.items[i]
	}
	return s
}

// ──────────────────────────── 查找与判断 ────────────────────────────

// ContainsFunc 判断切片中是否存在满足 pred 的元素。
func (s *Slice[T, R]) ContainsFunc(pred func(T) bool) bool {
	for _, v := range s.items {
		if pred(v) {
			return true
		}
	}
	return false
}

// IndexFunc 返回首个满足 pred 的元素的索引，-1 表示不存在。
func (s *Slice[T, R]) IndexFunc(pred func(T) bool) int {
	for i, v := range s.items {
		if pred(v) {
			return i
		}
	}
	return -1
}

// FindFunc 返回首个满足 pred 的元素。
func (s *Slice[T, R]) FindFunc(pred func(T) bool) (val T, found bool) {
	for _, v := range s.items {
		if pred(v) {
			return v, true
		}
	}
	return
}

// FindLastFunc 返回最后一个满足 pred 的元素。
func (s *Slice[T, R]) FindLastFunc(pred func(T) bool) (val T, found bool) {
	for i := len(s.items) - 1; i >= 0; i-- {
		if pred(s.items[i]) {
			return s.items[i], true
		}
	}
	return
}

// All 检查所有元素是否都满足 pred。
func (s *Slice[T, R]) All(pred func(T) bool) bool {
	for _, v := range s.items {
		if !pred(v) {
			return false
		}
	}
	return true
}

// Any 检查是否存在任意元素满足 pred。
func (s *Slice[T, R]) Any(pred func(T) bool) bool {
	for _, v := range s.items {
		if pred(v) {
			return true
		}
	}
	return false
}

// Count 统计满足 pred 的元素个数。
func (s *Slice[T, R]) Count(pred func(T) bool) int {
	n := 0
	for _, v := range s.items {
		if pred(v) {
			n++
		}
	}
	return n
}

// MaxFunc 返回 cmp 下最大的元素。
func (s *Slice[T, R]) MaxFunc(cmp func(a, b T) int) (max T, found bool) {
	if len(s.items) == 0 {
		return
	}
	max = s.items[0]
	for _, v := range s.items[1:] {
		if cmp(v, max) > 0 {
			max = v
		}
	}
	return max, true
}

// MinFunc 返回 cmp 下最小的元素。
func (s *Slice[T, R]) MinFunc(cmp func(a, b T) int) (min T, found bool) {
	if len(s.items) == 0 {
		return
	}
	min = s.items[0]
	for _, v := range s.items[1:] {
		if cmp(v, min) < 0 {
			min = v
		}
	}
	return min, true
}

// BinarySearchFunc 在有序切片中二分查找 target。
func (s *Slice[T, R]) BinarySearchFunc(target T, cmp func(a, b T) int) (int, bool) {
	idx := sort.Search(len(s.items), func(i int) bool {
		return cmp(s.items[i], target) >= 0
	})
	if idx < len(s.items) && cmp(s.items[idx], target) == 0 {
		return idx, true
	}
	return idx, false
}

// ──────────────────────────── 过滤与去重 ────────────────────────────

// Filter 保留满足 pred 的元素，原地过滤后返回自身。
func (s *Slice[T, R]) Filter(pred func(T) bool) *Slice[T, R] {
	n := 0
	for _, v := range s.items {
		if pred(v) {
			s.items[n] = v
			n++
		}
	}
	s.items = s.items[:n]
	return s
}

// CompactFunc 将连续相等（按 eq 判断）的元素去重，原地操作后返回自身。
func (s *Slice[T, R]) CompactFunc(eq func(a, b T) bool) *Slice[T, R] {
	if len(s.items) < 2 {
		return s
	}
	n := 1
	for i := 1; i < len(s.items); i++ {
		if !eq(s.items[i], s.items[n-1]) {
			s.items[n] = s.items[i]
			n++
		}
	}
	s.items = s.items[:n]
	return s
}

// DedupFunc 移除所有重复元素（不考虑是否连续，按 eq 判断），原地操作后返回自身。
func (s *Slice[T, R]) DedupFunc(eq func(a, b T) bool) *Slice[T, R] {
	if len(s.items) < 2 {
		return s
	}
	seen := make(map[any]struct{}, len(s.items))
	n := 0
	for _, v := range s.items {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			s.items[n] = v
			n++
		}
	}
	s.items = s.items[:n]
	return s
}

// ──────────────────────────── 增删改 ────────────────────────────

// Append 在末尾追加元素，返回自身。
func (s *Slice[T, R]) Append(items ...T) *Slice[T, R] {
	s.items = append(s.items, items...)
	return s
}

// Prepend 在头部插入元素，返回自身。
func (s *Slice[T, R]) Prepend(items ...T) *Slice[T, R] {
	s.items = append(items, s.items...)
	return s
}

// Insert 在指定位置插入元素，返回自身。若 idx < 0 插到头部，idx > len 插到尾部。
func (s *Slice[T, R]) Insert(idx int, items ...T) *Slice[T, R] {
	n := len(s.items)
	if idx < 0 {
		idx = 0
	}
	if idx > n {
		idx = n
	}
	m := len(items)
	if m == 0 {
		return s
	}
	s.items = append(s.items, make([]T, m)...)
	copy(s.items[idx+m:], s.items[idx:])
	copy(s.items[idx:], items)
	return s
}

// Delete 删除指定位置的元素，返回自身。
func (s *Slice[T, R]) Delete(idx int) *Slice[T, R] {
	if idx < 0 || idx >= len(s.items) {
		return s
	}
	s.items = append(s.items[:idx], s.items[idx+1:]...)
	return s
}

// DeleteRange 删除 [i, j) 范围内的元素，返回自身。
func (s *Slice[T, R]) DeleteRange(i, j int) *Slice[T, R] {
	n := len(s.items)
	if i < 0 {
		i = 0
	}
	if j > n {
		j = n
	}
	if i >= j {
		return s
	}
	s.items = append(s.items[:i], s.items[j:]...)
	return s
}

// Replace 将 [i, j) 范围内的元素替换为 items，返回自身。
func (s *Slice[T, R]) Replace(i, j int, items ...T) *Slice[T, R] {
	n := len(s.items)
	if i < 0 {
		i = 0
	}
	if j > n {
		j = n
	}
	if i > j {
		i = j
	}
	keep := len(s.items) - (j - i)
	newLen := keep + len(items)
	if cap(s.items) >= newLen {
		s.items = s.items[:newLen]
	} else {
		newSlice := make([]T, newLen)
		copy(newSlice, s.items[:i])
		copy(newSlice[i+len(items):], s.items[j:])
		s.items = newSlice
		copy(s.items[i:], items)
		return s
	}
	copy(s.items[i+len(items):], s.items[j:])
	copy(s.items[i:], items)
	return s
}

// ──────────────────────────── 截取 ────────────────────────────

// Take 保留前 n 个元素，返回自身。
func (s *Slice[T, R]) Take(n int) *Slice[T, R] {
	if n <= 0 {
		s.items = s.items[:0]
		return s
	}
	if n < len(s.items) {
		s.items = s.items[:n]
	}
	return s
}

// Drop 丢弃前 n 个元素，返回自身。
func (s *Slice[T, R]) Drop(n int) *Slice[T, R] {
	if n <= 0 {
		return s
	}
	if n >= len(s.items) {
		s.items = s.items[:0]
		return s
	}
	s.items = s.items[n:]
	return s
}

// SliceRange 保留 [i, j) 范围的元素，返回自身。
func (s *Slice[T, R]) SliceRange(i, j int) *Slice[T, R] {
	n := len(s.items)
	if i < 0 {
		i = 0
	}
	if j > n {
		j = n
	}
	if i >= j {
		s.items = s.items[:0]
		return s
	}
	s.items = s.items[i:j]
	return s
}

// Clip 释放未使用的容量，返回自身。
func (s *Slice[T, R]) Clip() *Slice[T, R] {
	if cap(s.items) > len(s.items) {
		clipped := make([]T, len(s.items))
		copy(clipped, s.items)
		s.items = clipped
	}
	return s
}

// Grow 增加切片容量以容纳 n 个额外元素，返回自身。
func (s *Slice[T, R]) Grow(n int) *Slice[T, R] {
	if n <= 0 {
		return s
	}
	s.items = append(s.items, make([]T, n)...)
	s.items = s.items[:len(s.items)-n]
	return s
}

// ──────────────────────────── 随机与克隆 ────────────────────────────

// Shuffle 使用随机源原地打乱切片，返回自身。
func (s *Slice[T, R]) Shuffle() *Slice[T, R] {
	rand.Shuffle(len(s.items), func(i, j int) {
		s.items[i], s.items[j] = s.items[j], s.items[i]
	})
	return s
}

// Clone 返回切片的浅拷贝副本。
func (s *Slice[T, R]) Clone() []T {
	out := make([]T, len(s.items))
	copy(out, s.items)
	return out
}

// Repeat 将切片重复 n 次后替换内部数据，返回自身。
func (s *Slice[T, R]) Repeat(n int) *Slice[T, R] {
	if n <= 0 {
		s.items = s.items[:0]
		return s
	}
	if n == 1 {
		return s
	}
	orig := make([]T, len(s.items))
	copy(orig, s.items)
	for i := 1; i < n; i++ {
		s.items = append(s.items, orig...)
	}
	return s
}

// ──────────────────────────── 分块与拆分 ────────────────────────────

// Chunk 将切片按大小分块，返回 [][]T（不再封装为 Slice）。
func (s *Slice[T, R]) Chunk(chunkSize int) [][]T {
	if chunkSize <= 0 || len(s.items) == 0 {
		return nil
	}
	chunks := make([][]T, 0, (len(s.items)+chunkSize-1)/chunkSize)
	for i := 0; i < len(s.items); i += chunkSize {
		end := i + chunkSize
		if end > len(s.items) {
			end = len(s.items)
		}
		chunks = append(chunks, s.items[i:end])
	}
	return chunks
}

// ChunkN 将切片均分为 n 份，返回 [][]T。
func (s *Slice[T, R]) ChunkN(n int) [][]T {
	if n <= 0 || len(s.items) == 0 {
		return nil
	}
	chunkSize := (len(s.items) + n - 1) / n
	return s.Chunk(chunkSize)
}

// Split 按条件将切片拆分为匹配和不匹配两部分，返回两个独立切片。
func (s *Slice[T, R]) Split(pred func(T) bool) (matched []T, unmatched []T) {
	for _, v := range s.items {
		if pred(v) {
			matched = append(matched, v)
		} else {
			unmatched = append(unmatched, v)
		}
	}
	return
}
