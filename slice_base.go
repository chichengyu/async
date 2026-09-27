package async

import (
	"context"

	"github.com/chichengyu/async/internal/sliceops"
)

// sliceBase 切片构建器共享基类，通过泛型 S 实现自引用，使得共享方法返回具体构建器类型。
// 三个构建器（SliceBuilder、ParallelSlice、SerialSlice）通过嵌入 *sliceBase 复用所有纯数据操作方法。
// 新增数据操作方法只需在 sliceBase 上添加一次即可。
type sliceBase[T any, S any] struct {
	ctx    context.Context // 上下文，自动注入 TraceID，用于超时控制和日志追踪
	items  []T             // 当前持有的切片数据，所有数据操作方法在此切片上原地或复制操作
	policy sliceops.Policy // 策略配置（并发度/分片/超时/FailFast 等），由各 Builder 的配置方法更新
	self   S               // 自引用指针，使链式方法返回具体构建器类型而非 *sliceBase
}

// newSliceBase 创建 sliceBase 实例。
func newSliceBase[T any, S any](ctx context.Context, items []T, policy sliceops.Policy, self S) *sliceBase[T, S] {
	return &sliceBase[T, S]{ctx: ctx, items: items, policy: policy, self: self}
}

// ── 排序操作 ──

// Sort 使用 cmp 比较函数对切片进行非稳定排序，返回自身以支持链式调用。
//
//	cmp: 比较函数，返回 <0 表示 a 排在 b 前，>0 表示 b 排在 a 前，==0 表示相等。
//
//	go async.Slice(ctx, []int{3, 1, 2}).Sort(func(a, b int) int { return a - b })
func (b *sliceBase[T, S]) Sort(cmp func(a, b T) int) S {
	slicesSortFunc(b.items, cmp)
	return b.self
}

// StableSort 使用 cmp 比较函数对切片进行稳定排序，相等元素保持原有相对顺序。
//
//	cmp: 比较函数，规则同 Sort。
//
//	go async.Slice(ctx, items).StableSort(func(a, b MyStruct) int { return a.Priority - b.Priority })
func (b *sliceBase[T, S]) StableSort(cmp func(a, b T) int) S {
	slicesSortStableFunc(b.items, cmp)
	return b.self
}

// IsSorted 判断切片是否已按 cmp 升序排列。
//
//	cmp: 比较函数，规则同 Sort。
//
//	go async.Slice(ctx, []int{1, 2, 3}).IsSorted(func(a, b int) int { return a - b }) // true
func (b *sliceBase[T, S]) IsSorted(cmp func(a, b T) int) bool {
	return slicesIsSortedFunc(b.items, cmp)
}

// ── 变换操作 ──

// Reverse 原地反转切片元素顺序，返回自身以支持链式调用。
//
//	go async.Slice(ctx, []int{1, 2, 3}).Reverse().Values() // [3, 2, 1]
func (b *sliceBase[T, S]) Reverse() S {
	for i, j := 0, len(b.items)-1; i < j; i, j = i+1, j-1 {
		b.items[i], b.items[j] = b.items[j], b.items[i]
	}
	return b.self
}

// Filter 使用 pred 谓词过滤切片，保留 pred 返回 true 的元素，返回自身以支持链式调用。
// 元素数 >= 1000 时自动启用并行过滤。
//
//	pred: 谓词函数，返回 true 保留该元素，false 丢弃。
//
//	go async.Slice(ctx, []int{1, 2, 3, 4}).Filter(func(v int) bool { return v%2 == 0 }).Values() // [2, 4]
func (b *sliceBase[T, S]) Filter(pred func(T) bool) S {
	b.items = filterItems(b.items, pred, b.policy)
	return b.self
}

// DeleteFunc 使用 pred 谓词删除切片元素，删除 pred 返回 true 的元素，返回自身以支持链式调用。
// 与 Filter 语义相反：Filter 保留满足条件的，DeleteFunc 删除满足条件的。
//
//	pred: 谓词函数，返回 true 删除该元素。
//
//	go async.Slice(ctx, []int{1, 2, 3, 4}).DeleteFunc(func(v int) bool { return v%2 == 0 }).Values() // [1, 3]
func (b *sliceBase[T, S]) DeleteFunc(pred func(T) bool) S {
	return b.Filter(func(t T) bool { return !pred(t) })
}

// Compact 移除相邻的重复元素，使用 eq 判断是否相等，返回自身以支持链式调用。
// 注意：仅移除相邻重复，如需全局去重请使用 Dedup。
//
//	eq: 相等判断函数，返回 true 表示两元素相等。
//
//	go async.Slice(ctx, []int{1, 1, 2, 3, 3}).Compact(func(a, b int) bool { return a == b }).Values() // [1, 2, 3]
func (b *sliceBase[T, S]) Compact(eq func(a, b T) bool) S {
	if len(b.items) < 2 {
		return b.self
	}
	n := 1
	for i := 1; i < len(b.items); i++ {
		if !eq(b.items[i], b.items[n-1]) {
			b.items[n] = b.items[i]
			n++
		}
	}
	b.items = b.items[:n]
	return b.self
}

// Dedup 全局去重，移除所有重复元素（基于 map 判重），保留首次出现的元素，返回自身以支持链式调用。
// 注意：元素必须可作为 map key（即 comparable 类型），否则会 panic。
//
//	eq: 相等判断函数，返回 true 表示两元素相等。
//
//	go async.Slice(ctx, []int{3, 1, 3, 2, 1}).Dedup(func(a, b int) bool { return a == b }).Values() // [3, 1, 2]
func (b *sliceBase[T, S]) Dedup(eq func(a, b T) bool) S {
	if len(b.items) < 2 {
		return b.self
	}
	seen := make(map[any]struct{}, len(b.items))
	n := 0
	for _, v := range b.items {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			b.items[n] = v
			n++
		}
	}
	b.items = b.items[:n]
	return b.self
}

// Clip 释放切片多余容量，使 cap(slice) == len(slice)，返回自身以支持链式调用。
// 适用于持有大量切片内存场景下主动减少内存占用。
//
//	go async.Slice(ctx, items).Filter(pred).Clip() // 过滤后释放多余容量
func (b *sliceBase[T, S]) Clip() S {
	if cap(b.items) > len(b.items) {
		clipped := make([]T, len(b.items))
		copy(clipped, b.items)
		b.items = clipped
	}
	return b.self
}

// Grow 扩展切片容量 n 个元素（不改变 len），返回自身以支持链式调用。
//
//	n: 要增加的容量数。
//
//	go async.Slice(ctx, items).Grow(100).Append(v1, v2) // 预先扩容，避免 Append 多次分配
func (b *sliceBase[T, S]) Grow(n int) S {
	b.items = slicesGrow(b.items, n)
	return b.self
}

// Append 在切片末尾追加元素，返回自身以支持链式调用。
//
//	vals: 要追加的元素。
//
//	go async.Slice(ctx, []int{1, 2}).Append(3, 4).Values() // [1, 2, 3, 4]
func (b *sliceBase[T, S]) Append(vals ...T) S {
	b.items = append(b.items, vals...)
	return b.self
}

// Prepend 在切片头部插入元素，返回自身以支持链式调用。
//
//	vals: 要插入的元素，按顺序出现在切片头部。
//
//	go async.Slice(ctx, []int{3, 4}).Prepend(1, 2).Values() // [1, 2, 3, 4]
func (b *sliceBase[T, S]) Prepend(vals ...T) S {
	b.items = append(vals, b.items...)
	return b.self
}

// Insert 在指定位置 idx 插入元素，返回自身以支持链式调用。
// idx 越界时自动修正：<0 修正为 0，>= len 修正为 len（即末尾追加）。
//
//	idx:  插入位置索引。
//	vals: 要插入的元素。
//
//	go async.Slice(ctx, []int{1, 3}).Insert(1, 2).Values() // [1, 2, 3]
func (b *sliceBase[T, S]) Insert(idx int, vals ...T) S {
	n := len(b.items)
	if idx < 0 {
		idx = 0
	}
	if idx > n {
		idx = n
	}
	m := len(vals)
	if m == 0 {
		return b.self
	}
	b.items = append(b.items, make([]T, m)...)
	copy(b.items[idx+m:], b.items[idx:])
	copy(b.items[idx:], vals)
	return b.self
}

// Delete 删除索引 i 处的单个元素，返回自身以支持链式调用。
// i 越界时无操作。
//
//	i: 要删除的元素索引。
//
//	go async.Slice(ctx, []int{1, 2, 3}).Delete(1).Values() // [1, 3]
func (b *sliceBase[T, S]) Delete(i int) S {
	if i < 0 || i >= len(b.items) {
		return b.self
	}
	b.items = append(b.items[:i], b.items[i+1:]...)
	return b.self
}

// DeleteRange 删除索引范围 [i, j) 的元素，返回自身以支持链式调用。
// i < 0 修正为 0，j > len 修正为 len。
//
//	i: 起始索引（含）。
//	j: 结束索引（不含）。
//
//	go async.Slice(ctx, []int{1, 2, 3, 4}).DeleteRange(1, 3).Values() // [1, 4]
func (b *sliceBase[T, S]) DeleteRange(i, j int) S {
	n := len(b.items)
	if i < 0 {
		i = 0
	}
	if j > n {
		j = n
	}
	if i >= j {
		return b.self
	}
	b.items = append(b.items[:i], b.items[j:]...)
	return b.self
}

// Replace 替换索引范围 [i, j) 的元素为新值 vals，返回自身以支持链式调用。
//
//	i:    起始索引（含）。
//	j:    结束索引（不含）。
//	vals: 替换后的新元素。
//
//	go async.Slice(ctx, []int{1, 2, 3}).Replace(1, 2, 99).Values() // [1, 99, 3]
func (b *sliceBase[T, S]) Replace(i, j int, vals ...T) S {
	b.items = slicesReplace(b.items, i, j, vals...)
	return b.self
}

// Take 保留前 n 个元素，丢弃其余，返回自身以支持链式调用。
// n <= 0 时清空切片，n >= len 时无操作。
//
//	n: 要保留的元素数量。
//
//	go async.Slice(ctx, []int{1, 2, 3, 4}).Take(2).Values() // [1, 2]
func (b *sliceBase[T, S]) Take(n int) S {
	if n <= 0 {
		b.items = b.items[:0]
	} else if n < len(b.items) {
		b.items = b.items[:n]
	}
	return b.self
}

// Drop 丢弃前 n 个元素，保留其余，返回自身以支持链式调用。
// n <= 0 时无操作，n >= len 时清空切片。
//
//	n: 要丢弃的元素数量。
//
//	go async.Slice(ctx, []int{1, 2, 3, 4}).Drop(2).Values() // [3, 4]
func (b *sliceBase[T, S]) Drop(n int) S {
	if n <= 0 {
		return b.self
	}
	if n >= len(b.items) {
		b.items = b.items[:0]
	} else {
		b.items = b.items[n:]
	}
	return b.self
}

// SliceRange 截取 [i, j) 范围的子切片，返回自身以支持链式调用。
// i < 0 修正为 0，j > len 修正为 len。
// SliceBuilder 上的 Slice(i, j int) 方法与此等价（向后兼容命名）。
//
//	i: 起始索引（含）。
//	j: 结束索引（不含）。
//
//	go async.Slice(ctx, []int{1, 2, 3, 4}).SliceRange(1, 3).Values() // [2, 3]
func (b *sliceBase[T, S]) SliceRange(i, j int) S {
	n := len(b.items)
	if i < 0 {
		i = 0
	}
	if j > n {
		j = n
	}
	if i >= j {
		b.items = b.items[:0]
	} else {
		b.items = b.items[i:j]
	}
	return b.self
}

// Shuffle 随机打乱切片元素顺序，返回自身以支持链式调用。
//
//	go async.Slice(ctx, items).Shuffle() // 每次执行结果不同
func (b *sliceBase[T, S]) Shuffle() S {
	randShuffle(len(b.items), func(i, j int) {
		b.items[i], b.items[j] = b.items[j], b.items[i]
	})
	return b.self
}

// Repeat 将现有切片重复 n 次拼接，返回自身以支持链式调用。
// n <= 0 时清空切片，n == 1 时无操作。
//
//	n: 重复次数。
//
//	go async.Slice(ctx, []int{1, 2}).Repeat(3).Values() // [1, 2, 1, 2, 1, 2]
func (b *sliceBase[T, S]) Repeat(n int) S {
	if n <= 0 {
		b.items = b.items[:0]
		return b.self
	}
	if n == 1 {
		return b.self
	}
	orig := make([]T, len(b.items))
	copy(orig, b.items)
	for i := 1; i < n; i++ {
		b.items = append(b.items, orig...)
	}
	return b.self
}

// ── 查询操作 ──

// Len 返回当前切片元素数量。
//
//	go async.Slice(ctx, []int{1, 2, 3}).Len() // 3
func (b *sliceBase[T, S]) Len() int {
	return len(b.items)
}

// IsEmpty 切片为空返回 true。
//
//	go async.Slice(ctx, []int{}).IsEmpty() // true
func (b *sliceBase[T, S]) IsEmpty() bool {
	return len(b.items) == 0
}

// First 返回第一个元素及是否存在的标志。
//
//	返回: val 第一个元素，found 是否存在。
//
//	go val, ok := async.Slice(ctx, []int{1, 2, 3}).First() // val=1, ok=true
func (b *sliceBase[T, S]) First() (val T, found bool) {
	if len(b.items) == 0 {
		return
	}
	return b.items[0], true
}

// Last 返回最后一个元素及是否存在的标志。
//
//	返回: val 最后一个元素，found 是否存在。
//
//	go val, ok := async.Slice(ctx, []int{1, 2, 3}).Last() // val=3, ok=true
func (b *sliceBase[T, S]) Last() (val T, found bool) {
	if len(b.items) == 0 {
		return
	}
	return b.items[len(b.items)-1], true
}

// Items 返回内部切片的直接引用（非拷贝），修改返回值会影响构建器内部状态。
// 如需安全拷贝请使用 Values()。
//
//	go items := async.Slice(ctx, data).Filter(pred).Items() // 直接引用，慎改
func (b *sliceBase[T, S]) Items() []T {
	return b.items
}

// Values 返回切片元素的浅拷贝，修改返回值不影响构建器内部状态。
//
//	go vals := async.Slice(ctx, data).Sort(cmp).Values() // 安全，可任意修改
func (b *sliceBase[T, S]) Values() []T {
	out := make([]T, len(b.items))
	copy(out, b.items)
	return out
}

// Clone 返回切片元素的浅拷贝，等价于 Values()。
func (b *sliceBase[T, S]) Clone() []T { return b.Values() }

// Contains 判断是否存在满足 pred 的元素，存在返回 true。
// 元素数 >= 100 且策略为并行时自动启用并行搜索（提前退出）。
//
//	pred: 谓词函数，返回 true 表示匹配。
//
//	go async.Slice(ctx, []int{1, 2, 3}).Contains(func(v int) bool { return v > 2 }) // true
func (b *sliceBase[T, S]) Contains(pred func(T) bool) bool {
	if len(b.items) == 0 {
		return false
	}
	if !b.policy.IsParallel() || len(b.items) < 100 {
		return funcContains(b.items, pred)
	}
	return funcContainsParallel(b.items, pred, b.policy.Concurrency())
}

// Index 返回第一个满足 pred 的元素索引，未找到返回 -1。
//
//	pred: 谓词函数。
//
//	go idx := async.Slice(ctx, []int{1, 2, 3}).Index(func(v int) bool { return v == 2 }) // 1
func (b *sliceBase[T, S]) Index(pred func(T) bool) int {
	for i, v := range b.items {
		if pred(v) {
			return i
		}
	}
	return -1
}

// Find 返回第一个满足 pred 的元素及是否找到。
//
//	pred: 谓词函数。
//
//	go val, ok := async.Slice(ctx, []int{1, 2, 3}).Find(func(v int) bool { return v > 1 }) // val=2, ok=true
func (b *sliceBase[T, S]) Find(pred func(T) bool) (val T, found bool) {
	for _, v := range b.items {
		if pred(v) {
			return v, true
		}
	}
	return
}

// FindLast 返回最后一个满足 pred 的元素及是否找到。
//
//	pred: 谓词函数。
//
//	go val, ok := async.Slice(ctx, []int{1, 2, 3, 2}).FindLast(func(v int) bool { return v == 2 }) // val=2(索引3), ok=true
func (b *sliceBase[T, S]) FindLast(pred func(T) bool) (val T, found bool) {
	for i := len(b.items) - 1; i >= 0; i-- {
		if pred(b.items[i]) {
			return b.items[i], true
		}
	}
	return
}

// All 判断是否所有元素都满足 pred。
// 空切片返回 true。元素数 >= 100 且策略为并行时自动启用并行检查。
//
//	pred: 谓词函数。
//
//	go async.Slice(ctx, []int{2, 4, 6}).All(func(v int) bool { return v%2 == 0 }) // true
func (b *sliceBase[T, S]) All(pred func(T) bool) bool {
	if len(b.items) == 0 {
		return true
	}
	if !b.policy.IsParallel() || len(b.items) < 100 {
		return funcAll(b.items, pred)
	}
	return funcAllParallel(b.items, pred, b.policy.Concurrency())
}

// Any 判断是否存在至少一个元素满足 pred。
// 空切片返回 false。元素数 >= 100 且策略为并行时自动启用并行检查。
//
//	pred: 谓词函数。
//
//	go async.Slice(ctx, []int{1, 3, 5}).Any(func(v int) bool { return v%2 == 0 }) // false
func (b *sliceBase[T, S]) Any(pred func(T) bool) bool {
	if len(b.items) == 0 {
		return false
	}
	if !b.policy.IsParallel() || len(b.items) < 100 {
		return funcAny(b.items, pred)
	}
	return funcAnyParallel(b.items, pred, b.policy.Concurrency())
}

// Count 统计满足 pred 的元素数量。
// 元素数 >= 100 且策略为并行时自动启用并行计数。
//
//	pred: 谓词函数。
//
//	go async.Slice(ctx, []int{1, 2, 3, 4}).Count(func(v int) bool { return v%2 == 0 }) // 2
func (b *sliceBase[T, S]) Count(pred func(T) bool) int {
	if len(b.items) == 0 {
		return 0
	}
	if !b.policy.IsParallel() || len(b.items) < 100 {
		return funcCount(b.items, pred)
	}
	return funcCountParallel(b.items, pred, b.policy.Concurrency())
}

// Max 使用 cmp 比较函数找出最大元素。
//
//	cmp: 比较函数，规则同 Sort。
//
//	go max, ok := async.Slice(ctx, []int{1, 3, 2}).Max(func(a, b int) int { return a - b }) // max=3, ok=true
func (b *sliceBase[T, S]) Max(cmp func(a, b T) int) (max T, found bool) {
	if len(b.items) == 0 {
		return
	}
	max = b.items[0]
	for _, v := range b.items[1:] {
		if cmp(v, max) > 0 {
			max = v
		}
	}
	return max, true
}

// Min 使用 cmp 比较函数找出最小元素。
//
//	cmp: 比较函数，规则同 Sort。
//
//	go min, ok := async.Slice(ctx, []int{3, 1, 2}).Min(func(a, b int) int { return a - b }) // min=1, ok=true
func (b *sliceBase[T, S]) Min(cmp func(a, b T) int) (min T, found bool) {
	if len(b.items) == 0 {
		return
	}
	min = b.items[0]
	for _, v := range b.items[1:] {
		if cmp(v, min) < 0 {
			min = v
		}
	}
	return min, true
}

// BinarySearch 在已排序（按 cmp 升序）的切片中二分查找 target。
// 返回插入位置及是否找到完全匹配项。
//
//	target: 查找目标值。
//	cmp:    比较函数，规则同 Sort。
//
//	go idx, found := async.Slice(ctx, []int{1, 2, 3}).Sort(...).BinarySearch(2, cmp) // idx=1, found=true
func (b *sliceBase[T, S]) BinarySearch(target T, cmp func(a, b T) int) (int, bool) {
	return slicesBinarySearch(b.items, target, cmp)
}

// Chunked 将切片按 chunkSize 切分为多个子切片，返回 [][]T。
// 最后一个子切片可能不足 chunkSize。不修改内部切片。
//
//	chunkSize: 每个子切片的期望大小。
//
//	go chunks := async.Slice(ctx, []int{1, 2, 3, 4, 5}).Chunked(2) // [[1,2], [3,4], [5]]
func (b *sliceBase[T, S]) Chunked(chunkSize int) [][]T {
	if chunkSize <= 0 || len(b.items) == 0 {
		return nil
	}
	chunks := make([][]T, 0, (len(b.items)+chunkSize-1)/chunkSize)
	for i := 0; i < len(b.items); i += chunkSize {
		end := i + chunkSize
		if end > len(b.items) {
			end = len(b.items)
		}
		chunks = append(chunks, b.items[i:end])
	}
	return chunks
}

// ChunkedN 将切片近似均匀地切分为 n 个子切片，返回 [][]T。
// 不修改内部切片。
//
//	n: 期望的子切片数量。
//
//	go chunks := async.Slice(ctx, []int{1, 2, 3, 4, 5}).ChunkedN(2) // [[1,2,3], [4,5]]
func (b *sliceBase[T, S]) ChunkedN(n int) [][]T {
	if n <= 0 || len(b.items) == 0 {
		return nil
	}
	return b.Chunked((len(b.items) + n - 1) / n)
}

// Split 将切片按 pred 拆分为两组：matched（满足 pred）和 unmatched（不满足 pred）。
// 不修改内部切片。
//
//	pred: 谓词函数。
//
//	go m, u := async.Slice(ctx, []int{1, 2, 3, 4}).Split(func(v int) bool { return v%2 == 0 })
//	// m = [2, 4], u = [1, 3]
func (b *sliceBase[T, S]) Split(pred func(T) bool) (matched []T, unmatched []T) {
	for _, v := range b.items {
		if pred(v) {
			matched = append(matched, v)
		} else {
			unmatched = append(unmatched, v)
		}
	}
	return
}
