package async

import (
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/chichengyu/async/internal/sliceops"
)

// ── sort ──

// slicesSortFunc 使用 cmp 比较函数对切片进行非稳定排序。
//
//	cmp: 返回 <0 表示 a 排在 b 之前。
func slicesSortFunc[T any](items []T, cmp func(a, b T) int) {
	sort.Slice(items, func(i, j int) bool {
		return cmp(items[i], items[j]) < 0
	})
}

// slicesSortStableFunc 使用 cmp 比较函数对切片进行稳定排序。
//
//	cmp: 返回 <0 表示 a 排在 b 之前。
func slicesSortStableFunc[T any](items []T, cmp func(a, b T) int) {
	sort.SliceStable(items, func(i, j int) bool {
		return cmp(items[i], items[j]) < 0
	})
}

// slicesIsSortedFunc 判断切片是否已按 cmp 升序排列。
//
//	cmp: 返回 <0 表示 a 排在 b 之前。
func slicesIsSortedFunc[T any](items []T, cmp func(a, b T) int) bool {
	return sort.SliceIsSorted(items, func(i, j int) bool {
		return cmp(items[i], items[j]) < 0
	})
}

// slicesGrow 扩展切片底层数组容量 n 个元素，len 不变。
//
//	n: 要扩展的容量数。
func slicesGrow[T any](items []T, n int) []T {
	return append(items, make([]T, n)...)[:len(items)]
}

// slicesReplace 替换切片 [i, j) 范围的元素为 vals。
//
//	i, j: 替换范围 [i, j)。
//	vals: 替换后的新元素。
func slicesReplace[T any](items []T, i, j int, vals ...T) []T {
	n := len(items)
	if i < 0 {
		i = 0
	}
	if j > n {
		j = n
	}
	if i > j {
		i = j
	}
	keep := n - (j - i)
	newLen := keep + len(vals)
	if cap(items) >= newLen {
		items = items[:newLen]
	} else {
		newSlice := make([]T, newLen)
		copy(newSlice, items[:i])
		copy(newSlice[i+len(vals):], items[j:])
		items = newSlice
		copy(items[i:], vals)
		return items
	}
	copy(items[i+len(vals):], items[j:])
	copy(items[i:], vals)
	return items
}

// slicesBinarySearch 在已排序切片中二分查找 target。
//
//	target: 查找目标值。
//	cmp:    比较函数，规则同 sort。
//
//	返回: 索引（未找到时为插入位置）及是否精确匹配。
func slicesBinarySearch[T any](items []T, target T, cmp func(a, b T) int) (int, bool) {
	idx := sort.Search(len(items), func(i int) bool {
		return cmp(items[i], target) >= 0
	})
	if idx < len(items) && cmp(items[idx], target) == 0 {
		return idx, true
	}
	return idx, false
}

// randShuffle 调用 rand.Shuffle 对 n 个元素随机排列。
//
//	swap: 交换 i 和 j 位置元素的回调。
func randShuffle(n int, swap func(i, j int)) {
	rand.Shuffle(n, swap)
}

// ── filter ──

// filterItems 根据策略对切片执行过滤，元素数 >= 1000 且策略为并行时启用并行过滤。
//
//	pred: 谓词函数，返回 true 保留该元素。
//	p:    策略对象，决定串行还是并行执行。
func filterItems[T any](items []T, pred func(T) bool, p sliceops.Policy) []T {
	if !p.IsParallel() || len(items) < 1000 {
		return filterSerial(items, pred)
	}
	return filterParallel(items, pred, p.Concurrency())
}

// filterSerial 串行过滤，保留 pred 返回 true 的元素，结果保持原顺序。
func filterSerial[T any](items []T, pred func(T) bool) []T {
	n := 0
	for _, v := range items {
		if pred(v) {
			items[n] = v
			n++
		}
	}
	return items[:n]
}

// filterParallel 并行过滤，将切片分片后多 goroutine 并发过滤，结果按原顺序合并。
//
//	conc: 并发 goroutine 数量。
func filterParallel[T any](items []T, pred func(T) bool, conc int) []T {
	if conc <= 0 {
		conc = runtime.GOMAXPROCS(0)
	}
	n := len(items)
	chunkSize := (n + conc - 1) / conc

	type chunk struct {
		start, end int
		kept       []T
	}
	chunks := make([]chunk, conc)
	var wg sync.WaitGroup
	for c := 0; c < conc; c++ {
		start := c * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(idx, s, e int) {
			defer wg.Done()
			var kept []T
			for i := s; i < e; i++ {
				if pred(items[i]) {
					kept = append(kept, items[i])
				}
			}
			chunks[idx] = chunk{start: s, end: e, kept: kept}
		}(c, start, end)
	}
	wg.Wait()

	total := 0
	for _, ch := range chunks {
		total += len(ch.kept)
	}
	result := make([]T, total)
	pos := 0
	for _, ch := range chunks {
		copy(result[pos:], ch.kept)
		pos += len(ch.kept)
	}
	return result
}

// ── contains ──

// funcContains 串行检查是否存在满足 pred 的元素，找到即返回。
//
//	pred: 谓词函数。
func funcContains[T any](items []T, pred func(T) bool) bool {
	for _, v := range items {
		if pred(v) {
			return true
		}
	}
	return false
}

// funcContainsParallel 并行检查是否存在满足 pred 的元素，任一 goroutine 找到即提前退出。
//
//	conc: 并发 goroutine 数量。
func funcContainsParallel[T any](items []T, pred func(T) bool, conc int) bool {
	if conc <= 0 {
		conc = runtime.GOMAXPROCS(0)
	}
	n := len(items)
	chunkSize := (n + conc - 1) / conc
	var found atomic.Bool
	var wg sync.WaitGroup
	for c := 0; c < conc; c++ {
		start := c * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for i := s; i < e; i++ {
				if found.Load() {
					return
				}
				if pred(items[i]) {
					found.Store(true)
					return
				}
			}
		}(start, end)
	}
	wg.Wait()
	return found.Load()
}

// ── all ──

// funcAll 串行检查是否所有元素都满足 pred。
//
//	pred: 谓词函数。
func funcAll[T any](items []T, pred func(T) bool) bool {
	for _, v := range items {
		if !pred(v) {
			return false
		}
	}
	return true
}

// funcAllParallel 并行检查是否所有元素都满足 pred，任一 goroutine 发现不满足即提前退出。
//
//	conc: 并发 goroutine 数量。
func funcAllParallel[T any](items []T, pred func(T) bool, conc int) bool {
	if conc <= 0 {
		conc = runtime.GOMAXPROCS(0)
	}
	n := len(items)
	chunkSize := (n + conc - 1) / conc
	var failed atomic.Bool
	var wg sync.WaitGroup
	for c := 0; c < conc; c++ {
		start := c * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for i := s; i < e; i++ {
				if failed.Load() {
					return
				}
				if !pred(items[i]) {
					failed.Store(true)
					return
				}
			}
		}(start, end)
	}
	wg.Wait()
	return !failed.Load()
}

// ── any ──

// funcAny 串行检查是否存在任一元素满足 pred。
//
//	pred: 谓词函数。
func funcAny[T any](items []T, pred func(T) bool) bool {
	for _, v := range items {
		if pred(v) {
			return true
		}
	}
	return false
}

// funcAnyParallel 并行检查是否存在任一元素满足 pred。
// 等价于 funcContainsParallel，任一 goroutine 找到即提前退出。
//
//	conc: 并发 goroutine 数量。
func funcAnyParallel[T any](items []T, pred func(T) bool, conc int) bool {
	return funcContainsParallel(items, pred, conc)
}

// ── count ──

// funcCount 串行统计满足 pred 的元素数量。
//
//	pred: 谓词函数。
func funcCount[T any](items []T, pred func(T) bool) int {
	n := 0
	for _, v := range items {
		if pred(v) {
			n++
		}
	}
	return n
}

// funcCountParallel 并行统计满足 pred 的元素数量，使用 atomic 累加各分片计数。
//
//	conc: 并发 goroutine 数量。
func funcCountParallel[T any](items []T, pred func(T) bool, conc int) int {
	if conc <= 0 {
		conc = runtime.GOMAXPROCS(0)
	}
	n := len(items)
	chunkSize := (n + conc - 1) / conc
	var total atomic.Int64
	var wg sync.WaitGroup
	for c := 0; c < conc; c++ {
		start := c * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			var local int64
			for i := s; i < e; i++ {
				if pred(items[i]) {
					local++
				}
			}
			total.Add(local)
		}(start, end)
	}
	wg.Wait()
	return int(total.Load())
}
