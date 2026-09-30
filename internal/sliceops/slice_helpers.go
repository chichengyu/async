package sliceops

import (
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

func slicesSortFunc[T any](items []T, cmp func(a, b T) int) {
	sort.Slice(items, func(i, j int) bool {
		return cmp(items[i], items[j]) < 0
	})
}

func slicesSortStableFunc[T any](items []T, cmp func(a, b T) int) {
	sort.SliceStable(items, func(i, j int) bool {
		return cmp(items[i], items[j]) < 0
	})
}

func slicesIsSortedFunc[T any](items []T, cmp func(a, b T) int) bool {
	return sort.SliceIsSorted(items, func(i, j int) bool {
		return cmp(items[i], items[j]) < 0
	})
}

func slicesGrow[T any](items []T, n int) []T {
	return append(items, make([]T, n)...)[:len(items)]
}

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

func slicesBinarySearch[T any](items []T, target T, cmp func(a, b T) int) (int, bool) {
	idx := sort.Search(len(items), func(i int) bool {
		return cmp(items[i], target) >= 0
	})
	if idx < len(items) && cmp(items[idx], target) == 0 {
		return idx, true
	}
	return idx, false
}

func randShuffle(n int, swap func(i, j int)) {
	rand.Shuffle(n, swap)
}

func filterItems[T any](items []T, pred func(T) bool, p Policy) []T {
	if !p.IsParallel() || len(items) < 1000 {
		return filterSerial(items, pred)
	}
	return filterParallel(items, pred, p.GetWorker())
}

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

func funcContains[T any](items []T, pred func(T) bool) bool {
	for _, v := range items {
		if pred(v) {
			return true
		}
	}
	return false
}

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

func funcAll[T any](items []T, pred func(T) bool) bool {
	for _, v := range items {
		if !pred(v) {
			return false
		}
	}
	return true
}

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

func funcAny[T any](items []T, pred func(T) bool) bool {
	for _, v := range items {
		if pred(v) {
			return true
		}
	}
	return false
}

func funcAnyParallel[T any](items []T, pred func(T) bool, conc int) bool {
	return funcContainsParallel(items, pred, conc)
}

func funcCount[T any](items []T, pred func(T) bool) int {
	n := 0
	for _, v := range items {
		if pred(v) {
			n++
		}
	}
	return n
}

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
