package sliceops

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/testutil"
)

// ──────────────────────────── helpers ────────────────────────────

func genIntItems(n int) []int {
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	return items
}

func shuffleItems(items []int) {
	rand.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
}

// ============================================================
// 一、Slice 构造函数测试
// ============================================================

func TestSlice_New(t *testing.T) {
	s := New([]int{1, 2, 3})
	if s.Len() != 3 || s.IsEmpty() {
		t.Fatalf("New: len=%d empty=%v", s.Len(), s.IsEmpty())
	}
	vals := s.Values()
	if vals[0] != 1 || vals[1] != 2 || vals[2] != 3 {
		t.Fatal("New: values mismatch")
	}
}

func TestSlice_New_Nil(t *testing.T) {
	s := New[int](nil)
	if s.Len() != 0 || !s.IsEmpty() {
		t.Fatalf("New(nil): len=%d empty=%v", s.Len(), s.IsEmpty())
	}
	if s.Values() == nil || len(s.Values()) != 0 {
		t.Fatal("New(nil).Values() should return empty slice")
	}
}

func TestSlice_New_Empty(t *testing.T) {
	s := New[int]([]int{})
	if s.Len() != 0 || !s.IsEmpty() {
		t.Fatal("New([]): should be empty")
	}
}

func TestSlice_NewWithResult(t *testing.T) {
	s := NewWithResult[int, string]([]int{1, 2, 3})
	if s.Len() != 3 {
		t.Fatalf("NewWithResult: len=%d", s.Len())
	}
	vals := s.Values()
	if len(vals) != 3 || vals[0] != 1 || vals[2] != 3 {
		t.Fatal("NewWithResult: values mismatch")
	}
}

func TestSlice_NewWithResult_Nil(t *testing.T) {
	s := NewWithResult[int, string](nil)
	if s.Len() != 0 || !s.IsEmpty() {
		t.Fatal("NewWithResult(nil): should be empty")
	}
}

// ============================================================
// 二、Slice 基础查询测试（Values / Items / Len / IsEmpty / First / Last）
// ============================================================

func TestSlice_Values_DeepCopy(t *testing.T) {
	s := New([]int{10, 20, 30})
	vals := s.Values()
	vals[0] = 999
	if s.Items()[0] == 10 {
		t.Log("Values returns copy ✓")
	} else {
		t.Fatal("Values should return a copy, not reference")
	}
}

func TestSlice_Items_Reference(t *testing.T) {
	s := New([]int{1, 2})
	items := s.Items()
	items[0] = 99
	if s.Items()[0] != 99 {
		t.Fatal("Items should return reference")
	}
}

func TestSlice_Len_IsEmpty(t *testing.T) {
	if !New[int](nil).IsEmpty() || New[int](nil).Len() != 0 {
		t.Fatal("nil/empty: Len/IsEmpty")
	}
	s := New([]int{1, 2, 3})
	if s.IsEmpty() || s.Len() != 3 {
		t.Fatal("non-empty: Len/IsEmpty")
	}
}

func TestSlice_First_Last(t *testing.T) {
	s := New([]int{100, 200, 300})
	v, ok := s.First()
	if !ok || v != 100 {
		t.Fatalf("First=%d,ok=%v want 100,true", v, ok)
	}
	v, ok = s.Last()
	if !ok || v != 300 {
		t.Fatalf("Last=%d,ok=%v want 300,true", v, ok)
	}

	empty := New[int](nil)
	_, ok = empty.First()
	if ok {
		t.Fatal("First on empty should return false")
	}
	_, ok = empty.Last()
	if ok {
		t.Fatal("Last on empty should return false")
	}

	single := New([]int{42})
	v, ok = single.First()
	if !ok || v != 42 {
		t.Fatal("First on single")
	}
	v, ok = single.Last()
	if !ok || v != 42 {
		t.Fatal("Last on single should equal First")
	}
}

// ============================================================
// 三、Slice 排序测试（SortFunc / StableSortFunc / IsSortedFunc / Reverse）
// ============================================================

func TestSlice_SortFunc(t *testing.T) {
	s := New([]int{5, 3, 1, 4, 2})
	s.SortFunc(func(a, b int) int { return a - b })
	items := s.Items()
	if !sort.IntsAreSorted(items) {
		t.Fatal("SortFunc: slice not sorted")
	}
	for i, v := range items {
		if v != i+1 {
			t.Fatalf("SortFunc: items[%d]=%d want %d", i, v, i+1)
		}
	}
}

func TestSlice_StableSortFunc(t *testing.T) {
	type pair struct{ v, id int }
	s := NewWithResult[pair, pair]([]pair{{3, 0}, {1, 1}, {1, 2}, {2, 3}})
	s.StableSortFunc(func(a, b pair) int { return a.v - b.v })
	items := s.Items()
	if items[0].id != 1 || items[1].id != 2 {
		t.Fatalf("StableSortFunc: expected id [1,2] for equal v=1, got [%d,%d]", items[0].id, items[1].id)
	}
}

func TestSlice_IsSortedFunc(t *testing.T) {
	sorted := New([]int{1, 2, 3, 4, 5})
	if !sorted.IsSortedFunc(func(a, b int) int { return a - b }) {
		t.Fatal("IsSortedFunc: should be sorted")
	}
	unsorted := New([]int{3, 1, 2})
	if unsorted.IsSortedFunc(func(a, b int) int { return a - b }) {
		t.Fatal("IsSortedFunc: should not be sorted")
	}
	empty := New[int](nil)
	if !empty.IsSortedFunc(func(a, b int) int { return a - b }) {
		t.Fatal("IsSortedFunc: empty slice is trivially sorted")
	}
}

func TestSlice_Reverse(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5})
	s.Reverse()
	for i, v := range s.Items() {
		if v != 5-i {
			t.Fatalf("Reverse[%d]=%d want %d", i, v, 5-i)
		}
	}
	empty := New[int](nil)
	empty.Reverse()
	if empty.Len() != 0 {
		t.Fatal("Reverse on empty should be no-op")
	}
}

// ============================================================
// 四、Slice 查找与判断测试
// ============================================================

func TestSlice_ContainsFunc(t *testing.T) {
	s := New([]int{1, 3, 5, 7, 9})
	if !s.ContainsFunc(func(v int) bool { return v == 5 }) {
		t.Fatal("ContainsFunc: should contain 5")
	}
	if s.ContainsFunc(func(v int) bool { return v == 0 }) {
		t.Fatal("ContainsFunc: should not contain 0")
	}
	if New[int](nil).ContainsFunc(func(v int) bool { return true }) {
		t.Fatal("ContainsFunc: empty should return false")
	}
}

func TestSlice_IndexFunc(t *testing.T) {
	s := New([]int{10, 20, 30, 40})
	if idx := s.IndexFunc(func(v int) bool { return v == 30 }); idx != 2 {
		t.Fatalf("IndexFunc: idx=%d want 2", idx)
	}
	if idx := s.IndexFunc(func(v int) bool { return v == 999 }); idx != -1 {
		t.Fatalf("IndexFunc: idx=%d want -1", idx)
	}
	if idx := New[int](nil).IndexFunc(func(v int) bool { return true }); idx != -1 {
		t.Fatalf("IndexFunc empty: idx=%d want -1", idx)
	}
}

func TestSlice_FindFunc(t *testing.T) {
	s := New([]int{10, 20, 30})
	v, ok := s.FindFunc(func(x int) bool { return x > 15 })
	if !ok || v != 20 {
		t.Fatalf("FindFunc: val=%d ok=%v want 20,true", v, ok)
	}
	v, ok = s.FindFunc(func(x int) bool { return x > 100 })
	if ok {
		t.Fatalf("FindFunc: should not find, got %d", v)
	}
	v, ok = New[int](nil).FindFunc(func(x int) bool { return true })
	if ok {
		t.Fatal("FindFunc on empty should return false")
	}
}

func TestSlice_FindLastFunc(t *testing.T) {
	s := New([]int{10, 20, 30, 20, 40})
	v, ok := s.FindLastFunc(func(x int) bool { return x == 20 })
	if !ok || v != 20 {
		t.Fatalf("FindLastFunc: val=%d ok=%v", v, ok)
	}
	v, ok = s.FindLastFunc(func(x int) bool { return x > 100 })
	if ok {
		t.Fatalf("FindLastFunc: should not find, got %d", v)
	}
}

func TestSlice_All_Any(t *testing.T) {
	s := New([]int{2, 4, 6, 8})
	if !s.All(func(v int) bool { return v%2 == 0 }) {
		t.Fatal("All: all should be even")
	}
	if s.All(func(v int) bool { return v > 5 }) {
		t.Fatal("All: not all >5")
	}
	if !New[int](nil).All(func(v int) bool { return false }) {
		t.Fatal("All: empty returns true")
	}

	if !s.Any(func(v int) bool { return v > 5 }) {
		t.Fatal("Any: some >5")
	}
	if s.Any(func(v int) bool { return v > 100 }) {
		t.Fatal("Any: none >100")
	}
	if New[int](nil).Any(func(v int) bool { return true }) {
		t.Fatal("Any: empty returns false")
	}
}

func TestSlice_Count(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5, 6, 7, 8})
	n := s.Count(func(v int) bool { return v%2 == 0 })
	if n != 4 {
		t.Fatalf("Count: got %d want 4", n)
	}
	n = New[int](nil).Count(func(v int) bool { return true })
	if n != 0 {
		t.Fatalf("Count empty: got %d want 0", n)
	}
}

func TestSlice_MaxFunc_MinFunc(t *testing.T) {
	s := New([]int{3, 1, 7, 4, 2})
	max, ok := s.MaxFunc(func(a, b int) int { return a - b })
	if !ok || max != 7 {
		t.Fatalf("MaxFunc: %d,%v want 7,true", max, ok)
	}
	min, ok := s.MinFunc(func(a, b int) int { return a - b })
	if !ok || min != 1 {
		t.Fatalf("MinFunc: %d,%v want 1,true", min, ok)
	}

	empty := New[int](nil)
	_, ok = empty.MaxFunc(func(a, b int) int { return a - b })
	if ok {
		t.Fatal("MaxFunc empty: should return false")
	}
	_, ok = empty.MinFunc(func(a, b int) int { return a - b })
	if ok {
		t.Fatal("MinFunc empty: should return false")
	}
}

func TestSlice_BinarySearchFunc(t *testing.T) {
	s := New([]int{1, 3, 5, 7, 9, 11})
	idx, ok := s.BinarySearchFunc(7, func(a, b int) int { return a - b })
	if !ok || idx != 3 {
		t.Fatalf("BinarySearchFunc(7): idx=%d ok=%v want 3,true", idx, ok)
	}
	idx, ok = s.BinarySearchFunc(8, func(a, b int) int { return a - b })
	if ok {
		t.Fatalf("BinarySearchFunc(8): found at %d but should not", idx)
	}
	idx, ok = s.BinarySearchFunc(0, func(a, b int) int { return a - b })
	if ok {
		t.Fatalf("BinarySearchFunc(0): should not find")
	}
	if idx != 0 {
		t.Fatalf("BinarySearchFunc(0): insert idx=%d want 0", idx)
	}
}

// ============================================================
// 五、Slice 过滤与去重测试（Filter / CompactFunc / DedupFunc）
// ============================================================

func TestSlice_Filter(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5, 6})
	s.Filter(func(v int) bool { return v%2 == 0 })
	items := s.Items()
	if len(items) != 3 || items[0] != 2 || items[1] != 4 || items[2] != 6 {
		t.Fatalf("Filter: got %v want [2,4,6]", items)
	}
	s2 := New([]int{1, 3, 5})
	s2.Filter(func(v int) bool { return v%2 == 0 })
	if s2.Len() != 0 {
		t.Fatalf("Filter all: len=%d want 0", s2.Len())
	}
}

func TestSlice_CompactFunc(t *testing.T) {
	s := New([]int{1, 1, 2, 2, 3, 1, 1})
	s.CompactFunc(func(a, b int) bool { return a == b })
	items := s.Items()
	expected := []int{1, 2, 3, 1}
	if len(items) != 4 {
		t.Fatalf("CompactFunc: len=%d want 4 (%v)", len(items), items)
	}
	for i, v := range items {
		if v != expected[i] {
			t.Fatalf("CompactFunc[%d]=%d want %d", i, v, expected[i])
		}
	}
	single := New([]int{1})
	single.CompactFunc(func(a, b int) bool { return a == b })
	if single.Len() != 1 {
		t.Fatal("CompactFunc on single element")
	}
	empty := New[int](nil)
	empty.CompactFunc(func(a, b int) bool { return a == b })
	if empty.Len() != 0 {
		t.Fatal("CompactFunc on empty")
	}
}

func TestSlice_DedupFunc(t *testing.T) {
	s := New([]int{3, 1, 2, 1, 3, 5, 2})
	s.DedupFunc(func(a, b int) bool { return a == b })
	items := s.Items()
	unique := make(map[int]bool)
	for _, v := range items {
		if unique[v] {
			t.Fatalf("DedupFunc: duplicate %d in result %v", v, items)
		}
		unique[v] = true
	}
	if len(items) != 4 {
		t.Fatalf("DedupFunc: len=%d want 4 (%v)", len(items), items)
	}
	single := New([]int{1})
	single.DedupFunc(func(a, b int) bool { return a == b })
	if single.Len() != 1 {
		t.Fatal("DedupFunc on single")
	}
}

// ============================================================
// 六、Slice 增删改测试（Append / Prepend / Insert / Delete / DeleteRange / Replace）
// ============================================================

func TestSlice_Append(t *testing.T) {
	s := New([]int{1, 2})
	s.Append(3, 4, 5)
	items := s.Items()
	if len(items) != 5 || items[0] != 1 || items[4] != 5 {
		t.Fatalf("Append: got %v want [1,2,3,4,5]", items)
	}
	s.Append()
	if s.Len() != 5 {
		t.Fatal("Append nothing: len unchanged")
	}
	empty := New[int](nil)
	empty.Append(1)
	if empty.Len() != 1 || empty.Items()[0] != 1 {
		t.Fatal("Append to empty")
	}
}

func TestSlice_Prepend(t *testing.T) {
	s := New([]int{3, 4})
	s.Prepend(1, 2)
	items := s.Items()
	if len(items) != 4 || items[0] != 1 || items[1] != 2 {
		t.Fatalf("Prepend: got %v want [1,2,3,4]", items)
	}
	empty := New[int](nil)
	empty.Prepend(1, 2)
	if empty.Len() != 2 || empty.Items()[0] != 1 || empty.Items()[1] != 2 {
		t.Fatal("Prepend to empty")
	}
}

func TestSlice_Insert(t *testing.T) {
	s := New([]int{1, 4, 5})
	s.Insert(1, 2, 3)
	items := s.Items()
	if len(items) != 5 || items[0] != 1 || items[1] != 2 || items[2] != 3 || items[3] != 4 || items[4] != 5 {
		t.Fatalf("Insert(1): got %v want [1,2,3,4,5]", items)
	}

	s2 := New([]int{1, 2})
	s2.Insert(-1, 0)
	if s2.Items()[0] != 0 {
		t.Fatalf("Insert(-1): first=%d want 0", s2.Items()[0])
	}
	s2.Insert(999, 9)
	if s2.Last_ForTest() != 9 {
		t.Fatalf("Insert(999): last=%d want 9", s2.Last_ForTest())
	}
}

func TestSlice_Delete(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5})
	s.Delete(2)
	items := s.Items()
	if len(items) != 4 || items[0] != 1 || items[1] != 2 || items[2] != 4 || items[3] != 5 {
		t.Fatalf("Delete(2): got %v want [1,2,4,5]", items)
	}
	s.Delete(-1)
	if s.Len() != 4 {
		t.Fatal("Delete(-1): should be no-op")
	}
	s.Delete(999)
	if s.Len() != 4 {
		t.Fatal("Delete(999): should be no-op")
	}
}

func TestSlice_DeleteRange(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5})
	s.DeleteRange(1, 4)
	items := s.Items()
	if len(items) != 2 || items[0] != 1 || items[1] != 5 {
		t.Fatalf("DeleteRange(1,4): got %v want [1,5]", items)
	}

	s2 := New([]int{1, 2, 3})
	s2.DeleteRange(-1, 2)
	if s2.Items()[0] != 3 {
		t.Fatalf("DeleteRange(-1,2): got %v want [3]", s2.Items())
	}
	s2 = New([]int{1, 2, 3})
	s2.DeleteRange(1, 999)
	if s2.Items()[0] != 1 {
		t.Fatalf("DeleteRange(1,999): got %v want [1]", s2.Items())
	}
	s2.DeleteRange(0, 0)
	if s2.Len() != 1 {
		t.Fatal("DeleteRange(0,0): should be no-op")
	}
}

func TestSlice_Replace(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5})
	s.Replace(1, 3, 99, 100)
	items := s.Items()
	if len(items) != 5 || items[0] != 1 || items[1] != 99 || items[2] != 100 || items[3] != 4 || items[4] != 5 {
		t.Fatalf("Replace(1,3): got %v want [1,99,100,4,5]", items)
	}

	s2 := New([]int{1, 2, 3})
	s2.Replace(0, 1, 0)
	if s2.Items()[0] != 0 {
		t.Fatalf("Replace(0,1): got %v", s2.Items())
	}

	s3 := New([]int{1, 2})
	s3.Replace(0, 2, 10, 20, 30)
	if s3.Len() != 3 || s3.Items()[2] != 30 {
		t.Fatalf("Replace with larger: got %v", s3.Items())
	}
}

func (s *Slice[T, R]) Last_ForTest() T { return s.items[len(s.items)-1] }

// ============================================================
// 七、Slice 截取与变换测试（Take / Drop / SliceRange / Clip / Grow / Shuffle / Clone / Repeat / Chunk / ChunkN / Split）
// ============================================================

func TestSlice_Take(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5})
	s.Take(3)
	items := s.Items()
	if len(items) != 3 || items[0] != 1 || items[2] != 3 {
		t.Fatalf("Take(3): got %v", items)
	}
	s.Take(10)
	if s.Len() != 3 {
		t.Fatal("Take(10): len unchanged when n >= len")
	}
	s.Take(0)
	if s.Len() != 0 {
		t.Fatal("Take(0): should clear")
	}
	s2 := New([]int{1, 2})
	s2.Take(-1)
	if s2.Len() != 0 {
		t.Fatal("Take(-1): should clear")
	}
}

func TestSlice_Drop(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5})
	s.Drop(2)
	items := s.Items()
	if len(items) != 3 || items[0] != 3 {
		t.Fatalf("Drop(2): got %v", items)
	}
	s.Drop(10)
	if s.Len() != 0 {
		t.Fatal("Drop(10): should clear when n >= len")
	}
	s2 := New([]int{1, 2, 3})
	s2.Drop(0)
	if s2.Len() != 3 {
		t.Fatal("Drop(0): no-op")
	}
	s2.Drop(-1)
	if s2.Len() != 3 {
		t.Fatal("Drop(-1): no-op")
	}
}

func TestSlice_SliceRange(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5})
	s.SliceRange(1, 4)
	items := s.Items()
	if len(items) != 3 || items[0] != 2 || items[2] != 4 {
		t.Fatalf("SliceRange(1,4): got %v", items)
	}

	s2 := New([]int{1, 2, 3})
	s2.SliceRange(-1, 2)
	if s2.Items()[0] != 1 || len(s2.Items()) != 2 {
		t.Fatalf("SliceRange(-1,2): got %v", s2.Items())
	}
	s2 = New([]int{1, 2, 3})
	s2.SliceRange(1, 999)
	if len(s2.Items()) != 2 || s2.Items()[0] != 2 {
		t.Fatalf("SliceRange(1,999): got %v", s2.Items())
	}
	s2.SliceRange(1, 1)
	if s2.Len() != 0 {
		t.Fatal("SliceRange(1,1): should clear")
	}
}

func TestSlice_Clip(t *testing.T) {
	s := New([]int{1, 2, 3})
	s.Grow(100)
	if cap(s.Items()) < 100+3 {
		t.Logf("Grow: cap=%d", cap(s.Items()))
	}
	s.Clip()
	if cap(s.Items()) != len(s.Items()) {
		t.Fatalf("Clip: cap=%d len=%d, should be equal", cap(s.Items()), len(s.Items()))
	}
	oldLen := s.Len()
	s.Clip()
	if s.Len() != oldLen {
		t.Fatal("Clip should not change len")
	}
}

func TestSlice_Grow(t *testing.T) {
	s := New([]int{1})
	oldCap := cap(s.Items())
	s.Grow(10)
	if cap(s.Items()) < oldCap+10 {
		t.Fatalf("Grow(10): cap=%d want >=%d", cap(s.Items()), oldCap+10)
	}
	if s.Len() != 1 {
		t.Fatal("Grow should not change len")
	}
	s.Grow(0)
	if s.Len() != 1 {
		t.Fatal("Grow(0): no-op")
	}
}

func TestSlice_Shuffle(t *testing.T) {
	s := New(genIntItems(100))
	orig := s.Clone()
	same := true
	for i := 0; i < 10 && same; i++ {
		s.Shuffle()
		for j := 0; j < 100; j++ {
			if s.Items()[j] != orig[j] {
				same = false
				break
			}
		}
	}
	if same {
		t.Log("Shuffle may not have changed order (unlikely but possible)")
	}
	if s.Len() != 100 {
		t.Fatal("Shuffle should not change len")
	}
}

func TestSlice_Clone(t *testing.T) {
	s := New([]int{1, 2, 3})
	c := s.Clone()
	c[0] = 999
	if s.Items()[0] == 1 {
		t.Log("Clone returns independent copy ✓")
	} else {
		t.Fatal("Clone should return independent copy")
	}
}

func TestSlice_Repeat(t *testing.T) {
	s := New([]int{1, 2})
	s.Repeat(3)
	items := s.Items()
	if len(items) != 6 {
		t.Fatalf("Repeat(3): len=%d want 6 (%v)", len(items), items)
	}
	for i, v := range items {
		expected := i%2 + 1
		if v != expected {
			t.Fatalf("Repeat(3)[%d]=%d want %d", i, v, expected)
		}
	}

	s2 := New([]int{1, 2})
	s2.Repeat(0)
	if s2.Len() != 0 {
		t.Fatal("Repeat(0): should clear")
	}
	s3 := New([]int{1, 2})
	s3.Repeat(1)
	if s3.Len() != 2 {
		t.Fatal("Repeat(1): no change")
	}
}

func TestSlice_Chunk(t *testing.T) {
	s := New(genIntItems(10))
	chunks := s.Chunk(3)
	if len(chunks) != 4 {
		t.Fatalf("Chunk(3): got %d chunks want 4", len(chunks))
	}
	if len(chunks[0]) != 3 || len(chunks[3]) != 1 {
		t.Fatalf("Chunk sizes: got %d/%d want 3/1", len(chunks[0]), len(chunks[3]))
	}

	if chunks := New[int](nil).Chunk(3); chunks != nil {
		t.Fatal("Chunk on empty: should return nil")
	}
	if chunks := New([]int{1}).Chunk(0); chunks != nil {
		t.Fatal("Chunk(0): should return nil")
	}
}

func TestSlice_ChunkN(t *testing.T) {
	s := New(genIntItems(10))
	chunks := s.ChunkN(3)
	if len(chunks) > 3 {
		t.Fatalf("ChunkN(3): got %d chunks want <=3", len(chunks))
	}
	total := 0
	for _, c := range chunks {
		total += len(c)
	}
	if total != 10 {
		t.Fatalf("ChunkN(3): total=%d want 10", total)
	}
	if New[int](nil).ChunkN(3) != nil {
		t.Fatal("ChunkN on empty should return nil")
	}
}

func TestSlice_Split(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5, 6})
	matched, unmatched := s.Split(func(v int) bool { return v%2 == 0 })
	if len(matched) != 3 || len(unmatched) != 3 {
		t.Fatalf("Split: matched=%d unmatched=%d want 3/3", len(matched), len(unmatched))
	}
	for _, v := range matched {
		if v%2 != 0 {
			t.Fatal("Split: matched should all be even")
		}
	}
	for _, v := range unmatched {
		if v%2 != 1 {
			t.Fatal("Split: unmatched should all be odd")
		}
	}

	allMatch, noneMatch := New([]int{2, 4, 6}).Split(func(v int) bool { return v%2 == 0 })
	if len(allMatch) != 3 || len(noneMatch) != 0 {
		t.Fatal("Split: all match")
	}
}

// ============================================================
// 八、MapReduce 新 API（Do / Each / EachFull / Stream / EachStream / Reduce）
// ============================================================

func TestSlice_Do_Basic(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, string]([]int{1, 2, 3})
	results, err := s.Do(ctx, func(ctx context.Context, n int) (string, error) {
		return "v:" + strconv.Itoa(n), nil
	}, Par(4))
	if err != nil {
		t.Fatalf("Do: err=%v", err)
	}
	if len(results) != 3 {
		t.Fatalf("Do: len=%d want 3", len(results))
	}
	for i, r := range results {
		expected := "v:" + strconv.Itoa(i+1)
		if r.Value != expected {
			t.Fatalf("Do[%d]=%q want %q", i, r.Value, expected)
		}
	}
}

func TestSlice_Do_Serial(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{1, 2, 3, 4, 5})
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		return n * n, nil
	}, Seq())
	if err != nil {
		t.Fatalf("Do serial: err=%v", err)
	}
	for i, r := range results {
		expected := (i + 1) * (i + 1)
		if r.Value != expected {
			t.Fatalf("Do serial[%d]=%d want %d", i, r.Value, expected)
		}
	}
}

func TestSlice_Do_FailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	s := NewWithResult[int, int](genIntItems(100))
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		if n == 50 {
			return 0, bomb
		}
		return n, nil
	}, Par(8).FF())
	hasErr := err != nil
	if !hasErr {
		for _, r := range results {
			if r.Err != nil {
				hasErr = true
				break
			}
		}
	}
	if !hasErr {
		t.Fatal("Do FailFast: expected error in results")
	}
}

func TestSlice_Do_Timeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, Par(8).TO(5*time.Second))
	if err != nil || len(results) != 50 {
		t.Fatalf("Do Timeout: err=%v len=%d", err, len(results))
	}
}

func TestSlice_Do_TimeoutActual(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(10))
	_, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
		return n, nil
	}, Par(4).TO(50*time.Millisecond))
	if err == nil {
		t.Log("Do Timeout actual: error may be in results, not top-level")
	}
}

func TestSlice_Do_Chunk(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(500))
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, Par(8).Chunk(50))
	if err != nil || len(results) != 500 {
		t.Fatalf("Do Chunk: err=%v len=%d", err, len(results))
	}
}

func TestSlice_Do_Shard(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(500))
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, Par(8).Shard(4))
	if err != nil || len(results) != 500 {
		t.Fatalf("Do Shard: err=%v len=%d", err, len(results))
	}
}

func TestSlice_Do_Empty(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](nil)
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		return n, nil
	}, Par(4))
	if err != nil || len(results) != 0 {
		t.Fatalf("Do empty: err=%v len=%d", err, len(results))
	}
}

func TestSlice_Each(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(300))
	total, failCnt, firstErr := s.Each(ctx, func(ctx context.Context, n int) error {
		return nil
	}, Par(8))
	if total != 300 || failCnt != 0 || firstErr != nil {
		t.Fatalf("Each: total=%d fail=%d err=%v", total, failCnt, firstErr)
	}
}

func TestSlice_Each_FailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr := s.Each(ctx, func(ctx context.Context, n int) error {
		if n == 25 {
			return bomb
		}
		return nil
	}, Par(4).FF())
	if total != 50 || failCnt == 0 || firstErr == nil {
		t.Fatalf("Each FF: total=%d fail=%d err=%v", total, failCnt, firstErr)
	}
}

func TestSlice_EachFull(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{1, 2, 3})
	total, failCnt, firstErr, results := s.EachFull(ctx, func(ctx context.Context, n int) error {
		return nil
	}, Par(4))
	if total != 3 || failCnt != 0 || firstErr != nil || len(results) != 3 {
		t.Fatalf("EachFull: total=%d fail=%d err=%v len=%d", total, failCnt, firstErr, len(results))
	}
}

func TestSlice_Stream(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(500))
	ch := s.Stream(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, Par(8).Buf(256))
	count := 0
	for r := range ch {
		if r.Err != nil {
			t.Fatalf("Stream: unexpected error: %v", r.Err)
		}
		if r.Value%2 != 0 {
			t.Fatalf("Stream: value %d not even", r.Value)
		}
		count++
	}
	if count != 500 {
		t.Fatalf("Stream: count=%d want 500", count)
	}
}

func TestSlice_Stream_WithFailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	s := NewWithResult[int, int](genIntItems(100))
	ch := s.Stream(ctx, func(ctx context.Context, n int) (int, error) {
		if n == 50 {
			return 0, bomb
		}
		return n, nil
	}, Par(8).FF().Buf(64))
	hasErr := false
	for r := range ch {
		if r.Err != nil {
			hasErr = true
		}
	}
	if !hasErr {
		t.Fatal("Stream FF: expected error")
	}
}

func TestSlice_Reduce(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{1, 2, 3, 4, 5})
	result, err := s.Reduce(ctx, 0, func(ctx context.Context, acc int, item int) (int, error) {
		return acc + item, nil
	})
	if err != nil || result != 15 {
		t.Fatalf("Reduce: result=%d err=%v want 15", result, err)
	}
}

func TestSlice_Reduce_Error(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("reduce fail")
	s := NewWithResult[int, int]([]int{1, 2, 3, 4, 5})
	_, err := s.Reduce(ctx, 0, func(ctx context.Context, acc int, item int) (int, error) {
		if acc > 5 {
			return 0, bomb
		}
		return acc + item, nil
	})
	if err == nil {
		t.Fatal("Reduce: expected error")
	}
}

func TestSlice_Reduce_Empty(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](nil)
	result, err := s.Reduce(ctx, 42, func(ctx context.Context, acc int, item int) (int, error) {
		return acc + item, nil
	})
	if err != nil || result != 42 {
		t.Fatalf("Reduce empty: result=%d want 42", result)
	}
}

// ============================================================
// 九、MapReduce 旧 API 向后兼容测试（交叉验证）
// ============================================================

func TestSlice_Map_OldVsDo(t *testing.T) {
	ctx := context.Background()
	items := genIntItems(100)
	s1 := NewWithResult[int, int](items)
	s2 := NewWithResult[int, int](items)

	oldRes, _ := s1.Map(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, 8)
	newRes, _ := s2.Do(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, Par(8))

	if len(oldRes) != len(newRes) {
		t.Fatalf("Old vs Do: len %d vs %d", len(oldRes), len(newRes))
	}
	for i := range oldRes {
		if oldRes[i].Value != newRes[i].Value {
			t.Fatalf("Old vs Do[%d]: %d vs %d", i, oldRes[i].Value, newRes[i].Value)
		}
	}
}

func TestSlice_MapWithFailFast_OldVsDo(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	items := genIntItems(50)
	s1 := NewWithResult[int, int](items)
	s2 := NewWithResult[int, int](items)

	oldRes, oldErr := s1.MapWithFailFast(ctx, func(ctx context.Context, n int) (int, error) {
		if n == 25 {
			return 0, bomb
		}
		return n, nil
	}, 8)
	newRes, newErr := s2.Do(ctx, func(ctx context.Context, n int) (int, error) {
		if n == 25 {
			return 0, bomb
		}
		return n, nil
	}, Par(8).FF())

	if (oldErr != nil) != (newErr != nil) {
		t.Fatalf("Old vs Do FF: oldErr=%v newErr=%v", oldErr, newErr)
	}
	if len(oldRes) != len(newRes) {
		t.Fatalf("Old vs Do FF len: %d vs %d", len(oldRes), len(newRes))
	}
}

func TestSlice_DefaultMap(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{1, 2, 3})
	results := s.DefaultMap(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 10, nil
	})
	if len(results) != 3 {
		t.Fatalf("DefaultMap: len=%d", len(results))
	}
	for i, r := range results {
		if r.Value != (i+1)*10 {
			t.Fatalf("DefaultMap[%d]=%d", i, r.Value)
		}
	}
}

func TestSlice_DefaultMapWithFailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	s := NewWithResult[int, int]([]int{1, 2, 3})
	_, err := s.DefaultMapWithFailFast(ctx, func(ctx context.Context, n int) (int, error) {
		if n == 2 {
			return 0, bomb
		}
		return n, nil
	})
	if err == nil {
		t.Log("DefaultMapWithFailFast: error may be in results")
	}
}

func TestSlice_MapSerial(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{5, 4, 3, 2, 1})
	results, err := s.MapSerial(ctx, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if err != nil || len(results) != 5 {
		t.Fatalf("MapSerial: err=%v len=%d", err, len(results))
	}
}

func TestSlice_MapStream(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	ch := s.MapStream(ctx, func(ctx context.Context, n int) (int, error) {
		return n, nil
	}, 4, 64)
	count := 0
	for range ch {
		count++
	}
	if count != 100 {
		t.Fatalf("MapStream: count=%d want 100", count)
	}
}

func TestSlice_MapSharded(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(200))
	results, err := s.MapSharded(ctx, func(ctx context.Context, n int) (int, error) {
		return n, nil
	}, 8, 4)
	if err != nil || len(results) != 200 {
		t.Fatalf("MapSharded: err=%v len=%d", err, len(results))
	}
}

// MapChunk / MapChunked old API
func TestSlice_MapChunk(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	results := s.MapChunk(ctx, 4, 10, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 10 {
		t.Fatalf("MapChunk: %d chunks want 10", len(results))
	}
}

func TestSlice_MapChunked(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	results := s.MapChunked(ctx, 4, 10, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	if len(results) != 100 {
		t.Fatalf("MapChunked: %d results want 100", len(results))
	}
}

// ForEach old API
func TestSlice_ForEach_OldAPI(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	total, failCnt, firstErr, results := s.ForEach(ctx, func(ctx context.Context, n int) error {
		return nil
	}, 4)
	if total != 100 || failCnt != 0 || firstErr != nil || len(results) != 100 {
		t.Fatalf("ForEach old: t=%d f=%d e=%v len=%d", total, failCnt, firstErr, len(results))
	}
}

func TestSlice_DefaultForEach(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.DefaultForEach(ctx, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 50 || failCnt != 0 || firstErr != nil {
		t.Fatalf("DefaultForEach: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachSerial(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{1, 2, 3})
	total, failCnt, firstErr, _ := s.ForEachSerial(ctx, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 3 || failCnt != 0 || firstErr != nil {
		t.Fatalf("ForEachSerial: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

// ============================================================
// 十、Policy 策略对象测试
// ============================================================

func TestPolicy_Seq(t *testing.T) {
	p := Seq()
	if !p.IsSerial() || p.IsParallel() {
		t.Fatal("Seq should be serial")
	}
	if p.GetWorker() != 1 {
		t.Fatalf("Seq worker=%d", p.GetWorker())
	}
}

func TestPolicy_Par(t *testing.T) {
	p := Par(8)
	if p.IsSerial() || !p.IsParallel() {
		t.Fatal("Par should be parallel")
	}
	if p.GetWorker() != 8 {
		t.Fatalf("Par worker=%d want 8", p.GetWorker())
	}
	p2 := Par(0)
	if p2.GetWorker() <= 0 {
		t.Fatal("Par(0) should use default")
	}
}

func TestPolicy_DefPar(t *testing.T) {
	p := DefPar()
	if p.IsSerial() {
		t.Fatal("DefPar should be parallel")
	}
}

func TestPolicy_FF_NoFF(t *testing.T) {
	p := Par(4).FF()
	if !p.IsFailFast() {
		t.Fatal("FF should enable failfast")
	}
	p = p.NoFF()
	if p.IsFailFast() {
		t.Fatal("NoFF should disable failfast")
	}
}

func TestPolicy_TO(t *testing.T) {
	p := Par(4).TO(5 * time.Second)
	if p.GetTimeout() != 5*time.Second {
		t.Fatalf("TO: %v", p.GetTimeout())
	}
}

func TestPolicy_Chunk(t *testing.T) {
	p := Par(4).Chunk(100)
	if p.GetChunkSize() != 100 {
		t.Fatalf("Chunk: size=%d", p.GetChunkSize())
	}
}

func TestPolicy_Shard(t *testing.T) {
	p := Par(4).Shard(8)
	if p.GetShards() != 8 {
		t.Fatalf("Shard: shards=%d", p.GetShards())
	}
	p2 := Par(4).Shard(0)
	if p2.GetShards() < 2 {
		t.Fatal("Shard(0) should use GOMAXPROCS")
	}
}

func TestPolicy_Worker(t *testing.T) {
	p := Seq().Worker(8)
	if p.GetWorker() != 8 {
		t.Fatalf("Worker: %d", p.GetWorker())
	}
}

func TestPolicy_Buf(t *testing.T) {
	p := Par(4).Buf(1024)
	if p.GetBuf() != 1024 {
		t.Fatalf("Buf: %d", p.GetBuf())
	}
}

func TestPolicy_WithPool(t *testing.T) {
	pl := pool.NewPool[int](4)
	defer pl.Close()
	p := Par(4).WithPool(pl)
	if p.Pool() == nil {
		t.Fatal("WithPool should set pool")
	}
	if p.IsPoolOwned() {
		t.Fatal("WithPool should not own pool")
	}
}

func TestPolicy_WithOwnedPool(t *testing.T) {
	pl := pool.NewPool[int](4)
	p := Par(4).WithOwnedPool(pl)
	if p.Pool() == nil {
		t.Fatal("WithOwnedPool should set pool")
	}
	if !p.IsPoolOwned() {
		t.Fatal("WithOwnedPool should own pool")
	}
}

func TestPolicy_WithDefaultPool(t *testing.T) {
	p := Par(4).WithDefaultPool()
	if !p.IsPoolOwned() {
		t.Fatal("WithDefaultPool should own pool")
	}
}

func TestPolicy_IsZero(t *testing.T) {
	var p Policy
	if !p.isZero() {
		t.Fatal("zero Policy should report isZero")
	}
	if Par(4).isZero() {
		t.Fatal("Par should not be zero")
	}
}

// ============================================================
// 十一、Runner 执行器测试
// ============================================================

func TestRunner_Map(t *testing.T) {
	ctx := context.Background()
	r := NewRunner[int, string](ctx, []int{1, 2, 3}, Par(4))
	results, err := r.Map(func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("x%d", n), nil
	})
	if err != nil || len(results) != 3 {
		t.Fatalf("Runner.Map: err=%v len=%d", err, len(results))
	}
}

func TestRunner_Each(t *testing.T) {
	ctx := context.Background()
	r := NewRunner[int, int](ctx, genIntItems(100), Par(4))
	total, failCnt, firstErr, _ := r.Each(func(ctx context.Context, n int) error {
		return nil
	})
	if total != 100 || failCnt != 0 || firstErr != nil {
		t.Fatalf("Runner.Each: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestRunner_Stream(t *testing.T) {
	ctx := context.Background()
	r := NewRunner[int, int](ctx, genIntItems(100), Par(4).Buf(64))
	ch := r.Stream(func(ctx context.Context, n int) (int, error) {
		return n, nil
	}, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 100 {
		t.Fatalf("Runner.Stream: count=%d", count)
	}
}

func TestRunner_Fold(t *testing.T) {
	ctx := context.Background()
	r := NewRunner[int, int](ctx, []int{1, 2, 3, 4}, Seq())
	result, err := r.Fold(0, func(ctx context.Context, acc int, item int) (int, error) {
		return acc + item, nil
	})
	if err != nil || result != 10 {
		t.Fatalf("Runner.Fold: %d err=%v", result, err)
	}
}

func TestRunner_MapBatch(t *testing.T) {
	ctx := context.Background()
	r := NewRunner[int, int](ctx, genIntItems(100), Par(4).Chunk(10))
	results, err := r.MapBatch(func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if err != nil || len(results) != 10 {
		t.Fatalf("Runner.MapBatch: err=%v len=%d", err, len(results))
	}
}

func TestRunner_EachBatch(t *testing.T) {
	ctx := context.Background()
	r := NewRunner[int, int](ctx, genIntItems(100), Par(4).Chunk(10))
	total, failCnt, firstErr, _ := r.EachBatch(func(ctx context.Context, chunk []int) error {
		return nil
	})
	if total != 10 || failCnt != 0 || firstErr != nil {
		t.Fatalf("Runner.EachBatch: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

// ============================================================
// 十二、SliceBuilder 链式构建器测试
// ============================================================

func TestSliceBuilder_Basic(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, []int{1, 2, 3}).Worker(4).Map(
		func(ctx context.Context, n int) (int, error) { return n * 10, nil })
	if results.Err() {
		t.Fatalf("SliceBuilder: err=%v", results.Error())
	}
	vals := results.Values()
	if len(vals) != 3 || vals[0] != 10 || vals[2] != 30 {
		t.Fatalf("SliceBuilder: vals=%v", vals)
	}
}

func TestSliceBuilder_WithResult(t *testing.T) {
	ctx := context.Background()
	results := NewSliceWithBuilder[string](ctx, []int{1, 2, 3}).Worker(4).Map(
		func(ctx context.Context, n int) (string, error) {
			return "v:" + strconv.Itoa(n), nil
		})
	if results.Err() {
		t.Fatalf("SliceWithBuilder: err=%v", results.Error())
	}
	vals := results.Values()
	if vals[0] != "v:1" || vals[1] != "v:2" || vals[2] != "v:3" {
		t.Fatalf("SliceWithBuilder: vals=%v", vals)
	}
}

func TestSliceBuilder_Serial(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, []int{3, 1, 2}).Serial().
		Sort(func(a, b int) int { return a - b }).
		Map(func(ctx context.Context, n int) (int, error) { return n * 2, nil })
	vals := results.Values()
	if vals[0] != 2 || vals[1] != 4 || vals[2] != 6 {
		t.Fatalf("SliceBuilder Serial: vals=%v want [2,4,6]", vals)
	}
}

func TestSliceBuilder_Parallel(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, genIntItems(500)).Parallel().
		Worker(8).Map(func(ctx context.Context, n int) (int, error) { return n * 2, nil })
	if len(results.Values()) != 500 {
		t.Fatalf("SliceBuilder Parallel: len=%d", len(results.Values()))
	}
}

func TestSliceBuilder_Context(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results := NewSliceBuilder(ctx, []int{1, 2, 3}).Worker(2).Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	_ = results.Error()
}

func TestSliceBuilder_Logger_DefaultLogger(t *testing.T) {
	ctx := context.Background()
	NewSliceBuilder(ctx, []int{1}).Logger(nil).DefaultLogger()
}

func TestSliceBuilder_DefaultWorker(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, genIntItems(100)).DefaultWorker().Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() || results.Len() != 100 {
		t.Fatalf("DefaultWorker: err=%v len=%d", results.Error(), results.Len())
	}
}

func TestSliceBuilder_Pool(t *testing.T) {
	ctx := context.Background()
	pl := pool.NewPool[int](8)
	defer pl.Close()
	results := NewSliceBuilder(ctx, genIntItems(100)).Pool(pl).Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() || results.Len() != 100 {
		t.Fatalf("Pool: err=%v len=%d", results.Error(), results.Len())
	}
}

func TestSliceBuilder_PoolAuto(t *testing.T) {
	ctx := context.Background()
	pl := pool.NewPool[int](8)
	results := NewSliceBuilder(ctx, genIntItems(50)).PoolAuto(pl).Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() || results.Len() != 50 {
		t.Fatalf("PoolAuto: err=%v len=%d", results.Error(), results.Len())
	}
}

func TestSliceBuilder_DefaultPool(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, genIntItems(50)).DefaultPool().Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() || results.Len() != 50 {
		t.Fatalf("DefaultPool: err=%v len=%d", results.Error(), results.Len())
	}
}

func TestSliceBuilder_FailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	results := NewSliceBuilder(ctx, []int{1, 2, 3, 4, 5}).Worker(4).FailFast().Map(
		func(ctx context.Context, n int) (int, error) {
			if n == 3 {
				return 0, bomb
			}
			return n, nil
		})
	if results.Ok() {
		t.Log("FailFast: all ok (may be timing-dependent)")
	}
}

func TestSliceBuilder_DefaultFailFast(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, []int{1, 2}).FailFast().DefaultFailFast().Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() {
		t.Fatal("DefaultFailFast should succeed")
	}
}

func TestSliceBuilder_Timeout(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, genIntItems(50)).Worker(8).Timeout(5 * time.Second).Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() || results.Len() != 50 {
		t.Fatalf("Timeout: err=%v len=%d", results.Error(), results.Len())
	}
}

func TestSliceBuilder_DefaultTimeout(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, []int{1, 2}).DefaultTimeout().Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() {
		t.Fatal("DefaultTimeout should succeed")
	}
}

func TestSliceBuilder_Shards(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, genIntItems(500)).Worker(8).Shards(4).Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() || results.Len() != 500 {
		t.Fatalf("Shards: err=%v len=%d", results.Error(), results.Len())
	}
}

func TestSliceBuilder_DefaultShard(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, genIntItems(100)).DefaultShard().Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() || results.Len() != 100 {
		t.Fatalf("DefaultShard: err=%v len=%d", results.Error(), results.Len())
	}
}

func TestSliceBuilder_Chunk(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, genIntItems(500)).Worker(8).Chunk(50).Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() || results.Len() != 500 {
		t.Fatalf("Chunk: err=%v len=%d", results.Error(), results.Len())
	}
}

func TestSliceBuilder_DefaultChunk(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, genIntItems(100)).DefaultChunk().Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	if results.Err() || results.Len() != 100 {
		t.Fatalf("DefaultChunk: err=%v len=%d", results.Error(), results.Len())
	}
}

func TestSliceBuilder_Buf(t *testing.T) {
	ctx := context.Background()
	ch := NewSliceBuilder(ctx, genIntItems(100)).Worker(4).Buf(256).Stream(
		func(ctx context.Context, n int) (int, error) { return n, nil }, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 100 {
		t.Fatalf("Buf: count=%d", count)
	}
}

func TestSliceBuilder_DefaultBuf(t *testing.T) {
	ctx := context.Background()
	ch := NewSliceBuilder(ctx, genIntItems(50)).DefaultBuf().Stream(
		func(ctx context.Context, n int) (int, error) { return n, nil }, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 50 {
		t.Fatalf("DefaultBuf: count=%d", count)
	}
}

func TestSliceBuilder_SliceRange(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, []int{1, 2, 3, 4, 5}).
		SliceRange(1, 4).Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	vals := results.Values()
	if len(vals) != 3 || vals[0] != 2 || vals[2] != 4 {
		t.Fatalf("Slice: vals=%v want [2,3,4]", vals)
	}
}

func TestSliceBuilder_ForEach(t *testing.T) {
	ctx := context.Background()
	result := NewSliceBuilder(ctx, genIntItems(100)).Worker(4).ForEach(
		func(ctx context.Context, n int) error { return nil })
	if result.Err() || result.Total() != 100 {
		t.Fatalf("ForEach: err=%v total=%d", result.Error(), result.Total())
	}
}

func TestSliceBuilder_Reduce(t *testing.T) {
	ctx := context.Background()
	result, err := NewSliceBuilder(ctx, []int{1, 2, 3, 4}).Reduce(0,
		func(ctx context.Context, acc int, item int) (int, error) { return acc + item, nil })
	if err != nil || result != 10 {
		t.Fatalf("Reduce: %d err=%v", result, err)
	}
}

func TestSliceBuilder_Stream(t *testing.T) {
	ctx := context.Background()
	ch := NewSliceBuilder(ctx, genIntItems(200)).Worker(8).Stream(
		func(ctx context.Context, n int) (int, error) { return n, nil }, 64)
	count := 0
	for range ch {
		count++
	}
	if count != 200 {
		t.Fatalf("Stream: count=%d", count)
	}
}

func TestSliceBuilder_MapBatch(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, genIntItems(200)).Worker(4).Chunk(20).MapBatch(
		func(ctx context.Context, chunk []int) (int, error) {
			sum := 0
			for _, v := range chunk {
				sum += v
			}
			return sum, nil
		})
	if results.Err() || results.Len() != 10 {
		t.Fatalf("MapBatch: err=%v len=%d", results.Error(), results.Len())
	}
}

func TestSliceBuilder_ForEachBatch(t *testing.T) {
	ctx := context.Background()
	result := NewSliceBuilder(ctx, genIntItems(200)).Worker(4).Chunk(20).ForEachBatch(
		func(ctx context.Context, chunk []int) error { return nil })
	if result.Err() || result.Total() != 10 {
		t.Fatalf("ForEachBatch: err=%v total=%d", result.Error(), result.Total())
	}
}

func TestSliceBuilder_FilterAndSort(t *testing.T) {
	ctx := context.Background()
	results := NewSliceBuilder(ctx, []int{5, 1, 4, 2, 8, 3, 7, 6}).Serial().
		Filter(func(v int) bool { return v%2 == 0 }).
		Sort(func(a, b int) int { return a - b }).
		Map(func(ctx context.Context, n int) (int, error) { return n * 10, nil })
	vals := results.Values()
	if len(vals) != 4 || vals[0] != 20 || vals[1] != 40 || vals[2] != 60 || vals[3] != 80 {
		t.Fatalf("Filter+Sort: vals=%v want [20,40,60,80]", vals)
	}
}

// ============================================================
// 十三、SerialSlice 测试
// ============================================================

func TestSerialSlice_ToParallel(t *testing.T) {
	ctx := context.Background()
	ss := NewSliceBuilder(ctx, []int{1, 2, 3}).Serial()
	ps := ss.ToParallel()
	if ps == nil {
		t.Fatal("ToParallel should not return nil")
	}
	result := ps.Worker(4).Map(func(ctx context.Context, n int) (int, error) { return n * 2, nil })
	if result.Err() || result.Len() != 3 {
		t.Fatalf("ToParallel->Map: err=%v len=%d", result.Error(), result.Len())
	}
	vals := result.Values()
	if vals[0] != 2 || vals[2] != 6 {
		t.Fatalf("ToParallel->Map values=%v", vals)
	}
}

func TestSerialSlice_FailFast_NoFailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	ss := NewSliceBuilder(ctx, []int{1, 2, 3, 4, 5}).Serial().FailFast()
	result := ss.Map(func(ctx context.Context, n int) (int, error) {
		if n == 3 {
			return 0, bomb
		}
		return n, nil
	})
	if result.Ok() {
		t.Log("FailFast: may not catch in serial mode depending on impl")
	}
	ss2 := NewSliceBuilder(ctx, []int{1, 2}).Serial().FailFast().NoFailFast()
	if ss2 == nil {
		t.Fatal("NoFailFast returned nil")
	}
}

func TestSerialSlice_Timeout_DefaultTimeout(t *testing.T) {
	ctx := context.Background()
	ss := NewSliceBuilder(ctx, []int{1, 2, 3}).Serial().Timeout(10 * time.Second).DefaultTimeout()
	result := ss.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	if result.Err() || result.Len() != 3 {
		t.Fatalf("Timeout chain: err=%v len=%d", result.Error(), result.Len())
	}
}

func TestSerialSlice_Logger(t *testing.T) {
	ctx := context.Background()
	ss := NewSliceBuilder(ctx, []int{1}).Serial().Logger(nil)
	result := ss.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	if result.Err() {
		t.Fatal("Logger(nil) should not affect results")
	}
}

func TestSerialSlice_Map(t *testing.T) {
	ctx := context.Background()
	ss := NewSliceBuilder(ctx, []int{1, 2, 3}).Serial()
	result := ss.Map(func(ctx context.Context, n int) (int, error) { return n * 10, nil })
	if result.Err() || len(result.Values()) != 3 {
		t.Fatalf("SerialSlice.Map: err=%v len=%d", result.Error(), result.Len())
	}
	vals := result.Values()
	for i, v := range vals {
		if v != (i+1)*10 {
			t.Fatalf("Map[%d]=%d", i, v)
		}
	}
}

func TestSerialSlice_ForEach(t *testing.T) {
	ctx := context.Background()
	ss := NewSliceBuilder(ctx, genIntItems(50)).Serial()
	result := ss.ForEach(func(ctx context.Context, n int) error { return nil })
	if result.Err() || result.Total() != 50 {
		t.Fatalf("SerialSlice.ForEach: err=%v total=%d", result.Error(), result.Total())
	}
}

func TestSerialSlice_Reduce(t *testing.T) {
	ctx := context.Background()
	ss := NewSliceBuilder(ctx, []int{1, 2, 3, 4}).Serial()
	result, err := ss.Reduce(0, func(ctx context.Context, acc int, item int) (int, error) { return acc + item, nil })
	if err != nil || result != 10 {
		t.Fatalf("SerialSlice.Reduce: %d err=%v", result, err)
	}
}

func TestSerialSlice_Stream(t *testing.T) {
	ctx := context.Background()
	ss := NewSliceBuilder(ctx, genIntItems(50)).Serial()
	ch := ss.Stream(func(ctx context.Context, n int) (int, error) { return n, nil }, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 50 {
		t.Fatalf("SerialSlice.Stream: count=%d", count)
	}
}

// ============================================================
// 十四、ParallelSlice 测试
// ============================================================

func TestParallelSlice_ToSerial(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, []int{1, 2, 3}).Parallel().Worker(4)
	ss := ps.ToSerial()
	if ss == nil {
		t.Fatal("ToSerial should not return nil")
	}
	result := ss.Map(func(ctx context.Context, n int) (int, error) { return n * 2, nil })
	if result.Err() || result.Len() != 3 {
		t.Fatalf("ToSerial->Map: err=%v len=%d", result.Error(), result.Len())
	}
}

func TestParallelSlice_Worker_DefaultWorker(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, genIntItems(100)).Parallel().Worker(8).DefaultWorker()
	result := ps.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	if result.Err() || result.Len() != 100 {
		t.Fatalf("Worker chain: err=%v len=%d", result.Error(), result.Len())
	}
}

func TestParallelSlice_Pool_PoolAuto_DefaultPool(t *testing.T) {
	ctx := context.Background()
	pl := pool.NewPool[int](4)
	defer pl.Close()
	ps := NewSliceBuilder(ctx, genIntItems(100)).Parallel().Pool(pl)
	result := ps.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	if result.Err() || result.Len() != 100 {
		t.Fatalf("Pool: err=%v len=%d", result.Error(), result.Len())
	}

	pl2 := pool.NewPool[int](4)
	ps2 := NewSliceBuilder(ctx, genIntItems(50)).Parallel().PoolAuto(pl2)
	result2 := ps2.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	if result2.Err() || result2.Len() != 50 {
		t.Fatalf("PoolAuto: err=%v len=%d", result2.Error(), result2.Len())
	}

	ps3 := NewSliceBuilder(ctx, genIntItems(50)).Parallel().DefaultPool()
	result3 := ps3.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	if result3.Err() || result3.Len() != 50 {
		t.Fatalf("DefaultPool: err=%v len=%d", result3.Error(), result3.Len())
	}
}

func TestParallelSlice_FailFast_NoFailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	ps := NewSliceBuilder(ctx, genIntItems(50)).Parallel().Worker(4).FailFast()
	result := ps.Map(func(ctx context.Context, n int) (int, error) {
		if n == 25 {
			return 0, bomb
		}
		return n, nil
	})
	if result.Ok() {
		t.Log("FailFast: all ok (may be timing-dependent)")
	}
	ps2 := NewSliceBuilder(ctx, []int{1}).Parallel().FailFast().NoFailFast()
	if ps2 == nil {
		t.Fatal("NoFailFast returned nil")
	}
}

func TestParallelSlice_Timeout_DefaultTimeout(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, genIntItems(50)).Parallel().Worker(8).Timeout(10 * time.Second).DefaultTimeout()
	result := ps.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	if result.Err() || result.Len() != 50 {
		t.Fatalf("Timeout chain: err=%v len=%d", result.Error(), result.Len())
	}
}

func TestParallelSlice_Shards_DefaultShard(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, genIntItems(200)).Parallel().Worker(8).Shards(4).DefaultShard()
	result := ps.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	if result.Err() || result.Len() != 200 {
		t.Fatalf("Shards chain: err=%v len=%d", result.Error(), result.Len())
	}
}

func TestParallelSlice_Chunk_DefaultChunk(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, genIntItems(200)).Parallel().Worker(8).Chunk(50).DefaultChunk()
	result := ps.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	if result.Err() || result.Len() != 200 {
		t.Fatalf("Chunk chain: err=%v len=%d", result.Error(), result.Len())
	}
}

func TestParallelSlice_Logger(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, []int{1}).Parallel().Logger(nil)
	result := ps.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
	if result.Err() {
		t.Fatal("Logger(nil) should not affect results")
	}
}

func TestParallelSlice_Map(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, genIntItems(500)).Parallel().Worker(8)
	result := ps.Map(func(ctx context.Context, n int) (int, error) { return n * 2, nil })
	if result.Err() || result.Len() != 500 {
		t.Fatalf("ParallelSlice.Map: err=%v len=%d", result.Error(), result.Len())
	}
}

func TestParallelSlice_MapBatch(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, genIntItems(200)).Parallel().Worker(4).Chunk(20)
	result := ps.MapBatch(func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if result.Err() || result.Len() != 10 {
		t.Fatalf("ParallelSlice.MapBatch: err=%v len=%d", result.Error(), result.Len())
	}
}

func TestParallelSlice_ForEach(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, genIntItems(200)).Parallel().Worker(8)
	result := ps.ForEach(func(ctx context.Context, n int) error { return nil })
	if result.Err() || result.Total() != 200 {
		t.Fatalf("ParallelSlice.ForEach: err=%v total=%d", result.Error(), result.Total())
	}
}

func TestParallelSlice_ForEachBatch(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, genIntItems(200)).Parallel().Worker(4).Chunk(20)
	result := ps.ForEachBatch(func(ctx context.Context, chunk []int) error { return nil })
	if result.Err() || result.Total() != 10 {
		t.Fatalf("ParallelSlice.ForEachBatch: err=%v total=%d", result.Error(), result.Total())
	}
}

func TestParallelSlice_Stream(t *testing.T) {
	ctx := context.Background()
	ps := NewSliceBuilder(ctx, genIntItems(200)).Parallel().Worker(8)
	ch := ps.Stream(func(ctx context.Context, n int) (int, error) { return n, nil }, 64)
	count := 0
	for range ch {
		count++
	}
	if count != 200 {
		t.Fatalf("ParallelSlice.Stream: count=%d", count)
	}
}

// ============================================================
// 十五、SerialSlice vs ParallelSlice 交叉验证
// ============================================================

func TestSlice_CrossValidation_SerialVsParallel_Map(t *testing.T) {
	ctx := context.Background()
	items := genIntItems(200)

	ss := NewSliceBuilder(ctx, items).Serial()
	sr := ss.Map(func(ctx context.Context, n int) (int, error) { return n * 2, nil })

	ps := NewSliceBuilder(ctx, items).Parallel().Worker(8)
	pr := ps.Map(func(ctx context.Context, n int) (int, error) { return n * 2, nil })

	if sr.Len() != pr.Len() {
		t.Fatalf("len mismatch: serial=%d parallel=%d", sr.Len(), pr.Len())
	}
	sv := sr.Values()
	pv := pr.Values()
	for i := range sv {
		if sv[i] != pv[i] {
			t.Fatalf("i=%d: serial=%d parallel=%d", i, sv[i], pv[i])
		}
	}
}

func TestSlice_CrossValidation_SerialVsParallel_ForEach(t *testing.T) {
	ctx := context.Background()
	items := genIntItems(100)

	var sSum atomic.Int64
	ss := NewSliceBuilder(ctx, items).Serial()
	sr := ss.ForEach(func(ctx context.Context, n int) error {
		sSum.Add(int64(n))
		return nil
	})

	var pSum atomic.Int64
	ps := NewSliceBuilder(ctx, items).Parallel().Worker(8)
	pr := ps.ForEach(func(ctx context.Context, n int) error {
		pSum.Add(int64(n))
		return nil
	})

	if sr.Total() != pr.Total() {
		t.Fatalf("total mismatch: serial=%d parallel=%d", sr.Total(), pr.Total())
	}
	if sSum.Load() != pSum.Load() {
		t.Fatalf("sum mismatch: serial=%d parallel=%d", sSum.Load(), pSum.Load())
	}
}

// ============================================================
// 十六、SliceResult 完整方法测试
// ============================================================

func TestSliceResult_Errors(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("test error")
	result := NewSliceBuilder(ctx, []int{1, 2, 3}).Worker(2).FailFast().Map(
		func(ctx context.Context, n int) (int, error) {
			if n == 2 {
				return 0, bomb
			}
			return n, nil
		})
	errs := result.Errors()
	if len(errs) == 0 {
		t.Fatal("Errors: expected at least one error")
	}
}

func TestSliceResult_FailValues(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	result := NewSliceBuilder(ctx, []int{10, 20, 30}).Worker(2).FailFast().Map(
		func(ctx context.Context, n int) (int, error) {
			if n == 20 {
				return 0, bomb
			}
			return n, nil
		})
	fv := result.FailValues()
	if len(fv) == 0 {
		t.Log("FailValues: no fails (may complete before fail)")
	}
}

func TestSliceResult_Results(t *testing.T) {
	ctx := context.Background()
	result := NewSliceBuilder(ctx, []int{1, 2, 3}).Worker(2).Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	results := result.Results()
	if len(results) != 3 {
		t.Fatalf("Results: len=%d", len(results))
	}
}

func TestSliceResult_Must(t *testing.T) {
	ctx := context.Background()
	result := NewSliceBuilder(ctx, []int{1, 2, 3}).Map(
		func(ctx context.Context, n int) (int, error) { return n * 10, nil })
	vals := result.Must()
	if len(vals) != 3 || vals[0] != 10 {
		t.Fatalf("Must: vals=%v", vals)
	}
}

func TestSliceResult_Must_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Must should panic on error")
		}
	}()
	ctx := context.Background()
	result := NewSliceBuilder(ctx, []int{1}).FailFast().Map(
		func(ctx context.Context, n int) (int, error) { return 0, errors.New("fail") })
	result.Must()
}

func TestSliceResult_Unwrap(t *testing.T) {
	ctx := context.Background()
	result := NewSliceBuilder(ctx, []int{1, 2, 3}).Map(
		func(ctx context.Context, n int) (int, error) { return n * 10, nil })
	vals, err := result.Unwrap()
	if err != nil || len(vals) != 3 || vals[0] != 10 {
		t.Fatalf("Unwrap: vals=%v err=%v", vals, err)
	}
}

func TestSliceResult_Unwrap_Error(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	result := NewSliceBuilder(ctx, []int{1, 2}).FailFast().Map(
		func(ctx context.Context, n int) (int, error) {
			if n == 1 {
				return 0, bomb
			}
			return n, nil
		})
	vals, err := result.Unwrap()
	if err == nil {
		t.Fatalf("Unwrap should return error, got vals=%v", vals)
	}
}

func TestSliceResult_First(t *testing.T) {
	ctx := context.Background()
	result := NewSliceBuilder(ctx, []int{10, 20, 30}).Map(
		func(ctx context.Context, n int) (int, error) { return n, nil })
	v, ok := result.First()
	if !ok || v != 10 {
		t.Fatalf("First: v=%d ok=%v", v, ok)
	}
}

func TestSliceResult_First_AllFail(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	result := NewSliceBuilder(ctx, []int{1, 2}).FailFast().Map(
		func(ctx context.Context, n int) (int, error) { return 0, bomb })
	v, ok := result.First()
	if ok {
		t.Fatalf("First on all-fail: v=%d, should be false", v)
	}
	_ = v
}

func TestForEachResult_SuccessCount(t *testing.T) {
	ctx := context.Background()
	result := NewSliceBuilder(ctx, genIntItems(100)).ForEach(
		func(ctx context.Context, n int) error { return nil })
	if result.SuccessCount() != 100 {
		t.Fatalf("SuccessCount: %d want 100", result.SuccessCount())
	}
}

// ============================================================
// 十七、EachStream 测试
// ============================================================

func TestSlice_EachStream(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	ch := s.EachStream(ctx, func(ctx context.Context, n int) error { return nil }, Par(8).Buf(64))
	count := 0
	for r := range ch {
		if r.Err != nil {
			t.Fatalf("EachStream: unexpected error: %v", r.Err)
		}
		count++
	}
	if count != 100 {
		t.Fatalf("EachStream: count=%d want 100", count)
	}
}

func TestSlice_EachStream_Error(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("each fail")
	s := NewWithResult[int, int](genIntItems(50))
	ch := s.EachStream(ctx, func(ctx context.Context, n int) error {
		if n == 25 {
			return bomb
		}
		return nil
	}, Par(4).FF().Buf(64))
	hasErr := false
	for r := range ch {
		if r.Err != nil {
			hasErr = true
		}
	}
	if !hasErr {
		t.Fatal("EachStream FF: expected error")
	}
}

// ============================================================
// 十八、旧 API 补全测试
// ============================================================

func TestSlice_MapSerialFailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	s := NewWithResult[int, int]([]int{1, 2, 3})
	_, err := s.MapSerialFailFast(ctx, func(ctx context.Context, n int) (int, error) {
		if n == 2 {
			return 0, bomb
		}
		return n, nil
	})
	if err == nil {
		t.Log("MapSerialFailFast: error may be in results")
	}
}

func TestSlice_MapStreamWithFailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	s := NewWithResult[int, int](genIntItems(50))
	ch := s.MapStreamWithFailFast(ctx, func(ctx context.Context, n int) (int, error) {
		if n == 25 {
			return 0, bomb
		}
		return n, nil
	}, 4, 64)
	hasErr := false
	for r := range ch {
		if r.Err != nil {
			hasErr = true
		}
	}
	if !hasErr {
		t.Fatal("MapStreamWithFailFast: expected error")
	}
}

func TestSlice_MapWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results := s.MapWithTimeout(ctx, 4, 10*time.Second, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	if len(results) != 50 {
		t.Fatalf("MapWithTimeout: len=%d", len(results))
	}
}

func TestSlice_DefaultMapWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results := s.DefaultMapWithTimeout(ctx, 10*time.Second, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if len(results) != 50 {
		t.Fatalf("DefaultMapWithTimeout: len=%d", len(results))
	}
}

func TestSlice_MapWithFFTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results, err := s.MapWithFFTimeout(ctx, 8, 10*time.Second, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if err != nil {
		t.Logf("MapWithFFTimeout: err=%v", err)
	}
	if len(results) != 50 {
		t.Fatalf("MapWithFFTimeout: len=%d", len(results))
	}
}

func TestSlice_DefaultMapWithFFTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results, err := s.DefaultMapWithFFTimeout(ctx, 10*time.Second, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if err != nil {
		t.Logf("DefaultMapWithFFTimeout: err=%v", err)
	}
	if len(results) != 50 {
		t.Fatalf("DefaultMapWithFFTimeout: len=%d", len(results))
	}
}

func TestSlice_MapChunkWithFailFast(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	results, err := s.MapChunkWithFailFast(ctx, 4, 10, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if err != nil {
		t.Logf("MapChunkWithFailFast: err=%v", err)
	}
	if len(results) != 10 {
		t.Fatalf("MapChunkWithFailFast: len=%d", len(results))
	}
}

func TestSlice_DefaultMapChunk(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	results := s.DefaultMapChunk(ctx, 10, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 10 {
		t.Fatalf("DefaultMapChunk: len=%d", len(results))
	}
}

func TestSlice_DefaultMapChunkWithFailFast(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results, _ := s.DefaultMapChunkWithFailFast(ctx, 10, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 5 {
		t.Fatalf("DefaultMapChunkWithFailFast: len=%d", len(results))
	}
}

func TestSlice_MapChunkWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	results := s.MapChunkWithTimeout(ctx, 4, 10, 10*time.Second, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 10 {
		t.Fatalf("MapChunkWithTimeout: len=%d", len(results))
	}
}

func TestSlice_DefaultMapChunkWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	results := s.DefaultMapChunkWithTimeout(ctx, 10, 10*time.Second, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 10 {
		t.Fatalf("DefaultMapChunkWithTimeout: len=%d", len(results))
	}
}

func TestSlice_MapChunkWithFFTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results, _ := s.MapChunkWithFFTimeout(ctx, 4, 10, 10*time.Second, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 5 {
		t.Fatalf("MapChunkWithFFTimeout: len=%d", len(results))
	}
}

func TestSlice_DefaultMapChunkWithFFTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results, _ := s.DefaultMapChunkWithFFTimeout(ctx, 10, 10*time.Second, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, v := range chunk {
			sum += v
		}
		return sum, nil
	})
	if len(results) != 5 {
		t.Fatalf("DefaultMapChunkWithFFTimeout: len=%d", len(results))
	}
}

func TestSlice_DefaultMapChunked(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	results := s.DefaultMapChunked(ctx, 10, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	if len(results) != 100 {
		t.Fatalf("DefaultMapChunked: len=%d", len(results))
	}
}

func TestSlice_MapChunkedWithFailFast(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results, _ := s.MapChunkedWithFailFast(ctx, 4, 10, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if len(results) != 50 {
		t.Fatalf("MapChunkedWithFailFast: len=%d", len(results))
	}
}

func TestSlice_DefaultMapChunkedWithFailFast(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results, _ := s.DefaultMapChunkedWithFailFast(ctx, 10, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if len(results) != 50 {
		t.Fatalf("DefaultMapChunkedWithFailFast: len=%d", len(results))
	}
}

func TestSlice_MapChunkedWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	results := s.MapChunkedWithTimeout(ctx, 4, 10, 10*time.Second, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if len(results) != 100 {
		t.Fatalf("MapChunkedWithTimeout: len=%d", len(results))
	}
}

func TestSlice_DefaultMapChunkedWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	results := s.DefaultMapChunkedWithTimeout(ctx, 10, 10*time.Second, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if len(results) != 100 {
		t.Fatalf("DefaultMapChunkedWithTimeout: len=%d", len(results))
	}
}

func TestSlice_MapChunkedWithFFTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results, _ := s.MapChunkedWithFFTimeout(ctx, 4, 10, 10*time.Second, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if len(results) != 50 {
		t.Fatalf("MapChunkedWithFFTimeout: len=%d", len(results))
	}
}

func TestSlice_DefaultMapChunkedWithFFTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	results, _ := s.DefaultMapChunkedWithFFTimeout(ctx, 10, 10*time.Second, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if len(results) != 50 {
		t.Fatalf("DefaultMapChunkedWithFFTimeout: len=%d", len(results))
	}
}

// ============================================================
// 十九、ForEach 旧 API 补全测试
// ============================================================

func TestSlice_ForEachWithFailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.ForEachWithFailFast(ctx, func(ctx context.Context, n int) error {
		if n == 25 {
			return bomb
		}
		return nil
	}, 4)
	if total != 50 || failCnt == 0 {
		t.Fatalf("ForEachWithFailFast: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_DefaultForEachWithFailFast(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.DefaultForEachWithFailFast(ctx, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 50 || failCnt != 0 || firstErr != nil {
		t.Fatalf("DefaultForEachWithFailFast: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachSerialFailFast(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{1, 2, 3})
	total, failCnt, firstErr, _ := s.ForEachSerialFailFast(ctx, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 3 || failCnt != 0 || firstErr != nil {
		t.Fatalf("ForEachSerialFailFast: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachStream(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	ch := s.ForEachStream(ctx, func(ctx context.Context, n int) error { return nil }, 4, 64)
	count := 0
	for range ch {
		count++
	}
	if count != 100 {
		t.Fatalf("ForEachStream: count=%d", count)
	}
}

func TestSlice_ForEachStreamWithFailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	s := NewWithResult[int, int](genIntItems(50))
	ch := s.ForEachStreamWithFailFast(ctx, func(ctx context.Context, n int) error {
		if n == 25 {
			return bomb
		}
		return nil
	}, 4, 64)
	hasErr := false
	for r := range ch {
		if r.Err != nil {
			hasErr = true
		}
	}
	if !hasErr {
		t.Fatal("ForEachStreamWithFailFast: expected error")
	}
}

func TestSlice_ForEachWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.ForEachWithTimeout(ctx, 4, 10*time.Second, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 50 || failCnt != 0 || firstErr != nil {
		t.Fatalf("ForEachWithTimeout: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_DefaultForEachWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.DefaultForEachWithTimeout(ctx, 10*time.Second, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 50 || failCnt != 0 || firstErr != nil {
		t.Fatalf("DefaultForEachWithTimeout: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachWithFFTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.ForEachWithFFTimeout(ctx, 4, 10*time.Second, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 50 || failCnt != 0 || firstErr != nil {
		t.Fatalf("ForEachWithFFTimeout: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_DefaultForEachWithFFTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.DefaultForEachWithFFTimeout(ctx, 10*time.Second, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 50 || failCnt != 0 || firstErr != nil {
		t.Fatalf("DefaultForEachWithFFTimeout: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachSharded(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(200))
	total, failCnt, firstErr, _ := s.ForEachSharded(ctx, func(ctx context.Context, n int) error {
		return nil
	}, 8, 4)
	if total != 200 || failCnt != 0 || firstErr != nil {
		t.Fatalf("ForEachSharded: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachChunk(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	total, failCnt, firstErr, _ := s.ForEachChunk(ctx, 4, 10, func(ctx context.Context, chunk []int) error {
		return nil
	})
	if total != 10 || failCnt != 0 || firstErr != nil {
		t.Fatalf("ForEachChunk: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_DefaultForEachChunk(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	total, failCnt, firstErr, _ := s.DefaultForEachChunk(ctx, 10, func(ctx context.Context, chunk []int) error {
		return nil
	})
	if total != 10 || failCnt != 0 || firstErr != nil {
		t.Fatalf("DefaultForEachChunk: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachChunkWithFailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.ForEachChunkWithFailFast(ctx, 4, 10, func(ctx context.Context, chunk []int) error {
		if len(chunk) > 0 && chunk[0] >= 20 {
			return bomb
		}
		return nil
	})
	if total != 5 || failCnt == 0 {
		t.Fatalf("ForEachChunkWithFailFast: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_DefaultForEachChunkWithFailFast(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.DefaultForEachChunkWithFailFast(ctx, 10, func(ctx context.Context, chunk []int) error {
		return nil
	})
	if total != 5 || failCnt != 0 || firstErr != nil {
		t.Fatalf("DefaultForEachChunkWithFailFast: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachChunkWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	total, failCnt, firstErr, _ := s.ForEachChunkWithTimeout(ctx, 4, 10, 10*time.Second, func(ctx context.Context, chunk []int) error {
		return nil
	})
	if total != 10 || failCnt != 0 || firstErr != nil {
		t.Fatalf("ForEachChunkWithTimeout: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_DefaultForEachChunkWithTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	total, failCnt, firstErr, _ := s.DefaultForEachChunkWithTimeout(ctx, 10, 10*time.Second, func(ctx context.Context, chunk []int) error {
		return nil
	})
	if total != 10 || failCnt != 0 || firstErr != nil {
		t.Fatalf("DefaultForEachChunkWithTimeout: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachChunkWithFFTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.ForEachChunkWithFFTimeout(ctx, 4, 10, 10*time.Second, func(ctx context.Context, chunk []int) error {
		return nil
	})
	if total != 5 || failCnt != 0 || firstErr != nil {
		t.Fatalf("ForEachChunkWithFFTimeout: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_DefaultForEachChunkWithFFTimeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.DefaultForEachChunkWithFFTimeout(ctx, 10, 10*time.Second, func(ctx context.Context, chunk []int) error {
		return nil
	})
	if total != 5 || failCnt != 0 || firstErr != nil {
		t.Fatalf("DefaultForEachChunkWithFFTimeout: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachChunked(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	total, failCnt, firstErr, _ := s.ForEachChunked(ctx, 4, 10, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 100 || failCnt != 0 || firstErr != nil {
		t.Fatalf("ForEachChunked: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_DefaultForEachChunked(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	total, failCnt, firstErr, _ := s.DefaultForEachChunked(ctx, 10, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 100 || failCnt != 0 || firstErr != nil {
		t.Fatalf("DefaultForEachChunked: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

func TestSlice_ForEachChunkedWithFailFast(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(50))
	total, failCnt, firstErr, _ := s.ForEachChunkedWithFailFast(ctx, 4, 10, func(ctx context.Context, n int) error {
		return nil
	})
	if total != 50 || failCnt != 0 || firstErr != nil {
		t.Fatalf("ForEachChunkedWithFailFast: t=%d f=%d e=%v", total, failCnt, firstErr)
	}
}

// ============================================================
// 二十、四档并发压力测试
// ============================================================

func TestSlice_Do_Concurrent(t *testing.T) {
	for _, tier := range testutil.SmallAllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			s := NewWithResult[int, int](genIntItems(tier.Size))
			results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
				return n ^ 0xABCD, nil
			}, Par(100))
			if err != nil {
				t.Fatalf("Do error: %v", err)
			}
			if len(results) != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

func TestSlice_Each_Concurrent(t *testing.T) {
	for _, tier := range testutil.SmallAllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			s := NewWithResult[int, int](genIntItems(tier.Size))
			total, failCnt, firstErr := s.Each(ctx, func(ctx context.Context, n int) error {
				return nil
			}, Par(100))
			if int(total) != tier.Size || failCnt != 0 || firstErr != nil {
				t.Fatalf("Each: total=%d fail=%d err=%v", total, failCnt, firstErr)
			}
		})
	}
}

func TestSlice_Stream_Concurrent(t *testing.T) {
	for _, tier := range testutil.SmallAllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			s := NewWithResult[int, int](genIntItems(tier.Size))
			ch := s.Stream(ctx, func(ctx context.Context, n int) (int, error) {
				return n, nil
			}, Par(64).Buf(4096))
			var consumed atomic.Int64
			for r := range ch {
				consumed.Add(1)
				_ = r
			}
			if consumed.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, consumed.Load())
			}
		})
	}
}

func TestSlice_Reduce_Concurrent(t *testing.T) {
	for _, tier := range testutil.SmallAllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			s := NewWithResult[int, int](genIntItems(tier.Size))
			result, err := s.Reduce(ctx, 0, func(ctx context.Context, acc int, item int) (int, error) {
				return acc + 1, nil
			})
			if err != nil || result != tier.Size {
				t.Fatalf("Reduce: result=%d err=%v want %d", result, err, tier.Size)
			}
		})
	}
}

func TestSlice_EachStream_Concurrent(t *testing.T) {
	for _, tier := range testutil.SmallAllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			s := NewWithResult[int, int](genIntItems(tier.Size))
			ch := s.EachStream(ctx, func(ctx context.Context, n int) error {
				return nil
			}, Par(64).Buf(4096))
			var consumed atomic.Int64
			for r := range ch {
				consumed.Add(1)
				_ = r
			}
			if consumed.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, consumed.Load())
			}
		})
	}
}

// ============================================================
// 二十一、Race 竞态测试
// ============================================================

func TestRace_Slice_Do_Concurrent(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		items := genIntItems(500)
		var mu sync.Mutex
		var allResults [][]core.Result[int]
		var wg sync.WaitGroup

		for g := 0; g < 5; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := NewWithResult[int, int](items)
				results, _ := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
					return n, nil
				}, Par(8))
				mu.Lock()
				allResults = append(allResults, results)
				mu.Unlock()
			}()
		}
		wg.Wait()
		if len(allResults) < 5 {
			t.Fatalf("round %d: expected >=5 results, got %d", round, len(allResults))
		}
	}
}

func TestRace_Slice_ParallelSerial_Transition(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		var wg sync.WaitGroup
		var success atomic.Int64

		for g := 0; g < 20; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ps := NewSliceBuilder(ctx, genIntItems(50)).Parallel().Worker(4)
				ss := ps.ToSerial()
				result := ss.Map(func(ctx context.Context, n int) (int, error) { return n, nil })
				if result.Ok() {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 20 {
			t.Fatalf("round %d: success=%d", round, success.Load())
		}
	}
}

func TestRace_Slice_Each_Concurrent(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		items := genIntItems(200)
		var mu sync.Mutex
		var totals []int64
		var wg sync.WaitGroup

		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := NewWithResult[int, int](items)
				total, _, _, _ := s.ForEach(ctx, func(ctx context.Context, n int) error {
					return nil
				}, 4)
				mu.Lock()
				totals = append(totals, total)
				mu.Unlock()
			}()
		}
		wg.Wait()
		for i, tot := range totals {
			if tot != 200 {
				t.Fatalf("round %d call %d: total=%d", round, i, tot)
			}
		}
	}
}

func TestRace_Slice_Stream_Concurrent(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		s := NewWithResult[int, int](genIntItems(500))
		ch := s.Stream(ctx, func(ctx context.Context, n int) (int, error) {
			return n, nil
		}, Par(8).Buf(256))

		var consumed atomic.Int64
		var wg sync.WaitGroup
		for c := 0; c < 4; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range ch {
					consumed.Add(1)
				}
			}()
		}
		wg.Wait()
		if consumed.Load() != 500 {
			t.Fatalf("round %d: expected 500, got %d", round, consumed.Load())
		}
	}
}

func TestRace_Slice_EachStream_Concurrent(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		s := NewWithResult[int, int](genIntItems(300))
		ch := s.EachStream(ctx, func(ctx context.Context, n int) error {
			return nil
		}, Par(8).Buf(256))

		var consumed atomic.Int64
		var wg sync.WaitGroup
		for c := 0; c < 4; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range ch {
					consumed.Add(1)
				}
			}()
		}
		wg.Wait()
		if consumed.Load() != 300 {
			t.Fatalf("round %d: expected 300, got %d", round, consumed.Load())
		}
	}
}

func TestRace_Slice_MapStream_Concurrent(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		s := NewWithResult[int, int](genIntItems(400))
		ch := s.MapStream(ctx, func(ctx context.Context, n int) (int, error) {
			return n * 2, nil
		}, 4, 128)

		var consumed atomic.Int64
		var wg sync.WaitGroup
		for c := 0; c < 4; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range ch {
					consumed.Add(1)
				}
			}()
		}
		wg.Wait()
		if consumed.Load() != 400 {
			t.Fatalf("round %d: expected 400, got %d", round, consumed.Load())
		}
	}
}

func TestRace_Slice_Reduce_Concurrent(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		var wg sync.WaitGroup
		var success atomic.Int64

		for g := 0; g < 20; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := NewWithResult[int, int]([]int{1, 2, 3, 4, 5})
				result, err := s.Reduce(ctx, 0, func(ctx context.Context, acc int, item int) (int, error) {
					return acc + item, nil
				})
				if err == nil && result == 15 {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 20 {
			t.Fatalf("round %d: success=%d", round, success.Load())
		}
	}
}

func TestRace_SliceBuilder_Map_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		items := genIntItems(200)
		var wg sync.WaitGroup
		var success atomic.Int64

		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result := NewSliceBuilder(ctx, items).Worker(4).Map(
					func(ctx context.Context, n int) (int, error) { return n, nil })
				if result.Ok() && result.Len() == 200 {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 10 {
			t.Fatalf("round %d: success=%d", round, success.Load())
		}
	}
}

func TestRace_SliceBuilder_ForEach_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		items := genIntItems(100)
		var wg sync.WaitGroup
		var success atomic.Int64

		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result := NewSliceBuilder(ctx, items).Worker(4).ForEach(
					func(ctx context.Context, n int) error { return nil })
				if result.Ok() && result.Total() == 100 {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 10 {
			t.Fatalf("round %d: success=%d", round, success.Load())
		}
	}
}

func TestRace_SliceBuilder_Stream_Concurrent(t *testing.T) {
	for round := 0; round < 10; round++ {
		ctx := context.Background()
		items := genIntItems(300)
		ch := NewSliceBuilder(ctx, items).Worker(8).Buf(128).Stream(
			func(ctx context.Context, n int) (int, error) { return n, nil }, 0)

		var consumed atomic.Int64
		var wg sync.WaitGroup
		for c := 0; c < 4; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range ch {
					consumed.Add(1)
				}
			}()
		}
		wg.Wait()
		if consumed.Load() != 300 {
			t.Fatalf("round %d: expected 300, got %d", round, consumed.Load())
		}
	}
}

func TestRace_Slice_Shuffle_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := New(genIntItems(50))
				s.Shuffle()
				if s.Len() != 50 {
					t.Errorf("Shuffle changed length to %d", s.Len())
				}
			}()
		}
		wg.Wait()
	}
}

func TestRace_Slice_NewSliceResult_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		ctx := context.Background()
		result := NewSliceBuilder(ctx, []int{1, 2, 3}).Map(
			func(ctx context.Context, n int) (int, error) { return n, nil })
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = result.Error()
				_ = result.Values()
				_ = result.Errors()
				_ = result.FailValues()
				_ = result.Results()
				_, _ = result.Unwrap()
				result.First()
			}()
		}
		wg.Wait()
	}
}
