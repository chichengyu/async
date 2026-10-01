package maps

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/testutil"
)

// ============================================================
// 一、Map 基础构造测试
// ============================================================

func TestNewMap_Basic(t *testing.T) {
	m := NewMap[string, int]()
	if !m.IsEmpty() {
		t.Fatal("new map should be empty")
	}
	if m.Len() != 0 {
		t.Fatalf("Len should be 0, got %d", m.Len())
	}
}

func TestNewWithCapacity(t *testing.T) {
	m := New[string, int](100)
	if !m.IsEmpty() {
		t.Fatal("new map should be empty")
	}
	if m.Len() != 0 {
		t.Fatalf("Len should be 0, got %d", m.Len())
	}
	m.Set("key", 42)
	if v, _ := m.Get("key"); v != 42 {
		t.Fatalf("Get = %d, want 42", v)
	}
}

func TestFrom_FromRef(t *testing.T) {
	src := map[string]int{"x": 1, "y": 2}
	m := From(src)
	src["x"] = 999
	if v, _ := m.Get("x"); v != 1 {
		t.Fatal("From should deep copy")
	}

	m2 := FromRef(src)
	if v, _ := m2.Get("x"); v != 999 {
		t.Fatal("FromRef should reference")
	}
}

func TestFromNil(t *testing.T) {
	m := From[string, int](nil)
	if !m.IsEmpty() {
		t.Fatal("From(nil) should create empty map")
	}
	m2 := FromRef[string, int](nil)
	if !m2.IsEmpty() {
		t.Fatal("FromRef(nil) should create empty map")
	}
}

// ============================================================
// 二、Map 基础读写 CRUD 测试
// ============================================================

func TestSetGet(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	if m.Len() != 3 {
		t.Fatalf("Len should be 3, got %d", m.Len())
	}
	v, ok := m.Get("a")
	if !ok || v != 1 {
		t.Fatalf("Get(a) = (%d, %v), want (1, true)", v, ok)
	}
	v, ok = m.Get("z")
	if ok {
		t.Fatalf("Get(z) should return false, got (%d, %v)", v, ok)
	}
	if !m.Has("b") {
		t.Fatal("Has(b) should be true")
	}
	if m.Has("z") {
		t.Fatal("Has(z) should be false")
	}
}

func TestSetAll(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	m.SetAll(map[string]int{"b": 2, "c": 3, "a": 10})
	if m.Len() != 3 {
		t.Fatalf("Len = %d, want 3", m.Len())
	}
	if v, _ := m.Get("a"); v != 10 {
		t.Fatalf("a should be overridden to 10, got %d", v)
	}
}

func TestDelete(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Delete("a")
	if m.Len() != 1 {
		t.Fatalf("Len should be 1, got %d", m.Len())
	}
	if m.Has("a") {
		t.Fatal("a should be deleted")
	}
	if !m.Has("b") {
		t.Fatal("b should still exist")
	}
}

func TestDeleteMultiple(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	m.Delete("a", "b")
	if m.Len() != 1 {
		t.Fatalf("Len = %d, want 1", m.Len())
	}
	if !m.Has("c") {
		t.Fatal("c should still exist")
	}
}

func TestGetAndDelete(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	v, ok := m.GetAndDelete("b")
	if !ok || v != 2 {
		t.Fatalf("GetAndDelete(b) = (%d, %v), want (2, true)", v, ok)
	}
	if m.Len() != 1 {
		t.Fatalf("Len should be 1 after GetAndDelete, got %d", m.Len())
	}
	if m.Has("b") {
		t.Fatal("b should be gone")
	}
	_, ok = m.GetAndDelete("z")
	if ok {
		t.Fatal("GetAndDelete on missing key should return false")
	}
}

func TestGetOrDefault(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	if v := m.GetOrDefault("a", 99); v != 1 {
		t.Fatalf("GetOrDefault(a) = %d, want 1", v)
	}
	if v := m.GetOrDefault("z", 99); v != 99 {
		t.Fatalf("GetOrDefault(z) = %d, want 99", v)
	}
}

func TestGetOrSet(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	v, isNew := m.GetOrSet("a", 99)
	if v != 1 || isNew {
		t.Fatalf("GetOrSet(a) = (%d, %v), want (1, false)", v, isNew)
	}
	v, isNew = m.GetOrSet("b", 99)
	if v != 99 || !isNew {
		t.Fatalf("GetOrSet(b) = (%d, %v), want (99, true)", v, isNew)
	}
	if !m.Has("b") {
		t.Fatal("b should exist after GetOrSet")
	}
}

func TestClear(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Clear()
	if !m.IsEmpty() {
		t.Fatal("map should be empty after Clear")
	}
	if m.Len() != 0 {
		t.Fatalf("Len should be 0 after Clear, got %d", m.Len())
	}
}

// ============================================================
// 三、Map 遍历测试
// ============================================================

func TestRange(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	count := 0
	m.Range(func(k string, v int) bool {
		count++
		return true
	})
	if count != 3 {
		t.Fatalf("Range should visit 3 items, got %d", count)
	}
	count = 0
	m.Range(func(k string, v int) bool {
		count++
		return false
	})
	if count != 1 {
		t.Fatalf("Range with early stop should visit 1 item, got %d", count)
	}
}

func TestForEach(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	sum := 0
	m.ForEach(func(k string, v int) {
		sum += v
	})
	if sum != 3 {
		t.Fatalf("ForEach sum = %d, want 3", sum)
	}
}

func TestKeys_Values(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	keys := m.Keys()
	if len(keys) != 3 {
		t.Fatalf("Keys length = %d, want 3", len(keys))
	}
	vals := m.Values()
	if len(vals) != 3 {
		t.Fatalf("Values length = %d, want 3", len(vals))
	}
}

func TestClone(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	cloned := m.Clone()
	if len(cloned) != 2 {
		t.Fatalf("Clone length = %d, want 2", len(cloned))
	}
	cloned["a"] = 999
	if v, _ := m.Get("a"); v != 1 {
		t.Fatal("modifying clone should not affect original")
	}
}

func TestAsMap(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	raw := m.AsMap()
	raw["a"] = 999
	if v, _ := m.Get("a"); v != 999 {
		t.Fatal("modifying AsMap should affect original (reference)")
	}
}

// ============================================================
// 四、Map 过滤与变换测试
// ============================================================

func TestFilter(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3).Set("d", 4)
	m.Filter(func(k string, v int) bool { return v%2 == 0 })
	if m.Len() != 2 {
		t.Fatalf("after Filter even: Len = %d, want 2", m.Len())
	}
	if m.Has("a") || m.Has("c") {
		t.Fatal("odd values should be filtered out")
	}
}

func TestFilterKeys(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("ab", 1).Set("bc", 2).Set("cd", 3)
	m.FilterKeys(func(k string) bool { return len(k) == 2 })
	if m.Len() != 3 {
		t.Fatalf("Len = %d, want 3", m.Len())
	}
}

func TestFilterValues(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	m.FilterValues(func(v int) bool { return v > 1 })
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}
}

func TestReject(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	m.Reject(func(k string, v int) bool { return v > 1 })
	if m.Len() != 1 {
		t.Fatalf("after Reject v>1: Len = %d, want 1", m.Len())
	}
	if !m.Has("a") {
		t.Fatal("a should remain")
	}
}

func TestMapValues(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	m.MapValues(func(k string, v int) int { return v * 10 })
	v, _ := m.Get("a")
	if v != 10 {
		t.Fatalf("Get(a) = %d, want 10", v)
	}
	v, _ = m.Get("b")
	if v != 20 {
		t.Fatalf("Get(b) = %d, want 20", v)
	}
}

// ============================================================
// 五、Map 合并与集合运算
// ============================================================

func TestMerge(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	m.Merge(map[string]int{"b": 20, "c": 3})
	if m.Len() != 3 {
		t.Fatalf("Len = %d, want 3", m.Len())
	}
	if v, _ := m.Get("b"); v != 20 {
		t.Fatalf("b should be overridden to 20, got %d", v)
	}
	if v, _ := m.Get("c"); v != 3 {
		t.Fatalf("Get(c) = %d, want 3", v)
	}
}

func TestMergeMap(t *testing.T) {
	m1 := NewMap[string, int]()
	m1.Set("a", 1)
	m2 := NewMap[string, int]()
	m2.Set("a", 10).Set("b", 2)
	m1.MergeMap(m2)
	if m1.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m1.Len())
	}
	if v, _ := m1.Get("a"); v != 10 {
		t.Fatalf("a should be overridden to 10, got %d", v)
	}
}

func TestMergeMap_Nil(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	m.MergeMap(nil)
	if m.Len() != 1 {
		t.Fatalf("Len = %d, want 1", m.Len())
	}
}

func TestMergeWithDefault(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	m.MergeWithDefault(map[string]int{"a": 10, "b": 2})
	if v, _ := m.Get("a"); v != 1 {
		t.Fatalf("a should NOT be overridden, got %d", v)
	}
	if v, _ := m.Get("b"); v != 2 {
		t.Fatalf("Get(b) = %d, want 2", v)
	}
}

func TestIntersect(t *testing.T) {
	a := NewMap[string, int]()
	a.Set("x", 1).Set("y", 2).Set("z", 3)
	b := NewMap[string, int]()
	b.Set("y", 20).Set("z", 30).Set("w", 40)
	r := a.Intersect(b)
	if r.Len() != 2 {
		t.Fatalf("Intersect Len = %d, want 2", r.Len())
	}
	if v, _ := r.Get("y"); v != 2 {
		t.Fatalf("y should use a's value 2, got %d", v)
	}
	if r.Has("x") || r.Has("w") {
		t.Fatal("x and w should not be in intersection")
	}
}

func TestIntersect_Empty(t *testing.T) {
	a := NewMap[string, int]()
	b := NewMap[string, int]()
	b.Set("y", 1)
	r := a.Intersect(b)
	if r.Len() != 0 {
		t.Fatalf("Intersect of empty should be empty, got %d", r.Len())
	}
}

func TestDifference(t *testing.T) {
	a := NewMap[string, int]()
	a.Set("x", 1).Set("y", 2).Set("z", 3)
	b := NewMap[string, int]()
	b.Set("y", 20)
	r := a.Difference(b)
	if r.Len() != 2 {
		t.Fatalf("Difference Len = %d, want 2", r.Len())
	}
	if !r.Has("x") || !r.Has("z") {
		t.Fatal("x and z should be in difference")
	}
	if r.Has("y") {
		t.Fatal("y should not be in difference")
	}
}

func TestUnion(t *testing.T) {
	a := NewMap[string, int]()
	a.Set("x", 1).Set("y", 2)
	b := NewMap[string, int]()
	b.Set("y", 20).Set("z", 3)
	r := a.Union(b)
	if r.Len() != 3 {
		t.Fatalf("Union Len = %d, want 3", r.Len())
	}
	if v, _ := r.Get("y"); v != 20 {
		t.Fatalf("y should use b's value 20, got %d", v)
	}
}

// ============================================================
// 六、Map 条件查询测试
// ============================================================

func TestFind(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	k, v, found := m.Find(func(k string, v int) bool { return v > 2 })
	if !found {
		t.Fatal("should find v>2")
	}
	if !(k == "c" && v == 3) {
		t.Logf("Find(fn) = (%s, %d, %v)", k, v, found)
	}
	_, _, found = m.Find(func(k string, v int) bool { return v > 100 })
	if found {
		t.Fatal("should not find v>100")
	}
}

func TestAll(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	if !m.All(func(k string, v int) bool { return v > 0 }) {
		t.Fatal("all v>0 should be true")
	}
	if m.All(func(k string, v int) bool { return v > 1 }) {
		t.Fatal("all v>1 should be false")
	}
}

func TestAny(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	if !m.Any(func(k string, v int) bool { return v == 2 }) {
		t.Fatal("any v==2 should be true")
	}
	if m.Any(func(k string, v int) bool { return v > 100 }) {
		t.Fatal("any v>100 should be false")
	}
}

func TestCount(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	if n := m.Count(func(k string, v int) bool { return v > 1 }); n != 2 {
		t.Fatalf("Count(v>1) = %d, want 2", n)
	}
}

func TestEmptyMap_AllAny(t *testing.T) {
	m := NewMap[string, int]()
	if !m.All(func(k string, v int) bool { return false }) {
		t.Fatal("All on empty map should be true (vacuously)")
	}
	if m.Any(func(k string, v int) bool { return true }) {
		t.Fatal("Any on empty map should be false")
	}
	if m.Count(func(k string, v int) bool { return true }) != 0 {
		t.Fatal("Count on empty map should be 0")
	}
}

// ============================================================
// 七、Map 全局函数测试
// ============================================================

func TestRemapValues(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	m2 := RemapValues(m, func(k string, v int) string {
		return k + "!"
	})
	if v, _ := m.Get("a"); v != 1 {
		t.Fatal("original should be unchanged")
	}
	if v, _ := m2.Get("a"); v != "a!" {
		t.Fatalf("Get(a) = %s, want 'a!'", v)
	}
	if m2.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m2.Len())
	}
}

func TestRemapKeys(t *testing.T) {
	m := NewMap[int, string]()
	m.Set(1, "a").Set(2, "b")
	m2 := RemapKeys(m, func(k int, v string) string {
		if k == 1 {
			return "one"
		}
		return "two"
	})
	if v, _ := m2.Get("one"); v != "a" {
		t.Fatalf("Get(one) = %s, want 'a'", v)
	}
	if v, _ := m2.Get("two"); v != "b" {
		t.Fatalf("Get(two) = %s, want 'b'", v)
	}
}

func TestToSlice(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	sl := ToSlice(m, func(k string, v int) string {
		return k
	})
	if len(sl) != 3 {
		t.Fatalf("ToSlice len = %d, want 3", len(sl))
	}
}

func TestInvert(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	inv := Invert(m)
	if inv.Len() != 2 {
		t.Fatalf("Invert Len = %d, want 2", inv.Len())
	}
	if v, _ := inv.Get(1); v != "a" {
		t.Fatalf("Get(1) = %s, want 'a'", v)
	}
	if v, _ := inv.Get(2); v != "b" {
		t.Fatalf("Get(2) = %s, want 'b'", v)
	}
}

func TestChain(t *testing.T) {
	m := New[string, int](8)
	m.Set("a", 1).Set("b", 2).Set("c", 3).Set("d", 4)
	m.Filter(func(k string, v int) bool { return v%2 == 0 }).
		MapValues(func(k string, v int) int { return v * 10 })
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}
	if v, _ := m.Get("b"); v != 20 {
		t.Fatalf("Get(b) = %d, want 20", v)
	}
}

// ============================================================
// 八、MapChain Builder 测试（交叉验证：Map vs MapChain）
// ============================================================

func TestMapChain_New(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	if mc.Len() != 3 {
		t.Fatalf("Len = %d, want 3", mc.Len())
	}
	if mc.IsEmpty() {
		t.Fatal("should not be empty")
	}
}

func TestMapChain_NewWith(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	if mc.Len() != 2 {
		t.Fatalf("Len = %d, want 2", mc.Len())
	}
}

func TestMapChain_BasicOps(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{})
	mc.Set("a", 1).Set("b", 2)
	if v, ok := mc.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = (%d, %v), want (1, true)", v, ok)
	}
	if !mc.Has("a") {
		t.Fatal("Has(a) should be true")
	}
	if mc.Len() != 2 {
		t.Fatalf("Len = %d, want 2", mc.Len())
	}
	mc.Delete("a")
	if mc.Len() != 1 {
		t.Fatalf("Len after delete = %d, want 1", mc.Len())
	}
}

func TestMapChain_SetAll_Merge(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{})
	mc.SetAll(map[string]int{"a": 1, "b": 2, "c": 3})
	if mc.Len() != 3 {
		t.Fatalf("Len = %d, want 3", mc.Len())
	}
	mc.Merge(map[string]int{"c": 30, "d": 4})
	if v, _ := mc.Get("c"); v != 30 {
		t.Fatalf("Get(c) = %d, want 30", v)
	}
	if mc.Len() != 4 {
		t.Fatalf("Len after Merge = %d, want 4", mc.Len())
	}
}

func TestMapChain_MergeMap(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1})
	m2 := NewMap[string, int]()
	m2.Set("a", 10).Set("b", 20)
	mc.MergeMap(m2)
	if v, _ := mc.Get("a"); v != 10 {
		t.Fatalf("Get(a) = %d, want 10", v)
	}
	if mc.Len() != 2 {
		t.Fatalf("Len = %d, want 2", mc.Len())
	}
	mc.MergeMap(nil)
	if mc.Len() != 2 {
		t.Fatalf("MergeMap(nil) should not change length, got %d", mc.Len())
	}
}

func TestMapChain_MergeWithDefault(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1})
	mc.MergeWithDefault(map[string]int{"a": 10, "b": 2})
	if v, _ := mc.Get("a"); v != 1 {
		t.Fatalf("a should NOT be overridden, got %d", v)
	}
	if v, _ := mc.Get("b"); v != 2 {
		t.Fatalf("Get(b) = %d, want 2", v)
	}
}

func TestMapChain_Filter_Reject_MapValues(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	mc.FilterKV(func(k string, v int) bool { return v%2 == 0 })
	if mc.Len() != 2 {
		t.Fatalf("Len after FilterKV = %d, want 2", mc.Len())
	}
	mc.MapValues(func(k string, v int) int { return v * 10 })
	if v, _ := mc.Get("b"); v != 20 {
		t.Fatalf("Get(b) = %d, want 20", v)
	}
	if v, _ := mc.Get("d"); v != 40 {
		t.Fatalf("Get(d) = %d, want 40", v)
	}
}

func TestMapChain_RejectKV(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	mc.Reject(func(k string, v int) bool { return v > 1 })
	if mc.Len() != 1 {
		t.Fatalf("Len after Reject = %d, want 1", mc.Len())
	}
	if !mc.Has("a") {
		t.Fatal("a should remain")
	}
}

func TestMapChain_Clear(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1, "b": 2})
	mc.Clear()
	if !mc.IsEmpty() {
		t.Fatal("should be empty after Clear")
	}
}

func TestMapChain_Range_Find_All_Any_Count(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	count := 0
	mc.Range(func(k string, v int) bool {
		count++
		return true
	})
	if count != 3 {
		t.Fatalf("Range count = %d, want 3", count)
	}
	if !mc.All(func(k string, v int) bool { return v > 0 }) {
		t.Fatal("All v>0 should be true")
	}
	if !mc.Any(func(k string, v int) bool { return v == 2 }) {
		t.Fatal("Any v==2 should be true")
	}
	if mc.Count(func(k string, v int) bool { return v > 1 }) != 2 {
		t.Fatal("Count v>1 should be 2")
	}
	k, v, found := mc.Find(func(k string, v int) bool { return v == 3 })
	if !found {
		t.Fatal("Find v==3 should succeed")
	}
	if k != "c" || v != 3 {
		t.Fatalf("Find result = (%s, %d), want (c, 3)", k, v)
	}
}

func TestMapChain_Keys_Values_AsMap(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChain[string, int](ctx, data)
	keys := mc.Keys()
	if len(keys) != 2 {
		t.Fatalf("Keys len = %d, want 2", len(keys))
	}
	vals := mc.Values()
	if len(vals) != 2 {
		t.Fatalf("Values len = %d, want 2", len(vals))
	}
	raw := mc.AsMap()
	raw["a"] = 999
	if v, _ := mc.Get("a"); v != 999 {
		t.Fatal("AsMap modification should affect MapChain")
	}
}

// ============================================================
// 九、MapChain 终端操作测试（Map/ForEach/Filter/Stream/Reduce/MapBatch/ForEachBatch）
// ============================================================

func TestMapChain_Map(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChainWith[string, int, string](ctx, data)
	result := mc.Worker(4).Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Err() {
		t.Fatalf("unexpected error: %v", result.Error())
	}
	if result.Len() != 3 {
		t.Fatalf("Len = %d, want 3", result.Len())
	}
	if v := result.Data()["a"]; v != "a!" {
		t.Fatalf("Data[a] = %s, want 'a!'", v)
	}
}

func TestMapChain_Map_Serial(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChainWith[string, int, string](ctx, data)
	result := mc.Serial().Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "_" + string(rune('0'+v)), nil
	})
	if result.Len() != 3 {
		t.Fatalf("Serial Map Len = %d, want 3", result.Len())
	}
	if v := result.Data()["a"]; v != "a_1" {
		t.Fatalf("Data[a] = %s, want 'a_1'", v)
	}
}

func TestMapChain_Map_Error(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChainWith[string, int, string](ctx, data)
	bomb := errors.New("map error")
	result := mc.Worker(2).Map(func(ctx context.Context, k string, v int) (string, error) {
		if v == 2 {
			return "", bomb
		}
		return k, nil
	})
	if !result.Err() {
		t.Fatal("expected error in result")
	}
	if result.Data()["a"] != "a" {
		t.Fatalf("Data[a] = %s, want 'a'", result.Data()["a"])
	}
}

func TestMapChain_ForEach(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	var sum atomic.Int64
	result := mc.Worker(4).ForEach(func(ctx context.Context, k string, v int) error {
		sum.Add(int64(v))
		return nil
	})
	if result.Err() {
		t.Fatalf("unexpected error: %v", result.Error())
	}
	if result.Total() != 4 {
		t.Fatalf("Total = %d, want 4", result.Total())
	}
	if result.FailCount() != 0 {
		t.Fatalf("FailCount = %d, want 0", result.FailCount())
	}
	if sum.Load() != 10 {
		t.Fatalf("sum = %d, want 10", sum.Load())
	}
}

func TestMapChain_ForEach_Error(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	bomb := errors.New("each error")
	result := mc.Worker(2).ForEach(func(ctx context.Context, k string, v int) error {
		if v == 2 {
			return bomb
		}
		return nil
	})
	if !result.Err() {
		t.Fatal("expected error")
	}
	if result.FailCount() == 0 {
		t.Fatal("expected fail count > 0")
	}
}

func TestMapChain_ForEach_FailFast(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	result := mc.FailFast().Worker(2).ForEach(func(ctx context.Context, k string, v int) error {
		return errors.New("fail")
	})
	if !result.Err() {
		t.Fatal("expected error with FailFast")
	}
}

func TestMapChain_Filter(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	result, err := mc.Filter(func(ctx context.Context, k string, v int) bool {
		return v%2 == 0
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapChain_Stream(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChainWith[string, int, string](ctx, data)
	ch := mc.Worker(4).Stream(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	}, 0)
	count := 0
	for r := range ch {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("stream count = %d, want 3", count)
	}
}

func TestMapChain_Reduce(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	result, err := mc.Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
		return acc + v, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 6 {
		t.Fatalf("Reduce sum = %d, want 6", result)
	}
}

func TestMapChain_MapBatch(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChainWith[string, int, string](ctx, data)
	result := mc.Chunk(2).Worker(4).MapBatch(func(ctx context.Context, entries []MapEntry[string, int]) (string, error) {
		return entries[0].Key, nil
	})
	if result.Len() < 1 {
		t.Fatal("expected at least 1 batch result")
	}
}

func TestMapChain_ForEachBatch(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	var count atomic.Int64
	result := mc.Chunk(2).Worker(2).ForEachBatch(func(ctx context.Context, entries []MapEntry[string, int]) error {
		count.Add(int64(len(entries)))
		return nil
	})
	if result.Err() {
		t.Fatalf("unexpected error: %v", result.Error())
	}
	if count.Load() != 4 {
		t.Fatalf("count = %d, want 4", count.Load())
	}
}

// ============================================================
// 十、MapParallel Builder 测试
// ============================================================

func TestMapParallel_Basic(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	mp := mc.Parallel()
	result := mp.Worker(4).Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapParallel_ToSerial(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	ms := mc.Parallel().ToSerial()
	result := ms.Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "_serial", nil
	})
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
	if v := result.Data()["a"]; v != "a_serial" {
		t.Fatalf("Data[a] = %s, want 'a_serial'", v)
	}
}

func TestMapParallel_AllConfigMethods(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	mp := mc.Parallel().Worker(8).DefaultWorker().FailFast().NoFailFast().
		DefaultFailFast().Timeout(30 * time.Second).DefaultTimeout().
		Shards(4).DefaultShard().Buf(256).DefaultBuf().
		Chunk(100).DefaultChunk()
	if mp == nil {
		t.Fatal("MapParallel is nil after chain")
	}
	result := mp.ForEach(func(ctx context.Context, k string, v int) error { return nil })
	if result.Total() != 3 {
		t.Fatalf("Total = %d, want 3", result.Total())
	}
}

func TestMapParallel_Filter_Stream(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	result, err := mc.Parallel().Filter(func(ctx context.Context, k string, v int) bool {
		return v%2 == 0
	})
	if err != nil {
		t.Fatalf("Filter error: %v", err)
	}
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapParallel_MapToFn(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	result := mc.Parallel().MapToFn(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Err() {
		t.Fatalf("unexpected error: %v", result.Error())
	}
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

// ============================================================
// 十一、MapSerial Builder 测试
// ============================================================

func TestMapSerial_Basic(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	ms := mc.Serial()
	result := ms.Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapSerial_ToParallel(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	mp := mc.Serial().ToParallel()
	result := mp.Worker(2).Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapSerial_FailFast_Timeout(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	ms := mc.Serial().FailFast().NoFailFast().Timeout(10 * time.Second).DefaultTimeout()
	if ms == nil {
		t.Fatal("MapSerial is nil after chain")
	}
	result := ms.Map(func(ctx context.Context, k string, v int) (string, error) {
		return k, nil
	})
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapSerial_MapToFn_Stream_Reduce(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChainWith[string, int, string](ctx, data)
	result := mc.Serial().MapToFn(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Len() != 3 {
		t.Fatalf("Len = %d, want 3", result.Len())
	}

	ch := mc.Serial().Stream(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	}, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 3 {
		t.Fatalf("Stream count = %d, want 3", count)
	}
}

func TestMapSerial_Reduce(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	sum, err := mc.Serial().Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
		return acc + v, nil
	})
	if err != nil {
		t.Fatalf("Reduce error: %v", err)
	}
	if sum != 6 {
		t.Fatalf("Reduce sum = %d, want 6", sum)
	}
}

// ============================================================
// 十二、MapResult 测试
// ============================================================

func TestMapResult_Basic(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2}
	r := NewMapResult(data, nil)
	if r.Err() {
		t.Fatal("should not have error")
	}
	if r.Error() != nil {
		t.Fatal("Error should be nil")
	}
	if r.Len() != 2 {
		t.Fatalf("Len = %d, want 2", r.Len())
	}
	if d := r.Data(); d["a"] != 1 {
		t.Fatalf("Data[a] = %d, want 1", d["a"])
	}
	keys := r.Keys()
	if len(keys) != 2 {
		t.Fatalf("Keys len = %d, want 2", len(keys))
	}
	vals := r.Values()
	if len(vals) != 2 {
		t.Fatalf("Values len = %d, want 2", len(vals))
	}
}

func TestMapResult_Error(t *testing.T) {
	bomb := errors.New("test error")
	r := NewMapResult(map[string]int{}, bomb)
	if !r.Err() {
		t.Fatal("should have error")
	}
	if r.Error() != bomb {
		t.Fatalf("Error = %v, want %v", r.Error(), bomb)
	}
}

func TestMapResult_NilData(t *testing.T) {
	r := NewMapResult[string, int](nil, nil)
	if r.Err() {
		t.Fatal("should not have error")
	}
	if r.Data() == nil {
		t.Fatal("Data should not be nil")
	}
	if r.Len() != 0 {
		t.Fatalf("Len = %d, want 0", r.Len())
	}
}

// ============================================================
// 十三、MapForEachResult 测试
// ============================================================

func TestMapForEachResult(t *testing.T) {
	r := &MapForEachResult{total: 100, failCnt: 3, firstErr: nil}
	if r.Total() != 100 {
		t.Fatalf("Total = %d, want 100", r.Total())
	}
	if r.FailCount() != 3 {
		t.Fatalf("FailCount = %d, want 3", r.FailCount())
	}
	if r.SuccessCount() != 97 {
		t.Fatalf("SuccessCount = %d, want 97", r.SuccessCount())
	}
	if r.Err() {
		t.Fatal("Err should be false when firstErr is nil")
	}
	if r.Error() != nil {
		t.Fatal("Error should be nil")
	}
}

func TestMapForEachResult_WithError(t *testing.T) {
	bomb := errors.New("test")
	r := &MapForEachResult{total: 50, failCnt: 1, firstErr: bomb}
	if !r.Err() {
		t.Fatal("Err should be true")
	}
	if r.Error() != bomb {
		t.Fatalf("Error = %v, want %v", r.Error(), bomb)
	}
}

// ============================================================
// 十四、MapChain 配置方法测试
// ============================================================

func TestMapChain_ConfigMethods(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1})
	mc.Worker(8).DefaultWorker().FailFast().DefaultFailFast().
		Timeout(10 * time.Second).DefaultTimeout().
		Shards(4).DefaultShard().Buf(256).DefaultBuf().
		Chunk(100).DefaultChunk()
	if mc == nil {
		t.Fatal("MapChain is nil after config chain")
	}
}

func TestMapChain_Logger(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1})
	mc.Logger(nil).DefaultLogger()
}

func TestMapChain_Context(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1})
	mc.Context(context.Background())
}

// ============================================================
// 十五、并发交叉验证：Map vs MapChain vs MapParallel vs MapSerial
// ============================================================

func TestMaps_Concurrent_ReadWrite(t *testing.T) {
	m := NewMap[int, int]()
	n := 1000
	for i := 0; i < n; i++ {
		m.Set(i, i)
	}
	if m.Len() != n {
		t.Fatalf("Len after Set = %d, want %d", m.Len(), n)
	}
	for i := 0; i < n; i++ {
		v, ok := m.Get(i)
		if !ok || v != i {
			t.Fatalf("Get(%d) = (%d, %v), want (%d, true)", i, v, ok, i)
		}
	}
}

func TestMaps_Concurrent_GetSet_Hybrid(t *testing.T) {
	m := NewMap[string, int]()
	n := 500
	for i := 0; i < n; i++ {
		m.Set(string(rune('a'+i%26)), i)
	}
	if m.Len() != 26 {
		t.Fatalf("Len = %d, want 26 (unique keys)", m.Len())
	}
	v, ok := m.Get("z")
	_ = v
	_ = ok
}

func TestMaps_Concurrent_MapVsMapChain(t *testing.T) {
	ctx := context.Background()

	m := NewMap[string, int]()
	for i := 0; i < 500; i++ {
		key := string(rune('a' + i%26))
		m.Set(key, m.GetOrDefault(key, 0)+1)
	}

	data := map[string]int{}
	for i := 0; i < 500; i++ {
		key := string(rune('a' + i%26))
		data[key] = data[key] + 1
	}

	mc := NewMapChain[string, int](ctx, data)
	result := mc.ForEach(func(ctx context.Context, k string, v int) error {
		return nil
	})
	if result.Err() {
		t.Fatalf("MapChain error: %v", result.Error())
	}

	mapSum := 0
	m.ForEach(func(k string, v int) {
		mapSum += v
	})
	if mapSum != 500 {
		t.Fatalf("cross-validation failed: Map sum=%d, want 500", mapSum)
	}
}

// ============================================================
// 十六、四档并发压力测试
// ============================================================

func TestMapChain_Map_Concurrent_Tiers(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			data := make(map[int]int, tier.Size)
			for i := 0; i < tier.Size; i++ {
				data[i] = i
			}
			mc := NewMapChain[int, int](ctx, data)
			result := mc.Worker(100).Map(func(ctx context.Context, k int, v int) (int, error) {
				return v * 2, nil
			})
			if result.Err() {
				t.Fatalf("unexpected error: %v", result.Error())
			}
			if result.Len() != tier.Size {
				t.Fatalf("Len = %d, want %d", result.Len(), tier.Size)
			}
			resData := result.Data()
			for i := 0; i < tier.Size; i++ {
				if resData[i] != i*2 {
					t.Fatalf("Data[%d] = %d, want %d", i, resData[i], i*2)
				}
			}
		})
	}
}

func TestMapChain_ForEach_Concurrent_Tiers(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			data := make(map[int]int, tier.Size)
			for i := 0; i < tier.Size; i++ {
				data[i] = i
			}
			mc := NewMapChain[int, int](ctx, data)
			var sum atomic.Int64
			result := mc.Worker(100).ForEach(func(ctx context.Context, k int, v int) error {
				sum.Add(int64(v))
				return nil
			})
			if result.Err() {
				t.Fatalf("unexpected error: %v", result.Error())
			}
			if result.Total() != int64(tier.Size) {
				t.Fatalf("Total = %d, want %d", result.Total(), tier.Size)
			}
			if result.FailCount() != 0 {
				t.Fatalf("FailCount = %d, want 0", result.FailCount())
			}
			expectedSum := int64(tier.Size) * int64(tier.Size-1) / 2
			if sum.Load() != expectedSum {
				t.Fatalf("sum = %d, want %d", sum.Load(), expectedSum)
			}
		})
	}
}

func TestMapChain_Stream_Concurrent_Tiers(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			data := make(map[int]int, tier.Size)
			for i := 0; i < tier.Size; i++ {
				data[i] = i
			}
			mc := NewMapChain[int, int](ctx, data)
			ch := mc.Worker(64).Stream(func(ctx context.Context, k int, v int) (int, error) {
				return v * 2, nil
			}, 4096)
			var consumed atomic.Int64
			for r := range ch {
				consumed.Add(1)
				_ = r
			}
			if consumed.Load() != int64(tier.Size) {
				t.Fatalf("consumed = %d, want %d", consumed.Load(), tier.Size)
			}
		})
	}
}

// ============================================================
// 十七、Race 竞态测试
// ============================================================

func TestMaps_Race_ConcurrentSetGet(t *testing.T) {
	for round := 0; round < 30; round++ {
		m := NewMap[int, int]()
		n := 500
		for i := 0; i < n; i++ {
			m.Set(i, i)
			m.Get(i)
		}
		if m.Len() != n {
			t.Fatalf("round %d: Len = %d, want %d", round, m.Len(), n)
		}
	}
}

func TestMaps_Race_ConcurrentDelete(t *testing.T) {
	for round := 0; round < 20; round++ {
		m := NewMap[int, int]()
		for i := 0; i < 200; i++ {
			m.Set(i, i)
		}
		for i := 0; i < 200; i++ {
			m.Set(i, i*2)
			m.Delete(i)
		}
		if m.Len() != 0 {
			t.Fatalf("round %d: Len = %d, want 0", round, m.Len())
		}
	}
}

func TestMaps_Race_ConcurrentFilterClear(t *testing.T) {
	for round := 0; round < 10; round++ {
		m := NewMap[int, int]()
		for i := 0; i < 100; i++ {
			m.Set(i, i)
		}
		m.Filter(func(k, v int) bool { return v%2 == 0 })
		if m.Len() != 50 {
			t.Fatalf("round %d: Len after Filter = %d, want 50", round, m.Len())
		}
		m.Clear()
		if m.Len() != 0 {
			t.Fatalf("round %d: Len after Clear = %d, want 0", round, m.Len())
		}
	}
}

func TestMaps_Race_MapChain_Concurrent(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		var wg sync.WaitGroup
		var success atomic.Int64

		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				data := map[string]int{"a": 1, "b": 2, "c": 3}
				mc := NewMapChainWith[string, int, string](ctx, data)
				result := mc.Worker(2).Map(func(ctx context.Context, k string, v int) (string, error) {
					return k + "!", nil
				})
				if !result.Err() && result.Len() == 3 {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 10 {
			t.Fatalf("round %d: success = %d, want 10", round, success.Load())
		}
	}
}

func TestMaps_Race_MapChain_ForEach_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		data := make(map[int]int, 500)
		for i := 0; i < 500; i++ {
			data[i] = i
		}
		var wg sync.WaitGroup
		var totalSum atomic.Int64

		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				mc := NewMapChain[int, int](ctx, data)
				var sum atomic.Int64
				mc.Worker(4).ForEach(func(ctx context.Context, k int, v int) error {
					sum.Add(int64(v))
					return nil
				})
				totalSum.Add(sum.Load())
			}()
		}
		wg.Wait()
	}
}

func TestMaps_Race_NewMapResult_Concurrent(t *testing.T) {
	for round := 0; round < 50; round++ {
		var wg sync.WaitGroup
		for g := 0; g < 20; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := NewMapResult(map[string]int{"x": 1}, nil)
				_ = r.Data()
				_ = r.Keys()
				_ = r.Values()
			}()
		}
		wg.Wait()
	}
}
