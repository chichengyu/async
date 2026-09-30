package maps

import "testing"

func TestNewMap_Basic(t *testing.T) {
	m := NewMap[string, int]()
	if !m.IsEmpty() {
		t.Fatal("new map should be empty")
	}
	if m.Len() != 0 {
		t.Fatalf("Len should be 0, got %d", m.Len())
	}
}

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

	v, ok := m.GetAndDelete("b")
	if !ok || v != 2 {
		t.Fatalf("GetAndDelete(b) = (%d, %v), want (2, true)", v, ok)
	}
	if m.Len() != 0 {
		t.Fatalf("Len should be 0 after GetAndDelete, got %d", m.Len())
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
}

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

func TestKeys(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)

	keys := m.Keys()
	if len(keys) != 3 {
		t.Fatalf("Keys length = %d, want 3", len(keys))
	}
}

func TestValues(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)

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

func TestFind(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)

	k, v, found := m.Find(func(k string, v int) bool { return v > 2 })
	if !found {
		t.Fatal("should find v>2")
	}
	if k != "c" && v != 3 {
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

func TestFrom(t *testing.T) {
	src := map[string]int{"x": 1, "y": 2}
	m := From(src)

	src["x"] = 999
	if v, _ := m.Get("x"); v != 1 {
		t.Fatal("From should deep copy, modifying src should not affect Map")
	}
}

func TestFromRef(t *testing.T) {
	src := map[string]int{"x": 1, "y": 2}
	m := FromRef(src)

	src["x"] = 999
	if v, _ := m.Get("x"); v != 999 {
		t.Fatal("FromRef should reference, modifying src should affect Map")
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

func TestRemapValues(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)

	m2 := RemapValues(m, func(k string, v int) string {
		return k + "!" // no int-to-string conversion needed
	})

	// Original unchanged
	if v, _ := m.Get("a"); v != 1 {
		t.Fatal("original should be unchanged")
	}

	// New map has transformed values
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

func TestMergeMap_Nil(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	m.MergeMap(nil) // should not panic

	if m.Len() != 1 {
		t.Fatalf("Len = %d, want 1", m.Len())
	}
}
