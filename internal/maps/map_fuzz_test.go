package maps

import (
	"slices"
	"testing"
)

// The oracle uses fixed slots rather than Go map mutation or library helpers.
// This checks sequences, including overwrites and empty-state transitions.
func FuzzMapContainerModel(f *testing.F) {
	f.Add([]byte{0, 1, 2, 0, 1, 3, 4, 2, 4, 6, 1, 1, 10, 2, 7})
	f.Add([]byte{0, 0, 0, 0, 1, 255, 0, 15, 127, 12, 3, 8, 13, 4, 9})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 3072 {
			input = input[:3072]
		}
		var present [16]bool
		var values [16]int
		m := NewMap[int, int]()
		check := func() {
			t.Helper()
			count := 0
			var wantKeys, wantValues []int
			for k := range present {
				want := 0
				if present[k] {
					count++
					want = values[k]
					wantKeys = append(wantKeys, k)
					wantValues = append(wantValues, want)
				}
				if got, ok := m.Get(k); ok != present[k] || got != want || m.Has(k) != present[k] {
					t.Fatalf("key %d: got (%d,%v), want (%d,%v)", k, got, ok, want, present[k])
				}
				if !present[k] {
					want = -999
				}
				if m.GetOrDefault(k, -999) != want {
					t.Fatalf("default lookup differs for %d", k)
				}
			}
			if m.Len() != count || m.IsEmpty() != (count == 0) {
				t.Fatalf("size: got %d, want %d", m.Len(), count)
			}
			keys, vals := m.Keys(), m.Values()
			slices.Sort(keys)
			slices.Sort(vals)
			slices.Sort(wantValues)
			if !slices.Equal(keys, wantKeys) || !slices.Equal(vals, wantValues) {
				t.Fatal("key/value enumeration differs from slots")
			}
		}
		for i := 0; i+2 < len(input); i += 3 {
			k, v := int(input[i+1]%16), int(int8(input[i+2]))
			switch input[i] % 14 {
			case 0:
				m.Set(k, v)
				present[k], values[k] = true, v
			case 1:
				m.Delete(k, (k+1)%16)
				present[k], present[(k+1)%16] = false, false
			case 2:
				got, ok := m.GetAndDelete(k)
				want := 0
				if present[k] {
					want = values[k]
				}
				if ok != present[k] || got != want {
					t.Fatal("GetAndDelete differs")
				}
				present[k] = false
			case 3:
				got, inserted := m.GetOrSet(k, v)
				want := v
				if present[k] {
					want = values[k]
				}
				if inserted == present[k] || got != want {
					t.Fatal("GetOrSet differs")
				}
				present[k], values[k] = true, want
			case 4:
				m.Clear()
				clear(present[:])
			case 5:
				m.FilterKeys(func(key int) bool { return key%3 != k%3 })
				for key := range present {
					if key%3 == k%3 {
						present[key] = false
					}
				}
			case 6:
				m.FilterValues(func(value int) bool { return value >= v })
				for key := range present {
					if values[key] < v {
						present[key] = false
					}
				}
			case 7:
				m.Reject(func(key, value int) bool { return key == k || value == v })
				for key := range present {
					if key == k || values[key] == v {
						present[key] = false
					}
				}
			case 8:
				m.MapValues(func(key, value int) int { return (value + key + v) % 256 })
				for key := range present {
					if present[key] {
						values[key] = (values[key] + key + v) % 256
					}
				}
			case 9, 10:
				other := map[int]int{k: v, (k + 1) % 16: -v}
				if input[i]%14 == 9 {
					m.SetAll(other)
				} else {
					m.Merge(other)
				}
				present[k], values[k] = true, v
				present[(k+1)%16], values[(k+1)%16] = true, -v
			case 11:
				m.MergeWithDefault(map[int]int{k: v})
				if !present[k] {
					present[k], values[k] = true, v
				}
			case 12:
				cloned := m.Clone()
				cloned[k] = 999
				delete(cloned, (k+1)%16)
			case 13:
				other := From(map[int]int{k: v})
				inter, diff, union := m.Intersect(other), m.Difference(other), m.Union(other)
				for key := range present {
					if inter.Has(key) != (present[key] && key == k) || diff.Has(key) != (present[key] && key != k) || union.Has(key) != (present[key] || key == k) {
						t.Fatal("set membership differs")
					}
					if present[key] && key == k {
						if got, _ := inter.Get(key); got != values[key] {
							t.Fatal("intersection must retain receiver value")
						}
					}
				}
				if got, _ := union.Get(k); got != v {
					t.Fatal("union must prefer other value")
				}
			}
			check()
		}
		check()
	})
}
