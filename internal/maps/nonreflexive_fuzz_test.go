package maps

import (
	"fmt"
	"math"
	"slices"
	"testing"
)

// Unique integer values label otherwise unfindable entries. The oracle is an
// ordered list of values, independent of map lookup and mutation mechanics.
func FuzzNonReflexiveMapTransforms(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 1, 1}, uint8(0))
	f.Add([]byte{}, uint8(1))
	f.Add([]byte{1, 1, 1, 1, 1, 1}, uint8(2))
	f.Fuzz(func(t *testing.T, input []byte, selector uint8) {
		if len(input) > 64 {
			input = input[:64]
		}
		for operation := range 5 {
			raw := make(map[any]int)
			keys := make(map[int]string)
			var want []int
			for i, kind := range input {
				nan := math.Float64frombits(0x7ff8000000000001 + uint64(kind%3))
				var key any
				switch kind % 6 {
				case 0:
					key = i
				case 1:
					key = nan
				case 2:
					key = [2]float64{nan, 1}
				case 3:
					key = struct{ F float64 }{nan}
				case 4:
					key = complex(nan, 1)
				case 5:
					key = [1]any{nan}
				}
				value := i + 1
				raw[key] = value
				keys[value] = nonReflexiveKeySignature(key)
				if operation == 0 {
					want = append(want, value+100)
				} else if (value+int(selector))%3 == 0 {
					want = append(want, value)
				}
			}
			m := FromRef(raw)
			calls := 0
			keep := func(v int) bool { calls++; return (v+int(selector))%3 == 0 }
			switch operation {
			case 0:
				m.MapValues(func(_ any, v int) int {
					calls++
					if calls > len(input) {
						t.Fatal("callback revisited inserted entry")
					}
					return v + 100
				})
			case 1:
				m.Filter(func(_ any, v int) bool { return keep(v) })
			case 2:
				m.FilterValues(keep)
			case 3:
				m.Reject(func(_ any, v int) bool { return !keep(v) })
			case 4:
				// All non-reflexive entries must disappear through key-only filtering.
				want = nil
				for i, kind := range input {
					if kind%6 == 0 {
						want = append(want, i+1)
					}
				}
				m.FilterKeys(func(k any) bool { calls++; return k == k })
			}
			var got []int
			for key, value := range raw {
				id := value
				if operation == 0 {
					id -= 100
				}
				if nonReflexiveKeySignature(key) != keys[id] {
					t.Fatalf("changed key for value=%d: %v", value, key)
				}
				got = append(got, value)
			}
			slices.Sort(got)
			if calls != len(input) || !slices.Equal(got, want) {
				t.Fatalf("operation=%d calls=%d got=%v want=%v", operation, calls, got, want)
			}
			m.Clear()
			if len(raw) != 0 {
				t.Fatal("mutation lost shared map identity")
			}
		}
	})
}

func nonReflexiveKeySignature(key any) string {
	switch key := key.(type) {
	case float64:
		return fmt.Sprintf("f:%x", math.Float64bits(key))
	case complex128:
		return fmt.Sprintf("c:%x:%x", math.Float64bits(real(key)), math.Float64bits(imag(key)))
	case [2]float64:
		return fmt.Sprintf("a:%x:%x", math.Float64bits(key[0]), math.Float64bits(key[1]))
	case struct{ F float64 }:
		return fmt.Sprintf("s:%x", math.Float64bits(key.F))
	case [1]any:
		return "i:" + nonReflexiveKeySignature(key[0])
	default:
		return fmt.Sprintf("%T:%v", key, key)
	}
}
