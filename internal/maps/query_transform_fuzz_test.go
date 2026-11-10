package maps

import (
	"context"
	"slices"
	"testing"
)

type mapQueries interface {
	All(func(int, int) bool) bool
	Any(func(int, int) bool) bool
	Count(func(int, int) bool) int
	Find(func(int, int) bool) (int, int, bool)
}

// Fixed input slots are independent of the map's unspecified iteration order.
// Collision representatives need only belong to their source equivalence class.
func FuzzMapQueryTransforms(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{1, 1, 2, 2, 255, 0}, uint8(2))
	f.Add([]byte{0, 0, 0, 0}, uint8(0))
	f.Add([]byte{128, 129, 250, 255, 16, 32}, uint8(255))
	f.Fuzz(func(t *testing.T, input []byte, config uint8) {
		if len(input) > 64 {
			input = input[:64]
		}
		data := make(map[int]int, len(input))
		for k, b := range input {
			data[k] = int(int8(b))
		}
		m := From(data)
		matches := func(k, v int) bool { return (k+v)&3 == int(config&3) }
		wantCount := 0
		for k, b := range input {
			if matches(k, int(int8(b))) {
				wantCount++
			}
		}
		chain := NewMapChain[int, int](context.Background(), data)
		queries := []mapQueries{m, chain, chain.Serial(), chain.Parallel()}
		for mode, query := range queries {
			for _, method := range []string{"all", "any", "count", "find"} {
				seen := make([]bool, len(input))
				calls := 0
				terminal := false
				fn := func(k, v int) bool {
					if k < 0 || k >= len(input) || seen[k] || v != int(int8(input[k])) || terminal {
						t.Fatalf("mode %d %s bad visit %d=%d afterTerminal=%v", mode, method, k, v, terminal)
					}
					seen[k] = true
					calls++
					match := matches(k, v)
					terminal = (method == "all" && !match) || ((method == "any" || method == "find") && match)
					return match
				}
				switch method {
				case "all":
					if got := query.All(fn); got != (wantCount == len(input)) {
						t.Fatalf("mode %d all=%v count=%d", mode, got, wantCount)
					}
				case "any":
					if got := query.Any(fn); got != (wantCount > 0) {
						t.Fatalf("mode %d any=%v count=%d", mode, got, wantCount)
					}
				case "count":
					if got := query.Count(fn); got != wantCount {
						t.Fatalf("mode %d count=%d want %d", mode, got, wantCount)
					}
				case "find":
					k, v, ok := query.Find(fn)
					if ok != (wantCount > 0) || (ok && (k < 0 || k >= len(input) || v != int(int8(input[k])) || !matches(k, v))) || (!ok && (k != 0 || v != 0)) {
						t.Fatalf("mode %d find=%d,%d,%v", mode, k, v, ok)
					}
				}
				if (!terminal && calls != len(input)) || (terminal && calls == 0) {
					t.Fatalf("mode %d %s calls=%d terminal=%v", mode, method, calls, terminal)
				}
			}
		}
		ranges := []func(func(int, int) bool){
			func(fn func(int, int) bool) { m.Range(fn) },
			func(fn func(int, int) bool) { chain.Range(fn) },
			func(fn func(int, int) bool) { chain.Serial().Range(fn) },
			func(fn func(int, int) bool) { chain.Parallel().Range(fn) },
		}
		stop := int(config%5) + 1
		for mode, visit := range ranges {
			seen := make([]bool, len(input))
			calls := 0
			visit(func(k, v int) bool {
				if k < 0 || k >= len(input) || seen[k] || v != int(int8(input[k])) || calls >= stop {
					t.Fatalf("mode %d range invalid/late visit %d=%d", mode, k, v)
				}
				seen[k] = true
				calls++
				return calls < stop
			})
			if calls != min(stop, len(input)) {
				t.Fatalf("mode %d range calls=%d", mode, calls)
			}
		}
		buckets := int(config%8) + 1
		mapped := RemapKeys(m, func(k, v int) int { return k % buckets })
		if mapped.Len() != min(buckets, len(input)) {
			t.Fatalf("remap keys length=%d", mapped.Len())
		}
		for bucket := 0; bucket < min(buckets, len(input)); bucket++ {
			v, ok := mapped.Get(bucket)
			candidate := false
			for k := bucket; k < len(input); k += buckets {
				candidate = candidate || v == int(int8(input[k]))
			}
			if !ok || !candidate {
				t.Fatalf("remap keys bucket %d invalid representative %d,%v", bucket, v, ok)
			}
		}
		inverted := Invert(m)
		var valuePresent [256]bool
		distinct := 0
		for _, b := range input {
			if !valuePresent[b] {
				valuePresent[b] = true
				distinct++
			}
			k, ok := inverted.Get(int(int8(b)))
			if !ok || k < 0 || k >= len(input) || input[k] != b {
				t.Fatalf("invert value %d representative %d,%v", b, k, ok)
			}
		}
		if inverted.Len() != distinct {
			t.Fatalf("invert length=%d want %d", inverted.Len(), distinct)
		}
		type pair struct{ key, value int }
		converted := ToSlice(m, func(k, v int) pair { return pair{k, v} })
		slices.SortFunc(converted, func(a, b pair) int { return a.key - b.key })
		values := RemapValues(m, func(k, v int) pair { return pair{k, 3*v + k} })
		if len(converted) != len(input) || values.Len() != len(input) {
			t.Fatal("conversion lost entries")
		}
		for k, b := range input {
			if converted[k] != (pair{k, int(int8(b))}) {
				t.Fatalf("slice conversion key %d: %+v", k, converted[k])
			}
			if got, ok := values.Get(k); !ok || got != (pair{k, 3*int(int8(b)) + k}) {
				t.Fatalf("value conversion key %d: %+v,%v", k, got, ok)
			}
		}
		mapped.Set(-1, 7)
		inverted.Set(1000, -1)
		values.Set(-1, pair{-1, 7})
		if m.Len() != len(input) || len(data) != len(input) {
			t.Fatal("conversion result keys alias input")
		}
		for k, b := range input {
			v, ok := m.Get(k)
			if !ok || v != int(int8(b)) || data[k] != v {
				t.Fatalf("conversion changed input key %d", k)
			}
		}
	})
}
