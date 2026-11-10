package maps

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
)

// Comparable keys can still be non-reflexive, including arrays/structs and
// interfaces containing NaNs. Check entries by value, never by NaN lookup.
func TestNonReflexiveMapEntryTransforms(t *testing.T) {
	for engine := range 4 {
		for _, operation := range []string{"map", "filter", "reject"} {
			t.Run(fmt.Sprintf("engine=%d/%s", engine, operation), func(t *testing.T) {
				nan := math.Float64frombits(0x7ff8000000000042)
				raw := map[any]int{"ordinary": 10}
				raw[nan], raw[[1]float64{nan}], raw[struct{ X float64 }{nan}] = 1, 2, 3
				var transform func(func(any, int) int)
				var filter, reject func(func(any, int) bool)
				switch engine {
				case 0:
					m := FromRef(raw)
					transform = func(fn func(any, int) int) { m.MapValues(fn) }
					filter = func(fn func(any, int) bool) { m.Filter(fn) }
					reject = func(fn func(any, int) bool) { m.Reject(fn) }
				case 1:
					m := NewMapChain[any, int](context.Background(), raw)
					transform = func(fn func(any, int) int) { m.MapValues(fn) }
					filter = func(fn func(any, int) bool) { m.FilterKV(fn) }
					reject = func(fn func(any, int) bool) { m.Reject(fn) }
				case 2:
					m := NewMapChain[any, int](context.Background(), raw).Serial()
					transform = func(fn func(any, int) int) { m.MapValues(fn) }
					filter = func(fn func(any, int) bool) { m.MapBase.Filter(fn) }
					reject = func(fn func(any, int) bool) { m.Reject(fn) }
				case 3:
					m := NewMapChain[any, int](context.Background(), raw).Parallel()
					transform = func(fn func(any, int) int) { m.MapValues(fn) }
					filter = func(fn func(any, int) bool) { m.MapBase.Filter(fn) }
					reject = func(fn func(any, int) bool) { m.Reject(fn) }
				}
				calls := 0
				want := []int{10}
				switch operation {
				case "map":
					transform(func(_ any, v int) int {
						calls++
						if calls > 4 {
							panic("newly inserted NaN entry visited")
						}
						return v + 100
					})
					want = []int{101, 102, 103, 110}
				case "filter":
					filter(func(_ any, v int) bool { calls++; return v == 10 })
				case "reject":
					reject(func(_ any, v int) bool { calls++; return v != 10 })
				}
				var got []int
				for _, v := range raw {
					got = append(got, v)
				}
				slices.Sort(got)
				if calls != 4 || !slices.Equal(got, want) {
					t.Fatalf("calls=%d values=%v want=%v", calls, got, want)
				}
			})
		}
	}
}

func TestMapEntryMutationPreservesOrdinaryProgressAndPanic(t *testing.T) {
	for _, operation := range []string{"map", "filter"} {
		t.Run(operation, func(t *testing.T) {
			raw := map[int]int{1: 1, 2: 2, 3: 3}
			m := FromRef(raw)
			calls := 0
			sentinel := &struct{}{}
			func() {
				defer func() {
					if r := recover(); r != sentinel {
						t.Fatalf("panic=%v", r)
					}
				}()
				check := func() {
					if operation == "map" {
						changed := 0
						for _, value := range raw {
							if value > 100 {
								changed++
							}
						}
						if changed != calls {
							t.Fatalf("prior changes=%d calls=%d", changed, calls)
						}
					} else if len(raw) != 3-calls {
						t.Fatalf("prior deletions len=%d calls=%d", len(raw), calls)
					}
					calls++
					if calls == 3 {
						panic(sentinel)
					}
				}
				if operation == "map" {
					m.MapValues(func(_ int, v int) int { check(); return v + 100 })
				} else {
					m.Filter(func(int, int) bool { check(); return false })
				}
			}()
			if calls != 3 {
				t.Fatalf("calls=%d", calls)
			}
			m.Clear()
			if len(raw) != 0 {
				t.Fatal("lost alias")
			}
		})
	}
}
