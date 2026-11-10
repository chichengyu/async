package maps

import (
	"context"
	"fmt"
	"math"
	"testing"
)

// Snapshot semantics: copy each source entry once, even when both map headers
// alias the same map and copying a non-reflexive key creates a new entry.
func TestMapAliasedMergeCopiesEachSourceEntryOnce(t *testing.T) {
	for engine := range 4 {
		for operation := range 4 {
			t.Run(fmt.Sprintf("engine=%d/operation=%d", engine, operation), func(t *testing.T) {
				raw := map[any]int{"ordinary": -1}
				nan := math.NaN()
				const entries = 64
				for i := range entries {
					raw[nan] = i
				}
				var setAll, merge, defaults func(map[any]int)
				var mergeMap func(*Map[any, int])
				switch engine {
				case 0:
					m := FromRef(raw)
					setAll = func(src map[any]int) { m.SetAll(src) }
					merge = func(src map[any]int) { m.Merge(src) }
					defaults = func(src map[any]int) { m.MergeWithDefault(src) }
					mergeMap = func(src *Map[any, int]) { m.MergeMap(src) }
				case 1:
					m := NewMapChain(context.Background(), raw)
					setAll = func(src map[any]int) { m.SetAll(src) }
					merge = func(src map[any]int) { m.Merge(src) }
					defaults = func(src map[any]int) { m.MergeWithDefault(src) }
					mergeMap = func(src *Map[any, int]) { m.MergeMap(src) }
				case 2:
					m := NewMapChain(context.Background(), raw).Serial()
					setAll = func(src map[any]int) { m.SetAll(src) }
					merge = func(src map[any]int) { m.Merge(src) }
					defaults = func(src map[any]int) { m.MergeWithDefault(src) }
					mergeMap = func(src *Map[any, int]) { m.MergeMap(src) }
				case 3:
					m := NewMapChain(context.Background(), raw).Parallel()
					setAll = func(src map[any]int) { m.SetAll(src) }
					merge = func(src map[any]int) { m.Merge(src) }
					defaults = func(src map[any]int) { m.MergeWithDefault(src) }
					mergeMap = func(src *Map[any, int]) { m.MergeMap(src) }
				}
				switch operation {
				case 0:
					setAll(raw)
				case 1:
					merge(raw)
				case 2:
					mergeMap(FromRef(raw))
				case 3:
					defaults(raw)
				}
				counts := make([]int, entries)
				for key, value := range raw {
					if key != key {
						if value < 0 || value >= entries {
							t.Fatalf("unexpected value %d", value)
						}
						counts[value]++
					}
				}
				if len(raw) != 2*entries+1 {
					t.Fatalf("merge copied inserted entries: len=%d want=%d", len(raw), 2*entries+1)
				}
				for i, count := range counts {
					if count != 2 {
						t.Fatalf("source entry %d copied %d times, want 2 including original", i, count)
					}
				}
			})
		}
	}
}

func TestMapMergeNilAndEmptyCompatibility(t *testing.T) {
	for _, operation := range []string{"set-all", "merge", "defaults", "merge-map"} {
		for _, base := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/base=%v", operation, base), func(t *testing.T) {
				var call func(map[int]int)
				if base {
					m := NewMapChain[int, int](context.Background(), nil)
					call = func(src map[int]int) {
						switch operation {
						case "set-all":
							m.SetAll(src)
						case "merge":
							m.Merge(src)
						case "defaults":
							m.MergeWithDefault(src)
						case "merge-map":
							if src == nil {
								m.MergeMap(nil)
							} else {
								m.MergeMap(FromRef(src))
							}
						}
					}
				} else {
					m := &Map[int, int]{}
					call = func(src map[int]int) {
						switch operation {
						case "set-all":
							m.SetAll(src)
						case "merge":
							m.Merge(src)
						case "defaults":
							m.MergeWithDefault(src)
						case "merge-map":
							if src == nil {
								m.MergeMap(nil)
							} else {
								m.MergeMap(FromRef(src))
							}
						}
					}
				}
				call(nil)
				call(map[int]int{})
				defer func() {
					if recover() == nil {
						t.Fatal("nonempty merge into nil map did not panic")
					}
				}()
				call(map[int]int{1: 2})
			})
		}
	}
}
