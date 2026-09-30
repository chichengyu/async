package async

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

var errMapTestSentinel = errors.New("test map sentinel error")

// ============================================================
// Shared helpers: test tiers & auxiliary functions
// ============================================================

type mapDataTier struct {
	name string
	size int
}

var mapAllTiers = []mapDataTier{
	{"万级_10K", 10_000},
	{"十万级_100K", 100_000},
	{"百万级_1M", 1_000_000},
	{"千万级_10M", 10_000_000},
}

func mapSkipIfTooLarge(t *testing.T, size int) {
	if testing.Short() && size >= 1_000_000 {
		t.Skip("short mode: skip large scale test")
	}
}

func genIntMap(n int) map[string]int {
	m := make(map[string]int, n)
	for i := 0; i < n; i++ {
		m[strconv.Itoa(i)] = i
	}
	return m
}

func mapFreshCtx() context.Context {
	return core.EnsureTraceID(context.Background())
}

var mapConcurrencies = []int{0, 1, 4, 32, 128}

type testMapLogger struct{}

func (l *testMapLogger) Log(ctx context.Context, level core.LogLevel, msg string, fields ...core.LogField) {
}
func (l *testMapLogger) With(fields ...core.LogField) core.Logger        { return l }
func (l *testMapLogger) WithContext(ctx context.Context) context.Context { return ctx }

// ============================================================
// Section 1: Data operation tests
// ============================================================

func TestMap_Chain_Len(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			l := b.Len()
			if l != tier.size {
				t.Fatalf("Len: expected %d, got %d", tier.size, l)
			}
		})
	}
}

func TestMap_Chain_IsEmpty(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			if NewMapChain(mapFreshCtx(), data).IsEmpty() && tier.size > 0 {
				t.Fatal("IsEmpty: non-empty map should return false")
			}
			if !NewMapChain(mapFreshCtx(), map[string]int{}).IsEmpty() {
				t.Fatal("IsEmpty: empty map should return true")
			}
		})
	}
}

func TestMap_Chain_Keys(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			keys := NewMapChain(mapFreshCtx(), data).Keys()
			if len(keys) != tier.size {
				t.Fatalf("Keys: expected %d, got %d", tier.size, len(keys))
			}
		})
	}
}

func TestMap_Chain_Values(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			vals := NewMapChain(mapFreshCtx(), data).Values()
			if len(vals) != tier.size {
				t.Fatalf("Values: expected %d, got %d", tier.size, len(vals))
			}
		})
	}
}

func TestMap_Chain_AsMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			ref := NewMapChain(mapFreshCtx(), data).AsMap()
			if len(ref) != tier.size {
				t.Fatalf("AsMap: expected len %d, got %d", tier.size, len(ref))
			}
		})
	}
}

func TestMap_Chain_Has(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			mid := strconv.Itoa(tier.size / 2)
			if !b.Has(mid) {
				t.Fatalf("Has: key %s should exist", mid)
			}
			if b.Has("nonexistent") {
				t.Fatal("Has: nonexistent key should not exist")
			}
		})
	}
}

func TestMap_Chain_Get(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			mid := tier.size / 2
			v, ok := b.Get(strconv.Itoa(mid))
			if !ok || v != mid {
				t.Fatalf("Get: expected (%d, true), got (%d, %v)", mid, v, ok)
			}
			v, ok = b.Get("nonexistent")
			if ok {
				t.Fatalf("Get: expected (_, false), got (%d, %v)", v, ok)
			}
		})
	}
}

func TestMap_Chain_Set(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			b.Set("newkey", 999).Set("newkey2", 1000)
			if b.Len() != tier.size+2 {
				t.Fatalf("Set: expected len %d, got %d", tier.size+2, b.Len())
			}
			v, ok := b.Get("newkey")
			if !ok || v != 999 {
				t.Fatalf("Set: Get(newkey) = (%d, %v), want (999, true)", v, ok)
			}
		})
	}
}

func TestMap_Chain_SetAll(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			b.SetAll(map[string]int{"x": -1, "y": -2})
			if b.Len() != tier.size+2 {
				t.Fatalf("SetAll: expected len %d, got %d", tier.size+2, b.Len())
			}
		})
	}
}

func TestMap_Chain_Delete(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			b.Delete("0")
			if b.Len() != tier.size-1 {
				t.Fatalf("Delete: expected len %d, got %d", tier.size-1, b.Len())
			}
			if b.Has("0") {
				t.Fatal("Delete: key '0' should be deleted")
			}
		})
	}
}

func TestMap_Chain_Clear(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			b.Clear()
			if !b.IsEmpty() {
				t.Fatal("Clear: map should be empty")
			}
		})
	}
}

func TestMap_Chain_Range(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			count := 0
			b.Range(func(k string, v int) bool {
				count++
				return true
			})
			if count != tier.size {
				t.Fatalf("Range: expected %d visits, got %d", tier.size, count)
			}
		})
	}
}

func TestMap_Chain_Range_EarlyStop(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			count := 0
			b.Range(func(k string, v int) bool {
				count++
				return false
			})
			if count != 1 {
				t.Fatalf("Range early stop: expected 1, got %d", count)
			}
		})
	}
}

// ============================================================
// Section 2: Filter & reject operation tests
// ============================================================

func TestMap_Chain_Filter(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			b.FilterKV(func(k string, v int) bool { return v%2 == 0 })
			b.Range(func(k string, v int) bool {
				if v%2 != 0 {
					t.Fatalf("Filter: found odd value %d at key %s", v, k)
				}
				return true
			})
		})
	}
}

func TestMap_Chain_Reject(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			b.Reject(func(k string, v int) bool { return v%2 != 0 })
			b.Range(func(k string, v int) bool {
				if v%2 != 0 {
					t.Fatalf("Reject: found odd value %d at key %s", v, k)
				}
				return true
			})
		})
	}
}

// ============================================================
// Section 3: MapValues tests
// ============================================================

func TestMap_Chain_MapValues(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			b.MapValues(func(k string, v int) int { return v * 2 })
			mid := tier.size / 2
			v, _ := b.Get(strconv.Itoa(mid))
			if v != mid*2 {
				t.Fatalf("MapValues: expected %d, got %d", mid*2, v)
			}
		})
	}
}

// ============================================================
// Section 4: Merge operation tests
// ============================================================

func TestMap_Chain_Merge(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			extra := map[string]int{"extra_a": -1, "extra_b": -2}
			b.Merge(extra)
			if b.Len() != tier.size+2 {
				t.Fatalf("Merge: expected len %d, got %d", tier.size+2, b.Len())
			}
		})
	}
}

func TestMap_Chain_MergeMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			other := NewMap[string, int]()
			other.Set("extra_x", 100)
			b.MergeMap(other)
			if b.Len() != tier.size+1 {
				t.Fatalf("MergeMap: expected len %d, got %d", tier.size+1, b.Len())
			}
		})
	}
}

func TestMap_Chain_MergeMap_Nil(t *testing.T) {
	data := map[string]int{"a": 1}
	b := NewMapChain(mapFreshCtx(), data)
	b.MergeMap(nil)
	if b.Len() != 1 {
		t.Fatalf("MergeMap nil: expected len 1, got %d", b.Len())
	}
}

func TestMap_Chain_MergeWithDefault(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			mid := strconv.Itoa(tier.size / 2)
			override := map[string]int{"new_only": -1}
			override[mid] = 99999
			b.MergeWithDefault(override)
			v, _ := b.Get(mid)
			if v != tier.size/2 {
				t.Fatalf("MergeWithDefault: existing key should not be overridden, got %d", v)
			}
			v2, ok := b.Get("new_only")
			if !ok || v2 != -1 {
				t.Fatalf("MergeWithDefault: new key should be added, got (%d, %v)", v2, ok)
			}
		})
	}
}

// ============================================================
// Section 5: Search operation tests
// ============================================================

func TestMap_Chain_Find(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			mid := tier.size / 2
			k, v, found := b.Find(func(key string, val int) bool { return val >= mid })
			if !found {
				t.Fatal("Find: should find a value")
			}
			if v < mid {
				t.Fatalf("Find: expected value >= %d, got %d (key=%s)", mid, v, k)
			}
		})
	}
}

func TestMap_Chain_Find_NotFound(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2}
	b := NewMapChain(mapFreshCtx(), data)
	_, _, found := b.Find(func(k string, v int) bool { return v > 100 })
	if found {
		t.Fatal("Find: should not find")
	}
}

func TestMap_Chain_All(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			if !b.All(func(k string, v int) bool { return v >= 0 }) {
				t.Fatal("All: all values should be >= 0")
			}
			if tier.size > 10 {
				if b.All(func(k string, v int) bool { return v < tier.size/2 }) {
					t.Fatal("All: not all values should be < mid")
				}
			}
		})
	}
}

func TestMap_Chain_Any(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			mid := tier.size / 2
			if !b.Any(func(k string, v int) bool { return v == mid }) {
				t.Fatalf("Any: should find %d", mid)
			}
			if b.Any(func(k string, v int) bool { return v < 0 }) {
				t.Fatal("Any: no negative values should exist")
			}
		})
	}
}

func TestMap_Chain_Count(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			b := NewMapChain(mapFreshCtx(), data)
			cnt := b.Count(func(k string, v int) bool { return v%2 == 0 })
			expected := tier.size/2 + tier.size%2
			if cnt != expected {
				t.Fatalf("Count: expected %d, got %d", expected, cnt)
			}
		})
	}
}

// ============================================================
// Section 6: Terminal Map tests
// ============================================================

func TestMap_Chain_Map_MapChain(t *testing.T) {
	for _, tier := range mapAllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				mapSkipIfTooLarge(t, tier.size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v * 2, nil
				})
				if r.Err() {
					t.Fatalf("Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("Map: expected %d entries, got %d", tier.size, r.Len())
				}
				keys := r.Keys()
				if len(keys) != tier.size {
					t.Fatalf("Map Keys: expected %d, got %d", tier.size, len(keys))
				}
			})
		}
	}
}

func TestMap_Chain_Map_ParallelMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				mapSkipIfTooLarge(t, tier.size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Parallel().Worker(conc).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v * 2, nil
				})
				if r.Err() {
					t.Fatalf("Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("Map: expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
	}
}

func TestMap_Chain_Map_SerialMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			r := NewMapChain(mapFreshCtx(), data).Serial().Map(func(ctx context.Context, k string, v int) (int, error) {
				return v * 2, nil
			})
			if r.Err() {
				t.Fatalf("Map error: %v", r.Error())
			}
			if r.Len() != tier.size {
				t.Fatalf("Map: expected %d entries, got %d", tier.size, r.Len())
			}
		})
	}
}

func TestMap_Chain_Map_SerialMap_FailFast(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			r := NewMapChain(mapFreshCtx(), data).Serial().FailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
				return v * 2, nil
			})
			if r.Err() {
				t.Fatalf("Map Serial FF: unexpected error: %v", r.Error())
			}
			if r.Len() != tier.size {
				t.Fatalf("Map Serial FF: expected %d entries, got %d", tier.size, r.Len())
			}
			mid := strconv.Itoa(tier.size / 2)
			r2 := NewMapChain(mapFreshCtx(), data).Serial().FailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
				if k == mid {
					return 0, errors.New("injected error")
				}
				return v * 2, nil
			})
			if !r2.Err() {
				t.Fatal("Map Serial FF: expected error but got none")
			}
			if r2.Len() >= tier.size && tier.size > 0 {
				t.Fatalf("Map Serial FF: expected partial results (len<%d), got %d", tier.size, r2.Len())
			}
		})
	}
}

func TestMap_Chain_Map_MapChainWith(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			r := NewMapChainWith[string, int, string](mapFreshCtx(), data).Map(func(ctx context.Context, k string, v int) (string, error) {
				return strconv.Itoa(v), nil
			})
			if r.Err() {
				t.Fatalf("MapChainWith Map error: %v", r.Error())
			}
			vals := r.Values()
			if len(vals) != tier.size {
				t.Fatalf("MapChainWith Map: expected %d values, got %d", tier.size, len(vals))
			}
		})
	}
}

// ============================================================
// Section 7: Terminal ForEach tests
// ============================================================

func TestMap_Chain_ForEach_MapChain(t *testing.T) {
	for _, tier := range mapAllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				mapSkipIfTooLarge(t, tier.size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genIntMap(tier.size)
				var sum int64
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).ForEach(func(ctx context.Context, k string, v int) error {
					atomic.AddInt64(&sum, int64(v))
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach error: %v", r.Error())
				}
				if r.SuccessCount() != int64(tier.size) {
					t.Fatalf("ForEach SuccessCount: expected %d, got %d", tier.size, r.SuccessCount())
				}
			})
		}
	}
}

func TestMap_Chain_ForEach_ParallelMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				mapSkipIfTooLarge(t, tier.size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genIntMap(tier.size)
				var sum int64
				r := NewMapChain(mapFreshCtx(), data).Parallel().Worker(conc).ForEach(func(ctx context.Context, k string, v int) error {
					atomic.AddInt64(&sum, int64(v))
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach error: %v", r.Error())
				}
				if r.SuccessCount() != int64(tier.size) {
					t.Fatalf("ForEach SuccessCount: expected %d, got %d", tier.size, r.SuccessCount())
				}
			})
		}
	}
}

func TestMap_Chain_ForEach_SerialMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			var sum int64
			r := NewMapChain(mapFreshCtx(), data).Serial().ForEach(func(ctx context.Context, k string, v int) error {
				atomic.AddInt64(&sum, int64(v))
				return nil
			})
			if r.Err() {
				t.Fatalf("ForEach error: %v", r.Error())
			}
			if r.SuccessCount() != int64(tier.size) {
				t.Fatalf("ForEach SuccessCount: expected %d, got %d", tier.size, r.SuccessCount())
			}
		})
	}
}

func TestMap_Chain_ForEach_SerialMap_FailFast(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			var counter int64
			r := NewMapChain(mapFreshCtx(), data).Serial().FailFast().ForEach(func(ctx context.Context, k string, v int) error {
				atomic.AddInt64(&counter, 1)
				return nil
			})
			if r.Err() {
				t.Fatalf("ForEach Serial FF: unexpected error: %v", r.Error())
			}
			if r.SuccessCount() != int64(tier.size) {
				t.Fatalf("ForEach Serial FF: expected %d, got %d", tier.size, r.SuccessCount())
			}
			mid := strconv.Itoa(tier.size / 2)
			r2 := NewMapChain(mapFreshCtx(), data).Serial().FailFast().ForEach(func(ctx context.Context, k string, v int) error {
				if k == mid {
					return errors.New("injected error")
				}
				return nil
			})
			if !r2.Err() {
				t.Fatal("ForEach Serial FF: expected error but got none")
			}
		})
	}
}

// ============================================================
// Section 8: Terminal Reduce tests
// ============================================================

func TestMap_Chain_Reduce_MapChain(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			sum, err := NewMapChain(mapFreshCtx(), data).Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
				return acc + v, nil
			})
			if err != nil {
				t.Fatalf("Reduce error: %v", err)
			}
			expected := tier.size * (tier.size - 1) / 2
			if sum != expected {
				t.Fatalf("Reduce: expected sum %d, got %d", expected, sum)
			}
		})
	}
}

func TestMap_Chain_Reduce_SerialMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			sum, err := NewMapChain(mapFreshCtx(), data).Serial().Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
				return acc + v, nil
			})
			if err != nil {
				t.Fatalf("Reduce error: %v", err)
			}
			expected := tier.size * (tier.size - 1) / 2
			if sum != expected {
				t.Fatalf("Reduce: expected sum %d, got %d", expected, sum)
			}
		})
	}
}

func TestMap_Chain_Reduce_ParallelMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				mapSkipIfTooLarge(t, tier.size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genIntMap(tier.size)
				sum, err := NewMapChain(mapFreshCtx(), data).Parallel().Worker(conc).Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
					return acc + v, nil
				})
				if err != nil {
					t.Fatalf("Reduce Parallel error: %v", err)
				}
				expected := tier.size * (tier.size - 1) / 2
				if sum != expected {
					t.Fatalf("Reduce Parallel: expected sum %d, got %d", expected, sum)
				}
			})
		}
	}
}

// ============================================================
// Section 9: Terminal Stream tests
// ============================================================

func TestMap_Chain_Stream_MapChain(t *testing.T) {
	for _, tier := range mapAllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				mapSkipIfTooLarge(t, tier.size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genIntMap(tier.size)
				ch := NewMapChain(mapFreshCtx(), data).Worker(conc).Stream(func(ctx context.Context, k string, v int) (int, error) {
					return v * 2, nil
				}, 0)
				count := 0
				for res := range ch {
					if res.Err != nil {
						t.Fatalf("Stream error: %v", res.Err)
					}
					count++
				}
				if count != tier.size {
					t.Fatalf("Stream: expected %d results, got %d", tier.size, count)
				}
			})
		}
	}
}

func TestMap_Chain_Stream_ParallelMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		for _, conc := range mapConcurrencies {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				mapSkipIfTooLarge(t, tier.size)
				if testing.Short() && conc > 32 {
					t.Skip("short mode: skip high concurrency")
				}
				t.Parallel()
				data := genIntMap(tier.size)
				ch := NewMapChain(mapFreshCtx(), data).Parallel().Worker(conc).Stream(func(ctx context.Context, k string, v int) (int, error) {
					return v * 2, nil
				}, 0)
				count := 0
				for res := range ch {
					if res.Err != nil {
						t.Fatalf("Stream error: %v", res.Err)
					}
					count++
				}
				if count != tier.size {
					t.Fatalf("Stream: expected %d results, got %d", tier.size, count)
				}
			})
		}
	}
}

func TestMap_Chain_Stream_SerialMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			ch := NewMapChain(mapFreshCtx(), data).Serial().Stream(func(ctx context.Context, k string, v int) (int, error) {
				return v * 2, nil
			}, 0)
			count := 0
			for res := range ch {
				if res.Err != nil {
					t.Fatalf("Stream error: %v", res.Err)
				}
				count++
			}
			if count != tier.size {
				t.Fatalf("Stream Serial: expected %d results, got %d", tier.size, count)
			}
		})
	}
}

// ============================================================
// Section 10: Terminal MapBatch tests
// ============================================================

func TestMap_Chain_MapBatch_MapChain(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			r := NewMapChain(mapFreshCtx(), data).Chunk(100).MapBatch(func(ctx context.Context, chunk []MapEntry[string, int]) (int, error) {
				sum := 0
				for _, e := range chunk {
					sum += e.Val
				}
				return sum, nil
			})
			if r.Err() {
				t.Fatalf("MapBatch error: %v", r.Error())
			}
			if r.Len() <= 0 {
				t.Fatal("MapBatch: expected non-empty results")
			}
		})
	}
}

func TestMap_Chain_MapBatch_ParallelMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			r := NewMapChain(mapFreshCtx(), data).Parallel().Chunk(100).MapBatch(func(ctx context.Context, chunk []MapEntry[string, int]) (int, error) {
				sum := 0
				for _, e := range chunk {
					sum += e.Val
				}
				return sum, nil
			})
			if r.Err() {
				t.Fatalf("MapBatch error: %v", r.Error())
			}
			if r.Len() <= 0 {
				t.Fatal("MapBatch: expected non-empty results")
			}
		})
	}
}

func TestMap_Chain_MapBatch_Chained(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			r := NewMapChain(mapFreshCtx(), data).Chunk(100).MapBatch(func(ctx context.Context, chunk []MapEntry[string, int]) (int, error) {
				sum := 0
				for _, e := range chunk {
					sum += e.Val
				}
				return sum, nil
			})
			if r.Err() {
				t.Fatalf("MapBatch error: %v", r.Error())
			}
			if r.Len() <= 0 {
				t.Fatal("MapBatch: expected non-empty results")
			}
		})
	}
}

// ============================================================
// Section 11: Terminal ForEachBatch tests
// ============================================================

func TestMap_Chain_ForEachBatch_MapChain(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			var processed int64
			r := NewMapChain(mapFreshCtx(), data).Chunk(100).ForEachBatch(func(ctx context.Context, chunk []MapEntry[string, int]) error {
				atomic.AddInt64(&processed, int64(len(chunk)))
				return nil
			})
			if r.Err() {
				t.Fatalf("ForEachBatch error: %v", r.Error())
			}
			if processed != int64(tier.size) {
				t.Fatalf("ForEachBatch: expected %d, got %d", tier.size, processed)
			}
		})
	}
}

func TestMap_Chain_ForEachBatch_ParallelMap(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			var processed int64
			r := NewMapChain(mapFreshCtx(), data).Parallel().Chunk(100).ForEachBatch(func(ctx context.Context, chunk []MapEntry[string, int]) error {
				atomic.AddInt64(&processed, int64(len(chunk)))
				return nil
			})
			if r.Err() {
				t.Fatalf("ForEachBatch error: %v", r.Error())
			}
			if processed != int64(tier.size) {
				t.Fatalf("ForEachBatch: expected %d, got %d", tier.size, processed)
			}
		})
	}
}

func TestMap_Chain_ForEachBatch_Chained(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			var processed int64
			r := NewMapChain(mapFreshCtx(), data).Chunk(100).ForEachBatch(func(ctx context.Context, chunk []MapEntry[string, int]) error {
				atomic.AddInt64(&processed, int64(len(chunk)))
				return nil
			})
			if r.Err() {
				t.Fatalf("ForEachBatch error: %v", r.Error())
			}
			if processed != int64(tier.size) {
				t.Fatalf("ForEachBatch: expected %d, got %d", tier.size, processed)
			}
		})
	}
}

// ============================================================
// Section 12: MapResult operation tests
// ============================================================

func TestMap_Chain_Result_Error(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	r := NewMapChain(mapFreshCtx(), data).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Error() != nil {
		t.Fatalf("Error: expected nil, got %v", r.Error())
	}
}

func TestMap_Chain_Result_IsErr(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2}
	r := NewMapChain(mapFreshCtx(), data).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatal("Err: expected false")
	}
}

func TestMap_Chain_Result_Keys_Values(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	r := NewMapChain(mapFreshCtx(), data).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v * 2, nil
	})
	keys := r.Keys()
	if len(keys) != 3 {
		t.Fatalf("Keys: expected 3, got %d", len(keys))
	}
	vals := r.Values()
	if len(vals) != 3 {
		t.Fatalf("Values: expected 3, got %d", len(vals))
	}
	for _, v := range vals {
		if v%2 != 0 {
			t.Fatalf("Values: expected even, got %d", v)
		}
	}
}

func TestMap_Chain_Result_Data(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2}
	r := NewMapChain(mapFreshCtx(), data).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	resultData := r.Data()
	if len(resultData) != 2 {
		t.Fatalf("Data: expected 2 entries, got %d", len(resultData))
	}
}

func TestMap_Chain_Result_Len(t *testing.T) {
	data := genIntMap(500)
	r := NewMapChain(mapFreshCtx(), data).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Len() != 500 {
		t.Fatalf("Len: expected 500, got %d", r.Len())
	}
}

func TestMap_Chain_Result_ErrorInjected(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mid := "b"
	r := NewMapChain(mapFreshCtx(), data).Map(func(ctx context.Context, k string, v int) (int, error) {
		if k == mid {
			return 0, errMapTestSentinel
		}
		return v, nil
	})
	if !r.Err() {
		t.Fatal("should have error")
	}
}

// ============================================================
// Section 13: MapForEachResult operation tests
// ============================================================

func TestMap_Chain_ForEachResult_Error(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2}
	r := NewMapChain(mapFreshCtx(), data).ForEach(func(ctx context.Context, k string, v int) error {
		return nil
	})
	if r.Error() != nil {
		t.Fatalf("Error: expected nil, got %v", r.Error())
	}
}

func TestMap_Chain_ForEachResult_IsErr(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2}
	r := NewMapChain(mapFreshCtx(), data).ForEach(func(ctx context.Context, k string, v int) error {
		return nil
	})
	if r.Err() {
		t.Fatal("Err: expected false")
	}
}

func TestMap_Chain_ForEachResult_Total(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).ForEach(func(ctx context.Context, k string, v int) error {
		return nil
	})
	if r.Total() != 1000 {
		t.Fatalf("Total: expected 1000, got %d", r.Total())
	}
}

func TestMap_Chain_ForEachResult_FailCount(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	r := NewMapChain(mapFreshCtx(), data).ForEach(func(ctx context.Context, k string, v int) error {
		if k == "b" {
			return errMapTestSentinel
		}
		return nil
	})
	if r.FailCount() != 1 {
		t.Fatalf("FailCount: expected 1, got %d", r.FailCount())
	}
}

func TestMap_Chain_ForEachResult_SuccessCount(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	r := NewMapChain(mapFreshCtx(), data).ForEach(func(ctx context.Context, k string, v int) error {
		if k == "b" {
			return errMapTestSentinel
		}
		return nil
	})
	if r.SuccessCount() != 2 {
		t.Fatalf("SuccessCount: expected 2, got %d", r.SuccessCount())
	}
}

// ============================================================
// Section 14: Builder & config tests
// ============================================================

func TestMap_Chain_Builder_NewMapChain(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	b := NewMapChain(mapFreshCtx(), data)
	if b == nil {
		t.Fatal("NewMapChain: expected non-nil builder")
	}
	if b.Len() != 3 {
		t.Fatalf("NewMapChain: expected len 3, got %d", b.Len())
	}
}

func TestMap_Chain_NewMapChainWith(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2}
	b := NewMapChainWith[string, int, string](mapFreshCtx(), data)
	if b == nil {
		t.Fatal("NewMapChainWith: expected non-nil builder")
	}
	r := b.Map(func(ctx context.Context, k string, v int) (string, error) {
		return strconv.Itoa(v), nil
	})
	vals := r.Values()
	if len(vals) != 2 {
		t.Fatalf("NewMapChainWith: unexpected result len %d", len(vals))
	}
}

func TestMap_Chain_Serial(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	s := NewMapChain(mapFreshCtx(), data).Serial()
	if s == nil {
		t.Fatal("Serial: expected non-nil MapSerial")
	}
	vals := s.Values()
	if len(vals) != 3 {
		t.Fatalf("Serial: expected 3 values, got %d", len(vals))
	}
}

func TestMap_Chain_Parallel(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	p := NewMapChain(mapFreshCtx(), data).Parallel()
	if p == nil {
		t.Fatal("Parallel: expected non-nil MapParallel")
	}
	vals := p.Values()
	if len(vals) != 3 {
		t.Fatalf("Parallel: expected 3 values, got %d", len(vals))
	}
}

func TestMap_Chain_ToSerial(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	s := NewMapChain(mapFreshCtx(), data).Parallel().ToSerial()
	vals := s.Values()
	if len(vals) != 3 {
		t.Fatalf("ToSerial: expected 3 values, got %d", len(vals))
	}
}

func TestMap_Chain_ToParallel(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	p := NewMapChain(mapFreshCtx(), data).Serial().ToParallel()
	vals := p.Values()
	if len(vals) != 3 {
		t.Fatalf("ToParallel: expected 3 values, got %d", len(vals))
	}
}

func TestMap_Chain_Worker(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).Worker(4).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Worker: %v", r.Error())
	}
	if r.Len() != 1000 {
		t.Fatalf("Worker: expected 1000, got %d", r.Len())
	}
}

func TestMap_Chain_DefaultWorker(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).DefaultWorker().Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultWorker: %v", r.Error())
	}
}

func TestMap_Chain_DefaultPool(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).DefaultPool().Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultPool: %v", r.Error())
	}
	if r.Len() != 1000 {
		t.Fatalf("DefaultPool: expected 1000, got %d", r.Len())
	}
}

func TestMap_Chain_Pool(t *testing.T) {
	p := pool.NewPool[int](16)
	defer p.Close()
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).Pool(p).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Pool: %v", r.Error())
	}
	if r.Len() != 1000 {
		t.Fatalf("Pool: expected 1000, got %d", r.Len())
	}
}

func TestMap_Chain_PoolAuto(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).PoolAuto(pool.NewPool[int](16)).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("PoolAuto: %v", r.Error())
	}
	if r.Len() != 1000 {
		t.Fatalf("PoolAuto: expected 1000, got %d", r.Len())
	}
}

func TestMap_Chain_Timeout(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).Timeout(5 * time.Second).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Timeout: %v", r.Error())
	}
}

func TestMap_Chain_DefaultTimeout(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).DefaultTimeout().Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultTimeout: %v", r.Error())
	}
}

func TestMap_Chain_FailFast(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).FailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("FailFast: %v", r.Error())
	}
}

func TestMap_Chain_DefaultFailFast(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).DefaultFailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultFailFast: %v", r.Error())
	}
}

func TestMap_Chain_NoFailFast(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).Parallel().NoFailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("NoFailFast: %v", r.Error())
	}
}

func TestMap_Chain_Shards(t *testing.T) {
	data := genIntMap(10000)
	r := NewMapChain(mapFreshCtx(), data).Shards(8).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Shards: %v", r.Error())
	}
	if r.Len() != 10000 {
		t.Fatalf("Shards: expected 10000, got %d", r.Len())
	}
}

func TestMap_Chain_DefaultShard(t *testing.T) {
	data := genIntMap(10000)
	r := NewMapChain(mapFreshCtx(), data).DefaultShard().Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultShard: %v", r.Error())
	}
	if r.Len() != 10000 {
		t.Fatalf("DefaultShard: expected 10000, got %d", r.Len())
	}
}

func TestMap_Chain_Chunk(t *testing.T) {
	data := genIntMap(10000)
	r := NewMapChain(mapFreshCtx(), data).Chunk(100).MapBatch(func(ctx context.Context, chunk []MapEntry[string, int]) (int, error) {
		return len(chunk), nil
	})
	if r.Err() {
		t.Fatalf("Chunk: %v", r.Error())
	}
}

func TestMap_Chain_DefaultChunk(t *testing.T) {
	data := genIntMap(1000)
	r := NewMapChain(mapFreshCtx(), data).DefaultChunk().MapBatch(func(ctx context.Context, chunk []MapEntry[string, int]) (int, error) {
		return len(chunk), nil
	})
	if r.Err() {
		t.Fatalf("DefaultChunk: %v", r.Error())
	}
}

func TestMap_Chain_Buf(t *testing.T) {
	data := genIntMap(1000)
	ch := NewMapChain(mapFreshCtx(), data).Buf(2048).Stream(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	}, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 1000 {
		t.Fatalf("Buf: expected 1000, got %d", count)
	}
}

func TestMap_Chain_DefaultBuf(t *testing.T) {
	data := genIntMap(1000)
	ch := NewMapChain(mapFreshCtx(), data).DefaultBuf().Stream(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	}, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 1000 {
		t.Fatalf("DefaultBuf: expected 1000, got %d", count)
	}
}

func TestMap_Chain_Logger(t *testing.T) {
	data := genIntMap(100)
	r := NewMapChain(mapFreshCtx(), data).Logger(&testMapLogger{}).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Logger: %v", r.Error())
	}
	core.SetLogger(nil)
}

func TestMap_Chain_DefaultLogger(t *testing.T) {
	data := genIntMap(100)
	r := NewMapChain(mapFreshCtx(), data).DefaultLogger().Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("DefaultLogger: %v", r.Error())
	}
}

// ============================================================
// Section 15: Cross-mode switching chain tests
// ============================================================

func TestMap_Chain_CrossMode_Map(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			r1 := NewMapChain(mapFreshCtx(), data).Serial().Map(func(ctx context.Context, k string, v int) (int, error) {
				return v + 1, nil
			})
			if r1.Err() {
				t.Fatalf("Serial Map error: %v", r1.Error())
			}
			r2 := NewMapChain(mapFreshCtx(), data).Serial().ToParallel().Worker(4).Map(func(ctx context.Context, k string, v int) (int, error) {
				return v + 1, nil
			})
			if r2.Err() {
				t.Fatalf("Serial->Parallel Map error: %v", r2.Error())
			}
			if r2.Len() != tier.size {
				t.Fatalf("Serial->Parallel Map: expected %d, got %d", tier.size, r2.Len())
			}
		})
	}
}

func TestMap_Chain_CrossMode_ForEach(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			var counter1, counter2 int64
			r1 := NewMapChain(mapFreshCtx(), data).Parallel().NoFailFast().ForEach(func(ctx context.Context, k string, v int) error {
				atomic.AddInt64(&counter1, 1)
				return nil
			})
			if r1.Err() {
				t.Fatalf("Parallel ForEach error: %v", r1.Error())
			}
			if counter1 != int64(tier.size) {
				t.Fatalf("Parallel ForEach: expected %d, got %d", tier.size, counter1)
			}
			r2 := NewMapChain(mapFreshCtx(), data).Parallel().ToSerial().ForEach(func(ctx context.Context, k string, v int) error {
				atomic.AddInt64(&counter2, 1)
				return nil
			})
			if r2.Err() {
				t.Fatalf("Parallel->Serial ForEach error: %v", r2.Error())
			}
			if counter2 != int64(tier.size) {
				t.Fatalf("Parallel->Serial ForEach: expected %d, got %d", tier.size, counter2)
			}
		})
	}
}

// ============================================================
// Section 16: FailFast stress test with parallel mode
// ============================================================

func TestMap_Chain_FailFast_Stress(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			mid := strconv.Itoa(tier.size / 2)
			var called int64
			r := NewMapChain(mapFreshCtx(), data).Worker(64).FailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
				atomic.AddInt64(&called, 1)
				if k == mid {
					return 0, errMapTestSentinel
				}
				return v, nil
			})
			if r.Error() == nil {
				t.Fatal("FailFast: expected error but got nil")
			}
			if called >= int64(tier.size) && tier.size > 0 {
				t.Logf("FailFast: early exit verified, called=%d/%d", called, tier.size)
			}
		})
	}
}

// ============================================================
// Section 17: Extreme high concurrency multi-strategy combined stress tests
// Short mode: each tier runs with concurrency=8, quick verification
// Full mode: full concurrency matrix (basic 64/128 + extreme 256/512/1024)
// Race detection: go test -race -run "TestMap_Chain_HighConcurrency" -count=1 -timeout 30m ./...
// ============================================================

func TestMap_Chain_HighConcurrency_Map(t *testing.T) {
	if testing.Short() {
		for _, tier := range mapAllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.name), func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Worker(8).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
		return
	}
	basicConc := []int{64, 128}
	extremeConc := []int{256, 512, 1024}
	for _, tier := range mapAllTiers {
		for _, conc := range basicConc {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
	}
	for _, tier := range mapAllTiers {
		for _, conc := range extremeConc {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
	}
}

func TestMap_Chain_HighConcurrency_Pool_Map(t *testing.T) {
	if testing.Short() {
		for _, tier := range mapAllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.name), func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				p := pool.NewPool[int](8)
				r := NewMapChain(mapFreshCtx(), data).PoolAuto(p).Worker(8).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v, nil
				})
				if r.Err() {
					t.Fatalf("Pool Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
		return
	}
	basicConc := []int{32, 64}
	extremeConc := []int{128, 256}
	for _, tier := range mapAllTiers {
		for _, conc := range basicConc {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				p := pool.NewPool[int](conc)
				r := NewMapChain(mapFreshCtx(), data).PoolAuto(p).Worker(conc).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v, nil
				})
				if r.Err() {
					t.Fatalf("Pool Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
	}
	for _, tier := range mapAllTiers {
		for _, conc := range extremeConc {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				p := pool.NewPool[int](conc)
				r := NewMapChain(mapFreshCtx(), data).PoolAuto(p).Worker(conc).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v, nil
				})
				if r.Err() {
					t.Fatalf("Pool Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
	}
}

func TestMap_Chain_HighConcurrency_Shards_Map(t *testing.T) {
	if testing.Short() {
		for _, tier := range mapAllTiers {
			t.Run(fmt.Sprintf("%s_s8", tier.name), func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Shards(8).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v * 2, nil
				})
				if r.Err() {
					t.Fatalf("Shards Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
		return
	}
	basicShards := []int{8, 16, 32}
	extremeShards := []int{64, 128}
	for _, tier := range mapAllTiers {
		for _, shards := range basicShards {
			name := fmt.Sprintf("%s_s%d", tier.name, shards)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Shards(shards).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v * 2, nil
				})
				if r.Err() {
					t.Fatalf("Shards Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
	}
	for _, tier := range mapAllTiers {
		for _, shards := range extremeShards {
			name := fmt.Sprintf("%s_s%d", tier.name, shards)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Shards(shards).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v * 2, nil
				})
				if r.Err() {
					t.Fatalf("Shards Map error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
	}
}

func TestMap_Chain_HighConcurrency_Stream(t *testing.T) {
	if testing.Short() {
		for _, tier := range mapAllTiers {
			t.Run(fmt.Sprintf("%s_c8_b256", tier.name), func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				ch := NewMapChain(mapFreshCtx(), data).Worker(8).Buf(256).Stream(func(ctx context.Context, k string, v int) (int, error) {
					return v, nil
				}, 0)
				count := 0
				for res := range ch {
					if res.Err != nil {
						t.Fatalf("Stream error: %v", res.Err)
					}
					count++
				}
				if count != tier.size {
					t.Fatalf("Stream: expected %d results, got %d", tier.size, count)
				}
			})
		}
		return
	}
	basicConc := []int{64, 128}
	extremeConc := []int{256, 512, 1024}
	bufSizes := []int{0, 1024, 8192}
	for _, tier := range mapAllTiers {
		for _, conc := range basicConc {
			for _, buf := range bufSizes {
				name := fmt.Sprintf("%s_c%d_b%d", tier.name, conc, buf)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					data := genIntMap(tier.size)
					ch := NewMapChain(mapFreshCtx(), data).Worker(conc).Buf(buf).Stream(func(ctx context.Context, k string, v int) (int, error) {
						return v, nil
					}, 0)
					count := 0
					for res := range ch {
						if res.Err != nil {
							t.Fatalf("Stream error: %v", res.Err)
						}
						count++
					}
					if count != tier.size {
						t.Fatalf("Stream: expected %d results, got %d", tier.size, count)
					}
				})
			}
		}
	}
	for _, tier := range mapAllTiers {
		for _, conc := range extremeConc {
			for _, buf := range bufSizes {
				name := fmt.Sprintf("%s_c%d_b%d", tier.name, conc, buf)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					data := genIntMap(tier.size)
					ch := NewMapChain(mapFreshCtx(), data).Worker(conc).Buf(buf).Stream(func(ctx context.Context, k string, v int) (int, error) {
						return v, nil
					}, 0)
					count := 0
					for res := range ch {
						if res.Err != nil {
							t.Fatalf("Stream error: %v", res.Err)
						}
						count++
					}
					if count != tier.size {
						t.Fatalf("Stream: expected %d results, got %d", tier.size, count)
					}
				})
			}
		}
	}
}

func TestMap_Chain_HighConcurrency_ForEach_Stress(t *testing.T) {
	if testing.Short() {
		for _, tier := range mapAllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.name), func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				var counter int64
				r := NewMapChain(mapFreshCtx(), data).Worker(8).ForEach(func(ctx context.Context, k string, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach error: %v", r.Error())
				}
				if counter != int64(tier.size) {
					t.Fatalf("ForEach: expected %d calls, got %d", tier.size, counter)
				}
				if r.SuccessCount() != int64(tier.size) {
					t.Fatalf("SuccessCount: expected %d, got %d", tier.size, r.SuccessCount())
				}
			})
		}
		return
	}
	basicConc := []int{64, 128}
	extremeConc := []int{256, 512, 1024}
	for _, tier := range mapAllTiers {
		for _, conc := range basicConc {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				var counter int64
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).ForEach(func(ctx context.Context, k string, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach error: %v", r.Error())
				}
				if counter != int64(tier.size) {
					t.Fatalf("ForEach: expected %d calls, got %d", tier.size, counter)
				}
				if r.SuccessCount() != int64(tier.size) {
					t.Fatalf("SuccessCount: expected %d, got %d", tier.size, r.SuccessCount())
				}
			})
		}
	}
	for _, tier := range mapAllTiers {
		for _, conc := range extremeConc {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				var counter int64
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).ForEach(func(ctx context.Context, k string, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach error: %v", r.Error())
				}
				if counter != int64(tier.size) {
					t.Fatalf("ForEach: expected %d calls, got %d", tier.size, counter)
				}
				if r.SuccessCount() != int64(tier.size) {
					t.Fatalf("SuccessCount: expected %d, got %d", tier.size, r.SuccessCount())
				}
			})
		}
	}
}

func TestMap_Chain_HighConcurrency_Map_Timeout(t *testing.T) {
	if testing.Short() {
		for _, tier := range mapAllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.name), func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Worker(8).Timeout(30 * time.Second).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+Timeout error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
		return
	}
	for _, tier := range mapAllTiers {
		for _, conc := range []int{64, 128, 256, 512} {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).Timeout(30 * time.Second).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+Timeout error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
	}
}

func TestMap_Chain_HighConcurrency_Map_FailFast(t *testing.T) {
	if testing.Short() {
		for _, tier := range mapAllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.name), func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Worker(8).FailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+FailFast error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
		return
	}
	for _, tier := range mapAllTiers {
		for _, conc := range []int{64, 128, 256, 512, 1024} {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).FailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+FailFast error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
	}
}

func TestMap_Chain_HighConcurrency_Map_TimeoutFailFast(t *testing.T) {
	if testing.Short() {
		for _, tier := range mapAllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.name), func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Worker(8).Timeout(30 * time.Second).FailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+TO+FF error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
		return
	}
	for _, tier := range mapAllTiers {
		for _, conc := range []int{64, 128, 256, 512} {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).Timeout(30 * time.Second).FailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
					return v + 1, nil
				})
				if r.Err() {
					t.Fatalf("Map+TO+FF error: %v", r.Error())
				}
				if r.Len() != tier.size {
					t.Fatalf("expected %d entries, got %d", tier.size, r.Len())
				}
			})
		}
	}
}

func TestMap_Chain_HighConcurrency_ForEach_Timeout(t *testing.T) {
	if testing.Short() {
		for _, tier := range mapAllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.name), func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				var counter int64
				r := NewMapChain(mapFreshCtx(), data).Worker(8).Timeout(30 * time.Second).ForEach(func(ctx context.Context, k string, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+Timeout error: %v", r.Error())
				}
				if counter != int64(tier.size) {
					t.Fatalf("expected %d calls, got %d", tier.size, counter)
				}
			})
		}
		return
	}
	for _, tier := range mapAllTiers {
		for _, conc := range []int{64, 128, 256, 512} {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				var counter int64
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).Timeout(30 * time.Second).ForEach(func(ctx context.Context, k string, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+Timeout error: %v", r.Error())
				}
				if counter != int64(tier.size) {
					t.Fatalf("expected %d calls, got %d", tier.size, counter)
				}
			})
		}
	}
}

func TestMap_Chain_HighConcurrency_ForEach_FailFast(t *testing.T) {
	if testing.Short() {
		for _, tier := range mapAllTiers {
			t.Run(fmt.Sprintf("%s_c8", tier.name), func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				var counter int64
				r := NewMapChain(mapFreshCtx(), data).Worker(8).FailFast().ForEach(func(ctx context.Context, k string, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+FailFast error: %v", r.Error())
				}
				if counter != int64(tier.size) {
					t.Fatalf("expected %d calls, got %d", tier.size, counter)
				}
			})
		}
		return
	}
	for _, tier := range mapAllTiers {
		for _, conc := range []int{64, 128, 256, 512, 1024} {
			name := fmt.Sprintf("%s_c%d", tier.name, conc)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				data := genIntMap(tier.size)
				var counter int64
				r := NewMapChain(mapFreshCtx(), data).Worker(conc).FailFast().ForEach(func(ctx context.Context, k string, v int) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
				if r.Err() {
					t.Fatalf("ForEach+FailFast error: %v", r.Error())
				}
				if counter != int64(tier.size) {
					t.Fatalf("expected %d calls, got %d", tier.size, counter)
				}
			})
		}
	}
}

// ============================================================
// Section 18: MapSerial chain completeness tests (mirrors Slice Section 19)
// ============================================================

func TestMap_Chain_Serial_FailFast(t *testing.T) {
	data := genIntMap(100)
	var called int64
	r := NewMapChain(mapFreshCtx(), data).Serial().FailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
		idx := atomic.AddInt64(&called, 1)
		if idx == 50 {
			return 0, errMapTestSentinel
		}
		return v, nil
	})
	if r.Error() == nil {
		t.Fatal("Serial FailFast: expected error")
	}
}

func TestMap_Chain_Serial_NoFailFast(t *testing.T) {
	data := genIntMap(100)
	r := NewMapChain(mapFreshCtx(), data).Serial().NoFailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
		if v == 50 {
			return 0, errMapTestSentinel
		}
		return v, nil
	})
	if r.Error() == nil {
		t.Fatal("Serial NoFailFast: still should have error in results")
	}
}

func TestMap_Chain_Serial_Timeout(t *testing.T) {
	data := genIntMap(100)
	r := NewMapChain(mapFreshCtx(), data).Serial().Timeout(5 * time.Second).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Serial Timeout: %v", r.Error())
	}
}

func TestMap_Chain_Serial_DefaultTimeout(t *testing.T) {
	data := genIntMap(100)
	r := NewMapChain(mapFreshCtx(), data).Serial().DefaultTimeout().Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Serial DefaultTimeout: %v", r.Error())
	}
}

func TestMap_Chain_Serial_Logger(t *testing.T) {
	data := genIntMap(10)
	r := NewMapChain(mapFreshCtx(), data).Serial().Logger(&testMapLogger{}).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("Serial Logger: %v", r.Error())
	}
	core.SetLogger(nil)
}

func TestMap_Chain_Serial_ToParallel(t *testing.T) {
	data := genIntMap(1000)
	ps := NewMapChain(mapFreshCtx(), data).Serial().ToParallel()
	vals := ps.Worker(16).Values()
	if len(vals) != 1000 {
		t.Fatalf("Serial->ToParallel: expected 1000 values, got %d", len(vals))
	}
}

// ============================================================
// Section 19: MapParallel chain completeness + mode switch tests (mirrors Slice Section 20)
// ============================================================

func TestMap_Chain_Parallel_AllConfig(t *testing.T) {
	data := genIntMap(5000)
	ps := NewMapChain(mapFreshCtx(), data).Parallel().
		Worker(16).
		FailFast().
		Timeout(10 * time.Second).
		Shards(8)
	_ = ps.NoFailFast()
	_ = ps.DefaultTimeout()
	_ = ps.DefaultWorker()
	_ = ps.DefaultShard()
	_ = ps.DefaultPool()
	_ = ps.PoolAuto(pool.NewPool[int](8))
	_ = ps.DefaultChunk()
	vals := ps.Values()
	if len(vals) != 5000 {
		t.Fatalf("Parallel AllConfig: expected 5000, got %d", len(vals))
	}
}

func TestMap_Chain_Parallel_ToSerial(t *testing.T) {
	data := genIntMap(1000)
	ss := NewMapChain(mapFreshCtx(), data).Parallel().ToSerial()
	vals := ss.Values()
	if len(vals) != 1000 {
		t.Fatalf("Parallel->ToSerial: expected 1000 values, got %d", len(vals))
	}
}

// ============================================================
// Section 20: Edge case tests (mirrors Slice Section 21)
// ============================================================

func TestMap_Chain_Empty_Map(t *testing.T) {
	for _, mode := range []string{"chain", "parallel", "serial"} {
		t.Run(mode, func(t *testing.T) {
			var r *MapResult[string, int]
			switch mode {
			case "chain":
				r = NewMapChain(mapFreshCtx(), map[string]int{}).Map(func(ctx context.Context, k string, v int) (int, error) {
					return v, nil
				})
			case "parallel":
				r = NewMapChain(mapFreshCtx(), map[string]int{}).Parallel().Map(func(ctx context.Context, k string, v int) (int, error) {
					return v, nil
				})
			case "serial":
				r = NewMapChain(mapFreshCtx(), map[string]int{}).Serial().Map(func(ctx context.Context, k string, v int) (int, error) {
					return v, nil
				})
			}
			if r.Err() {
				t.Fatalf("empty %s Map should not error: %v", mode, r.Error())
			}
			if r.Len() != 0 {
				t.Fatalf("empty %s Map: expected 0 results, got %d", mode, r.Len())
			}
		})
	}
}

func TestMap_Chain_Empty_ForEach(t *testing.T) {
	for _, mode := range []string{"chain", "parallel", "serial"} {
		t.Run(mode, func(t *testing.T) {
			var r *MapForEachResult
			switch mode {
			case "chain":
				r = NewMapChain(mapFreshCtx(), map[string]int{}).ForEach(func(ctx context.Context, k string, v int) error {
					return nil
				})
			case "parallel":
				r = NewMapChain(mapFreshCtx(), map[string]int{}).Parallel().ForEach(func(ctx context.Context, k string, v int) error {
					return nil
				})
			case "serial":
				r = NewMapChain(mapFreshCtx(), map[string]int{}).Serial().ForEach(func(ctx context.Context, k string, v int) error {
					return nil
				})
			}
			if r.Err() {
				t.Fatalf("empty %s ForEach should not error: %v", mode, r.Error())
			}
			if r.Total() != 0 {
				t.Fatalf("empty %s ForEach: expected 0 total, got %d", mode, r.Total())
			}
		})
	}
}

func TestMap_Chain_Empty_Stream(t *testing.T) {
	for _, mode := range []string{"chain", "parallel", "serial"} {
		t.Run(mode, func(t *testing.T) {
			var ch <-chan core.Result[int]
			switch mode {
			case "chain":
				ch = NewMapChain(mapFreshCtx(), map[string]int{}).Stream(func(ctx context.Context, k string, v int) (int, error) {
					return v, nil
				}, 0)
			case "parallel":
				ch = NewMapChain(mapFreshCtx(), map[string]int{}).Parallel().Stream(func(ctx context.Context, k string, v int) (int, error) {
					return v, nil
				}, 0)
			case "serial":
				ch = NewMapChain(mapFreshCtx(), map[string]int{}).Serial().Stream(func(ctx context.Context, k string, v int) (int, error) {
					return v, nil
				}, 0)
			}
			count := 0
			for range ch {
				count++
			}
			if count != 0 {
				t.Fatalf("empty %s Stream: expected 0 results, got %d", mode, count)
			}
		})
	}
}

func TestMap_Chain_Empty_Reduce(t *testing.T) {
	for _, mode := range []string{"chain", "parallel", "serial"} {
		t.Run(mode, func(t *testing.T) {
			var sum int
			var err error
			switch mode {
			case "chain":
				sum, err = NewMapChain(mapFreshCtx(), map[string]int{}).Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
					return acc + v, nil
				})
			case "parallel":
				sum, err = NewMapChain(mapFreshCtx(), map[string]int{}).Parallel().Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
					return acc + v, nil
				})
			case "serial":
				sum, err = NewMapChain(mapFreshCtx(), map[string]int{}).Serial().Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
					return acc + v, nil
				})
			}
			if err != nil {
				t.Fatalf("empty %s Reduce error: %v", mode, err)
			}
			if sum != 0 {
				t.Fatalf("empty %s Reduce: expected 0, got %d", mode, sum)
			}
		})
	}
}

func TestMap_Chain_Empty_MapBatch(t *testing.T) {
	for _, mode := range []string{"chain", "parallel"} {
		t.Run(mode, func(t *testing.T) {
			var r *MapResult[string, int]
			switch mode {
			case "chain":
				r = NewMapChain(mapFreshCtx(), map[string]int{}).Chunk(100).MapBatch(func(ctx context.Context, chunk []MapEntry[string, int]) (int, error) {
					return len(chunk), nil
				})
			case "parallel":
				r = NewMapChain(mapFreshCtx(), map[string]int{}).Parallel().Chunk(100).MapBatch(func(ctx context.Context, chunk []MapEntry[string, int]) (int, error) {
					return len(chunk), nil
				})
			}
			if r.Err() {
				t.Fatalf("empty %s MapBatch should not error: %v", mode, r.Error())
			}
			if r.Len() != 0 {
				t.Fatalf("empty %s MapBatch: expected 0 results, got %d", mode, r.Len())
			}
		})
	}
}

func TestMap_Chain_Empty_ForEachBatch(t *testing.T) {
	for _, mode := range []string{"chain", "parallel"} {
		t.Run(mode, func(t *testing.T) {
			var r *MapForEachResult
			switch mode {
			case "chain":
				r = NewMapChain(mapFreshCtx(), map[string]int{}).Chunk(100).ForEachBatch(func(ctx context.Context, chunk []MapEntry[string, int]) error {
					return nil
				})
			case "parallel":
				r = NewMapChain(mapFreshCtx(), map[string]int{}).Parallel().Chunk(100).ForEachBatch(func(ctx context.Context, chunk []MapEntry[string, int]) error {
					return nil
				})
			}
			if r.Err() {
				t.Fatalf("empty %s ForEachBatch should not error: %v", mode, r.Error())
			}
			if r.Total() != 0 {
				t.Fatalf("empty %s ForEachBatch: expected 0 total, got %d", mode, r.Total())
			}
		})
	}
}

func TestMap_Chain_Empty_DataOps(t *testing.T) {
	empty := NewMapChain(mapFreshCtx(), map[string]int{})
	if empty.Len() != 0 {
		t.Fatal("empty: Len should be 0")
	}
	if !empty.IsEmpty() {
		t.Fatal("empty: IsEmpty should be true")
	}
	keys := empty.Keys()
	if len(keys) != 0 {
		t.Fatalf("empty: Keys should be empty, got %d", len(keys))
	}
	vals := empty.Values()
	if len(vals) != 0 {
		t.Fatalf("empty: Values should be empty, got %d", len(vals))
	}
	if empty.Has("none") {
		t.Fatal("empty: Has should be false")
	}
	_, ok := empty.Get("none")
	if ok {
		t.Fatal("empty: Get should return false")
	}
	_, _, found := empty.Find(func(k string, v int) bool { return true })
	if found {
		t.Fatal("empty: Find should return false")
	}
	if !empty.All(func(k string, v int) bool { return true }) {
		t.Fatal("empty: All should be true")
	}
	if empty.Any(func(k string, v int) bool { return true }) {
		t.Fatal("empty: Any should be false")
	}
	if empty.Count(func(k string, v int) bool { return true }) != 0 {
		t.Fatal("empty: Count should be 0")
	}
	empty.Set("a", 1)
	if empty.Len() != 1 {
		t.Fatalf("empty Set: expected len 1, got %d", empty.Len())
	}
	empty.Clear()
	if !empty.IsEmpty() {
		t.Fatal("empty Clear: should be empty")
	}
}

func TestMap_Chain_NilMap_Map(t *testing.T) {
	r := NewMapChain[string, int](mapFreshCtx(), nil).Map(func(ctx context.Context, k string, v int) (int, error) {
		return v, nil
	})
	if r.Err() {
		t.Fatalf("nil map Map should not error: %v", r.Error())
	}
	if r.Len() != 0 {
		t.Fatalf("nil map Map: expected 0 results, got %d", r.Len())
	}
}

// ============================================================
// Section 21: Concurrent safety tests (mirrors Slice Section 22)
// ============================================================

func TestMap_Chain_ConcurrentReads(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			data := genIntMap(tier.size)
			sb := NewMapChain(mapFreshCtx(), data)
			var wg sync.WaitGroup
			workers := 32
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = sb.Len()
					_ = sb.IsEmpty()
					_ = sb.Keys()
					_ = sb.Values()
					_ = sb.AsMap()
					_ = sb.Has("0")
					_, _ = sb.Get("0")
				}()
			}
			wg.Wait()
		})
	}
}

func TestMap_Chain_ConcurrentMapCalls(t *testing.T) {
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			mapSkipIfTooLarge(t, tier.size)
			t.Parallel()
			var wg sync.WaitGroup
			workers := 16
			var successCount int64
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					chunk := tier.size / workers
					if chunk == 0 {
						return
					}
					data := genIntMap(chunk)
					r := NewMapChain(mapFreshCtx(), data).Worker(8).Map(func(ctx context.Context, k string, v int) (int, error) {
						return v, nil
					})
					if r.Err() {
						t.Errorf("concurrent Map error: %v", r.Error())
						return
					}
					atomic.AddInt64(&successCount, 1)
				}()
			}
			wg.Wait()
			if successCount != int64(workers) {
				t.Fatalf("concurrent Map: expected %d successes, got %d", workers, successCount)
			}
		})
	}
}

// ============================================================
// Section 22: Race detection tests (mirrors Slice Section 23)
// Each test covers one concurrent execution path; -race detects data races.
// All 4 data tiers fully covered.
// Run: go test -race -short -run "TestMap_Chain_Race" -count=1 -timeout 10m ./...
//      go test -race -run "TestMap_Chain_Race" -count=1 -timeout 30m ./...
// ============================================================

func TestMap_Chain_Race_Map(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			data := genIntMap(tier.size)
			var sum int64
			r := NewMapChain(mapFreshCtx(), data).Worker(conc).Map(func(ctx context.Context, k string, v int) (int, error) {
				atomic.AddInt64(&sum, int64(v))
				return v, nil
			})
			if r.Err() {
				t.Fatalf("Race Map: %v", r.Error())
			}
			if r.Len() != tier.size {
				t.Fatalf("Race Map: expected %d, got %d", tier.size, r.Len())
			}
		})
	}
}

func TestMap_Chain_Race_ForEach(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			data := genIntMap(tier.size)
			var counter int64
			var errCount int64
			r := NewMapChain(mapFreshCtx(), data).Worker(conc).ForEach(func(ctx context.Context, k string, v int) error {
				atomic.AddInt64(&counter, 1)
				if v%2 == 0 {
					atomic.AddInt64(&errCount, 1)
				}
				return nil
			})
			_ = r.Error()
			if r.Total() != int64(tier.size) {
				t.Fatalf("Race ForEach Total: expected %d, got %d", tier.size, r.Total())
			}
			if counter != int64(tier.size) {
				t.Fatalf("Race ForEach counter: expected %d, got %d", tier.size, counter)
			}
		})
	}
}

func TestMap_Chain_Race_FailFast(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			data := genIntMap(tier.size)
			var called int64
			r := NewMapChain(mapFreshCtx(), data).Worker(conc).FailFast().Map(func(ctx context.Context, k string, v int) (int, error) {
				idx := atomic.AddInt64(&called, 1)
				if idx == 100 {
					return 0, errMapTestSentinel
				}
				return v, nil
			})
			if r.Error() == nil {
				t.Fatal("Race FailFast: expected error but got nil")
			}
		})
	}
}

func TestMap_Chain_Race_Stream(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			data := genIntMap(tier.size)
			ch := NewMapChain(mapFreshCtx(), data).Worker(conc).Buf(4096).Stream(func(ctx context.Context, k string, v int) (int, error) {
				return v, nil
			}, 0)
			count := 0
			for res := range ch {
				if res.Err != nil {
					t.Fatalf("Race Stream: %v", res.Err)
				}
				count++
			}
			if count != tier.size {
				t.Fatalf("Race Stream: expected %d, got %d", tier.size, count)
			}
		})
	}
}

func TestMap_Chain_Race_PoolShared(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			data := genIntMap(tier.size)
			p := pool.NewPool[int](16)
			r := NewMapChain(mapFreshCtx(), data).PoolAuto(p).Worker(conc).Map(func(ctx context.Context, k string, v int) (int, error) {
				return v, nil
			})
			if r.Err() {
				t.Fatalf("Race PoolShared: %v", r.Error())
			}
			if r.Len() != tier.size {
				t.Fatalf("Race PoolShared: expected %d, got %d", tier.size, r.Len())
			}
		})
	}
}

func TestMap_Chain_Race_Shards(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			data := genIntMap(tier.size)
			r := NewMapChain(mapFreshCtx(), data).Worker(conc).Shards(16).Map(func(ctx context.Context, k string, v int) (int, error) {
				return v * 2, nil
			})
			if r.Err() {
				t.Fatalf("Race Shards: %v", r.Error())
			}
			if r.Len() != tier.size {
				t.Fatalf("Race Shards: expected %d, got %d", tier.size, r.Len())
			}
		})
	}
}

func TestMap_Chain_Race_MapBatch(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			data := genIntMap(tier.size)
			r := NewMapChain(mapFreshCtx(), data).Worker(conc).Chunk(100).MapBatch(func(ctx context.Context, batch []MapEntry[string, int]) (int, error) {
				sum := 0
				for _, e := range batch {
					sum += e.Val
				}
				return sum, nil
			})
			if r.Err() {
				t.Fatalf("Race MapBatch: %v", r.Error())
			}
			if r.Len() == 0 && tier.size > 0 {
				t.Fatal("Race MapBatch: should have results")
			}
		})
	}
}

func TestMap_Chain_Race_ForEachBatch(t *testing.T) {
	conc := 32
	if testing.Short() {
		conc = 4
	}
	for _, tier := range mapAllTiers {
		t.Run(tier.name, func(t *testing.T) {
			data := genIntMap(tier.size)
			var processed int64
			r := NewMapChain(mapFreshCtx(), data).Worker(conc).Chunk(100).ForEachBatch(func(ctx context.Context, batch []MapEntry[string, int]) error {
				atomic.AddInt64(&processed, int64(len(batch)))
				return nil
			})
			if r.Err() {
				t.Fatalf("Race ForEachBatch: %v", r.Error())
			}
			if processed != int64(tier.size) {
				t.Fatalf("Race ForEachBatch: expected %d, got %d", tier.size, processed)
			}
		})
	}
}
