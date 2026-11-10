package sliceops

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
)

func TestSliceCompactNilPredicateBoundary(t *testing.T) {
	for engine := range 4 {
		for size := range 3 {
			t.Run(fmt.Sprintf("%d/%d", engine, size), func(t *testing.T) {
				data := []int{1, 2}[:size]
				var compact func()
				switch engine {
				case 0:
					s := NewSliceBuilder(context.Background(), data)
					compact = func() { s.Compact(nil) }
				case 1:
					s := NewSliceBuilder(context.Background(), data).Serial()
					compact = func() { s.Compact(nil) }
				case 2:
					s := NewSliceBuilder(context.Background(), data).Parallel()
					compact = func() { s.Compact(nil) }
				default:
					s := New(data)
					compact = func() { s.CompactFunc(nil) }
				}
				panicked := false
				func() { defer func() { panicked = recover() != nil }(); compact() }()
				if panicked != (size >= 2) {
					t.Fatalf("size=%d panic=%v", size, panicked)
				}
			})
		}
	}
}

type floatSliceContract[S any] interface {
	Sort(func(float64, float64) int) S
	StableSort(func(float64, float64) int) S
	IsSorted(func(float64, float64) int) bool
	BinarySearch(float64, func(float64, float64) int) (int, bool)
	Min(func(float64, float64) int) (float64, bool)
	Max(func(float64, float64) int) (float64, bool)
	Dedup(func(float64, float64) bool) S
	Compact(func(float64, float64) bool) S
	Items() []float64
}

type floatSliceSubject struct {
	sort, stable   func(func(float64, float64) int)
	ordered        func(func(float64, float64) int) bool
	search         func(float64, func(float64, float64) int) (int, bool)
	min, max       func(func(float64, float64) int) (float64, bool)
	dedup, compact func(func(float64, float64) bool)
	items          func() []float64
}

func floatSliceAdapter[S floatSliceContract[S]](s S) floatSliceSubject {
	return floatSliceSubject{
		sort:    func(c func(float64, float64) int) { s.Sort(c) },
		stable:  func(c func(float64, float64) int) { s.StableSort(c) },
		ordered: s.IsSorted, search: s.BinarySearch, min: s.Min, max: s.Max,
		dedup:   func(eq func(float64, float64) bool) { s.Dedup(eq) },
		compact: func(eq func(float64, float64) bool) { s.Compact(eq) }, items: s.Items,
	}
}

// Rank buckets, independent of the production comparator and sorting helpers,
// model NaN equivalence, signed zero ties and first representatives by bits.
func FuzzSliceFloatOrderingAndEquality(f *testing.F) {
	f.Add([]byte{}, false)
	f.Add([]byte{0, 1, 7, 6, 0, 1, 2, 11, 3, 10, 4, 9, 5, 8}, false)
	f.Add([]byte{7, 6, 1, 0, 7, 6, 11, 2}, true)
	f.Fuzz(func(t *testing.T, input []byte, descending bool) {
		if len(input) > 64 {
			input = input[:64]
		}
		catalog := []float64{math.Float64frombits(0x7ff8000000000042), math.Float64frombits(0xfff8000000000011), math.Inf(-1), -math.MaxFloat64, -1, -math.SmallestNonzeroFloat64, math.Copysign(0, -1), 0, math.SmallestNonzeroFloat64, 1, math.MaxFloat64, math.Inf(1)}
		ranks := []int{0, 0, 1, 2, 3, 4, 5, 5, 6, 7, 8, 9}
		rankOf := make(map[uint64]int)
		for i, value := range catalog {
			rankOf[math.Float64bits(value)] = ranks[i]
		}
		var values []float64
		var buckets [10][]float64
		for _, b := range input {
			i := int(b) % len(catalog)
			values = append(values, catalog[i])
			buckets[ranks[i]] = append(buckets[ranks[i]], catalog[i])
		}
		var want []float64
		for i := range 10 {
			rank := i
			if descending {
				rank = 9 - i
			}
			want = append(want, buckets[rank]...)
		}
		compare := cmp.Compare[float64]
		if descending {
			compare = func(a, b float64) int { return cmp.Compare(b, a) }
		}
		bits := func(values []float64) []uint64 {
			out := make([]uint64, len(values))
			for i, v := range values {
				out[i] = math.Float64bits(v)
			}
			return out
		}
		for engine := range 4 {
			newSubject := func() floatSliceSubject {
				ctx := context.Background()
				data := slices.Clone(values)
				switch engine {
				case 0:
					return floatSliceAdapter(NewSliceBuilder(ctx, data))
				case 1:
					return floatSliceAdapter(NewSliceBuilder(ctx, data).Serial())
				case 2:
					return floatSliceAdapter(NewSliceBuilder(ctx, data).Parallel())
				default:
					s := New(data)
					return floatSliceSubject{sort: func(c func(float64, float64) int) { s.SortFunc(c) }, stable: func(c func(float64, float64) int) { s.StableSortFunc(c) }, ordered: s.IsSortedFunc, search: s.BinarySearchFunc, min: s.MinFunc, max: s.MaxFunc, dedup: func(eq func(float64, float64) bool) { s.DedupFunc(eq) }, compact: func(eq func(float64, float64) bool) { s.CompactFunc(eq) }, items: s.Items}
				}
			}
			s := newSubject()
			wantOrdered := true
			for i := 1; i < len(values); i++ {
				a, b := rankOf[math.Float64bits(values[i-1])], rankOf[math.Float64bits(values[i])]
				if (!descending && a > b) || (descending && a < b) {
					wantOrdered = false
				}
			}
			if s.ordered(compare) != wantOrdered {
				t.Fatalf("engine=%d input sortedness", engine)
			}
			for which, extrema := range []func(func(float64, float64) int) (float64, bool){s.min, s.max} {
				got, ok := extrema(compare)
				if ok != (len(values) > 0) {
					t.Fatalf("engine=%d extrema presence", engine)
				}
				if len(want) > 0 {
					rank := rankOf[math.Float64bits(want[0])]
					if which == 1 {
						rank = rankOf[math.Float64bits(want[len(want)-1])]
					}
					if math.Float64bits(got) != math.Float64bits(buckets[rank][0]) {
						t.Fatalf("engine=%d extrema representative", engine)
					}
				} else if math.Float64bits(got) != 0 {
					t.Fatal("empty extrema not zero")
				}
			}
			for _, stable := range []bool{false, true} {
				s = newSubject()
				if stable {
					s.stable(compare)
				} else {
					s.sort(compare)
				}
				got := s.items()
				if !s.ordered(compare) || len(got) != len(want) {
					t.Fatalf("engine=%d ordered/length", engine)
				}
				for i, v := range got {
					if rankOf[math.Float64bits(v)] != rankOf[math.Float64bits(want[i])] {
						t.Fatalf("engine=%d rank order", engine)
					}
				}
				a, b := bits(got), bits(want)
				if !stable {
					slices.Sort(a)
					slices.Sort(b)
				}
				if !slices.Equal(a, b) {
					t.Fatalf("engine=%d stable=%v changed values/order", engine, stable)
				}
				for i, target := range catalog {
					position := 0
					for position < len(want) {
						rank := rankOf[math.Float64bits(want[position])]
						if (!descending && rank >= ranks[i]) || (descending && rank <= ranks[i]) {
							break
						}
						position++
					}
					found := position < len(want) && rankOf[math.Float64bits(want[position])] == ranks[i]
					if p, ok := s.search(target, compare); p != position || ok != found {
						t.Fatalf("engine=%d search=%d/%v want=%d/%v", engine, p, ok, position, found)
					}
				}
			}
			for mode := range 3 {
				var eq func(float64, float64) bool
				if mode == 1 {
					eq = func(a, b float64) bool { return a == b }
				}
				if mode == 2 {
					eq = func(a, b float64) bool { return cmp.Compare(a, b) == 0 }
				}
				for _, compact := range []bool{false, true} {
					if mode == 0 && compact {
						continue
					}
					var expected []float64
					seen := make(map[int]bool)
					previous := -1
					for _, v := range values {
						rank := rankOf[math.Float64bits(v)]
						duplicate := seen[rank]
						if compact {
							duplicate = rank == previous
						}
						if rank == 0 && mode != 2 {
							duplicate = false
						}
						if !duplicate {
							expected = append(expected, v)
						}
						seen[rank] = true
						previous = rank
					}
					s = newSubject()
					if compact {
						s.compact(eq)
					} else {
						s.dedup(eq)
					}
					if !slices.Equal(bits(s.items()), bits(expected)) {
						t.Fatalf("engine=%d mode=%d compact=%v equality representatives", engine, mode, compact)
					}
				}
			}
		}
	})
}
