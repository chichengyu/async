package sliceops

import (
	"cmp"
	"context"
	"slices"
	"testing"
)

type stableQueryRecord struct {
	key, position int
}

type stableQuerySlice[S any] interface {
	StableSort(func(stableQueryRecord, stableQueryRecord) int) S
	Max(func(stableQueryRecord, stableQueryRecord) int) (stableQueryRecord, bool)
	Min(func(stableQueryRecord, stableQueryRecord) int) (stableQueryRecord, bool)
	Find(func(stableQueryRecord) bool) (stableQueryRecord, bool)
	FindLast(func(stableQueryRecord) bool) (stableQueryRecord, bool)
	Index(func(stableQueryRecord) bool) int
	Items() []stableQueryRecord
}

type stableQuerySubject struct {
	sort       func(func(stableQueryRecord, stableQueryRecord) int)
	max, min   func(func(stableQueryRecord, stableQueryRecord) int) (stableQueryRecord, bool)
	find, last func(func(stableQueryRecord) bool) (stableQueryRecord, bool)
	index      func(func(stableQueryRecord) bool) int
	items      func() []stableQueryRecord
}

func stableQueryAdapter[S stableQuerySlice[S]](subject S) stableQuerySubject {
	return stableQuerySubject{
		sort: func(compare func(stableQueryRecord, stableQueryRecord) int) { subject.StableSort(compare) },
		max:  subject.Max, min: subject.Min, find: subject.Find, last: subject.FindLast,
		index: subject.Index, items: subject.Items,
	}
}

// Preserve original positions in an independent per-key bucket oracle; integer
// sorting alone cannot distinguish an equal-key reorder or a later tied maximum.
func FuzzSliceStableRepresentativesAndQueries(f *testing.F) {
	f.Add([]byte{}, uint8(0), false)
	f.Add([]byte{3, 1, 3, 0, 1, 3, 7, 7}, uint8(3), false)
	f.Add([]byte{7, 0, 7, 0, 4}, uint8(15), true)
	f.Fuzz(func(t *testing.T, data []byte, query uint8, descending bool) {
		if len(data) > 256 {
			data = data[:256]
		}
		input := make([]stableQueryRecord, len(data))
		var buckets [8][]stableQueryRecord
		for i, b := range data {
			input[i] = stableQueryRecord{key: int(b % 8), position: i + 1}
			buckets[b%8] = append(buckets[b%8], input[i])
		}
		var want []stableQueryRecord
		for rank := 0; rank < 8; rank++ {
			key := rank
			if descending {
				key = 7 - rank
			}
			want = append(want, buckets[key]...)
		}
		compare := func(a, b stableQueryRecord) int {
			if descending {
				return cmp.Compare(b.key, a.key)
			}
			return cmp.Compare(a.key, b.key)
		}
		ctx := context.Background()
		legacy := New(slices.Clone(input))
		subjects := []stableQuerySubject{
			stableQueryAdapter(NewSliceBuilder(ctx, slices.Clone(input))),
			stableQueryAdapter(NewSliceBuilder(ctx, slices.Clone(input)).Serial()),
			stableQueryAdapter(NewSliceBuilder(ctx, slices.Clone(input)).Parallel()),
			{sort: func(compare func(stableQueryRecord, stableQueryRecord) int) { legacy.StableSortFunc(compare) },
				max: legacy.MaxFunc, min: legacy.MinFunc, find: legacy.FindFunc, last: legacy.FindLastFunc,
				index: legacy.IndexFunc, items: legacy.Items},
		}
		for engine, s := range subjects {
			var minimum, maximum stableQueryRecord
			if len(want) > 0 {
				minimum = want[0]
				// The first original representative at the maximal key, not the
				// last entry in that bucket, is the documented maximum.
				maximum = want[len(want)-1]
				for _, v := range input {
					if v.key == maximum.key {
						maximum = v
						break
					}
				}
			}
			if got, ok := s.min(compare); got != minimum || ok != (len(input) > 0) {
				t.Fatalf("engine=%d minimum=%v/%v want=%v", engine, got, ok, minimum)
			}
			if got, ok := s.max(compare); got != maximum || ok != (len(input) > 0) {
				t.Fatalf("engine=%d maximum=%v/%v want=%v", engine, got, ok, maximum)
			}
			key := int(query % 10) // 8 and 9 exercise a missing key.
			first, last := -1, -1
			for i, v := range input {
				if v.key == key {
					if first < 0 {
						first = i
					}
					last = i
				}
			}
			for direction, find := range []func(func(stableQueryRecord) bool) (stableQueryRecord, bool){s.find, s.last} {
				var calls []int
				got, ok := find(func(v stableQueryRecord) bool { calls = append(calls, v.position); return v.key == key })
				index := first
				if direction == 1 {
					index = last
				}
				var expected stableQueryRecord
				if index >= 0 {
					expected = input[index]
				}
				if got != expected || ok != (index >= 0) {
					t.Fatalf("engine=%d direction=%d find=%v/%v want=%v", engine, direction, got, ok, expected)
				}
				wantCalls := len(input)
				if index >= 0 {
					if direction == 0 {
						wantCalls = index + 1
					} else {
						wantCalls = len(input) - index
					}
				}
				if len(calls) != wantCalls {
					t.Fatalf("engine=%d direction=%d predicate count=%d want=%d", engine, direction, len(calls), wantCalls)
				}
				for i, position := range calls {
					wantPosition := i + 1
					if direction == 1 {
						wantPosition = len(input) - i
					}
					if position != wantPosition {
						t.Fatalf("engine=%d direction=%d predicate order=%v", engine, direction, calls)
					}
				}
			}
			calls := 0
			if got := s.index(func(v stableQueryRecord) bool { calls++; return v.key == key }); got != first {
				t.Fatalf("engine=%d index=%d want=%d", engine, got, first)
			}
			wantCalls := len(input)
			if first >= 0 {
				wantCalls = first + 1
			}
			if calls != wantCalls {
				t.Fatalf("engine=%d index predicate count=%d want=%d", engine, calls, wantCalls)
			}
			s.sort(compare)
			if !slices.Equal(s.items(), want) {
				t.Fatalf("engine=%d stable order=%v want=%v", engine, s.items(), want)
			}
		}
	})
}
