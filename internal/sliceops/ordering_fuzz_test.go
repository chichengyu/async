package sliceops

import (
	"cmp"
	"context"
	"slices"
	"testing"
)

type orderedSlice[S any] interface {
	Sort(func(int, int) int) S
	IsSorted(func(int, int) int) bool
	BinarySearch(int, func(int, int) int) (int, bool)
	Max(func(int, int) int) (int, bool)
	Min(func(int, int) int) (int, bool)
	Items() []int
}

// An independent frequency histogram is the sorting oracle; a linear scan is
// the insertion-point oracle. Include machine integer extremes without a-b.
func FuzzSliceOrderingAndSearch(f *testing.F) {
	f.Add([]byte{}, int64(0), false)
	f.Add([]byte{0, 1, 255, 1, 127}, int64(-1), true)
	f.Add([]byte{2, 2, 2}, int64(127), false)
	f.Fuzz(func(t *testing.T, data []byte, target int64, descending bool) {
		if len(data) > 512 {
			data = data[:512]
		}
		var frequencies [256]int
		input := make([]int, 0, len(data)+2)
		for _, value := range data {
			frequencies[value]++
			input = append(input, int(value)-128)
		}
		maxInt := int(^uint(0) >> 1)
		minInt := -maxInt - 1
		if len(data) > 0 {
			input = append(input, maxInt, minInt)
		}
		want := make([]int, 0, len(input))
		if len(data) > 0 {
			want = append(want, minInt)
		}
		for value, count := range frequencies {
			for range count {
				want = append(want, value-128)
			}
		}
		if len(data) > 0 {
			want = append(want, maxInt)
		}
		compare := cmp.Compare[int]
		if descending {
			slices.Reverse(want)
			compare = func(a, b int) int { return cmp.Compare(b, a) }
		}
		ctx := context.Background()
		checkOrdering(t, NewSliceBuilder(ctx, slices.Clone(input)), want, int(target), compare)
		checkOrdering(t, NewSliceBuilder(ctx, slices.Clone(input)).Parallel(), want, int(target), compare)
		checkOrdering(t, NewSliceBuilder(ctx, slices.Clone(input)).Serial(), want, int(target), compare)
		legacy := New(slices.Clone(input)).SortFunc(compare)
		if !legacy.IsSortedFunc(compare) || !slices.Equal(legacy.Items(), want) {
			t.Fatal("legacy sorting disagrees with frequency oracle")
		}
		position, found := legacy.BinarySearchFunc(int(target), compare)
		checkInsertion(t, want, int(target), compare, position, found)
	})
}

func checkOrdering[S orderedSlice[S]](t *testing.T, subject S, want []int, target int, compare func(int, int) int) {
	t.Helper()
	subject.Sort(compare)
	if !subject.IsSorted(compare) || !slices.Equal(subject.Items(), want) {
		t.Fatal("sorting disagrees with frequency oracle")
	}
	position, found := subject.BinarySearch(target, compare)
	checkInsertion(t, want, target, compare, position, found)
	minimum, hasMin := subject.Min(compare)
	maximum, hasMax := subject.Max(compare)
	if len(want) == 0 {
		if minimum != 0 || maximum != 0 || hasMin || hasMax {
			t.Fatal("empty extrema must be zero and absent")
		}
	} else if !hasMin || !hasMax || minimum != want[0] || maximum != want[len(want)-1] {
		t.Fatal("extrema disagree with order")
	}
}

func checkInsertion(t *testing.T, sorted []int, target int, compare func(int, int) int, position int, found bool) {
	t.Helper()
	wantPosition := len(sorted)
	for i, value := range sorted {
		if compare(value, target) >= 0 {
			wantPosition = i
			break
		}
	}
	wantFound := wantPosition < len(sorted) && compare(sorted[wantPosition], target) == 0
	if position != wantPosition || found != wantFound {
		t.Fatalf("search(%d)=(%d,%v), want (%d,%v)", target, position, found, wantPosition, wantFound)
	}
}
