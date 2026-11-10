package sliceops

import (
	"context"
	"slices"
	"testing"
)

type mutableSlice[S any] interface {
	Append(...int) S
	Prepend(...int) S
	Insert(int, ...int) S
	DeleteRange(int, int) S
	Replace(int, int, ...int) S
	Take(int) S
	Drop(int) S
	SliceRange(int, int) S
	Reverse() S
	Repeat(int) S
	Clip() S
	Grow(int) S
	Filter(func(int) bool) S
	Items() []int
	Values() []int
	Len() int
	IsEmpty() bool
	First() (int, bool)
	Last() (int, bool)
}

// Rebuild the oracle from logical intervals rather than using in-place slice
// mutation helpers. Run identical mixed operations through four public forms.
func FuzzSliceMutationState(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
	f.Add([]byte{255, 0, 127, 13, 255, 1, 2, 9})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 256 {
			data = data[:256]
		}
		ctx := context.Background()
		checkSliceMutation(t, NewSliceBuilder(ctx, []int{1, 2, 3}), data)
		checkSliceMutation(t, NewSliceBuilder(ctx, []int{1, 2, 3}).Serial(), data)
		checkSliceMutation(t, NewSliceBuilder(ctx, []int{1, 2, 3}).Parallel(), data)
		checkSliceMutation(t, New([]int{1, 2, 3}), data)
	})
}

func checkSliceMutation[S mutableSlice[S]](t *testing.T, subject S, data []byte) {
	t.Helper()
	want := []int{1, 2, 3}
	for step, operation := range data {
		value := int(int8(operation))
		n := len(want)
		i := (int(operation)/14)%(n+3) - 1
		j := (int(operation)+step)%(n+3) - 1
		if i > j {
			i, j = j, i
		}
		copyRange := func(begin, end int) []int {
			var out []int
			for k, v := range want {
				if k >= begin && k < end {
					out = append(out, v)
				}
			}
			return out
		}
		switch operation % 14 {
		case 0:
			subject.Append(value)
			want = append(copyRange(0, n), value)
		case 1:
			subject.Prepend(value)
			want = append([]int{value}, copyRange(0, n)...)
		case 2:
			subject.Insert(i, value)
			index := min(max(i, 0), n)
			out := copyRange(0, index)
			out = append(out, value)
			want = append(out, copyRange(index, n)...)
		case 3:
			subject.DeleteRange(i, j)
			begin, end := max(i, 0), min(j, n)
			if begin < end {
				want = append(copyRange(0, begin), copyRange(end, n)...)
			}
		case 4:
			// Replace has a stricter remaining-boundary contract than DeleteRange.
			begin, end := min(max(i, 0), n), min(max(j, 0), n)
			subject.Replace(begin, end, value)
			out := append(copyRange(0, begin), value)
			want = append(out, copyRange(end, n)...)
		case 5:
			subject.Take(i)
			want = copyRange(0, max(i, 0))
		case 6:
			subject.Drop(i)
			want = copyRange(max(i, 0), n)
		case 7:
			subject.SliceRange(i, j)
			want = copyRange(max(i, 0), min(j, n))
		case 8:
			subject.Reverse()
			var out []int
			for k := n - 1; k >= 0; k-- {
				out = append(out, want[k])
			}
			want = out
		case 9:
			count := int(operation/14)%4 - 1
			if n > 128 {
				count = 0
			}
			subject.Repeat(count)
			var out []int
			for repeat := 0; repeat < count; repeat++ {
				out = append(out, want...)
			}
			want = out
		case 10:
			subject.Clip()
			if cap(subject.Items()) != n {
				t.Fatalf("step %d: Clip cap=%d, length=%d", step, cap(subject.Items()), n)
			}
		case 11:
			extra := int(operation/14) % 8
			subject.Grow(extra)
			if cap(subject.Items()) < n+extra {
				t.Fatalf("step %d: Grow cap=%d, need %d", step, cap(subject.Items()), n+extra)
			}
		case 12:
			subject.Filter(func(value int) bool { return value%2 == 0 })
			var out []int
			for _, value := range want {
				if value%2 == 0 {
					out = append(out, value)
				}
			}
			want = out
		case 13:
			// Values must copy element slots, even when the input has spare capacity.
			copy := subject.Values()
			if len(copy) > 0 {
				copy[0]++
			}
		}
		if !slices.Equal(subject.Items(), want) || subject.Len() != len(want) || subject.IsEmpty() != (len(want) == 0) {
			t.Fatalf("step %d operation %d: got=%v want=%v", step, operation%14, subject.Items(), want)
		}
		first, firstOK := subject.First()
		last, lastOK := subject.Last()
		wantFirst, wantLast := 0, 0
		if len(want) > 0 {
			wantFirst, wantLast = want[0], want[len(want)-1]
		}
		if first != wantFirst || last != wantLast || firstOK != (len(want) > 0) || lastOK != firstOK {
			t.Fatalf("step %d: invalid First/Last for %v", step, want)
		}
	}
}
