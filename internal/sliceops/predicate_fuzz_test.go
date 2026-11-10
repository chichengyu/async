package sliceops

import (
	"context"
	"slices"
	"testing"
)

type predicateSlice[S any] interface {
	Filter(func(int) bool) S
	DeleteFunc(func(int) bool) S
	Contains(func(int) bool) bool
	All(func(int) bool) bool
	Any(func(int) bool) bool
	Count(func(int) bool) int
	Items() []int
}

// Exercise both threshold boundaries with pure predicates and compare stable
// filtering against an independent sequential oracle. Input slots are unique.
func FuzzSlicePredicateStrategies(f *testing.F) {
	for _, length := range []uint16{0, 1, 99, 100, 101, 999, 1000, 1001, 2048} {
		f.Add(length, uint8(7), uint8(3), uint8(9))
	}
	f.Fuzz(func(t *testing.T, rawLength uint16, workers, modulus, residue uint8) {
		length := int(rawLength % 2049)
		divisor := 1 + int(modulus%31)
		match := int(residue % uint8(divisor))
		predicate := func(value int) bool { return (value%divisor+divisor)%divisor == match }
		input := make([]int, length)
		var kept, removed []int
		for i := range input {
			input[i] = i - length/2
			if predicate(input[i]) {
				kept = append(kept, input[i])
			} else {
				removed = append(removed, input[i])
			}
		}
		ctx := context.Background()
		count := 1 + int(workers%17)
		checkPredicateStrategy(t, func() *SliceBuilder[int, int] { return NewSliceBuilder(ctx, slices.Clone(input)).Worker(count) }, kept, removed, predicate)
		checkPredicateStrategy(t, func() *ParallelSlice[int, int] {
			return NewSliceBuilder(ctx, slices.Clone(input)).Parallel().Worker(count)
		}, kept, removed, predicate)
		checkPredicateStrategy(t, func() *SerialSlice[int, int] { return NewSliceBuilder(ctx, slices.Clone(input)).Serial() }, kept, removed, predicate)
	})
}

func checkPredicateStrategy[S predicateSlice[S]](t *testing.T, makeSubject func() S, kept, removed []int, predicate func(int) bool) {
	t.Helper()
	subject := makeSubject()
	input := slices.Clone(subject.Items())
	if got := subject.Count(predicate); got != len(kept) {
		t.Fatalf("Count=%d want=%d length=%d", got, len(kept), len(input))
	}
	if subject.Contains(predicate) != (len(kept) > 0) || subject.Any(predicate) != (len(kept) > 0) || subject.All(predicate) != (len(removed) == 0) {
		t.Fatalf("predicate result inconsistent: length=%d kept=%d", len(input), len(kept))
	}
	if !slices.Equal(subject.Items(), input) {
		t.Fatal("query changed input")
	}
	subject.Filter(predicate)
	if !slices.Equal(subject.Items(), kept) {
		t.Fatalf("Filter changed stable order: got=%v want=%v", subject.Items(), kept)
	}
	subject = makeSubject()
	subject.DeleteFunc(predicate)
	if !slices.Equal(subject.Items(), removed) {
		t.Fatalf("DeleteFunc changed stable order: got=%v want=%v", subject.Items(), removed)
	}
}
