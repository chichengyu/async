package sliceops

import (
	"context"
	"slices"
	"testing"
)

// Compare against a keyed equivalence-class oracle instead of reproducing the
// comparator scan. Verify both entry points, stable representatives and nil eq.
func FuzzDedupEquivalenceClasses(f *testing.F) {
	f.Add([]byte{1, 17, 2, 18, 3, 1})
	f.Add([]byte{})
	f.Add([]byte{255, 0, 15, 16})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 512 {
			data = data[:512]
		}
		var seen [16]bool
		want := make([]byte, 0)
		for _, b := range data {
			if !seen[b%16] {
				seen[b%16] = true
				want = append(want, b)
			}
		}
		eq := func(a, b byte) bool { return a%16 == b%16 }
		got := NewSliceBuilder(context.Background(), slices.Clone(data)).Dedup(eq).Values()
		legacy := New(slices.Clone(data)).DedupFunc(eq).Values()
		if !slices.Equal(got, want) || !slices.Equal(legacy, want) {
			t.Fatalf("input=%v: builder=%v legacy=%v want=%v", data, got, legacy, want)
		}
		nilGot := New(slices.Clone(data)).DedupFunc(nil).Values()
		slices.Sort(nilGot)
		nilWant := slices.Clone(data)
		slices.Sort(nilWant)
		nilWant = slices.Compact(nilWant)
		if !slices.Equal(nilGot, nilWant) {
			t.Fatalf("nil comparator: got=%v want=%v", nilGot, nilWant)
		}
	})
}
