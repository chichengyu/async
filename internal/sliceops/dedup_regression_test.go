package sliceops

import (
	"context"
	"slices"
	"strings"
	"testing"
)

func TestDedupHonorsComparatorAcrossBuilders(t *testing.T) {
	for _, mode := range []string{"builder", "serial", "parallel", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			input := []string{"Alpha", "beta", "ALPHA", "BETA", "gamma"}
			ctx := context.Background()
			var got []string
			switch mode {
			case "builder":
				got = NewSliceBuilder(ctx, input).Dedup(strings.EqualFold).Values()
			case "serial":
				got = NewSliceBuilder(ctx, input).Serial().Dedup(strings.EqualFold).Values()
			case "parallel":
				got = NewSliceBuilder(ctx, input).Parallel().Dedup(strings.EqualFold).Values()
			case "legacy":
				got = New(input).DedupFunc(strings.EqualFold).Values()
			}
			if want := []string{"Alpha", "beta", "gamma"}; !slices.Equal(got, want) {
				t.Fatalf("got %v, want first representative of each equality class %v", got, want)
			}
		})
	}
}

func TestDedupSupportsNonComparableElements(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		input := [][]int{{1, 2}, {3}, {1, 2}, {3}, {4}}
		var got [][]int
		if legacy {
			got = New(input).DedupFunc(slices.Equal[[]int]).Values()
		} else {
			got = NewSliceBuilder(context.Background(), input).Dedup(slices.Equal[[]int]).Values()
		}
		if want := [][]int{{1, 2}, {3}, {4}}; !slices.EqualFunc(got, want, slices.Equal[[]int]) {
			t.Fatalf("legacy=%v: got %v, want %v", legacy, got, want)
		}
	}
}
