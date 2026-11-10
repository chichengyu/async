package sliceops

import (
	"context"
	"fmt"
	"slices"
	"testing"
)

type boundedView[S any] interface {
	Take(int) S
	Drop(int) S
	SliceRange(int, int) S
	Items() []int
}

func TestSliceViewExtremeBoundsPreserveArrayAndHeader(t *testing.T) {
	ctx := context.Background()
	checkViewBounds(t, "builder", func(input []int) *SliceBuilder[int, int] { return NewSliceBuilder(ctx, input) })
	checkViewBounds(t, "serial", func(input []int) *SerialSlice[int, int] { return NewSliceBuilder(ctx, input).Serial() })
	checkViewBounds(t, "parallel", func(input []int) *ParallelSlice[int, int] { return NewSliceBuilder(ctx, input).Parallel() })
	checkViewBounds(t, "legacy", func(input []int) *Slice[int, int] { return New(input) })
}

func checkViewBounds[S boundedView[S]](t *testing.T, name string, build func([]int) S) {
	maxInt := int(^uint(0) >> 1)
	bounds := []int{-maxInt - 1, -1, 0, 1, 2, 4, 5, 6, maxInt}
	type operation struct {
		name string
		i, j int
	}
	var operations []operation
	for _, n := range bounds {
		operations = append(operations, operation{"Take", n, 0}, operation{"Drop", n, 0})
		for _, end := range bounds {
			operations = append(operations, operation{"SliceRange", n, end})
			if name == "builder" {
				operations = append(operations, operation{"Slice", n, end})
			}
		}
	}
	for _, shape := range []string{"nil", "empty", "offset"} {
		for _, op := range operations {
			t.Run(fmt.Sprintf("%s/%s/%s/%d/%d", name, shape, op.name, op.i, op.j), func(t *testing.T) {
				backing := []int{10, 20, 30, 40, 50, 60, 70, 80}
				var input []int
				if shape == "empty" {
					input = backing[2:2]
				}
				if shape == "offset" {
					input = backing[2:7]
				}
				inputLen, inputCap := len(input), cap(input)
				subject := build(input)
				before := subject.Items()
				start, length := 0, len(before)
				switch op.name {
				case "Take":
					if op.i <= 0 {
						length = 0
					} else if op.i < length {
						length = op.i
					}
				case "Drop":
					if op.i >= length && op.i > 0 {
						length = 0
					} else if op.i > 0 {
						start = op.i
						length -= op.i
					}
				case "SliceRange", "Slice":
					low, high := max(op.i, 0), min(op.j, length)
					if low >= high {
						length = 0
					} else {
						start, length = low, high-low
					}
				}
				var returned S
				switch op.name {
				case "Take":
					returned = subject.Take(op.i)
				case "Drop":
					returned = subject.Drop(op.i)
				case "SliceRange":
					returned = subject.SliceRange(op.i, op.j)
				case "Slice":
					returned = any(subject).(interface{ Slice(int, int) S }).Slice(op.i, op.j)
				}
				if any(returned) != any(subject) {
					t.Fatal("view operation did not return receiver")
				}
				got := subject.Items()
				want := before[start : start+length]
				if len(got) != length || cap(got) != cap(before)-start || (got == nil) != (before == nil) || !slices.Equal(got, want) {
					t.Fatalf("view shape got=%v len/cap=%d/%d nil=%t want=%v %d/%d nil=%t", got, len(got), cap(got), got == nil, want, length, cap(before)-start, before == nil)
				}
				if cap(got) > 0 && &got[:cap(got)][0] != &before[start:cap(before)][0] {
					t.Fatal("view operation copied or rebased array")
				}
				if !slices.Equal(backing, []int{10, 20, 30, 40, 50, 60, 70, 80}) {
					t.Fatal("view operation changed excluded slots")
				}
				if len(input) != inputLen || cap(input) != inputCap {
					t.Fatal("external header changed")
				}
				if len(got) > 0 {
					got[0] = 99
					if input[start] != 99 || before[start] != 99 {
						t.Fatal("view slot stopped sharing original array")
					}
				}
			})
		}
	}
}
