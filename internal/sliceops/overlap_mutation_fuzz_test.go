package sliceops

import (
	"context"
	"slices"
	"testing"
)

type overlapMutation[S any] interface {
	Append(...int) S
	Prepend(...int) S
	Insert(int, ...int) S
	Replace(int, int, ...int) S
	Items() []int
}

// Snapshot both logical inputs before calling the wrapper. Rebuild the output
// element by element; do not use slices.Insert/Replace as the oracle. Observe
// the original array too, because Prepend may adopt the argument array.
func FuzzSliceOverlappingMutation(f *testing.F) {
	f.Add([]byte{}, uint8(0), 0, 0, uint8(0), uint8(0), false)
	f.Add([]byte{1, 2, 3, 4}, uint8(1), 1, 3, uint8(0), uint8(2), false)
	f.Add([]byte{1, 2, 3, 4}, uint8(2), -1, 3, uint8(4), uint8(6), false)
	f.Add([]byte{1, 2, 3, 4}, uint8(3), 1, 4, uint8(4), uint8(5), false)
	f.Add([]byte{1, 2, 3, 4}, uint8(3), 2, -1, uint8(0), uint8(3), true)
	f.Add([]byte{1, 2, 3}, uint8(3), int(^uint(0)>>1), -int(^uint(0)>>1)-1, uint8(2), uint8(7), false)
	f.Add([]byte{1, 2, 3}, uint8(2), int(^uint(0)>>1), 0, uint8(0), uint8(7), true)
	f.Add([]byte{1, 2, 3, 4}, uint8(3), 3, 1, uint8(1), uint8(3), false)
	f.Add([]byte{1, 2, 3, 4}, uint8(3), 0, 4, uint8(0), uint8(0), false)
	f.Fuzz(func(t *testing.T, data []byte, operation uint8, i, j int, a, z uint8, capped bool) {
		if len(data) > 32 {
			data = data[:32]
		}
		n := len(data)
		// Keep values distinct even when fuzz data repeats.
		backing := make([]int, n+6)
		for k := range backing {
			backing[k] = 1000 + k*256
			if n > 0 {
				backing[k] += int(data[k%n])
			}
		}
		start, end := int(a)%(len(backing)+1), int(z)%(len(backing)+1)
		if start > end {
			start, end = end, start
		}
		for engine := 0; engine < 4; engine++ {
			source := make([]int, len(backing))
			copy(source, backing)
			input := source[2 : 2+n]
			vals := source[start:end]
			if capped {
				input = input[:len(input):len(input)]
				vals = vals[:len(vals):len(vals)]
			}
			ctx := context.Background()
			switch engine {
			case 0:
				checkOverlappingMutation(t, NewSliceBuilder(ctx, input), source, vals, start, int(operation%4), i, j)
			case 1:
				checkOverlappingMutation(t, NewSliceBuilder(ctx, input).Serial(), source, vals, start, int(operation%4), i, j)
			case 2:
				checkOverlappingMutation(t, NewSliceBuilder(ctx, input).Parallel(), source, vals, start, int(operation%4), i, j)
			case 3:
				checkOverlappingMutation(t, New(input), source, vals, start, int(operation%4), i, j)
			}
		}
	})
}

func checkOverlappingMutation[S overlapMutation[S]](t *testing.T, subject S, backing, vals []int, argumentStart, operation, i, j int) {
	t.Helper()
	original := subject.Items()
	inputSnapshot, valsSnapshot := slices.Clone(original), slices.Clone(vals)
	backingWant := slices.Clone(backing)
	n := len(original)
	begin, end := i, j
	if operation == 2 {
		begin = min(max(i, 0), n)
		end = begin
	}
	if operation == 3 {
		if begin < 0 {
			begin = 0
		}
		if end > n {
			end = n
		}
		if begin > end {
			begin = end
		}
	}
	wantPanic := operation == 3 && end < 0
	var want []int
	push := func(values []int) {
		for _, v := range values {
			want = append(want, v)
		}
	}
	if !wantPanic {
		switch operation {
		case 0:
			push(inputSnapshot)
			push(valsSnapshot)
		case 1:
			push(valsSnapshot)
			push(inputSnapshot)
		case 2, 3:
			push(inputSnapshot[:begin])
			push(valsSnapshot)
			push(inputSnapshot[end:])
		}
	}
	// Only the selected destination can be overwritten. An allocation must not
	// modify the original array. Replace clears the obsolete old-length tail.
	reuse := !wantPanic && len(want) <= cap(original)
	destination := 2
	if operation == 1 {
		reuse = len(want) <= cap(vals)
		destination = argumentStart
	}
	if reuse {
		for k, v := range want {
			backingWant[destination+k] = v
		}
		if operation == 3 {
			for k := len(want); k < n; k++ {
				backingWant[2+k] = 0
			}
		}
	}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		switch operation {
		case 0:
			subject.Append(vals...)
		case 1:
			subject.Prepend(vals...)
		case 2:
			subject.Insert(i, vals...)
		case 3:
			subject.Replace(i, j, vals...)
		}
	}()
	if (recovered != nil) != wantPanic {
		t.Fatalf("op=%d i/j=%d/%d panic=%v wantPanic=%v", operation, i, j, recovered, wantPanic)
	}
	if wantPanic {
		if !slices.Equal(subject.Items(), inputSnapshot) || !slices.Equal(backing, backingWant) {
			t.Fatal("invalid replace mutated receiver or backing")
		}
		return
	}
	got := subject.Items()
	if !slices.Equal(got, want) {
		t.Fatalf("op=%d i/j=%d/%d got=%v want=%v", operation, i, j, got, want)
	}
	if !slices.Equal(backing, backingWant) {
		t.Fatalf("op=%d shared backing=%v want=%v", operation, backing, backingWant)
	}
	if len(got) > 0 {
		if reuse && &got[0] != &backing[destination] {
			t.Fatalf("op=%d lost destination array ownership", operation)
		}
		if !reuse {
			for k := range backing {
				if &got[0] == &backing[k] {
					t.Fatalf("op=%d unexpectedly reused insufficient array", operation)
				}
			}
		}
	}
}
