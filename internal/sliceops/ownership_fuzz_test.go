package sliceops

import (
	"context"
	"slices"
	"testing"
)

type ownershipCell struct{ value int }

// Use a padded input view to distinguish length, remaining capacity, element
// slots and referenced objects. The oracle builds groups one element at a time.
func FuzzSliceChunkAndPartitionOwnership(f *testing.F) {
	f.Add([]byte{}, 0, false)
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 6, true)
	f.Add([]byte{1, 2, 3, 4, 5}, 2, false)
	f.Add([]byte{0, 0, 0}, -1, true)
	f.Add([]byte{1, 2, 3}, int(^uint(0)>>1), false)
	f.Add([]byte{1, 2, 3}, -int(^uint(0)>>1)-1, true)
	f.Fuzz(func(t *testing.T, data []byte, size int, byCount bool) {
		if len(data) > 64 {
			data = data[:64]
		}
		n := len(data)
		var groups [][]int
		if n > 0 && size > 0 {
			width := size
			if byCount {
				width = n / size
				if n%size != 0 {
					width++
				}
			}
			for i := 0; i < n; i++ {
				if len(groups) == 0 || len(groups[len(groups)-1]) == width {
					groups = append(groups, nil)
				}
				groups[len(groups)-1] = append(groups[len(groups)-1], i)
			}
		}
		ctx := context.Background()
		for engine := 0; engine < 5; engine++ {
			backing := make([]*ownershipCell, n+5)
			for i := range backing {
				backing[i] = &ownershipCell{value: i - 2}
			}
			view := backing[2 : 2+n]
			before := slices.Clone(backing)
			b := NewSliceBuilder(ctx, view)
			legacy := New(view)
			var chunks [][]*ownershipCell
			switch engine {
			case 0:
				if byCount {
					chunks = ChunkN(view, size)
				} else {
					chunks = Chunk(view, size)
				}
			case 1:
				if byCount {
					chunks = b.ChunkedN(size)
				} else {
					chunks = b.Chunked(size)
				}
			case 2:
				if byCount {
					chunks = b.Serial().ChunkedN(size)
				} else {
					chunks = b.Serial().Chunked(size)
				}
			case 3:
				if byCount {
					chunks = b.Parallel().ChunkedN(size)
				} else {
					chunks = b.Parallel().Chunked(size)
				}
			case 4:
				if byCount {
					chunks = legacy.ChunkN(size)
				} else {
					chunks = legacy.Chunk(size)
				}
			}
			if len(chunks) != len(groups) || (len(groups) == 0 && chunks != nil) {
				t.Fatalf("engine=%d group count=%d want=%d", engine, len(chunks), len(groups))
			}
			if !slices.Equal(backing, before) {
				t.Fatalf("engine=%d grouping changed input", engine)
			}
			for block, indices := range groups {
				part := chunks[block]
				start := indices[0]
				if len(part) != len(indices) || cap(part) != cap(view)-start {
					t.Fatalf("engine=%d block=%d shape=%d/%d want=%d/%d", engine, block, len(part), cap(part), len(indices), cap(view)-start)
				}
				for j, index := range indices {
					if part[j] != before[2+index] || &part[j] != &view[index] {
						t.Fatalf("engine=%d block=%d element=%d ownership mismatch", engine, block, j)
					}
				}
			}
			if len(chunks) > 0 {
				part := chunks[0]
				marker := &ownershipCell{value: -999}
				limited := part[:len(part):len(part)]
				expanded := append(limited, marker)
				if expanded[len(part)] != marker || !slices.Equal(backing, before) {
					t.Fatalf("engine=%d capped append changed source", engine)
				}
				_ = append(part, marker) // padded capacity guarantees an in-place append
				if backing[2+len(part)] != marker {
					t.Fatalf("engine=%d uncapped append did not reach source", engine)
				}
				part[0] = marker
				if view[0] != marker {
					t.Fatalf("engine=%d element write not shared", engine)
				}
			}
		}

		for engine := 0; engine < 5; engine++ {
			input := make([]*ownershipCell, n)
			for i, b := range data {
				input[i] = &ownershipCell{value: int(b)}
			}
			before := slices.Clone(input)
			var calls, yes, no []*ownershipCell
			predicate := func(v *ownershipCell) bool { calls = append(calls, v); return v.value%2 == 0 }
			b := NewSliceBuilder(ctx, input)
			switch engine {
			case 0:
				yes, no = Partition(input, predicate)
			case 1:
				yes, no = b.Split(predicate)
			case 2:
				yes, no = b.Serial().Split(predicate)
			case 3:
				yes, no = b.Parallel().Split(predicate)
			case 4:
				yes, no = New(input).Split(predicate)
			}
			if !slices.Equal(calls, before) || !slices.Equal(input, before) {
				t.Fatalf("engine=%d partition predicate order/input changed", engine)
			}
			var wantYes, wantNo []*ownershipCell
			for _, v := range before {
				if v.value%2 == 0 {
					wantYes = append(wantYes, v)
				} else {
					wantNo = append(wantNo, v)
				}
			}
			if !slices.Equal(yes, wantYes) || !slices.Equal(no, wantNo) {
				t.Fatalf("engine=%d partition groups mismatch", engine)
			}
			if (len(wantYes) == 0 && yes != nil) || (len(wantNo) == 0 && no != nil) {
				t.Fatalf("engine=%d empty partition must be nil", engine)
			}
			for _, group := range [][]*ownershipCell{yes, no} {
				for i, v := range group {
					for j := range input {
						if &group[i] == &input[j] {
							t.Fatalf("engine=%d partition slots shared", engine)
						}
					}
					index := slices.Index(before, v)
					expected := input[index].value + 1
					v.value++ // pointer content remains shared with original input
					if input[index].value != expected {
						t.Fatalf("engine=%d partition reference copied", engine)
					}
					group[i] = nil
				}
			}
			if !slices.Equal(input, before) {
				t.Fatalf("engine=%d partition slot write changed input", engine)
			}
		}
	})
}
