package maps

import (
	"math"
	"slices"
	"testing"
)

// Independent fixed slots model ordinary key collisions, while a value list
// models NaN entries without looking them up by their non-reflexive keys.
func FuzzMapMergeSourceSnapshot(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7}, true)
	f.Add([]byte{1, 1, 1, 1, 1}, false)
	f.Add([]byte{}, true)
	f.Add([]byte{0, 3, 6, 9}, false) // Distinct-source ordinary-key collision.
	f.Fuzz(func(t *testing.T, data []byte, alias bool) {
		if len(data) > 64 {
			data = data[:64]
		}
		for operation := range 4 {
			src := make(map[any]int)
			var present [8]bool
			var sourceValues [8]int
			var sourceNaNs []int
			for i, b := range data {
				value := 100 + i
				if b%3 == 0 {
					key := int(b % 8)
					src[key] = value
					present[key], sourceValues[key] = true, value
				} else {
					nan := math.Float64frombits(0x7ff8000000000001 + uint64(b%4))
					var key any = nan
					if b%3 == 2 {
						key = [1]any{nan}
					}
					src[key] = value
					sourceNaNs = append(sourceNaNs, value)
				}
			}
			dst := map[any]int{0: -1, 2: -2}
			dst[math.NaN()] = -3
			var want []int
			if alias {
				dst = src
				want = append(want, sourceNaNs...)
				want = append(want, sourceNaNs...)
				for key, ok := range present {
					if ok {
						want = append(want, sourceValues[key])
					}
				}
			} else {
				want = append(want, -3)
				want = append(want, sourceNaNs...)
				for key := range 8 {
					old, exists := 0, key == 0 || key == 2
					if key == 0 {
						old = -1
					}
					if key == 2 {
						old = -2
					}
					if present[key] && (operation != 3 || !exists) {
						want = append(want, sourceValues[key])
					} else if exists {
						want = append(want, old)
					}
				}
			}
			var sourceBefore []int
			keySignatures := make(map[int]string)
			for key, value := range dst {
				keySignatures[value] = nonReflexiveKeySignature(key)
			}
			for key, value := range src {
				keySignatures[value] = nonReflexiveKeySignature(key)
				sourceBefore = append(sourceBefore, value)
			}
			slices.Sort(sourceBefore)
			m := FromRef(dst)
			switch operation {
			case 0:
				if m.SetAll(src) != m {
					t.Fatal("SetAll did not return self")
				}
			case 1:
				if m.Merge(src) != m {
					t.Fatal("Merge did not return self")
				}
			case 2:
				if m.MergeMap(FromRef(src)) != m {
					t.Fatal("MergeMap did not return self")
				}
			case 3:
				if m.MergeWithDefault(src) != m {
					t.Fatal("MergeWithDefault did not return self")
				}
			}
			var got []int
			for key, value := range dst {
				if nonReflexiveKeySignature(key) != keySignatures[value] {
					t.Fatalf("changed key for value=%d", value)
				}
				got = append(got, value)
			}
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("operation=%d alias=%v got=%v want=%v", operation, alias, got, want)
			}
			if !alias {
				var sourceAfter []int
				for _, value := range src {
					sourceAfter = append(sourceAfter, value)
				}
				slices.Sort(sourceAfter)
				if !slices.Equal(sourceBefore, sourceAfter) {
					t.Fatal("modified distinct source")
				}
			}
		}
	})
}
