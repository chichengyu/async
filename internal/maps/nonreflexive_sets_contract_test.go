package maps

import (
	"math"
	"slices"
	"testing"
)

func TestNonReflexiveMapSetAndConversionContracts(t *testing.T) {
	nan := math.Float64frombits(0x7ff8000000000042)
	raw := map[float64]int{1: 10}
	raw[nan] = 20
	raw[nan] = 30
	m := FromRef(raw)
	clone := From(m.Clone())
	values := func(m *Map[float64, int]) []int { out := m.Values(); slices.Sort(out); return out }
	for name, result := range map[string]*Map[float64, int]{
		"self-intersection":   m.Intersect(m),
		"cloned-intersection": m.Intersect(clone),
		"self-difference":     m.Difference(m),
		"cloned-difference":   m.Difference(clone),
		"self-union":          m.Union(m),
		"cloned-union":        m.Union(clone),
	} {
		t.Run(name, func(t *testing.T) {
			want := []int{10}
			if name == "self-difference" || name == "cloned-difference" {
				want = []int{20, 30}
			}
			if name == "self-union" || name == "cloned-union" {
				want = []int{10, 20, 20, 30, 30}
			}
			if got := values(result); !slices.Equal(got, want) {
				t.Fatalf("values=%v want=%v", got, want)
			}
			result.Clear()
			if m.Len() != 3 || clone.Len() != 3 {
				t.Fatal("set result shares input map")
			}
		})
	}
	converted := RemapValues(m, func(_ float64, v int) int { return v + 1 })
	if got := values(converted); !slices.Equal(got, []int{11, 21, 31}) {
		t.Fatalf("remapped=%v", got)
	}
	for key, value := range converted.AsMap() {
		if value != 11 && math.Float64bits(key) != math.Float64bits(nan) {
			t.Fatal("NaN key bits changed")
		}
	}
	remapped := RemapKeys(m, func(float64, int) float64 { return nan })
	if got := values(remapped); !slices.Equal(got, []int{10, 20, 30}) {
		t.Fatalf("nonreflexive target lost entries: %v", got)
	}
	input := From(map[int]float64{1: nan, 2: nan})
	inverted := Invert(input)
	if got := values(inverted); !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("inverted=%v", got)
	}
	if got := values(m); !slices.Equal(got, []int{10, 20, 30}) {
		t.Fatalf("input changed: %v", got)
	}
}
