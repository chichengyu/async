package sliceops

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

func TestSliceResultFailureInputsStaySaved(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("saved failed input")
	input := []int{10, 20, 30, 40, 50}
	for engine := 0; engine < 3; engine++ {
		b := NewSliceBuilder(ctx, input)
		fn := func(_ context.Context, v int) (int, error) {
			if v == 20 || v == 50 {
				return v, boom
			}
			return v, nil
		}
		var r *SliceResult[int]
		switch engine {
		case 0:
			r = b.Map(fn)
		case 1:
			r = b.Serial().Map(fn)
		case 2:
			r = b.Parallel().Map(fn)
		}
		if !slices.Equal(r.FailValues(), []any{20, 50}) || len(r.Errors()) != 2 {
			t.Fatalf("engine=%d failed inputs=%v errors=%v", engine, r.FailValues(), r.Errors())
		}
		for i := range r.Results() {
			r.Results()[i].Err = nil
		}
		if !slices.Equal(r.FailValues(), []any{20, 50}) || len(r.Errors()) != 0 {
			t.Fatalf("engine=%d saved failure inputs changed after results edit", engine)
		}
	}
	for engine := 0; engine < 2; engine++ {
		for _, size := range []int{-1, 0, 1, 2, 3, int(^uint(0) >> 1)} {
			width := size
			if width <= 0 {
				width = 1
			}
			var want []any
			wantFailures := 0
			for start := 0; start < len(input); {
				end := len(input)
				if width < len(input)-start {
					end = start + width
				}
				fails := false
				for _, v := range input[start:end] {
					fails = fails || v == 20 || v == 50
				}
				if fails {
					wantFailures++
					for _, v := range input[start:end] {
						want = append(want, v)
					}
				}
				start = end
			}
			fn := func(_ context.Context, part []int) (int, error) {
				for _, v := range part {
					if v == 20 || v == 50 {
						return len(part), boom
					}
				}
				return len(part), nil
			}
			b := NewSliceBuilder(ctx, input)
			var r *SliceResult[int]
			if engine == 0 {
				r = b.Chunk(size).MapBatch(fn)
			} else {
				r = b.Parallel().Chunk(size).MapBatch(fn)
			}
			if !slices.Equal(r.FailValues(), want) || len(r.Errors()) != wantFailures {
				t.Fatalf("engine=%d size=%d batch failed inputs=%v want=%v errors=%v", engine, size, r.FailValues(), want, r.Errors())
			}
			for i := range r.Results() {
				r.Results()[i].Err = nil
			}
			if !slices.Equal(r.FailValues(), want) || len(r.Errors()) != 0 {
				t.Fatalf("engine=%d size=%d saved batch failures changed after results edit", engine, size)
			}
		}
	}
}

type projectionNilError struct{}

func (*projectionNilError) Error() string { return "typed nil error" }

// Build the expected projections from input states, not from another result
// accessor. Occupied deliberately varies independently of values and errors.
func FuzzSliceResultProjections(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, uint8(1))
	f.Add([]byte{5, 5, 0, 6, 7, 1}, uint8(7))
	f.Fuzz(func(t *testing.T, data []byte, flags uint8) {
		if len(data) > 64 {
			data = data[:64]
		}
		errorsByCode := []error{nil, errors.New("first"), errors.New("second"), (*projectionNilError)(nil)}
		var expectedValues []*int
		var expectedErrors []error
		var allValues []*int
		input := make([]core.Result[*int], len(data))
		for i, b := range data {
			value := new(int)
			*value = i + 1
			if b&8 != 0 {
				value = nil
			}
			err := errorsByCode[int(b)%4]
			input[i] = core.Result[*int]{Value: value, Err: err, Occupied: b&4 != 0}
			allValues = append(allValues, value)
			if err == nil {
				expectedValues = append(expectedValues, value)
			} else {
				expectedErrors = append(expectedErrors, err)
			}
		}
		top := errorsByCode[int(flags)%4]
		isSummary := flags&4 != 0
		result := &SliceResult[*int]{results: input, err: top, errIsSummary: isSummary, failValues: []any{"saved input"}}
		wantError := top
		if wantError == nil && len(expectedErrors) > 0 {
			wantError = expectedErrors[0]
		}
		wantErrors := slices.Clone(expectedErrors)
		if top != nil && !isSummary {
			wantErrors = append([]error{top}, wantErrors...)
		}
		if result.Error() != wantError || result.Ok() != (wantError == nil) || result.Err() != (wantError != nil) {
			t.Fatal("summary priority or boolean projection")
		}
		if !slices.Equal(result.Errors(), wantErrors) {
			t.Fatal("error order or duplicate handling")
		}
		if !slices.Equal(result.Values(), expectedValues) || result.Len() != len(data) {
			t.Fatal("value projection or retained length")
		}
		first, found := result.First()
		var expectedFirst *int
		if len(expectedValues) > 0 {
			expectedFirst = expectedValues[0]
		}
		if first != expectedFirst || found != (len(expectedValues) > 0) {
			t.Fatal("first success projection")
		}
		values, err := result.Unwrap()
		if err != wantError || !slices.Equal(values, expectedValues) {
			t.Fatal("partial unwrap projection")
		}
		panicked := true
		var panicValue any
		func() {
			defer func() {
				if panicked {
					panicValue = recover()
				}
			}()
			values = result.Must()
			panicked = false
		}()
		if panicked != (wantError != nil) || (panicked && panicValue != wantError) || (!panicked && !slices.Equal(values, expectedValues)) {
			t.Fatal("must panic identity/value projection")
		}
		if !slices.Equal(Flat(input), allValues) || !slices.Equal(ResultValues(input), expectedValues) {
			t.Fatal("flat versus successful values")
		}
		if !slices.Equal(ResultErrors(input), expectedErrors) || !slices.Equal(OnlyErrors(input), expectedErrors) {
			t.Fatal("standalone error projection")
		}
		successes, failures := PartitionResults(input)
		if !slices.Equal(successes, expectedValues) || !slices.Equal(failures, expectedErrors) {
			t.Fatal("partition result projection")
		}
		if Every(input) != (len(expectedErrors) == 0) || Some(input) != (len(expectedValues) > 0) || AnyError(input) != (len(expectedErrors) > 0) {
			t.Fatal("empty/success/error quantifiers")
		}
		// Returned value/error slices copy slots; their referenced objects remain shared.
		copiedValues := result.Values()
		if len(copiedValues) > 0 {
			copiedValues[0] = nil
		}
		copiedErrors := result.Errors()
		if len(copiedErrors) > 0 {
			copiedErrors[0] = nil
		}
		if !slices.Equal(result.Values(), expectedValues) || !slices.Equal(result.Errors(), wantErrors) {
			t.Fatal("projection slots unexpectedly shared")
		}
		if len(input) > 0 {
			marker := 99
			result.Results()[0] = core.Result[*int]{Value: &marker, Occupied: true}
			if input[0].Value != &marker || input[0].Err != nil {
				t.Fatal("results slots must be shared")
			}
			if value, ok := result.First(); !ok || value != &marker {
				t.Fatal("queries must reflect modified result slots")
			}
			if top != nil && result.Error() != top {
				t.Fatal("saved summary unexpectedly recomputed")
			}
		}
		if result.FailValues()[0] != "saved input" {
			t.Fatal("saved failure inputs unexpectedly recomputed")
		}
		result.FailValues()[0] = "updated saved input"
		if result.failValues[0] != "updated saved input" {
			t.Fatal("failure inputs slots must be shared")
		}
	})
}
