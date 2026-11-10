package core_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

func TestResultErrorChainAndFormattingContracts(t *testing.T) {
	business := errors.New("business")
	pe := &core.PanicError{Value: business, Stack: []byte("private-stack")}
	var nilPanic *core.PanicError
	for _, tc := range []struct {
		name       string
		err        error
		panicError bool
	}{
		{"nil", nil, false},
		{"business", business, false},
		{"panic", pe, true},
		{"wrapped-panic", fmt.Errorf("outer: %w", pe), true},
		{"joined-panic", errors.Join(business, pe), true},
		{"panic-as-value-only", &core.PanicError{Value: pe}, true},
		{"typed-nil-panic", nilPanic, true},
	} {
		for _, occupied := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/occupied=%t", tc.name, occupied), func(t *testing.T) {
				r := core.Result[string]{Value: "value-retained", Err: tc.err, Occupied: occupied}
				if r.Error() != tc.err || r.Ok() != (tc.err == nil) || r.IsPanic() != tc.panicError {
					t.Fatalf("result error projection changed for %s", tc.name)
				}
				if r.Value != "value-retained" || r.Occupied != occupied {
					t.Fatal("query changed result fields")
				}
			})
		}
	}
	if errors.Is(pe, business) || pe.Unwrap() != nil {
		t.Fatal("panic Value unexpectedly entered the error unwrap chain")
	}
	var found *core.PanicError
	if !errors.As(fmt.Errorf("outer: %w", pe), &found) || found != pe {
		t.Fatal("wrapped error lost original PanicError identity")
	}
	r := core.Result[string]{Value: "hidden-value", Err: pe}
	for _, format := range []string{"%v", "%+v"} {
		if got := fmt.Sprintf(format, r); got != "err=panic: business" {
			t.Fatalf("Result %s = %q: error must win without forwarding stack flags", format, got)
		}
	}
	if got := (core.Result[string]{Value: "visible"}).String(); got != "value=visible" {
		t.Fatalf("success string = %q", got)
	}
}

func TestPanicErrorFormattingFlagsAndQuoting(t *testing.T) {
	pe := &core.PanicError{Value: "a\nb", Stack: []byte("private-stack")}
	for _, tc := range []struct{ format, want string }{
		{"%v", "panic: a\nb"},
		{"%s", "panic: a\nb"},
		{"%q", "panic: a\nb"},
		{"%x", "panic: a\nb"},
		{"%#v", "panic: a\nb"},
		{"%40.1v", "panic: a\nb"},
		{"%+s", "panic: a\nb"},
		{"%+q", "panic: a\nb"},
		{"%+v", "panic: a\nb\nprivate-stack"},
		{"%+40.1v", "panic: a\nb\nprivate-stack"},
	} {
		t.Run(tc.format, func(t *testing.T) {
			if got := fmt.Sprintf(tc.format, pe); got != tc.want {
				t.Fatalf("format %s = %q, want %q", tc.format, got, tc.want)
			}
		})
	}
	if got := fmt.Sprintf("%q", pe.Error()); got != "\"panic: a\\nb\"" {
		t.Fatalf("explicit string quoting = %q", got)
	}
}

type panicFormatState struct {
	strings.Builder
	writes, failAt int
}

func (s *panicFormatState) Write(data []byte) (int, error) {
	s.writes++
	if s.writes == s.failAt {
		return 0, errors.New("writer unavailable")
	}
	return s.Builder.Write(data)
}
func (*panicFormatState) Width() (int, bool)     { panic("width must not be queried") }
func (*panicFormatState) Precision() (int, bool) { panic("precision must not be queried") }
func (*panicFormatState) Flag(flag int) bool     { return flag == '+' }

func TestPanicErrorFormatWriterFailures(t *testing.T) {
	pe := &core.PanicError{Value: "boom", Stack: []byte("stack")}
	for _, tc := range []struct {
		failAt, writes int
		want           string
	}{
		{0, 2, "panic: boom\nstack"},
		{1, 1, ""},
		{2, 2, "panic: boom"},
	} {
		t.Run(fmt.Sprintf("write-error-%d", tc.failAt), func(t *testing.T) {
			state := &panicFormatState{failAt: tc.failAt}
			pe.Format(state, 'v')
			if state.writes != tc.writes || state.String() != tc.want {
				t.Fatalf("writes=%d output=%q; want %d %q", state.writes, state.String(), tc.writes, tc.want)
			}
		})
	}
}

func TestNewPanicErrorRetainsValueReference(t *testing.T) {
	value := &struct{ message string }{message: "before"}
	pe := core.NewPanicError(value)
	if pe.Value != value || !strings.Contains(string(pe.Stack), "TestNewPanicErrorRetainsValueReference") {
		t.Fatal("constructor lost value reference or creation stack")
	}
	value.message = "after"
	if got := pe.Error(); got != "panic: &{after}" {
		t.Fatalf("retained value was deep copied: %q", got)
	}
	pe.Stack = []byte("replacement-stack")
	if got := fmt.Sprintf("%+v", pe); got != "panic: &{after}\nreplacement-stack" {
		t.Fatalf("format did not read current exported fields: %q", got)
	}
}
