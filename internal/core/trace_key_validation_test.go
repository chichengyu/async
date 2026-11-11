package core_test

import (
	"context"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

func TestTraceIDKeyDynamicComparability(t *testing.T) {
	old := core.GetTraceIDKey()
	t.Cleanup(func() { core.SetTraceIDKey(old) })
	type keyHolder struct{ Value any }
	var slicePointer *[]int
	for _, key := range []any{keyHolder{42}, [1]any{"trace"}, slicePointer} {
		core.SetTraceIDKey(key)
		ctx := core.WithTraceID(context.Background(), "trace-value")
		if got := core.GetTraceID(ctx); got != "trace-value" {
			t.Fatalf("trace ID round-trip with key %T = %q", key, got)
		}
	}

	validKey := keyHolder{"working"}
	core.SetTraceIDKey(validKey)
	ctx := core.WithTraceID(context.Background(), "preserved")
	for _, invalid := range []any{
		keyHolder{[]int{1}},
		keyHolder{[]int(nil)},
		[1]any{map[string]int{}},
		keyHolder{[1]any{func() {}}},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("accepted dynamically incomparable context key %T", invalid)
				}
			}()
			core.SetTraceIDKey(invalid)
		}()
		if got := core.GetTraceIDKey(); got != validKey {
			t.Fatalf("invalid key %T replaced the working configuration", invalid)
		}
		if got := core.GetTraceID(ctx); got != "preserved" {
			t.Fatalf("trace ID after rejecting %T = %q", invalid, got)
		}
	}
}
