package core_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// These callbacks deliberately exceed the guarantees of context.CancelFunc.
// MergeCancel is synchronous composition, so a callback that does not return
// prevents subsequent callbacks; it must not silently gain defer/once/recover.
func TestMergeCancelCallbackOrderAndAbnormalExit(t *testing.T) {
	for _, oldMode := range []string{"nil", "return", "panic", "goexit"} {
		for _, newMode := range []string{"nil", "return", "panic", "goexit"} {
			t.Run(fmt.Sprintf("old=%s/new=%s", oldMode, newMode), func(t *testing.T) {
				oldPanic, newPanic := errors.New("old panic"), errors.New("new panic")
				var order []string
				callback := func(label, mode string, marker error) context.CancelFunc {
					if mode == "nil" {
						return nil
					}
					return func() {
						order = append(order, label)
						switch mode {
						case "panic":
							panic(marker)
						case "goexit":
							runtime.Goexit()
						}
					}
				}
				combined := core.MergeCancel(callback("old", oldMode, oldPanic), callback("new", newMode, newPanic))
				if (combined == nil) != (oldMode == "nil" && newMode == "nil") {
					t.Fatal("nil result changed")
				}
				type outcome struct {
					returned  bool
					recovered any
				}
				done := make(chan outcome, 1)
				go func() {
					result := outcome{}
					defer func() { result.recovered = recover(); done <- result }()
					combined()
					result.returned = true
				}()
				got := <-done
				var expected []string
				if newMode != "nil" {
					expected = append(expected, "new")
				}
				if newMode == "return" && oldMode != "nil" {
					expected = append(expected, "old")
				}
				if !reflect.DeepEqual(order, expected) {
					t.Fatalf("callback order=%v want=%v", order, expected)
				}
				switch {
				case newMode == "nil":
					if got.returned || got.recovered == nil {
						t.Fatalf("nil callback outcome=%+v", got)
					}
				case newMode == "panic":
					if got.returned || got.recovered != newPanic {
						t.Fatalf("new panic outcome=%+v", got)
					}
				case newMode == "goexit" || oldMode == "goexit":
					if got.returned || got.recovered != nil {
						t.Fatalf("Goexit outcome=%+v", got)
					}
				case oldMode == "panic":
					if got.returned || got.recovered != oldPanic {
						t.Fatalf("old panic outcome=%+v", got)
					}
				default:
					if !got.returned || got.recovered != nil {
						t.Fatalf("normal outcome=%+v", got)
					}
				}
			})
		}
	}
}

func TestMergeCancelNestedAndConcurrentCalls(t *testing.T) {
	t.Run("nested-repeat", func(t *testing.T) {
		var calls []int
		makeCancel := func(i int) context.CancelFunc { return func() { calls = append(calls, i) } }
		combined := core.MergeCancel(core.MergeCancel(makeCancel(1), makeCancel(2)), makeCancel(3))
		combined()
		combined()
		if !reflect.DeepEqual(calls, []int{3, 2, 1, 3, 2, 1}) {
			t.Fatalf("nested calls=%v", calls)
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		parent, oldCancel := context.WithCancel(context.Background())
		defer oldCancel()
		child, newCancel := context.WithCancel(parent)
		defer newCancel()
		var oldCalls, newCalls atomic.Int64
		combined := core.MergeCancel(func() { oldCalls.Add(1); oldCancel() }, func() { newCalls.Add(1); newCancel() })
		const count = 64
		var wg sync.WaitGroup
		gate := make(chan struct{})
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-gate; combined() }()
		}
		close(gate)
		wg.Wait()
		if oldCalls.Load() != count || newCalls.Load() != count {
			t.Fatalf("calls old=%d new=%d", oldCalls.Load(), newCalls.Load())
		}
		if parent.Err() != context.Canceled || child.Err() != context.Canceled {
			t.Fatal("contexts not canceled")
		}
	})
}
