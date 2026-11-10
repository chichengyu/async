package sliceops

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type filterContractCell struct{ index int }

// Observe the whole padded input, not only the returned values: crossing the
// parallel threshold changes slot ownership but never copies pointed-to cells.
func TestFilterThresholdSlotOwnership(t *testing.T) {
	for _, engine := range []string{"builder", "builder-one-worker", "serial", "parallel", "parallel-one-worker", "legacy", "builder-delete", "parallel-delete"} {
		for _, length := range []int{0, 1, 999, 1000, 1001} {
			for _, selection := range []string{"none", "all", "odd"} {
				t.Run(fmt.Sprintf("%s/%d/%s", engine, length, selection), func(t *testing.T) {
					backing := make([]*filterContractCell, length+4)
					for i := range backing {
						backing[i] = &filterContractCell{index: i - 2}
					}
					input := backing[2 : 2+length]
					before := slices.Clone(backing)
					var want []*filterContractCell
					for _, cell := range input {
						if selection == "all" || selection == "odd" && cell.index%2 == 1 {
							want = append(want, cell)
						}
					}
					visits := make([]atomic.Int32, length)
					pred := func(cell *filterContractCell) bool {
						visits[cell.index].Add(1)
						return selection == "all" || selection == "odd" && cell.index%2 == 1
					}
					ctx, cancel := context.WithCancel(context.Background())
					cancel() // Data operations ignore execution context and task budgets.
					var got []*filterContractCell
					switch engine {
					case "builder":
						got = NewSliceBuilder(ctx, input).Worker(3).Timeout(time.Nanosecond).FailFast().Filter(pred).Items()
					case "builder-one-worker":
						got = NewSliceBuilder(ctx, input).Worker(1).Filter(pred).Items()
					case "builder-delete":
						got = NewSliceBuilder(ctx, input).Worker(3).DeleteFunc(func(cell *filterContractCell) bool { return !pred(cell) }).Items()
					case "serial":
						got = NewSliceBuilder(ctx, input).Serial().Timeout(time.Nanosecond).FailFast().Filter(pred).Items()
					case "parallel":
						got = NewSliceBuilder(ctx, input).Parallel().Worker(3).Timeout(time.Nanosecond).FailFast().Filter(pred).Items()
					case "parallel-one-worker":
						got = NewSliceBuilder(ctx, input).Parallel().Worker(1).Filter(pred).Items()
					case "parallel-delete":
						got = NewSliceBuilder(ctx, input).Parallel().Worker(3).DeleteFunc(func(cell *filterContractCell) bool { return !pred(cell) }).Items()
					case "legacy":
						got = New(input).Filter(pred).Items()
					}
					if !slices.Equal(got, want) || got == nil {
						t.Fatalf("filter values/nil differ: length=%d want=%d nil=%v", len(got), len(want), got == nil)
					}
					for i := range visits {
						if visits[i].Load() != 1 {
							t.Fatalf("predicate calls[%d]=%d", i, visits[i].Load())
						}
					}
					allocated := engine != "serial" && engine != "legacy" && length >= 1000
					backingWant := slices.Clone(before)
					if !allocated {
						for i, cell := range want {
							backingWant[2+i] = cell
						}
					}
					if !slices.Equal(backing, backingWant) {
						t.Fatal("filter changed unexpected input or tail slots")
					}
					if len(got) > 0 {
						if (&got[0] == &input[0]) == allocated {
							t.Fatal("filter slot ownership differs at threshold")
						}
						got[0].index = -77
						if want[0].index != -77 {
							t.Fatal("filter deep-copied reference content")
						}
						marker := &filterContractCell{index: -88}
						got[0] = marker
						if (input[0] == marker) == allocated {
							t.Fatal("filter slot replacement visibility differs")
						}
					}
				})
			}
		}
	}
}

type panicFilter[S any] interface {
	Filter(func(int) bool) S
	Items() []int
}

func TestSerialFilterPanicKeepsOldHeaderAndPartialWrites(t *testing.T) {
	for _, engine := range []string{"builder", "serial", "parallel", "legacy"} {
		t.Run(engine, func(t *testing.T) {
			input := []int{0, 1, 2, 3, 4}
			ctx := context.Background()
			switch engine {
			case "builder":
				checkFilterPanic(t, NewSliceBuilder(ctx, input), input)
			case "serial":
				checkFilterPanic(t, NewSliceBuilder(ctx, input).Serial(), input)
			case "parallel":
				checkFilterPanic(t, NewSliceBuilder(ctx, input).Parallel(), input)
			case "legacy":
				checkFilterPanic(t, New(input), input)
			}
		})
	}
}

func checkFilterPanic[S panicFilter[S]](t *testing.T, subject S, input []int) {
	t.Helper()
	marker := &struct{ name string }{"predicate panic"}
	var calls []int
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		subject.Filter(func(v int) bool {
			calls = append(calls, v)
			if v == 3 {
				panic(marker)
			}
			return v%2 == 1
		})
	}()
	if recovered != marker || !slices.Equal(calls, []int{0, 1, 2, 3}) {
		t.Fatalf("panic identity/order differ: panic=%v calls=%v", recovered, calls)
	}
	if len(subject.Items()) != 5 || &subject.Items()[0] != &input[0] || !slices.Equal(input, []int{1, 1, 2, 3, 4}) {
		t.Fatalf("panic unexpectedly committed header or rolled back writes: items=%v input=%v", subject.Items(), input)
	}
	subject.Filter(func(v int) bool { return v%2 == 1 })
	if !slices.Equal(subject.Items(), []int{1, 1, 3}) {
		t.Fatalf("reuse did not observe partial writes: %v", subject.Items())
	}
}

func TestParallelFilterPanicExitsProcess(t *testing.T) {
	for _, mode := range []string{"builder-filter", "builder-delete", "parallel-filter", "parallel-delete"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestParallelFilterPanicChild$")
			cmd.Env = append(os.Environ(), "ASYNC_FILTER_PANIC_CHILD="+mode)
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil || err == nil || !strings.Contains(string(output), "panic: filter worker marker") || strings.Contains(string(output), "caller-recovered") {
				t.Fatalf("worker panic outcome: deadline=%v err=%v output=%s", ctx.Err(), err, output)
			}
		})
	}
}

func TestParallelFilterPanicChild(t *testing.T) {
	mode, child := os.LookupEnv("ASYNC_FILTER_PANIC_CHILD")
	if !child {
		t.Skip("subprocess helper")
	}
	defer func() {
		if recover() != nil {
			fmt.Println("caller-recovered")
		}
	}()
	pred := func(int) bool { panic("filter worker marker") }
	b := NewSliceBuilder(context.Background(), make([]int, 1000)).Worker(2)
	switch mode {
	case "builder-filter":
		b.Filter(pred)
	case "builder-delete":
		b.DeleteFunc(pred)
	case "parallel-filter":
		b.Parallel().Worker(2).Filter(pred)
	case "parallel-delete":
		b.Parallel().Worker(2).DeleteFunc(pred)
	default:
		t.Fatal("invalid subprocess mode")
	}
}
