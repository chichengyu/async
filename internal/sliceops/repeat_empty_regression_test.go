package sliceops

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"testing"
	"time"
)

// Use a subprocess so an unbounded empty append loop cannot leak a busy
// goroutine into the test runner. The deadline is a liveness guard, not a
// throughput assertion; an empty repetition has no element work to perform.
func TestRepeatEmptyLargeCount(t *testing.T) {
	for engine := range 4 {
		t.Run(strconv.Itoa(engine), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRepeatEmptyLargeCountChild$")
			command.Env = append(os.Environ(), "ASYNC_REPEAT_EMPTY_CHILD="+strconv.Itoa(engine))
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("empty Repeat(MaxInt) did not finish: %v; output=%s", ctx.Err(), output)
			}
			if err != nil {
				t.Fatalf("empty repeat child: %v; output=%s", err, output)
			}
		})
	}
}

func TestRepeatEmptyLargeCountChild(t *testing.T) {
	engineText, child := os.LookupEnv("ASYNC_REPEAT_EMPTY_CHILD")
	if !child {
		t.Skip("subprocess helper")
	}
	engine, err := strconv.Atoi(engineText)
	if err != nil {
		t.Fatal(err)
	}
	for _, shape := range []string{"nil", "allocated", "offset"} {
		t.Run(shape, func(t *testing.T) {
			backing := []int{10, 20, 30, 40}
			before := slices.Clone(backing)
			var input []int
			switch shape {
			case "allocated":
				input = make([]int, 0, 3)
			case "offset":
				input = backing[2:2]
			}
			maxInt := int(^uint(0) >> 1)
			switch engine {
			case 0:
				checkEmptyRepeat(t, NewSliceBuilder(context.Background(), input), maxInt)
			case 1:
				checkEmptyRepeat(t, NewSliceBuilder(context.Background(), input).Serial(), maxInt)
			case 2:
				checkEmptyRepeat(t, NewSliceBuilder(context.Background(), input).Parallel(), maxInt)
			case 3:
				checkEmptyRepeat(t, New(input), maxInt)
			default:
				t.Fatalf("unknown engine %d", engine)
			}
			if !slices.Equal(backing, before) {
				t.Fatalf("backing changed: %v", backing)
			}
		})
	}
}

type repeatView[T any, S any] interface {
	Repeat(int) S
	Items() []T
}

func checkEmptyRepeat[S repeatView[int, S]](t *testing.T, subject S, count int) {
	t.Helper()
	before := subject.Items()
	subject.Repeat(count)
	after := subject.Items()
	if len(after) != 0 || cap(after) != cap(before) || (after == nil) != (before == nil) {
		t.Fatalf("empty view changed: before len/cap/nil=%d/%d/%v after=%d/%d/%v", len(before), cap(before), before == nil, len(after), cap(after), after == nil)
	}
	if cap(before) > 0 && &before[:cap(before)][0] != &after[:cap(after)][0] {
		t.Fatal("empty view backing changed")
	}
}

func TestRepeatElementValuesAndOwnership(t *testing.T) {
	for engine := range 4 {
		for _, count := range []int{-1, 0, 1, 2, 3} {
			for _, capacity := range []int{2, 8} {
				t.Run(strconv.Itoa(engine)+"/"+strconv.Itoa(count)+"/"+strconv.Itoa(capacity), func(t *testing.T) {
					backing := make([]*ownershipCell, capacity)
					for i := range backing {
						backing[i] = &ownershipCell{value: i + 10}
					}
					input := backing[:2]
					switch engine {
					case 0:
						checkRepeatOwnership(t, NewSliceBuilder(context.Background(), input), count, backing)
					case 1:
						checkRepeatOwnership(t, NewSliceBuilder(context.Background(), input).Serial(), count, backing)
					case 2:
						checkRepeatOwnership(t, NewSliceBuilder(context.Background(), input).Parallel(), count, backing)
					case 3:
						checkRepeatOwnership(t, New(input), count, backing)
					}
				})
			}
		}
	}
}

func checkRepeatOwnership[S repeatView[*ownershipCell, S]](t *testing.T, subject S, count int, backing []*ownershipCell) {
	t.Helper()
	before := slices.Clone(backing)
	original := subject.Items()
	subject.Repeat(count)
	got := subject.Items()
	wantLength := 0
	if count > 0 {
		wantLength = 2 * count
	}
	if len(got) != wantLength {
		t.Fatalf("length=%d want=%d", len(got), wantLength)
	}
	for i, value := range got {
		if value != before[i%2] {
			t.Fatalf("slot %d lost original reference", i)
		}
	}
	if count <= 1 {
		if cap(got) != cap(original) || !slices.Equal(backing, before) {
			t.Fatal("empty/one repetition changed backing or capacity")
		}
		if &got[:cap(got)][0] != &original[:cap(original)][0] {
			t.Fatal("empty/one repetition changed view origin")
		}
		return
	}
	if cap(original) >= wantLength {
		if &got[0] != &original[0] {
			t.Fatal("sufficient capacity no longer reuses element slots")
		}
		for i, value := range backing {
			want := before[i]
			if i < wantLength {
				want = before[i%2]
			}
			if value != want {
				t.Fatalf("backing slot %d=%p want=%p", i, value, want)
			}
		}
	} else if &got[0] == &original[0] {
		t.Fatal("expanded view still points to insufficient original array")
	}
	got[0].value++
	if got[2].value != before[0].value {
		t.Fatal("repeat unexpectedly deep-copied element contents")
	}
	got[0] = &ownershipCell{value: 99}
	if got[2] != before[0] {
		t.Fatal("repeated element slots are not independently assignable")
	}
}
