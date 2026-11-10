package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Configuration is captured at Pipe(), but input values are read at each Run.
// Only public builder operations change configuration; no concurrent mutation
// of the builder or shared input is allowed by this contract.
func TestPipeCapturesConfigurationAndRereadsSharedInput(t *testing.T) {
	for _, mode := range []string{"default", "serial", "parallel"} {
		for _, ff := range []bool{false, true} {
			for _, timed := range []bool{false, true} {
				for _, observed := range []bool{false, true} {
					for _, fails := range []bool{false, true} {
						for _, buf := range []int{-2, 1, 7} {
							t.Run(fmt.Sprintf("%s/ff=%v/timed=%v/observed=%v/fails=%v/buf=%d", mode, ff, timed, observed, fails, buf), func(t *testing.T) {
								synctest.Test(t, func(t *testing.T) {
									testPipeSnapshot(t, mode, ff, timed, observed, fails, buf)
								})
							})
						}
					}
				}
			}
		}
	}
}

func testPipeSnapshot(t *testing.T, mode string, ff, timed, observed, fails bool, buf int) {
	t.Helper()
	type contextKey struct{}
	parent := context.WithValue(context.Background(), contextKey{}, "captured")
	replacement, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "replacement"))
	cancel()
	items := []int{1}
	b := NewPipelineBuilder(items).Context(parent).Stage("first", 1).Stage("last", 1)
	var oldObserver, newObserver, business atomic.Int32
	if ff {
		b.FailFast()
	}
	if timed {
		b.Timeout(time.Hour)
	}
	if observed {
		b.OnResult(func(string, core.Result[int]) { oldObserver.Add(1) })
	}
	var pipe *PipelinePipeBuilder[int]
	var changeSource func()
	switch mode {
	case "default":
		pipe = b.Pipe()
		changeSource = func() {
			b.Context(replacement).Stage("too-late", 1).OnResult(func(string, core.Result[int]) { newObserver.Add(1) })
			if ff {
				b.DefaultFailFast()
			} else {
				b.FailFast()
			}
			if timed {
				b.DefaultTimeout()
			} else {
				b.Timeout(time.Nanosecond)
			}
		}
	case "serial":
		s := b.Serial()
		pipe = s.Pipe()
		changeSource = func() {
			s.Context(replacement).Stage("too-late", 1).OnResult(func(string, core.Result[int]) { newObserver.Add(1) })
			if ff {
				s.DefaultFailFast()
			} else {
				s.FailFast()
			}
			if timed {
				s.DefaultTimeout()
			} else {
				s.Timeout(time.Nanosecond)
			}
		}
	case "parallel":
		p := b.Parallel()
		pipe = p.Pipe()
		changeSource = func() {
			p.Context(replacement).Stage("too-late", 1).OnResult(func(string, core.Result[int]) { newObserver.Add(1) })
			if ff {
				p.DefaultFailFast()
			} else {
				p.FailFast()
			}
			if timed {
				p.DefaultTimeout()
			} else {
				p.Timeout(time.Nanosecond)
			}
		}
	}
	changeSource()
	pipe.Buf(buf)
	wantErr := errors.New("first stage failure")
	fn := func(ctx context.Context, stage string, value int) (int, error) {
		business.Add(1)
		if ctx.Value(contextKey{}) != "captured" || ctx.Err() != nil {
			t.Errorf("captured context replaced: value=%v err=%v", ctx.Value(contextKey{}), ctx.Err())
		}
		deadline, hasDeadline := ctx.Deadline()
		// Group may impose its default per-task deadline in the final stage.
		// First stage has only the captured builder's timeout.
		if stage == "first" && (hasDeadline != timed || (timed && deadline.Sub(time.Now()) != time.Hour)) {
			t.Errorf("first deadline=%v/%v timed=%v", deadline, hasDeadline, timed)
		}
		switch stage {
		case "first":
			if fails {
				return value + 1, wantErr
			}
			return value + 1, nil
		case "last":
			return value + 100, nil
		default:
			t.Errorf("stage appended after Pipe executed: %s", stage)
			return -1, nil
		}
	}
	var previous <-chan core.Result[int]
	for run := 1; run <= 2; run++ {
		items[0] = run * 10
		if run == 2 {
			pipe.Buf(9).DefaultBuf()
		}
		handle := pipe.Run(fn)
		ch := handle.Receive()
		if ch != handle.Receive() || ch == previous {
			t.Fatal("Receive must reuse the handle channel; separate Run must create a fresh channel")
		}
		wantCap := buf
		if wantCap <= 0 || run == 2 {
			wantCap = 1
		}
		if cap(ch) != wantCap {
			t.Errorf("buffer=%d want %d", cap(ch), wantCap)
		}
		var results []core.Result[int]
		if run == 1 {
			for r := range ch {
				results = append(results, r)
			}
		} else {
			handle.Drain(func(r core.Result[int]) { results = append(results, r) })
		}
		wantValue := run*10 + 101
		var expectedErr error
		if ff && fails {
			wantValue = run*10 + 1
			expectedErr = wantErr
		}
		if mode == "serial" && ff && fails {
			// Serial emits only final-stage results, even when an earlier
			// stage's failure triggers FailFast. OnResult sees that error.
			if len(results) != 0 {
				t.Fatalf("serial early FailFast emitted non-final results: %+v", results)
			}
		} else if len(results) != 1 || results[0].Value != wantValue || results[0].Err != expectedErr {
			t.Fatalf("run=%d results=%+v want value=%d err=%v", run, results, wantValue, expectedErr)
		}
		if _, ok := <-handle.Receive(); ok {
			t.Fatal("drained handle replayed a result")
		}
		previous = ch
	}
	wantCalls := int32(4)
	if ff && fails {
		wantCalls = 2
	}
	wantObserved := int32(0)
	if observed {
		wantObserved = wantCalls
	}
	if business.Load() != wantCalls || oldObserver.Load() != wantObserved || newObserver.Load() != 0 {
		t.Fatalf("business=%d original observer=%d replacement=%d want %d/%d/0", business.Load(), oldObserver.Load(), newObserver.Load(), wantCalls, wantObserved)
	}
}

func TestPipeReceiveAndDrainConsumeOneQueue(t *testing.T) {
	handle := NewPipelineBuilder([]int{1, 2, 3}).Stage("only", 1).Pipe().Buf(3).Run(
		func(_ context.Context, _ string, value int) (int, error) { return value, nil })
	first, ok := <-handle.Receive()
	if !ok || first.Err != nil {
		t.Fatalf("first=%+v open=%v", first, ok)
	}
	seen := map[int]int{first.Value: 1}
	handle.Drain(func(r core.Result[int]) {
		seen[r.Value]++
		if r.Err != nil {
			t.Error(r.Err)
		}
	})
	if len(seen) != 3 || seen[1] != 1 || seen[2] != 1 || seen[3] != 1 {
		t.Fatalf("shared queue results=%v", seen)
	}
	called := false
	handle.Drain(func(core.Result[int]) { called = true })
	if called {
		t.Fatal("second Drain replayed data")
	}
}

func TestPipeRetainsCapturedParentCancellation(t *testing.T) {
	for _, mode := range []string{"default", "serial", "parallel"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				var observed atomic.Int32
				b := NewPipelineBuilder([]int{1}).Context(parent).Stage("only", 1).
					OnResult(func(_ string, r core.Result[int]) {
						if !errors.Is(r.Err, context.Canceled) {
							t.Errorf("observer lost cancellation: %+v", r)
						}
						observed.Add(1)
					})
				var pipe *PipelinePipeBuilder[int]
				switch mode {
				case "default":
					pipe = b.Pipe()
					b.Context(context.Background())
				case "serial":
					s := b.Serial()
					pipe = s.Pipe()
					s.Context(context.Background())
				case "parallel":
					p := b.Parallel()
					pipe = p.Pipe()
					p.Context(context.Background())
				}
				entered := make(chan struct{})
				ch := pipe.Run(func(ctx context.Context, _ string, value int) (int, error) {
					close(entered)
					<-ctx.Done()
					return value, ctx.Err()
				}).Receive()
				<-entered
				cancel()
				for r := range ch {
					t.Errorf("canceled output was sent: %+v", r)
				}
				if observed.Load() != 1 {
					t.Errorf("observer calls=%d want 1", observed.Load())
				}
			})
		})
	}
}

func TestPipelineModeCopiesStageListAndSharesInput(t *testing.T) {
	for _, mode := range []string{"serial", "parallel", "stream"} {
		t.Run(mode, func(t *testing.T) {
			items := []int{1}
			b := NewPipelineBuilder(items).Stage("a", 1).Stage("b", 1).Stage("c", 1)
			type executeFunc func(func(context.Context, string, int) (int, error)) ([]core.Result[int], error)
			var run executeFunc
			switch mode {
			case "serial":
				run = b.Serial().Stage("branch", 1).Run
			case "parallel":
				run = b.Parallel().Stage("branch", 1).Run
			case "stream":
				run = b.Stream().Stage("branch", 1).Run
			}
			// Both appends fit in the source's original spare capacity; a
			// shallow stage slice would overwrite the branch's final stage.
			b.Stage("source", 1)
			fn := func(_ context.Context, stage string, value int) (int, error) {
				switch stage {
				case "a", "b", "c":
					return value + 1, nil
				case "branch":
					return value + 10, nil
				case "source":
					return value + 100, nil
				default:
					return -1, fmt.Errorf("unexpected stage %s", stage)
				}
			}
			for attempt := 1; attempt <= 2; attempt++ {
				items[0] = attempt * 10
				branch, err := run(fn)
				if err != nil || len(branch) != 1 || branch[0].Err != nil || branch[0].Value != attempt*10+13 {
					t.Fatalf("branch=%+v/%v want %d", branch, err, attempt*10+13)
				}
				source, err := b.Run(fn)
				if err != nil || len(source) != 1 || source[0].Err != nil || source[0].Value != attempt*10+103 {
					t.Fatalf("source=%+v/%v want %d", source, err, attempt*10+103)
				}
			}
		})
	}
}
