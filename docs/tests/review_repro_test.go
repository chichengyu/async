//go:build audit

package review_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	async "github.com/chichengyu/async"
	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/group"
	"github.com/chichengyu/async/internal/maps"
	"github.com/chichengyu/async/internal/pipeline"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/ratelimit"
	"github.com/chichengyu/async/internal/retry"
	"github.com/chichengyu/async/internal/sliceops"
)

func TestReviewSafeCallPanicErrorExample(t *testing.T) {
	_, err := async.SafeCall(context.Background(), 1, func(context.Context, int) (int, error) {
		panic("example panic")
	})
	var panicErr *async.PanicError
	if !errors.As(err, &panicErr) || panicErr.Value != "example panic" || len(panicErr.Stack) == 0 {
		t.Fatalf("panic error = %#v, error = %v", panicErr, err)
	}

	want := errors.New("business error")
	err = async.SafeCallVoid(context.Background(), 1, func(context.Context, int) error { return want })
	panicErr = nil
	if !errors.Is(err, want) || errors.As(err, &panicErr) {
		t.Fatalf("business error = %v, panic error = %#v", err, panicErr)
	}
}

func waitUntil(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("synchronization timed out")
		}
		runtime.Gosched()
	}
}

func TestReviewTraceKey(t *testing.T) {
	old := core.GetTraceIDKey()
	defer core.SetTraceIDKey(old)
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("documented string key panicked: %v", p)
		}
	}()
	core.SetTraceIDKey("X-Trace-Id")
	ctx := core.WithTraceID(context.Background(), "example")
	if core.GetTraceID(ctx) != "example" {
		t.Error("trace ID did not round-trip")
	}
}

func TestReviewFlowClose(t *testing.T) {
	fl := pipeline.NewFlow(context.Background(), pipeline.FlowConfig[int]{
		Stages: []pipeline.Stage[int]{{Name: "echo", Concurrency: 1}}, BufSize: 1,
	}, func(_ context.Context, _ string, n int) (int, error) { return n, nil })
	for i := 0; i < 2; i++ {
		if _, err := fl.Submit(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan []core.Result[int], 1)
	go func() { done <- fl.Close() }()
	select {
	case results := <-done:
		if len(results) != 2 {
			t.Errorf("got %d results, want 2", len(results))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Close blocked on its own full result buffer")
		for range fl.Results() {
		}
		<-done
	}
}

func TestReviewFlowPanic(t *testing.T) {
	fl := pipeline.NewFlow(context.Background(), pipeline.FlowConfig[int]{
		Stages: []pipeline.Stage[int]{{Name: "panic", Concurrency: 1}}, BufSize: 2,
	}, func(_ context.Context, _ string, n int) (int, error) {
		if n == 1 {
			panic("example")
		}
		return n, nil
	})
	for i := 0; i < 2; i++ {
		_, _ = fl.Submit(context.Background(), i)
	}
	results := fl.Close()
	if len(results) != 2 {
		t.Errorf("submitted 2, received %d; error count=%d", len(results), fl.ErrCount())
	}
}

func TestReviewGroupTimeout(t *testing.T) {
	for _, variant := range []string{"timeout", "context"} {
		t.Run(variant, func(t *testing.T) {
			g := group.NewGroup[int](1).WithTimeout(0)
			started, release := make(chan struct{}), make(chan struct{})
			_ = g.Go(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil })
			<-started
			done := make(chan struct{})
			go func() {
				if variant == "timeout" {
					g.WaitTimeout(10 * time.Millisecond)
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
					defer cancel()
					g.WaitContext(ctx)
				}
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(100 * time.Millisecond):
				t.Error("10ms wait still blocked after 100ms")
			}
			close(release)
			<-done
		})
	}
}

func TestReviewGroupExternalPool(t *testing.T) {
	p := pool.NewPool[int](1).WithTimeout(0)
	defer p.Close()
	started, release := make(chan struct{}), make(chan struct{})
	_ = p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil })
	<-started
	g := group.NewGroup[int](1).WithPool(p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = g.Go(ctx, func(context.Context) (int, error) { return 2, nil })
	_ = g.Go(context.Background(), func(context.Context) (int, error) { return 3, nil })
	waitUntil(t, func() bool { return p.TotalCount() == 3 })
	cancel()
	close(release)
	p.Wait()
	results, ok := g.WaitTimeout(30 * time.Millisecond)
	if !ok || len(results) != 2 {
		t.Errorf("pool fully drained but group unfinished: ok=%v, results=%v, active=%d", ok, results, g.Active())
	}
}

func TestReviewPoolCancel(t *testing.T) {
	for _, mode := range []string{"normal", "maxResults", "sparseIndex"} {
		t.Run(mode, func(t *testing.T) {
			p := pool.NewPool[int](2).WithTimeout(0)
			defer p.Close()
			if mode == "maxResults" {
				p.WithMaxResults(1)
			}
			_ = p.SubmitAt(1, context.Background(), func(context.Context) (int, error) { return 1, nil })
			started, release, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
			fn := func(ctx context.Context) (int, error) {
				close(started)
				select {
				case <-ctx.Done():
					close(canceled)
					return 0, ctx.Err()
				case <-release:
					return 2, nil
				}
			}
			if mode == "sparseIndex" {
				_ = p.SubmitAt(0, context.Background(), fn)
			} else {
				_ = p.Submit(context.Background(), fn)
			}
			<-started
			p.WaitTimeout(10 * time.Millisecond)
			select {
			case <-canceled:
			case <-time.After(50 * time.Millisecond):
				t.Error("WaitTimeout did not cancel a context-aware task")
			}
			close(release)
		})
	}
}

func TestReviewPoolResetStreaming(t *testing.T) {
	p := pool.NewPool[int](1).WithStreaming(2)
	defer p.Close()
	_ = p.Submit(context.Background(), func(context.Context) (int, error) { return 1, nil })
	p.Wait()
	for range p.StreamResults() {
	}
	p.ResetWait()
	select {
	case _, ok := <-p.StreamResults():
		if !ok {
			t.Error("ResetWait retained a closed stream; the next Submit writes to this channel")
		}
	default:
	}
}

func TestReviewSliceFailFast(t *testing.T) {
	for _, mode := range []string{"normal", "shard", "chunk", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			p := sliceops.Par(1).FF()
			switch mode {
			case "shard":
				p = p.Shard(1)
			case "chunk":
				p = p.Chunk(1)
			case "timeout":
				p = p.TO(time.Second)
			}
			var calls atomic.Int32
			_, err := sliceops.NewRunner[int, int](context.Background(), []int{1, 2, 3, 4}, p).Map(func(context.Context, int) (int, error) {
				calls.Add(1)
				return 0, errors.New("example")
			})
			if calls.Load() != 1 {
				t.Errorf("single-worker failfast called all %d callbacks", calls.Load())
			}
			if err == nil {
				t.Error("missing aggregate error")
			}
		})
	}
}

func TestReviewMapTimeout(t *testing.T) {
	res := maps.NewMapChain(context.Background(), map[int]int{1: 1}).Timeout(time.Millisecond).Map(func(ctx context.Context, _, _ int) (int, error) {
		if _, ok := ctx.Deadline(); !ok {
			return 0, errors.New("no configured deadline in callback")
		}
		return 1, nil
	})
	if res.Error() != nil {
		t.Error(res.Error())
	}
}

func TestReviewRetryCanceledZeroBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	_, err := retry.RetryWithBackoff(ctx, func(context.Context) (int, error) {
		calls++
		cancel()
		return 0, context.Canceled
	}, 3, 0, 0)
	if calls != 1 || !errors.Is(err, context.Canceled) {
		t.Errorf("calls=%d, err=%v", calls, err)
	}
}

func TestReviewTokenBucketNegative(t *testing.T) {
	tb := ratelimit.NewTokenBucket(0, 1)
	if !tb.Allow() {
		t.Fatal("initial token absent")
	}
	if !tb.AllowN(-2) {
		t.Fatal("nonpositive request should succeed")
	}
	if tb.Allow() {
		t.Error("negative request minted tokens in an exhausted zero-rate bucket")
	}
}

func TestReviewSliceReplace(t *testing.T) {
	for _, end := range []int{3, 4} {
		t.Run(string(rune('0'+end)), func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("valid shrinking Replace panicked: %v", p)
				}
			}()
			input := []int{1, 2, 3, 4, 5}
			want := slices.Replace(slices.Clone(input), 1, end, 9)
			got := sliceops.NewSliceBuilder(context.Background(), input).Replace(1, end, 9).Values()
			if !slices.Equal(got, want) {
				t.Errorf("got=%v want=%v", got, want)
			}
		})
	}
}

func TestReviewSlowRate(t *testing.T) {
	rl := ratelimit.NewRateLimiter(1, 10*time.Second).WithStrategy(ratelimit.Reject)
	defer rl.Close()
	if err := rl.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := rl.Acquire(context.Background()); err == nil {
		t.Error("1/10s limiter replenished after just 1.1s")
	}
}

func TestReviewMapRace(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		data := make(map[int]int, 16)
		for i := 0; i < 16; i++ {
			data[i] = i
		}
		gate := make(chan struct{})
		var entered atomic.Int32
		maps.NewMapChain(context.Background(), data).Worker(16).FailFast().ForEach(func(context.Context, int, int) error {
			if entered.Add(1) == 16 {
				close(gate)
			}
			<-gate
			return errors.New("example")
		})
	}
}

func TestReviewLimiterSizeRace(t *testing.T) {
	rl := ratelimit.NewRateLimiter(1, time.Hour)
	defer rl.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			rl.Resize(1 + i%2)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = rl.Size()
		}
	}()
	wg.Wait()
}

func TestReviewPoolStreamCrash(t *testing.T) {
	if mode := os.Getenv("GO_ASYNC_REVIEW_STREAM_CRASH"); mode != "" {
		p := pool.NewPool[int](1).WithTimeout(0).WithStreaming(2)
		if mode == "reset" {
			_ = p.Submit(context.Background(), func(context.Context) (int, error) { return 1, nil })
			p.Wait()
			p.ResetWait()
			_ = p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil })
			p.Wait()
		} else {
			started, release := make(chan struct{}), make(chan struct{})
			_ = p.Submit(context.Background(), func(ctx context.Context) (int, error) {
				close(started)
				<-ctx.Done()
				<-release
				return 0, ctx.Err()
			})
			<-started
			p.WaitTimeout(time.Millisecond)
			close(release)
		}
		p.Close()
		return
	}
	for _, mode := range []string{"reset", "timeout"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestReviewPoolStreamCrash$", "-test.timeout=5s")
		cmd.Env = append(os.Environ(), "GO_ASYNC_REVIEW_STREAM_CRASH="+mode)
		out, err := cmd.CombinedOutput()
		if err != nil {
			if bytes.Contains(out, []byte("panic: send on closed channel")) {
				t.Errorf("%s: child process crashed with send on closed channel", mode)
			} else {
				t.Errorf("%s: unexpected child failure: %v\n%s", mode, err, out)
			}
		}
	}
}
