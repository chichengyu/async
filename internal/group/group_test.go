package group

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

func init() {
	core.SetDefaultMaxResults(100000)
}

func TestMultiGroupTimeoutThenWaitForUncooperativeTasks(t *testing.T) {
	mg := NewGroup[int](1).Shard(2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); mg.Close() })
	started := make(chan int, 2)
	for shard := 0; shard < 2; shard++ {
		value := shard
		if err := mg.GoKeyed(uint64(shard), context.Background(), func(context.Context) (int, error) {
			started <- value
			<-release
			return value, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	<-started
	<-started
	results, ok := mg.WaitTimeout(10 * time.Millisecond)
	if ok || len(results) != 0 {
		t.Fatalf("timeout results = %v, ok = %v", results, ok)
	}
	done := make(chan []core.Result[int], 1)
	go func() { done <- mg.Wait() }()
	select {
	case got := <-done:
		t.Fatalf("Wait returned before task release: %v", got)
	default:
	}
	unblock()
	select {
	case got := <-done:
		if len(got) != 2 || got[0].Value != 0 || got[1].Value != 1 || got[0].Err != nil || got[1].Err != nil {
			t.Fatalf("final results = %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not finish after task release")
	}
}

func TestMultiGroupConcurrentWaitersReceiveResults(t *testing.T) {
	mg := NewGroup[int](1).Shard(2)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); mg.Close() })
	if err := mg.GoKeyed(0, context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := mg.GoKeyed(1, context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	results := make(chan []core.Result[int], 2)
	go func() { results <- mg.Wait() }()
	go func() { results <- mg.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for !mg.groups[0].waiting.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !mg.groups[0].waiting.Load() {
		t.Fatal("wait did not begin")
	}
	unblock()
	for i := 0; i < 2; i++ {
		select {
		case got := <-results:
			if len(got) != 2 || got[0].Value != 1 || got[1].Value != 2 {
				t.Fatalf("waiter %d results = %v", i, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waiter did not finish")
		}
	}
}

func TestMultiGroupStreamingAndCallback(t *testing.T) {
	const shards, tasks = 4, 80
	var callbacks atomic.Int64
	mg := NewGroup[int](2).Shard(shards).WithStreaming(tasks).WithResultCallback(func(r core.Result[int]) {
		callbacks.Add(1)
	})
	defer mg.Close()
	stream := mg.StreamResults()
	collected := make(chan []int, 1)
	go func() {
		var values []int
		for r := range stream {
			values = append(values, r.Value)
		}
		collected <- values
	}()
	for i := 0; i < tasks; i++ {
		value := i
		if err := mg.GoKeyed(uint64(i%shards), context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if got := mg.Wait(); len(got) != tasks {
		t.Fatalf("Wait returned %d results, want %d", len(got), tasks)
	}
	select {
	case values := <-collected:
		sort.Ints(values)
		if len(values) != tasks {
			t.Fatalf("stream returned %d results, want %d", len(values), tasks)
		}
		for i, value := range values {
			if value != i {
				t.Fatalf("stream value[%d] = %d", i, value)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("merged stream did not close")
	}
	if got := callbacks.Load(); got != tasks {
		t.Fatalf("callback count = %d, want %d", got, tasks)
	}
}

func TestGroupShardConcurrentWait(t *testing.T) {
	for i := 0; i < 100; i++ {
		g, _ := NewGroup[int](1).WithContext(context.Background())
		start := make(chan struct{})
		done := make(chan struct{})
		go func() {
			<-start
			g.Wait()
			close(done)
		}()
		close(start)
		mg := g.Shard(2)
		<-done
		mg.Close()
	}
}

func TestGroupShardConcurrentSubmit(t *testing.T) {
	for i := 0; i < 100; i++ {
		g, ctx := NewGroup[int](1).WithFailFast(context.Background())
		start := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			<-start
			done <- g.Go(ctx, func(context.Context) (int, error) { return 1, nil })
		}()
		close(start)
		mg := g.Shard(2)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		mg.Close()
	}
}

func TestMultiBuilderWaitKeepsOtherShardRunning(t *testing.T) {
	err := NewMultiBuilder[int]().Worker(1).Shards(2).FailFast().Run(func(ctx context.Context, mg *MultiGroup[int]) error {
		started, release := make(chan struct{}), make(chan struct{})
		if err := mg.GetShard(1).Go(ctx, func(ctx context.Context) (int, error) {
			close(started)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-release:
				return 42, nil
			}
		}); err != nil {
			return err
		}
		<-started
		done := make(chan []core.Result[int], 1)
		go func() { done <- mg.Wait() }()
		select {
		case <-ctx.Done():
			close(release)
			<-done
			return errors.New("normal wait canceled builder context")
		case results := <-done:
			close(release)
			return fmt.Errorf("wait returned before release: %v", results)
		case <-time.After(30 * time.Millisecond):
		}
		close(release)
		results := <-done
		if len(results) != 1 || results[0].Err != nil || results[0].Value != 42 {
			return fmt.Errorf("unexpected results: %v", results)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultiGroupCloseCancelsSharedContext(t *testing.T) {
	for _, failFast := range []bool{false, true} {
		t.Run(fmt.Sprint("failFast=", failFast), func(t *testing.T) {
			g := NewGroup[int](1)
			var ctx context.Context
			if failFast {
				g, ctx = g.WithFailFast(context.Background())
			} else {
				g, ctx = g.WithContext(context.Background())
			}
			mg := g.Shard(2)
			mg.Close()
			if ctx.Err() == nil {
				t.Fatal("shared context remains active after close")
			}
		})
	}
}

func TestMultiGroupWaitWithContextKeepsOtherShardRunning(t *testing.T) {
	g, ctx := NewGroup[int](1).WithContext(context.Background())
	mg := g.Shard(2)
	defer mg.Close()
	started, release := make(chan struct{}), make(chan struct{})
	if err := mg.GetShard(1).Go(ctx, func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-release:
			return 42, nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	done := make(chan []core.Result[int], 1)
	go func() { done <- mg.Wait() }()
	select {
	case <-ctx.Done():
		close(release)
		<-done
		t.Fatal("normal completion canceled the shared context")
	case results := <-done:
		close(release)
		t.Fatalf("wait returned before release: %v", results)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	results := <-done
	if len(results) != 1 || results[0].Err != nil || results[0].Value != 42 {
		t.Fatalf("unexpected results: %v", results)
	}
}

func TestMultiGroupWaitKeepsOtherShardRunning(t *testing.T) {
	g, ctx := NewGroup[int](1).WithFailFast(context.Background())
	mg := g.Shard(2)
	defer mg.Close()
	started, release := make(chan struct{}), make(chan struct{})
	if err := mg.GetShard(1).Go(ctx, func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-release:
			return 42, nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	done := make(chan []core.Result[int], 1)
	go func() { done <- mg.Wait() }()
	select {
	case results := <-done:
		close(release)
		t.Fatalf("wait returned before the other shard was released: %v", results)
	case <-ctx.Done():
		close(release)
		<-done
		t.Fatal("normal completion canceled the shared context")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	results := <-done
	if len(results) != 1 || results[0].Err != nil || results[0].Value != 42 {
		t.Fatalf("unexpected results: %v", results)
	}
}

func TestWaitContextCancellationDuringInFlightSubmission(t *testing.T) {
	g := NewGroup[int](1)
	g.addInFlight.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	returned := make(chan bool, 1)
	go func() {
		_, ok := g.WaitContext(ctx)
		returned <- ok
	}()
	select {
	case ok := <-returned:
		if ok {
			g.addInFlight.Add(-1)
			g.Wait()
			t.Fatal("canceled wait unexpectedly completed")
		}
		g.addInFlight.Add(-1)
		g.Wait()
	case <-time.After(250 * time.Millisecond):
		g.addInFlight.Add(-1)
		<-returned
		g.Wait()
		t.Fatal("canceled wait blocked on in-flight submission")
	}
}

func TestWaitContextCancelsLateRegisteredTask(t *testing.T) {
	g := NewGroup[int](1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.addInFlight.Add(1)
	_, ok := g.WaitContext(ctx)
	if ok {
		g.addInFlight.Add(-1)
		g.Wait()
		t.Fatal("canceled wait unexpectedly completed")
	}
	taskCtx, taskCancel := context.WithCancel(context.Background())
	g.groupRecordCancel(taskCancel)
	g.addInFlight.Add(-1)
	g.Wait()
	if taskCtx.Err() == nil {
		taskCancel()
		t.Fatal("late registered task was not canceled")
	}
}

func TestWaitContextCancelsTaskAfterPrecheck(t *testing.T) {
	g := NewGroup[int](1)
	taskCtx, taskCancel, err := g.groupPrecheck(context.Background(), g.addResult, -1, "Go")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := g.WaitContext(ctx); ok {
		t.Fatal("canceled wait unexpectedly completed")
	}
	g.groupRecordCancel(taskCancel)
	task := groupTask[int]{taskCtx: taskCtx, taskCancel: taskCancel, record: g.addResult, fn: func(context.Context) (int, error) { return 1, nil }}
	if err := g.groupSendTask(context.Background(), task, g.addResult, taskCancel); !errors.Is(err, context.Canceled) && !errors.Is(err, core.ErrGroupWaited) {
		t.Fatalf("late task submission returned %v", err)
	}
	results := g.Wait()
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("expected one discarded task result, got %+v", results)
	}
}

func TestAutoScaleCheckThresholdsAndBounds(t *testing.T) {
	g := NewGroup[int](2)
	defer g.Wait()
	cfg := &core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 3, ScaleUpThreshold: 0.75, ScaleDownThreshold: 0.25, ScaleUpChecks: 2, ScaleDownChecks: 2, ScaleUpFactor: 2, ScaleDownFactor: 0.5}
	up, down := 0, 0
	g.busy.Store(2)
	g.autoScaleMu.Lock()
	g.performAutoScaleCheck(cfg, &up, &down)
	if got := g.Worker(); got != 2 || up != 1 {
		g.autoScaleMu.Unlock()
		t.Fatalf("first high-load check: workers=%d up=%d", got, up)
	}
	g.performAutoScaleCheck(cfg, &up, &down)
	if got := g.Worker(); got != 3 || up != 0 {
		g.autoScaleMu.Unlock()
		t.Fatalf("scale up: workers=%d up=%d", got, up)
	}
	g.busy.Store(0)
	g.performAutoScaleCheck(cfg, &up, &down)
	g.performAutoScaleCheck(cfg, &up, &down)
	got := g.Worker()
	g.autoScaleMu.Unlock()
	if got != 1 {
		t.Fatalf("scale down: workers=%d, want 1", got)
	}
}

func TestBuilderFailFastCancelsRunContext(t *testing.T) {
	boom := errors.New("task failed")
	t.Run("group", func(t *testing.T) {
		err := NewGroupBuilder[int]().Worker(1).FailFast().Run(func(ctx context.Context, g *Group[int]) error {
			if err := g.Go(ctx, func(context.Context) (int, error) { return 0, boom }); err != nil {
				return err
			}
			g.Wait()
			return ctx.Err()
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run context error = %v, want canceled", err)
		}
	})
	t.Run("no result", func(t *testing.T) {
		err := NewGroupNoResultBuilder().Worker(1).FailFast().Run(func(ctx context.Context, g *NoResult) error {
			if err := g.Go(ctx, func(context.Context) error { return boom }); err != nil {
				return err
			}
			g.Wait()
			return ctx.Err()
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run context error = %v, want canceled", err)
		}
	})
	t.Run("multi", func(t *testing.T) {
		err := NewMultiBuilder[int]().Worker(1).Shards(2).FailFast().Run(func(ctx context.Context, g *MultiGroup[int]) error {
			if err := g.GetShard(1).Go(ctx, func(context.Context) (int, error) { return 0, boom }); err != nil {
				return err
			}
			g.Wait()
			return ctx.Err()
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run context error = %v, want canceled", err)
		}
	})
}

func TestBuilderFailFastCancelsRunningSibling(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprintf("external=%v", external), func(t *testing.T) {
			b := NewGroupBuilder[int]().Worker(2).FailFast()
			if external {
				p := pool.NewPool[int](2)
				defer p.Close()
				b.Pool(p)
			}
			started := make(chan struct{})
			observed := make(chan error, 1)
			err := b.Run(func(ctx context.Context, g *Group[int]) error {
				if err := g.Go(ctx, func(ctx context.Context) (int, error) {
					close(started)
					select {
					case <-ctx.Done():
						observed <- ctx.Err()
					case <-time.After(5 * time.Second):
						observed <- errors.New("cancellation timed out")
					}
					return 0, nil
				}); err != nil {
					return err
				}
				<-started
				if err := g.Go(ctx, func(context.Context) (int, error) { return 0, errors.New("failure") }); err != nil {
					return err
				}
				return <-observed
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("sibling error=%v", err)
			}
		})
	}
}

func TestMultiBuilderFailFastCancelsOtherShard(t *testing.T) {
	started := make(chan struct{})
	observed := make(chan error, 1)
	err := NewMultiBuilder[int]().Worker(1).Shards(2).FailFast().Run(func(ctx context.Context, mg *MultiGroup[int]) error {
		if err := mg.GetShard(0).Go(ctx, func(ctx context.Context) (int, error) {
			close(started)
			select {
			case <-ctx.Done():
				observed <- ctx.Err()
			case <-time.After(5 * time.Second):
				observed <- errors.New("cancellation timed out")
			}
			return 0, nil
		}); err != nil {
			return err
		}
		<-started
		if err := mg.GetShard(1).Go(ctx, func(context.Context) (int, error) { return 0, errors.New("failure") }); err != nil {
			return err
		}
		return <-observed
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sibling error=%v", err)
	}
}

func TestBuilderWithoutFailFastDoesNotCancelContext(t *testing.T) {
	err := NewGroupBuilder[int]().Worker(1).Run(func(ctx context.Context, g *Group[int]) error {
		if err := g.Go(ctx, func(context.Context) (int, error) { return 0, errors.New("failure") }); err != nil {
			return err
		}
		g.Wait()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("context canceled without FailFast: %v", err)
	}
}

func TestMultiGroupRoundRobinCounterOverflow(t *testing.T) {
	mg := &MultiGroup[int]{groups: []*Group[int]{NewGroup[int](1), NewGroup[int](1), NewGroup[int](1)}}
	defer mg.Close()
	mg.nextIdx.Store(uint64(math.MaxInt) + 1)
	if err := mg.Go(context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
		t.Fatal(err)
	}
	if got := mg.Wait(); len(got) != 1 || got[0].Value != 1 {
		t.Fatalf("results=%v", got)
	}
}

// ==================== NewGroup / DefaultGroup ====================

func TestGroupExternalPoolTerminalPathsRegression(t *testing.T) {
	for _, mode := range []string{"canceled", "drop", "reject", "closed"} {
		t.Run(mode, func(t *testing.T) {
			p := pool.NewPool[int](1).WithTimeout(0)
			defer p.Close()
			started, release := make(chan struct{}), make(chan struct{})
			if err := p.Submit(context.Background(), func(context.Context) (int, error) {
				close(started)
				<-release
				return 1, nil
			}); err != nil {
				t.Fatal(err)
			}
			<-started
			if mode == "drop" || mode == "reject" {
				p.WithMaxPending(1)
				if mode == "drop" {
					p.WithOverflow(core.OverflowDrop)
				} else {
					p.WithOverflow(core.OverflowError)
				}
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "closed" {
				close(release)
				p.Close()
			}
			var callbacks atomic.Int32
			g := NewGroup[int](1).WithPool(p).WithResultCallback(func(core.Result[int]) {
				callbacks.Add(1)
				panic("callback failure")
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			if err := g.Go(ctx, func(context.Context) (int, error) { calls.Add(1); return 3, nil }); err != nil {
				t.Fatal(err)
			}
			if mode == "canceled" {
				if err := g.Go(context.Background(), func(context.Context) (int, error) { return 4, nil }); err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(time.Second)
				for p.TotalCount() < 3 && time.Now().Before(deadline) {
					runtime.Gosched()
				}
				if p.TotalCount() != 3 {
					close(release)
					t.Fatal("group tasks did not reach external pool")
				}
				cancel()
				close(release)
			}
			results, ok := g.WaitTimeout(time.Second)
			if mode == "drop" || mode == "reject" {
				close(release)
			}
			want := 1
			if mode == "canceled" {
				want = 2
			}
			var failures int64
			for _, result := range results {
				if result.Err != nil {
					failures++
				}
			}
			if !ok || len(results) != want || calls.Load() != 0 || int(callbacks.Load()) != want || g.Active() != 0 || failures != 1 || g.FailCount() != failures {
				t.Fatalf("mode=%s complete=%v results=%v calls=%d callbacks=%d active=%d failures=%d", mode, ok, results, calls.Load(), callbacks.Load(), g.Active(), g.FailCount())
			}
			if g.FirstError() == nil {
				t.Fatal("terminal error not recorded")
			}
			g.Close()
		})
	}
}

func TestFailFastExternalPoolRejectionDoesNotCancelContext(t *testing.T) {
	for _, mode := range []string{"closed", "drop", "reject"} {
		t.Run(mode, func(t *testing.T) {
			p := pool.NewPool[int](1).WithTimeout(0)
			defer p.Close()
			if mode == "closed" {
				p.Close()
			} else {
				started, release := make(chan struct{}), make(chan struct{})
				defer close(release)
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 0, nil }); err != nil {
					t.Fatal(err)
				}
				<-started
				p.WithMaxPending(1)
				if mode == "drop" {
					p.WithOverflow(core.OverflowDrop)
				} else {
					p.WithOverflow(core.OverflowError)
				}
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 0, nil }); err != nil {
					t.Fatal(err)
				}
			}
			recorded := make(chan struct{})
			g, ctx := NewGroup[int](1).WithPool(p).WithResultCallback(func(core.Result[int]) { close(recorded) }).WithFailFast(context.Background())
			defer g.Close()
			var ran atomic.Bool
			if err := g.Go(ctx, func(context.Context) (int, error) { ran.Store(true); return 0, nil }); err != nil {
				t.Fatal(err)
			}
			select {
			case <-recorded:
			case <-time.After(5 * time.Second):
				t.Fatal("rejection not recorded")
			}
			contextErr := ctx.Err()
			results, ok := g.WaitTimeout(5 * time.Second)
			if !ok || len(results) != 1 || results[0].Err == nil || g.FailCount() != 1 || g.Active() != 0 || ran.Load() || contextErr != nil {
				t.Fatalf("complete=%v results=%v failures=%d active=%d ran=%v context before wait=%v", ok, results, g.FailCount(), g.Active(), ran.Load(), contextErr)
			}
		})
	}
}

func TestGroupBoundedWaitAndLateStreamRegression(t *testing.T) {
	for _, variant := range []string{"timeout", "context"} {
		t.Run(variant, func(t *testing.T) {
			g := NewGroup[int](1).WithTimeout(0).WithStreaming(4)
			started, release := make(chan struct{}), make(chan struct{})
			if err := g.Go(context.Background(), func(ctx context.Context) (int, error) {
				close(started)
				<-release
				return 5, ctx.Err()
			}); err != nil {
				t.Fatal(err)
			}
			<-started
			done := make(chan bool, 1)
			go func() {
				var ok bool
				if variant == "timeout" {
					_, ok = g.WaitTimeout(time.Millisecond)
				} else {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					_, ok = g.WaitContext(ctx)
				}
				done <- ok
			}()
			select {
			case ok := <-done:
				if ok {
					t.Error("unfinished task reported complete")
				}
			case <-time.After(time.Second):
				close(release)
				<-done
				g.Close()
				t.Fatal("bounded wait blocked on a running callback")
			}
			if _, err := g.Reset(); err == nil {
				t.Error("reset permitted while old task is running")
			}
			close(release)
			results := g.Wait()
			if len(results) != 1 || results[0].Value != 5 {
				t.Fatalf("late result lost: %v", results)
			}
			oldStream := g.StreamResults()
			if r, ok := <-oldStream; !ok || r.Value != 5 {
				t.Fatalf("late stream result lost: %+v, open=%v", r, ok)
			}
			if _, ok := <-oldStream; ok {
				t.Fatal("old stream has extra results")
			}
			if _, err := g.Reset(); err != nil {
				t.Fatal(err)
			}
			if err := g.Go(context.Background(), func(context.Context) (int, error) { return 6, nil }); err != nil {
				t.Fatal(err)
			}
			if got := g.Wait(); len(got) != 1 || got[0].Value != 6 {
				t.Fatalf("reset result=%v", got)
			}
			newStream := g.StreamResults()
			if r, ok := <-newStream; !ok || r.Value != 6 || r.Err != nil {
				t.Fatalf("reset stream result=%+v, open=%v", r, ok)
			}
			if _, ok := <-newStream; ok {
				t.Fatal("reset stream has extra results")
			}
		})
	}
}

func TestNewGroup_Defaults(t *testing.T) {
	g := NewGroup[int](5)
	defer g.Close()
	if g.Worker() != 5 {
		t.Fatalf("expected concurrency 5, got %d", g.Worker())
	}
}

func TestNewGroup_ZeroConcurrency(t *testing.T) {
	g := NewGroup[int](0)
	defer g.Close()
	if g.Worker() != 1 {
		t.Fatalf("expected concurrency 1, got %d", g.Worker())
	}
}

func TestNewGroup_NegativeConcurrency(t *testing.T) {
	g := NewGroup[int](-5)
	defer g.Close()
	if g.Worker() != 1 {
		t.Fatalf("expected concurrency 1 for -5, got %d", g.Worker())
	}
}

func TestDefaultGroup(t *testing.T) {
	g := DefaultGroup[int]()
	defer g.Close()
	if g.Worker() != core.IO() {
		t.Fatalf("DefaultGroup Worker = %d, want %d", g.Worker(), core.IO())
	}
}

// ==================== Go - Basic Scenarios ====================

func TestGo_NormalExecution(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			return 42, nil
		})
		if err != nil {
			t.Errorf("Go(%d) error: %v", i, err)
		}
	}

	results := g.Wait()
	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Value != 42 || !r.Ok() {
			t.Errorf("unexpected result: %+v", r)
		}
	}
}

func TestGo_ErrorExecution(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	testErr := errors.New("intentional error")
	for i := 0; i < 10; i++ {
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			return 0, testErr
		})
		if err != nil {
			t.Errorf("Go(%d) submit error: %v", i, err)
		}
	}

	results := g.Wait()
	if g.FailCount() != 10 {
		t.Errorf("FailCount = %d, want 10", g.FailCount())
	}
	for _, r := range results {
		if r.Ok() {
			t.Errorf("expected error result, got %+v", r)
		}
	}
}

func TestGo_PanicRecovery(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		panic("boom")
	})
	if err != nil {
		t.Errorf("Go submit error: %v", err)
	}

	results := g.Wait()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].IsPanic() {
		t.Error("expected panic error")
	}
}

func TestGo_PanicWithErrorType(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		panic(errors.New("panic error"))
	})
	if err != nil {
		t.Errorf("Go submit error: %v", err)
	}

	results := g.Wait()
	if !results[0].IsPanic() {
		t.Error("expected panic result")
	}
}

func TestGo_NonDefaultResults(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		val := i
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			return val * 2, nil
		})
		if err != nil {
			t.Errorf("Go(%d) error: %v", i, err)
		}
	}

	results := g.Wait()
	if g.TotalCount() != 50 {
		t.Errorf("TotalCount = %d, want 50", g.TotalCount())
	}
	if g.OkCount() != 50 {
		t.Errorf("OkCount = %d, want 50", g.OkCount())
	}
	if g.FailCount() != 0 {
		t.Errorf("FailCount = %d, want 0", g.FailCount())
	}

	vals := make(map[int]bool)
	for _, r := range results {
		vals[r.Value] = true
	}
	for i := 0; i < 50; i++ {
		if !vals[i*2] {
			t.Errorf("missing result value: %d", i*2)
		}
	}
}

// ==================== Go - Context Cancel ====================

func TestGo_CanceledContext(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	})
	if err == nil {
		t.Error("Go with cancelled context should return error")
	}
}

func TestGo_TimeoutContext(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	})
	if err == nil {
		t.Error("Go with expired context should return error")
	}
}

// ==================== GoAt ====================

func TestGoAt_Basic(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.GoAt(0, ctx, func(ctx context.Context) (int, error) {
		return 100, nil
	})
	if err != nil {
		t.Errorf("GoAt(0): %v", err)
	}
	err = g.GoAt(1, ctx, func(ctx context.Context) (int, error) {
		return 200, nil
	})
	if err != nil {
		t.Errorf("GoAt(1): %v", err)
	}

	results := g.Wait()
	if len(results) < 2 {
		t.Fatalf("expected >=2 results, got %d", len(results))
	}
	found100, found200 := false, false
	for _, r := range results {
		if r.Value == 100 {
			found100 = true
		}
		if r.Value == 200 {
			found200 = true
		}
	}
	if !found100 || !found200 {
		t.Error("GoAt results missing expected values")
	}
}

// ==================== GoWithTimeout ====================

func TestGoWithTimeout(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.GoWithTimeout(ctx, 50*time.Millisecond, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})
	if err != nil {
		t.Errorf("GoWithTimeout submit error: %v", err)
	}

	results := g.Wait()
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0].Err == nil {
		t.Error("expected timeout error")
	}
}

// ==================== Go - After Wait ====================

func TestGo_AfterWait(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	g.Wait()

	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		return 2, nil
	})
	if !errors.Is(err, core.ErrGroupWaited) {
		t.Errorf("expected ErrGroupWaited after Wait, got %v", err)
	}
}

// ==================== Wait - Timeout / Context ====================

func TestWait_Timeout(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return 1, nil
		})
	}

	results, completed := g.WaitTimeout(50 * time.Millisecond)
	if completed {
		t.Error("expected completed=false (timeout)")
	}
	_ = results
}

func TestWait_ContextCancel(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return 1, nil
		})
	}

	waitCtx, waitCancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		waitCancel()
	}()
	results, completed := g.WaitContext(waitCtx)
	if completed {
		t.Error("expected completed=false on context cancel")
	}
	_ = results
}

func TestWait_Twice(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})

	results1 := g.Wait()
	results2 := g.Wait()
	if len(results1) != 1 || len(results2) != 1 {
		t.Errorf("Wait twice: len1=%d len2=%d", len(results1), len(results2))
	}
}

// ==================== WaitTimeout / WaitContext ====================

func TestWaitTimeout_CompletesBeforeTimeout(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return 100, nil
		})
	}

	results, completed := g.WaitTimeout(10 * time.Second)
	if !completed {
		t.Error("WaitTimeout should complete (ok=true)")
	}
	if len(results) != 20 {
		t.Errorf("WaitTimeout len = %d, want 20", len(results))
	}
}

func TestWaitTimeout_Empty(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	results, completed := g.WaitTimeout(time.Second)
	if !completed {
		t.Error("WaitTimeout on empty should return ok=true")
	}
	if len(results) != 0 {
		t.Errorf("WaitTimeout on empty: len=%d, want 0", len(results))
	}
}

func TestWaitContext_NormalComplete(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) { return 99, nil })
	results, completed := g.WaitContext(context.Background())
	if !completed {
		t.Error("WaitContext should complete (ok=true)")
	}
	if len(results) != 1 || results[0].Value != 99 {
		t.Errorf("WaitContext results: %+v", results)
	}
}

// ==================== FailFast ====================

func TestFailFast_Basic(t *testing.T) {
	g := NewGroup[int](4)
	ctx := g.ctx
	g, ctx = g.WithFailFast(ctx)
	defer g.Close()

	testErr := errors.New("fail fast error")
	var executed atomic.Int64
	for i := 0; i < 50; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			executed.Add(1)
			return 0, testErr
		})
	}

	results := g.Wait()
	if g.FailCount() < 1 {
		t.Error("should have failures")
	}
	_ = results
	t.Logf("executed: %d, results: %d", executed.Load(), len(results))
}

// ==================== WithTimeout (Group) ====================

func TestWithTimeout_Group(t *testing.T) {
	g := NewGroup[int](4)
	g.WithTimeout(50 * time.Millisecond)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})

	results := g.Wait()
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0].Err == nil {
		t.Error("expected timeout error")
	}
}

// ==================== Worker / Active / Busy ====================

func TestWorker_Active_Busy(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()

	if g.Worker() != 4 {
		t.Errorf("Worker = %d, want 4", g.Worker())
	}
	if g.Busy() != 0 {
		t.Errorf("Busy (idle) = %d, want 0", g.Busy())
	}
}

func TestStats(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	g.Go(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err") })
	g.Wait()

	stats := g.Stats()
	if stats.TotalTask != 2 {
		t.Errorf("TotalTask = %d, want 2", stats.TotalTask)
	}
	if stats.OkTask != 1 {
		t.Errorf("OkTask = %d, want 1", stats.OkTask)
	}
	if stats.FailTask != 1 {
		t.Errorf("FailTask = %d, want 1", stats.FailTask)
	}
}

// ==================== Results Accessors ====================

func TestErrors_HasError(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	testErr := errors.New("my error")
	g.Go(ctx, func(ctx context.Context) (int, error) { return 0, testErr })
	g.Go(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	g.Wait()

	errs := g.Errors()
	if len(errs) != 1 {
		t.Errorf("Errors len = %d, want 1", len(errs))
	}
	if errs[0] != testErr {
		t.Errorf("Errors[0] = %v, want %v", errs[0], testErr)
	}
	if !g.HasError() {
		t.Error("HasError should be true")
	}
}

func TestFirstError(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	testErr := errors.New("first error")
	g.Go(ctx, func(ctx context.Context) (int, error) { return 0, testErr })
	g.Wait()

	err := g.FirstError()
	if err == nil {
		t.Error("FirstError should return non-nil")
	}
	t.Logf("FirstError = %v", err)
}

func TestJoinErrors(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	g.Go(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	g.Wait()

	if err := g.JoinErrors(); err != nil {
		t.Errorf("JoinErrors with no errors should return nil, got: %v", err)
	}

	g2 := NewGroup[int](4)
	defer g2.Close()
	g2.Go(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err1") })
	g2.Go(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err2") })
	g2.Wait()

	err := g2.JoinErrors()
	if err == nil {
		t.Error("JoinErrors should return non-nil")
	}
}

func TestValues(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		val := i
		g.Go(ctx, func(ctx context.Context) (int, error) { return val * 10, nil })
	}
	g.Wait()

	vals := g.Values()
	sort.Ints(vals)
	if len(vals) != 10 {
		t.Errorf("Values len = %d, want 10", len(vals))
	}
	for i, v := range vals {
		if v != i*10 {
			t.Errorf("Values[%d] = %d, want %d", i, v, i*10)
		}
	}
}

// ==================== Reset ====================

func TestResetRetainsResultCallback(t *testing.T) {
	callbackResults := make(chan core.Result[int], 2)
	g := NewGroup[int](1).WithResultCallback(func(result core.Result[int]) { callbackResults <- result })
	defer g.Close()
	for cycle := 1; cycle <= 2; cycle++ {
		value := cycle
		if err := g.Go(context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
			t.Fatal(err)
		}
		results := g.Wait()
		if len(results) != 1 || results[0].Value != value || results[0].Err != nil {
			t.Fatalf("cycle %d results=%v", cycle, results)
		}
		select {
		case got := <-callbackResults:
			if got.Value != value || got.Err != nil {
				t.Fatalf("cycle %d callback=%+v", cycle, got)
			}
		default:
			t.Fatalf("cycle %d missing callback", cycle)
		}
		select {
		case got := <-callbackResults:
			t.Fatalf("cycle %d extra callback=%+v", cycle, got)
		default:
		}
		if cycle == 1 {
			if _, err := g.Reset(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestResetRecreatesStreamWithoutOldResults(t *testing.T) {
	g := NewGroup[int](1).WithStreaming(2)
	defer g.Close()
	oldStream := g.StreamResults()
	if err := g.Go(context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
		t.Fatal(err)
	}
	if results := g.Wait(); len(results) != 1 || results[0].Value != 1 {
		t.Fatalf("first results=%v", results)
	}
	if _, err := g.Reset(); err != nil {
		t.Fatal(err)
	}
	newStream := g.StreamResults()
	if oldStream == newStream {
		t.Fatal("Reset reused old stream")
	}
	if result, ok := <-oldStream; !ok || result.Value != 1 || result.Err != nil {
		t.Fatalf("old stream result=%+v open=%v", result, ok)
	}
	if _, ok := <-oldStream; ok {
		t.Fatal("old stream was not closed")
	}
	if err := g.Go(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	if results := g.Wait(); len(results) != 1 || results[0].Value != 2 {
		t.Fatalf("second results=%v", results)
	}
	if result, ok := <-newStream; !ok || result.Value != 2 || result.Err != nil {
		t.Fatalf("new stream result=%+v open=%v", result, ok)
	}
	if _, ok := <-newStream; ok {
		t.Fatal("new stream has extra results")
	}
}

func TestReset(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	g.Wait()

	g2, err := g.Reset()
	if err != nil {
		t.Errorf("Reset error: %v", err)
	}

	g2.Go(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	results := g2.Wait()
	if len(results) != 1 || results[0].Value != 2 {
		t.Errorf("after Reset: %+v", results)
	}
	g2.Close()
}

// ==================== Shard / MultiGroup ====================

func TestShard(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	mg := g.Shard(4)
	defer mg.Close()

	var counter atomic.Int64
	for i := 0; i < 100; i++ {
		err := mg.Go(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
		if err != nil {
			t.Errorf("Shard.Go(%d): %v", i, err)
		}
	}
	results := mg.Wait()
	if counter.Load() != 100 {
		t.Errorf("counter = %d, want 100", counter.Load())
	}
	_ = len(results)
}

func TestDefaultShard(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	mg := g.DefaultShard()
	defer mg.Close()

	mg.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	results := mg.Wait()
	if len(results) != 1 || results[0].Value != 1 {
		t.Errorf("DefaultShard: %+v", results)
	}
}

func TestMultiGroup_WithTimeout(t *testing.T) {
	g := NewGroup[int](4)
	ctx := g.ctx
	mg := g.DefaultShard()
	mg.WithTimeout(50 * time.Millisecond)
	defer mg.Close()

	mg.Go(ctx, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})
	results := mg.Wait()
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0].Err == nil {
		t.Error("expected timeout error")
	}
}

func TestMultiGroup_Stats(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.DefaultShard()
	defer mg.Close()
	ctx := context.Background()

	mg.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	mg.Wait()

	if mg.TotalWorker() == 0 {
		t.Error("TotalWorker should be >0")
	}
	if mg.TotalTaskCount() != 1 {
		t.Errorf("TotalTaskCount = %d, want 1", mg.TotalTaskCount())
	}
	if mg.Errors() != nil {
		t.Errorf("Errors = %v, want nil", mg.Errors())
	}
	vals := mg.Values()
	if len(vals) != 1 || vals[0] != 1 {
		t.Errorf("Values = %v, want [1]", vals)
	}
}

func TestMultiGroup_GoKeyed(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.DefaultShard()
	defer mg.Close()
	ctx := context.Background()

	err := mg.GoKeyed(42, ctx, func(ctx context.Context) (int, error) {
		return 100, nil
	})
	if err != nil {
		t.Errorf("GoKeyed: %v", err)
	}
	results := mg.Wait()
	if len(results) != 1 || results[0].Value != 100 {
		t.Errorf("GoKeyed results: %+v", results)
	}
}

// ==================== StreamResults ====================

type gatedDoneContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
}

func (c *gatedDoneContext) Done() <-chan struct{} {
	close(c.entered)
	<-c.release
	return c.Context.Done()
}

func TestFailFastCountsCascadedCancellation(t *testing.T) {
	g, ctx := NewGroup[int](4).WithFailFast(context.Background())
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	bomb := errors.New("failure")
	for i := 0; i < 4; i++ {
		i := i
		if err := g.Go(ctx, func(ctx context.Context) (int, error) {
			started <- struct{}{}
			<-release
			if i == 0 {
				return 0, bomb
			}
			<-ctx.Done()
			return 0, ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		<-started
	}
	close(release)
	results := g.Wait()
	if len(results) != 4 || len(g.Errors()) != 4 || g.FailCount() != 4 || g.OkCount() != 0 {
		t.Fatalf("results=%v failures=%d successes=%d", results, g.FailCount(), g.OkCount())
	}
}

func TestFailFastCountsCanceledCallback(t *testing.T) {
	g, ctx := NewGroup[int](1).WithFailFast(context.Background())
	if err := g.Go(ctx, func(context.Context) (int, error) { return 0, context.Canceled }); err != nil {
		t.Fatal(err)
	}
	results := g.Wait()
	if len(results) != 1 || !errors.Is(results[0].Err, context.Canceled) || g.FailCount() != 1 || g.OkCount() != 0 || !g.HasError() {
		t.Fatalf("results=%v failures=%d successes=%d hasError=%v", results, g.FailCount(), g.OkCount(), g.HasError())
	}
}

func TestCanceledSubmissionConcurrentWithWait(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%v", indexed), func(t *testing.T) {
			base, cancel := context.WithCancel(context.Background())
			cancel()
			ctx := &gatedDoneContext{Context: base, entered: make(chan struct{}), release: make(chan struct{})}
			g := NewGroup[int](1).WithStreaming(1)
			result := make(chan any, 1)
			go func() {
				defer func() { result <- recover() }()
				fn := func(context.Context) (int, error) { return 1, nil }
				if indexed {
					_ = g.GoAt(0, ctx, fn)
				} else {
					_ = g.Go(ctx, fn)
				}
			}()
			<-ctx.entered
			waitDone := make(chan struct{})
			go func() { g.Wait(); close(waitDone) }()
			deadline := time.After(5 * time.Second)
			for !g.waiting.Load() {
				select {
				case <-waitDone:
					t.Fatal("Wait returned while submission precheck was in progress")
				case <-deadline:
					t.Fatal("Wait did not start")
				default:
					runtime.Gosched()
				}
			}
			close(ctx.release)
			if recovered := <-result; recovered != nil {
				t.Fatalf("submission panicked: %v", recovered)
			}
			<-waitDone
			results := g.Wait()
			if len(results) != 1 || !errors.Is(results[0].Err, context.Canceled) || g.FailCount() != 1 {
				t.Fatalf("results=%v failures=%d", results, g.FailCount())
			}
		})
	}
}

func TestGoAtAfterWaitPreservesExistingResult(t *testing.T) {
	g := NewGroup[int](1)
	if err := g.GoAt(0, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	g.Wait()
	if err := g.GoAt(0, context.Background(), func(context.Context) (int, error) { return 99, nil }); !errors.Is(err, core.ErrGroupWaited) {
		t.Fatalf("got error %v", err)
	}
	results := g.Wait()
	if len(results) != 2 || results[0].Value != 42 || results[0].Err != nil || !errors.Is(results[1].Err, core.ErrGroupWaited) {
		t.Fatalf("completed result changed: %v", results)
	}
}

func TestGoAtReverseCompletionWithGo(t *testing.T) {
	g := NewGroup[int](4)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	for i := 0; i < 3; i++ {
		i := i
		if err := g.GoAt(i, context.Background(), func(context.Context) (int, error) {
			<-releases[i]
			return 10 + i, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Go(context.Background(), func(context.Context) (int, error) { return 99, nil }); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for want := int64(1); want <= 3; want++ {
		for g.OkCount() < want {
			if time.Now().After(deadline) {
				for i := 0; i < 3; i++ {
					select {
					case <-releases[i]:
					default:
						close(releases[i])
					}
				}
				g.Wait()
				t.Fatal("task did not finish")
			}
			runtime.Gosched()
		}
		close(releases[3-int(want)])
	}
	results := g.Wait()
	if len(results) != 4 {
		t.Fatalf("results=%v", results)
	}
	for i := 0; i < 3; i++ {
		if !results[i].Occupied || results[i].Value != 10+i {
			t.Fatalf("results=%v", results)
		}
	}
	if results[3].Value != 99 {
		t.Fatalf("results=%v", results)
	}
}

func TestGoAtMixedWithGoPreservesResults(t *testing.T) {
	g := NewGroup[int](2)
	release := make(chan struct{})
	started := make(chan struct{})
	if err := g.GoAt(2, context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 42, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := g.Go(context.Background(), func(context.Context) (int, error) {
		return 7, nil
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for g.OkCount() == 0 {
		if time.Now().After(deadline) {
			close(release)
			g.Wait()
			t.Fatal("ordinary task did not complete")
		}
		runtime.Gosched()
	}
	close(release)
	results := g.Wait()
	if len(results) < 3 || results[2].Value != 42 || !results[2].Occupied {
		t.Fatalf("indexed result missing: %v", results)
	}
	values := g.Values()
	sort.Ints(values)
	if len(values) != 2 || values[0] != 7 || values[1] != 42 {
		t.Fatalf("values=%v", values)
	}
}

func TestGoAtDuplicateIndexKeepsBothResults(t *testing.T) {
	g := NewGroup[int](1)
	for _, value := range []int{1, 2} {
		value := value
		if err := g.GoAt(0, context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
			t.Fatal(err)
		}
	}
	results := g.Wait()
	if len(results) != 2 || results[0].Value != 2 || results[1].Value != 1 || g.TotalCount() != 2 {
		t.Fatalf("results=%v total=%d", results, g.TotalCount())
	}
}

func TestGoAtConcurrentDuplicateIndexPreservesAllResults(t *testing.T) {
	const tasks = 128
	g := NewGroup[int](16)
	var submitted sync.WaitGroup
	submitted.Add(tasks)
	for i := 0; i < tasks; i++ {
		go func(value int) {
			defer submitted.Done()
			if err := g.GoAt(7, context.Background(), func(context.Context) (int, error) {
				return value, nil
			}); err != nil {
				t.Errorf("submit %d: %v", value, err)
			}
		}(i)
	}
	submitted.Wait()
	results := g.Wait()
	seen := make([]bool, tasks)
	count := 0
	for _, result := range results {
		if !result.Occupied {
			continue
		}
		if result.Err != nil || result.Value < 0 || result.Value >= tasks || seen[result.Value] {
			t.Fatalf("invalid or duplicate result: %+v", result)
		}
		seen[result.Value] = true
		count++
	}
	if count != tasks || g.TotalCount() != tasks || g.OkCount() != tasks || g.FailCount() != 0 {
		t.Fatalf("results=%d total=%d ok=%d failed=%d", count, g.TotalCount(), g.OkCount(), g.FailCount())
	}
}

func TestGoAtRejectsNegativeIndex(t *testing.T) {
	g := NewGroup[int](1)
	defer g.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("GoAt panicked: %v", r)
		}
	}()
	if err := g.GoAt(-1, ctx, func(context.Context) (int, error) { return 0, nil }); !errors.Is(err, core.ErrInvalidIndex) {
		t.Fatalf("error=%v, want ErrInvalidIndex", err)
	}
	if err := g.GoAt(-1, context.Background(), func(context.Context) (int, error) { return 0, nil }); !errors.Is(err, core.ErrInvalidIndex) {
		t.Fatalf("error=%v, want ErrInvalidIndex", err)
	}
}

func TestValuesExcludeUnoccupiedGoAtSlots(t *testing.T) {
	g := NewGroup[int](1)
	if err := g.GoAt(3, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.GoAt(5, context.Background(), func(context.Context) (int, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	g.Wait()
	values := g.Values()
	if len(values) != 2 || values[0] != 42 || values[1] != 0 || g.TotalCount() != 2 {
		t.Fatalf("values=%v total=%d", values, g.TotalCount())
	}
}

func TestStreamResultsAfterWaitWithOverflow(t *testing.T) {
	g := NewGroup[int](1).WithStreaming(1)
	defer g.Close()
	for i := 0; i < 3; i++ {
		if err := g.Go(context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
			t.Fatal(err)
		}
	}
	results := g.Wait()
	count := 0
	for range g.StreamResults() {
		count++
	}
	if count != 1 || len(results) != 3 {
		t.Fatalf("stream=%d results=%d", count, len(results))
	}
}

func TestStreamResults(t *testing.T) {
	g := NewGroup[int](4)
	g.WithStreaming(200)
	defer g.Close()
	ctx := context.Background()

	n := 50
	for i := 0; i < n; i++ {
		val := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return val, nil
		})
	}

	ch := g.StreamResults()
	var collected []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range ch {
			mu.Lock()
			collected = append(collected, r.Value)
			mu.Unlock()
		}
	}()

	g.Wait()
	wg.Wait()

	sort.Ints(collected)
	if len(collected) != n {
		t.Errorf("StreamResults collected = %d, want %d", len(collected), n)
	}
}

func TestExternalPoolResultCallbackPanic(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%v", closed), func(t *testing.T) {
			p := pool.NewPool[int](1).WithTimeout(0)
			defer p.Close()
			if closed {
				p.Close()
			}
			var calls atomic.Int32
			g := NewGroup[int](1).WithPool(p).WithResultCallback(func(core.Result[int]) {
				calls.Add(1)
				panic("callback failure")
			})
			if err := g.Go(context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
				t.Fatal(err)
			}
			results, ok := g.WaitTimeout(5 * time.Second)
			if !ok || len(results) != 1 || calls.Load() != 1 || g.Active() != 0 {
				t.Fatalf("complete=%v results=%v callbacks=%d active=%d", ok, results, calls.Load(), g.Active())
			}
			if closed {
				if results[0].Err == nil || g.FailCount() != 1 {
					t.Fatalf("results=%v failures=%d", results, g.FailCount())
				}
			} else if results[0].Err != nil || results[0].Value != 42 || g.FailCount() != 0 {
				t.Fatalf("results=%v failures=%d", results, g.FailCount())
			}
		})
	}
}

func TestResultCallbackPanicDoesNotDuplicateTask(t *testing.T) {
	var calls atomic.Int32
	g := NewGroup[int](1).WithResultCallback(func(core.Result[int]) {
		calls.Add(1)
		panic("callback failure")
	})
	if err := g.Go(context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	results := g.Wait()
	if len(results) != 1 || results[0].Value != 42 || results[0].Err != nil || calls.Load() != 1 || g.FailCount() != 0 {
		t.Fatalf("results=%v callbacks=%d failures=%d", results, calls.Load(), g.FailCount())
	}
}

func TestResultCallback(t *testing.T) {
	var collected []int
	var mu sync.Mutex
	g := NewGroup[int](4)
	g.WithResultCallback(func(r core.Result[int]) {
		mu.Lock()
		collected = append(collected, r.Value)
		mu.Unlock()
	})
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		val := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			return val, nil
		})
	}
	g.Wait()

	sort.Ints(collected)
	if len(collected) != 20 {
		t.Errorf("ResultCallback collected = %d, want 20", len(collected))
	}
}

// ==================== Builder ====================

func TestBuilder_Basic(t *testing.T) {
	g := NewGroupBuilder[int]().
		Worker(4).
		Timeout(10 * time.Second).
		FailFast().
		Build()
	defer g.Close()

	ctx := context.Background()
	g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	results := g.Wait()
	if len(results) != 1 || results[0].Value != 1 {
		t.Errorf("Builder basic: results = %+v", results)
	}
}

func TestBuilder_Run(t *testing.T) {
	err := NewGroupBuilder[int]().
		Worker(4).
		Timeout(10 * time.Second).
		Run(func(ctx context.Context, g *Group[int]) error {
			g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
			results := g.Wait()
			if len(results) != 1 || results[0].Value != 1 {
				t.Errorf("Run results: %+v", results)
			}
			return nil
		})
	if err != nil {
		t.Errorf("Run error: %v", err)
	}
}

func TestBuilder_DefaultWorker(t *testing.T) {
	g := NewGroupBuilder[int]().
		DefaultWorker().
		Build()
	defer g.Close()

	if g.Worker() != core.IO() {
		t.Errorf("DefaultWorker = %d, want %d", g.Worker(), core.IO())
	}
}

func TestBuilder_Streaming(t *testing.T) {
	g := NewGroupBuilder[int]().
		Worker(2).
		Streaming(10).
		Build()
	defer g.Close()
	ctx := context.Background()

	ch := g.StreamResults()
	var collected []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range ch {
			mu.Lock()
			collected = append(collected, r.Value)
			mu.Unlock()
		}
	}()

	for i := 0; i < 10; i++ {
		v := i
		g.Go(ctx, func(ctx context.Context) (int, error) { return v, nil })
	}
	g.Wait()
	wg.Wait()

	sort.Ints(collected)
	if len(collected) != 10 {
		t.Errorf("Streaming collected = %d, want 10", len(collected))
	}
}

// ==================== NoResult ====================

func TestNoResult_Basic(t *testing.T) {
	g := NewNoResult(4)
	defer g.Close()
	ctx := context.Background()

	var counter atomic.Int64
	for i := 0; i < 20; i++ {
		err := g.Go(ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		if err != nil {
			t.Errorf("NoResult Go(%d): %v", i, err)
		}
	}
	g.Wait()
	if counter.Load() != 20 {
		t.Errorf("counter = %d, want 20", counter.Load())
	}
}

func TestNoResult_Error(t *testing.T) {
	g := NewNoResult(4)
	defer g.Close()
	ctx := context.Background()

	testErr := errors.New("no result error")
	g.Go(ctx, func(ctx context.Context) error {
		return testErr
	})
	g.Wait()

	if !g.HasError() {
		t.Error("HasError should be true")
	}
	errs := g.Errors()
	if len(errs) != 1 || errs[0] != testErr {
		t.Errorf("NoResult errors: %v", errs)
	}
}

func TestNoResult_Panic(t *testing.T) {
	g := NewNoResult(4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) error {
		panic("no result panic")
	})
	g.Wait()
	if g.FailCount() != 1 {
		t.Errorf("FailCount = %d, want 1 (panic counts as failure)", g.FailCount())
	}
}

func TestNoResult_FailFast(t *testing.T) {
	g := NewNoResult(4)
	ctx := g.ctx
	g, ctx = g.WithFailFast(ctx)
	defer g.Close()

	var executed atomic.Int64
	for i := 0; i < 20; i++ {
		g.Go(ctx, func(ctx context.Context) error {
			executed.Add(1)
			return errors.New("fail")
		})
	}
	g.Wait()
	_ = executed.Load()
}

func TestNoResult_AfterWait(t *testing.T) {
	g := NewNoResult(4)
	defer g.Close()
	g.Wait()

	err := g.Go(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, core.ErrGroupWaited) {
		t.Errorf("expected ErrGroupWaited, got: %v", err)
	}
}

func TestDefaultNoResult(t *testing.T) {
	g := DefaultNoResult()
	defer g.Close()
	if g.Worker() != core.IO() {
		t.Errorf("DefaultNoResult Worker = %d, want %d", g.Worker(), core.IO())
	}
}

func TestNoResult_WithTimeout(t *testing.T) {
	g := NewNoResult(4)
	g.WithTimeout(50 * time.Millisecond)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return nil
		}
	})
	g.Wait()
	if g.FailCount() != 1 {
		t.Errorf("FailCount = %d, want 1 (timeout)", g.FailCount())
	}
}

// ==================== GroupNoResultBuilder ====================

func TestGroupNoResultBuilder(t *testing.T) {
	g := NewGroupNoResultBuilder().
		Worker(4).
		Timeout(10 * time.Second).
		Build()
	defer g.Close()

	ctx := context.Background()
	g.Go(ctx, func(ctx context.Context) error { return nil })
	g.Wait()
	if g.HasError() {
		t.Error("should have no errors")
	}
}

// ==================== WithTraceID ====================

func TestWithTraceID(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := g.ctx
	g, ctx = g.WithTraceID(ctx)

	traceID := core.GetTraceID(ctx)
	if traceID == "" {
		t.Error("WithTraceID should ensure trace_id in context")
	}
	if len(traceID) != 32 {
		t.Errorf("trace_id length = %d, want 32", len(traceID))
	}
}

func TestWithContext_Group(t *testing.T) {
	g := NewGroup[int](4)
	ctx := g.ctx
	g, ctx = g.WithContext(ctx)
	defer g.Close()

	g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	results := g.Wait()
	if len(results) != 1 {
		t.Errorf("WithContext results: %+v", results)
	}
}

// ==================== AutoScale ====================

func TestAutoScale(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()

	cfg := &core.AutoScaleConfig{
		MinWorkers:         2,
		MaxWorkers:         8,
		CheckInterval:      100 * time.Millisecond,
		ScaleUpThreshold:   0.8,
		ScaleDownThreshold: 0.2,
	}
	g.EnableAutoScale(cfg)
	if !g.IsAutoScaleEnabled() {
		t.Error("IsAutoScaleEnabled should be true")
	}

	g.DisableAutoScale()
	if g.IsAutoScaleEnabled() {
		t.Error("IsAutoScaleEnabled should be false after DisableAutoScale")
	}
}

// ==================== WithSubmitTimeout ====================

func TestWithSubmitTimeout(t *testing.T) {
	g := NewGroup[int](1)
	g.WithSubmitTimeout(10 * time.Millisecond)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); g.Close() })
	ctx := context.Background()
	if err := g.Go(ctx, func(ctx context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	for i := 0; i < cap(g.taskCh); i++ {
		if err := g.Go(ctx, func(context.Context) (int, error) { return 2, nil }); err != nil {
			t.Fatal(err)
		}
	}
	var rejectedRan atomic.Bool
	err := g.Go(ctx, func(ctx context.Context) (int, error) {
		rejectedRan.Store(true)
		return 3, nil
	})
	if !errors.Is(err, core.ErrSubmitTimeout) {
		t.Fatalf("Go error = %v, want ErrSubmitTimeout", err)
	}
	unblock()
	results := g.Wait()
	if rejectedRan.Load() {
		t.Fatal("timed-out task executed")
	}
	if g.TotalCount() != 3 || g.OkCount() != 2 || g.FailCount() != 1 {
		t.Fatalf("counts: total=%d ok=%d fail=%d, want 3, 2, 1", g.TotalCount(), g.OkCount(), g.FailCount())
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	var timeoutResults int
	for _, result := range results {
		if errors.Is(result.Err, core.ErrSubmitTimeout) {
			timeoutResults++
		} else if result.Err != nil {
			t.Fatalf("unexpected result error: %v", result.Err)
		}
	}
	if timeoutResults != 1 {
		t.Fatalf("got %d timeout results, want 1", timeoutResults)
	}
}

func TestBlockedSubmitCanceled(t *testing.T) {
	g := NewGroup[int](1).WithSubmitTimeout(time.Hour)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); g.Close() })
	if err := g.Go(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := g.Go(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ran atomic.Bool
	submitted := make(chan error, 1)
	go func() {
		submitted <- g.Go(ctx, func(context.Context) (int, error) { ran.Store(true); return 3, nil })
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for g.active.Load() != 3 {
		select {
		case err := <-submitted:
			t.Fatalf("submit returned before cancellation: %v", err)
		case <-deadline.C:
			t.Fatal("submit did not reach the full queue")
		default:
			runtime.Gosched()
		}
	}
	cancel()
	select {
	case err := <-submitted:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("submit error = %v, want context.Canceled", err)
		}
	case <-deadline.C:
		t.Fatal("canceled submit did not return")
	}
	unblock()
	results := g.Wait()
	if ran.Load() || g.TotalCount() != 3 || g.OkCount() != 2 || g.FailCount() != 1 {
		t.Fatalf("ran=%t total=%d ok=%d fail=%d", ran.Load(), g.TotalCount(), g.OkCount(), g.FailCount())
	}
	var canceled int
	for _, result := range results {
		if errors.Is(result.Err, context.Canceled) {
			canceled++
		} else if result.Err != nil {
			t.Fatalf("unexpected result error: %v", result.Err)
		}
	}
	if len(results) != 3 || canceled != 1 {
		t.Fatalf("results=%d canceled=%d, want 3 and 1", len(results), canceled)
	}
}

// ==================== GoAtWithTimeout ====================

func TestAutoScaleShrinkWithFullQueue(t *testing.T) {
	g := NewGroup[int](2)
	g.EnableAutoScale(&core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 2, CheckInterval: time.Hour})
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	for i := 0; i < 2; i++ {
		if err := g.Go(context.Background(), func(context.Context) (int, error) { started <- struct{}{}; <-release; return 0, nil }); err != nil {
			t.Fatal(err)
		}
	}
	<-started
	<-started
	queued := make(chan struct{}, 2)
	finish := make(chan struct{})
	for i := 0; i < 2; i++ {
		if err := g.Go(context.Background(), func(context.Context) (int, error) { queued <- struct{}{}; <-finish; return 0, nil }); err != nil {
			t.Fatal(err)
		}
	}
	g.DisableAutoScale()
	close(release)
	<-queued
	var exceeded bool
	select {
	case <-queued:
		exceeded = true
	case <-time.After(200 * time.Millisecond):
	}
	close(finish)
	g.Wait()
	if exceeded {
		t.Fatal("actual concurrency exceeds Worker() after shrink")
	}
}

func TestAutoScaleShrinkThenGrowWithBusyWorkers(t *testing.T) {
	g := NewGroup[int](2)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	for i := 0; i < 2; i++ {
		if err := g.Go(context.Background(), func(context.Context) (int, error) { started <- struct{}{}; <-release; return 0, nil }); err != nil {
			t.Fatal(err)
		}
	}
	<-started
	<-started
	g.autoScaleMu.Lock()
	g.resizeWorkerPool(1)
	g.resizeWorkerPool(2)
	g.autoScaleMu.Unlock()
	close(release)
	g.Wait()
	if got := g.workerCount.Load(); got != 0 {
		t.Fatalf("remaining workers=%d", got)
	}
	if _, err := g.Reset(); err != nil {
		t.Fatal(err)
	}
	if got := g.workerCount.Load(); got != 2 {
		t.Fatalf("workers after Reset=%d, want 2", got)
	}
	g.Wait()
}

func TestAutoScaleStopsAfterWait(t *testing.T) {
	g := NewGroup[int](1)
	g.EnableAutoScale(nil)
	if !g.IsAutoScaleEnabled() {
		t.Fatal("autoscale not enabled")
	}
	g.Wait()
	if g.IsAutoScaleEnabled() {
		t.Fatal("autoscale remains enabled after Wait")
	}
}

func TestAutoScaleConcurrentLifecycle(t *testing.T) {
	for i := 0; i < 100; i++ {
		g := NewGroup[int](1)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				g.EnableAutoScale(nil)
				g.DisableAutoScale()
			}()
		}
		close(start)
		g.Wait()
		wg.Wait()
		if g.IsAutoScaleEnabled() {
			t.Fatal("autoscale enabled after Wait")
		}
	}
}

func TestAutoScaleRepeatedEnableKeepsFirstConfig(t *testing.T) {
	g := NewGroup[int](2)
	defer g.Close()
	first := &core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 3, CheckInterval: time.Hour}
	second := &core.AutoScaleConfig{MinWorkers: 3, MaxWorkers: 4, CheckInterval: time.Hour}
	g.EnableAutoScale(first)
	g.EnableAutoScale(second)
	first.MinWorkers = 3
	g.DisableAutoScale()
	if got := g.Worker(); got != 1 {
		t.Fatalf("workers=%d, want 1", got)
	}
}

func TestBuildAggregateNoResultSparseIndex(t *testing.T) {
	nr := NewNoResult(1)
	if err := nr.GoAt(3, context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	nr.Wait()
	total, failures, _, results := BuildAggregateNoResult(nr)
	if total != 1 || failures != 0 || len(results) != 4 {
		t.Fatalf("total=%d failures=%d results=%v", total, failures, results)
	}
}

func TestFillNoResultSkippedCounts(t *testing.T) {
	nr := NewNoResult(1)
	if err := nr.Go(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	nr.Wait()
	FillNoResultSkipped(nr, 3)
	FillNoResultSkipped(nr, 3)
	total, failures, _, results := BuildAggregateNoResult(nr)
	if nr.TotalCount() != 3 || nr.OkCount() != 1 || nr.FailCount() != 2 || total != 3 || failures != 2 || len(results) != 3 {
		t.Fatalf("counts: total=%d success=%d failed=%d aggregate=%d/%d results=%v", nr.TotalCount(), nr.OkCount(), nr.FailCount(), total, failures, results)
	}
}

func TestFillNoResultSkippedSparseIndices(t *testing.T) {
	nr := NewNoResult(1)
	if err := nr.GoAt(3, context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	nr.Wait()
	FillNoResultSkipped(nr, 4)
	FillNoResultSkipped(nr, 2)
	FillNoResultSkipped(nr, 4)
	total, failures, _, results := BuildAggregateNoResult(nr)
	if nr.TotalCount() != 4 || nr.OkCount() != 1 || nr.FailCount() != 3 || total != 4 || failures != 3 || len(results) != 4 {
		t.Fatalf("counts: total=%d success=%d failed=%d aggregate=%d/%d results=%v", nr.TotalCount(), nr.OkCount(), nr.FailCount(), total, failures, results)
	}
}

func TestGroupCountsWhileTaskRunning(t *testing.T) {
	g := NewGroup[int](1)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := g.Go(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 42, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	total, success, failed := g.TotalCount(), g.OkCount(), g.FailCount()
	close(release)
	g.Wait()
	if total != 1 || success != 0 || failed != 0 {
		t.Fatalf("running: total=%d success=%d failed=%d", total, success, failed)
	}
	if g.TotalCount() != 1 || g.OkCount() != 1 || g.FailCount() != 0 {
		t.Fatalf("completed: total=%d success=%d failed=%d", g.TotalCount(), g.OkCount(), g.FailCount())
	}
	if _, err := g.Reset(); err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if g.TotalCount() != 0 || g.OkCount() != 0 || g.FailCount() != 0 {
		t.Fatalf("reset: total=%d success=%d failed=%d", g.TotalCount(), g.OkCount(), g.FailCount())
	}
}

func TestMultiGroupCountsWhileTaskRunning(t *testing.T) {
	g := NewGroup[int](1)
	mg := g.Shard(2)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := mg.Go(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	total, success, failed := mg.TotalTaskCount(), mg.TotalOkCount(), mg.TotalFailCount()
	close(release)
	mg.Wait()
	if total != 1 || success != 0 || failed != 0 {
		t.Fatalf("running: total=%d success=%d failed=%d", total, success, failed)
	}
	if mg.TotalTaskCount() != 1 || mg.TotalOkCount() != 1 || mg.TotalFailCount() != 0 {
		t.Fatalf("completed: total=%d success=%d failed=%d", mg.TotalTaskCount(), mg.TotalOkCount(), mg.TotalFailCount())
	}
}

func TestGroupCountsRejectedAndRepeatedIndex(t *testing.T) {
	g := NewGroup[int](1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fn := func(context.Context) (int, error) { return 1, nil }
	if err := g.Go(ctx, fn); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled submission: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := g.GoAt(3, context.Background(), fn); err != nil {
			t.Fatal(err)
		}
	}
	g.Wait()
	if err := g.Go(context.Background(), fn); !errors.Is(err, core.ErrGroupWaited) {
		t.Fatalf("after wait: %v", err)
	}
	if g.TotalCount() != 4 || g.OkCount() != 2 || g.FailCount() != 2 {
		t.Fatalf("total=%d success=%d failed=%d", g.TotalCount(), g.OkCount(), g.FailCount())
	}
}

func TestGoAtWithTimeoutNegativeIndex(t *testing.T) {
	g := NewGroup[int](1)
	defer g.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := g.GoAtWithTimeout(-1, ctx, time.Second, func(context.Context) (int, error) { return 1, nil })
	if !errors.Is(err, core.ErrInvalidIndex) {
		t.Fatalf("error = %v, want ErrInvalidIndex", err)
	}
	err = g.GoAtWithTimeout(-1, context.Background(), time.Second, func(context.Context) (int, error) { return 1, nil })
	if !errors.Is(err, core.ErrInvalidIndex) {
		t.Fatalf("background context: error = %v, want ErrInvalidIndex", err)
	}
	if got := g.TotalCount(); got != 0 {
		t.Fatalf("TotalCount = %d, want 0", got)
	}
	nr := NewNoResult(1)
	defer nr.Close()
	if err := nr.GoAtWithTimeout(-1, context.Background(), time.Second, func(context.Context) error { return nil }); !errors.Is(err, core.ErrInvalidIndex) {
		t.Fatalf("NoResult: error = %v, want ErrInvalidIndex", err)
	}
}

func TestGoAtWithTimeout(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.GoAtWithTimeout(0, ctx, 30*time.Millisecond, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})
	if err != nil {
		t.Errorf("GoAtWithTimeout submit: %v", err)
	}
	results := g.Wait()
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0].Err == nil {
		t.Error("expected timeout error")
	}
}

// ==================== Production Concurrent Tests ====================

func TestProduction_MixedResults(t *testing.T) {
	g := NewGroup[int](runtime.NumCPU() * 2)
	defer g.Close()
	ctx := context.Background()

	var okCount, errorCount, panicCount atomic.Int64
	n := 1000

	for i := 0; i < n; i++ {
		mod := i % 5
		err := g.Go(ctx, func(ctx context.Context) (int, error) {
			switch mod {
			case 0, 1, 2:
				okCount.Add(1)
				return i, nil
			case 3:
				errorCount.Add(1)
				return 0, errors.New("planned error")
			default:
				panicCount.Add(1)
				panic("planned panic")
			}
		})
		if err != nil {
			t.Errorf("Go(%d) submit error: %v", i, err)
		}
	}

	results := g.Wait()
	if len(results) != n {
		t.Errorf("results len = %d, want %d", len(results), n)
	}
	if g.OkCount() != okCount.Load() {
		t.Errorf("OkCount = %d, want %d", g.OkCount(), okCount.Load())
	}
	if g.FailCount() != errorCount.Load()+panicCount.Load() {
		t.Errorf("FailCount = %d, want %d (err+panic)", g.FailCount(), errorCount.Load()+panicCount.Load())
	}

	panicResults := 0
	for _, r := range results {
		if r.IsPanic() {
			panicResults++
		}
	}
	if panicResults != int(panicCount.Load()) {
		t.Errorf("panic results = %d, want %d", panicResults, panicCount.Load())
	}
}

func TestProduction_HighConcurrency(t *testing.T) {
	g := NewGroup[int](runtime.NumCPU() * 4)
	defer g.Close()
	ctx := context.Background()

	n := 5000
	var counter atomic.Int64

	for i := 0; i < n; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
	}

	results := g.Wait()
	if len(results) != n {
		t.Errorf("results len = %d, want %d", len(results), n)
	}
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
}

func TestProduction_FailFastRace(t *testing.T) {
	g := NewGroup[int](runtime.NumCPU() * 2)
	ctx := g.ctx
	g, ctx = g.WithFailFast(ctx)
	defer g.Close()

	n := 500
	var started atomic.Int64

	for i := 0; i < n; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			started.Add(1)
			return 0, errors.New("fail")
		})
	}

	g.Wait()
	if started.Load() < int64(n) {
		t.Logf("FailFast stopped at %d/%d", started.Load(), n)
	}
}

func TestProduction_NoResultHighConcurrency(t *testing.T) {
	g := NewNoResult(runtime.NumCPU() * 4)
	defer g.Close()
	ctx := context.Background()

	n := 5000
	var counter atomic.Int64

	for i := 0; i < n; i++ {
		g.Go(ctx, func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	g.Wait()
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
	if g.OkCount() != int64(n) {
		t.Errorf("OkCount = %d, want %d", g.OkCount(), n)
	}
}

func TestProduction_ContextCancelDuringExecution(t *testing.T) {
	g := NewGroup[int](runtime.NumCPU())
	defer g.Close()

	pCtx, pCancel := context.WithCancel(context.Background())
	g, ctx := g.WithContext(pCtx)
	_ = ctx

	n := 500
	var started, completed atomic.Int64

	for i := 0; i < n; i++ {
		g.Go(ctx, func(ctx context.Context) (int, error) {
			started.Add(1)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(100 * time.Millisecond):
				completed.Add(1)
				return 1, nil
			}
		})
	}

	time.Sleep(20 * time.Millisecond)
	pCancel()
	g.Wait()

	t.Logf("started=%d completed=%d", started.Load(), completed.Load())
}

func TestProduction_BuilderRace(t *testing.T) {
	var wg sync.WaitGroup
	n := 50

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g := NewGroupBuilder[int]().
				Worker(runtime.NumCPU()).
				Timeout(10 * time.Second).
				Build()
			defer g.Close()

			ctx := context.Background()
			g.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
			g.Wait()
		}()
	}
	wg.Wait()
}

func TestProduction_MultiGroupRace(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.DefaultShard()
	defer mg.Close()
	ctx := context.Background()

	n := 2000
	var counter atomic.Int64

	for i := 0; i < n; i++ {
		mg.Go(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
	}

	results := mg.Wait()
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
	if len(results) != n {
		t.Errorf("results len = %d, want %d", len(results), n)
	}
}

func TestProduction_ValuesRace(t *testing.T) {
	g := NewGroup[int](runtime.NumCPU() * 2)
	defer g.Close()
	ctx := context.Background()

	n := 1000
	for i := 0; i < n; i++ {
		val := i
		g.Go(ctx, func(ctx context.Context) (int, error) {
			if val%2 == 0 {
				return val, nil
			}
			return 0, errors.New("odd")
		})
	}

	g.Wait()
	vals := g.Values()
	// Should only contain successful results
	if len(vals) != n/2 {
		t.Errorf("Values len = %d, want %d", len(vals), n/2)
	}
	sort.Ints(vals)
	for i, v := range vals {
		if v != i*2 {
			t.Errorf("Values[%d] = %d, want %d", i, v, i*2)
		}
	}
}

// ==================== Closed Pool 不死锁 ====================

func TestProduction_GroupClosedPool_NoDeadlock(t *testing.T) {
	p := pool.NewPool[int](16)
	p.Close()

	ctx := context.Background()
	b := NewGroupBuilder[int]()
	b.Context(ctx)
	b.Worker(2)
	b.Pool(p)
	g := b.Build()

	for i := 0; i < 10; i++ {
		_ = g.Go(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}

	done := make(chan struct{})
	go func() {
		g.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait timed out after 5s — possible deadlock")
	}
}

func TestProduction_GroupSubmitError_Consistency(t *testing.T) {
	p := pool.NewPool[int](16)
	p.Close()

	ctx := context.Background()
	b := NewGroupBuilder[int]()
	b.Context(ctx)
	b.Worker(4)
	b.Pool(p)
	g := b.Build()

	taskCount := 200
	var submitErrCount int64
	var wg sync.WaitGroup

	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := g.Go(ctx, func(ctx context.Context) (int, error) {
				return idx, nil
			}); err != nil {
				atomic.AddInt64(&submitErrCount, 1)
			}
		}(i)
	}
	wg.Wait()

	results := g.Wait()
	failCnt := g.FailCount()

	if failCnt == 0 {
		t.Error("expected some failures when using closed pool")
	}
	if len(results) < 1 {
		t.Error("expected results to be populated even on failure")
	}
	_ = submitErrCount
}

// ==================== GoAt Boundary Tests ====================

func TestGoAt_LargeIndex(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.GoAt(1000, ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Logf("GoAt(1000) error: %v", err)
	}

	results := g.Wait()
	t.Logf("GoAt(1000) results: %d", len(results))
}

func TestGoAt_CancelledContext(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := g.GoAt(0, ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Error("GoAt with cancelled context should return an error")
	}
	t.Logf("GoAt cancelled ctx error: %v", err)
}

func TestGoWithTimeout_Negative(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.GoWithTimeout(ctx, -1, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	t.Logf("GoWithTimeout(-1) error: %v", err)

	results := g.Wait()
	t.Logf("results count: %d", len(results))
}

func TestGoAtWithTimeout_LargeIndex(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	err := g.GoAtWithTimeout(1000, ctx, 100*time.Millisecond, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Logf("GoAtWithTimeout(1000) error: %v", err)
	}

	results := g.Wait()
	t.Logf("GoAtWithTimeout(1000) results: %d", len(results))
}

// ==================== NoResult Boundary Tests ====================

func TestNoResult_GoAt_LargeIndex(t *testing.T) {
	nr := NewNoResult(4)
	ctx := context.Background()

	err := nr.GoAt(1000, ctx, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Logf("NoResult.GoAt(1000) error: %v", err)
	}
	nr.Wait()
}

func TestNoResult_GoAt_CancelledContext(t *testing.T) {
	nr := NewNoResult(4)
	defer nr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := nr.GoAt(0, ctx, func(ctx context.Context) error {
		return nil
	})
	if err == nil {
		t.Error("NoResult.GoAt with cancelled context should return an error")
	}
	t.Logf("NoResult.GoAt cancelled ctx error: %v", err)
	nr.Wait()
}

func TestNoResult_GoWithTimeout_Negative(t *testing.T) {
	nr := NewNoResult(4)
	defer nr.Close()
	ctx := context.Background()

	err := nr.GoWithTimeout(ctx, -1, func(ctx context.Context) error {
		return nil
	})
	t.Logf("NoResult.GoWithTimeout(-1) error: %v", err)

	nr.Wait()
	t.Log("NoResult negative timeout completed")
}

// ==================== Group WaitTimeout Boundary ====================

func TestGroup_WaitTimeout_Negative(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 1, nil
	})

	results, ok := g.WaitTimeout(-1)
	t.Logf("Group WaitTimeout(-1): ok=%v, results=%d", ok, len(results))
}

func TestGroup_WaitTimeout_Zero(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 1, nil
	})

	results, ok := g.WaitTimeout(0)
	t.Logf("Group WaitTimeout(0): ok=%v, results=%d", ok, len(results))
}

// ==================== MultiGroup WaitTimeout / WaitContext ====================

func TestMultiGroup_WaitTimeout(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(4)
	defer mg.Close()
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		_ = mg.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return i, nil
		})
	}
	results, ok := mg.WaitTimeout(5 * time.Second)
	if !ok || len(results) != 100 {
		t.Fatalf("MultiGroup.WaitTimeout failed: ok=%v, len=%d", ok, len(results))
	}
}

func TestMultiGroup_WaitContext(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(4)
	defer mg.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		_ = mg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	results, ok := mg.WaitContext(ctx)
	if !ok || len(results) != 100 {
		t.Fatalf("MultiGroup.WaitContext failed: ok=%v, len=%d", ok, len(results))
	}
}

func TestMultiGroup_WaitContext_Cancel(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(4)
	defer mg.Close()
	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < 100; i++ {
		_ = mg.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return i, nil
		})
	}
	cancel()
	_, ok := mg.WaitContext(ctx)
	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
}

func TestMultiGroup_WaitTimeout_ActuallyTimesOut(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(4)
	defer mg.Close()
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		_ = mg.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return i, nil
		})
	}
	_, ok := mg.WaitTimeout(50 * time.Millisecond)
	if ok {
		t.Fatal("expected timeout (ok=false)")
	}
}

func TestMultiGroup_WaitContext_WithDeadline(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(4)
	defer mg.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	for i := 0; i < 100; i++ {
		_ = mg.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return i, nil
		})
	}
	_, ok := mg.WaitContext(ctx)
	if ok {
		t.Fatal("expected deadline exceeded (ok=false)")
	}
}

func TestMultiGroup_WaitTimeout_Negative(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(2)
	defer mg.Close()
	_, ok := mg.WaitTimeout(-1)
	if ok {
		t.Fatal("expected negative timeout → ok=false")
	}
}

func TestMultiGroup_WaitTimeout_Zero(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(2)
	defer mg.Close()
	for i := 0; i < 10; i++ {
		_ = mg.Go(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return i, nil
		})
	}
	_, ok := mg.WaitTimeout(0)
	if ok {
		t.Fatal("expected zero timeout → ok=false")
	}
}

func TestMultiGroup_WaitContext_AlreadyCancelled(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(2)
	defer mg.Close()
	for i := 0; i < 10; i++ {
		_ = mg.Go(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return i, nil
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok := mg.WaitContext(ctx)
	if ok {
		t.Fatal("expected already-cancelled context → ok=false")
	}
}

// ==================== MultiGroup 边界：空组 ====================

func TestMultiGroup_WaitTimeout_Empty(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(2)
	defer mg.Close()
	results, ok := mg.WaitTimeout(time.Second)
	if !ok {
		t.Fatal("empty MultiGroup WaitTimeout should return ok=true")
	}
	if len(results) != 0 {
		t.Fatalf("empty MultiGroup: len=%d, want 0", len(results))
	}
}

func TestMultiGroup_WaitContext_Empty(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(2)
	defer mg.Close()
	results, ok := mg.WaitContext(context.Background())
	if !ok {
		t.Fatal("empty MultiGroup WaitContext should return ok=true")
	}
	if len(results) != 0 {
		t.Fatalf("empty MultiGroup: len=%d, want 0", len(results))
	}
}

// ==================== MultiGroup 破坏性：重复调用 / 交叉调用 ====================

func TestMultiGroup_WaitTimeout_Twice(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(2)
	defer mg.Close()
	for i := 0; i < 100; i++ {
		_ = mg.Go(context.Background(), func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	r1, ok1 := mg.WaitTimeout(time.Second)
	r2, ok2 := mg.WaitTimeout(time.Second)
	if !ok1 || !ok2 || len(r1) != 100 || len(r2) != 100 {
		t.Fatalf("WaitTimeout twice: ok1=%v len1=%d ok2=%v len2=%d", ok1, len(r1), ok2, len(r2))
	}
}

func TestMultiGroup_WaitContext_Twice(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(2)
	defer mg.Close()
	for i := 0; i < 100; i++ {
		_ = mg.Go(context.Background(), func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	r1, ok1 := mg.WaitContext(context.Background())
	r2, ok2 := mg.WaitContext(context.Background())
	if !ok1 || !ok2 || len(r1) != 100 || len(r2) != 100 {
		t.Fatalf("WaitContext twice: ok1=%v len1=%d ok2=%v len2=%d", ok1, len(r1), ok2, len(r2))
	}
}

func TestMultiGroup_WaitTimeout_Then_WaitContext(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(2)
	defer mg.Close()
	for i := 0; i < 100; i++ {
		_ = mg.Go(context.Background(), func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	r1, ok1 := mg.WaitTimeout(time.Second)
	r2, ok2 := mg.WaitContext(context.Background())
	if !ok1 || !ok2 || len(r1) != 100 || len(r2) != 100 {
		t.Fatalf("WaitTimeout→WaitContext: ok1=%v len1=%d ok2=%v len2=%d", ok1, len(r1), ok2, len(r2))
	}
}

func TestMultiGroup_WaitContext_Then_WaitTimeout(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(2)
	defer mg.Close()
	for i := 0; i < 100; i++ {
		_ = mg.Go(context.Background(), func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}
	r1, ok1 := mg.WaitContext(context.Background())
	r2, ok2 := mg.WaitTimeout(time.Second)
	if !ok1 || !ok2 || len(r1) != 100 || len(r2) != 100 {
		t.Fatalf("WaitContext→WaitTimeout: ok1=%v len1=%d ok2=%v len2=%d", ok1, len(r1), ok2, len(r2))
	}
}

// ──────────────────────── WaitContext 取消传播修复验证 ────────────────────────
//
// Timed waits propagate cancellation and return a partial snapshot promptly.
// Wait joins background cleanup before final result and callback assertions.

// ==================== WaitContext 取消传播 — 核心验证 ====================

// TestWaitContext_CancelPropagation_SingleWorker
// 核心验证：单个 worker 执行阻塞在 <-ctx.Done() 的任务。
// WaitContext 超时取消后，cancelAll() 触达任务 → 任务立即返回 → WaitContext 不再永久阻塞。
func TestWaitContext_CancelPropagation_SingleWorker(t *testing.T) {
	g := NewGroup[int](1)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(taskCtx context.Context) (int, error) {
		<-taskCtx.Done()
		return 0, taskCtx.Err()
	})

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("WaitContext blocked too long: %v (cancel signal did NOT reach task)", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Error("expected cancellation error in result")
	}
	t.Logf("cancel propagation OK: returned in %v, err=%v", elapsed, results[0].Err)
}

// TestWaitContext_CancelPropagation_MultiWorker
// 核心验证：多个 worker 全部阻塞在 <-ctx.Done()，cancelAll 同时触达所有 worker。
func TestWaitContext_CancelPropagation_MultiWorker(t *testing.T) {
	g := NewGroup[int](8)
	defer g.Close()
	ctx := context.Background()
	n := 16

	for i := 0; i < n; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("WaitContext blocked too long with %d workers: %v", n, elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	errCount := 0
	for _, r := range results {
		if r.Err != nil {
			errCount++
		}
	}
	if errCount != n {
		t.Errorf("expected all %d results to have cancellation error, got %d with err", n, errCount)
	}
	t.Logf("multi-worker cancel OK: returned in %v, %d/%d results have errors", elapsed, errCount, n)
}

// ==================== WaitTimeout 取消传播 — 核心验证 ====================

// TestWaitTimeout_TimeoutPropagation
// 核心验证：WaitTimeout 超时后 cancelAll() 触达阻塞在 ctx.Done() 的任务。
func TestWaitTimeout_TimeoutPropagation(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	start := time.Now()
	results, ok := g.WaitTimeout(100 * time.Millisecond)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected timeout (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("WaitTimeout blocked too long: %v (timeout signal did NOT reach tasks)", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Err == nil {
			t.Errorf("result[%d]: expected error, got nil", i)
		}
	}
	t.Logf("WaitTimeout propagation OK: returned in %v", elapsed)
}

// ==================== WaitContext 超时场景 — Deadline 传播 ====================

// TestWaitContext_WithDeadline_Propagation
// 与 WithTimeout 相同语义，验证 deadline 到期时 cancelAll 触达任务。
func TestWaitContext_WithDeadline_Propagation(t *testing.T) {
	g := NewGroup[int](6)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	deadline := time.Now().Add(100 * time.Millisecond)
	deadlineCtx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(deadlineCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected deadline exceeded (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("WaitContext blocked too long with deadline: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("deadline propagation OK: returned in %v", elapsed)
}

// ==================== WaitContext 取消传播 — 外部主动取消 ====================

// TestWaitContext_ExternalCancel_Propagation
// 外部调用 cancel()（非 timeout/deadline 触发）时，cancelAll 同样触达任务。
func TestWaitContext_ExternalCancel_Propagation(t *testing.T) {
	g := NewGroup[int](6)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("WaitContext blocked too long after external cancel: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("external cancel propagation OK: returned in %v", elapsed)
}

// ==================== WaitContext — 混合任务（部分响应取消 / 部分不响应） ====================

// TestWaitContext_CancelPropagation_Mixed
// 混合场景：部分任务阻塞在 ctx.Done()，部分任务使用 time.Sleep（不响应取消）。
// 取消后所有响应取消的任务应立即退出，不响应的任务会自然结束。
func TestWaitContext_CancelPropagation_Mixed(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	var cancelledCount atomic.Int64
	nResponsive := 4
	nUnresponsive := 4

	// 响应取消的：阻塞在 ctx.Done()
	for i := 0; i < nResponsive; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			select {
			case <-taskCtx.Done():
				cancelledCount.Add(1)
				return 0, taskCtx.Err()
			case <-time.After(30 * time.Second):
				return 1, nil
			}
		})
	}

	// 不响应取消的：纯 sleep（短时间，让测试不卡太久）
	for i := 0; i < nUnresponsive; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 42, nil
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected timeout (ok=false)")
	}
	// 总时间：不响应任务 sleep 200ms + 调度开销
	if elapsed > 2*time.Second {
		t.Fatalf("WaitContext blocked too long with mixed tasks: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != nResponsive+nUnresponsive {
		t.Fatalf("expected %d results, got %d", nResponsive+nUnresponsive, len(results))
	}
	if cancelledCount.Load() != int64(nResponsive) {
		t.Errorf("expected %d tasks cancelled, got %d", nResponsive, cancelledCount.Load())
	}
	t.Logf("mixed tasks OK: returned in %v, responsive cancelled=%d", elapsed, cancelledCount.Load())
}

// ==================== WaitContext — 部分结果完整性 ====================

// TestWaitContext_ResultsIntegrity
// 部分任务先完成，部分任务在取消后返回。验证结果完整性：已完成任务的结果 + 取消任务的结果。
func TestWaitContext_ResultsIntegrity(t *testing.T) {
	g := NewGroup[int](8)
	defer g.Close()
	ctx := context.Background()
	nSlow := 4
	nFast := 4
	n := nSlow + nFast

	for i := 0; i < n; i++ {
		idx := i
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			if idx < nFast {
				// 前几个快速完成
				return idx * 100, nil
			}
			// 其余阻塞直到取消
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	results, ok := g.WaitContext(waitCtx)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}

	okCount := 0
	cancelCount := 0
	for _, r := range results {
		if r.Err == nil && r.Value%100 == 0 {
			okCount++
		} else if r.Err != nil {
			cancelCount++
		}
	}
	if okCount != nFast {
		t.Errorf("expected %d successful results, got %d", nFast, okCount)
	}
	if cancelCount != nSlow {
		t.Errorf("expected %d cancelled results, got %d", nSlow, cancelCount)
	}
	t.Logf("results integrity OK: success=%d, cancelled=%d", okCount, cancelCount)
}

// ==================== WaitContext — 已取消 ctx 的边界 ====================

// TestWaitContext_AlreadyCancelledCtx
// 传入已经取消的 context，WaitContext 应立即返回而不阻塞。
func TestWaitContext_AlreadyCancelledCtx(t *testing.T) {
	g := NewGroup[int](6)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	alreadyCancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	results, ok := g.WaitContext(alreadyCancelledCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false) with already-cancelled ctx")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("WaitContext blocked too long with already-cancelled ctx: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("already-cancelled ctx OK: returned in %v", elapsed)
}

// ==================== WaitContext — 空 Group ====================

// TestWaitContext_EmptyGroup
// 未提交任何任务时，WaitContext 应正常完成。
func TestWaitContext_EmptyGroup(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()

	results, ok := g.WaitContext(context.Background())
	if !ok {
		t.Fatal("empty group WaitContext should return ok=true")
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

// TestWaitTimeout_EmptyGroup
// 未提交任何任务时，WaitTimeout 应正常完成。
func TestWaitTimeout_EmptyGroup(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()

	results, ok := g.WaitTimeout(time.Second)
	if !ok {
		t.Fatal("empty group WaitTimeout should return ok=true")
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

// ==================== WaitContext — 取消后不可再提交 ====================

// TestWaitContext_GoAfterCancel
// WaitContext 取消返回后，再调用 Go 应返回错误。
func TestWaitContext_GoAfterCancel(t *testing.T) {
	g := NewGroup[int](6)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, ok := g.WaitContext(waitCtx)
	if ok {
		t.Fatal("expected timeout (ok=false)")
	}

	err := g.Go(ctx, func(taskCtx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Error("Go after WaitContext cancel should return error")
	}
	t.Logf("Go after cancel error: %v", err)
}

// ==================== WaitContext — 大并发取消压力 ====================

// TestWaitContext_CancelPropagation_HighConcurrency
// 高并发下，cancelAll 应同时取消所有阻塞任务。
func TestWaitContext_CancelPropagation_HighConcurrency(t *testing.T) {
	g := NewGroup[int](16)
	defer g.Close()
	ctx := context.Background()
	n := 32

	for i := 0; i < n; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("high-concurrency WaitContext blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	t.Logf("high-concurrency cancel OK: returned in %v, %d results", elapsed, n)
}

// ==================== WaitContext — Panic 恢复安全 ====================

// TestWaitContext_PanicSafety_CancelPath
// 取消路径中任务 panic，确保能被捕获并记录，不影响其它任务。
func TestWaitContext_PanicSafety_CancelPath(t *testing.T) {
	g := NewGroup[int](2)
	defer g.Close()
	ctx := context.Background()

	g.Go(ctx, func(taskCtx context.Context) (int, error) {
		<-taskCtx.Done()
		panic("intentional panic in cancel path")
	})

	g.Go(ctx, func(taskCtx context.Context) (int, error) {
		<-taskCtx.Done()
		return 0, taskCtx.Err()
	})

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	results, ok := g.WaitContext(waitCtx)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	panicCount := 0
	cancelCount := 0
	for _, r := range results {
		if r.Err != nil {
			if _, isPanic := r.Err.(*core.PanicError); isPanic {
				panicCount++
			} else {
				cancelCount++
			}
		}
	}
	if panicCount != 1 {
		t.Errorf("expected 1 panic error, got %d", panicCount)
	}
	if cancelCount != 1 {
		t.Errorf("expected 1 cancel error, got %d", cancelCount)
	}
	t.Logf("panic safety on cancel path OK: panic=%d, cancel=%d", panicCount, cancelCount)
}

// ==================== WaitContext — 无返回值 NoResult 场景 ====================

// TestNoResult_WaitContext_CancelPropagation
// NoResult.WaitContext 的取消传播（委托给 Group.WaitContext）。
func TestNoResult_WaitContext_CancelPropagation(t *testing.T) {
	nr := NewNoResult(6)
	defer nr.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		nr.Go(ctx, func(taskCtx context.Context) error {
			<-taskCtx.Done()
			return taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	completed, ok := nr.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("NoResult.WaitContext blocked too long: %v", elapsed)
	}
	if completed < 0 || completed > 5 {
		t.Errorf("unexpected completed count: %d", completed)
	}
	t.Logf("NoResult cancel propagation OK: returned in %v, completed=%d", elapsed, completed)
}

// ==================== MultiGroup WaitContext — 跨分片取消传播 ====================

// TestMultiGroup_WaitContext_CancelPropagation
// MultiGroup 的所有分片同时收到取消信号，任务全部退出。
func TestMultiGroup_WaitContext_CancelPropagation(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(4)
	defer mg.Close()
	ctx := context.Background()

	n := 24
	for i := 0; i < n; i++ {
		_ = mg.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := mg.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("MultiGroup.WaitContext blocked too long: %v (cancel signal NOT propagated to all shards)", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = mg.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	t.Logf("MultiGroup cancel propagation OK: returned in %v, %d/%d results", elapsed, len(results), n)
}

// TestMultiGroup_WaitTimeout_TimeoutPropagation
// MultiGroup.WaitTimeout 超时信号同时触达所有分片。
func TestMultiGroup_WaitTimeout_TimeoutPropagation(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	mg := g.Shard(4)
	defer mg.Close()
	ctx := context.Background()

	n := 24
	for i := 0; i < n; i++ {
		_ = mg.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	start := time.Now()
	results, ok := mg.WaitTimeout(100 * time.Millisecond)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected timeout (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("MultiGroup.WaitTimeout blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = mg.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	t.Logf("MultiGroup timeout propagation OK: returned in %v", elapsed)
}

// ==================== WaitContext — 连续两次调用 ====================

// TestWaitContext_Cancel_TwiceCall
// WaitContext 取消返回后再次调用，返回缓存结果。
func TestWaitContext_Cancel_TwiceCall(t *testing.T) {
	g := NewGroup[int](6)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	r1, ok1 := g.WaitContext(waitCtx)
	r2, ok2 := g.WaitContext(context.Background())

	if ok1 || !ok2 {
		t.Fatalf("first call ok=%v (expected false, timeout), second call ok=%v (expected true, waited)", ok1, ok2)
	}
	if len(r1) > 5 || len(r2) != 5 {
		t.Fatalf("results: len1=%d, len2=%d", len(r1), len(r2))
	}
	t.Logf("WaitContext twice-call OK: len1=%d, len2=%d", len(r1), len(r2))
}

// ==================== WaitContext — GoAt 索引任务 ====================

// TestWaitContext_CancelPropagation_GoAt
// 使用 GoAt 提交索引位置的任务，取消传播同样生效。
func TestWaitContext_CancelPropagation_GoAt(t *testing.T) {
	g := NewGroup[int](4)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		idx := i
		_ = g.GoAt(idx, ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("WaitContext blocked too long with GoAt: %v", elapsed)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("GoAt cancel propagation OK: returned in %v", elapsed)
}

// ==================== WaitContext — 组级 WithTimeout 叠加 ====================

// TestWaitContext_WithGroupTimeout_And_WaitContextTimeout
// 组级别有 WithTimeout，WaitContext 也有 timeout。两者叠加，取消信号正确传播。
func TestWaitContext_WithGroupTimeout_And_WaitContextTimeout(t *testing.T) {
	g := NewGroup[int](6)
	g.WithTimeout(5 * time.Second)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("WaitContext blocked too long with group timeout: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("group+WaitContext timeout OK: returned in %v", elapsed)
}

// ==================== WaitContext — WithContext 组级 ctx ====================

// TestWaitContext_WithGroupContext_AndWaitContextCancel
// 组级别有 WithContext，WaitContext 使用独立 ctx。取消 WaitContext 的 ctx 不影响组级 ctx。
func TestWaitContext_WithGroupContext_AndWaitContextCancel(t *testing.T) {
	groupCtx, groupCancel := context.WithCancel(context.Background())
	defer groupCancel()
	g := NewGroup[int](6)
	g.WithContext(groupCtx)
	defer g.Close()

	for i := 0; i < 5; i++ {
		g.Go(groupCtx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("WaitContext blocked too long with group context: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("group context + WaitContext cancel OK: returned in %v", elapsed)
}

// ==================== WaitContext — Wait 后 WaitContext ====================

// TestWaitContext_AfterWait
// 先正常 Wait，再 WaitContext（已 waited，第二次调用走缓存）。
func TestWaitContext_AfterWait(t *testing.T) {
	g := NewGroup[int](2)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			return i, nil
		})
	}

	results1 := g.Wait()
	if len(results1) != 5 {
		t.Fatalf("Wait: expected 5 results, got %d", len(results1))
	}

	results2, ok2 := g.WaitContext(context.Background())
	if !ok2 {
		t.Fatal("WaitContext after Wait should return ok=true (cached)")
	}
	if len(results2) != 5 {
		t.Fatalf("WaitContext after Wait: expected 5 results, got %d", len(results2))
	}
	t.Logf("Wait then WaitContext OK: len1=%d, len2=%d", len(results1), len(results2))
}

// ==================== WaitTimeout — 连续两次调用 ====================

// TestWaitTimeout_Cancel_TwiceCall
// WaitTimeout 超时返回后再调用，返回缓存结果。
func TestWaitTimeout_Cancel_TwiceCall(t *testing.T) {
	g := NewGroup[int](6)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	r1, ok1 := g.WaitTimeout(100 * time.Millisecond)
	r2, ok2 := g.WaitTimeout(time.Second)

	if ok1 || !ok2 {
		t.Fatalf("first call ok=%v (expected false, timeout), second call ok=%v (expected true, waited)", ok1, ok2)
	}
	if len(r1) > 5 || len(r2) != 5 {
		t.Fatalf("results: len1=%d, len2=%d", len(r1), len(r2))
	}
	t.Logf("WaitTimeout twice-call OK: len1=%d, len2=%d", len(r1), len(r2))
}

// ==================== Builder 链式调用 + WaitContext/WaitTimeout ====================

// TestChain_WithTimeout_WaitContext
// NewGroup.WithTimeout 链式调用后，WaitContext 取消信号应正确传播。
func TestChain_WithTimeout_WaitContext(t *testing.T) {
	g := NewGroup[int](6).WithTimeout(5 * time.Second)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("chain WithTimeout+WaitContext blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err == nil {
			t.Error("all results should have error from cancel")
		}
	}
	t.Logf("chain WithTimeout+WaitContext OK: returned in %v", elapsed)
}

// TestChain_WithTimeout_WaitTimeout
// NewGroup.WithTimeout 链式调用后，WaitTimeout 超时应正确传播。
func TestChain_WithTimeout_WaitTimeout(t *testing.T) {
	g := NewGroup[int](6).WithTimeout(10 * time.Second)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	start := time.Now()
	results, ok := g.WaitTimeout(100 * time.Millisecond)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected timeout (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("chain WithTimeout+WaitTimeout blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("chain WithTimeout+WaitTimeout OK: returned in %v", elapsed)
}

// TestChain_WithSubmitTimeout_WaitContext
// NewGroup.WithSubmitTimeout 链式调用后，WaitContext 取消信号应正确传播。
func TestChain_WithSubmitTimeout_WaitContext(t *testing.T) {
	g := NewGroup[int](6).WithSubmitTimeout(5 * time.Second)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("chain WithSubmitTimeout+WaitContext blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("chain WithSubmitTimeout+WaitContext OK: returned in %v", elapsed)
}

// TestChain_DoubleBuilder_WaitContext
// NewGroup.WithTimeout.WithSubmitTimeout 双重链式调用后，WaitContext 取消信号正确传播。
func TestChain_DoubleBuilder_WaitContext(t *testing.T) {
	g := NewGroup[int](6).WithTimeout(5 * time.Second).WithSubmitTimeout(3 * time.Second)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("chain DoubleBuilder+WaitContext blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("chain DoubleBuilder+WaitContext OK: returned in %v", elapsed)
}

// TestChain_DoubleBuilder_WaitTimeout
// NewGroup.WithTimeout.WithSubmitTimeout 双重链式调用后，WaitTimeout 超时应正确传播。
func TestChain_DoubleBuilder_WaitTimeout(t *testing.T) {
	g := NewGroup[int](6).WithTimeout(10 * time.Second).WithSubmitTimeout(5 * time.Second)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	start := time.Now()
	results, ok := g.WaitTimeout(100 * time.Millisecond)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected timeout (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("chain DoubleBuilder+WaitTimeout blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("chain DoubleBuilder+WaitTimeout OK: returned in %v", elapsed)
}

// TestChain_WithStreaming_WaitContext
// NewGroup.WithStreaming 链式调用后，WaitContext 取消信号正确传播。
func TestChain_WithStreaming_WaitContext(t *testing.T) {
	g := NewGroup[int](6).WithStreaming(10)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("chain WithStreaming+WaitContext blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("chain WithStreaming+WaitContext OK: returned in %v", elapsed)
}

// TestChain_WithResultCallback_WaitContext
// NewGroup.WithResultCallback 链式调用后，WaitContext 取消信号正确传播，回调应触发。
func TestChain_WithResultCallback_WaitContext(t *testing.T) {
	var callbackResults []core.Result[int]
	var mu sync.Mutex
	g := NewGroup[int](6).WithResultCallback(func(r core.Result[int]) {
		mu.Lock()
		callbackResults = append(callbackResults, r)
		mu.Unlock()
	})
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("chain WithResultCallback+WaitContext blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	mu.Lock()
	cbCount := len(callbackResults)
	mu.Unlock()
	if cbCount != 5 {
		t.Errorf("expected 5 callback results, got %d", cbCount)
	}
	t.Logf("chain WithResultCallback+WaitContext OK: returned in %v, callbacks=%d", elapsed, cbCount)
}

// TestChain_WithContext_WaitContext
// WithContext 返回 (*Group, ctx)，不能链在 NewGroup 后，但仍然是 builder 模式的一部分。
// 验证此模式下 WaitContext 取消信号正确传播。
func TestChain_WithContext_WaitContext(t *testing.T) {
	g := NewGroup[int](6)
	groupCtx, groupCancel := context.WithCancel(context.Background())
	defer groupCancel()
	g, _ = g.WithContext(groupCtx)
	defer g.Close()

	for i := 0; i < 5; i++ {
		g.Go(groupCtx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("chain WithContext+WaitContext blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("chain WithContext+WaitContext OK: returned in %v", elapsed)
}

// TestChain_TripleBuilder_WaitContext
// NewGroup.WithTimeout.WithSubmitTimeout.WithStreaming 三重链式调用后，取消信号正确传播。
func TestChain_TripleBuilder_WaitContext(t *testing.T) {
	g := NewGroup[int](6).WithTimeout(5 * time.Second).WithSubmitTimeout(3 * time.Second).WithStreaming(10)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("chain TripleBuilder+WaitContext blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("chain TripleBuilder+WaitContext OK: returned in %v", elapsed)
}

// TestChain_DefaultGroup_WaitContext
// DefaultGroup 返回已经可以链式调用的 Group，验证 WaitContext 取消信号传播。
func TestChain_DefaultGroup_WaitContext(t *testing.T) {
	g := DefaultGroup[int]().WithTimeout(5 * time.Second)
	defer g.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		g.Go(ctx, func(taskCtx context.Context) (int, error) {
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		})
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, ok := g.WaitContext(waitCtx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("chain DefaultGroup+WaitContext blocked too long: %v", elapsed)
	}
	// A timed wait returns a snapshot; join before asserting final completeness.
	results = g.Wait()
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	t.Logf("chain DefaultGroup+WaitContext OK: returned in %v", elapsed)
}
