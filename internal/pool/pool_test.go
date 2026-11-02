package pool

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
)

func init() {
	core.SetDefaultMaxResults(100000)
}

func TestMultiPoolStreamingResults(t *testing.T) {
	const shards, tasks = 4, 80
	mp := NewPool[int](2).Shard(shards).WithStreaming(tasks)
	defer mp.Close()
	stream := mp.StreamResults()
	collected := make(chan []int, 1)
	go func() {
		var values []int
		for result := range stream {
			values = append(values, result.Value)
		}
		collected <- values
	}()
	for i := 0; i < tasks; i++ {
		value := i
		if err := mp.SubmitKeyed(uint64(i%shards), context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if results := mp.Wait(); len(results) != tasks {
		t.Fatalf("Wait returned %d results, want %d", len(results), tasks)
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
}

func TestPoolShardConcurrentCtx(t *testing.T) {
	for i := 0; i < 200; i++ {
		p, _ := NewPool[int](1).WithContext(context.Background())
		start, done := make(chan struct{}), make(chan struct{})
		go func() {
			<-start
			for j := 0; j < 20; j++ {
				_ = p.Ctx().Err()
			}
			close(done)
		}()
		close(start)
		mp := p.Shard(2)
		<-done
		mp.Close()
	}
}

func TestPoolShardConcurrentWait(t *testing.T) {
	for i := 0; i < 200; i++ {
		p, _ := NewPool[int](1).WithContext(context.Background())
		start := make(chan struct{})
		done := make(chan struct{})
		go func() { <-start; p.Wait(); close(done) }()
		close(start)
		mp := p.Shard(2)
		<-done
		mp.Close()
	}
}

func TestPoolShardConcurrentWaitContext(t *testing.T) {
	for i := 0; i < 200; i++ {
		p, _ := NewPool[int](1).WithContext(context.Background())
		start := make(chan struct{})
		done := make(chan struct{})
		go func() { <-start; p.WaitContext(context.Background()); close(done) }()
		close(start)
		mp := p.Shard(2)
		<-done
		mp.Close()
	}
}

func TestPoolShardWhileFailFastTaskFinishes(t *testing.T) {
	for i := 0; i < 100; i++ {
		p, ctx := NewPool[int](1).WithFailFast(context.Background())
		started, release := make(chan struct{}), make(chan struct{})
		if err := p.Submit(ctx, func(context.Context) (int, error) {
			close(started)
			<-release
			return 0, core.ErrTimeout
		}); err != nil {
			t.Fatal(err)
		}
		<-started
		close(release)
		mp := p.Shard(2)
		mp.Wait()
		mp.Close()
	}
}

func TestMultiPoolFailureCancelsShardContexts(t *testing.T) {
	p, _ := NewPool[int](1).WithFailFast(context.Background())
	mp := p.Shard(2)
	defer mp.Close()
	started, release := make(chan struct{}), make(chan struct{})
	canceled := make(chan bool, 1)
	if err := mp.GetShard(0).Submit(mp.GetShard(0).Ctx(), func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-ctx.Done():
			canceled <- true
		case <-release:
			canceled <- false
		}
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := mp.GetShard(1).Submit(mp.GetShard(1).Ctx(), func(context.Context) (int, error) { return 0, core.ErrTimeout }); err != nil {
		close(release)
		t.Fatal(err)
	}
	select {
	case ok := <-canceled:
		if !ok {
			t.Error("other shard was not canceled")
		}
	case <-time.After(time.Second):
		close(release)
		<-canceled
		t.Error("failure did not cancel the other shard")
	}
	mp.Wait()
}

func TestBuildersDefaultRingBufferUsesGlobalCapacity(t *testing.T) {
	old := core.GetDefaultRingBufferCap()
	oldOverflow := core.GetDefaultOverflowStrategy()
	core.SetDefaultRingBuffer(3, oldOverflow)
	t.Cleanup(func() { core.SetDefaultRingBuffer(old, oldOverflow) })
	check := func(p *Pool[int]) error {
		if p.ringBuf == nil || p.ringBuf.Cap() != 3 {
			return fmt.Errorf("global ring buffer capacity was not applied")
		}
		return nil
	}
	if err := NewBuilder[int]().Worker(1).RingBuf(7).DefaultRingBuf().Run(func(_ context.Context, p *Pool[int]) error {
		return check(p)
	}); err != nil {
		t.Fatal(err)
	}
	if err := NewMultiBuilder[int]().Worker(1).Shards(2).RingBuf(7).DefaultRingBuf().Run(func(_ context.Context, mp *MultiPool[int]) error {
		for i := 0; i < mp.ShardCount(); i++ {
			if err := check(mp.GetShard(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMultiPoolBuilderDefaultShardsUsesOnePool(t *testing.T) {
	err := NewMultiBuilder[int]().Worker(1).Shards(3).DefaultShards().Run(func(_ context.Context, mp *MultiPool[int]) error {
		if got := mp.ShardCount(); got != 1 {
			return fmt.Errorf("shard count = %d, want 1", got)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultiPoolCloseCancelsSharedContext(t *testing.T) {
	for _, failFast := range []bool{false, true} {
		t.Run(fmt.Sprint("failFast=", failFast), func(t *testing.T) {
			p := NewPool[int](1)
			var ctx context.Context
			if failFast {
				p, ctx = p.WithFailFast(context.Background())
			} else {
				p, ctx = p.WithContext(context.Background())
			}
			mp := p.Shard(2)
			mp.Close()
			if ctx.Err() == nil {
				t.Fatal("shared context remains active after close")
			}
		})
	}
}

func TestMultiPoolBuilderWaitKeepsOtherShardRunning(t *testing.T) {
	err := NewMultiBuilder[int]().Worker(1).Shards(2).FailFast().Run(func(ctx context.Context, mp *MultiPool[int]) error {
		started, release := make(chan struct{}), make(chan struct{})
		if err := mp.GetShard(1).Submit(ctx, func(ctx context.Context) (int, error) {
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
		go func() { done <- mp.Wait() }()
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

func TestMultiPoolFailureCancelsOtherShard(t *testing.T) {
	p, ctx := NewPool[int](1).WithFailFast(context.Background())
	mp := p.Shard(2)
	defer mp.Close()
	started, release := make(chan struct{}), make(chan struct{})
	canceled := make(chan bool, 1)
	if err := mp.GetShard(0).Submit(ctx, func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-ctx.Done():
			canceled <- true
		case <-release:
			canceled <- false
		}
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := mp.GetShard(1).Submit(ctx, func(context.Context) (int, error) { return 0, core.ErrTimeout }); err != nil {
		close(release)
		t.Fatal(err)
	}
	select {
	case ok := <-canceled:
		if !ok {
			t.Error("other shard was not canceled")
		}
	case <-time.After(time.Second):
		close(release)
		<-canceled
		t.Error("failure did not cancel the other shard")
	}
	mp.Wait()
}

func TestMultiPoolWaitKeepsOtherShardRunning(t *testing.T) {
	for _, failFast := range []bool{false, true} {
		t.Run(fmt.Sprint("failFast=", failFast), func(t *testing.T) {
			p := NewPool[int](1)
			var ctx context.Context
			if failFast {
				p, ctx = p.WithFailFast(context.Background())
			} else {
				p, ctx = p.WithContext(context.Background())
			}
			mp := p.Shard(2)
			defer mp.Close()
			started, release := make(chan struct{}), make(chan struct{})
			if err := mp.GetShard(1).Submit(ctx, func(ctx context.Context) (int, error) {
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
			go func() { done <- mp.Wait() }()
			select {
			case <-ctx.Done():
				close(release)
				<-done
				t.Fatal("normal wait canceled shared context")
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
		})
	}
}

func BenchmarkPoolConcurrentSubmit(b *testing.B) {
	p := NewPool[int](runtime.GOMAXPROCS(0)).WithMaxResults(1)
	ctx := context.Background()
	task := func(context.Context) (int, error) { return 1, nil }
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := p.Submit(ctx, task); err != nil {
				b.Error(err)
				return
			}
		}
	})
	p.Wait()
	b.StopTimer()
	p.Close()
}

func TestSubmitAtRejectedResultUsesOverflowBuffer(t *testing.T) {
	p := NewPool[int](1).WithMaxResults(1).WithRingBuffer(4, core.OverflowBlock)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 3; i++ {
		if err := p.SubmitAt(i, ctx, func(context.Context) (int, error) { t.Error("rejected task ran"); return 0, nil }); !errors.Is(err, context.Canceled) {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	results := p.Wait()
	if len(results) != 3 {
		t.Fatalf("results=%v, want three cancellation results", results)
	}
	for _, result := range results {
		if !errors.Is(result.Err, context.Canceled) {
			t.Errorf("result=%v", result)
		}
	}
}

func TestSubmitAtWithCompletionReportsOutcomesOnce(t *testing.T) {
	p := NewPool[int](1).WithMaxPending(1).WithOverflow(core.OverflowDrop)
	defer p.Close()
	started, release := make(chan struct{}), make(chan struct{})
	var calls [3]atomic.Int32
	var outcomes [3]core.Result[int]
	finish := func(i int) func(core.Result[int]) {
		return func(r core.Result[int]) { outcomes[i] = r; calls[i].Add(1) }
	}
	if err := p.SubmitAtWithCompletion(0, context.Background(), func(context.Context) (int, error) { close(started); <-release; return 10, nil }, finish(0)); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.SubmitAtWithCompletion(1, context.Background(), func(context.Context) (int, error) { return 20, nil }, finish(1)); err != nil {
		close(release)
		t.Fatal(err)
	}
	if err := p.SubmitAtWithCompletion(2, context.Background(), func(context.Context) (int, error) { t.Error("rejected task ran"); return 0, nil }, finish(2)); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	p.Wait()
	for i := range calls {
		if calls[i].Load() != 1 {
			t.Errorf("index=%d calls=%d", i, calls[i].Load())
		}
	}
	if outcomes[0].Value != 10 || outcomes[1].Value != 20 || !errors.Is(outcomes[2].Err, core.ErrQueueOverflow) {
		t.Fatalf("outcomes=%v", outcomes)
	}
}

func TestSubmitAtCompletionPanicDoesNotBlockWait(t *testing.T) {
	p := NewPool[int](1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	func() {
		defer func() { _ = recover() }()
		_ = p.SubmitAtWithCompletion(0, ctx, func(context.Context) (int, error) { return 1, nil }, func(core.Result[int]) { panic("completion") })
	}()
	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()
	select {
	case <-done:
		p.Close()
	case <-time.After(300 * time.Millisecond):
		t.Fatal("completion panic left submission in flight")
	}
}

func TestSubmitAtRejectedCompletionCanWait(t *testing.T) {
	p := NewPool[int](1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		_ = p.SubmitAtWithCompletion(0, ctx, func(context.Context) (int, error) { return 0, nil }, func(core.Result[int]) { p.Wait() })
		close(done)
	}()
	select {
	case <-done:
		p.Close()
	case <-time.After(300 * time.Millisecond):
		t.Fatal("completion blocked in Wait")
	}
}

func TestSubmitAtResultTransformPanicIsResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := NewPool[int](1)
	defer p.Close()
	_ = p.SubmitAtWithResultTransform(0, ctx, func(context.Context) (int, error) { return 0, nil }, func(core.Result[int]) core.Result[int] {
		panic("transform")
	})
	results := p.Wait()
	if len(results) != 1 || !results[0].IsPanic() {
		t.Fatalf("results = %v", results)
	}
}

func TestSubmitAtResultTransformForQueuedCancellation(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	err := p.SubmitAtWithResultTransform(1, ctx, func(context.Context) (int, error) {
		t.Error("canceled task ran")
		return 2, nil
	}, func(r core.Result[int]) core.Result[int] {
		calls.Add(1)
		if !errors.Is(r.Err, context.Canceled) {
			t.Errorf("transform received %v, want context cancellation", r.Err)
		}
		panic("canceled callback")
	})
	if err != nil {
		cancel()
		close(release)
		t.Fatal(err)
	}
	cancel()
	close(release)
	results := p.Wait()
	if calls.Load() != 1 || len(results) != 2 || !results[1].IsPanic() {
		t.Fatalf("calls=%d results=%v", calls.Load(), results)
	}
}

func TestSubmitAtCompletionForQueuedCancellation(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	var outcome core.Result[int]
	if err := p.SubmitAtWithCompletion(1, ctx, func(context.Context) (int, error) { t.Error("canceled task ran"); return 2, nil }, func(r core.Result[int]) { outcome = r; calls.Add(1) }); err != nil {
		close(release)
		t.Fatal(err)
	}
	cancel()
	close(release)
	p.Wait()
	if calls.Load() != 1 || !errors.Is(outcome.Err, context.Canceled) {
		t.Fatalf("calls=%d outcome=%v", calls.Load(), outcome)
	}
}

func TestRejectedSubmitUsesResultOverflowBuffer(t *testing.T) {
	for _, method := range []string{"Submit", "TrySubmit"} {
		t.Run(method, func(t *testing.T) {
			p := NewPool[int](1).WithMaxResults(1).WithRingBuffer(4, core.OverflowBlock)
			defer p.Close()
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var err error
			if method == "Submit" {
				err = p.Submit(ctx, func(context.Context) (int, error) { return 0, nil })
			} else {
				err = p.TrySubmit(ctx, func(context.Context) (int, error) { return 0, nil })
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("submission error = %v", err)
			}
			results := p.Wait()
			if len(results) != 2 || results[0].Value != 42 || !errors.Is(results[1].Err, context.Canceled) {
				t.Fatalf("results = %v", results)
			}
		})
	}
}

func TestErrorsAllocationBoundedByStoredResults(t *testing.T) {
	p := NewPool[int](1).WithMaxResults(1)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 1000; i++ {
		_ = p.Submit(ctx, func(context.Context) (int, error) { return 0, nil })
	}
	p.Wait()
	errs := p.Errors()
	if len(errs) != 1 || !errors.Is(errs[0], context.Canceled) || cap(errs) > 8 {
		t.Fatalf("errors len=%d cap=%d, want one error with bounded allocation", len(errs), cap(errs))
	}
}

func TestErrorAccessorsOnlyReadMainResults(t *testing.T) {
	p := NewPool[int](1).WithMaxResults(1).WithRingBuffer(4, core.OverflowBlock)
	defer p.Close()
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("overflow")
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 0, sentinel }); err != nil {
		t.Fatal(err)
	}
	results := p.Wait()
	if len(results) != 2 || !errors.Is(results[1].Err, sentinel) || !p.HasError() || len(p.Errors()) != 0 || p.FirstError() != nil || p.JoinErrors() != nil {
		t.Fatalf("results=%v hasError=%v errors=%v", results, p.HasError(), p.Errors())
	}
	values := p.Values()
	if len(values) != 1 || values[0] != 42 {
		t.Fatalf("values=%v", values)
	}
}

func TestWaitReturnsOverflowInCompletionOrder(t *testing.T) {
	p := NewPool[int](2).WithMaxResults(1).WithRingBuffer(4, core.OverflowBlock)
	defer p.Close()
	firstStarted, releaseFirst, secondDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	if err := p.SubmitAt(1, context.Background(), func(context.Context) (int, error) { close(firstStarted); <-releaseFirst; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-firstStarted
	if err := p.SubmitAtWithCompletion(2, context.Background(), func(context.Context) (int, error) { return 2, nil }, func(core.Result[int]) { close(secondDone) }); err != nil {
		close(releaseFirst)
		t.Fatal(err)
	}
	<-secondDone
	close(releaseFirst)
	results := p.Wait()
	if len(results) != 3 || results[0].Value != 0 || results[1].Value != 2 || results[2].Value != 1 {
		t.Fatalf("results = %v", results)
	}
}

func TestRejectedSubmitQueueOverflowUsesResultOverflowBuffer(t *testing.T) {
	for _, method := range []string{"Submit", "TrySubmit"} {
		for _, strategy := range []core.OverflowStrategy{core.OverflowDrop, core.OverflowError} {
			t.Run(fmt.Sprintf("%s/%v", method, strategy), func(t *testing.T) {
				p := NewPool[int](1).WithMaxPending(1).WithOverflow(strategy).WithMaxResults(1).WithRingBuffer(4, core.OverflowBlock)
				defer p.Close()
				started, release := make(chan struct{}), make(chan struct{})
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
					t.Fatal(err)
				}
				<-started
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
					close(release)
					t.Fatal(err)
				}
				fn := func(context.Context) (int, error) { t.Error("rejected task ran"); return 3, nil }
				var err error
				if method == "Submit" {
					err = p.Submit(context.Background(), fn)
				} else {
					err = p.TrySubmit(context.Background(), fn)
				}
				close(release)
				results := p.Wait()
				if (strategy == core.OverflowError && !errors.Is(err, core.ErrQueueOverflow)) || (strategy == core.OverflowDrop && err != nil) {
					t.Fatalf("submission error = %v", err)
				}
				count := 0
				for _, result := range results {
					if errors.Is(result.Err, core.ErrQueueOverflow) {
						count++
					}
				}
				if len(results) != 3 || count != 1 {
					t.Fatalf("results = %v", results)
				}
			})
		}
	}
}

func TestSubmitAtQueueOverflowUsesResultOverflowBuffer(t *testing.T) {
	for _, strategy := range []core.OverflowStrategy{core.OverflowDrop, core.OverflowError} {
		t.Run(fmt.Sprint(strategy), func(t *testing.T) {
			p := NewPool[int](1).WithMaxPending(1).WithOverflow(strategy).WithMaxResults(1).WithRingBuffer(4, core.OverflowBlock)
			defer p.Close()
			started, release := make(chan struct{}), make(chan struct{})
			if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { close(started); <-release; return 42, nil }); err != nil {
				t.Fatal(err)
			}
			<-started
			if err := p.SubmitAt(1, context.Background(), func(context.Context) (int, error) { return 21, nil }); err != nil {
				close(release)
				t.Fatal(err)
			}
			err := p.SubmitAt(2, context.Background(), func(context.Context) (int, error) { t.Error("rejected task ran"); return 0, nil })
			close(release)
			if strategy == core.OverflowError && !errors.Is(err, core.ErrQueueOverflow) {
				t.Fatalf("error=%v", err)
			}
			if strategy == core.OverflowDrop && err != nil {
				t.Fatalf("error=%v", err)
			}
			results := p.Wait()
			if len(results) != 3 || results[0].Value != 42 {
				t.Fatalf("results=%v", results)
			}
			var accepted, rejected bool
			for _, result := range results[1:] {
				accepted = accepted || result.Value == 21 && result.Err == nil
				rejected = rejected || errors.Is(result.Err, core.ErrQueueOverflow)
			}
			if !accepted || !rejected {
				t.Fatalf("results=%v", results)
			}
		})
	}
}

func TestBuilderFailFastCancelsRunContext(t *testing.T) {
	boom := errors.New("task failed")
	t.Run("pool", func(t *testing.T) {
		err := NewBuilder[int]().Worker(1).FailFast().Run(func(ctx context.Context, p *Pool[int]) error {
			if err := p.Submit(ctx, func(context.Context) (int, error) { return 0, boom }); err != nil {
				return err
			}
			p.Wait()
			return ctx.Err()
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run context error = %v, want canceled", err)
		}
	})
	t.Run("multi", func(t *testing.T) {
		err := NewMultiBuilder[int]().Worker(1).Shards(2).FailFast().Run(func(ctx context.Context, p *MultiPool[int]) error {
			if err := p.SubmitKeyed(1, ctx, func(context.Context) (int, error) { return 0, boom }); err != nil {
				return err
			}
			p.Wait()
			return ctx.Err()
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run context error = %v, want canceled", err)
		}
	})
}

func TestPoolFailFastCountsRunningCancellation(t *testing.T) {
	boom := errors.New("failure")
	p, ctx := NewPool[int](2).WithFailFast(context.Background())
	defer p.Close()
	started := make(chan struct{})
	if err := p.Submit(ctx, func(ctx context.Context) (int, error) { close(started); <-ctx.Done(); return 0, ctx.Err() }); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(ctx, func(context.Context) (int, error) { return 0, boom }); err != nil {
		t.Fatal(err)
	}
	results := p.Wait()
	if len(results) != 2 || !errors.Is(results[0].Err, context.Canceled) || !errors.Is(results[1].Err, boom) {
		t.Fatalf("results=%v", results)
	}
	if p.FailCount() != 2 || p.OkCount() != 0 {
		t.Fatalf("failures=%d successes=%d", p.FailCount(), p.OkCount())
	}
}

func TestMultiPoolRoundRobinCounterOverflow(t *testing.T) {
	mp := &MultiPool[int]{pools: []*Pool[int]{NewPool[int](1), NewPool[int](1), NewPool[int](1)}}
	defer mp.Close()
	mp.nextIdx.Store(uint64(math.MaxInt) + 1)
	fn := func(context.Context) (int, error) { return 1, nil }
	if err := mp.Submit(context.Background(), fn); err != nil {
		t.Fatal(err)
	}
	if err := mp.TrySubmit(context.Background(), fn); err != nil {
		t.Fatal(err)
	}
	if got := mp.SubmitBatch(context.Background(), []int{1}, func(context.Context, int) (int, error) { return 1, nil }); got[0].Err != nil {
		t.Fatal(got[0].Err)
	}
	if got := mp.Wait(); len(got) != 3 {
		t.Fatalf("got %d results", len(got))
	}
}

func TestMultiPoolTotalOkCount(t *testing.T) {
	mp := &MultiPool[int]{pools: []*Pool[int]{NewPool[int](1), NewPool[int](1)}}
	defer mp.Close()
	ctx := context.Background()
	if err := mp.SubmitKeyed(0, ctx, func(context.Context) (int, error) { return 1, nil }); err != nil {
		t.Fatal(err)
	}
	if err := mp.SubmitKeyed(1, ctx, func(context.Context) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	if err := mp.SubmitKeyed(0, ctx, func(context.Context) (int, error) { return 0, errors.New("expected failure") }); err != nil {
		t.Fatal(err)
	}
	mp.Wait()
	if got := mp.TotalOkCount(); got != 2 {
		t.Fatalf("TotalOkCount() = %d, want 2", got)
	}
	if got := mp.TotalFailCount(); got != 1 {
		t.Fatalf("TotalFailCount() = %d, want 1", got)
	}
}

// ==================== NewPool / DefaultPool ====================

func TestPoolLateStreamingAndResetRegression(t *testing.T) {
	for _, variant := range []string{"timeout", "context"} {
		t.Run(variant, func(t *testing.T) {
			p := NewPool[int](1).WithTimeout(0).WithStreaming(4)
			defer p.Close()
			started, release := make(chan struct{}), make(chan struct{})
			if err := p.Submit(context.Background(), func(ctx context.Context) (int, error) {
				close(started)
				<-ctx.Done()
				<-release
				return 7, ctx.Err()
			}); err != nil {
				t.Fatal(err)
			}
			<-started
			oldStream := p.StreamResults()
			var ok bool
			if variant == "timeout" {
				_, ok = p.WaitTimeout(time.Millisecond)
			} else {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, ok = p.WaitContext(ctx)
			}
			if ok {
				t.Error("unfinished task reported complete")
			}
			select {
			case _, open := <-oldStream:
				if !open {
					t.Error("stream closed while producer is running")
				}
			default:
			}
			close(release)
			var results []core.Result[int]
			for r := range oldStream {
				results = append(results, r)
			}
			if len(results) != 1 || results[0].Value != 7 || !errors.Is(results[0].Err, context.Canceled) {
				t.Fatalf("lost late result: %v", results)
			}
			p.ResetWait()
			if oldStream == p.StreamResults() {
				t.Fatal("reset reused the completed stream")
			}
			for round := 0; round < 3; round++ {
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 9, nil }); err != nil {
					t.Fatal(err)
				}
				if got := p.Wait(); len(got) != 1 || got[0].Value != 9 {
					t.Fatalf("reuse result: %v", got)
				}
				count := 0
				for range p.StreamResults() {
					count++
				}
				if count != 1 {
					t.Fatalf("stream results=%d", count)
				}
				p.ResetWait()
			}
		})
	}
}

func TestPoolCancellationIndependentOfResultsRegression(t *testing.T) {
	for _, mode := range []string{"max-submit", "max-try", "sparse", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			p := NewPool[int](2).WithTimeout(0)
			defer p.Close()
			if mode == "max-submit" || mode == "max-try" {
				p.WithMaxResults(1)
			}
			if err := p.SubmitAt(1, context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{}, 2)
			fn := func(ctx context.Context) (int, error) {
				started <- struct{}{}
				<-ctx.Done()
				return 0, ctx.Err()
			}
			var err error
			switch mode {
			case "max-submit":
				err = p.Submit(context.Background(), fn)
			case "max-try":
				err = p.TrySubmit(context.Background(), fn)
			default:
				err = p.SubmitAt(0, context.Background(), fn)
			}
			if err != nil {
				t.Fatal(err)
			}
			<-started
			if mode == "duplicate" {
				if err := p.SubmitAt(0, context.Background(), fn); err != nil {
					t.Fatal(err)
				}
				<-started
			}
			p.WaitTimeout(time.Millisecond)
			done := make(chan struct{})
			go func() { p.wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				p.cancelAllShards()
				t.Fatal("running context-aware task did not receive cancellation")
			}
			for i := range p.shards {
				s := &p.shards[i]
				s.mu.Lock()
				n := len(s.cancels)
				s.mu.Unlock()
				if n != 0 {
					t.Fatalf("completed tasks retain %d cancellation entries", n)
				}
			}
		})
	}
}

func TestConcurrentWaitAndCloseRecordsAllTasks(t *testing.T) {
	p := NewPool[int](1).WithTimeout(0)
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); p.Close() })
	if err := p.Submit(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	resultCh := make(chan []core.Result[int], 1)
	go func() { resultCh <- p.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for !p.submitGuard.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !p.submitGuard.Load() {
		t.Fatal("Wait did not guard submissions")
	}
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	unblock()
	var failures int64
	select {
	case results := <-resultCh:
		if len(results) != 2 {
			t.Fatalf("results = %#v", results)
		}
		for i, result := range results {
			if !result.Occupied || (result.Err != nil && !errors.Is(result.Err, core.ErrPoolClosed)) {
				t.Fatalf("result[%d] = %#v", i, result)
			}
			if result.Err != nil {
				failures++
			} else if result.Value != i+1 {
				t.Fatalf("result[%d] = %#v, want value %d", i, result, i+1)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not complete")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not complete")
	}
	if p.TotalCount() != 2 || p.FailCount() != failures || p.OkCount() != 2-failures {
		t.Fatalf("total=%d failed=%d ok=%d results failed=%d", p.TotalCount(), p.FailCount(), p.OkCount(), failures)
	}
}

func TestMultiPoolConcurrentWaitResultOwnership(t *testing.T) {
	mp := &MultiPool[int]{pools: []*Pool[int]{NewPool[int](1), NewPool[int](1)}}
	shared, cancel := context.WithCancel(context.Background())
	mp.sharedCancel = cancel
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); mp.Close() })
	if err := mp.SubmitKeyed(0, context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := mp.SubmitKeyed(1, context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
		t.Fatal(err)
	}
	first := make(chan []core.Result[int], 1)
	go func() { first <- mp.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for !mp.pools[0].waitInvoked.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !mp.pools[0].waitInvoked.Load() {
		t.Fatal("first Wait did not begin")
	}
	second := make(chan []core.Result[int], 1)
	go func() { second <- mp.Wait() }()
	select {
	case got := <-second:
		if len(got) != 0 {
			t.Errorf("second Wait results = %v, want none", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second Wait did not return")
	}
	if got, ok := mp.WaitContext(context.Background()); ok || len(got) != 0 {
		t.Errorf("concurrent WaitContext = %v, %v; want no results and false", got, ok)
	}
	if shared.Err() != nil {
		t.Fatal("concurrent wait canceled the shared context")
	}
	unblock()
	if got := <-first; len(got) != 2 {
		t.Fatalf("first Wait results = %v, want both shards", got)
	}
}

func TestMultiPoolConcurrentWaitContextOwnsResults(t *testing.T) {
	mp := NewPool[int](1).Shard(2)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); mp.Close() })
	if err := mp.SubmitKeyed(0, context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := mp.SubmitKeyed(1, context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	type outcome struct {
		results []core.Result[int]
		ok      bool
	}
	first := make(chan outcome, 1)
	go func() { results, ok := mp.WaitContext(context.Background()); first <- outcome{results, ok} }()
	deadline := time.Now().Add(5 * time.Second)
	for !mp.pools[0].waitInvoked.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !mp.pools[0].waitInvoked.Load() {
		t.Fatal("WaitContext did not begin")
	}
	if got := mp.Wait(); len(got) != 0 {
		t.Errorf("concurrent Wait = %v, want no results", got)
	}
	if got, ok := mp.WaitTimeout(time.Second); ok || len(got) != 0 {
		t.Errorf("concurrent WaitTimeout = %v, %v", got, ok)
	}
	unblock()
	select {
	case got := <-first:
		if !got.ok || len(got.results) != 2 || got.results[0].Value != 1 || got.results[1].Value != 2 {
			t.Fatalf("first WaitContext = %v, %v", got.results, got.ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitContext did not finish")
	}
}

func TestMultiPoolConcurrentWaitAndCloseClosesShards(t *testing.T) {
	mp := NewPool[int](1).Shard(2)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); mp.Close() })
	if err := mp.SubmitKeyed(0, context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	first := make(chan []core.Result[int], 1)
	go func() { first <- mp.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for !mp.pools[0].waitInvoked.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !mp.pools[0].waitInvoked.Load() {
		t.Fatal("Wait did not begin")
	}
	closed := make(chan []core.Result[int], 1)
	go func() { closed <- mp.WaitAndClose() }()
	deadline = time.Now().Add(5 * time.Second)
	for !mp.pools[0].closed.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !mp.pools[0].closed.Load() {
		t.Fatal("WaitAndClose did not begin closing")
	}
	unblock()
	select {
	case got := <-first:
		if len(got) != 1 || got[0].Value != 1 || got[0].Err != nil {
			t.Errorf("first Wait = %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not finish")
	}
	select {
	case got := <-closed:
		if len(got) != 0 || !mp.pools[1].closed.Load() {
			t.Errorf("WaitAndClose = %v, second shard closed = %v", got, mp.pools[1].closed.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitAndClose did not finish")
	}
}

func TestMultiPoolWaitAfterShardReset(t *testing.T) {
	mp := NewPool[int](1).Shard(2)
	defer mp.Close()
	for batch := 0; batch < 2; batch++ {
		for shard := 0; shard < mp.ShardCount(); shard++ {
			value := batch*10 + shard
			if err := mp.SubmitKeyed(uint64(shard), context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
				t.Fatal(err)
			}
		}
		results := mp.Wait()
		if len(results) != 2 || results[0].Value != batch*10 || results[1].Value != batch*10+1 {
			t.Fatalf("batch %d results = %v", batch, results)
		}
		for shard := 0; shard < mp.ShardCount(); shard++ {
			mp.GetShard(shard).ResetWait()
		}
	}
}

func TestMaxPendingBlockHonorsLimit(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			p := NewPool[int](1).WithMaxPending(1).WithOverflow(core.OverflowBlock)
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() { unblock(); p.Close() })
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
				t.Fatal(err)
			}
			<-started
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			fn := func(context.Context) (int, error) { return 3, nil }
			var err error
			if indexed {
				err = p.SubmitAt(10, ctx, fn)
			} else {
				err = p.Submit(ctx, fn)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("submit error = %v, want context deadline exceeded with full pending limit", err)
			}
		})
	}
}

func TestMaxPendingBlockWaitOutcomes(t *testing.T) {
	for _, mode := range []string{"release", "close", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			p := NewPool[int](1).WithMaxPending(1).WithOverflow(core.OverflowBlock)
			if mode == "timeout" {
				p.WithSubmitTimeout(30 * time.Millisecond)
			}
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() { unblock(); p.Close() })
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
				t.Fatal(err)
			}
			<-started
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- p.Submit(context.Background(), func(context.Context) (int, error) { return 3, nil }) }()
			deadline := time.After(5 * time.Second)
			for p.TotalCount() < 3 {
				select {
				case <-deadline:
					t.Fatal("submit was not registered")
				default:
					runtime.Gosched()
				}
			}
			if mode == "release" {
				unblock()
			}
			if mode == "close" {
				go p.Close()
			}
			select {
			case err := <-done:
				if mode == "release" && err != nil {
					t.Fatalf("submit after release: %v", err)
				}
				if mode == "close" && !errors.Is(err, core.ErrPoolClosed) {
					t.Fatalf("submit after close: %v", err)
				}
				if mode == "timeout" && !errors.Is(err, core.ErrSubmitTimeout) {
					t.Fatalf("submit timeout: %v", err)
				}
			case <-deadline:
				t.Fatal("blocked submit did not finish")
			}
			unblock()
		})
	}
}

func TestMaxPendingBlockTrySubmitDoesNotWait(t *testing.T) {
	p := NewPool[int](1).WithMaxPending(1).WithOverflow(core.OverflowBlock)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); p.Close() })
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	err := p.TrySubmit(context.Background(), func(context.Context) (int, error) { return 3, nil })
	if !errors.Is(err, core.ErrSubmitTimeout) {
		t.Fatalf("TrySubmit error = %v, want ErrSubmitTimeout at pending limit", err)
	}
}

func TestMaxPendingBlockConcurrentWaitersDrain(t *testing.T) {
	p := NewPool[int](1).WithMaxPending(1).WithOverflow(core.OverflowBlock)
	started, release := make(chan struct{}), make(chan struct{})
	var startOnce sync.Once
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		p.Close()
	})

	if err := p.Submit(context.Background(), func(context.Context) (int, error) {
		startOnce.Do(func() { close(started) })
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}

	const waiterCount = 64
	errs := make(chan error, waiterCount)
	for i := 0; i < waiterCount; i++ {
		go func() {
			errs <- p.Submit(context.Background(), func(context.Context) (int, error) { return 3, nil })
		}()
	}

	deadline := time.After(5 * time.Second)
	for p.TotalCount() < int64(waiterCount+2) {
		select {
		case <-deadline:
			t.Fatalf("concurrent submissions were not registered: total=%d", p.TotalCount())
		default:
			runtime.Gosched()
		}
	}
	if pending := p.pending.Load(); pending > 1 || pending < 0 {
		t.Fatalf("pending=%d, want within [0,1] while waiters are blocked", pending)
	}
	close(release)

	for i := 0; i < waiterCount; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("waiter %d submit error: %v", i, err)
			}
		case <-deadline:
			t.Fatal("concurrent blocked submissions did not drain")
		}
	}

	results := p.Wait()
	if len(results) != waiterCount+2 {
		t.Fatalf("result count=%d, want %d", len(results), waiterCount+2)
	}
	if got := p.pending.Load(); got != 0 {
		t.Fatalf("pending=%d after Wait, want 0", got)
	}
	if got := p.OkCount(); got != int64(waiterCount+2) {
		t.Fatalf("ok count=%d, want %d", got, waiterCount+2)
	}
}

func TestPoolBlockedSubmitCanceled(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Hour} {
		t.Run(timeout.String(), func(t *testing.T) {
			p := NewPool[int](1).WithSubmitTimeout(timeout)
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() { unblock(); p.Close() })
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
				t.Fatal(err)
			}
			<-started
			for i := 0; i < cap(p.taskCh); i++ {
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var ran atomic.Bool
			submitted := make(chan error, 1)
			go func() {
				submitted <- p.Submit(ctx, func(context.Context) (int, error) { ran.Store(true); return 3, nil })
			}()
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			for p.Pending() < cap(p.taskCh)+1 {
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
			if got, want := p.Pending(), cap(p.taskCh); got != want {
				t.Fatalf("pending after cancellation = %d, want %d", got, want)
			}
			unblock()
			results := p.Wait()
			if got := p.Pending(); got != 0 {
				t.Fatalf("pending after Wait = %d, want 0", got)
			}
			wantOK := int64(cap(p.taskCh) + 1)
			if ran.Load() || p.TotalCount() != wantOK+1 || p.OkCount() != wantOK || p.FailCount() != 1 {
				t.Fatalf("ran=%t total=%d ok=%d fail=%d", ran.Load(), p.TotalCount(), p.OkCount(), p.FailCount())
			}
			var canceled int
			for _, result := range results {
				if errors.Is(result.Err, context.Canceled) {
					canceled++
				} else if result.Err != nil {
					t.Fatalf("unexpected result error: %v", result.Err)
				}
			}
			if len(results) != int(wantOK+1) || canceled != 1 {
				t.Fatalf("results=%d canceled=%d, want %d and 1", len(results), canceled, wantOK+1)
			}
		})
	}
}

func TestPoolCloseUnblocksBlockedSubmitRegression(t *testing.T) {
	for _, mode := range []string{"close", "close-timeout"} {
		t.Run(mode, func(t *testing.T) {
			p := NewPool[int](1).WithTimeout(0)
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseTask := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(func() {
				releaseTask()
				p.Close()
			})
			if err := p.Submit(context.Background(), func(context.Context) (int, error) {
				close(started)
				<-release
				return 1, nil
			}); err != nil {
				t.Fatal(err)
			}
			<-started
			for i := 0; i < cap(p.taskCh); i++ {
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
					t.Fatal(err)
				}
			}

			submitStarted := make(chan struct{})
			submitDone := make(chan error, 1)
			go func() {
				close(submitStarted)
				submitDone <- p.Submit(context.Background(), func(context.Context) (int, error) { return 3, nil })
			}()
			<-submitStarted
			deadline := time.Now().Add(time.Second)
			for p.Pending() < cap(p.taskCh)+1 && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			if got, want := p.Pending(), cap(p.taskCh)+1; got != want {
				releaseTask()
				p.Close()
				t.Fatalf("submit did not reach full queue: pending=%d want=%d", got, want)
			}

			closeDone := make(chan struct{})
			var closeOK bool
			var workerDone <-chan struct{}
			go func() {
				defer close(closeDone)
				if mode == "close" {
					p.Close()
					return
				}
				closeOK, workerDone = p.CloseAndWaitTimeout(20 * time.Millisecond)
			}()

			select {
			case err := <-submitDone:
				if !errors.Is(err, core.ErrPoolClosed) {
					releaseTask()
					<-closeDone
					t.Fatalf("blocked submit returned %v, want ErrPoolClosed", err)
				}
			case <-time.After(time.Second):
				releaseTask()
				<-closeDone
				t.Fatal("Close failed to unblock Submit on a full queue")
			}

			if mode == "close-timeout" {
				<-closeDone
				if closeOK || workerDone == nil {
					releaseTask()
					t.Fatalf("CloseAndWaitTimeout returned ok=%v workerDone=%v", closeOK, workerDone != nil)
				}
				releaseTask()
				select {
				case <-workerDone:
				case <-time.After(time.Second):
					t.Fatal("timed close cleanup did not finish after task release")
				}
				return
			}

			releaseTask()
			select {
			case <-closeDone:
			case <-time.After(time.Second):
				t.Fatal("Close did not finish after the running task was released")
			}
		})
	}
}

func TestTrySubmitCloseAfterPrecheckReturnsClosed(t *testing.T) {
	p := NewPool[int](1)
	p.shards[0].mu.Lock()
	submitted := make(chan error, 1)
	go func() {
		submitted <- p.TrySubmit(context.Background(), func(context.Context) (int, error) { return 1, nil })
	}()
	deadline := time.Now().Add(5 * time.Second)
	for p.submitIdx.Load() == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if p.submitIdx.Load() == 0 {
		p.shards[0].mu.Unlock()
		p.Close()
		t.Fatal("submission did not reach result allocation")
	}
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	for !p.closed.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !p.closed.Load() {
		p.shards[0].mu.Unlock()
		<-closed
		t.Fatal("close did not start")
	}
	p.shards[0].mu.Unlock()
	err := <-submitted
	<-closed
	if !errors.Is(err, core.ErrPoolClosed) {
		t.Fatalf("TrySubmit error = %v, want ErrPoolClosed", err)
	}
}

func TestNewPool_Defaults(t *testing.T) {
	p := NewPool[int](5)
	defer p.Close()
	if p.Size() != 5 {
		t.Fatalf("Size = %d, want 5", p.Size())
	}
}

func TestNewPool_ZeroSize(t *testing.T) {
	p := NewPool[int](0)
	defer p.Close()
	if p.Size() != core.IO() {
		t.Fatalf("Size = %d, want %d (core.IO)", p.Size(), core.IO())
	}
}

func TestDefaultPool(t *testing.T) {
	p := DefaultPool[int]()
	defer p.Close()
	if p.Size() != core.IO() {
		t.Fatalf("DefaultPool Size = %d, want %d", p.Size(), core.IO())
	}
}

// ==================== Submit - Basic ====================

func TestSubmit_Normal(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 42, nil
		})
		if err != nil {
			t.Errorf("Submit(%d): %v", i, err)
		}
	}
	results := p.Wait()
	if len(results) != 10 {
		t.Errorf("len = %d, want 10", len(results))
	}
	for _, r := range results {
		if r.Value != 42 || !r.Ok() {
			t.Errorf("unexpected: %+v", r)
		}
	}
}

func TestSubmit_Error(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	testErr := errors.New("pool error")
	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return 0, testErr
		})
	}
	p.Wait()
	if p.FailCount() != 10 {
		t.Errorf("FailCount = %d, want 10", p.FailCount())
	}
}

func TestSubmit_PanicRecovery(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		panic("boom")
	})
	results := p.Wait()
	if len(results) != 1 {
		t.Fatalf("len = %d, want 1", len(results))
	}
	if !results[0].IsPanic() {
		t.Error("expected panic result")
	}
}

// ==================== SubmitAt ====================

func TestSubmitAt(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.SubmitAt(0, ctx, func(ctx context.Context) (int, error) { return 100, nil })
	p.SubmitAt(1, ctx, func(ctx context.Context) (int, error) { return 200, nil })
	results := p.Wait()
	if len(results) < 2 {
		t.Fatalf("len = %d, want >= 2", len(results))
	}
}

// ==================== TrySubmit ====================

func TestTrySubmitCanceledContext(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var ran atomic.Bool
	err := p.TrySubmit(ctx, func(context.Context) (int, error) { ran.Store(true); return 1, nil })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("TrySubmit error = %v, want context.Canceled", err)
	}
	results := p.Wait()
	if ran.Load() {
		t.Error("canceled task executed")
	}
	if len(results) != 1 || !errors.Is(results[0].Err, context.Canceled) || p.TotalCount() != 1 || p.FailCount() != 1 {
		t.Fatalf("results=%v total=%d failures=%d", results, p.TotalCount(), p.FailCount())
	}
}

func TestTrySubmitCanceledBeforeSend(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.shards[0].mu.Lock()
	done := make(chan error, 1)
	go func() { done <- p.TrySubmit(ctx, func(context.Context) (int, error) { return 42, nil }) }()
	deadline := time.Now().Add(5 * time.Second)
	for p.submitIdx.Load() == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if p.submitIdx.Load() == 0 {
		p.shards[0].mu.Unlock()
		<-done
		t.Fatal("submission did not reach result allocation")
	}
	cancel()
	p.shards[0].mu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("TrySubmit error = %v, want canceled", err)
	}
	results := p.Wait()
	if len(results) != 1 || !errors.Is(results[0].Err, context.Canceled) {
		t.Fatalf("results = %#v", results)
	}
}

func TestTrySubmit(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Errorf("TrySubmit: %v", err)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != 1 {
		t.Errorf("results: %+v", results)
	}
}

// ==================== WithPool / WithCfg ====================

func TestWithPool(t *testing.T) {
	err := WithPool[int](4, func(p *Pool[int]) error {
		ctx := context.Background()
		p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		results := p.Wait()
		if len(results) != 1 || results[0].Value != 1 {
			t.Errorf("results: %+v", results)
		}
		return nil
	})
	if err != nil {
		t.Errorf("WithPool: %v", err)
	}
}

func TestWithCfg(t *testing.T) {
	cfg := DefaultConfig().WithSize(4).WithTimeout(10 * time.Second)
	err := WithCfg[int](context.Background(), cfg, func(p *Pool[int]) error {
		p.Submit(p.Ctx(), func(ctx context.Context) (int, error) { return 2, nil })
		results := p.Wait()
		if len(results) != 1 || results[0].Value != 2 {
			t.Errorf("results: %+v", results)
		}
		return nil
	})
	if err != nil {
		t.Errorf("WithCfg: %v", err)
	}
}

// ==================== FailFast ====================

func TestFailFast(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	pCtx, pCancel := context.WithCancel(context.Background())
	defer pCancel()

	p, ctx := p.WithFailFast(pCtx)

	var executed atomic.Int64
	for i := 0; i < 50; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			executed.Add(1)
			return 0, errors.New("fail")
		})
	}
	p.Wait()
	_ = executed.Load()
}

// ==================== WithTimeout ====================

func TestWithTimeout_Pool(t *testing.T) {
	p := NewPool[int](4)
	p.WithTimeout(50 * time.Millisecond)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return 1, nil
		}
	})
	results := p.Wait()
	if len(results) == 0 {
		t.Fatal("no results")
	}
	if results[0].Err == nil {
		t.Error("expected timeout error")
	}
}

// ==================== Stats ====================

func TestPoolStats(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err") })
	p.Wait()

	stats := p.Stats()
	if stats.TotalTask != 2 {
		t.Errorf("TotalTask = %d, want 2", stats.TotalTask)
	}
	if stats.OkTask != 1 {
		t.Errorf("OkTask = %d, want 1", stats.OkTask)
	}
	if stats.FailTask != 1 {
		t.Errorf("FailTask = %d, want 1", stats.FailTask)
	}
	if stats.Size != 4 {
		t.Errorf("Size = %d, want 4", stats.Size)
	}
	t.Logf("Stats: %+v", stats)
}

// ==================== Results Accessors ====================

func TestPool_Errors_HasError(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	testErr := errors.New("my error")
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 0, testErr })
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	p.Wait()

	errs := p.Errors()
	if len(errs) != 1 {
		t.Errorf("Errors len = %d, want 1", len(errs))
	}
	if !p.HasError() {
		t.Error("HasError should be true")
	}
}

func TestPool_FirstError(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("first") })
	p.Wait()

	err := p.FirstError()
	if err == nil {
		t.Error("FirstError should not be nil")
	}
}

func TestPool_JoinErrors(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	p.Wait()
	if err := p.JoinErrors(); err != nil {
		t.Errorf("JoinErrors nil: %v", err)
	}

	p2 := NewPool[int](4)
	defer p2.Close()
	p2.Submit(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err1") })
	p2.Submit(ctx, func(ctx context.Context) (int, error) { return 0, errors.New("err2") })
	p2.Wait()
	if err := p2.JoinErrors(); err == nil {
		t.Error("JoinErrors should have error")
	}
}

func TestPool_Values(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		v := i
		p.Submit(ctx, func(ctx context.Context) (int, error) { return v * 10, nil })
	}
	p.Wait()
	vals := p.Values()
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

// ==================== WaitTimeout / WaitContext ====================

func TestPool_WaitTimeout(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(300 * time.Millisecond)
			return 1, nil
		})
	}

	results, completed := p.WaitTimeout(30 * time.Millisecond)
	if completed {
		t.Error("expected completed=false")
	}
	_ = results
}

func TestPool_WaitContext(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) { return 99, nil })
	results, completed := p.WaitContext(context.Background())
	if !completed {
		t.Error("expected completed=true")
	}
	if len(results) != 1 || results[0].Value != 99 {
		t.Errorf("results: %+v", results)
	}
}

// ==================== Resize ====================

func TestResizeDownThenUpWithBusyWorkers(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	for i := 0; i < 4; i++ {
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { started <- struct{}{}; <-release; return 1, nil }); err != nil {
			close(release)
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		<-started
	}
	p.Resize(1)
	p.Resize(4)
	close(release)
	// Wait until all pending retirement requests are consumed.
	deadline := time.Now().Add(5 * time.Second)
	for p.quitting.Load() != 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := p.quitting.Load(); got != 0 {
		t.Fatalf("pending retirements: %d", got)
	}
	running := make(chan struct{}, 4)
	unblock := make(chan struct{})
	for i := 0; i < 4; i++ {
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { running <- struct{}{}; <-unblock; return 2, nil }); err != nil {
			close(unblock)
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		select {
		case <-running:
		case <-time.After(5 * time.Second):
			close(unblock)
			t.Fatalf("only %d workers started", i)
		}
	}
	close(unblock)
	results := p.Wait()
	if len(results) != 8 {
		t.Fatalf("got %d results, want 8", len(results))
	}
}

func TestMultiPoolClonePreservesRingBufferOverflowStrategy(t *testing.T) {
	for _, strategy := range []core.OverflowStrategy{core.OverflowDrop, core.OverflowError} {
		t.Run(fmt.Sprintf("ring=%d", strategy), func(t *testing.T) {
			queueStrategy := core.OverflowDrop
			if strategy == core.OverflowDrop {
				queueStrategy = core.OverflowBlock
			}
			p := NewPool[int](1).WithMaxResults(0).WithOverflow(queueStrategy).WithRingBuffer(1, strategy)
			mp := p.Shard(3)
			defer mp.Close()
			for shard := 0; shard < mp.ShardCount(); shard++ {
				for i := 0; i < 2; i++ {
					value := shard*10 + i
					if err := mp.GetShard(shard).Submit(context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
						t.Fatal(err)
					}
				}
			}
			mp.Wait()
			for shard := 0; shard < mp.ShardCount(); shard++ {
				want := shard * 10
				if strategy == core.OverflowDrop {
					want++
				}
				got := mp.GetShard(shard).Flush(0)
				if len(got) != 1 || got[0].Value != want {
					t.Fatalf("shard %d flushed=%v, want %d", shard, got, want)
				}
			}
		})
	}
}

func TestResetPreservesRingBufferOverflowStrategy(t *testing.T) {
	for _, strategy := range []core.OverflowStrategy{core.OverflowDrop, core.OverflowError} {
		t.Run(fmt.Sprintf("ring=%d", strategy), func(t *testing.T) {
			queueStrategy := core.OverflowDrop
			if strategy == core.OverflowDrop {
				queueStrategy = core.OverflowBlock
			}
			p := NewPool[int](1).WithMaxResults(0).WithOverflow(queueStrategy).WithRingBuffer(1, strategy)
			defer p.Close()
			p.Wait()
			if _, err := p.Reset(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				value := i
				if err := p.Submit(context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
					t.Fatal(err)
				}
			}
			p.Wait()
			got := p.Flush(0)
			want := 0
			if strategy == core.OverflowDrop {
				want = 1
			}
			if len(got) != 1 || got[0].Value != want {
				t.Fatalf("flushed=%v, want value %d", got, want)
			}
		})
	}
}

func TestResizeWhileSubmittingPreservesEveryResult(t *testing.T) {
	const tasks = 200
	p := NewPool[int](4).WithTimeout(0)
	defer p.Close()
	start := make(chan struct{})
	resizeDone := make(chan struct{})
	go func() {
		defer close(resizeDone)
		<-start
		for i := 0; i < 100; i++ {
			p.Resize(1 + i%4)
		}
		p.Resize(4)
	}()
	close(start)
	for i := 0; i < tasks; i++ {
		value := i
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	<-resizeDone
	results, ok := p.WaitTimeout(5 * time.Second)
	if !ok || len(results) != tasks {
		t.Fatalf("completed=%v results=%d, want %d", ok, len(results), tasks)
	}
	for i, result := range results {
		if !result.Occupied || result.Err != nil || result.Value != i {
			t.Fatalf("result %d = %+v", i, result)
		}
	}
}

func TestConcurrentResizeDoesNotExceedTarget(t *testing.T) {
	for round := 0; round < 100; round++ {
		p := NewPool[int](1)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; p.Resize(2) }()
		}
		close(start)
		wg.Wait()
		started := make(chan struct{}, 3)
		release := make(chan struct{})
		for i := 0; i < 3; i++ {
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { started <- struct{}{}; <-release; return 0, nil }); err != nil {
				close(release)
				p.Close()
				t.Fatal(err)
			}
		}
		<-started
		<-started
		select {
		case <-started:
			close(release)
			p.Close()
			t.Fatalf("round %d: more than two workers ran concurrently", round)
		case <-time.After(10 * time.Millisecond):
		}
		close(release)
		p.Close()
	}
}

func TestResizeAfterClose(t *testing.T) {
	p := NewPool[int](1)
	p.Close()
	if changed := p.Resize(3); changed != 0 || p.Size() != 1 {
		t.Fatalf("resize after close changed=%d size=%d", changed, p.Size())
	}
}

func TestResize(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	added := p.Resize(8)
	if added != 4 {
		t.Errorf("Resize added = %d, want 4", added)
	}
	if p.Size() != 8 {
		t.Errorf("Size after Resize = %d, want 8", p.Size())
	}

	// Resize to smaller
	removed := p.Resize(2)
	if removed != 6 {
		t.Errorf("Resize removed = %d, want 6", removed)
	}
	if p.Size() != 2 {
		t.Errorf("Size after Resize down = %d, want 2", p.Size())
	}
}

func TestResizeAndWaitTimeoutDoesNotClearWaitGuard(t *testing.T) {
	p := NewPool[int](1)
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()
	deadline := time.After(time.Second)
	for !p.submitGuard.Load() {
		select {
		case <-deadline:
			close(release)
			<-done
			p.Close()
			t.Fatal("wait did not start")
		default:
			runtime.Gosched()
		}
	}
	p.ResizeAndWaitTimeout(1, 0)
	err := p.TrySubmit(context.Background(), func(context.Context) (int, error) { return 2, nil })
	close(release)
	<-done
	p.Close()
	if !errors.Is(err, core.ErrPoolWaiting) {
		t.Fatalf("TrySubmit error=%v, want ErrPoolWaiting", err)
	}
}

func TestResizeAndWaitTimeoutOverlappingCallsKeepSubmitGuard(t *testing.T) {
	p := NewPool[int](1)
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	done := make(chan struct{})
	go func() { p.ResizeAndWaitTimeout(1, time.Second); close(done) }()
	deadline := time.After(time.Second)
	for p.resizeGuards.Load() == 0 {
		select {
		case <-deadline:
			close(release)
			<-done
			p.Close()
			t.Fatal("wait did not start")
		default:
			runtime.Gosched()
		}
	}
	p.ResizeAndWaitTimeout(1, 0)
	err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil })
	close(release)
	<-done
	p.Wait()
	p.Close()
	if !errors.Is(err, core.ErrPoolWaiting) {
		t.Fatalf("Submit error=%v, want ErrPoolWaiting", err)
	}
}

func TestResizeAndWaitTimeoutDoesNotAccumulateWaitersWithNewTasks(t *testing.T) {
	p := NewPool[int](2)
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
			close(release)
			p.Close()
			t.Fatal(err)
		}
		p.ResizeAndWaitTimeout(2, 0)
	}
	after := runtime.NumGoroutine()
	close(release)
	p.Wait()
	p.Close()
	if after-before > 10 {
		t.Fatalf("waiters grew by %d while tasks continued to arrive", after-before)
	}
}

func TestResizeAndWaitTimeoutDoesNotAccumulateWaiters(t *testing.T) {
	p := NewPool[int](1)
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		p.ResizeAndWaitTimeout(1, 0)
	}
	after := runtime.NumGoroutine()
	close(release)
	p.Wait()
	p.Close()
	if after-before > 10 {
		t.Fatalf("waiters grew by %d after repeated timeouts", after-before)
	}
}

func TestResizeAndWaitTimeoutCleanupAfterCanceledWait(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	p.ResizeAndWaitTimeout(1, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := p.WaitContext(ctx); ok {
		close(release)
		t.Fatal("canceled wait succeeded")
	}
	resetDone := make(chan struct{})
	go func() { p.ResetWait(); close(resetDone) }()
	close(release)
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("reset did not finish")
	}
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != 2 {
		t.Fatalf("results=%v", results)
	}
}

func TestResizeAndWaitTimeoutWaitsForTaskAfterReset(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	firstStarted, firstRelease := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(firstStarted); <-firstRelease; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-firstStarted
	p.ResizeAndWaitTimeout(1, 0)
	close(firstRelease)
	p.Wait()
	p.ResetWait()
	secondStarted, secondRelease := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(secondStarted); <-secondRelease; return 2, nil }); err != nil {
		t.Fatal(err)
	}
	<-secondStarted
	done := make(chan struct{})
	go func() { p.ResizeAndWaitTimeout(1, time.Second); close(done) }()
	select {
	case <-done:
		close(secondRelease)
		t.Fatal("old batch completion released wait for new task")
	case <-time.After(20 * time.Millisecond):
	}
	close(secondRelease)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait did not finish")
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != 2 {
		t.Fatalf("results=%v", results)
	}
}

func TestResizeAndWaitTimeoutWaitsForNewSubmission(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()
	firstStarted, firstRelease := make(chan struct{}), make(chan struct{})
	secondStarted, secondRelease := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(firstStarted); <-firstRelease; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-firstStarted
	p.ResizeAndWaitTimeout(2, 0)
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(secondStarted); <-secondRelease; return 2, nil }); err != nil {
		close(firstRelease)
		t.Fatal(err)
	}
	<-secondStarted
	done := make(chan struct{})
	go func() { p.ResizeAndWaitTimeout(2, time.Second); close(done) }()
	close(firstRelease)
	select {
	case <-done:
		close(secondRelease)
		t.Fatal("wait returned while the new task was running")
	case <-time.After(20 * time.Millisecond):
	}
	close(secondRelease)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait did not finish")
	}
	p.Wait()
}

func TestResizeAndWaitTimeoutAllowsSubmissionsAfterTimeout(t *testing.T) {
	p := NewPool[int](1)
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
	p.ResizeAndWaitTimeout(1, time.Millisecond)
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	results := p.Wait()
	if len(results) != 2 || results[0].Value != 1 || results[1].Value != 2 {
		t.Fatalf("results=%v", results)
	}
}

func TestResizeAndWaitTimeoutConcurrentNextSubmission(t *testing.T) {
	for i := 0; i < 500; i++ {
		p := NewPool[int](1)
		started, release := make(chan struct{}), make(chan struct{})
		if err := p.Submit(context.Background(), func(context.Context) (int, error) {
			close(started)
			<-release
			return 1, nil
		}); err != nil {
			t.Fatal(err)
		}
		<-started
		p.ResizeAndWaitTimeout(1, 0)
		close(release)
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
			t.Fatal(err)
		}
		if results := p.Wait(); len(results) != 2 {
			t.Fatalf("iteration %d: got %d results", i, len(results))
		}
		p.Close()
	}
}

func TestResizeAndWaitTimeout(t *testing.T) {
	p := NewPool[int](8)
	defer p.Close()

	p.ResizeAndWaitTimeout(2, 5*time.Second)
	if p.Size() != 2 {
		t.Errorf("Size = %d, want 2", p.Size())
	}
}

// ==================== Pending / QueueDepth / Active ====================

func TestPending_QueueDepth(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()
	ctx := context.Background()

	if p.Pending() != 0 {
		t.Errorf("Pending idle = %d, want 0", p.Pending())
	}
	if p.QueueDepth() != 0 {
		t.Errorf("QueueDepth idle = %d, want 0", p.QueueDepth())
	}

	// Submit tasks and check
	for i := 0; i < 6; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(20 * time.Millisecond)
			return 1, nil
		})
	}
	p.Wait()
}

func TestPool_Active_Busy(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	if p.Size() != 4 {
		t.Errorf("Size = %d, want 4", p.Size())
	}
}

// ==================== StreamResults / ResultCallback ====================

func TestPoolResultCallbackPanicDoesNotLoseTask(t *testing.T) {
	var callbacks atomic.Int64
	p := NewPool[int](1).WithResultCallback(func(core.Result[int]) { callbacks.Add(1); panic("callback") })
	defer p.Close()
	for i := 0; i < 2; i++ {
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
			t.Fatal(err)
		}
	}
	results := p.Wait()
	if len(results) != 2 || results[0].Err != nil || results[1].Err != nil || results[0].Value != 42 || results[1].Value != 42 {
		t.Fatalf("results = %#v", results)
	}
	if callbacks.Load() != 2 || p.OkCount() != 2 || p.FailCount() != 0 {
		t.Fatalf("callbacks=%d success=%d failure=%d", callbacks.Load(), p.OkCount(), p.FailCount())
	}
}

func TestPoolResultCallbackPanicOnFailureAndCancellation(t *testing.T) {
	boom := errors.New("task failure")
	var callbacks atomic.Int64
	p := NewPool[int](1).WithResultCallback(func(core.Result[int]) { callbacks.Add(1); panic("callback") })
	defer p.Close()
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 0, boom }); err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Submit(ctx, func(context.Context) (int, error) { return 1, nil }); err != nil {
		close(release)
		cancel()
		t.Fatal(err)
	}
	cancel()
	close(release)
	results := p.Wait()
	if len(results) != 2 || !errors.Is(results[0].Err, boom) || !errors.Is(results[1].Err, context.Canceled) {
		t.Fatalf("results = %#v", results)
	}
	if callbacks.Load() != 2 || p.FailCount() != 2 || p.OkCount() != 0 {
		t.Fatalf("callbacks=%d failures=%d successes=%d", callbacks.Load(), p.FailCount(), p.OkCount())
	}
}

func TestPoolCompletionPanicPreservesTaskResult(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	if err := p.SubmitWithCompletion(context.Background(), func(context.Context) (int, error) { return 42, nil }, func(core.Result[int]) { panic("completion") }); err != nil {
		t.Fatal(err)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Err != nil || results[0].Value != 42 || p.OkCount() != 1 || p.FailCount() != 0 {
		t.Fatalf("results=%#v success=%d failure=%d", results, p.OkCount(), p.FailCount())
	}
}

func TestPoolCompletionPanicOnRejectedSubmission(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls int
	err := p.SubmitWithCompletion(ctx, nil, func(core.Result[int]) { calls++; panic("completion") })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("error=%v calls=%d", err, calls)
	}
	results := p.Wait()
	if len(results) != 1 || !errors.Is(results[0].Err, context.Canceled) {
		t.Fatalf("results=%#v", results)
	}
}

func TestPool_StreamResults(t *testing.T) {
	p := NewPool[int](4)
	p.WithStreaming(200)
	defer p.Close()
	ctx := context.Background()

	n := 50
	for i := 0; i < n; i++ {
		v := i
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return v, nil
		})
	}

	ch := p.StreamResults()
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
	p.Wait()
	wg.Wait()

	sort.Ints(collected)
	if len(collected) != n {
		t.Errorf("collected = %d, want %d", len(collected), n)
	}
}

func TestPool_ResultCallback(t *testing.T) {
	var collected []int
	var mu sync.Mutex
	p := NewPool[int](4)
	p.WithResultCallback(func(r core.Result[int]) {
		mu.Lock()
		collected = append(collected, r.Value)
		mu.Unlock()
	})
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		v := i
		p.Submit(ctx, func(ctx context.Context) (int, error) { return v, nil })
	}
	p.Wait()

	sort.Ints(collected)
	if len(collected) != 20 {
		t.Errorf("collected = %d, want 20", len(collected))
	}
}

// ==================== WithSubmitTimeout ====================

func TestPool_WithSubmitTimeout(t *testing.T) {
	p := NewPool[int](1)
	p.WithSubmitTimeout(10 * time.Millisecond)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); p.Close() })
	ctx := context.Background()
	if err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	for i := 0; i < cap(p.taskCh); i++ {
		if err := p.Submit(ctx, func(context.Context) (int, error) { return 2, nil }); err != nil {
			t.Fatal(err)
		}
	}
	err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 3, nil
	})
	if !errors.Is(err, core.ErrSubmitTimeout) {
		t.Fatalf("Submit error = %v, want ErrSubmitTimeout", err)
	}
	unblock()
	p.Wait()
}

// ==================== RingBuffer / Overflow ====================

func TestRingBuffer(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(100, core.OverflowBlock)
	defer p.Close()
	ctx := context.Background()

	var counter atomic.Int64
	for i := 0; i < 200; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
		if err != nil {
			t.Logf("Submit(%d) error: %v", i, err)
		}
	}
	p.Wait()
	if counter.Load() != 200 {
		t.Errorf("counter = %d, want 200", counter.Load())
	}
}

// ==================== Flush ====================

func TestFlush(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(100, core.OverflowBlock)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(50 * time.Millisecond)
			return 1, nil
		})
	}

	flushed := p.Flush(10)
	_ = flushed // Flush returns results
}

// ==================== RingBufDropped ====================

func TestRingBufDropped(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(10, core.OverflowDrop)
	defer p.Close()

	dropped := p.RingBufDropped()
	_ = dropped
}

// ==================== StreamDropped ====================

func TestStreamDropped(t *testing.T) {
	p := NewPool[int](4)
	p.WithStreaming(5)
	defer p.Close()

	dropped := p.StreamDropped()
	_ = dropped
}

// ==================== MaxResults / MaxPending ====================

func TestMaxPendingBeyondInt32DoesNotReject(t *testing.T) {
	limit := int64(1)<<32 + 1
	if int64(int(limit)) != limit {
		t.Skip("requires 64-bit int")
	}
	p := NewPool[int](1).WithMaxPending(int(limit)).WithOverflow(core.OverflowError)
	started, release := make(chan struct{}), make(chan struct{})
	defer p.Close()
	defer close(release)
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 0, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	for i := 0; i < 2; i++ {
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
			t.Fatalf("submission %d: %v", i, err)
		}
	}
}

func TestMaxResultsBeyondInt32DoesNotTruncate(t *testing.T) {
	limit := int64(1)<<32 + 1
	if int64(int(limit)) != limit {
		t.Skip("requires 64-bit int")
	}
	old := core.GetDefaultMaxResults()
	defer core.SetDefaultMaxResults(old)
	for _, global := range []bool{false, true} {
		if global {
			core.SetDefaultMaxResults(int(limit))
		}
		p := NewPool[int](1)
		if !global {
			p.WithMaxResults(int(limit))
		}
		for i := 0; i < 2; i++ {
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
				t.Fatal(err)
			}
		}
		got := p.Wait()
		p.Close()
		if len(got) != 2 {
			t.Fatalf("global=%v got %d results, want 2", global, len(got))
		}
	}
}

func TestMaxResults(t *testing.T) {
	p := NewPool[int](4)
	p.WithMaxResults(100)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	p.Wait()
}

// ==================== PoolBuilder ====================

func TestPoolBuilder(t *testing.T) {
	err := NewBuilder[int]().Worker(4).Timeout(10 * time.Second).FailFast().Run(func(ctx context.Context, p *Pool[int]) error {
		p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		results := p.Wait()
		if len(results) != 1 || results[0].Value != 1 {
			t.Errorf("results: %+v", results)
		}
		return nil
	})
	if err != nil {
		t.Errorf("Builder: %v", err)
	}
}

func TestPoolBuilder_Run(t *testing.T) {
	err := NewBuilder[int]().Worker(4).Timeout(10 * time.Second).Run(func(ctx context.Context, p *Pool[int]) error {
		p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		results := p.Wait()
		if len(results) != 1 || results[0].Value != 1 {
			t.Errorf("results: %+v", results)
		}
		return nil
	})
	if err != nil {
		t.Errorf("Run: %v", err)
	}
}

// ==================== Close / CloseAndWait / ResetWait / WaitAndClose ====================

func TestTimedWaitResetRepeatedCycles(t *testing.T) {
	p := NewPool[int](1).WithTimeout(0).WithStreaming(2)
	defer p.Close()
	for cycle := 0; cycle < 20; cycle++ {
		started := make(chan struct{})
		if err := p.Submit(context.Background(), func(ctx context.Context) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
		<-started
		if _, ok := p.WaitTimeout(time.Millisecond); ok {
			t.Fatal("wait unexpectedly completed before timeout")
		}
		p.ResetWait()
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
			t.Fatal(err)
		}
		results, ok := p.WaitTimeout(time.Second)
		if !ok || len(results) != 1 || results[0].Err != nil || results[0].Value != 42 {
			t.Fatalf("cycle %d: ok=%v results=%+v", cycle, ok, results)
		}
		stream := p.StreamResults()
		result, open := <-stream
		if !open || result.Value != 42 || result.Err != nil {
			t.Fatalf("cycle %d: stream result=%+v open=%v", cycle, result, open)
		}
		if _, open = <-stream; open {
			t.Fatalf("cycle %d: stream has extra result", cycle)
		}
		p.ResetWait()
	}
}

func TestTimedWaitPartialResultsAndResetJoinsRunningTask(t *testing.T) {
	p := NewPool[int](2).WithTimeout(0).WithStreaming(2)
	release := make(chan struct{})
	defer p.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 99, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if result := <-p.StreamResults(); result.Value != 42 || result.Err != nil {
		t.Fatalf("first result = %+v", result)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results, ok := p.WaitContext(ctx)
	if ok || len(results) != 2 || !results[0].Occupied || results[0].Value != 42 || results[0].Err != nil || results[1].Occupied {
		t.Fatalf("partial results: ok=%v results=%+v", ok, results)
	}
	resetDone := make(chan struct{})
	go func() { p.ResetWait(); close(resetDone) }()
	select {
	case <-resetDone:
		t.Fatal("ResetWait returned while task was running")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-resetDone:
	case <-time.After(5 * time.Second):
		t.Fatal("ResetWait did not finish after task exited")
	}
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 7, nil }); err != nil {
		t.Fatal(err)
	}
	results = p.Wait()
	if len(results) != 1 || results[0].Value != 7 || results[0].Err != nil {
		t.Fatalf("results after reset = %+v", results)
	}
}

func TestTimedWaitConcurrentCloseWithStreaming(t *testing.T) {
	for i := 0; i < 50; i++ {
		p := NewPool[int](1).WithTimeout(0).WithStreaming(4)
		started := make(chan struct{})
		if err := p.Submit(context.Background(), func(ctx context.Context) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		}); err != nil {
			p.Close()
			t.Fatal(err)
		}
		<-started
		waitDone := make(chan struct{})
		go func() { p.WaitTimeout(time.Millisecond); close(waitDone) }()
		closeDone := make(chan struct{})
		go func() { p.Close(); close(closeDone) }()
		select {
		case <-waitDone:
		case <-time.After(time.Second):
			t.Fatal("timed wait did not return")
		}
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatal("close did not finish")
		}
		select {
		case _, ok := <-p.StreamResults():
			if ok {
				if _, ok = <-p.StreamResults(); ok {
					t.Fatal("unexpected extra stream result")
				}
			}
		case <-time.After(time.Second):
			t.Fatal("stream did not close")
		}
	}
}

func TestTimedWaitBoundsInFlightSubmission(t *testing.T) {
	for _, mode := range []string{"timeout", "context"} {
		t.Run(mode, func(t *testing.T) {
			p := NewPool[int](1)
			p.addInFlight.Add(1)
			done := make(chan bool, 1)
			go func() {
				if mode == "timeout" {
					_, ok := p.WaitTimeout(10 * time.Millisecond)
					done <- ok
				} else {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					_, ok := p.WaitContext(ctx)
					done <- ok
				}
			}()
			select {
			case ok := <-done:
				p.addInFlight.Add(-1)
				p.Close()
				if ok {
					t.Fatal("wait unexpectedly completed")
				}
			case <-time.After(250 * time.Millisecond):
				p.addInFlight.Add(-1)
				<-done
				p.Close()
				t.Fatal("wait blocked beyond cancellation or timeout")
			}
		})
	}
}

func TestTimedWaitCancelsLateRegistrationAndResetJoins(t *testing.T) {
	p := NewPool[int](1)
	p.addInFlight.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := p.WaitContext(ctx); ok {
		p.addInFlight.Add(-1)
		p.Close()
		t.Fatal("canceled wait unexpectedly completed")
	}
	taskCtx, taskCancel := context.WithCancel(context.Background())
	trackedCancel := p.trackCancel(0, taskCancel)
	if taskCtx.Err() == nil {
		p.addInFlight.Add(-1)
		trackedCancel()
		p.Close()
		t.Fatal("late registration was not canceled")
	}
	resetDone := make(chan struct{})
	go func() { p.ResetWait(); close(resetDone) }()
	select {
	case <-resetDone:
		p.addInFlight.Add(-1)
		p.Close()
		t.Fatal("ResetWait returned before submit cleanup")
	case <-time.After(20 * time.Millisecond):
	}
	p.addInFlight.Add(-1)
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("ResetWait did not finish")
	}
	defer p.Close()
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != 42 || results[0].Err != nil {
		t.Fatalf("unexpected results after reset: %+v", results)
	}
}

func TestConcurrentResetAndCloseAfterWait(t *testing.T) {
	for i := 0; i < 100; i++ {
		p := NewPool[int](1)
		p.Wait()
		start := make(chan struct{})
		resetDone := make(chan error, 1)
		closeDone := make(chan struct{})
		go func() {
			<-start
			_, err := p.Reset()
			resetDone <- err
		}()
		go func() {
			<-start
			p.Close()
			close(closeDone)
		}()
		close(start)
		select {
		case err := <-resetDone:
			if err != nil {
				t.Fatalf("iteration %d: Reset: %v", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: Reset blocked", i)
		}
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: Close blocked", i)
		}
		p.Close()
	}
}

func TestTimedCloseBoundsInFlightSubmissionWait(t *testing.T) {
	p := NewPool[int](1)
	p.addInFlight.Add(1)
	type result struct {
		ok   bool
		done <-chan struct{}
	}
	returned := make(chan result, 1)
	go func() {
		ok, done := p.CloseAndWaitTimeout(10 * time.Millisecond)
		returned <- result{ok, done}
	}()
	select {
	case r := <-returned:
		if r.ok || r.done == nil {
			p.addInFlight.Add(-1)
			p.Close()
			t.Fatalf("expected timeout and completion channel, got ok=%v done=%v", r.ok, r.done)
		}
		p.addInFlight.Add(-1)
		select {
		case <-r.done:
		case <-time.After(time.Second):
			t.Fatal("workers did not exit")
		}
		p.Close()
	case <-time.After(250 * time.Millisecond):
		p.addInFlight.Add(-1)
		<-returned
		p.Close()
		t.Fatal("timed close waited for an in-flight submit beyond its timeout")
	}
}

func TestRepeatedTimedCloseReturnsCompletionChannel(t *testing.T) {
	p := NewPool[int](1)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	first := make(chan struct{})
	go func() { p.Close(); close(first) }()
	<-p.done
	ok, done := p.CloseAndWaitTimeout(time.Millisecond)
	if ok || done == nil {
		close(release)
		<-first
		t.Fatalf("closing pool returned ok=%v, done=%v", ok, done)
	}
	select {
	case <-done:
		close(release)
		<-first
		t.Fatal("completion signaled before worker exited")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("completion was not signaled")
	}
	<-first
}

func TestTimedCloseAfterCompletedClose(t *testing.T) {
	p := NewPool[int](1)
	p.Close()
	ok, done := p.CloseAndWaitTimeout(time.Second)
	if !ok || done != nil {
		t.Fatalf("completed close returned ok=%v, done=%v", ok, done)
	}
}

func TestCloseWaitsAfterTimedClose(t *testing.T) {
	p := NewPool[int](1)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	ok, workerDone := p.CloseAndWaitTimeout(time.Millisecond)
	if ok || workerDone == nil {
		close(release)
		t.Fatal("expected timed close to return a worker completion channel")
	}
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-closed:
		close(release)
		<-workerDone
		t.Fatal("Close returned before the running worker exited")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
}

func TestConcurrentCloseWaitsForWorker(t *testing.T) {
	p := NewPool[int](1)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	first := make(chan struct{})
	go func() { p.Close(); close(first) }()
	<-p.done
	second := make(chan struct{})
	go func() { p.Close(); close(second) }()
	select {
	case <-second:
		close(release)
		<-first
		t.Fatal("concurrent Close returned before worker exited")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("concurrent Close did not return after worker exited")
	}
	<-first
}

func TestClose(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	p.Wait()
	p.Close()
}

func TestCloseAndWait(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 1, nil
	})
	p.CloseAndWait()
}

func TestCloseAndWaitTimeout(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(200 * time.Millisecond)
		return 1, nil
	})
	ok, doneCh := p.CloseAndWaitTimeout(300 * time.Millisecond)
	if ok {
		t.Log("workers stopped within timeout")
	}
	select {
	case <-doneCh:
		t.Log("done channel closed")
	case <-time.After(500 * time.Millisecond):
		// may complete
	}
}

func TestCloseByIdle(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	p.Wait()
	p.CloseByIdle(1 * time.Second)
}

func TestResetWait(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	p.Wait()

	p2 := p.ResetWait()
	defer p2.Close()

	p2.Submit(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	results := p2.Wait()
	if len(results) != 1 || results[0].Value != 2 {
		t.Errorf("ResetWait: %+v", results)
	}
}

func TestReset(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	p.Wait()

	p2, err := p.Reset()
	if err != nil {
		t.Fatalf("Reset: %v", err)
	}
	defer p2.Close()

	p2.Submit(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	results := p2.Wait()
	if len(results) != 1 || results[0].Value != 2 {
		t.Errorf("Reset: %+v", results)
	}
}

// ==================== WaitAndClose ====================

func TestWaitAndClose(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	results := p.WaitAndClose()
	if len(results) != 1 || results[0].Value != 1 {
		t.Errorf("WaitAndClose: %+v", results)
	}
}

// ==================== Busy / TotalCount / Active ====================

func TestBusy_TotalCount_Active(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	if p.Size() != 4 {
		t.Errorf("Size = %d, want 4", p.Size())
	}
	if p.TotalCount() != 0 {
		t.Errorf("TotalCount = %d, want 0", p.TotalCount())
	}
	_ = p.Busy()
	_ = p.Active()
}

// ==================== WithFF variants ====================

func TestPool_WithFFCtx(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFCtx(ctx)
	_ = ctx
}

func TestPool_WithFFTraceID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFTraceID(ctx)
	traceID := core.GetTraceID(ctx)
	if traceID == "" {
		t.Error("trace id should not be empty")
	}
}

func TestPool_WithFFSto(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFSto(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithFFStoTID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFStoTID(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithFFTimeout(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFTimeout(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithCtxTraceID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithCtxTraceID(ctx)
	traceID := core.GetTraceID(ctx)
	if traceID == "" {
		t.Error("trace id should not be empty")
	}
}

func TestPool_WithFFTOTID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFTOTID(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithFFTOSto(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFTOSto(ctx, 10*time.Second, 5*time.Second)
	_ = ctx
}

func TestPool_WithFFTOStoTID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithFFTOStoTID(ctx, 10*time.Second, 5*time.Second)
	_ = ctx
}

func TestPool_WithCtxTimeout(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithCtxTimeout(ctx, 10*time.Second)
	_ = ctx
}

// ==================== SubmitAt 边界与并发 ====================

func TestPoolValuesSkipsUnoccupiedSubmitAtSlots(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	if err := p.SubmitAt(3, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	values := p.Values()
	if len(values) != 1 || values[0] != 42 {
		t.Fatalf("Values() = %v, want [42]", values)
	}
}

func TestPoolOkCountSparseSubmitAt(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	if err := p.SubmitAt(3, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	if got := p.OkCount(); got != 1 {
		t.Fatalf("OkCount() = %d, want 1", got)
	}
}

func TestPoolTotalCountSparseAndRepeatedSubmitAt(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	for _, index := range []int{3, 3} {
		if err := p.SubmitAt(index, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 7, nil }); err != nil {
		t.Fatal(err)
	}
	results := p.Wait()
	if p.TotalCount() != 3 || p.OkCount() != 3 {
		t.Fatalf("total=%d success=%d", p.TotalCount(), p.OkCount())
	}
	if len(results) != 5 || results[3].Value != 42 || results[4].Value != 7 {
		t.Fatalf("results = %#v", results)
	}
	p.ResetWait()
	if p.TotalCount() != 0 {
		t.Fatalf("total after reset = %d", p.TotalCount())
	}
}

func TestPoolCanceledSubmissionFailureStats(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprint(indexed), func(t *testing.T) {
			p := NewPool[int](1)
			defer p.Close()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var err error
			if indexed {
				err = p.SubmitAt(3, ctx, nil)
			} else {
				err = p.Submit(ctx, nil)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("submission error = %v", err)
			}
			p.Wait()
			if p.TotalCount() != 1 || p.FailCount() != 1 || !p.HasError() || len(p.Errors()) != 1 {
				t.Fatalf("total=%d failures=%d hasError=%v errors=%v", p.TotalCount(), p.FailCount(), p.HasError(), p.Errors())
			}
		})
	}
}

func TestRejectedSubmissionRemainsInFlightUntilRecorded(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprint(indexed), func(t *testing.T) {
			p := NewPool[int](1)
			defer p.Close()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			p.shards[0].mu.Lock()
			submitted := make(chan struct{})
			go func() {
				defer close(submitted)
				if indexed {
					_ = p.SubmitAt(0, ctx, nil)
				} else {
					_ = p.Submit(ctx, nil)
				}
			}()
			deadline := time.Now().Add(5 * time.Second)
			for p.submitIdx.Load() == 0 && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			if p.submitIdx.Load() == 0 {
				p.shards[0].mu.Unlock()
				<-submitted
				t.Fatal("submission did not reach result recording")
			}
			inFlight := p.addInFlight.Load()
			p.shards[0].mu.Unlock()
			<-submitted
			if inFlight != 1 {
				t.Fatalf("in-flight submissions while recording = %d, want 1", inFlight)
			}
			results := p.Wait()
			if len(results) != 1 || !errors.Is(results[0].Err, context.Canceled) {
				t.Fatalf("results = %#v", results)
			}
		})
	}
}

func TestPoolOkCountCompletedAndUnstored(t *testing.T) {
	p := NewPool[int](1).WithMaxResults(1)
	defer p.Close()
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	if got := p.OkCount(); got != 0 {
		close(release)
		t.Fatalf("running task counted as success: %d", got)
	}
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	p.Wait()
	if got := p.OkCount(); got != 2 {
		t.Fatalf("OkCount() = %d, want 2", got)
	}
	p.ResetWait()
	if got := p.OkCount(); got != 0 {
		t.Fatalf("after reset OkCount() = %d", got)
	}
}

func TestSubmitAtRejectedConcurrentSubmitIndex(t *testing.T) {
	const workers, attempts = 32, 2000
	p := NewPool[int](1)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := make(chan struct{})
	var wg sync.WaitGroup
	var highest atomic.Int64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(at bool) {
			defer wg.Done()
			<-start
			for j := 0; j < attempts; j++ {
				if at {
					index := p.submitIdx.Load() + 1
					for old := highest.Load(); index > old && !highest.CompareAndSwap(old, index); old = highest.Load() {
					}
					_ = p.SubmitAt(int(index), ctx, nil)
				} else {
					_ = p.Submit(ctx, nil)
				}
			}
		}(i%2 == 0)
	}
	close(start)
	wg.Wait()
	if got, want := p.submitIdx.Load(), int64(workers/2*attempts); got < want {
		t.Fatalf("submission index regressed: got %d, want at least %d", got, want)
	}
	if got, want := p.submitIdx.Load(), highest.Load()+1; got < want {
		t.Fatalf("submission index = %d, want at least %d", got, want)
	}
}

func TestSubmitAtOverflowRecordsIndexedResult(t *testing.T) {
	for _, strategy := range []core.OverflowStrategy{core.OverflowDrop, core.OverflowError} {
		t.Run(fmt.Sprint(strategy), func(t *testing.T) {
			p := NewPool[int](1).WithMaxPending(1).WithOverflow(strategy)
			defer p.Close()
			started, release := make(chan struct{}), make(chan struct{})
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 42, nil }); err != nil {
				t.Fatal(err)
			}
			<-started
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 21, nil }); err != nil {
				close(release)
				t.Fatal(err)
			}
			err := p.SubmitAt(7, context.Background(), func(context.Context) (int, error) { return 0, nil })
			close(release)
			if strategy == core.OverflowError && !errors.Is(err, core.ErrQueueOverflow) {
				t.Fatalf("error = %v", err)
			}
			if strategy == core.OverflowDrop && err != nil {
				t.Fatalf("drop error = %v", err)
			}
			results := p.Wait()
			if len(results) != 8 || !results[7].Occupied || !errors.Is(results[7].Err, core.ErrQueueOverflow) {
				t.Fatalf("results = %#v", results)
			}
			if results[0].Value != 42 || results[0].Err != nil || results[1].Value != 21 || results[1].Err != nil || p.TotalCount() != 3 || p.FailCount() != 1 {
				t.Fatalf("result[0] = %#v, total = %d, failures = %d", results[0], p.TotalCount(), p.FailCount())
			}
		})
	}
}

func TestSubmitAtOverflowPreservesCompletedResult(t *testing.T) {
	for _, strategy := range []core.OverflowStrategy{core.OverflowDrop, core.OverflowError} {
		t.Run(fmt.Sprint(strategy), func(t *testing.T) {
			p := NewPool[int](1).WithMaxPending(1).WithOverflow(strategy)
			defer p.Close()
			if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
				t.Fatal(err)
			}
			for p.OkCount() == 0 {
				runtime.Gosched()
			}
			started, release := make(chan struct{}), make(chan struct{})
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
				t.Fatal(err)
			}
			<-started
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
				close(release)
				t.Fatal(err)
			}
			err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { return 99, nil })
			close(release)
			results := p.Wait()
			if strategy == core.OverflowError && !errors.Is(err, core.ErrQueueOverflow) {
				t.Fatalf("error=%v", err)
			}
			if strategy == core.OverflowDrop && err != nil {
				t.Fatalf("error=%v", err)
			}
			if results[0].Err != nil || results[0].Value != 42 {
				t.Fatalf("results=%v", results)
			}
			if errs := p.Errors(); len(errs) != 1 || !errors.Is(errs[0], core.ErrQueueOverflow) {
				t.Fatalf("errors=%v", errs)
			}
		})
	}
}

func TestSubmitAtTimeoutRecordsFreshIndex(t *testing.T) {
	p := NewPool[int](1).WithSubmitTimeout(20 * time.Millisecond)
	defer p.Close()
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	for i := 0; i < cap(p.taskCh); i++ {
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
			close(release)
			t.Fatal(err)
		}
	}
	err := p.SubmitAt(7, context.Background(), func(context.Context) (int, error) { return 99, nil })
	close(release)
	results := p.Wait()
	if !errors.Is(err, core.ErrSubmitTimeout) || len(results) <= 7 || !errors.Is(results[7].Err, core.ErrSubmitTimeout) {
		t.Fatalf("error=%v results=%v", err, results)
	}
}

func TestSubmitAtTimeoutPreservesCompletedResult(t *testing.T) {
	p := NewPool[int](1).WithSubmitTimeout(20 * time.Millisecond)
	defer p.Close()
	if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	for p.OkCount() == 0 {
		runtime.Gosched()
	}
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	for i := 0; i < cap(p.taskCh); i++ {
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
			close(release)
			t.Fatal(err)
		}
	}
	err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { return 99, nil })
	close(release)
	results := p.Wait()
	if !errors.Is(err, core.ErrSubmitTimeout) {
		t.Fatalf("error=%v", err)
	}
	if results[0].Err != nil || results[0].Value != 42 {
		t.Fatalf("results=%v", results)
	}
	if errs := p.Errors(); len(errs) != 1 || !errors.Is(errs[0], core.ErrSubmitTimeout) {
		t.Fatalf("errors=%v", errs)
	}
}

func TestSubmitAtCanceledPreservesCompletedResult(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	done := make(chan struct{})
	if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	for p.OkCount() == 0 {
		runtime.Gosched()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.SubmitAt(0, ctx, func(context.Context) (int, error) { close(done); return 99, nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	results := p.Wait()
	if len(results) != 2 || results[0].Value != 42 || results[0].Err != nil || !errors.Is(results[1].Err, context.Canceled) {
		t.Fatalf("results=%v", results)
	}
	select {
	case <-done:
		t.Fatal("canceled task ran")
	default:
	}
}

func TestSubmitAtAfterWaitPreservesCompletedResult(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { return 99, nil }); !errors.Is(err, core.ErrPoolWaited) {
		t.Fatalf("error=%v", err)
	}
	values := p.Values()
	if len(values) != 1 || values[0] != 42 {
		t.Fatalf("values=%v, want [42]", values)
	}
	if errs := p.Errors(); len(errs) != 1 || !errors.Is(errs[0], core.ErrPoolWaited) {
		t.Fatalf("errors=%v", errs)
	}
	if p.TotalCount() != 2 || p.OkCount() != 1 || p.FailCount() != 1 {
		t.Fatalf("counts=%d/%d/%d", p.TotalCount(), p.OkCount(), p.FailCount())
	}
}

func TestSubmitAtAfterClosePreservesCompletedResult(t *testing.T) {
	p := NewPool[int](1)
	if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	p.Close()
	const attempts = 32
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.SubmitAt(0, context.Background(), func(context.Context) (int, error) { return 99, nil }); !errors.Is(err, core.ErrPoolClosed) {
				t.Errorf("error=%v, want ErrPoolClosed", err)
			}
		}()
	}
	wg.Wait()
	if values := p.Values(); len(values) != 1 || values[0] != 42 {
		t.Fatalf("values=%v", values)
	}
	if errs := p.Errors(); len(errs) != attempts {
		t.Fatalf("errors=%v", errs)
	} else {
		for _, err := range errs {
			if !errors.Is(err, core.ErrPoolClosed) {
				t.Fatalf("error=%v", err)
			}
		}
	}
	if p.TotalCount() != attempts+1 || p.OkCount() != 1 || p.FailCount() != attempts {
		t.Fatalf("counts=%d/%d/%d", p.TotalCount(), p.OkCount(), p.FailCount())
	}
}

func TestTrySubmitAfterWaitReportsWaited(t *testing.T) {
	p := NewPool[int](1)
	defer p.Close()
	p.Wait()
	err := p.TrySubmit(context.Background(), func(context.Context) (int, error) { return 1, nil })
	if !errors.Is(err, core.ErrPoolWaited) {
		t.Fatalf("error=%v, want ErrPoolWaited", err)
	}
}

func TestTrySubmitWhileWaitingReportsWaiting(t *testing.T) {
	p := NewPool[int](1)
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()
	deadline := time.After(5 * time.Second)
	for !p.submitGuard.Load() {
		select {
		case <-deadline:
			close(release)
			<-done
			p.Close()
			t.Fatal("Wait did not start")
		default:
			runtime.Gosched()
		}
	}
	err := p.TrySubmit(context.Background(), func(context.Context) (int, error) { return 2, nil })
	close(release)
	<-done
	p.Close()
	if !errors.Is(err, core.ErrPoolWaiting) {
		t.Fatalf("error=%v, want ErrPoolWaiting", err)
	}
}

func TestRejectedSubmitCountsAfterWaitOrClose(t *testing.T) {
	for _, closed := range []bool{false, true} {
		for _, indexed := range []bool{false, true} {
			t.Run(fmt.Sprintf("closed=%v/indexed=%v", closed, indexed), func(t *testing.T) {
				p := NewPool[int](1)
				if closed {
					p.Close()
				} else {
					p.Wait()
				}
				fn := func(context.Context) (int, error) { return 1, nil }
				var err error
				if indexed {
					err = p.SubmitAt(0, context.Background(), fn)
				} else {
					err = p.Submit(context.Background(), fn)
				}
				if err == nil {
					t.Fatal("expected rejection")
				}
				if p.TotalCount() != 1 || p.FailCount() != 1 || p.OkCount() != 0 || len(p.Errors()) != 1 {
					t.Fatalf("total=%d failures=%d successes=%d errors=%v", p.TotalCount(), p.FailCount(), p.OkCount(), p.Errors())
				}
				p.Close()
			})
		}
	}
}

func TestTrySubmitOverflowCounts(t *testing.T) {
	for _, strategy := range []core.OverflowStrategy{core.OverflowDrop, core.OverflowError} {
		t.Run(fmt.Sprint(strategy), func(t *testing.T) {
			p := NewPool[int](1).WithMaxPending(1).WithOverflow(strategy)
			defer p.Close()
			started, release := make(chan struct{}), make(chan struct{})
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 1, nil }); err != nil {
				t.Fatal(err)
			}
			<-started
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 2, nil }); err != nil {
				close(release)
				t.Fatal(err)
			}
			err := p.TrySubmit(context.Background(), func(context.Context) (int, error) { return 3, nil })
			close(release)
			if strategy == core.OverflowError && !errors.Is(err, core.ErrQueueOverflow) {
				t.Fatalf("error = %v", err)
			}
			if strategy == core.OverflowDrop && err != nil {
				t.Fatalf("drop error = %v", err)
			}
			results := p.Wait()
			if p.TotalCount() != 3 || p.OkCount() != 2 || p.FailCount() != 1 {
				t.Fatalf("total=%d success=%d fail=%d", p.TotalCount(), p.OkCount(), p.FailCount())
			}
			if len(results) != 3 || !errors.Is(results[2].Err, core.ErrQueueOverflow) || len(p.Errors()) != 1 || !errors.Is(p.Errors()[0], core.ErrQueueOverflow) {
				t.Fatalf("results=%v errors=%v", results, p.Errors())
			}
		})
	}
}

func TestSubmitAt_IndexBounds(t *testing.T) {
	p := NewPool[int](16)
	defer p.Close()
	ctx := context.Background()

	if err := p.SubmitAt(0, ctx, func(ctx context.Context) (int, error) { return 1, nil }); err != nil {
		t.Fatalf("SubmitAt(0): %v", err)
	}
	if err := p.SubmitAt(100, ctx, func(ctx context.Context) (int, error) { return 100, nil }); err != nil {
		t.Fatalf("SubmitAt(100): %v", err)
	}

	if err := p.SubmitAt(core.MaxPoolIndex+1, ctx, func(ctx context.Context) (int, error) {
		return 0, nil
	}); err != core.ErrInvalidIndex {
		t.Fatalf("SubmitAt(MaxPoolIndex+1) = %v, want ErrInvalidIndex", err)
	}

	if err := p.SubmitAt(math.MaxInt, ctx, func(ctx context.Context) (int, error) {
		return 0, nil
	}); err != core.ErrInvalidIndex {
		t.Fatalf("SubmitAt(MaxInt) = %v, want ErrInvalidIndex", err)
	}

	if err := p.SubmitAt(-1, ctx, func(ctx context.Context) (int, error) {
		return 0, nil
	}); err != core.ErrInvalidIndex {
		t.Fatalf("SubmitAt(-1) = %v, want ErrInvalidIndex", err)
	}
}

func TestProduction_SubmitAt_Concurrent(t *testing.T) {
	p := NewPool[int](16)
	defer p.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	concurrency := 100
	eachCount := 1000
	var errCount int64

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < eachCount; i++ {
				idx := gid*eachCount + i
				if err := p.SubmitAt(idx, ctx, func(ctx context.Context) (int, error) {
					return idx, nil
				}); err != nil {
					atomic.AddInt64(&errCount, 1)
				}
			}
		}(g)
	}
	wg.Wait()
	if errCount > 0 {
		t.Fatalf("concurrent SubmitAt: %d failures, want 0", errCount)
	}
	results := p.Wait()
	if len(results) != concurrency*eachCount {
		t.Fatalf("got %d results, want %d", len(results), concurrency*eachCount)
	}
	for i, result := range results {
		if result.Err != nil || result.Value != i {
			t.Fatalf("result[%d] = %+v", i, result)
		}
	}
}

func TestProduction_SubmitAt_ClosedPool(t *testing.T) {
	p := NewPool[int](16)
	p.Close()
	ctx := context.Background()

	err := p.SubmitAt(0, ctx, func(ctx context.Context) (int, error) {
		return 0, nil
	})
	if err == nil {
		t.Fatal("expected error on closed pool, got nil")
	}
}

func TestPool_WithCtxTOTID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithCtxTOTID(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithCtxSto(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithCtxSto(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithCtxStoTID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()
	p, ctx = p.WithCtxStoTID(ctx, 10*time.Second)
	_ = ctx
}

func TestPool_WithOverflow(t *testing.T) {
	p := NewPool[int](4)
	p.WithOverflow(core.OverflowBlock)
	defer p.Close()
}

// ==================== AutoScale ====================

func TestAutoScaleResetAfterWait(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()
	p.EnableAutoScale(&core.AutoScaleConfig{MinWorkers: 2, MaxWorkers: 4, CheckInterval: time.Millisecond})
	for i := 0; i < 20; i++ {
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
			t.Fatal(err)
		}
		p.Wait()
		if _, err := p.Reset(); err != nil {
			t.Fatal(err)
		}
	}
	p.DisableAutoScale()
}

func TestResetAfterTimeoutAllowsTaskResize(t *testing.T) {
	p := NewPool[int](1)
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		p.Resize(2)
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, ok := p.WaitTimeout(time.Millisecond); ok {
		close(release)
		p.Close()
		t.Fatal("task unexpectedly completed")
	}
	resetDone := make(chan error, 1)
	go func() { _, err := p.Reset(); resetDone <- err }()
	time.Sleep(50 * time.Millisecond)
	close(release)
	select {
	case err := <-resetDone:
		if err != nil {
			t.Fatal(err)
		}
		p.Close()
	case <-time.After(time.Second):
		t.Fatal("Reset blocked task Resize while waiting for cleanup")
	}
}

func TestAutoScaleResetAfterCloseInvalidatesOldCheck(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()
	p.EnableAutoScale(&core.AutoScaleConfig{MinWorkers: 2, MaxWorkers: 4, CheckInterval: time.Hour})
	stale := p.autoScale
	p.Wait()
	p.Close()
	if _, err := p.Reset(); err != nil {
		t.Fatal(err)
	}
	p.resizeAutoScale(stale, 4)
	if p.Size() != 2 {
		t.Fatalf("old check changed size to %d", p.Size())
	}
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 7, nil }); err != nil {
		t.Fatal(err)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != 7 || results[0].Err != nil {
		t.Fatalf("results = %#v", results)
	}
}

func TestAutoScaleStaleCheckCannotResize(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()
	p.EnableAutoScale(&core.AutoScaleConfig{MinWorkers: 2, MaxWorkers: 4, CheckInterval: time.Hour})
	stale := p.autoScale
	p.DisableAutoScale()
	p.resizeAutoScale(stale, 4)
	if got := p.Size(); got != 2 {
		t.Fatalf("stale check resized disabled pool to %d", got)
	}
	p.EnableAutoScale(&core.AutoScaleConfig{MinWorkers: 2, MaxWorkers: 4, CheckInterval: time.Hour})
	p.resizeAutoScale(stale, 4)
	if got := p.Size(); got != 2 {
		t.Fatalf("old check resized newly enabled pool to %d", got)
	}
	p.resizeAutoScale(p.autoScale, 4)
	if got := p.Size(); got != 4 {
		t.Fatalf("current check size = %d, want 4", got)
	}
	p.DisableAutoScale()
}

func TestPoolAutoScaleRepeatedEnableIsIdempotent(t *testing.T) {
	p := NewPool[int](3)
	defer p.Close()
	p.EnableAutoScale(&core.AutoScaleConfig{MinWorkers: 2, MaxWorkers: 8, CheckInterval: time.Hour})
	p.EnableAutoScale(&core.AutoScaleConfig{MinWorkers: 5, MaxWorkers: 8, CheckInterval: time.Hour})
	p.DisableAutoScale()
	if got := p.Size(); got != 2 {
		t.Fatalf("size after disabling repeated enable = %d, want 2", got)
	}
}

func TestPoolAutoScaleConcurrentResize(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()
	cfg := &core.AutoScaleConfig{MinWorkers: 2, MaxWorkers: 4, CheckInterval: time.Hour, ScaleDownChecks: 1}
	p.EnableAutoScale(cfg)
	cfg = p.autoScale
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			p.Resize(5)
			p.Resize(1)
		}
	}()
	go func() {
		defer wg.Done()
		up, down := 0, 0
		for i := 0; i < 100; i++ {
			p.performAutoScaleCheck(cfg, &up, &down)
		}
	}()
	wg.Wait()
	p.DisableAutoScale()
}

func TestPool_AutoScale(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	cfg := &core.AutoScaleConfig{
		MinWorkers:    2,
		MaxWorkers:    8,
		CheckInterval: 100 * time.Millisecond,
	}
	p.EnableAutoScale(cfg)
	if !p.IsAutoScaleEnabled() {
		t.Error("should be enabled")
	}
	p.DisableAutoScale()
	if p.IsAutoScaleEnabled() {
		t.Error("should be disabled")
	}
}

// ==================== WithTraceID ====================

func TestPool_WithTraceID(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := p.Ctx()
	p, ctx = p.WithTraceID(ctx)

	traceID := core.GetTraceID(ctx)
	if traceID == "" {
		t.Error("trace id should not be empty")
	}
}

// ==================== WithContext ====================

func TestPool_WithContext(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	pCtx := p.Ctx()
	p, ctx := p.WithContext(pCtx)
	_ = ctx

	p.Submit(p.Ctx(), func(ctx context.Context) (int, error) { return 1, nil })
	p.Wait()
}

// ==================== MergeFailFastCancel ====================

func TestMergeFailFastCancel_Pool(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	pCtx, pCancel := context.WithCancel(context.Background())
	p.MergeFailFastCancel(pCancel)
	_ = pCtx
}

// ==================== WithMaxPending ====================

func TestWithMaxPending(t *testing.T) {
	p := NewPool[int](2)
	p.WithMaxPending(5)
	defer p.Close()
	ctx := context.Background()

	var started atomic.Int64
	for i := 0; i < 20; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			started.Add(1)
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
	}
	p.Wait()
	_ = started.Load()
}

// ==================== Production Concurrent Tests ====================

func TestProduction_PoolMixed(t *testing.T) {
	p := NewPool[int](runtime.NumCPU() * 2)
	defer p.Close()
	ctx := context.Background()

	var okCount, errorCount, panicCount atomic.Int64
	n := 1000

	for i := 0; i < n; i++ {
		mod := i % 5
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
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
			t.Errorf("Submit(%d) error: %v", i, err)
		}
	}

	results := p.Wait()
	if len(results) != n {
		t.Errorf("len = %d, want %d", len(results), n)
	}
	if p.OkCount() != okCount.Load() {
		t.Errorf("OkCount = %d, want %d", p.OkCount(), okCount.Load())
	}
	if p.FailCount() != errorCount.Load()+panicCount.Load() {
		t.Errorf("FailCount = %d, want %d", p.FailCount(), errorCount.Load()+panicCount.Load())
	}
}

func TestProduction_PoolHighConcurrency(t *testing.T) {
	p := NewPool[int](runtime.NumCPU() * 4)
	defer p.Close()
	ctx := context.Background()

	n := 5000
	var counter atomic.Int64
	for i := 0; i < n; i++ {
		if err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return i, nil
		}); err != nil {
			t.Fatalf("Submit(%d): %v", i, err)
		}
	}
	results := p.Wait()
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
	if len(results) != n {
		t.Fatalf("len = %d, want %d", len(results), n)
	}
	seen := make([]bool, n)
	for _, result := range results {
		if result.Err != nil || result.Value < 0 || result.Value >= n || seen[result.Value] {
			t.Fatalf("invalid or duplicate result: %+v", result)
		}
		seen[result.Value] = true
	}
}

func TestProduction_PoolResizeRace(t *testing.T) {
	p := NewPool[int](16)
	defer p.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			p.Resize(2 + i%8)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	accepted := make(map[int]bool)
	for i := 0; i < 500; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
		if err == nil {
			accepted[i] = true
		} else if !errors.Is(err, core.ErrPoolWaiting) {
			t.Fatalf("Submit(%d): unexpected error %v", i, err)
		}
	}
	results := p.Wait()
	wg.Wait()
	if len(results) != len(accepted) {
		t.Fatalf("got %d results for %d accepted tasks", len(results), len(accepted))
	}
	for _, result := range results {
		if result.Err != nil || !accepted[result.Value] {
			t.Fatalf("unexpected or duplicate result: %+v", result)
		}
		delete(accepted, result.Value)
	}
}

func TestProduction_PoolFailFastRace(t *testing.T) {
	p := NewPool[int](runtime.NumCPU() * 2)
	defer p.Close()
	pCtx := p.Ctx()
	p, ctx := p.WithFailFast(pCtx)

	n := 500
	var started atomic.Int64
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			started.Add(1)
			return 0, errors.New("fail")
		})
	}
	p.Wait()
	t.Logf("started = %d/%d", started.Load(), n)
}

func TestProduction_PoolSubmitAtRace(t *testing.T) {
	p := NewPool[int](runtime.NumCPU() * 2)
	defer p.Close()
	ctx := context.Background()

	n := 500
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	results := p.Wait()
	if len(results) != n {
		t.Errorf("len = %d, want %d", len(results), n)
	}
}

func TestProduction_PoolRingBufferRace(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(1000, core.OverflowBlock)
	defer p.Close()
	ctx := context.Background()

	n := 2000
	var counter atomic.Int64
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
	}
	p.Wait()
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
}

func TestProduction_PoolMaxPendingRace(t *testing.T) {
	p := NewPool[int](4)
	p.WithMaxPending(200)
	defer p.Close()
	ctx := context.Background()

	n := 1000
	var counter atomic.Int64
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 1, nil
		})
	}
	p.Wait()
	if counter.Load() != int64(n) {
		t.Errorf("counter = %d, want %d", counter.Load(), n)
	}
}

func TestProduction_PoolMultiWorkerTypes(t *testing.T) {
	// Submit, SubmitAt, TrySubmit mixed
	p := NewPool[int](8)
	defer p.Close()
	ctx := context.Background()

	var okCount, tryOk atomic.Int64
	n := 500

	for i := 0; i < n; i++ {
		switch i % 3 {
		case 0:
			p.Submit(ctx, func(ctx context.Context) (int, error) {
				okCount.Add(1)
				return 1, nil
			})
		case 1:
			p.SubmitAt(i, ctx, func(ctx context.Context) (int, error) {
				okCount.Add(1)
				return 1, nil
			})
		case 2:
			err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
				okCount.Add(1)
				tryOk.Add(1)
				return 1, nil
			})
			if err != nil {
				t.Logf("TrySubmit failed: %v", err)
			}
		}
	}
	p.Wait()
	t.Logf("okCount=%d tryOk=%d", okCount.Load(), tryOk.Load())
}

// ==================== helper.go: Submit ====================

func TestHelper_Submit(t *testing.T) {
	ctx := context.Background()

	p, idx, err := Submit(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("Submit err = %v, want nil", err)
	}
	if idx < 0 {
		t.Fatalf("Submit idx = %d, want >= 0", idx)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != 42 {
		t.Fatalf("Submit result: %+v", results)
	}
}

func TestHelper_Submit_Error(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("submit_task_error")

	p, idx, err := Submit(ctx, func(ctx context.Context) (int, error) {
		return 0, sentinel
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("Submit err = %v, want nil", err)
	}
	_ = idx
	results := p.Wait()
	if len(results) != 1 {
		t.Fatalf("len = %d, want 1", len(results))
	}
	if !errors.Is(results[0].Err, sentinel) {
		t.Fatalf("result.Err = %v, want %v", results[0].Err, sentinel)
	}
}

func TestHelper_Submit_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := Submit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Skip("cancelled context submission behavior is implementation-defined")
	}
}

// ==================== helper.go: SubmitN ====================

func TestHelper_SubmitN_Zero(t *testing.T) {
	ctx := context.Background()
	p, results, err := SubmitN(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	}, 0)
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitN err = %v, want nil", err)
	}
	if len(results) != 0 {
		t.Fatalf("SubmitN(0) len = %d, want 0", len(results))
	}
	p.Wait()
}

func TestHelper_SubmitN_One(t *testing.T) {
	ctx := context.Background()
	p, results, err := SubmitN(ctx, func(ctx context.Context) (int, error) {
		return 100, nil
	}, 1)
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitN err = %v, want nil", err)
	}
	if len(results) != 1 {
		t.Fatalf("SubmitN(1) len = %d, want 1", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("SubmitN(1)[0].Err = %v, want nil", results[0].Err)
	}
	if results[0].Index < 0 {
		t.Fatalf("SubmitN(1)[0].Index = %d, want >= 0", results[0].Index)
	}
	out := p.Wait()
	if len(out) != 1 || out[0].Value != 100 {
		t.Fatalf("SubmitN(1) results: %+v", out)
	}
}

func TestHelper_SubmitN_Five(t *testing.T) {
	ctx := context.Background()
	p, results, err := SubmitN(ctx, func(ctx context.Context) (int, error) {
		return 7, nil
	}, 5)
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitN err = %v, want nil", err)
	}
	if len(results) != 5 {
		t.Fatalf("SubmitN(5) len = %d, want 5", len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("SubmitN(5)[%d].Err = %v", i, r.Err)
		}
	}
	out := p.Wait()
	if len(out) != 5 {
		t.Fatalf("SubmitN(5) output len = %d, want 5", len(out))
	}
	for _, o := range out {
		if o.Value != 7 {
			t.Fatalf("SubmitN(5) value = %d, want 7", o.Value)
		}
	}
}

func TestHelper_SubmitN_Hundred(t *testing.T) {
	ctx := context.Background()
	n := 100
	p, results, err := SubmitN(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	}, n)
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitN err = %v, want nil", err)
	}
	if len(results) != n {
		t.Fatalf("SubmitN(%d) len = %d", n, len(results))
	}
	out := p.Wait()
	if len(out) != n {
		t.Fatalf("SubmitN(%d) output len = %d", n, len(out))
	}
}

// ==================== helper.go: SubmitSafeN ====================

func TestHelper_SubmitSafeN_Zero(t *testing.T) {
	ctx := context.Background()
	p, results := SubmitSafeN(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	}, 0)
	defer p.Close()
	if len(results) != 0 {
		t.Fatalf("SubmitSafeN(0) len = %d, want 0", len(results))
	}
}

func TestHelper_SubmitSafeN_One(t *testing.T) {
	ctx := context.Background()
	p, results := SubmitSafeN(ctx, func(ctx context.Context) (int, error) {
		return 200, nil
	}, 1)
	defer p.Close()
	if len(results) != 1 {
		t.Fatalf("SubmitSafeN(1) len = %d, want 1", len(results))
	}
	out := p.Wait()
	if len(out) != 1 || out[0].Value != 200 {
		t.Fatalf("SubmitSafeN(1) results: %+v", out)
	}
}

func TestHelper_SubmitSafeN_Five(t *testing.T) {
	ctx := context.Background()
	p, results := SubmitSafeN(ctx, func(ctx context.Context) (int, error) {
		return 3, nil
	}, 5)
	defer p.Close()
	if len(results) != 5 {
		t.Fatalf("SubmitSafeN(5) len = %d, want 5", len(results))
	}
	out := p.Wait()
	if len(out) != 5 {
		t.Fatalf("SubmitSafeN(5) output len = %d", len(out))
	}
}

func TestHelper_SubmitSafeN_Hundred(t *testing.T) {
	ctx := context.Background()
	n := 100
	p, results := SubmitSafeN(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	}, n)
	defer p.Close()
	if len(results) != n {
		t.Fatalf("SubmitSafeN(%d) len = %d", n, len(results))
	}
	out := p.Wait()
	if len(out) != n {
		t.Fatalf("SubmitSafeN(%d) output len = %d", n, len(out))
	}
}

// ==================== helper.go: SubmitBatch ====================

func TestHelper_SubmitBatch_Empty(t *testing.T) {
	ctx := context.Background()
	items := []int{}
	p, results, err := SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 10, nil
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitBatch empty err = %v, want nil", err)
	}
	if len(results) != 0 {
		t.Fatalf("SubmitBatch empty len = %d, want 0", len(results))
	}
}

func TestHelper_SubmitBatch_One(t *testing.T) {
	ctx := context.Background()
	items := []int{5}
	p, results, err := SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 10, nil
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitBatch err = %v, want nil", err)
	}
	if len(results) != 1 {
		t.Fatalf("SubmitBatch len = %d, want 1", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("SubmitBatch[0].Err = %v", results[0].Err)
	}
	out := p.Wait()
	if len(out) != 1 || out[0].Value != 50 {
		t.Fatalf("SubmitBatch results: %+v", out)
	}
}

func TestHelper_SubmitBatch_Five(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3, 4, 5}
	p, results, err := SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * item, nil
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitBatch err = %v, want nil", err)
	}
	if len(results) != 5 {
		t.Fatalf("SubmitBatch len = %d, want 5", len(results))
	}
	out := p.Wait()
	sort.Slice(out, func(i, j int) bool { return out[i].Value < out[j].Value })
	expected := []int{1, 4, 9, 16, 25}
	for i, o := range out {
		if o.Value != expected[i] {
			t.Fatalf("SubmitBatch[%d] = %d, want %d", i, o.Value, expected[i])
		}
	}
}

func TestHelper_SubmitBatch_DifferentType(t *testing.T) {
	ctx := context.Background()
	items := []string{"a", "bb", "ccc"}
	p, results, err := SubmitBatch(ctx, items, func(ctx context.Context, item string) (int, error) {
		return len(item), nil
	})
	defer p.Close()
	if err != nil {
		t.Fatalf("SubmitBatch err = %v, want nil", err)
	}
	if len(results) != 3 {
		t.Fatalf("SubmitBatch len = %d, want 3", len(results))
	}
	out := p.Wait()
	sort.Slice(out, func(i, j int) bool { return out[i].Value < out[j].Value })
	if out[0].Value != 1 || out[1].Value != 2 || out[2].Value != 3 {
		t.Fatalf("SubmitBatch string lengths: %+v", out)
	}
}

// ==================== helper.go: MapPool ====================

func TestHelper_MapPool_Empty(t *testing.T) {
	ctx := context.Background()
	items := []int{}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 2, nil
	}, 0)
	if err != nil {
		t.Fatalf("MapPool empty err = %v, want nil", err)
	}
	if len(results) != 0 {
		t.Fatalf("MapPool empty len = %d, want 0", len(results))
	}
}

func TestHelper_MapPool_One(t *testing.T) {
	ctx := context.Background()
	items := []int{7}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 3, nil
	}, 0)
	if err != nil {
		t.Fatalf("MapPool err = %v, want nil", err)
	}
	if len(results) != 1 || results[0].Value != 21 {
		t.Fatalf("MapPool results: %+v", results)
	}
}

func TestHelper_MapPool_Multiple(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3, 4, 5}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 10, nil
	}, 4)
	if err != nil {
		t.Fatalf("MapPool err = %v, want nil", err)
	}
	if len(results) != 5 {
		t.Fatalf("MapPool len = %d, want 5", len(results))
	}
	vals := make([]int, len(results))
	for i, r := range results {
		vals[i] = r.Value
	}
	sort.Ints(vals)
	for i, v := range vals {
		if v != (i+1)*10 {
			t.Fatalf("MapPool[%d] = %d, want %d", i, v, (i+1)*10)
		}
	}
}

func TestHelper_MapPool_ConcurrencyZero(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item, nil
	}, 0)
	if err != nil {
		t.Fatalf("MapPool concurrency=0 err = %v, want nil", err)
	}
	if len(results) != 3 {
		t.Fatalf("MapPool len = %d, want 3", len(results))
	}
}

func TestHelper_MapPool_ConcurrencyExceedsLen(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item, nil
	}, 100)
	if err != nil {
		t.Fatalf("MapPool concurrency>len err = %v, want nil", err)
	}
	if len(results) != 2 {
		t.Fatalf("MapPool len = %d, want 2", len(results))
	}
}

func TestHelper_MapPool_Error(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("map_error")
	items := []int{1, 2, 3, 4, 5}
	_, results, err := MapPool(ctx, items, func(ctx context.Context, item int) (int, error) {
		if item == 3 {
			return 0, sentinel
		}
		return item * 10, nil
	}, 2)
	if err != nil {
		t.Fatalf("MapPool err = %v, want nil", err)
	}
	errFound := false
	for _, r := range results {
		if r.Err != nil {
			errFound = true
			if !errors.Is(r.Err, sentinel) {
				t.Fatalf("MapPool error = %v, want %v", r.Err, sentinel)
			}
		}
	}
	if !errFound {
		t.Fatal("MapPool expected error in results")
	}
}

// ==================== helper.go: ForEachPool ====================

func TestHelper_ForEachPool_Empty(t *testing.T) {
	ctx := context.Background()
	items := []int{}
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		return nil
	}, 4)
	if err != nil {
		t.Fatalf("ForEachPool empty err = %v, want nil", err)
	}
	if p == nil {
		t.Fatal("ForEachPool returned nil pool")
	}
}

func TestHelper_ForEachPool_One(t *testing.T) {
	ctx := context.Background()
	var called atomic.Int64
	items := []int{42}
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		called.Add(1)
		if item != 42 {
			t.Errorf("item = %d, want 42", item)
		}
		return nil
	}, 4)
	if err != nil {
		t.Fatalf("ForEachPool err = %v, want nil", err)
	}
	_ = p
	if called.Load() != 1 {
		t.Fatalf("called = %d, want 1", called.Load())
	}
}

func TestHelper_ForEachPool_Multiple(t *testing.T) {
	ctx := context.Background()
	var sum atomic.Int64
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		sum.Add(int64(item))
		return nil
	}, 4)
	if err != nil {
		t.Fatalf("ForEachPool err = %v, want nil", err)
	}
	_ = p
	if sum.Load() != 55 {
		t.Fatalf("sum = %d, want 55", sum.Load())
	}
}

func TestHelper_ForEachPool_Error(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("foreach_error")
	items := []int{1, 2, 3, 4, 5}
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		if item == 3 {
			return sentinel
		}
		return nil
	}, 2)
	_ = p
	if err == nil {
		t.Fatal("ForEachPool expected error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("ForEachPool error = %v, want %v", err, sentinel)
	}
}

func TestHelper_ForEachPool_ConcurrencyZero(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int64
	items := make([]int, 10)
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		counter.Add(1)
		return nil
	}, 0)
	if err != nil {
		t.Fatalf("ForEachPool concurrency=0 err = %v, want nil", err)
	}
	_ = p
	if counter.Load() != 10 {
		t.Fatalf("counter = %d, want 10", counter.Load())
	}
}

func TestHelper_ForEachPool_ConcurrencyExceedsLen(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int64
	items := make([]int, 5)
	p, err := ForEachPool(ctx, items, func(ctx context.Context, item int) error {
		counter.Add(1)
		return nil
	}, 100)
	if err != nil {
		t.Fatalf("ForEachPool concurrency>len err = %v, want nil", err)
	}
	_ = p
	if counter.Load() != 5 {
		t.Fatalf("counter = %d, want 5", counter.Load())
	}
}

// ==================== Resize Boundary Tests ====================

func TestResize_Zero(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	p.Resize(0)
	if p.Size() != core.IO() {
		t.Fatalf("Size after Resize(0) = %d, want %d", p.Size(), core.IO())
	}
}

func TestResize_Negative(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	p.Resize(-1)
	if p.Size() != core.IO() {
		t.Fatalf("Size after Resize(-1) = %d, want %d", p.Size(), core.IO())
	}
	if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		t.Fatal(err)
	}
	results, ok := p.WaitTimeout(time.Second * 5)
	if !ok || len(results) != 1 || results[0].Err != nil || results[0].Value != 42 {
		t.Fatalf("result after Resize(-1): %#v, complete=%v", results, ok)
	}
}

func TestResize_SameSize(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	changed := p.Resize(4)
	if changed != 0 {
		t.Fatalf("Resize(4) changed = %d, want 0", changed)
	}
	if p.Size() != 4 {
		t.Fatalf("Size after Resize(4) = %d, want 4", p.Size())
	}
}

func TestResize_VeryLarge(t *testing.T) {
	p := NewPool[int](2)
	defer p.Close()

	added := p.Resize(100)
	if added != 98 {
		t.Fatalf("Resize(100) added = %d, want 98", added)
	}
	if p.Size() != 100 {
		t.Fatalf("Size after Resize(100) = %d, want 100", p.Size())
	}
}

func TestResize_RoundTrip(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	if s := p.Size(); s != 4 {
		t.Fatalf("initial size = %d, want 4", s)
	}
	p.Resize(8)
	if s := p.Size(); s != 8 {
		t.Fatalf("after resize up: size = %d, want 8", s)
	}
	p.Resize(2)
	if s := p.Size(); s != 2 {
		t.Fatalf("after resize down: size = %d, want 2", s)
	}
	p.Resize(6)
	if s := p.Size(); s != 6 {
		t.Fatalf("after resize up again: size = %d, want 6", s)
	}
	p.Resize(4)
	if s := p.Size(); s != 4 {
		t.Fatalf("after resize down to original: size = %d, want 4", s)
	}
}

// ==================== CloseAndWaitTimeout Boundary Tests ====================

func TestCloseAndWaitTimeout_Zero(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 1, nil
	})
	ok, doneCh := p.CloseAndWaitTimeout(0)
	if ok {
		t.Log("immediate timeout returned ok=true (may race)")
	}

	select {
	case <-doneCh:
		t.Log("doneCh closed immediately with zero timeout")
	case <-time.After(200 * time.Millisecond):
		t.Log("doneCh not closed within extra wait")
	}
}

func TestCloseAndWaitTimeout_Negative(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 1, nil
	})
	ok, doneCh := p.CloseAndWaitTimeout(-1)
	if ok {
		t.Log("negative timeout returned ok=true (may race)")
	}

	select {
	case <-doneCh:
		t.Log("doneCh closed with negative timeout")
	case <-time.After(200 * time.Millisecond):
		t.Log("doneCh not closed within extra wait")
	}
}

// ==================== WithMaxPending Boundary Tests ====================

func TestWithMaxPending_Zero(t *testing.T) {
	p := NewPool[int](2)
	p.WithMaxPending(0)
	defer p.Close()
	ctx := context.Background()

	var done atomic.Int64
	var submitted atomic.Int64

	for i := 0; i < 10; i++ {
		err := p.Submit(ctx, func(ctx context.Context) (int, error) {
			done.Add(1)
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
		if err == nil {
			submitted.Add(1)
		}
	}
	p.Wait()
	t.Logf("WithMaxPending(0): submitted=%d done=%d", submitted.Load(), done.Load())
}

func TestWithMaxPending_Negative(t *testing.T) {
	p := NewPool[int](2)
	p.WithMaxPending(-1)
	defer p.Close()
	ctx := context.Background()

	var done atomic.Int64
	for i := 0; i < 10; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			done.Add(1)
			time.Sleep(10 * time.Millisecond)
			return 1, nil
		})
	}
	p.Wait()
	t.Logf("WithMaxPending(-1): done=%d", done.Load())
}

func TestWithMaxPending_BigValue(t *testing.T) {
	p := NewPool[int](2)
	p.WithMaxPending(1_000_000)
	defer p.Close()
	ctx := context.Background()

	var done atomic.Int64
	n := 200
	for i := 0; i < n; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			done.Add(1)
			time.Sleep(5 * time.Millisecond)
			return 1, nil
		})
	}
	p.Wait()
	if done.Load() != int64(n) {
		t.Fatalf("done = %d, want %d", done.Load(), n)
	}
}

// ==================== Close Boundary Test ====================

func TestClose_CloseTwice(t *testing.T) {
	p := NewPool[int](4)
	p.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Logf("double close caused panic: %v", r)
		}
	}()
	p.Close()
}

func TestClose_SubmitAfterClose(t *testing.T) {
	p := NewPool[int](4)
	p.Close()
	err := p.Submit(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Error("Submit after Close should return an error")
	}
	t.Logf("Submit after Close error: %v", err)
}

func TestClose_TrySubmitAfterClose(t *testing.T) {
	p := NewPool[int](4)
	p.Close()
	err := p.TrySubmit(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Error("TrySubmit after Close should return an error")
	}
	t.Logf("TrySubmit after Close error: %v", err)
}

func TestClose_SubmitAtAfterClose(t *testing.T) {
	p := NewPool[int](4)
	p.Close()
	err := p.SubmitAt(0, context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Error("SubmitAt after Close should return an error")
	}
	t.Logf("SubmitAt after Close error: %v", err)
}

// ==================== Flush Tests ====================

func TestFlush_EmptyPool(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	flushed := p.Flush(100)
	if len(flushed) != 0 {
		t.Fatalf("Flush empty pool len = %d, want 0", len(flushed))
	}
}

func TestFlush_AfterTasks(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	n := 20
	for i := 0; i < n; i++ {
		v := i
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return v, nil
		})
	}

	flushed := p.Flush(100)
	t.Logf("Flushed %d results", len(flushed))
}

// ==================== Stats Boundary Tests ====================

func TestStats_NewPool(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	stats := p.Stats()
	if stats.TotalTask != 0 {
		t.Fatalf("new pool TotalTask = %d, want 0", stats.TotalTask)
	}
	if stats.OkTask != 0 {
		t.Fatalf("new pool OkTask = %d, want 0", stats.OkTask)
	}
	if stats.FailTask != 0 {
		t.Fatalf("new pool FailTask = %d, want 0", stats.FailTask)
	}
}

func TestStats_AfterTasks(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	n := 10
	for i := 0; i < n; i++ {
		var fn func(ctx context.Context) (int, error)
		if i%2 == 0 {
			fn = func(ctx context.Context) (int, error) {
				return i, nil
			}
		} else {
			fn = func(ctx context.Context) (int, error) {
				return 0, errors.New("odd error")
			}
		}
		p.Submit(ctx, fn)
	}
	p.Wait()

	stats := p.Stats()
	if stats.TotalTask != int64(n) {
		t.Fatalf("TotalTask = %d, want %d", stats.TotalTask, n)
	}
	if stats.OkTask != 5 {
		t.Fatalf("OkTask = %d, want 5", stats.OkTask)
	}
	if stats.FailTask != 5 {
		t.Fatalf("FailTask = %d, want 5", stats.FailTask)
	}
}

// ==================== ResizeAndWaitTimeout Boundaries ====================

func TestResizeAndWaitTimeout_NotifyCh(t *testing.T) {
	p := NewPool[int](8)
	defer p.Close()

	notify := make(chan struct{})
	go func() {
		p.ResizeAndWaitTimeout(2, 5*time.Second)
		close(notify)
	}()

	select {
	case <-notify:
		if p.Size() != 2 {
			t.Errorf("Size = %d, want 2", p.Size())
		}
	case <-time.After(6 * time.Second):
		t.Fatal("ResizeAndWaitTimeout did not complete in time")
	}
}

// ==================== Concurrent Resize Tests ====================

func TestConcurrent_Resize(t *testing.T) {
	p := NewPool[int](16)
	defer p.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			sizes := []int{4, 8, 2, 6, 10, 4}
			for _, sz := range sizes {
				p.Resize(sz)
				time.Sleep(time.Duration(gid+1) * time.Millisecond)
			}
		}(g)
	}

	for i := 0; i < 100; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(5 * time.Millisecond)
			return 1, nil
		})
	}

	wg.Wait()
	p.Wait()
	t.Logf("concurrent resize completed, final size=%d", p.Size())
}

// ==================== Flush Boundary Tests ====================

func TestFlush_Boundary_Zero(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(100, core.OverflowBlock)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	p.Wait()

	flushed := p.Flush(0)
	if len(flushed) != 0 {
		t.Logf("Flush(0) returned %d results (ring buffer may retain items)", len(flushed))
	}
}

func TestFlush_Boundary_Negative(t *testing.T) {
	p := NewPool[int](4)
	p.WithRingBuffer(100, core.OverflowBlock)
	defer p.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	p.Wait()

	flushed := p.Flush(-1)
	t.Logf("Flush(-1) returned %d results (negative treated as flush-all)", len(flushed))
}

// ==================== SubmitAt Cancelled Context ====================

func TestSubmitAt_CancelledContext(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.SubmitAt(0, ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err == nil {
		t.Error("SubmitAt with cancelled context should return an error")
	}
	t.Logf("SubmitAt cancelled ctx error: %v", err)
}

// ==================== TrySubmit Cancelled Context ====================

func TestTrySubmit_CancelledContext(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	t.Logf("TrySubmit with cancelled ctx: err=%v (TrySubmit only checks pool state, not context)", err)
}

// ==================== WithTimeout Negative ====================

func TestWithTimeout_Negative(t *testing.T) {
	p := NewPool[int](4)
	p.WithTimeout(-1)
	defer p.Close()

	if p.timeout != -1 {
		t.Fatalf("WithTimeout(-1) timeout = %v, want -1", p.timeout)
	}

	ctx := context.Background()
	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 1, nil
	})
	results := p.Wait()
	t.Logf("results with negative timeout: %d", len(results))
}

func TestWithSubmitTimeout_Negative(t *testing.T) {
	p := NewPool[int](4)
	p.WithSubmitTimeout(-1)
	defer p.Close()

	if p.submitTimeout != -1 {
		t.Fatalf("WithSubmitTimeout(-1) = %v, want -1", p.submitTimeout)
	}

	ctx := context.Background()
	err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 1, nil
	})
	t.Logf("submit with negative submitTimeout: err=%v", err)
	p.Close()
}

// ==================== CloseByIdle Boundary ====================

func TestCloseByIdleTimeoutStillWaitsForWorker(t *testing.T) {
	p := NewPool[int](1).WithTimeout(0)
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseTask := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() { releaseTask(); p.Close() })
	if err := p.Submit(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	closed := make(chan struct{})
	go func() { p.CloseByIdle(0); close(closed) }()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not start")
	}
	select {
	case <-closed:
		t.Fatal("CloseByIdle returned while the worker was running")
	default:
	}
	releaseTask()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("CloseByIdle did not return after the worker exited")
	}
}

func TestCloseByIdle_Zero(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		p.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return i, nil
		})
	}
	p.Wait()

	p.CloseByIdle(0)
	t.Log("CloseByIdle(0) completed")
}

func TestCloseByIdle_Negative(t *testing.T) {
	p := NewPool[int](4)
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(10 * time.Millisecond)
		return 1, nil
	})
	p.Wait()

	p.CloseByIdle(-1)
	t.Log("CloseByIdle(-1) completed")
}

// ==================== Config Boundary Tests ====================

func TestConfig_WithSize_Zero(t *testing.T) {
	cfg := Config{}.WithSize(0)
	if cfg.Size != 0 {
		t.Fatalf("WithSize(0) = %d, want 0", cfg.Size)
	}
}

func TestConfig_WithSize_Negative(t *testing.T) {
	cfg := Config{}.WithSize(-1)
	if cfg.Size != -1 {
		t.Fatalf("WithSize(-1) = %d, want -1", cfg.Size)
	}
}

func TestConfig_WithMaxResults_Zero(t *testing.T) {
	cfg := Config{}.WithMaxResults(0)
	if cfg.MaxResults != 0 {
		t.Fatalf("WithMaxResults(0) = %d, want 0", cfg.MaxResults)
	}
}

func TestConfig_WithRingBuf_Zero(t *testing.T) {
	cfg := Config{}.WithRingBuf(0)
	if cfg.RingBufCap != 0 {
		t.Fatalf("WithRingBuf(0) = %d, want 0", cfg.RingBufCap)
	}
}

func TestConfig_WithStreaming_Zero(t *testing.T) {
	cfg := Config{}.WithStreaming(0)
	if cfg.Streaming != 0 {
		t.Fatalf("WithStreaming(0) = %d, want 0", cfg.Streaming)
	}
}

// ==================== WaitTimeout Boundary ====================

func TestWaitTimeout_Negative(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 1, nil
	})

	results, ok := p.WaitTimeout(-1)
	t.Logf("WaitTimeout(-1): ok=%v, results=%d", ok, len(results))
}

func TestWaitTimeout_Zero(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()
	ctx := context.Background()

	p.Submit(ctx, func(ctx context.Context) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 1, nil
	})

	results, ok := p.WaitTimeout(0)
	if ok {
		t.Log("WaitTimeout(0) returned immediately with complete results (race)")
	}
	_ = results
}

// ==================== ResizeAndWaitTimeout Boundary ====================

func TestResizeAndWaitTimeout_Zero(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	p.ResizeAndWaitTimeout(0, 2500*time.Millisecond)
	t.Logf("ResizeAndWaitTimeout(0): size=%d", p.Size())
}

// ==================== MultiPool WaitTimeout / WaitContext ====================

func TestMultiPool_WaitTimeout(t *testing.T) {
	mp := NewPool[int](4).Shard(2)
	defer mp.Close()
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		_ = mp.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return i, nil
		})
	}
	results, ok := mp.WaitTimeout(5 * time.Second)
	if !ok || len(results) != 100 {
		t.Fatalf("MultiPool.WaitTimeout failed: ok=%v, len=%d", ok, len(results))
	}
}

func TestMultiPool_WaitContext(t *testing.T) {
	mp := NewPool[int](4).Shard(2)
	defer mp.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		_ = mp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	results, ok := mp.WaitContext(ctx)
	if !ok || len(results) != 100 {
		t.Fatalf("MultiPool.WaitContext failed: ok=%v, len=%d", ok, len(results))
	}
}

func TestMultiPool_WaitContext_Cancel(t *testing.T) {
	mp := NewPool[int](4).Shard(2)
	defer mp.Close()
	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < 100; i++ {
		_ = mp.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return i, nil
		})
	}
	cancel()
	_, ok := mp.WaitContext(ctx)
	if ok {
		t.Fatal("expected cancelled (ok=false)")
	}
}

func TestMultiPool_WaitTimeout_ActuallyTimesOut(t *testing.T) {
	mp := NewPool[int](4).Shard(2)
	defer mp.Close()
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		_ = mp.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return i, nil
		})
	}
	_, ok := mp.WaitTimeout(50 * time.Millisecond)
	if ok {
		t.Fatal("expected timeout (ok=false)")
	}
}

func TestMultiPool_WaitContext_WithDeadline(t *testing.T) {
	mp := NewPool[int](4).Shard(2)
	defer mp.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	for i := 0; i < 100; i++ {
		_ = mp.Submit(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return i, nil
		})
	}
	_, ok := mp.WaitContext(ctx)
	if ok {
		t.Fatal("expected deadline exceeded (ok=false)")
	}
}

func TestMultiPool_WaitTimeout_Negative(t *testing.T) {
	mp := NewPool[int](4).Shard(2)
	defer mp.Close()
	_, ok := mp.WaitTimeout(-1)
	if ok {
		t.Fatal("expected negative timeout → ok=false")
	}
}

func TestMultiPool_WaitTimeout_Zero(t *testing.T) {
	mp := NewPool[int](4).Shard(2)
	defer mp.Close()
	for i := 0; i < 10; i++ {
		_ = mp.Submit(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return i, nil
		})
	}
	_, ok := mp.WaitTimeout(0)
	if ok {
		t.Fatal("expected zero timeout → ok=false")
	}
}

func TestMultiPool_WaitContext_AlreadyCancelled(t *testing.T) {
	mp := NewPool[int](4).Shard(2)
	defer mp.Close()
	for i := 0; i < 10; i++ {
		_ = mp.Submit(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(500 * time.Millisecond)
			return i, nil
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok := mp.WaitContext(ctx)
	if ok {
		t.Fatal("expected already-cancelled context → ok=false")
	}
}

// ==================== MultiPool 边界：空池 ====================

func TestMultiPool_WaitTimeout_Empty(t *testing.T) {
	mp := NewPool[int](4).Shard(2)
	defer mp.Close()
	results, ok := mp.WaitTimeout(time.Second)
	if !ok {
		t.Fatal("empty MultiPool WaitTimeout should return ok=true")
	}
	if len(results) != 0 {
		t.Fatalf("empty MultiPool: len=%d, want 0", len(results))
	}
}

func TestMultiPool_WaitContext_Empty(t *testing.T) {
	mp := NewPool[int](4).Shard(2)
	defer mp.Close()
	results, ok := mp.WaitContext(context.Background())
	if !ok {
		t.Fatal("empty MultiPool WaitContext should return ok=true")
	}
	if len(results) != 0 {
		t.Fatalf("empty MultiPool: len=%d, want 0", len(results))
	}
}
