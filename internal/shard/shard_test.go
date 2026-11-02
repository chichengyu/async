package shard

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/group"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/test"
)

var errShardTest = core.ErrTimeout

func TestShardedPoolResetPreservesPerShardRingStrategy(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1})
	defer sp.Close()
	for i := 0; i < sp.ShardCount(); i++ {
		sp.GetShard(i).WithMaxResults(0).WithOverflow(core.OverflowBlock).WithRingBuffer(1, core.OverflowDrop)
	}
	sp.Wait()
	if err := sp.Reset(); err != nil {
		t.Fatal(err)
	}
	for shard := 0; shard < sp.ShardCount(); shard++ {
		for index := 0; index < 2; index++ {
			value := shard*10 + index
			if err := sp.SubmitAt(shard, index, context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
				t.Fatal(err)
			}
		}
	}
	sp.Wait()
	for shard := 0; shard < sp.ShardCount(); shard++ {
		results := sp.GetShard(shard).Flush(0)
		if len(results) != 1 || results[0].Value != shard*10+1 {
			t.Fatalf("shard %d flushed=%v, want newest result", shard, results)
		}
	}
}

func TestNewAutoScaleShardedPoolLifecycle(t *testing.T) {
	sp := NewAutoScaleShardedPool[int](3, 1, &core.AutoScaleConfig{
		MinWorkers: 1, MaxWorkers: 2, CheckInterval: time.Hour,
	})
	t.Cleanup(sp.Close)
	for i := 0; i < sp.ShardCount(); i++ {
		p := sp.GetShard(i)
		if !p.IsAutoScaleEnabled() || p.Size() != 1 {
			t.Fatalf("shard %d: enabled=%v size=%d", i, p.IsAutoScaleEnabled(), p.Size())
		}
		value := i
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
			t.Fatal(err)
		}
	}
	results := sp.Wait()
	if len(results) != 3 {
		t.Fatalf("results=%v, want 3", results)
	}
	seen := make(map[int]bool)
	for _, result := range results {
		if result.Err != nil || !result.Occupied || result.Value < 0 || result.Value >= 3 || seen[result.Value] {
			t.Fatalf("unexpected result: %+v", result)
		}
		seen[result.Value] = true
	}
	sp.Close()
	for i := 0; i < sp.ShardCount(); i++ {
		p := sp.GetShard(i)
		if !p.IsAutoScaleEnabled() {
			t.Fatalf("shard %d lost auto scaling configuration", i)
		}
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 0, nil }); !errors.Is(err, core.ErrPoolClosed) {
			t.Fatalf("shard %d Submit after close: %v", i, err)
		}
	}
	if err := sp.Reset(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < sp.ShardCount(); i++ {
		p := sp.GetShard(i)
		if !p.IsAutoScaleEnabled() {
			t.Fatalf("shard %d did not retain auto scaling after reset", i)
		}
		if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if results := sp.Wait(); len(results) != 3 {
		t.Fatalf("results after reset=%v, want 3", results)
	}
}

func TestWithShardedPoolClosesOnErrorAndPanic(t *testing.T) {
	wantErr := errors.New("callback error")
	for _, panicMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%t", panicMode), func(t *testing.T) {
			var sp *ShardedPool[int]
			var gotErr error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				gotErr = WithShardedPool(2, 1, func(p *ShardedPool[int]) error {
					sp = p
					if panicMode {
						panic(wantErr)
					}
					return wantErr
				})
			}()
			if panicMode && recovered != wantErr || !panicMode && (recovered != nil || !errors.Is(gotErr, wantErr)) {
				t.Fatalf("error=%v panic=%v", gotErr, recovered)
			}
			if sp == nil {
				t.Fatal("callback did not receive a pool")
			}
			for shard := 0; shard < sp.ShardCount(); shard++ {
				err := sp.GetShard(shard).Submit(context.Background(), func(context.Context) (int, error) { return 1, nil })
				if !errors.Is(err, core.ErrPoolClosed) {
					t.Fatalf("shard %d Submit after return: %v", shard, err)
				}
			}
		})
	}
}

func TestShardPoolBuilderMaxResultsAndDefault(t *testing.T) {
	for _, unlimited := range []bool{false, true} {
		t.Run(fmt.Sprintf("default=%t", unlimited), func(t *testing.T) {
			builder := NewPoolBuilder[int]().Shards(2).Worker(1).MaxResults(1)
			if unlimited {
				builder.DefaultMaxResults()
			}
			err := builder.Run(func(ctx context.Context, sp *ShardedPool[int]) error {
				for shard := 0; shard < 2; shard++ {
					for index := 0; index < 2; index++ {
						value := shard*10 + index
						if err := sp.SubmitAt(shard, index, ctx, func(context.Context) (int, error) { return value, nil }); err != nil {
							return err
						}
					}
				}
				results := sp.Wait()
				want := 2
				if unlimited {
					want = 4
				}
				if len(results) != want {
					return fmt.Errorf("results=%v, want %d", results, want)
				}
				seen := make(map[int]bool)
				for _, result := range results {
					if result.Err != nil || !result.Occupied || seen[result.Value] {
						return fmt.Errorf("unexpected result: %+v", result)
					}
					seen[result.Value] = true
				}
				for _, value := range []int{0, 10} {
					if !seen[value] {
						return fmt.Errorf("missing result %d: %v", value, seen)
					}
				}
				if unlimited && (!seen[1] || !seen[11]) {
					return fmt.Errorf("missing unlimited results: %v", seen)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestShardedPoolResizePerShardBeforeSubmit(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 3, SizePerShard: 2})
	defer sp.Close()
	for _, size := range []int{4, 1} {
		if got := sp.ResizePerShard(size); got != sp {
			t.Fatal("ResizePerShard did not return the pool")
		}
		for i := 0; i < sp.ShardCount(); i++ {
			if got := sp.GetShard(i).Size(); got != size {
				t.Fatalf("shard %d size=%d, want %d", i, got, size)
			}
		}
	}
	for i := 0; i < 9; i++ {
		value := i
		if err := sp.Submit(context.Background(), func(context.Context) (int, error) { return value, nil }); err != nil {
			t.Fatal(err)
		}
	}
	results := sp.Wait()
	if len(results) != 9 {
		t.Fatalf("results=%d, want 9", len(results))
	}
	seen := make(map[int]bool)
	for _, result := range results {
		if result.Err != nil || !result.Occupied || result.Value < 0 || result.Value >= 9 || seen[result.Value] {
			t.Fatalf("unexpected result: %+v", result)
		}
		seen[result.Value] = true
	}
}

func TestShardedGroupStreamingAndActiveCounts(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 1}).WithStreaming(2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); sg.Close() })
	stream := sg.StreamResults()
	started := make(chan int, 2)
	for i := 0; i < 2; i++ {
		value := i
		if err := sg.Go(context.Background(), func(context.Context) (int, error) {
			started <- value
			<-release
			return value, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("tasks did not start on both shards")
		}
	}
	if active, busy := sg.TotalActive(), sg.TotalBusy(); active != 2 || busy != 2 {
		t.Fatalf("active=%d busy=%d, want 2 each", active, busy)
	}
	unblock()
	results := sg.Wait()
	if len(results) != 2 {
		t.Fatalf("Wait returned %d results, want 2", len(results))
	}
	seen := make(map[int]bool)
	for result := range stream {
		if result.Err != nil || result.Value < 0 || result.Value > 1 || seen[result.Value] {
			t.Fatalf("unexpected streamed result: %+v", result)
		}
		seen[result.Value] = true
	}
	if len(seen) != 2 || sg.TotalActive() != 0 || sg.TotalBusy() != 0 {
		t.Fatalf("streamed=%v active=%d busy=%d", seen, sg.TotalActive(), sg.TotalBusy())
	}
}

func TestShardedConcurrentResultsPreserveUniqueValues(t *testing.T) {
	const tasks = 200
	wantErr := errors.New("expected task failure")
	for _, groupMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("group=%t", groupMode), func(t *testing.T) {
			sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 8})
			sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 8})
			t.Cleanup(sp.Close)
			t.Cleanup(sg.Close)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < tasks; i++ {
				wg.Add(1)
				go func(value int) {
					defer wg.Done()
					<-start
					fn := func(context.Context) (int, error) {
						if value%5 == 0 {
							return value, wantErr
						}
						return value, nil
					}
					var err error
					if groupMode {
						err = sg.Go(context.Background(), fn)
					} else {
						err = sp.Submit(context.Background(), fn)
					}
					if err != nil {
						t.Errorf("submit %d: %v", value, err)
					}
				}(i)
			}
			close(start)
			wg.Wait()
			var results []core.Result[int]
			var okCount, failCount int64
			if groupMode {
				results = sg.Wait()
				okCount, failCount = sg.TotalOkCount(), sg.TotalFailCount()
			} else {
				results = sp.Wait()
				okCount, failCount = sp.TotalOkCount(), sp.TotalFailCount()
			}
			if len(results) != tasks || okCount != tasks-tasks/5 || failCount != tasks/5 {
				t.Fatalf("results=%d ok=%d fail=%d", len(results), okCount, failCount)
			}
			seen := make([]bool, tasks)
			for _, result := range results {
				if result.Value < 0 || result.Value >= tasks || seen[result.Value] {
					t.Fatalf("unexpected or duplicate result: %+v", result)
				}
				seen[result.Value] = true
				if result.Value%5 == 0 && !errors.Is(result.Err, wantErr) || result.Value%5 != 0 && result.Err != nil {
					t.Fatalf("wrong error for value %d: %v", result.Value, result.Err)
				}
			}
		})
	}
}

func TestShardPoolConfigInheritsGlobalTimeoutAndRingBuffer(t *testing.T) {
	oldTimeout := core.GetDefaultTimeout()
	oldCapacity, oldOverflow := core.GetDefaultRingBufferCap(), core.GetDefaultOverflowStrategy()
	t.Cleanup(func() {
		core.SetDefaultTimeout(oldTimeout)
		core.SetDefaultRingBuffer(oldCapacity, oldOverflow)
	})
	core.SetDefaultTimeout(time.Millisecond)
	core.SetDefaultRingBuffer(4, core.OverflowDrop)
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1})
	defer sp.Close()
	for shard := 0; shard < 2; shard++ {
		if err := sp.SubmitAt(shard, 0, context.Background(), func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
	}
	for shard := 0; shard < 2; shard++ {
		results := sp.GetShard(shard).Wait()
		buffered := sp.GetShard(shard).Flush(0)
		if len(results) != 1 || !errors.Is(results[0].Err, context.DeadlineExceeded) ||
			len(buffered) != 1 || !errors.Is(buffered[0].Err, context.DeadlineExceeded) {
			t.Fatalf("shard %d: results=%v buffered=%v", shard, results, buffered)
		}
	}
}

func TestShardPoolConfigAppliesPerShardLimits(t *testing.T) {
	base := DefaultShardConfig[int]()
	cfg := base.WithShardMaxResults(1).WithShardStreaming(4)
	cfg.Shards, cfg.SizePerShard = 2, 2
	if base.PoolCfg.MaxResults != 0 || base.PoolCfg.Streaming != 0 {
		t.Fatal("config methods modified the original value")
	}
	sp := NewShardedPool(cfg)
	defer sp.Close()
	for shard := 0; shard < 2; shard++ {
		if sp.GetShard(shard).StreamResults() == nil {
			t.Fatalf("shard %d has no stream", shard)
		}
		for item := 0; item < 2; item++ {
			value := shard*10 + item
			if err := sp.SubmitAt(shard, item, context.Background(), func(context.Context) (int, error) {
				return value, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for shard := 0; shard < 2; shard++ {
		results := sp.GetShard(shard).Wait()
		if len(results) != 1 || results[0].Value != shard*10 || results[0].Err != nil {
			t.Fatalf("shard %d stored results=%v", shard, results)
		}
		seen := map[int]bool{}
		for result := range sp.GetShard(shard).StreamResults() {
			seen[result.Value] = true
		}
		if len(seen) != 2 || !seen[shard*10] || !seen[shard*10+1] {
			t.Fatalf("shard %d streamed values=%v", shard, seen)
		}
	}
}

func TestShardedPoolBatchUsesHashKey(t *testing.T) {
	for _, try := range []bool{false, true} {
		t.Run(fmt.Sprintf("try=%v", try), func(t *testing.T) {
			sp := NewShardedPool(ShardPoolConfig[int]{
				Shards: 4, SizePerShard: 4, Distribution: Hash,
				KeyFn: func(item int) uint64 { return math.MaxUint64 - uint64(item%4) },
			})
			defer sp.Close()
			items := []int{0, 1, 2, 3, 4, 5, 6, 7}
			fn := func(_ context.Context, item int) (int, error) { return item, nil }
			var results []SubmitBatchResult
			var err error
			if try {
				results, err = sp.TrySubmitBatch(context.Background(), items, fn)
			} else {
				results, err = sp.SubmitBatch(context.Background(), items, fn)
			}
			if err != nil {
				t.Fatal(err)
			}
			for i, result := range results {
				if result.Err != nil || result.ShardIdx != 3-items[i]%4 || result.Index != i {
					t.Fatalf("item %d: result=%+v", items[i], result)
				}
			}
			for shard := 0; shard < 4; shard++ {
				got := sp.GetShard(shard).Wait()
				if len(got) != 2 {
					t.Fatalf("shard %d: results=%v", shard, got)
				}
				for _, result := range got {
					if result.Err != nil || 3-result.Value%4 != shard {
						t.Fatalf("shard %d: result=%+v", shard, result)
					}
				}
			}
		})
	}
}

func TestShardedPoolBatchRoutingFallbacks(t *testing.T) {
	for _, tc := range []struct {
		name string
		dist Distribution
		key  func(int) uint64
		want []int
	}{
		{"round-robin-ignores-key", RoundRobin, func(int) uint64 { panic("unexpected KeyFn call") }, []int{0, 1, 2, 0}},
		{"hash-without-key", Hash, nil, []int{0, 0, 0, 0}},
	} {
		for _, try := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/try=%v", tc.name, try), func(t *testing.T) {
				sp := NewShardedPool(ShardPoolConfig[int]{Shards: 3, SizePerShard: 4, Distribution: tc.dist, KeyFn: tc.key})
				defer sp.Close()
				items := []int{0, 1, 2, 3}
				fn := func(_ context.Context, item int) (int, error) { return item, nil }
				var results []SubmitBatchResult
				var err error
				if try {
					results, err = sp.TrySubmitBatch(context.Background(), items, fn)
				} else {
					results, err = sp.SubmitBatch(context.Background(), items, fn)
				}
				if err != nil || len(results) != len(items) {
					t.Fatalf("results=%v err=%v", results, err)
				}
				for i, result := range results {
					if result.Index != i || result.ShardIdx != tc.want[i] || result.Err != nil {
						t.Fatalf("item %d: result=%+v", i, result)
					}
				}
				if results := sp.Wait(); len(results) != len(items) {
					t.Fatalf("completed results=%v", results)
				}
			})
		}
	}
}

func TestShardedGroupFailureCancelsOtherShard(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 1})
	defer sg.Close()
	_, ctx := sg.WithFailFast(context.Background())
	started, release := make(chan struct{}), make(chan struct{})
	canceled := make(chan bool, 1)
	if err := sg.GoAt(0, 0, ctx, func(ctx context.Context) (int, error) {
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
	if err := sg.GoAt(1, 0, ctx, func(context.Context) (int, error) { return 0, errShardTest }); err != nil {
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
	sg.Wait()
}

func TestShardedGroupResetAndReenableFailFast(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 1})
	defer sg.Close()
	var previous context.Context
	for round := 0; round < 2; round++ {
		if round > 0 {
			if err := sg.Reset(); err != nil {
				t.Fatal(err)
			}
		}
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, ctx := sg.WithFailFast(parent)
		if ctx.Err() != nil || (previous != nil && previous.Err() == nil) {
			t.Fatalf("round %d: new context error=%v, previous=%v", round, ctx.Err(), previous)
		}
		previous = ctx
		started := make(chan struct{})
		canceled := make(chan error, 1)
		if err := sg.GoAt(0, 0, ctx, func(ctx context.Context) (int, error) {
			close(started)
			<-ctx.Done()
			canceled <- ctx.Err()
			return 0, ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
		<-started
		if err := sg.GoAt(1, 0, ctx, func(context.Context) (int, error) { return 0, errShardTest }); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-canceled:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("round %d: cancellation error=%v", round, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("round %d: other shard was not canceled", round)
		}
		if results := sg.Wait(); len(results) != 2 {
			t.Fatalf("round %d: results=%v", round, results)
		}
	}
}

func TestShardedGroupGoBatchRoundRobinAndValues(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 3, ConcurrencyPerShard: 2})
	defer sg.Close()
	items := []int{10, 20, 30, 40, 50, 60, 70}
	submitted, err := sg.GoBatch(context.Background(), items, func(_ context.Context, item int) (int, error) {
		return item * 2, nil
	})
	if err != nil || len(submitted) != len(items) {
		t.Fatalf("GoBatch = (%v, %v)", submitted, err)
	}
	for i, result := range submitted {
		if result.Index != i || result.ShardIdx != i%3 || result.Err != nil {
			t.Fatalf("submission %d = %+v", i, result)
		}
	}
	results := sg.Wait()
	if len(results) != len(items) {
		t.Fatalf("got %d results, want %d", len(results), len(items))
	}
	want := make(map[int]bool, len(items))
	for _, item := range items {
		want[item*2] = true
	}
	for _, result := range results {
		if result.Err != nil || !want[result.Value] {
			t.Fatalf("unexpected or duplicate result: %+v", result)
		}
		delete(want, result.Value)
	}
}

func TestShardedGroupGoBatchPartialRejection(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 2})
	defer sg.Close()
	sg.GetShard(0).Wait()
	items := []int{10, 20, 30, 40}
	var calls atomic.Int32
	submitted, err := sg.GoBatch(context.Background(), items, func(_ context.Context, item int) (int, error) {
		calls.Add(1)
		return item, nil
	})
	if !errors.Is(err, core.ErrGroupWaited) || len(submitted) != len(items) {
		t.Fatalf("GoBatch = (%v, %v)", submitted, err)
	}
	for i, result := range submitted {
		if result.Index != i || result.ShardIdx != i%2 {
			t.Fatalf("submission %d = %+v", i, result)
		}
		if (i%2 == 0 && !errors.Is(result.Err, core.ErrGroupWaited)) || (i%2 == 1 && result.Err != nil) {
			t.Fatalf("submission %d: unexpected error %v", i, result.Err)
		}
	}
	results := sg.Wait()
	if calls.Load() != 2 || len(results) != 4 {
		t.Fatalf("calls=%d results=%v", calls.Load(), results)
	}
	want := map[int]bool{20: true, 40: true}
	rejected := 0
	for _, result := range results {
		if errors.Is(result.Err, core.ErrGroupWaited) {
			rejected++
			continue
		}
		if result.Err != nil || !want[result.Value] {
			t.Fatalf("unexpected result: %+v", result)
		}
		delete(want, result.Value)
	}
	if rejected != 2 || len(want) != 0 {
		t.Fatalf("rejected=%d missing=%v", rejected, want)
	}
}

func TestShardedGroupWaitDoesNotCancelOtherShard(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 1})
	defer sg.Close()
	_, ctx := sg.WithFailFast(context.Background())
	started, release := make(chan struct{}), make(chan struct{})
	result := make(chan struct{})
	if err := sg.GoAt(1, 0, ctx, func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-ctx.Done():
		case <-release:
		}
		close(result)
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	sg.GetShard(0).Wait()
	if err := ctx.Err(); err != nil {
		t.Errorf("other shard canceled on normal wait: %v", err)
	}
	close(release)
	<-result
	sg.Wait()
}

func TestShardedPoolWaitDoesNotCancelOtherShard(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(fmt.Sprint("reset=", reset), func(t *testing.T) {
			sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1, PoolCfg: pool.Config{FailFast: true}})
			defer sp.Close()
			if reset {
				sp.Wait()
				if err := sp.Reset(); err != nil {
					t.Fatal(err)
				}
			}
			started, release := make(chan struct{}), make(chan struct{})
			result := make(chan error, 1)
			if err := sp.SubmitAt(1, 0, sp.GetShard(1).Ctx(), func(ctx context.Context) (int, error) {
				close(started)
				select {
				case <-ctx.Done():
					result <- ctx.Err()
				case <-release:
					result <- nil
				}
				return 0, nil
			}); err != nil {
				t.Fatal(err)
			}
			<-started
			sp.GetShard(0).Wait()
			if err := sp.GetShard(1).Ctx().Err(); err != nil {
				t.Errorf("other shard canceled on normal wait: %v", err)
			}
			close(release)
			<-result
			sp.Wait()
		})
	}
}

func TestShardedPoolResetPreservesCrossShardFailFast(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1, PoolCfg: pool.Config{FailFast: true}})
	defer sp.Close()
	sp.Wait()
	if err := sp.Reset(); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	canceled := make(chan bool, 1)
	if err := sp.SubmitAt(0, 0, sp.GetShard(0).Ctx(), func(ctx context.Context) (int, error) {
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
	if err := sp.SubmitAt(1, 0, sp.GetShard(1).Ctx(), func(context.Context) (int, error) { return 0, errShardTest }); err != nil {
		close(release)
		t.Fatal(err)
	}
	sp.GetShard(1).Wait()
	select {
	case ok := <-canceled:
		if !ok {
			t.Error("other shard was not canceled")
		}
	case <-time.After(100 * time.Millisecond):
		close(release)
		<-canceled
		t.Error("failure did not cancel the other shard after reset")
	}
	sp.Wait()
}

func TestPoolBuilderFailFastCancelsRunContext(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan bool, 1)
	release := make(chan struct{})
	err := NewPoolBuilder[int]().Shards(2).Worker(1).FailFast().Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		if err := sp.SubmitAt(0, 0, ctx, func(ctx context.Context) (int, error) {
			close(started)
			select {
			case <-ctx.Done():
				canceled <- true
			case <-release:
				canceled <- false
			}
			return 0, nil
		}); err != nil {
			return err
		}
		<-started
		if err := sp.SubmitAt(1, 0, ctx, func(context.Context) (int, error) { return 0, errShardTest }); err != nil {
			close(release)
			return err
		}
		sp.GetShard(1).Wait()
		if ctx.Err() == nil {
			close(release)
			<-canceled
			return fmt.Errorf("failure did not cancel Run context")
		}
		select {
		case ok := <-canceled:
			close(release)
			if !ok {
				return fmt.Errorf("other shard was not canceled")
			}
		case <-time.After(time.Second):
			close(release)
			return fmt.Errorf("other shard did not finish")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRoundRobinCounterOverflow(t *testing.T) {
	sp := &ShardedPool[int]{pools: make([]*pool.Pool[int], 3), dist: RoundRobin}
	sp.nextIdx.Store(uint64(math.MaxInt) + 1)
	if idx := sp.pick(); idx < 0 || idx >= 3 {
		t.Fatalf("pool index=%d", idx)
	}
	sg := &ShardedGroup[int]{groups: make([]*group.Group[int], 3)}
	sg.next.Store(uint64(math.MaxInt) + 1)
	if idx := sg.pick(); idx < 0 || idx >= 3 {
		t.Fatalf("group index=%d", idx)
	}
}

func hashKey(key string, shardCount int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(uint64(h.Sum32()) % uint64(shardCount))
}

func printMemStatsShard(tag string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("[%s] G=%d Heap=%dMB\n", tag, runtime.NumGoroutine(), m.HeapAlloc/1024/1024)
}

// ============================================================
// 一、ShardedPool 基础构造测试
// ============================================================

func TestNewShardedPool_Defaults(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{})
	defer sp.Close()
	if sp.ShardCount() != DefaultShardCount {
		t.Fatalf("expected %d shards, got %d", DefaultShardCount, sp.ShardCount())
	}
}

func TestDefaultShardedPool(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	if sp.ShardCount() != DefaultShardCount {
		t.Fatalf("expected %d shards, got %d", DefaultShardCount, sp.ShardCount())
	}
}

func TestNewShardedPool_CustomConfig(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 2, Distribution: RoundRobin})
	defer sp.Close()
	if sp.ShardCount() != 8 {
		t.Fatalf("expected 8 shards, got %d", sp.ShardCount())
	}
	for i := 0; i < 8; i++ {
		if shard := sp.GetShard(i); shard == nil {
			t.Fatalf("shard %d is nil", i)
		} else if shard.Size() != 2 {
			t.Fatalf("shard %d expected size 2, got %d", i, shard.Size())
		}
	}
}

func TestShardedPool_GetShard_OOB(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	if s := sp.GetShard(-1); s != nil {
		t.Fatal("expected nil for negative index")
	}
	if s := sp.GetShard(sp.ShardCount()); s != nil {
		t.Fatal("expected nil for out-of-bounds index")
	}
}

// ============================================================
// 二、ShardedPool Submit / Wait 测试
// ============================================================

func TestShardedPool_Submit_Wait_Basic(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	n := 100
	for i := 0; i < n; i++ {
		if err := sp.Submit(ctx, func(ctx context.Context) (int, error) { return 42, nil }); err != nil {
			t.Fatalf("Submit error: %v", err)
		}
	}
	results := sp.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for _, r := range results {
		if r.Err != nil || r.Value != 42 {
			t.Fatalf("unexpected result: %v", r)
		}
	}
}

func TestShardedPool_SubmitAt(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	_ = sp.SubmitAt(0, 0, ctx, func(ctx context.Context) (int, error) { return 100, nil })
	_ = sp.SubmitAt(0, 1, ctx, func(ctx context.Context) (int, error) { return 200, nil })
	results := sp.Wait()
	if len(results) != 2 || results[0].Value != 100 || results[1].Value != 200 {
		t.Fatalf("unexpected results: %v", results)
	}
}

func TestShardedPool_TrySubmit(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 32})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		if err := sp.TrySubmit(ctx, func(ctx context.Context) (int, error) { return i, nil }); err != nil {
			t.Fatalf("TrySubmit error: %v", err)
		}
	}
	if len(sp.Wait()) != 200 {
		t.Fatal("expected 200 results")
	}
}

func TestShardedPool_SubmitKeyed(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := sp.SubmitKeyed("user-123", ctx, func(ctx context.Context) (int, error) { return i, nil }); err != nil {
			t.Fatalf("SubmitKeyed error: %v", err)
		}
	}
	if len(sp.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
	idx1 := hashKey("user-123", sp.ShardCount())
	idx2 := hashKey("user-123", sp.ShardCount())
	if idx1 != idx2 {
		t.Fatal("same key should land on same shard")
	}
}

func TestShardedPool_TrySubmitKeyed(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 64})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := sp.TrySubmitKeyed("shard-key", ctx, func(ctx context.Context) (int, error) { return i, nil }); err != nil {
			t.Fatalf("TrySubmitKeyed error: %v", err)
		}
	}
	if len(sp.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedPool_SubmitBatch(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 8})
	defer sp.Close()
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}
	ctx := context.Background()
	batchResults, err := sp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) { return item * 10, nil })
	if err != nil || len(batchResults) != 100 {
		t.Fatalf("SubmitBatch failed: err=%v, len=%d", err, len(batchResults))
	}
	if len(sp.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedPool_TrySubmitBatch(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 16})
	defer sp.Close()
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}
	ctx := context.Background()
	batchResults, err := sp.TrySubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) { return item * 2, nil })
	if err != nil || len(batchResults) != 100 {
		t.Fatalf("TrySubmitBatch failed: err=%v, len=%d", err, len(batchResults))
	}
	if len(sp.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedPoolBatchReportsPartialRejection(t *testing.T) {
	for _, nonblocking := range []bool{false, true} {
		t.Run(fmt.Sprintf("try=%v", nonblocking), func(t *testing.T) {
			sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 4})
			defer sp.Close()
			sp.GetShard(0).Close()
			items := []int{1, 2, 3, 4}
			fn := func(_ context.Context, value int) (int, error) { return value * 10, nil }
			var batch []SubmitBatchResult
			var err error
			if nonblocking {
				batch, err = sp.TrySubmitBatch(context.Background(), items, fn)
			} else {
				batch, err = sp.SubmitBatch(context.Background(), items, fn)
			}
			if !errors.Is(err, core.ErrPoolClosed) || len(batch) != len(items) {
				t.Fatalf("batch=%v err=%v", batch, err)
			}
			for i, result := range batch {
				if result.Index != i || result.ShardIdx != i%2 || (i%2 == 0 && !errors.Is(result.Err, core.ErrPoolClosed)) || (i%2 == 1 && result.Err != nil) {
					t.Fatalf("batch[%d]=%+v", i, result)
				}
			}
			results := sp.GetShard(1).Wait()
			if len(results) != 2 || results[0].Value != 20 || results[1].Value != 40 || results[0].Err != nil || results[1].Err != nil {
				t.Fatalf("successful shard results=%v", results)
			}
		})
	}
}

// ============================================================
// 三、ShardedPool WaitAndClose / Close
// ============================================================

func TestShardedPool_WaitAndClose(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	if len(sp.WaitAndClose()) != 200 {
		t.Fatal("expected 200 results")
	}
}

func TestShardedPoolConcurrentWaitAndCloseResultOwnership(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1})
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); sp.Close() })
	if err := sp.SubmitAt(0, 0, context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := sp.SubmitAt(1, 0, context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
		t.Fatal(err)
	}
	waited := make(chan []core.Result[int], 1)
	go func() { waited <- sp.Wait() }()
	deadline := time.After(5 * time.Second)
	for !sp.waitInvoked.Load() {
		select {
		case <-deadline:
			t.Fatal("Wait did not begin")
		default:
			runtime.Gosched()
		}
	}
	closed := make(chan []core.Result[int], 1)
	go func() { closed <- sp.WaitAndClose() }()
	unblock()
	first, second := <-waited, <-closed
	if len(first) != 2 || len(second) != 0 {
		t.Fatalf("Wait results=%v, WaitAndClose results=%v; want all results owned by first Wait", first, second)
	}
}

func TestShardedPoolWaitAfterWaitAndCloseReturnsNoResults(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1})
	t.Cleanup(sp.Close)
	for i := 0; i < 2; i++ {
		if err := sp.SubmitAt(i, 0, context.Background(), func(context.Context) (int, error) { return i, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if got := sp.WaitAndClose(); len(got) != 2 {
		t.Fatalf("WaitAndClose results = %v", got)
	}
	if got := sp.Wait(); got != nil {
		t.Fatalf("Wait after WaitAndClose = %v, want nil", got)
	}
}

func TestShardedPoolConcurrentWaitAndCloseStillCloses(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 1, SizePerShard: 1})
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); sp.Close() })
	if err := sp.Submit(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 42, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	waited := make(chan []core.Result[int], 1)
	go func() { waited <- sp.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for !sp.waitInvoked.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !sp.waitInvoked.Load() {
		t.Fatal("Wait did not begin")
	}
	closed := make(chan []core.Result[int], 1)
	go func() { closed <- sp.WaitAndClose() }()
	unblock()
	if got := <-waited; len(got) != 1 || got[0].Value != 42 || got[0].Err != nil {
		t.Fatalf("Wait results = %v", got)
	}
	if got := <-closed; got != nil {
		t.Fatalf("WaitAndClose results = %v, want nil", got)
	}
	if err := sp.Submit(context.Background(), func(context.Context) (int, error) { return 0, nil }); !errors.Is(err, core.ErrPoolClosed) {
		t.Fatalf("Submit after WaitAndClose = %v, want closed", err)
	}
}

func TestShardedPool_Close(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	sp.Close()
}

// ============================================================
// 四、ShardedPool 配置代理
// ============================================================

func TestShardedPool_WithTimeout(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	sp.WithTimeout(5 * time.Second)
	for i := 0; i < sp.ShardCount(); i++ {
		if sp.GetShard(i) == nil {
			t.Fatal("shard is nil")
		}
	}
}

func TestShardedPool_WithSubmitTimeout(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	sp.WithSubmitTimeout(2 * time.Second)
}

func TestShardedPool_WithFailFast(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	ctx := context.Background()
	if sp, _ = sp.WithFailFast(ctx); sp == nil {
		t.Fatal("nil after WithFailFast")
	}
}

func TestShardedPool_WithStreaming(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithStreaming(64)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	if len(sp.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedPool_WithResultCallback(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	var count atomic.Int64
	sp.WithResultCallback(func(r core.Result[int]) { count.Add(1) })
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	if len(sp.Wait()) != 100 || count.Load() != 100 {
		t.Fatalf("results=%d callback=%d", len(sp.Wait()), count.Load())
	}
}

func TestShardedPool_WithRingBuffer(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithRingBuffer(500, core.OverflowDrop)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	if len(sp.Flush(0)) != 1000 {
		t.Fatal("Flush expected 1000")
	}
}

func TestShardedPool_WithRingBuffer_OverflowBlock(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithRingBuffer(200, core.OverflowBlock)
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	firstBatch := sp.Flush(80)
	secondBatch := sp.Flush(0)
	if len(firstBatch)+len(secondBatch) != 200 {
		t.Fatalf("Flush total expected 200, got %d", len(firstBatch)+len(secondBatch))
	}
}

func TestShardedPool_WithMaxPending(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1})
	defer sp.Close()
	sp.WithMaxPending(1000)
}

func TestShardedPool_WithOverflow(t *testing.T) {
	sp := DefaultShardedPool[int]()
	defer sp.Close()
	sp.WithOverflow(core.OverflowBlock)
	sp.WithMaxPending(1000)
	sp.WithOverflow(core.OverflowDrop)
}

// ============================================================
// 五、ShardedPool 统计聚合
// ============================================================

func TestShardedPool_ShardStats(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	stats := sp.ShardStats()
	if len(stats) != 4 {
		t.Fatalf("expected 4 stats entries, got %d", len(stats))
	}
	for i, s := range stats {
		if s.Size != 4 {
			t.Fatalf("shard %d expected size 4, got %d", i, s.Size)
		}
	}
}

func TestShardedPool_TotalWorkerCount(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 3})
	defer sp.Close()
	if tc := sp.TotalWorkerCount(); tc != 12 {
		t.Fatalf("TotalWorkerCount: expected 12, got %d", tc)
	}
}

func TestShardedPool_OkFailCount(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	if sc := sp.TotalOkCount(); sc != 200 {
		t.Fatalf("TotalOkCount: expected 200, got %d", sc)
	}
	if fc := sp.TotalFailCount(); fc != 0 {
		t.Fatalf("TotalFailCount: expected 0, got %d", fc)
	}
}

// ============================================================
// 六、ShardedPool Reset
// ============================================================

func TestShardedPool_Reset(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	if err := sp.Reset(); err != nil {
		t.Fatalf("Reset error: %v", err)
	}
	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i * 2, nil })
	}
	if len(sp.Wait()) != 50 {
		t.Fatal("expected 50 results after reset")
	}
}

// ============================================================
// 七、ShardedGroup 基础构�?// ============================================================

func TestNewShardedGroup_Defaults(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{})
	if sg.ShardCount() != DefaultShardCount {
		t.Fatalf("expected %d shards, got %d", DefaultShardCount, sg.ShardCount())
	}
}

func TestDefaultShardedGroup(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	if sg.ShardCount() != DefaultShardCount {
		t.Fatalf("expected %d shards, got %d", DefaultShardCount, sg.ShardCount())
	}
}

func TestNewShardedGroup_Custom(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 2, Distribution: RoundRobin})
	if sg.ShardCount() != 8 {
		t.Fatalf("expected 8 shards, got %d", sg.ShardCount())
	}
	if tc := sg.TotalWorker(); tc != 16 {
		t.Fatalf("TotalWorker: expected 16, got %d", tc)
	}
}

func TestShardedGroup_GetShard_OOB(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	if s := sg.GetShard(-1); s != nil {
		t.Fatal("expected nil for negative index")
	}
	if s := sg.GetShard(sg.ShardCount()); s != nil {
		t.Fatal("expected nil for out-of-bounds")
	}
}

// ============================================================
// 八、ShardedGroup Go / Wait
// ============================================================

func TestShardedGroup_Go_Wait_Basic(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	n := 200
	for i := 0; i < n; i++ {
		if err := sg.Go(ctx, func(ctx context.Context) (int, error) { return 42, nil }); err != nil {
			t.Fatalf("Go error: %v", err)
		}
	}
	results := sg.Wait()
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for _, r := range results {
		if r.Err != nil || r.Value != 42 {
			t.Fatalf("unexpected result: %v", r)
		}
	}
}

func TestShardedGroup_GoAt(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	_ = sg.GoAt(0, 0, ctx, func(ctx context.Context) (int, error) { return 100, nil })
	_ = sg.GoAt(0, 1, ctx, func(ctx context.Context) (int, error) { return 200, nil })
	if len(sg.Wait()) < 2 {
		t.Fatal("expected at least 2 results")
	}
}

func TestShardedGroup_GoKeyed(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := sg.GoKeyed("key-A", ctx, func(ctx context.Context) (int, error) { return i, nil }); err != nil {
			t.Fatalf("GoKeyed error: %v", err)
		}
	}
	if len(sg.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedGroup_GoBatch(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 8})
	ctx := context.Background()
	items := make([]int, 500)
	for i := range items {
		items[i] = i
	}
	batchResults, err := sg.GoBatch(ctx, items, func(ctx context.Context, item int) (int, error) { return item * 10, nil })
	if err != nil || len(batchResults) != 500 {
		t.Fatalf("GoBatch failed: err=%v, len=%d", err, len(batchResults))
	}
	if len(sg.Wait()) != 500 {
		t.Fatal("expected 500 results")
	}
}

// ============================================================
// 九、ShardedGroup WaitTimeout / WaitContext
// ============================================================

func TestShardedGroupWaitCancellationReachesEveryShard(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprintf("timeout=%v", timeout), func(t *testing.T) {
			const shards = 4
			sg := NewShardedGroup(ShardGroupConfig[int]{Shards: shards, ConcurrencyPerShard: 1})
			defer sg.Close()
			started := make(chan int, shards)
			canceled := make(chan int, shards)
			for shard := 0; shard < shards; shard++ {
				shard := shard
				if err := sg.GoAt(shard, 0, context.Background(), func(ctx context.Context) (int, error) {
					started <- shard
					<-ctx.Done()
					canceled <- shard
					return shard, ctx.Err()
				}); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < shards; i++ {
				<-started
			}
			var ok bool
			if timeout {
				_, ok = sg.WaitTimeout(0)
			} else {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, ok = sg.WaitContext(ctx)
			}
			if ok {
				t.Fatal("canceled wait reported completion")
			}
			seen := make(map[int]bool)
			for i := 0; i < shards; i++ {
				select {
				case shard := <-canceled:
					seen[shard] = true
				case <-time.After(5 * time.Second):
					t.Fatalf("cancellation reached %d shards", len(seen))
				}
			}
			if len(seen) != shards {
				t.Fatalf("canceled shards=%v", seen)
			}
		})
	}
}

func TestShardedGroupWaitTimeoutDoesNotStopNonCooperativeTasks(t *testing.T) {
	const shards = 2
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: shards, ConcurrencyPerShard: 1})
	defer sg.Close()
	started := make(chan struct{}, shards)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	for i := 0; i < shards; i++ {
		if err := sg.GoAt(i, 0, context.Background(), func(context.Context) (int, error) {
			started <- struct{}{}
			<-release
			return i, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < shards; i++ {
		<-started
	}
	_, ok := sg.WaitTimeout(0)
	if ok {
		t.Fatal("timed wait reported completion while tasks were blocked")
	}
	releaseOnce.Do(func() { close(release) })
	results := sg.Wait()
	if len(results) != shards {
		t.Fatalf("final results = %v", results)
	}
	seen := make(map[int]bool)
	for _, result := range results {
		if result.Err != nil || result.Value < 0 || result.Value >= shards || seen[result.Value] {
			t.Fatalf("unexpected final result: %+v", result)
		}
		seen[result.Value] = true
	}
	if err := sg.Reset(); err != nil {
		t.Fatalf("reset after final wait: %v", err)
	}
	for i := 0; i < shards; i++ {
		if err := sg.GoAt(i, 0, context.Background(), func(context.Context) (int, error) { return i + 10, nil }); err != nil {
			t.Fatal(err)
		}
	}
	results = sg.Wait()
	if len(results) != shards {
		t.Fatalf("reused results = %v", results)
	}
	for i, result := range results {
		if result.Err != nil || result.Value != i+10 {
			t.Fatalf("reused result %d = %+v", i, result)
		}
	}
}

func TestShardedGroup_WaitTimeout(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return i, nil
		})
	}
	results, ok := sg.WaitTimeout(5 * time.Second)
	if !ok || len(results) != 100 {
		t.Fatalf("WaitTimeout failed: ok=%v, len=%d", ok, len(results))
	}
}

func TestShardedGroup_WaitContext(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	results, ok := sg.WaitContext(ctx)
	if !ok || len(results) != 100 {
		t.Fatalf("WaitContext failed: ok=%v, len=%d", ok, len(results))
	}
}

// ============================================================
// 十、ShardedGroup 配置代理
// ============================================================

func TestShardedGroup_WithTimeout(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	sg.WithTimeout(10 * time.Second)
}

func TestShardedGroup_WithSubmitTimeout(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	sg.WithSubmitTimeout(3 * time.Second)
}

func TestShardedGroup_WithFailFast(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	ctx := context.Background()
	if sg, _ = sg.WithFailFast(ctx); sg == nil {
		t.Fatal("nil after WithFailFast")
	}
}

func TestShardedGroup_WithFFCtx(t *testing.T) {
	sg := DefaultShardedGroup[int]()
	ctx := context.Background()
	if sg, _ = sg.WithFFCtx(ctx); sg == nil {
		t.Fatal("nil after WithFFCtx")
	}
}

func TestShardedGroup_WithStreaming(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 2})
	sg.WithStreaming(64)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	if len(sg.Wait()) != 100 {
		t.Fatal("expected 100 results")
	}
}

func TestShardedGroup_WithResultCallback(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 2})
	var count atomic.Int64
	sg.WithResultCallback(func(r core.Result[int]) { count.Add(1) })
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sg.Wait()
	if count.Load() != 100 {
		t.Fatalf("callback count: expected 100, got %d", count.Load())
	}
}

// ============================================================
// 十一、ShardedGroup 统计�?Reset
// ============================================================

func TestShardedGroup_TotalFailCount(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sg.Wait()
	if fc := sg.TotalFailCount(); fc != 0 {
		t.Fatalf("TotalFailCount: expected 0, got %d", fc)
	}
	if sc := sg.TotalOkCount(); sc != 50 {
		t.Fatalf("TotalOkCount: expected 50, got %d", sc)
	}
}

func TestShardedGroup_TotalWorker(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 5})
	if tc := sg.TotalWorker(); tc != 20 {
		t.Fatalf("TotalWorker: expected 20, got %d", tc)
	}
}

func TestShardedGroup_Reset(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sg.Wait()
	if err := sg.Reset(); err != nil {
		t.Fatalf("Reset error: %v", err)
	}
	for i := 0; i < 50; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i * 2, nil })
	}
	if len(sg.Wait()) != 50 {
		t.Fatal("expected 50 results after reset")
	}
}

// ============================================================
// 十二、四档并发压力测试（�?十万/百万/千万�?// ============================================================

func TestShardedPool_Submit_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
			defer sp.Close()

			ctx := context.Background()
			var wg sync.WaitGroup
			goroutines := 50
			tasksPerGoroutine := tier.Size / 50
			if tasksPerGoroutine < 1 {
				tasksPerGoroutine = 1
			}
			var submitted atomic.Int64

			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				go func(gid int) {
					defer wg.Done()
					for i := 0; i < tasksPerGoroutine; i++ {
						if sp.Submit(ctx, func(ctx context.Context) (int, error) {
							return gid*10000 + i, nil
						}) == nil {
							submitted.Add(1)
						}
					}
				}(g)
			}
			wg.Wait()
			results := sp.Wait()
			expected := int(submitted.Load())
			if len(results) != expected {
				t.Fatalf("expected %d results, got %d", expected, len(results))
			}
			for _, r := range results {
				if r.Err != nil {
					t.Fatalf("unexpected error: %v", r.Err)
				}
			}
		})
	}
}

func TestShardedPool_SubmitKeyed_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
			defer sp.Close()

			ctx := context.Background()
			keys := []string{"user-a", "user-b", "user-c", "user-d"}
			var wg sync.WaitGroup
			var submitted atomic.Int64
			tasksPerKey := tier.Size / len(keys)

			for _, key := range keys {
				wg.Add(1)
				go func(k string) {
					defer wg.Done()
					for i := 0; i < tasksPerKey; i++ {
						if sp.SubmitKeyed(k, ctx, func(ctx context.Context) (int, error) {
							return 1, nil
						}) == nil {
							submitted.Add(1)
						}
					}
				}(key)
			}
			wg.Wait()
			results := sp.Wait()
			if len(results) != int(submitted.Load()) {
				t.Fatalf("expected %d results, got %d", submitted.Load(), len(results))
			}
		})
	}
}

func TestShardedGroup_Go_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 16})
			ctx := context.Background()

			var wg sync.WaitGroup
			goroutines := 50
			tasksPerGoroutine := tier.Size / 50
			if tasksPerGoroutine < 1 {
				tasksPerGoroutine = 1
			}
			var submitted atomic.Int64

			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				go func(gid int) {
					defer wg.Done()
					for i := 0; i < tasksPerGoroutine; i++ {
						if sg.Go(ctx, func(ctx context.Context) (int, error) {
							return gid*10000 + i, nil
						}) == nil {
							submitted.Add(1)
						}
					}
				}(g)
			}
			wg.Wait()
			results := sg.Wait()
			if len(results) != int(submitted.Load()) {
				t.Fatalf("expected %d results, got %d", submitted.Load(), len(results))
			}
		})
	}
}

// ============================================================
// 十三、Race 竞态测�?// ============================================================

func TestRace_ShardedPool_SubmitWait(t *testing.T) {
	for round := 0; round < 20; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		goroutines := 100
		tasksPerGoroutine := 200

		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < tasksPerGoroutine; i++ {
					if sp.Submit(ctx, func(ctx context.Context) (int, error) {
						return gid*10000 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		results := sp.Wait()
		sp.Close()
		if len(results) != int(submitted.Load()) {
			t.Fatalf("round %d: expected %d, got %d", round, submitted.Load(), len(results))
		}
	}
}

func TestRace_ShardedPool_SubmitKeyed(t *testing.T) {
	for round := 0; round < 20; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		keys := []string{"user-1", "user-2", "user-3", "user-4", "user-5"}
		var submitted atomic.Int64

		for _, key := range keys {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					if sp.SubmitKeyed(k, ctx, func(ctx context.Context) (int, error) { return 1, nil }) == nil {
						submitted.Add(1)
					}
				}
			}(key)
		}
		wg.Wait()
		results := sp.Wait()
		sp.Close()
		if len(results) != int(submitted.Load()) {
			t.Fatalf("round %d: expected %d, got %d", round, submitted.Load(), len(results))
		}
	}
}

func TestRace_ShardedPool_MixedSubmit(t *testing.T) {
	for round := 0; round < 10; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64

		wg.Add(3)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				if sp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil }) == nil {
					submitted.Add(1)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				if sp.TrySubmit(ctx, func(ctx context.Context) (int, error) { return 2, nil }) == nil {
					submitted.Add(1)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				if sp.SubmitKeyed("mix", ctx, func(ctx context.Context) (int, error) { return 3, nil }) == nil {
					submitted.Add(1)
				}
			}
		}()
		wg.Wait()
		results := sp.Wait()
		sp.Close()
		expected := int(submitted.Load())
		if len(results) < expected {
			t.Fatalf("round %d: expected at least %d, got %d", round, expected, len(results))
		}
		if len(results) > expected {
			t.Logf("round %d: results=%d > submitted=%d (race: possible dups)", round, len(results), expected)
		}
	}
}

func TestRace_ShardedPool_SubmitBatch(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 8, SizePerShard: 16})
	defer sp.Close()
	ctx := context.Background()
	items := make([]int, 1000)
	for i := range items {
		items[i] = i
	}
	var wg sync.WaitGroup
	var submitted atomic.Int64

	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batchResults, err := sp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
				return item * 10, nil
			})
			if err != nil {
				return
			}
			for _, br := range batchResults {
				if br.Err == nil {
					submitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	results := sp.Wait()
	if len(results) != int(submitted.Load()) {
		t.Fatalf("expected %d results, got %d", submitted.Load(), len(results))
	}
}

func TestRace_ShardedPool_Streaming(t *testing.T) {
	for round := 0; round < 10; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 8})
		sp.WithStreaming(0)
		var streamedCount atomic.Int64
		sp.WithResultCallback(func(r core.Result[int]) {
			if r.Ok() {
				streamedCount.Add(1)
			}
		})

		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		goroutines := 50

		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					if sp.Submit(ctx, func(ctx context.Context) (int, error) {
						return gid*10000 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		results := sp.Wait()
		sp.Close()
		if len(results) != int(submitted.Load()) || streamedCount.Load() != int64(len(results)) {
			t.Fatalf("round %d: submit=%d results=%d streamed=%d", round, submitted.Load(), len(results), streamedCount.Load())
		}
	}
}

func TestRace_ShardedPool_RingBuffer(t *testing.T) {
	for round := 0; round < 10; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 8})
		sp.WithRingBuffer(2000, core.OverflowDrop)
		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		goroutines := 50

		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					if sp.Submit(ctx, func(ctx context.Context) (int, error) {
						return gid*10000 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		sp.Wait()
		flushed := sp.Flush(0)
		sp.Close()
		if len(flushed) != int(submitted.Load()) {
			t.Fatalf("round %d: expected %d, got %d", round, submitted.Load(), len(flushed))
		}
	}
}

func TestRace_ShardedGroup_GoWait(t *testing.T) {
	for round := 0; round < 20; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		goroutines := 100

		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 200; i++ {
					if sg.Go(ctx, func(ctx context.Context) (int, error) {
						return gid*10000 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		results := sg.Wait()
		if len(results) != int(submitted.Load()) {
			t.Fatalf("round %d: expected %d, got %d", round, submitted.Load(), len(results))
		}
	}
}

func TestRace_ShardedGroup_GoKeyed(t *testing.T) {
	for round := 0; round < 20; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 16})
		ctx := context.Background()
		var wg sync.WaitGroup
		keys := []string{"k-a", "k-b", "k-c", "k-d", "k-e"}
		var submitted atomic.Int64

		for _, key := range keys {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					if sg.GoKeyed(k, ctx, func(ctx context.Context) (int, error) { return 1, nil }) == nil {
						submitted.Add(1)
					}
				}
			}(key)
		}
		wg.Wait()
		results := sg.Wait()
		if len(results) != int(submitted.Load()) {
			t.Fatalf("round %d: expected %d, got %d", round, submitted.Load(), len(results))
		}
	}
}

func TestRace_ShardedGroup_GoBatch(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 8, ConcurrencyPerShard: 16})
	ctx := context.Background()
	items := make([]int, 500)
	for i := range items {
		items[i] = i
	}
	var wg sync.WaitGroup
	var submitted atomic.Int64

	for r := 0; r < 10; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batchResults, _ := sg.GoBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
				return item * 10, nil
			})
			for _, br := range batchResults {
				if br.Err == nil {
					submitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	results := sg.Wait()
	if len(results) != int(submitted.Load()) {
		t.Fatalf("expected %d results, got %d", submitted.Load(), len(results))
	}
}

func TestRace_ShardedGroup_Streaming(t *testing.T) {
	for round := 0; round < 10; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 8})
		sg.WithStreaming(0)
		var streamedCount atomic.Int64
		sg.WithResultCallback(func(r core.Result[int]) {
			if r.Ok() {
				streamedCount.Add(1)
			}
		})

		ctx := context.Background()
		var wg sync.WaitGroup
		var submitted atomic.Int64
		goroutines := 50

		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					if sg.Go(ctx, func(ctx context.Context) (int, error) {
						return gid*10000 + i, nil
					}) == nil {
						submitted.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		results := sg.Wait()
		if len(results) != int(submitted.Load()) || streamedCount.Load() != int64(len(results)) {
			t.Fatalf("round %d: submit=%d results=%d streamed=%d", round, submitted.Load(), len(results), streamedCount.Load())
		}
	}
}

// ============================================================
// 十四、ShardPoolBuilder 测试
// ============================================================

func TestShardPoolBuilder_Basic(t *testing.T) {
	b := NewPoolBuilder[int]()
	b.Context(context.Background()).Shards(4).Worker(2).Distribution(RoundRobin).
		Timeout(10 * time.Second).FailFast().MaxPending(1000).
		Config(func(cfg ShardPoolConfig[int]) ShardPoolConfig[int] {
			cfg.Shards = 4
			return cfg
		})
	err := b.Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		if sp.ShardCount() != 4 {
			t.Fatalf("ShardCount = %d, want 4", sp.ShardCount())
		}
		for i := 0; i < 100; i++ {
			_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		results := sp.Wait()
		if len(results) != 100 {
			t.Fatalf("expected 100, got %d", len(results))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestShardPoolBuilder_KeyFn(t *testing.T) {
	b := NewPoolBuilder[int]()
	b.Context(context.Background()).Shards(4).Worker(2).Distribution(Hash).
		KeyFn(func(item int) uint64 { return uint64(item) })
	err := b.Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		for i := 0; i < 100; i++ {
			_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		sp.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestShardPoolBuilder_DefaultMethods(t *testing.T) {
	b := NewPoolBuilder[int]()
	b.DefaultShardPoolConfig().DefaultShards().DefaultWorker().
		DefaultDistribution().DefaultTimeout().DefaultFailFast().
		DefaultMaxPending()
	err := b.Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		for i := 0; i < 50; i++ {
			_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		sp.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestShardPoolBuilder_Logger(t *testing.T) {
	b := NewPoolBuilder[int]()
	b.Logger(nil).DefaultLogger()
	err := b.Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		sp.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

// ============================================================
// 十五、ShardedGroupBuilder 测试
// ============================================================

func TestShardedGroupBuilder_Basic(t *testing.T) {
	b := NewGroupBuilder[int]()
	b.Context(context.Background()).Shards(4).Worker(2).Distribution(RoundRobin).
		Config(func(cfg ShardGroupConfig[int]) ShardGroupConfig[int] {
			cfg.Shards = 4
			return cfg
		})
	err := b.Run(func(ctx context.Context, sg *ShardedGroup[int]) error {
		for i := 0; i < 100; i++ {
			_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		results := sg.Wait()
		if len(results) != 100 {
			t.Fatalf("expected 100, got %d", len(results))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestShardedGroupBuilder_DefaultMethods(t *testing.T) {
	b := NewGroupBuilder[int]()
	b.DefaultShardedGroupConfig().DefaultShards().DefaultWorker().
		DefaultDistribution()
	err := b.Run(func(ctx context.Context, sg *ShardedGroup[int]) error {
		for i := 0; i < 50; i++ {
			_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		sg.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

func TestShardedGroupBuilder_Logger(t *testing.T) {
	b := NewGroupBuilder[int]()
	b.Logger(nil).DefaultLogger()
	err := b.Run(func(ctx context.Context, sg *ShardedGroup[int]) error {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return 1, nil })
		sg.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
}

// ============================================================
// 十六、便捷构造函数测试
// ============================================================

func TestNewShardedPoolSimple(t *testing.T) {
	sp := NewShardedPoolSimple[int](8, 16)
	defer sp.Close()
	if sp.ShardCount() != 8 {
		t.Fatalf("ShardCount = %d, want 8", sp.ShardCount())
	}
	for i := 0; i < sp.ShardCount(); i++ {
		if sp.GetShard(i).Size() != 16 {
			t.Fatalf("shard %d size = %d, want 16", i, sp.GetShard(i).Size())
		}
	}
}

func TestNewShardedPoolSimple_DefaultWorker(t *testing.T) {
	sp := NewShardedPoolSimple[int](4, 0)
	defer sp.Close()
	if sp.ShardCount() != 4 {
		t.Fatalf("ShardCount = %d, want 4", sp.ShardCount())
	}
}

func TestDefaultShardedPoolWith(t *testing.T) {
	sp := DefaultShardedPoolWith[int](8)
	defer sp.Close()
	if sp.ShardCount() != 8 {
		t.Fatalf("ShardCount = %d, want 8", sp.ShardCount())
	}
}

// ============================================================
// 十七、WithShardCfg 测试
// ============================================================

func TestWithShardCfg(t *testing.T) {
	cfg := ShardPoolConfig[int]{Shards: 4, SizePerShard: 2, Distribution: RoundRobin}
	err := WithShardCfg(context.Background(), cfg, func(sp *ShardedPool[int]) error {
		if sp.ShardCount() != 4 {
			return nil
		}
		for i := 0; i < 100; i++ {
			_ = sp.Submit(context.Background(), func(ctx context.Context) (int, error) { return i, nil })
		}
		results := sp.Wait()
		if len(results) != 100 {
			t.Fatalf("expected 100, got %d", len(results))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithShardCfg error: %v", err)
	}
}

// ============================================================
// 十八、StreamResults 测试
// ============================================================

func TestShardedPoolStreamPreservesValuesAndErrors(t *testing.T) {
	const shards, perShard = 4, 16
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: shards, SizePerShard: 4})
	defer sp.Close()
	sp.WithStreaming(perShard)
	stream := sp.StreamResults()
	wantErr := errors.New("item failed")
	for shard := 0; shard < shards; shard++ {
		for i := 0; i < perShard; i++ {
			value := shard*perShard + i
			err := sp.SubmitAt(shard, i, context.Background(), func(context.Context) (int, error) {
				if value%2 == 0 {
					return value, wantErr
				}
				return value, nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	sp.Wait()
	seen := make([]bool, shards*perShard)
	for result := range stream {
		value := result.Value
		if value < 0 || value >= len(seen) || seen[value] {
			t.Fatalf("duplicate or invalid result: %+v", result)
		}
		if (value%2 == 0 && !errors.Is(result.Err, wantErr)) || (value%2 != 0 && result.Err != nil) {
			t.Fatalf("result %d: err=%v", value, result.Err)
		}
		seen[value] = true
	}
	for value, found := range seen {
		if !found {
			t.Fatalf("missing result %d", value)
		}
	}
}

func TestShardedPool_StreamResults(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	sp.WithStreaming(64)
	ctx := context.Background()

	n := 200
	for i := 0; i < n; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}

	ch := sp.StreamResults()
	var wg sync.WaitGroup
	var count int

	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range ch {
			if r.Err != nil {
				t.Errorf("unexpected error: %v", r.Err)
			}
			count++
		}
	}()

	sp.Wait()
	wg.Wait()

	if count != n {
		t.Fatalf("streamed count = %d, want %d", count, n)
	}
}

// ============================================================
// 十九、Ctx / Cancel 测试
// ============================================================

func TestShardedPool_Ctx(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctxx := sp.Ctx()
	if ctxx == nil {
		t.Fatal("Ctx should not return nil")
	}
}

func TestShardedPool_Cancel(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	sp.Close()
}

// ============================================================
// 二十、统计方法补充测试
// ============================================================

func TestShardedPool_TotalActiveBusyPending(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	defer sp.Close()
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	if ta := sp.TotalActive(); ta != 0 {
		t.Logf("TotalActive after wait = %d (expected 0)", ta)
	}
	if tb := sp.TotalBusy(); tb != 0 {
		t.Logf("TotalBusy after wait = %d (expected 0)", tb)
	}
}

func TestShardedPool_TotalPending_Default(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 1})
	defer sp.Close()
	tp := sp.TotalPending()
	if tp != 0 {
		t.Fatalf("TotalPending on empty = %d, want 0", tp)
	}
}

func TestShardedPool_Flush_Multiple(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 2})
	defer sp.Close()
	sp.WithRingBuffer(500, core.OverflowDrop)
	ctx := context.Background()
	for i := 0; i < 300; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
	first := sp.Flush(50)
	second := sp.Flush(0)
	if len(first)+len(second) != 300 {
		t.Fatalf("total flushed = %d, want 300", len(first)+len(second))
	}
}

// ============================================================
// 二十一、ShardedPool TrySubmit 限流测试
// ============================================================

func TestShardedPool_TrySubmit_Overflow(t *testing.T) {
	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1})
	defer sp.Close()
	sp.WithMaxPending(10)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		sp.TrySubmit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	sp.Wait()
}

// ============================================================
// 二十二、交叉验证：Pool vs Group vs Builder
// ============================================================

func TestShard_CrossValidation_PoolVsGroup(t *testing.T) {
	n := 200
	ctx := context.Background()

	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	for i := 0; i < n; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	poolResults := sp.Wait()
	sp.Close()

	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
	for i := 0; i < n; i++ {
		_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	groupResults := sg.Wait()

	if len(poolResults) != n || len(groupResults) != n {
		t.Fatalf("Pool=%d Group=%d, both want %d", len(poolResults), len(groupResults), n)
	}
}

func TestShard_CrossValidation_BuilderVsDirect(t *testing.T) {
	n := 100
	ctx := context.Background()

	sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
	for i := 0; i < n; i++ {
		_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
	}
	directResults := sp.Wait()
	sp.Close()

	var builderResultsLen int
	b := NewPoolBuilder[int]()
	b.Context(ctx).Shards(4).Worker(4).Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		for i := 0; i < n; i++ {
			_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
		}
		builderResultsLen = len(sp.Wait())
		return nil
	})

	if len(directResults) != n || builderResultsLen != n {
		t.Fatalf("Direct=%d Builder=%d, both want %d", len(directResults), builderResultsLen, n)
	}
}

// ============================================================
// 二十三、Race 竞态测试 - ShardPoolBuilder / ShardedGroupBuilder
// ============================================================

func TestRace_ShardPoolBuilder_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		var success atomic.Int64
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				b := NewPoolBuilder[int]()
				err := b.Shards(2).Worker(2).Run(func(ctx context.Context, sp *ShardedPool[int]) error {
					for i := 0; i < 50; i++ {
						_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
					}
					sp.Wait()
					return nil
				})
				if err == nil {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 10 {
			t.Fatalf("round %d: success=%d", round, success.Load())
		}
	}
}

func TestRace_ShardedGroupBuilder_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		var success atomic.Int64
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				b := NewGroupBuilder[int]()
				err := b.Shards(2).Worker(2).Run(func(ctx context.Context, sg *ShardedGroup[int]) error {
					for i := 0; i < 50; i++ {
						_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
					}
					sg.Wait()
					return nil
				})
				if err == nil {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 10 {
			t.Fatalf("round %d: success=%d", round, success.Load())
		}
	}
}

func TestRace_ShardedPool_Reset_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		sp := NewShardedPool(ShardPoolConfig[int]{Shards: 4, SizePerShard: 4})
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = sp.Submit(ctx, func(ctx context.Context) (int, error) { return i, nil })
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			sp.Reset()
		}()
		wg.Wait()
		sp.Close()
	}
}

func TestRace_ShardedGroup_Reset_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 4})
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = sg.Go(ctx, func(ctx context.Context) (int, error) { return i, nil })
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			sg.Reset()
		}()
		wg.Wait()
		sg.Close()
	}
}

func TestRace_ShardedGroup_WaitTimeout(t *testing.T) {
	for round := 0; round < 10; round++ {
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 4, ConcurrencyPerShard: 8})
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				_ = sg.Go(ctx, func(ctx context.Context) (int, error) {
					time.Sleep(5 * time.Millisecond)
					return i, nil
				})
			}
		}()
		time.Sleep(50 * time.Millisecond)
		_, ok := sg.WaitTimeout(10 * time.Second)
		wg.Wait()
		if !ok {
			t.Fatalf("round %d: WaitTimeout returned false", round)
		}
	}
}
