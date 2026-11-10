package review_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	async "github.com/chichengyu/async"
)

type startupExampleLogger struct{ marker any }

func Example_taskBusinessGoexit() {
	ar := async.Task[int]().Bounded(1).Go(func(context.Context) (int, error) { runtime.Goexit(); return 0, nil })
	v, err := ar.Wait()
	fmt.Println("result:", v, errors.Is(err, async.ErrTaskGoexit), ar.Ok(), ar.IsPanic())
	_, again := ar.Wait()
	fmt.Println("cached:", again == err)
	// Output:
	// result: 0 true false false
	// cached: true
}

func Example_mapNativeGoexit() {
	var calls atomic.Int32
	result := async.NewMapChain(context.Background(), map[int]int{1: 1, 2: 2, 3: 3}).Worker(1).Shards(2).Map(func(_ context.Context, _ int, value int) (int, error) {
		if calls.Add(1) == 1 {
			runtime.Goexit()
		}
		return value * 10, nil
	})
	fmt.Println("map:", calls.Load(), result.Len(), errors.Is(result.Error(), async.ErrTaskGoexit))
	// Output:
	// map: 3 2 true
}

func Example_sliceStreamGoexitFailFast() {
	var calls atomic.Int32
	ch := async.Slice(context.Background(), []int{1, 2, 3}).Worker(1).FailFast().Stream(func(_ context.Context, value int) (int, error) {
		calls.Add(1)
		if value == 1 {
			runtime.Goexit()
		}
		return value * 10, nil
	}, 1)
	goexits := 0
	for r := range ch {
		if errors.Is(r.Err, async.ErrTaskGoexit) {
			goexits++
		}
	}
	fmt.Println("calls:", calls.Load(), "goexits:", goexits)
	// Output:
	// calls: 1 goexits: 1
}

func Example_sliceNativeGoexit() {
	fn := func(_ context.Context, value int) (int, error) {
		if value == 1 {
			runtime.Goexit()
		}
		return value * 10, nil
	}
	plain := async.Slice(context.Background(), []int{1, 2, 3}).Worker(1).Map(fn)
	rs := plain.Results()
	fmt.Println("map:", errors.Is(rs[0].Err, async.ErrTaskGoexit), rs[0].IsPanic(), rs[1].Value, rs[2].Value)
	sharded := async.Slice(context.Background(), []int{1, 2, 3}).Worker(1).Shards(2).ForEach(func(ctx context.Context, value int) error { _, err := fn(ctx, value); return err })
	fmt.Println("each:", sharded.Total(), sharded.FailCount(), errors.Is(sharded.Error(), async.ErrTaskGoexit))
	// Output:
	// map: true false 20 30
	// each: 3 1 true
}

func Example_pipelineStreamAndFlowGoexit() {
	fn := func(_ context.Context, _ string, value int) (int, error) {
		if value == 1 {
			runtime.Goexit()
		}
		return value * 10, nil
	}
	results, err := async.Pipeline([]int{1, 2, 3}).Stage("one", 1).Stream().Run(fn)
	fmt.Println("stream:", err, results[0].Occupied, errors.Is(results[0].Err, async.ErrTaskGoexit), results[1].Value, results[2].Value)
	flow := async.Pipeline([]int{}).Stage("one", 1).Flow().Run(fn)
	for _, value := range []int{1, 2, 3} {
		if _, err := flow.Submit(context.Background(), value); err != nil {
			panic(err)
		}
	}
	remaining := flow.Close()
	fmt.Println("flow:", len(remaining), flow.ErrCount(), flow.Stats().Stages[0].Active, errors.Is(remaining[0].Err, async.ErrTaskGoexit), remaining[1].Value, remaining[2].Value)
	// Output:
	// stream: <nil> true true 20 30
	// flow: 3 1 0 true 20 30
}

func Example_pipelineNativeGoexit() {
	results, err := async.Pipeline([]int{1, 2, 3}).Stage("one", 1).Run(func(_ context.Context, _ string, value int) (int, error) {
		if value == 1 {
			runtime.Goexit()
		}
		return value * 10, nil
	})
	fmt.Println("terminal:", err)
	fmt.Println("first:", errors.Is(results[0].Err, async.ErrTaskGoexit), results[0].IsPanic())
	fmt.Println("remaining:", results[1].Value, results[2].Value)
	// Output:
	// terminal: <nil>
	// first: true false
	// remaining: 20 30
}

func Example_poolAndGroupGoexit() {
	err := async.Pool[int]().Worker(1).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
		if err := p.Submit(ctx, func(context.Context) (int, error) {
			runtime.Goexit()
			return 0, nil
		}); err != nil {
			return err
		}
		if err := p.Submit(ctx, func(context.Context) (int, error) { return 7, nil }); err != nil {
			return err
		}
		results := p.Wait()
		fmt.Println("pool:", errors.Is(results[0].Err, async.ErrTaskGoexit), results[1].Value)
		return nil
	})
	if err != nil {
		panic(err)
	}
	g := async.Group[int]().Worker(1).Build()
	if err := g.Go(context.Background(), func(context.Context) (int, error) {
		runtime.Goexit()
		return 0, nil
	}); err != nil {
		panic(err)
	}
	results := g.Wait()
	fmt.Println("group:", errors.Is(results[0].Err, async.ErrTaskGoexit), g.Active(), g.Busy())
	// Output:
	// pool: true 7
	// group: true 0 0
}

func (l startupExampleLogger) Log(context.Context, async.LogLevel, string, ...async.LogField) {}
func (l startupExampleLogger) With(...async.LogField) async.Logger                            { panic(l.marker) }
func (l startupExampleLogger) WithContext(ctx context.Context) context.Context                { return ctx }

func Example_boundedStartupRecovery() {
	previousLogger, previousTrace := async.GetLogger(), async.GetTraceLogEnabled()
	defer async.SetLogger(previousLogger)
	defer async.SetTraceLogEnabled(previousTrace)
	async.SetTraceLogEnabled(true)
	builder := async.Task[int]().Bounded(1) // Reuses the same concurrency limit.
	marker := errors.New("logger setup panic")
	async.SetLogger(startupExampleLogger{marker: marker})
	called := false
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		builder.Go(func(context.Context) (int, error) { called = true; return 1, nil })
	}()
	fmt.Println("same panic:", recovered == marker, "business called:", called)
	async.SetLogger(nil)
	value, err := builder.Go(func(context.Context) (int, error) { return 7, nil }).Wait()
	fmt.Println("same builder:", value, err)
	// Output:
	// same panic: true business called: false
	// same builder: 7 <nil>
}

func Example_retryGoexitCleanup() {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	chain := async.Retry[int](parent).RateLimiter().Rate(1).Per(time.Hour).
		MaxRetries(2).PerCallTimeout(time.Hour)
	var child context.Context
	calls, returned := 0, false
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = chain.Execute(func(ctx context.Context) (int, error) {
			child = ctx
			calls++
			runtime.Goexit()
			return 0, nil
		})
		returned = true
	}()
	<-done // Synchronizes access to fields written by the exited goroutine.
	fmt.Println("calls:", calls, "returned:", returned)
	fmt.Println("child canceled:", child.Err() == context.Canceled, "parent live:", parent.Err() == nil)
	// A new execution constructs a new limiter and can complete normally.
	value, err := chain.Execute(func(context.Context) (int, error) { return 7, nil })
	fmt.Println("next execution:", value, err)
	// Output:
	// calls: 1 returned: false
	// child canceled: true parent live: true
	// next execution: 7 <nil>
}

func Example_resultErrorChainAndFormatting() {
	pe := &async.PanicError{Value: "line1\nline2", Stack: []byte("private-stack")}
	result := async.Result[int]{Value: 42, Err: fmt.Errorf("task: %w", pe), Occupied: true}
	var extracted *async.PanicError
	fmt.Println("panic:", result.IsPanic(), "ok:", result.Ok(), "value:", result.Value)
	fmt.Println("same error:", errors.As(result.Error(), &extracted) && extracted == pe)
	fmt.Println("result has stack:", strings.Contains(fmt.Sprintf("%+v", result), "private-stack"))
	// Convert to a string before requesting quoted/escaped output.
	fmt.Println("quoted:", fmt.Sprintf("%q", pe.Error()))
	if extracted != nil {
		fmt.Println("extracted has stack:", strings.Contains(fmt.Sprintf("%+v", extracted), "private-stack"))
	}
	// Output:
	// panic: true ok: false value: 42
	// same error: true
	// result has stack: false
	// quoted: "panic: line1\nline2"
	// extracted has stack: true
}

func Example_pipelineDrainPanicAndResume() {
	handle := async.Pipeline([]int{1, 2, 3}).Stage("only", 1).Serial().Pipe().Buf(3).
		Run(func(_ context.Context, _ string, value int) (int, error) { return value, nil })
	consumed, sum := 0, 0
	marker := errors.New("consumer panic")
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		handle.Drain(func(r async.Result[int]) {
			consumed++
			sum += r.Value
			panic(marker)
		})
	}()
	fmt.Println("same panic:", recovered == marker)
	// This resumes the same queue after its current item was consumed.
	handle.Drain(func(r async.Result[int]) {
		if r.Err != nil {
			panic(r.Err)
		}
		consumed++
		sum += r.Value
	})
	fmt.Println("consumed:", consumed, "sum:", sum)
	// Output:
	// same panic: true
	// consumed: 3 sum: 6
}

func Example_pipelinePipeConfigurationSnapshot() {
	items := []int{1}
	source := async.Pipeline(items).Stage("captured", 1)
	output := source.Pipe().Buf(7)
	// Changing the source configuration does not update the existing output.
	source.Stage("later", 1).Timeout(time.Nanosecond)
	fn := func(_ context.Context, stage string, value int) (int, error) {
		if stage != "captured" {
			return 0, errors.New("unexpected later stage")
		}
		return value + 1, nil
	}
	for run := 1; run <= 2; run++ {
		// The preceding run has been fully drained before this shared write.
		items[0] = run * 10
		if run == 2 {
			output.DefaultBuf()
		}
		handle := output.Run(fn)
		fmt.Println("buffer:", cap(handle.Receive()))
		handle.Drain(func(r async.Result[int]) { fmt.Println(r.Value, r.Err) })
	}
	// Output:
	// buffer: 7
	// 11 <nil>
	// buffer: 1
	// 21 <nil>
}

func Example_pipelineSerialPipeEarlyFailure() {
	want := errors.New("first stage failure")
	var observed atomic.Int32
	output := async.Pipeline([]int{1}).Stage("first", 1).Stage("last", 1).
		FailFast().OnResult(func(_ string, r async.Result[int]) {
		if errors.Is(r.Err, want) {
			observed.Add(1)
		}
	}).Serial().Pipe()
	count := 0
	output.Run(func(_ context.Context, stage string, value int) (int, error) {
		if stage == "first" {
			return value, want
		}
		return value + 100, nil
	}).Drain(func(async.Result[int]) { count++ })
	fmt.Println("observed errors:", observed.Load(), "output results:", count)
	// Output:
	// observed errors: 1 output results: 0
}

func Example_taskResultReferences() {
	type payload struct {
		Label string
		Items []int
		Index map[string]int
	}
	ar := async.Task[payload]().Go(func(context.Context) (payload, error) {
		return payload{Label: "original", Items: []int{1, 2}, Index: map[string]int{"key": 3}}, nil
	})
	first, err := ar.Wait()
	if err != nil {
		panic(err)
	}
	first.Label = "local"
	first.Items[0] = 9
	first.Index["key"] = 8
	next, err := ar.Wait()
	if err != nil {
		panic(err)
	}
	fmt.Println(next.Label, next.Items, next.Index["key"])
	first.Items = []int{-1}
	first.Index = map[string]int{"key": -1}
	notice := <-ar.WaitCh()
	fmt.Println(notice.Value.Label, notice.Value.Items, notice.Value.Index["key"])
	// Output:
	// original [9 2] 8
	// original [9 2] 8
}

func Example_retryDefaultConfigRetainsLimiter() {
	ctx := context.Background()
	chain := async.Retry[int](ctx).
		SlidingWindow().Limit(1).Window(time.Hour).
		MaxRetries(8).DefaultConfig()
	calls := 0
	for range 2 {
		_, err := chain.Execute(func(context.Context) (int, error) {
			calls++
			return 0, errors.New("temporary failure")
		})
		fmt.Println("final limit rejection:", err != nil && strings.Contains(err.Error(), "rate limit exceeded"))
	}
	fmt.Println("business calls:", calls)
	// Output:
	// final limit rejection: true
	// final limit rejection: true
	// business calls: 2
}

func Example_multiGroupKeyedAndRoundRobin() {
	err := async.GroupMulti[int]().Worker(1).Shards(3).Run(func(ctx context.Context, mg *async.MultiGroup[int]) error {
		notices := make(chan int, 1)
		for shard := range mg.ShardCount() {
			mg.GetShard(shard).WithResultCallback(func(async.Result[int]) { notices <- shard })
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		// Rejection still consumes round-robin shard 0.
		rejected := mg.Go(canceled, func(context.Context) (int, error) { return -1, nil })
		if !errors.Is(rejected, context.Canceled) {
			return fmt.Errorf("expected canceled submission, got %v", rejected)
		}
		fmt.Println("rejected shard:", <-notices)
		// Key 5 chooses 5%3=2 without advancing the round-robin counter.
		if err := mg.GoKeyed(5, ctx, func(context.Context) (int, error) { return 50, nil }); err != nil {
			return err
		}
		fmt.Println("keyed shard:", <-notices)
		for _, value := range []int{20, 30} {
			if err := mg.Go(ctx, func(context.Context) (int, error) { return value, nil }); err != nil {
				return err
			}
			fmt.Println("round-robin shard:", <-notices)
		}
		mg.Wait()
		fmt.Println("values by shard:", mg.Values())
		fmt.Println("counts:", mg.TotalTaskCount(), mg.TotalOkCount(), mg.TotalFailCount())
		return nil // The expected canceled submission is already handled above.
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// rejected shard: 0
	// keyed shard: 2
	// round-robin shard: 1
	// round-robin shard: 2
	// values by shard: [20 50 30]
	// counts: 4 3 1
}

func Example_noResultExtremeTargetAndFirstError() {
	boom := errors.New("business failure")
	var calls atomic.Int32
	nr := async.GroupVoid().Worker(1).Build()
	defer nr.Close()
	nr.WithResultCallback(func(async.Result[struct{}]) { calls.Add(1) })
	if err := nr.GoAt(3, context.Background(), func(context.Context) error { return boom }); err != nil {
		panic(err)
	}
	nr.Wait()
	async.FillNoResultSkipped(nr, math.MinInt) // Smaller targets leave the group intact.
	total, failed, first, slots := async.BuildAggregateNoResult(nr)
	fmt.Println("negative:", total, failed, len(slots), first == boom)
	async.FillNoResultSkipped(nr, 4)
	async.FillNoResultSkipped(nr, 4) // Repeating the same target adds nothing.
	total, failed, first, slots = async.BuildAggregateNoResult(nr)
	fmt.Println("filled:", total, failed, len(slots), first == async.ErrSkipped)
	fmt.Println("business retained:", errors.Is(nr.JoinErrors(), boom), "notifications:", calls.Load())
	// Output:
	// negative: 1 1 4 true
	// filled: 4 4 4 true
	// business retained: true notifications: 1
}

func Example_builderModeSwitchViews() {
	ctx := context.Background()
	source := async.Slice(ctx, []int{1, 2, 3}).Timeout(time.Hour)
	target := source.Serial() // New policy; source's explicit timeout is absent.
	target.Take(1).Items()[0] = 9
	fmt.Println("slice views:", source.Values(), target.Values())
	result := target.Map(func(ctx context.Context, v int) (int, error) {
		_, hasDeadline := ctx.Deadline()
		fmt.Println("target deadline:", hasDeadline)
		return v * 2, nil
	})
	fmt.Println("mapped:", result.Values(), result.Error())

	mapSource := async.NewMapChain(ctx, map[string]int{"a": 1})
	mapTarget := mapSource.Serial()
	mapTarget.Set("b", 2)
	value, found := mapSource.Get("b")
	fmt.Println("map shared:", value, found)
	mapTarget.Delete("a")
	fmt.Println("map lengths:", mapSource.Len(), mapTarget.Len())

	// Output:
	// slice views: [9 2 3] [9]
	// target deadline: false
	// mapped: [18] <nil>
	// map shared: 2 true
	// map lengths: 1 1
}

func Example_sliceFilterOwnership() {
	ctx := context.Background()
	small := []int{0, 1, 2, 3}
	filtered := async.Slice(ctx, small).Filter(func(v int) bool { return v%2 == 1 })
	fmt.Println("small:", filtered.Values(), small)

	large := make([]int, 1000)
	for i := range large {
		large[i] = i
	}
	parallel := async.Slice(ctx, large).Worker(2).Filter(func(v int) bool { return v%2 == 1 })
	parallel.Items()[0] = -1
	fmt.Println("large:", parallel.Len(), large[0], large[1])

	source := []int{0, 1, 2, 3}
	copyOfSlots := append([]int(nil), source...)
	isolated := async.Slice(ctx, copyOfSlots).Serial().Filter(func(v int) bool { return v%2 == 1 })
	fmt.Println("copied:", isolated.Values(), source)

	// Output:
	// small: [1 3] [1 3 2 3]
	// large: 500 0 1
	// copied: [1 3] [0 1 2 3]
}

func Example_tokenBucketFractionalCapacity() {
	// Zero refill makes this finite budget independent of elapsed wall time.
	bucket := async.Ratelimit(context.Background()).TokenBucket().Rate(0).Capacity(0.75).Build()
	fmt.Println("one token:", bucket.Allow())
	fmt.Println("half token:", bucket.AllowN(0.5))
	fmt.Println("another half:", bucket.AllowN(0.5))
	fmt.Println("remaining quarter:", bucket.AllowN(0.25))
	fmt.Println("no refill:", bucket.AllowN(0.25))

	// Output:
	// one token: false
	// half token: true
	// another half: false
	// remaining quarter: true
	// no refill: false
}

func Example_groupWaitDuringResultCallback() {
	g := async.Group[int]().Worker(1).Timeout(0).Build()
	defer g.Close()
	started, release := make(chan struct{}), make(chan struct{})
	g.WithResultCallback(func(async.Result[int]) { close(started); <-release })
	if err := g.GoAt(3, context.Background(), func(context.Context) (int, error) { return 42, nil }); err != nil {
		panic(err)
	}
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	partial, ok := g.WaitContext(ctx)
	fmt.Println("snapshot slots/value/wait complete:", len(partial), partial[3].Value, ok)
	_, resetErr := g.Reset()
	fmt.Println("reset while notifying rejected:", resetErr != nil)
	close(release)
	final := g.Wait()
	fmt.Println("final slots/succeeded/active:", len(final), g.OkCount(), g.Active())

	// Output:
	// snapshot slots/value/wait complete: 4 42 false
	// reset while notifying rejected: true
	// final slots/succeeded/active: 4 1 0
}

func Example_poolTransformStatistics() {
	converted := errors.New("result rejected by adapter")
	err := async.Pool[int]().Worker(1).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
		p.WithTimeout(0).WithMaxResults(0)
		if err := p.SubmitAtWithResultTransform(0, ctx, func(context.Context) (int, error) {
			return 42, nil
		}, func(r async.Result[int]) async.Result[int] {
			r.Err = converted
			return r
		}); err != nil {
			return err
		}
		results := p.Wait()
		fmt.Println("stored result:", results[0].Value, errors.Is(results[0].Err, converted))
		fmt.Println("business succeeded/failed:", p.OkCount(), p.FailCount())
		return results[0].Err
	})
	fmt.Println("owner sees converted error:", errors.Is(err, converted))
	// Output:
	// stored result: 42 true
	// business succeeded/failed: 1 0
	// owner sees converted error: true
}

func Example_pipelineObserverContext() {
	var businessCtx context.Context
	results, err := async.Pipeline([]int{1}).Timeout(time.Minute).Stage("save", 1).
		OnResult(func(stage string, result async.Result[int]) {
			fmt.Println("observed business success:", stage, result.Err == nil)
			fmt.Println("business context already canceled:", errors.Is(businessCtx.Err(), context.Canceled))
		}).Serial().Run(func(ctx context.Context, _ string, value int) (int, error) {
		businessCtx = ctx
		// Perform required writes here and return any write error.
		return value + 1, nil
	})
	fmt.Println("result:", results[0].Value, err)
	// Output:
	// observed business success: save true
	// business context already canceled: true
	// result: 2 <nil>
}

func Example_sliceBatchFailureProjection() {
	failed := errors.New("batch failed")
	items := []int{1, 2, 3, 4, 5}
	result := async.Slice[int](context.Background(), items).Worker(2).Chunk(2).
		MapBatch(func(_ context.Context, batch []int) (int, error) {
			if batch[0] == 3 {
				return 7, failed
			}
			sum := 0
			for _, value := range batch {
				sum += value
			}
			return sum, nil
		})
	fmt.Println("batch slots:", result.Len())
	fmt.Println("successful batches:", result.Values())
	fmt.Println("failed inputs:", result.FailValues())
	fmt.Println("failed batch returned value:", result.Results()[1].Value)
	fmt.Println("business error:", errors.Is(result.Error(), failed))
	// Retry must account for any side effects the failed batch already made.

	// Output:
	// batch slots: 3
	// successful batches: [3 5]
	// failed inputs: [3 4]
	// failed batch returned value: 7
	// business error: true
}

func Example_groupExternalPoolResults() {
	businessErr := errors.New("business failed")
	err := async.Pool[int]().Worker(1).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
		p.WithTimeout(0).WithMaxResults(0)
		g := async.Group[int]().Worker(1).Pool(p).Build().WithTimeout(0)
		defer g.Close()
		if err := g.Go(ctx, func(context.Context) (int, error) { return 42, businessErr }); err != nil {
			return err
		}
		groupResults := g.Wait()
		fmt.Println("Group business:", groupResults[0].Value, errors.Is(groupResults[0].Err, businessErr))
		fmt.Println("Group failed/succeeded:", g.FailCount(), g.OkCount())
		// Only the pool owner waits the whole pool. Its callback is the Group
		// adapter, whose successful return cannot replace checking Group errors.
		poolResults := p.Wait()
		fmt.Println("Pool adapter:", poolResults[0].Value, poolResults[0].Err == nil)
		fmt.Println("Pool failed/succeeded:", p.FailCount(), p.OkCount())
		return g.JoinErrors()
	})
	fmt.Println("owner sees business error:", errors.Is(err, businessErr))
	// Output:
	// Group business: 42 true
	// Group failed/succeeded: 1 0
	// Pool adapter: 0 true
	// Pool failed/succeeded: 0 1
	// owner sees business error: true
}

func Example_taskCallbackDeadline() {
	// A production callback should pass ctx to I/O and return ctx.Err() when
	// cancellation invalidates its result. Here it deliberately returns success
	// after the deadline to show that the library preserves business outcomes.
	handle := async.Task[int]().WithTimeout(time.Millisecond).GoResult(func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 42, nil
	})
	defer handle.Cancel()
	value, err := handle.Result()
	fmt.Println("business result:", value, err)
	fmt.Println("outer context still live:", handle.Ctx.Err() == nil)
	handle.Cancel()
	fmt.Println("outer context canceled:", errors.Is(handle.Ctx.Err(), context.Canceled))
	value, err = handle.Result()
	fmt.Println("cached result:", value, err)
	// Output:
	// business result: 42 <nil>
	// outer context still live: true
	// outer context canceled: true
	// cached result: 42 <nil>
}

func Example_poolStreamEarlyStop() {
	err := async.Pool[int]().Worker(2).Run(func(owner context.Context, p *async.PoolInstance[int]) error {
		streamCtx, cancel := context.WithCancel(owner)
		defer cancel()
		out := async.Slice(streamCtx, []int{1, 2, 3, 4, 5, 6}).Pool(p).Stream(func(ctx context.Context, value int) (int, error) {
			return value, ctx.Err()
		}, 1)
		first, open := <-out
		fmt.Println("first valid:", open && first.Err == nil && first.Value >= 1 && first.Value <= 6)
		cancel() // Release blocked delivery when the consumer stops early.
		for range out {
			// Wait for callbacks and borrowed-pool reset before reusing p.
		}
		if err := p.Submit(owner, func(context.Context) (int, error) { return 99, nil }); err != nil {
			return err
		}
		results := p.Wait()
		fmt.Println("reused:", len(results), results[0].Value, results[0].Err == nil)
		return results[0].Err
	})
	fmt.Println("owner complete:", err == nil)
	// Output:
	// first valid: true
	// reused: 1 99 true
	// owner complete: true
}

func Example_mapPoolRetention() {
	// MaxResults limits the pool's storage. The Map adapter still allocates
	// complete result slots; batch large inputs before calling it in production.
	err := async.Pool[int]().Worker(2).MaxResults(1).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
		p.WithRingBuffer(1, async.OverflowDrop)
		chain := async.NewMapChain(ctx, map[string]int{"a": 10, "b": 20, "c": 30}).Pool(p)
		mapped := chain.Map(func(_ context.Context, _ string, value int) (int, error) { return value * 2, nil })
		fmt.Println("mapped:", mapped.Len(), mapped.Data()["a"], mapped.Data()["b"], mapped.Data()["c"])
		if mapped.Error() != nil {
			return mapped.Error()
		}
		businessErr := errors.New("batch failed")
		each := chain.Chunk(2).ForEachBatch(func(_ context.Context, entries []async.MapEntry[string, int]) error {
			if len(entries) == 2 {
				return businessErr
			}
			return nil
		})
		fmt.Println("elements:", each.Total(), each.FailCount(), each.OkCount(), errors.Is(each.Error(), businessErr))
		return each.Error()
	})
	fmt.Println("owner sees error:", err != nil)
	// Output:
	// mapped: 3 20 40 60
	// elements: 3 2 1 true
	// owner sees error: true
}

func Example_sliceFloatComparison() {
	nan := math.NaN()
	negativeZero := math.Copysign(0, -1)
	input := []float64{2, nan, nan, negativeZero, 0, -1}
	ordered := async.Slice(context.Background(), append([]float64(nil), input...)).StableSort(cmp.Compare[float64])
	values := ordered.Values()
	fmt.Println("ordered:", math.IsNaN(values[0]), math.IsNaN(values[1]), math.Signbit(values[3]), math.Signbit(values[4]))
	position, found := ordered.BinarySearch(nan, cmp.Compare[float64])
	fmt.Println("search NaN:", position, found)
	goEquality := async.Slice(context.Background(), append([]float64(nil), input...)).Dedup(nil).Values()
	compareEquality := async.Slice(context.Background(), append([]float64(nil), input...)).
		Dedup(func(a, b float64) bool { return cmp.Compare(a, b) == 0 }).Values()
	fmt.Println("dedup:", len(goEquality), len(compareEquality), math.Signbit(compareEquality[2]))
	// Output:
	// ordered: true true true false
	// search NaN: 0 true
	// dedup: 5 4 true
}

func Example_slidingWindowDurationBoundary() {
	// Validate a positive window at the configuration boundary in production.
	ctx := context.Background()
	invalid := async.Ratelimit(ctx).SlidingWindow().Limit(2).Window(time.Duration(math.MinInt64)).Build()
	fmt.Println("nonpositive window:", invalid.AllowN(2), invalid.AllowN(2), invalid.AllowN(3))
	valid := async.Ratelimit(ctx).SlidingWindow().Limit(2).Window(time.Hour).Build()
	fmt.Println("positive window:", valid.AllowN(2), valid.AllowN(1))
	// Output:
	// nonpositive window: true true false
	// positive window: true false
}

func Example_mapNaNEntryMutation() {
	nan := math.NaN()
	raw := make(map[float64]int)
	raw[nan] = 1
	raw[nan] = 2 // NaN != NaN, so these are separate entries.
	m := async.MapFromRef(raw)
	_, found := m.Get(nan)
	fmt.Println("before:", m.Len(), found)
	m.MapValues(func(_ float64, value int) int { return value * 10 })
	fmt.Println("transformed:", len(raw))
	m.FilterValues(func(value int) bool { return value == 20 })
	for _, value := range raw {
		fmt.Println("retained:", value)
	}
	m.Clear()
	fmt.Println("cleared:", len(raw))
	// Output:
	// before: 2 false
	// transformed: 2
	// retained: 20
	// cleared: 0
}

func Example_mapAliasedMerge() {
	raw := map[float64]int{1: 7}
	nan := math.NaN()
	raw[nan] = 10
	raw[nan] = 20
	m := async.MapFromRef(raw)
	m.Merge(raw) // Copy each of the two original NaN entries once.
	counts := map[int]int{}
	for _, value := range raw {
		counts[value]++
	}
	fmt.Println("merged:", len(raw), counts[7], counts[10], counts[20])
	m.MergeWithDefault(map[float64]int{1: 99, 2: 8})
	fmt.Println("defaults:", raw[1], raw[2], len(raw))
	// Output:
	// merged: 5 1 2 2
	// defaults: 7 8 6
}

func Example_shardedBucketFractionalRouting() {
	bucket := async.Ratelimit(context.Background()).Sharded().TokenBucket().
		Shards(2).Rate(0).Capacity(1.5).Build() // Zero refill isolates this finite budget.
	fmt.Println("first shard:", bucket.AllowN(1.25))
	fmt.Println("zero request:", bucket.AllowN(0)) // Advances over the untouched second shard.
	fmt.Println("selected first shard:", bucket.AllowN(0.5))
	fmt.Println("selected second shard:", bucket.AllowN(1.5))
	// Output:
	// first shard: true
	// zero request: true
	// selected first shard: false
	// selected second shard: true
}

func Example_safeCallReturnedValues() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	businessErr := errors.New("partial result")
	calls := 0
	value, err := async.SafeCall(ctx, 21, func(got context.Context, n int) (int, error) {
		calls++
		fmt.Println("callback sees cancellation:", got.Err() == context.Canceled)
		return n * 2, businessErr
	})
	fmt.Println("returned:", calls, value, errors.Is(err, businessErr))
	// Output:
	// callback sees cancellation: true
	// returned: 1 42 true
}

func Example_traceIDPreservation() {
	parent := async.WithTraceID(context.Background(), "owner")
	kept := async.WithTraceID(parent, "")
	replaced := async.WithTraceID(parent, "request")
	fmt.Println("preserved:", async.GetTraceID(kept), async.GetTraceID(parent))
	fmt.Println("replaced:", async.GetTraceID(replaced), async.GetTraceID(parent))
	empty := context.WithValue(parent, async.GetTraceIDKey(), "")
	fmt.Println("existing empty:", async.GetTraceID(async.EnsureTraceID(empty)) == "")
	generated := async.WithTraceID(context.Background(), "")
	fmt.Println("missing:", len(async.GetTraceID(generated)))
	// Output:
	// preserved: owner owner
	// replaced: request owner
	// existing empty: true
	// missing: 32
}

func Example_shardSharedContextCleanup() {
	parent := context.Background()
	err := async.PoolSharded[int]().Context(parent).Shards(2).Worker(1).FailFast().
		Run(func(shared context.Context, sp *async.ShardedPool[int]) error {
			if err := sp.SubmitAt(1, 0, shared, func(ctx context.Context) (int, error) { return 42, ctx.Err() }); err != nil {
				return err
			}
			sp.GetShard(0).Wait()
			fmt.Println("after first shard:", shared.Err() == nil, sp.GetShard(0).Ctx().Err() != nil)
			results := sp.Wait()
			fmt.Println("after all shards:", len(results), results[0].Value, results[0].Err == nil, shared.Err() != nil)
			return nil
		})
	fmt.Println("owner:", err == nil, parent.Err() == nil)
	// Output:
	// after first shard: true true
	// after all shards: 1 42 true true
	// owner: true true
}

func Example_shardBatchDrop() {
	err := async.PoolSharded[int]().Shards(2).Worker(1).Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
		sp.WithTimeout(0).WithMaxResults(0).WithMaxPending(1).WithOverflow(async.OverflowDrop)
		sp.GetShard(1).WithMaxPending(4)
		release, started := make(chan struct{}), make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		hot := sp.GetShard(0)
		if err := hot.Submit(ctx, func(context.Context) (int, error) { close(started); <-release; return 100, nil }); err != nil {
			return err
		}
		<-started
		if err := hot.Submit(ctx, func(context.Context) (int, error) { return 101, nil }); err != nil {
			return err
		}
		// The hot shard has one running callback and one pending task.
		// RoundRobin still selects it for input positions 0 and 2.
		var calls atomic.Int32
		submitted, submitErr := sp.TrySubmitBatch(ctx, []int{2, 3, 4, 5}, func(_ context.Context, v int) (int, error) { calls.Add(1); return v, nil })
		noSubmitError := submitErr == nil
		for _, r := range submitted {
			noSubmitError = noSubmitError && r.Err == nil
		}
		close(release)
		results := sp.Wait()
		var businessErr error
		for _, r := range results {
			businessErr = errors.Join(businessErr, r.Err)
		}
		fmt.Println("submission errors absent:", noSubmitError)
		fmt.Println("batch callbacks:", calls.Load())
		fmt.Println("recorded failures:", sp.TotalFailCount(), errors.Is(businessErr, async.ErrQueueOverflow))
		return errors.Join(submitErr, businessErr)
	})
	fmt.Println("owner sees overflow:", errors.Is(err, async.ErrQueueOverflow))
	// Output:
	// submission errors absent: true
	// batch callbacks: 2
	// recorded failures: 2 true
	// owner sees overflow: true
}

func Example_shardRejectedSubmissions() {
	_ = async.PoolSharded[int]().Shards(2).Worker(1).Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
		sp.GetShard(0).Close()
		var calls atomic.Int32
		fn := func(_ context.Context, item int) (int, error) { calls.Add(1); return item, nil }
		items := []int{1, 2, 3, 4}
		submitted, first := sp.TrySubmitBatch(ctx, items, fn)
		rejected, admitted := 0, 0
		for _, r := range submitted {
			if errors.Is(r.Err, async.ErrPoolClosed) {
				rejected++
			} else if r.Err == nil {
				admitted++
			} else {
				return r.Err
			}
		}
		results := sp.Wait()
		var total int64
		for _, stats := range sp.ShardStats() {
			total += stats.TotalTask
		}
		fmt.Println(errors.Is(first, async.ErrPoolClosed), rejected, admitted)
		fmt.Println(calls.Load(), len(results), total, sp.TotalFailCount())
		return nil
	})
	// Output:
	// true 2 2
	// 2 2 2 0
}

func Example_taskBoundedCancellationAndReuse() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	builder := async.Task[int]().Context(ctx).Bounded(1)
	started, release := make(chan struct{}), make(chan struct{})
	first := builder.Go(func(context.Context) (int, error) {
		close(started)
		<-release // A controlled example that deliberately ignores cancellation.
		return 41, nil
	})
	<-started
	cancel()
	called := false
	rejected := builder.Go(func(context.Context) (int, error) {
		called = true
		return 99, nil
	})
	_, err := rejected.Wait()
	fmt.Println("waiting submission:", errors.Is(err, context.Canceled), called)
	close(release)
	value, err := first.Wait()
	fmt.Println("running task:", value, err)
	// Reconfigure only after submissions stop; the same limit can be reused.
	builder.Context(context.Background())
	value, err = builder.Go(func(context.Context) (int, error) { return 42, nil }).Wait()
	fmt.Println("reused:", value, err)
	blocked, unblock := make(chan struct{}), make(chan struct{})
	first = builder.Go(func(context.Context) (int, error) {
		close(blocked)
		<-unblock
		return 43, nil
	})
	<-blocked
	// Configuration is sequential; the already-started task keeps its original permit.
	builder.DefaultBounded()
	value, err = builder.Go(func(context.Context) (int, error) { return 44, nil }).Wait()
	fmt.Println("limit cleared:", value, err)
	close(unblock)
	_, _ = first.Wait()
	// Output:
	// waiting submission: true false
	// running task: 41 <nil>
	// reused: 42 <nil>
	// limit cleared: 44 <nil>
}

func Example_muFactoryAndSnapshot() {
	var m async.Mu[int]
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		m.Append(func() int {
			close(started)
			<-release
			return 5
		})
	}()
	<-started
	fmt.Println("factory pending:", len(m.Snapshot()))
	close(release)
	<-finished
	func() {
		defer func() {
			fmt.Println("factory panic:", recover())
		}()
		m.Append(func() int {
			m.Append(func() int { return 6 })
			panic("outer") // The completed inner append is not rolled back.
		})
	}()
	snapshot := m.Snapshot()
	snapshot[0] = 99
	fmt.Println("stored:", m.Snapshot())

	value := 7
	var refs async.Mu[*int]
	refs.Append(func() *int { return &value })
	refSnapshot := refs.Snapshot()
	*refSnapshot[0] = 9 // Shared pointee; synchronize such writes in concurrent code.
	refSnapshot[0] = nil
	fmt.Println("shared value:", *refs.Snapshot()[0])
	// Output:
	// factory pending: 0
	// factory panic: outer
	// stored: [5 6]
	// shared value: 9
}

func Example_poolResizeWaitTimeout() {
	err := async.Pool[int]().Worker(1).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
		started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		if err := p.Submit(ctx, func(context.Context) (int, error) {
			close(started)
			<-release // Deliberately controlled by this example, not cancellation.
			close(finished)
			return 5, nil
		}); err != nil {
			return err
		}
		<-started
		p.ResizeAndWaitTimeout(2, 0)
		select {
		case <-finished:
			fmt.Println("still running:", false)
		default:
			fmt.Println("still running:", true)
		}
		err := p.Submit(ctx, func(context.Context) (int, error) { return 7, nil })
		fmt.Println("later submission:", err == nil)
		close(release)
		if err != nil {
			return err
		}
		results := p.Wait()
		fmt.Println("results:", len(results), results[0].Value, results[1].Value)
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// still running: true
	// later submission: true
	// results: 2 5 7
}

func Example_poolRepeatedTimedClose() {
	err := async.Pool[int]().Worker(1).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
		started, release := make(chan struct{}), make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		if err := p.Submit(ctx, func(context.Context) (int, error) {
			close(started)
			<-release
			return 1, nil
		}); err != nil {
			return err
		}
		<-started
		firstOK, workerDone := p.CloseAndWaitTimeout(0)
		secondOK, closeDone := p.CloseAndWaitTimeout(0)
		fmt.Println("first:", firstOK, workerDone != nil)
		fmt.Println("repeated:", secondOK, closeDone != nil)
		close(release)
		p.Close() // Wait for full cleanup; the first channel only tracks workers.
		finalOK, done := p.CloseAndWaitTimeout(time.Hour)
		fmt.Println("cleaned up:", finalOK, done == nil)
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// first: false true
	// repeated: false true
	// cleaned up: true true
}

func Example_poolIndexedRejection() {
	err := async.Pool[int]().Worker(1).MaxResults(0).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
		completed := make(chan struct{})
		if err := p.SubmitAtWithCompletion(0, ctx, func(context.Context) (int, error) {
			return 42, nil
		}, func(async.Result[int]) { close(completed) }); err != nil {
			return err
		}
		<-completed
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		for _, index := range []int{0, 5} {
			err := p.SubmitAt(index, canceled, func(context.Context) (int, error) {
				return 99, nil
			})
			fmt.Println("rejected:", index, errors.Is(err, context.Canceled))
		}
		results := p.Wait()
		fmt.Println("stored:", len(results), results[0].Value, errors.Is(results[1].Err, context.Canceled))
		fmt.Println("sparse:", results[2].Occupied, errors.Is(results[5].Err, context.Canceled))
		fmt.Println("counts:", p.TotalCount(), p.FailCount())
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// rejected: 0 true
	// rejected: 5 true
	// stored: 6 42 true
	// sparse: false true
	// counts: 3 2
}

func Example_autoScaleTargetLifecycle() {
	config := &async.AutoScaleConfig{MinWorkers: 2, MaxWorkers: 4, CheckInterval: time.Hour}
	err := async.Pool[int]().Worker(1).Run(func(_ context.Context, p *async.PoolInstance[int]) error {
		p.EnableAutoScale(config)
		// Enable preserves the initial target; use an initial size within bounds in production.
		fmt.Println("pool enabled:", p.Size(), p.IsAutoScaleEnabled())
		p.DisableAutoScale()
		fmt.Println("pool disabled:", p.Size(), p.IsAutoScaleEnabled())
		return nil
	})
	if err != nil {
		panic(err)
	}
	g := async.Group[int]().Worker(1).Build()
	defer g.Wait()
	g.EnableAutoScale(config)
	fmt.Println("group enabled:", g.Worker(), g.IsAutoScaleEnabled())
	g.DisableAutoScale()
	fmt.Println("group disabled:", g.Worker(), g.IsAutoScaleEnabled())
	// Output:
	// pool enabled: 1 true
	// pool disabled: 2 false
	// group enabled: 1 true
	// group disabled: 2 false
}

func Example_mapForEachRejectedStatistics() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	data := map[int]int{1: 10, 2: 20, 3: 30}
	for _, pooled := range []bool{false, true} {
		chain := async.NewMapChain[int, int](ctx, data).Worker(1)
		if pooled {
			chain.DefaultPool()
		}
		var calls atomic.Int32
		r := chain.ForEach(func(context.Context, int, int) error {
			calls.Add(1)
			return nil // This callback deliberately ignores the canceled context.
		})
		fmt.Println(pooled, calls.Load(), r.Total(), r.FailCount(), errors.Is(r.Error(), context.Canceled))
	}
	// Output:
	// false 3 3 0 false
	// true 0 3 3 true
}

func Example_mapAggregateResults() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	data := map[string]int{"one": 7}
	filtered, err := async.NewMapChain[string, int](ctx, data).Filter(func(ctx context.Context, _ string, _ int) bool {
		return errors.Is(ctx.Err(), context.Canceled)
	})
	// Filter reports the predicate's decision, even with a canceled parent.
	fmt.Println("filter:", filtered.Len(), err)
	filtered.Set("other", 9)
	fmt.Println("input length:", len(data))
	businessErr := errors.New("save failed")
	value, err := async.NewMapChain[string, int](context.Background(), data).
		Reduce(10, func(_ context.Context, acc int, _ string, value int) (int, error) {
			return acc + value, businessErr
		})
	// The value returned by the failing callback is preserved; it is not a commit.
	fmt.Println("reduce:", value, errors.Is(err, businessErr))
	// Output:
	// filter: 1 <nil>
	// input length: 1
	// reduce: 17 true
}

func Example_retryCancellationResultPriority() {
	businessErr := errors.New("business failed")
	for _, retries := range []int{0, 1} {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		value, err := async.Retry[int](ctx).MaxRetries(retries).Backoff(0, 0).Execute(func(context.Context) (int, error) {
			calls++
			cancel()
			return 7, businessErr
		})
		cancel()
		fmt.Println(retries, calls, value, errors.Is(err, businessErr), errors.Is(err, context.Canceled))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	value, err := async.Retry[int](ctx).MaxRetries(0).Execute(func(context.Context) (int, error) {
		cancel()
		return 7, nil
	})
	fmt.Println("success:", value, err)
	// Output:
	// 0 1 0 true false
	// 1 1 0 false true
	// success: 7 <nil>
}

func Example_retryShardedWindowBudget() {
	for _, shards := range []int{1, 4} {
		calls := 0
		_, err := async.Retry[int](context.Background()).SlidingWindow().Limit(1).Window(time.Hour).
			Shards(shards).MaxRetries(4).Backoff(0, 0).Execute(func(context.Context) (int, error) {
			calls++
			return 0, errors.New("temporary failure")
		})
		fmt.Println(shards, calls, strings.Contains(err.Error(), "rate limit exceeded"))
	}
	// Output:
	// 1 1 true
	// 4 4 true
}

func Example_pipelineMetadataHistory() {
	businessErr := errors.New("parse failed")
	builder := async.Pipeline[int]([]int{1}).Stage("parse", 1).Stage("recover", 1)
	fn := func(_ context.Context, stage string, value int) (int, error) {
		if stage == "parse" {
			return value + 10, businessErr
		}
		return value + 1, nil
	}
	// Builder stages continue after errors, so recovery replaces the final error.
	for _, result := range builder.ExecuteWithMeta(fn) {
		fmt.Println("history:", result.Stage, result.Value, errors.Is(result.Err, businessErr))
	}
	// Stream metadata only labels the final result, omitting intermediate errors.
	for _, result := range builder.Stream().ExecuteWithMeta(fn) {
		fmt.Println("stream:", result.Stage, result.Value, result.Err)
	}
	var calls atomic.Int32
	stopped := builder.Stream().FailFast().ExecuteWithMeta(func(_ context.Context, _ string, value int) (int, error) {
		calls.Add(1)
		return value, businessErr
	})
	// The retained first-stage failure is also labeled with the last stage name.
	fmt.Println("stopped:", calls.Load(), stopped[0].Stage, errors.Is(stopped[0].Err, businessErr))
	empty := async.Pipeline[int]([]int{7})
	fmt.Println("no stages:", len(empty.ExecuteWithMeta(fn)))
	final := empty.Stream().ExecuteWithMeta(fn)
	fmt.Println("stream identity:", len(final), final[0].Stage == "", final[0].Value)
	// Output:
	// history: parse 11 true
	// history: recover 12 false
	// stream: recover 12 <nil>
	// stopped: 1 recover true
	// no stages: 0
	// stream identity: 1 true 7
}

func Example_sliceResultEmptySlots() {
	ctx := context.Background()
	r := async.Slice(ctx, []int{1, 2}).Serial().Map(func(_ context.Context, n int) (int, error) {
		return n * 10, nil
	})
	// Results exposes the actual slots: this deliberately replaces a finished
	// result with an empty slot to show the ownership and filtering rules.
	slots := r.Results()
	slots[0] = async.Result[int]{}
	slots[1].Err = errors.New("external replacement")
	first, found := r.First()
	fmt.Println("retained:", r.Len(), slots[0].Occupied)
	fmt.Println("values/first:", r.Values(), first, found)
	fmt.Println("error recorded:", r.Err())
	var empty async.SliceResult[int]
	fmt.Println("empty ok:", empty.Ok())
	// Output:
	// retained: 2 false
	// values/first: [0] 0 true
	// error recorded: true
	// empty ok: true
}

func Example_retryTraceContext() {
	ctx := context.Background()
	readTrace := func(ctx context.Context) (bool, error) {
		return async.GetTraceID(ctx) != "", nil
	}
	plain, plainErr := async.Retry[bool](ctx).Execute(readTrace)
	explicit, explicitErr := async.Retry[bool](ctx).Context(ctx).Execute(readTrace)
	prepared, preparedErr := async.Retry[bool](async.EnsureTraceID(ctx)).Execute(readTrace)
	fmt.Println(plain, explicit, prepared)
	fmt.Println(plainErr, explicitErr, preparedErr)
	// Output:
	// false true true
	// <nil> <nil> <nil>
}

func Example_poolRejectedTransform() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := async.Pool[int]().Worker(1).Run(func(_ context.Context, p *async.PoolInstance[int]) error {
		p.WithMaxResults(0)
		calls := 0
		transform := func(r async.Result[int]) async.Result[int] {
			calls++
			r.Value, r.Err = 7, nil
			return r
		}
		submitErr := p.SubmitAtWithResultTransform(0, ctx, func(context.Context) (int, error) {
			return 1, nil
		}, transform)
		invalidErr := p.SubmitAtWithResultTransform(-1, ctx, nil, transform)
		results := p.Wait()
		fmt.Println("rejected:", errors.Is(submitErr, context.Canceled), invalidErr != nil)
		fmt.Println("transformed:", calls, results[0].Value, results[0].Err)
		fmt.Println("business counts:", p.OkCount(), p.FailCount())
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// rejected: true true
	// transformed: 1 7 <nil>
	// business counts: 0 1
}

func Example_groupRejectedCallback() {
	g := async.Group[int]().Worker(1).Build()
	defer g.Close()
	var calls atomic.Int32
	g.WithResultCallback(func(async.Result[int]) { calls.Add(1) })
	g.Wait()
	err := g.Go(context.Background(), func(context.Context) (int, error) { return 1, nil })
	fmt.Println("rejected:", errors.Is(err, async.ErrGroupWaited))
	fmt.Println("result callbacks:", calls.Load())
	// Output:
	// rejected: true
	// result callbacks: 0
}

func Example_sliceSharedCapacity() {
	input := []int{1, 2, 3, 4}
	b := async.Slice(context.Background(), input[:2])
	b.Grow(1)
	fmt.Println(input, b.Values())

	blocks := async.Slice(context.Background(), []int{1, 2, 3, 4}).Chunked(2)
	_ = append(blocks[0], 9)
	fmt.Println(blocks[1])
	// Limit capacity before appending to avoid overwriting the following block.
	part := blocks[0][:len(blocks[0]):len(blocks[0])]
	part = append(part, 8)
	fmt.Println(part, blocks[1])
	// Output:
	// [1 2 0 4] [1 2]
	// [9 4]
	// [1 2 8] [9 4]
}

func Example_poolRejectedCompletion() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var resultCalls, completionCalls atomic.Int32
	err := async.Pool[int]().Worker(1).Run(func(_ context.Context, p *async.PoolInstance[int]) error {
		p.WithResultCallback(func(async.Result[int]) { resultCalls.Add(1) })
		submitErr := p.SubmitWithCompletion(ctx, func(context.Context) (int, error) {
			return 1, nil
		}, func(r async.Result[int]) {
			completionCalls.Add(1)
			fmt.Println("completion canceled:", errors.Is(r.Err, context.Canceled))
		})
		p.Wait()
		return submitErr
	})
	fmt.Println("submit canceled:", errors.Is(err, context.Canceled))
	fmt.Println("result/completion:", resultCalls.Load(), completionCalls.Load())
	// Output:
	// completion canceled: true
	// submit canceled: true
	// result/completion: 0 1
}

func Example_noResultSkippedAggregate() {
	nr := async.GroupVoid().Worker(1).Build()
	defer nr.Close()
	if err := nr.GoAt(2, context.Background(), func(context.Context) error { return nil }); err != nil {
		panic(err)
	}
	length, done := nr.WaitTimeout(time.Minute)
	total, failed, _, results := async.BuildAggregateNoResult(nr)
	fmt.Println("before:", length, done, total, failed, len(results))
	async.FillNoResultSkipped(nr, 3)
	total, failed, first, results := async.BuildAggregateNoResult(nr)
	fmt.Println("after:", total, failed, len(results), errors.Is(first, async.ErrSkipped))
	fmt.Println("counts:", nr.TotalCount(), nr.OkCount(), nr.FailCount())
	// Output:
	// before: 3 true 1 0 3
	// after: 3 2 3 true
	// counts: 3 1 2
}

func Example_dedupComparator() {
	values := async.Slice(context.Background(), []string{"Alpha", "beta", "ALPHA"}).
		Dedup(strings.EqualFold).Values()
	fmt.Println(values)
	// Output: [Alpha beta]
}

func Example_poolLifecycle() {
	err := async.Pool[int]().Worker(2).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
		for _, n := range []int{1, 2, 3} {
			if err := p.Submit(ctx, func(context.Context) (int, error) { return n * n, nil }); err != nil {
				return err
			}
		}
		for _, r := range p.Wait() {
			if r.Err != nil {
				return r.Err
			}
			fmt.Println(r.Value)
		}
		return nil
	})
	fmt.Println(err)
	// Output:
	// 1
	// 4
	// 9
	// <nil>
}

func Example_forEachPool() {
	_, err := async.ForEachPool(context.Background(), []int{1, 2, 3},
		func(_ context.Context, n int) error {
			if n == 2 {
				return fmt.Errorf("item %d failed", n)
			}
			return nil
		}, 2)
	fmt.Println(err)
	// Output: item 2 failed
}

func Example_taskWait() {
	ar := async.Task[int]().Context(context.Background()).Go(func(context.Context) (int, error) {
		return 42, nil
	})
	value, err := ar.Wait()
	fmt.Println(value, err)
	// Output: 42 <nil>
}

func Example_retry() {
	attempts := 0
	value, err := async.Retry[int](context.Background()).MaxRetries(1).Backoff(0, 0).
		Execute(func(context.Context) (int, error) {
			attempts++
			if attempts == 1 {
				return 0, fmt.Errorf("transient")
			}
			return 7, nil
		})
	fmt.Println(value, err, attempts)
	// Output: 7 <nil> 2
}

func Example_groupLifecycle() {
	g := async.Group[int]().Worker(2).Build()
	defer g.Close()
	// Go stores results in completion order; unique GoAt indexes preserve input positions.
	for i, n := range []int{1, 2, 3} {
		if err := g.GoAt(i, context.Background(), func(context.Context) (int, error) { return n * 2, nil }); err != nil {
			panic(err)
		}
	}
	for _, r := range g.Wait() {
		fmt.Println(r.Value, r.Err)
	}
	// Output:
	// 2 <nil>
	// 4 <nil>
	// 6 <nil>
}

func Example_pipelineStages() {
	results, err := async.Pipeline([]int{1, 2}).
		Stage("double", 2).Stage("increment", 1).
		Execute(func(_ context.Context, stage string, n int) (int, error) {
			if stage == "double" {
				return n * 2, nil
			}
			return n + 1, nil
		})
	for _, r := range results {
		fmt.Println(r.Value, r.Err)
	}
	fmt.Println(err)
	// Output:
	// 3 <nil>
	// 5 <nil>
	// <nil>
}

func Example_rateLimiterLifecycle() {
	rl := async.Ratelimit(context.Background()).RateLimiter().Rate(1).Build()
	defer rl.Close()
	if err := rl.Acquire(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println(rl.Size(), rl.Available())
	rl.Release()
	fmt.Println(rl.Available())
	// Output:
	// 1 0
	// 1
}

func Example_mapShallowValuesAndSets() {
	leftValue, rightValue := 10, 20
	left := async.NewMap[string, *int]().Set("shared", &leftValue)
	right := async.NewMap[string, *int]().Set("shared", &rightValue)
	inter, union, diff := left.Intersect(right), left.Union(right), left.Difference(right)
	a, _ := inter.Get("shared")
	b, _ := union.Get("shared")
	fmt.Println("selected:", *a, *b, diff.Len())
	// Independent map entries still share the pointed-to value.
	*a = 11
	inter.Set("extra", &rightValue)
	fmt.Println("shared value, separate keys:", leftValue, left.Len(), inter.Len())
	key, value, found := async.NewMap[int, int]().Set(0, 0).Find(func(int, int) bool { return true })
	fmt.Println("valid zero entry:", key, value, found)
	empty := async.NewMap[int, int]()
	fmt.Println("empty:", empty.All(func(int, int) bool { return false }), empty.Any(func(int, int) bool { return true }), empty.Count(func(int, int) bool { return true }))
	// Output:
	// selected: 10 20 0
	// shared value, separate keys: 11 1 2
	// valid zero entry: 0 0 true
	// empty: true false 0
}

func Example_mapOwnership() {
	input := map[string]int{"a": 1}
	copy := async.MapFrom(input)
	ref := async.MapFromRef(input)
	ref.Set("a", 2)
	v, _ := copy.Get("a")
	fmt.Println(v, input["a"])
	fmt.Println(copy.GetOrSet("a", 9))
	fmt.Println(copy.GetOrSet("b", 9))
	// Output:
	// 1 2
	// 1 false
	// 9 true
}

func Example_batchStatistics() {
	ctx := context.Background()
	sliceResult := async.Slice(ctx, []int{1, 2, 3, 4}).Chunk(2).
		ForEachBatch(func(context.Context, []int) error { return fmt.Errorf("batch failed") })
	mapResult := async.NewMapChain[int, int](ctx, map[int]int{1: 1, 2: 2, 3: 3, 4: 4}).Chunk(2).
		ForEachBatch(func(context.Context, []async.MapEntry[int, int]) error { return fmt.Errorf("batch failed") })
	fmt.Println("slice batches:", sliceResult.Total(), sliceResult.FailCount())
	fmt.Println("map entries:", mapResult.Total(), mapResult.FailCount())
	// Output:
	// slice batches: 2 2
	// map entries: 4 4
}

func Example_mapBatchRejectedStatistics() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	data := map[int]int{1: 10, 2: 20, 3: 30}
	// A pre-canceled submission is rejected before the batch callback runs.
	err := async.Pool[int]().Worker(1).Run(func(_ context.Context, p *async.PoolInstance[int]) error {
		calls := 0
		r := async.NewMapChain[int, int](ctx, data).Pool(p).Chunk(2).
			ForEachBatch(func(context.Context, []async.MapEntry[int, int]) error {
				calls++
				return nil
			})
		fmt.Println("callbacks:", calls)
		fmt.Println("entries:", r.Total(), r.FailCount(), r.OkCount())
		fmt.Println("canceled:", errors.Is(r.Error(), context.Canceled))
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// callbacks: 0
	// entries: 3 3 0
	// canceled: true
}

func Example_autoScaleConfiguration() {
	config := &async.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 2}
	err := async.Pool[int]().Worker(1).Run(func(_ context.Context, p *async.PoolInstance[int]) error {
		p.EnableAutoScale(config)
		// Normalization applies to the saved copy. The caller's zero field stays zero.
		fmt.Println(config.CheckInterval, p.IsAutoScaleEnabled())
		p.DisableAutoScale()
		fmt.Println(p.Size(), p.IsAutoScaleEnabled())
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// 0s true
	// 1 false
}

func Example_streamResultOrder() {
	secondFinished := make(chan struct{})
	results, err := async.Pipeline([]int{1, 2}).Stage("double", 2).Stream().
		Run(func(_ context.Context, _ string, n int) (int, error) {
			if n == 1 {
				<-secondFinished
			} else {
				close(secondFinished)
			}
			return n * 2, nil
		})
	// Concurrent stage calls finish in a different order; the returned slice uses input indexes.
	for _, r := range results {
		fmt.Println(r.Value, r.Err, r.Occupied)
	}
	fmt.Println(err)
	// Output:
	// 2 <nil> true
	// 4 <nil> true
	// <nil>
}

func Example_taskTimeoutOverrides() {
	ctx := context.Background()
	g := async.Group[bool]().Worker(1).Timeout(time.Minute).Build()
	defer g.Close()
	if err := g.GoWithTimeout(ctx, 0, func(ctx context.Context) (bool, error) {
		_, hasDeadline := ctx.Deadline()
		return hasDeadline, nil
	}); err != nil {
		panic(err)
	}
	groupResults := g.Wait()
	fmt.Println("group deadline:", groupResults[0].Value, groupResults[0].Err)

	err := async.Pool[bool]().Worker(1).Timeout(time.Minute).Run(func(ctx context.Context, p *async.PoolInstance[bool]) error {
		if err := p.SubmitWithCompletionTimeout(ctx, func(ctx context.Context) (bool, error) {
			_, hasDeadline := ctx.Deadline()
			return hasDeadline, nil
		}, func(async.Result[bool]) {}, 0); err != nil {
			return err
		}
		poolResults := p.Wait()
		fmt.Println("pool deadline:", poolResults[0].Value, poolResults[0].Err)
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// group deadline: false <nil>
	// pool deadline: true <nil>
}

func Example_mapFailFastContext() {
	calls := 0
	var callbackContext context.Context
	result := async.NewMapChain[int, int](context.Background(), map[int]int{1: 1, 2: 2, 3: 3}).
		Worker(1).FailFast().ForEach(func(ctx context.Context, _, _ int) error {
		calls++
		callbackContext = ctx
		return fmt.Errorf("stop")
	})
	// The non-pool map path stops scheduling but does not cancel callback contexts.
	fmt.Println(calls, result.Total(), result.FailCount(), callbackContext.Err())
	// Output: 1 1 1 <nil>
}

func Example_reduceTimeoutPolicies() {
	ctx := context.Background()
	mapHasDeadline := true
	mapSum, mapErr := async.NewMapChain[int, int](ctx, map[int]int{1: 1, 2: 2, 3: 3}).
		Worker(4).Timeout(time.Minute).Reduce(0, func(ctx context.Context, acc, _, value int) (int, error) {
		_, deadline := ctx.Deadline()
		mapHasDeadline = mapHasDeadline && deadline
		return acc + value, nil
	})
	sliceHasDeadline := false
	sliceSum, sliceErr := async.Slice(ctx, []int{1, 2, 3}).
		Worker(4).Timeout(time.Minute).Reduce(0, func(ctx context.Context, acc, value int) (int, error) {
		_, deadline := ctx.Deadline()
		sliceHasDeadline = sliceHasDeadline || deadline
		return acc + value, nil
	})
	// Both reductions run synchronously. Only Map.Reduce applies its policy timeout.
	fmt.Println(mapSum, mapErr, mapHasDeadline)
	fmt.Println(sliceSum, sliceErr, sliceHasDeadline)
	// Output:
	// 6 <nil> true
	// 6 <nil> false
}

func Example_flowStageErrorCounts() {
	ctx := context.Background()
	businessErr := errors.New("stage failed")
	flow := async.NewFlow(ctx, async.FlowConfig[int]{
		Stages:  []async.Stage[int]{{Name: "parse", Concurrency: 1}, {Name: "validate", Concurrency: 1}},
		BufSize: 1,
	}, func(_ context.Context, _ string, value int) (int, error) { return value, businessErr })
	if _, err := flow.Submit(ctx, 7); err != nil {
		panic(err)
	}
	results := flow.Close()
	stats := flow.Stats()
	fmt.Println("submissions/stage errors/results:", stats.TotalSubmit, stats.TotalError, stats.TotalResult)
	fmt.Println("final failed items:", len(results), errors.Is(results[0].Err, businessErr))
	// Output:
	// submissions/stage errors/results: 1 2 1
	// final failed items: 1 true
}

func Example_flowPanicValue() {
	ctx := context.Background()
	stages := []async.Stage[int]{{Name: "panic", Concurrency: 1}}
	fn := func(context.Context, string, int) (int, error) { panic("business panic") }
	finite, err := async.ExecuteStream(ctx, stages, []int{7}, fn)
	if err != nil {
		panic(err)
	}
	flow := async.NewFlow(ctx, async.FlowConfig[int]{Stages: stages, BufSize: 1}, fn)
	if _, err := flow.Submit(ctx, 7); err != nil {
		panic(err)
	}
	results := flow.Close()
	// Flow retains the input when a stage panics; finite execution uses the zero value.
	fmt.Println(finite[0].Value, finite[0].IsPanic())
	fmt.Println(results[0].Value, results[0].IsPanic(), flow.ErrCount())
	// Output:
	// 0 true
	// 7 true 1
}

func Example_groupRunErrors() {
	businessErr := fmt.Errorf("business failure")
	var taskErr error
	err := async.Group[int]().Worker(1).Run(func(ctx context.Context, g *async.GroupInstance[int]) error {
		if err := g.Go(ctx, func(context.Context) (int, error) { return 0, businessErr }); err != nil {
			return err
		}
		_ = g.Wait()
		taskErr = g.FirstError()
		return nil // Run propagates only this callback's error.
	})
	fmt.Println(err, taskErr == businessErr)

	err = async.GroupVoid().Worker(1).Run(func(ctx context.Context, g *async.GroupNoResult) error {
		if err := g.Go(ctx, func(context.Context) error { return businessErr }); err != nil {
			return err
		}
		g.Wait()
		return g.FirstError() // Explicitly propagate the task failure.
	})
	fmt.Println(err == businessErr)
	// Output:
	// <nil> true
	// true
}

func Example_panicErrorOriginalValue() {
	original := errors.New("business panic")
	panicErr := async.NewPanicError(original)
	var wrapped *async.PanicError
	fmt.Println(errors.As(fmt.Errorf("task: %w", panicErr), &wrapped))
	fmt.Println(errors.Is(panicErr, original), panicErr.Unwrap() == nil)
	fmt.Println(wrapped.Value == original, len(wrapped.Stack) > 0)
	// Output:
	// true
	// false true
	// true true
}

func Example_cachedTaskWait() {
	ar := async.Task[int]().Go(func(context.Context) (int, error) { return 42, nil })
	_, _ = ar.Wait() // Establish the cache before canceling the waiting context.
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	value, err, ready := ar.WaitContext(waitCtx)
	fmt.Println(value, err, ready)
	value, err, ready = ar.WaitTimeout(0)
	fmt.Println(value, err, ready)
	// Output:
	// 42 <nil> true
	// 42 <nil> true
}

func Example_pipelineExternalPoolOwnership() {
	for _, auto := range []bool{false, true} {
		err := async.Pool[int]().Worker(2).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
			chain := async.Pipeline[int]([]int{2, 3}).Stage("double", 2).Parallel()
			if auto {
				chain.PoolAuto(p)
			} else {
				chain.Pool(p)
			}
			results, err := chain.Run(func(_ context.Context, _ string, value int) (int, error) {
				return value * 2, nil
			})
			if err != nil {
				return err
			}
			for _, result := range results {
				if result.Err != nil {
					return result.Err
				}
			}
			submitErr := p.Submit(ctx, func(context.Context) (int, error) { return 9, nil })
			fmt.Println(auto, results[0].Value, results[1].Value, errors.Is(submitErr, async.ErrPoolClosed))
			if !auto {
				_ = p.Wait()
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	// Output:
	// false 4 6 false
	// true 4 6 true
}

func Example_limiterBuilderContext() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	// The builder's context is not passed to the constructed limiter.
	rl := async.Ratelimit(parent).RateLimiter().Rate(1).Build()
	defer rl.Close()
	err := rl.Wait(context.Background())
	fmt.Println(err == nil)
	if err == nil {
		rl.Release()
	}

	// Pass a context to each blocking acquisition to control that wait.
	al := async.Ratelimit(parent).Adaptive().MinWorker(1).MaxWorker(1).Build()
	// Available capacity is checked before cancellation.
	if err := al.Acquire(parent); err != nil {
		panic(err)
	}
	fmt.Println("canceled with capacity: accepted")
	fmt.Println(errors.Is(al.Acquire(parent), context.Canceled))
	al.Release()
	// Output:
	// true
	// canceled with capacity: accepted
	// true
}

func Example_limiterExplicitDefaults() {
	ctx := context.Background()
	// A new TokenBucket sub-builder starts with zero rate and capacity.
	empty := async.Ratelimit(ctx).TokenBucket().Build()
	configured := async.Ratelimit(ctx).TokenBucket().DefaultRate().DefaultCapacity().Build()
	fmt.Println(empty.Allow(), configured.Allow())

	// Defaults assign their documented constants, independently of custom Rate.
	custom := async.Ratelimit(ctx).RateLimiter().Rate(1).DefaultBurst().Build()
	defer custom.Close()
	fmt.Println(custom.Size(), custom.Available() > 1)

	// Sharded token buckets use Capacity per shard; sliding windows divide a total.
	tokens := async.Ratelimit(ctx).Sharded().TokenBucket().Shards(2).Rate(0).Capacity(2).Build()
	window := async.Ratelimit(ctx).Sharded().SlidingWindow().Shards(2).Limit(2).Window(time.Hour).Build()
	for range 4 {
		fmt.Print(tokens.Allow(), " ")
	}
	fmt.Println(tokens.Allow())
	for range 3 {
		fmt.Print(window.Allow(), " ")
	}
	fmt.Println()
	// Output:
	// false true
	// 1 true
	// true true true true false
	// true true false
}

func Example_pipelineOutputPoolOwnership() {
	for _, output := range []string{"meta", "pipe"} {
		err := async.Pool[int]().Worker(1).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
			chain := async.Pipeline[int]([]int{3}).Stage("double", 1).Parallel().PoolAuto(p)
			fn := func(_ context.Context, _ string, value int) (int, error) { return value * 2, nil }
			value := 0
			if output == "meta" {
				results := chain.ExecuteWithMeta(fn)
				for _, result := range results {
					if result.Err != nil {
						return result.Err
					}
					value = result.Value
				}
			} else {
				for result := range chain.Pipe().Buf(1).Run(fn).Receive() {
					if result.Err != nil {
						return result.Err
					}
					value = result.Value
				}
			}
			// These output methods used native workers; p remains open.
			submitErr := p.Submit(ctx, func(context.Context) (int, error) { return 9, nil })
			fmt.Println(output, value, submitErr == nil)
			_ = p.Wait()
			return submitErr
		})
		if err != nil {
			panic(err)
		}
	}
	// Output:
	// meta 6 true
	// pipe 6 true
}

func Example_slicePreCanceledDispatch() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	items := []int{0, 1, 2, 3}
	var mapCalls atomic.Int32
	// Ordinary parallel Map passes the canceled context to every callback.
	mapped := async.Slice(ctx, items).Worker(2).Map(func(ctx context.Context, value int) (int, error) {
		mapCalls.Add(1)
		return value, ctx.Err()
	})
	fmt.Println("map:", mapCalls.Load(), len(mapped.Errors()))

	var eachCalls atomic.Int32
	// Group dispatch rejects these elements before calling the callback.
	each := async.Slice(ctx, items).Worker(2).ForEach(func(context.Context, int) error {
		eachCalls.Add(1)
		return nil
	})
	fmt.Println("each:", eachCalls.Load(), each.Total(), each.FailCount())

	var batchCalls atomic.Int32
	// One batch clamps concurrency to one and calls the callback synchronously.
	batch := async.Slice(ctx, items).Worker(2).Chunk(4).FailFast().ForEachBatch(func(ctx context.Context, _ []int) error {
		batchCalls.Add(1)
		return ctx.Err()
	})
	fmt.Println("batch:", batchCalls.Load(), batch.Total(), batch.FailCount())
	// Output:
	// map: 4 4
	// each: 0 4 4
	// batch: 1 1 1
}

func Example_sliceRepeatEmptyAndReferences() {
	ctx := context.Background()
	backing := []int{10, 20, 30, 40}
	empty := async.Slice(ctx, backing[2:2])
	empty.Repeat(int(^uint(0) >> 1))
	fmt.Println("empty:", empty.Len(), cap(empty.Items()), backing)

	value := 7
	repeated := async.Slice(ctx, []*int{&value}).Repeat(3).Items()
	*repeated[0] = 8 // Repeated pointers still refer to the same value.
	fmt.Println("references:", len(repeated), *repeated[2])
	replacement := 9
	repeated[0] = &replacement // Element slots can be assigned independently.
	fmt.Println("slots:", *repeated[0], *repeated[2])
	// Output:
	// empty: 0 2 [10 20 30 40]
	// references: 3 8
	// slots: 9 8
}

func Example_sliceMutationSharedArrays() {
	ctx := context.Background()
	arguments := []int{9, 8, 7, 6, 5, 4}
	b := async.Slice(ctx, []int{1, 2, 3})
	b.Prepend(arguments[:1]...) // Result adopts the argument array.
	fmt.Println("prepend:", b.Values(), arguments)
	b.Items()[0] = 99
	fmt.Println("shared:", arguments[0])

	original := []int{1, 2, 3, 4, 5}
	replaced := async.Slice(ctx, original).Replace(1, 4, original[4:]...)
	fmt.Println("replace:", replaced.Values(), original)
	// Copy the receiver/argument slots first if other views must keep old values.

	// Output:
	// prepend: [9 1 2 3] [9 1 2 3 5 4]
	// shared: 99
	// replace: [1 5 5] [1 5 5 0 0]
}
