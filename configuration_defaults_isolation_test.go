package async

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Non-default global values catch a local Default* accidentally resetting a
// global to its initial value, which a test at initial defaults cannot detect.
func TestDefaultsPreserveCustomGlobalConfiguration(t *testing.T) {
	timeout, submit, cleanup := core.GetDefaultTimeout(), core.GetSubmitTimeoutValue(), core.GetMaxCleanupDuration()
	maxResults, ring, overflow := core.GetDefaultMaxResults(), core.GetDefaultRingBufferCap(), core.GetDefaultOverflowStrategy()
	level, trace, key := core.GetTaskFailLogLevel(), core.GetTraceLogEnabled(), core.GetTraceIDKey()
	defer func() {
		core.SetDefaultTimeout(timeout)
		core.SetSubmitTimeout(submit)
		core.SetMaxCleanupDuration(cleanup)
		core.SetDefaultMaxResults(maxResults)
		core.SetDefaultRingBuffer(ring, overflow)
		core.SetTaskFailLogLevel(level)
		core.SetTraceLogEnabled(trace)
		core.SetTraceIDKey(key)
	}()
	core.SetDefaultTimeout(37 * time.Second)
	core.SetSubmitTimeout(19 * time.Second)
	core.SetMaxCleanupDuration(41 * time.Second)
	core.SetDefaultMaxResults(23)
	core.SetDefaultRingBuffer(17, core.OverflowError)
	core.SetTaskFailLogLevel(core.LogLevelWarn)
	core.SetTraceLogEnabled(false)
	core.SetTraceIDKey(struct{ Name string }{"local-defaults-audit"})
	t.Run("all-default-methods", TestFluentLoggerDoesNotMutateGlobal)
	want := configurationGlobals()
	ctx := context.Background()
	Pool[int]().Timeout(time.Second).SubmitTimeout(time.Second).Worker(2).MaxResults(2).RingBuf(3).MaxPending(4)
	PoolMulti[int]().Timeout(time.Second).SubmitTimeout(time.Second).Worker(2).MaxResults(2).RingBuf(3).Shards(2)
	PoolSharded[int]().Timeout(time.Second).Worker(2).MaxResults(2).MaxPending(4).Shards(2)
	Group[int]().Timeout(time.Second).SubmitTimeout(time.Second).Worker(2).Streaming(3)
	GroupVoid().Timeout(time.Second).SubmitTimeout(time.Second).Worker(2).Streaming(3)
	GroupMulti[int]().Timeout(time.Second).SubmitTimeout(time.Second).Worker(2).Shards(2)
	Task[int]().WithTimeout(time.Second).Bounded(2)
	Slice(ctx, []int{1}).Timeout(time.Second).Worker(2).Chunk(3).Buf(4).Shards(2)
	NewMapChain(ctx, map[int]int{1: 1}).Timeout(time.Second).Worker(2).Chunk(3).Buf(4).Shards(2)
	Pipeline([]int{1}).Timeout(time.Second).Parallel().Worker(2).Shard(2)
	Retry[int](ctx).PerCallTimeout(time.Second).MaxRetries(2).Backoff(time.Second, 2*time.Second)
	Ratelimit(ctx).RateLimiter().Rate(2).Per(time.Second).Burst(3).DefaultRate().DefaultPer().DefaultBurst()
	if !reflect.DeepEqual(want, configurationGlobals()) {
		t.Fatal("local setters changed global configuration")
	}
}

// Resetting one builder must neither change another builder's override nor a
// previously constructed instance. Only SetDefaultTimeout changes the baseline.
func TestTimeoutDefaultsOnlyAffectCurrentBuilder(t *testing.T) {
	old := GetDefaultTimeout()
	defer SetDefaultTimeout(old)
	SetDefaultTimeout(37 * time.Second)
	a := Group[int]().Worker(1).Timeout(3 * time.Second)
	b := Group[int]().Worker(1).Timeout(5 * time.Second)
	built := a.Build()
	defer built.Close()
	a.DefaultTimeout()
	reset, other := a.Build(), b.Build()
	defer reset.Close()
	defer other.Close()
	if built.Stats().Timeout != 3*time.Second || other.Stats().Timeout != 5*time.Second || reset.Stats().Timeout != 37*time.Second || GetDefaultTimeout() != 37*time.Second {
		t.Fatal("DefaultTimeout escaped its builder scope")
	}
	p := Pool[int]().Worker(1).Timeout(3 * time.Second)
	if err := p.Run(func(_ context.Context, first *PoolInstance[int]) error {
		p.DefaultTimeout()
		return p.Run(func(_ context.Context, second *PoolInstance[int]) error {
			if first.Stats().Timeout != 3*time.Second || second.Stats().Timeout != 37*time.Second || GetDefaultTimeout() != 37*time.Second {
				t.Error("pool DefaultTimeout changed another instance or global timeout")
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}
