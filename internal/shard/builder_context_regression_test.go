package shard

import (
	"context"
	"errors"
	"testing"
)

func TestPoolBuilderNormalWaitKeepsOtherShardContext(t *testing.T) {
	err := NewPoolBuilder[int]().Shards(2).Worker(1).FailFast().Run(func(ctx context.Context, sp *ShardedPool[int]) error {
		release := make(chan struct{})
		defer close(release)
		started := make(chan struct{})
		if err := sp.SubmitAt(1, 0, ctx, func(taskCtx context.Context) (int, error) {
			close(started)
			<-release
			return 42, taskCtx.Err()
		}); err != nil {
			return err
		}
		<-started
		// Completing an empty first shard is normal lifecycle cleanup, not failure.
		sp.GetShard(0).Wait()
		if ctx.Err() != nil {
			return errors.New("normal first-shard wait canceled Run context")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPoolBuilderFullWaitDoesNotCancelLateShard(t *testing.T) {
	err := NewPoolBuilder[int]().Shards(2).Worker(1).FailFast().Run(func(shared context.Context, sp *ShardedPool[int]) error {
		started := make(chan struct{})
		first := sp.GetShard(0).Ctx()
		if err := sp.SubmitAt(1, 0, shared, func(ctx context.Context) (int, error) {
			close(started)
			<-first.Done() // Wait reaches the empty first shard before this task ends.
			return 42, ctx.Err()
		}); err != nil {
			return err
		}
		<-started
		results := sp.Wait()
		if len(results) != 1 || results[0].Value != 42 || results[0].Err != nil {
			return errors.New("full Wait canceled a healthy late shard")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestShardedPoolReturnedFailFastContextLifetime(t *testing.T) {
	sp := NewShardedPoolSimple[int](2, 1)
	defer sp.Close()
	_, ctx := sp.WithFailFast(context.Background())
	sp.Wait()
	// Every child has finished. The shared derived context must also be released.
	if ctx.Err() != context.Canceled {
		t.Fatal("shared FailFast context remains live after all shards Wait")
	}
}

func TestShardedGroupReturnedFailFastContextLifetime(t *testing.T) {
	for _, waitContext := range []bool{false, true} {
		for _, count := range []int{1, 2, 4} {
			sg := NewShardedGroup(ShardGroupConfig[int]{Shards: count, ConcurrencyPerShard: 1})
			_, ctx := sg.WithFailFast(context.Background())
			if waitContext {
				_, ok := sg.WaitContext(context.Background())
				if !ok {
					t.Fatal("completed wait failed")
				}
			} else {
				sg.Wait()
			}
			if ctx.Err() != context.Canceled {
				sg.Close()
				t.Fatalf("shards=%d waitContext=%v shared context not released", count, waitContext)
			}
			sg.Close()
		}
	}
}

func TestShardedSharedContextLifetimeAcrossReset(t *testing.T) {
	for engine := range 2 {
		sp := NewShardedPoolSimple[int](3, 1)
		sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 3, ConcurrencyPerShard: 1})
		defer sp.Close()
		defer sg.Close()
		for round := range 3 {
			var shared context.Context
			parent, cancel := context.WithCancel(context.Background())
			if engine == 0 {
				_, shared = sp.WithFailFast(parent)
			} else {
				_, shared = sg.WithFailFast(parent)
			}
			// Each shard has started before one is allowed to finish. No timer or
			// scheduler speed is needed to establish the normal-cleanup invariant.
			started := make([]chan struct{}, 3)
			release := make([]chan struct{}, 3)
			for s := range 3 {
				started[s] = make(chan struct{})
				release[s] = make(chan struct{})
				fn := func(ctx context.Context) (int, error) { close(started[s]); <-release[s]; return s + 1, ctx.Err() }
				var err error
				if engine == 0 {
					err = sp.SubmitAt(s, 0, shared, fn)
				} else {
					err = sg.GoAt(s, 0, shared, fn)
				}
				if err != nil {
					for j := 0; j <= s; j++ {
						close(release[j])
					}
					cancel()
					t.Fatal(err)
				}
			}
			for _, ch := range started {
				<-ch
			}
			close(release[0])
			if engine == 0 {
				sp.GetShard(0).Wait()
			} else {
				sg.GetShard(0).Wait()
			}
			if shared.Err() != nil {
				close(release[1])
				close(release[2])
				cancel()
				t.Fatalf("engine=%d round=%d first shard canceled shared context", engine, round)
			}
			close(release[1])
			close(release[2])
			if engine == 0 {
				// Individual Wait consumed shard 0's one-time Pool result already.
				results := sp.Wait()
				if len(results) != 2 {
					t.Fatalf("pool results=%v", results)
				}
				for _, r := range results {
					if r.Err != nil {
						t.Fatalf("pool normal wait result=%+v", r)
					}
				}
			} else {
				results := sg.Wait()
				if len(results) != 3 {
					t.Fatalf("group results=%v", results)
				}
				for _, r := range results {
					if r.Err != nil {
						t.Fatalf("group normal wait result=%+v", r)
					}
				}
			}
			if shared.Err() != context.Canceled || parent.Err() != nil {
				cancel()
				t.Fatalf("engine=%d shared=%v parent=%v", engine, shared.Err(), parent.Err())
			}
			cancel()
			if round != 2 {
				var err error
				if engine == 0 {
					err = sp.Reset()
				} else {
					err = sg.Reset()
				}
				if err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

func TestShardedCloseReleasesSharedContext(t *testing.T) {
	sp := NewShardedPoolSimple[int](2, 1)
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 1})
	_, pctx := sp.WithFailFast(context.Background())
	_, gctx := sg.WithFailFast(context.Background())
	sp.Close()
	sg.Close()
	if pctx.Err() != context.Canceled || gctx.Err() != context.Canceled {
		t.Fatalf("Close contexts: pool=%v group=%v", pctx.Err(), gctx.Err())
	}
}

func TestShardedRepeatedFailFastReleasesEveryDerivedContext(t *testing.T) {
	sp := NewShardedPoolSimple[int](2, 1)
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 1})
	defer sp.Close()
	defer sg.Close()
	var derived []context.Context
	for range 3 {
		_, pctx := sp.WithFailFast(context.Background())
		_, gctx := sg.WithFailFast(context.Background())
		derived = append(derived, pctx, gctx)
	}
	sp.Wait()
	sg.Wait()
	for i, ctx := range derived {
		if ctx.Err() != context.Canceled {
			t.Fatalf("derived context %d remains live", i)
		}
	}
}

func TestShardedGroupIncompleteWaitDefersSharedCleanup(t *testing.T) {
	sg := NewShardedGroup(ShardGroupConfig[int]{Shards: 2, ConcurrencyPerShard: 1})
	defer sg.Close()
	_, shared := sg.WithFailFast(context.Background())
	release := make(chan struct{})
	started := make(chan struct{})
	if err := sg.GoAt(1, 0, shared, func(context.Context) (int, error) { close(started); <-release; return 42, nil }); err != nil {
		close(release)
		t.Fatal(err)
	}
	<-started
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok := sg.WaitContext(waitCtx)
	live := shared.Err() == nil
	close(release)
	results := sg.Wait()
	if ok || !live {
		t.Fatalf("incomplete WaitContext ok=%v sharedLive=%v", ok, live)
	}
	if len(results) != 1 || results[0].Err != nil || shared.Err() != context.Canceled {
		t.Fatalf("final results=%v shared=%v", results, shared.Err())
	}
}
