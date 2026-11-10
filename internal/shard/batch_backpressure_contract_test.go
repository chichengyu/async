package shard

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

func TestShardedBatchHotShardBackpressure(t *testing.T) {
	for _, dist := range []Distribution{RoundRobin, Hash} {
		for _, overflow := range []core.OverflowStrategy{core.OverflowBlock, core.OverflowError, core.OverflowDrop} {
			for _, try := range []bool{false, true} {
				t.Run(fmt.Sprintf("dist=%d/overflow=%d/try=%v", dist, overflow, try), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1, Distribution: dist, KeyFn: func(v int) uint64 { return uint64(v % 2) }})
						defer sp.Close()
						sp.WithTimeout(0).WithMaxPending(1).WithOverflow(overflow).WithSubmitTimeout(2 * time.Second)
						// Keep the cold queue large enough for the complete batch even if its
						// worker is not scheduled between two nonblocking submissions.
						sp.GetShard(1).WithMaxPending(4)
						hot := sp.GetShard(0)
						release := make(chan struct{})
						defer func() {
							select {
							case <-release:
							default:
								close(release)
							}
						}()
						started := make(chan struct{})
						if err := hot.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 100, nil }); err != nil {
							t.Fatal(err)
						}
						<-started
						if err := hot.Submit(context.Background(), func(context.Context) (int, error) { return 101, nil }); err != nil {
							t.Fatal(err)
						}
						var visits [6]atomic.Int32
						results := make(chan []SubmitBatchResult, 1)
						firstErr := make(chan error, 1)
						begin := time.Now()
						go func() {
							fn := func(_ context.Context, value int) (int, error) { visits[value].Add(1); return value, nil }
							var r []SubmitBatchResult
							var err error
							if try {
								r, err = sp.TrySubmitBatch(context.Background(), []int{2, 3, 4, 5}, fn)
							} else {
								r, err = sp.SubmitBatch(context.Background(), []int{2, 3, 4, 5}, fn)
							}
							firstErr <- err
							results <- r
						}()
						synctest.Wait()
						waiting := !try && overflow == core.OverflowBlock
						if waiting {
							if len(results) != 0 || visits[3].Load() != 0 || sp.GetShard(1).TotalCount() != 0 {
								t.Fatal("later idle shard bypassed blocked earlier submission")
							}
							time.Sleep(2 * time.Second)
							synctest.Wait()
							if len(results) != 0 || visits[3].Load() != 1 || visits[5].Load() != 0 {
								t.Fatal("batch timeout was shared or later item did not continue")
							}
							time.Sleep(2 * time.Second)
							synctest.Wait()
						}
						r := <-results
						gotFirst := <-firstErr
						var wantSubmit, wantRecord error
						switch overflow {
						case core.OverflowBlock:
							wantSubmit, wantRecord = core.ErrSubmitTimeout, core.ErrSubmitTimeout
						case core.OverflowError:
							wantSubmit, wantRecord = core.ErrQueueOverflow, core.ErrQueueOverflow
						case core.OverflowDrop:
							wantRecord = core.ErrQueueOverflow
						}
						if len(r) != 4 || !errors.Is(gotFirst, wantSubmit) {
							t.Fatalf("batch result len=%d first=%v want=%v", len(r), gotFirst, wantSubmit)
						}
						for i, item := range r {
							wantErr := error(nil)
							if i%2 == 0 {
								wantErr = wantSubmit
							}
							if item.Index != i || item.ShardIdx != i%2 || !errors.Is(item.Err, wantErr) {
								t.Fatalf("batch item %d = %+v want err=%v", i, item, wantErr)
							}
						}
						wantElapsed := time.Duration(0)
						if waiting {
							wantElapsed = 4 * time.Second
						}
						if time.Since(begin) != wantElapsed {
							t.Fatalf("batch elapsed=%v want=%v", time.Since(begin), wantElapsed)
						}
						synctest.Wait()
						if hot.Pending() != 1 || hot.Busy() != 1 || hot.TotalCount() != 4 || hot.FailCount() != 2 || visits[2].Load() != 0 || visits[4].Load() != 0 || visits[3].Load() != 1 || visits[5].Load() != 1 {
							t.Fatal("hot rejection/cold progress counts disagree")
						}
						close(release)
						all := sp.Wait()
						// Wait joins registered tasks; Active is decremented by the worker
						// immediately after processTask returns. Join that scheduler step too.
						synctest.Wait()
						seen := map[int]int{}
						failed := 0
						for _, item := range all {
							if !item.Occupied {
								t.Fatal("unexpected empty result")
							}
							if item.Err != nil {
								if item.Value != 0 || !errors.Is(item.Err, wantRecord) {
									t.Fatalf("rejection result=%+v", item)
								}
								failed++
							} else {
								seen[item.Value]++
							}
						}
						if len(all) != 6 || failed != 2 || len(seen) != 4 || seen[100] != 1 || seen[101] != 1 || seen[3] != 1 || seen[5] != 1 || sp.TotalFailCount() != 2 || sp.TotalOkCount() != 4 || sp.TotalPending() != 0 || sp.TotalActive() != 0 {
							t.Fatalf("final results/counts: len=%d failed=%d seen=%v", len(all), failed, seen)
						}
					})
				})
			}
		}
	}
}

func TestShardedBatchParentDeadlineStopsLaterAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sp := NewShardedPoolSimple[int](2, 1)
		defer sp.Close()
		sp.WithTimeout(0).WithMaxPending(1).WithOverflow(core.OverflowBlock).WithSubmitTimeout(2 * time.Second)
		release := make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		started := make(chan struct{})
		hot := sp.GetShard(0)
		if err := hot.Submit(context.Background(), func(context.Context) (int, error) { close(started); <-release; return 100, nil }); err != nil {
			t.Fatal(err)
		}
		<-started
		if err := hot.Submit(context.Background(), func(context.Context) (int, error) { return 101, nil }); err != nil {
			t.Fatal(err)
		}
		parent, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var called atomic.Int32
		finished := make(chan struct{})
		var results []SubmitBatchResult
		var firstErr error
		begin := time.Now()
		go func() {
			results, firstErr = sp.SubmitBatch(parent, []int{2, 3, 4, 5}, func(context.Context, int) (int, error) { called.Add(1); return 1, nil })
			close(finished)
		}()
		synctest.Wait()
		select {
		case <-finished:
			t.Fatal("batch returned before parent deadline")
		default:
		}
		if sp.GetShard(1).TotalCount() != 0 {
			t.Fatal("batch bypassed blocked earlier item")
		}
		time.Sleep(time.Second)
		<-finished
		if time.Since(begin) != time.Second || !errors.Is(firstErr, context.DeadlineExceeded) || called.Load() != 0 || len(results) != 4 {
			t.Fatal("parent deadline did not bound complete batch admission")
		}
		for i, r := range results {
			if r.Index != i || r.ShardIdx != i%2 || !errors.Is(r.Err, context.DeadlineExceeded) {
				t.Fatalf("parent rejection %d: %+v", i, r)
			}
		}
		if hot.Pending() != 1 || hot.TotalCount() != 4 || sp.GetShard(1).TotalCount() != 2 || sp.TotalFailCount() != 4 {
			t.Fatal("parent rejection counters/slots")
		}
		close(release)
		all := sp.Wait()
		failed := 0
		for _, r := range all {
			if errors.Is(r.Err, context.DeadlineExceeded) {
				failed++
			}
		}
		if len(all) != 6 || failed != 4 || sp.TotalOkCount() != 2 || sp.TotalPending() != 0 {
			t.Fatal("parent rejection final results")
		}
	})
}

func TestShardedBatchKeyFnPanicLeavesAdmittedPrefix(t *testing.T) {
	for _, try := range []bool{false, true} {
		t.Run(fmt.Sprintf("try=%v", try), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				keyCalls := []int{}
				sp := NewShardedPool(ShardPoolConfig[int]{Shards: 2, SizePerShard: 1, Distribution: Hash, KeyFn: func(v int) uint64 {
					keyCalls = append(keyCalls, v)
					if v == 4 {
						panic("key failure")
					}
					return uint64(v % 2)
				}})
				defer sp.Close()
				sp.WithTimeout(0)
				release := make(chan struct{})
				defer func() {
					select {
					case <-release:
					default:
						close(release)
					}
				}()
				var visits [6]atomic.Int32
				var recovered any
				var results []SubmitBatchResult
				var submitErr error
				func() {
					defer func() { recovered = recover() }()
					fn := func(_ context.Context, v int) (int, error) { visits[v].Add(1); <-release; return v, nil }
					if try {
						results, submitErr = sp.TrySubmitBatch(context.Background(), []int{2, 3, 4, 5}, fn)
					} else {
						results, submitErr = sp.SubmitBatch(context.Background(), []int{2, 3, 4, 5}, fn)
					}
				}()
				synctest.Wait()
				if recovered != "key failure" || results != nil || submitErr != nil || fmt.Sprint(keyCalls) != "[2 3 4]" || visits[2].Load() != 1 || visits[3].Load() != 1 || visits[4].Load() != 0 || visits[5].Load() != 0 {
					t.Fatalf("key panic prefix: panic=%v calls=%v results=%v error=%v", recovered, keyCalls, results, submitErr)
				}
				close(release)
				all := sp.Wait()
				if len(all) != 2 || all[0].Value != 2 || all[1].Value != 3 || all[0].Err != nil || all[1].Err != nil || sp.TotalOkCount() != 2 || sp.TotalFailCount() != 0 || sp.TotalPending() != 0 {
					t.Fatalf("admitted prefix lost after routing panic: %+v", all)
				}
			})
		})
	}
}
