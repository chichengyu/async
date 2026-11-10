package shard

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Catch premature shared cancellation, missing task cancellation, snapshots
// presented as completion, incorrect shard ordering, and snapshots that alias
// the final stored results. Keep business success independent of cancellation
// so a FailFast business failure cannot mask the wait's own cancellation scope.
func TestShardedGroupBoundedWaitSnapshotAndCleanup(t *testing.T) {
	for _, count := range []int{1, 3} {
		for _, notification := range []bool{false, true} {
			for _, canceledWait := range []bool{false, true} {
				for _, failFast := range []bool{false, true} {
					t.Run(fmt.Sprintf("shards=%d/notification=%v/canceled=%v/failfast=%v", count, notification, canceledWait, failFast), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							parent, cancel := context.WithCancel(context.Background())
							defer cancel()
							sg := NewShardedGroup(ShardGroupConfig[int]{Shards: count, ConcurrencyPerShard: 1}).WithTimeout(0)
							defer sg.Close()
							shared := parent
							if failFast {
								_, shared = sg.WithFailFast(parent)
							}
							gate := make(chan struct{})
							defer func() {
								select {
								case <-gate:
								default:
									close(gate)
								}
							}()
							last := count - 1
							started := make(chan context.Context, count)
							notified := make(chan core.Result[int], count+1)
							for shard := range count {
								sg.GetShard(shard).WithResultCallback(func(r core.Result[int]) {
									notified <- r
									if notification && shard == last {
										<-gate
									}
								})
								if err := sg.GoAt(shard, 2, shared, func(ctx context.Context) (int, error) {
									started <- ctx
									if !notification && shard == last {
										<-gate
									}
									return 100 + shard, nil
								}); err != nil {
									t.Fatal(err)
								}
							}
							contexts := make([]context.Context, count)
							for i := range count {
								contexts[i] = <-started
							}
							for shard := 0; shard < last; shard++ {
								sg.GetShard(shard).Wait()
							}
							synctest.Wait()
							begin := time.Now()
							var partial []core.Result[int]
							var ok bool
							if canceledWait {
								ctx, stop := context.WithCancel(context.Background())
								stop()
								partial, ok = sg.WaitContext(ctx)
							} else {
								partial, ok = sg.WaitTimeout(2 * time.Second)
							}
							if ok || len(partial) != count*3 {
								t.Fatalf("snapshot=%+v complete=%v", partial, ok)
							}
							wantElapsed := 2 * time.Second
							if canceledWait {
								wantElapsed = 0
							}
							if time.Since(begin) != wantElapsed {
								t.Fatalf("wait elapsed=%v want=%v", time.Since(begin), wantElapsed)
							}
							for _, ctx := range contexts {
								if ctx.Err() != context.Canceled {
									t.Fatal("accepted task context not canceled")
								}
							}
							if parent.Err() != nil || shared.Err() != nil {
								t.Fatal("bounded wait prematurely canceled parent/shared context")
							}
							for i, r := range partial {
								occupied := i%3 == 2 && (i/3 != last || notification)
								want := 0
								if occupied {
									want = 100 + i/3
								}
								if r.Occupied != occupied || r.Value != want || r.Err != nil {
									t.Fatalf("snapshot slot %d=%+v occupied=%v value=%d", i, r, occupied, want)
								}
							}
							partial[2].Value = -1
							finished := make(chan []core.Result[int], 1)
							go func() { finished <- sg.Wait() }()
							synctest.Wait()
							if len(finished) != 0 {
								t.Fatal("final Wait ignored blocked business/notification")
							}
							close(gate)
							final := <-finished
							if len(final) != count*3 || sg.TotalActive() != 0 || sg.TotalFailCount() != 0 || sg.TotalOkCount() != int64(count) || len(notified) != count {
								t.Fatalf("final=%+v active=%d failed=%d ok=%d notifications=%d", final, sg.TotalActive(), sg.TotalFailCount(), sg.TotalOkCount(), len(notified))
							}
							for shard := range count {
								if r := final[shard*3+2]; !r.Occupied || r.Value != 100+shard || r.Err != nil {
									t.Fatalf("final shard %d=%+v", shard, r)
								}
							}
							if partial[2].Value != -1 || (shared.Err() != nil) != failFast || parent.Err() != nil {
								t.Fatal("snapshot or final context lifetime changed")
							}
							if err := sg.Reset(); err != nil {
								t.Fatal(err)
							}
							if err := sg.GoAt(last, 0, parent, func(context.Context) (int, error) { return 7, nil }); err != nil {
								t.Fatal(err)
							}
							if next := sg.Wait(); len(next) != 1 || next[0].Value != 7 || next[0].Err != nil || sg.TotalOkCount() != 1 {
								t.Fatalf("reset batch=%+v", next)
							}
						})
					})
				}
			}
		}
	}
}
