package shard

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// Mix entry points before waiting, with independently computed routes and
// per-shard outcome multisets. One shard may already be closed; canceled inputs
// never run. This does not model simultaneous configuration or closing.
func FuzzShardRejectedRouting(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{40, 41, 2}, uint8(8)) // Batch begins at a closed shard, then has later items.
	f.Add([]byte{0, 1, 2, 3, 4, 5, 8, 19, 23, 127, 255}, uint8(0))
	f.Add([]byte{4, 5, 4, 5, 2, 0, 3, 1}, uint8(19))
	f.Add([]byte{24, 25, 26, 27, 28, 29, 6, 7, 8, 9}, uint8(10))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 24 {
			data = data[:24]
		}
		shards := int(config%4) + 1
		closed := -1
		if config&8 != 0 {
			closed = int(config/16) % shards
		}
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		businessErr := errors.New("shard routing business error")
		for engine := range 2 {
			sp := NewShardedPool(ShardPoolConfig[int]{Shards: shards, SizePerShard: 2,
				Distribution: Distribution(config / 2 % 3), KeyFn: func(v int) uint64 { return uint64(v*7 + 3) }})
			sg := NewShardedGroup(ShardGroupConfig[int]{Shards: shards, ConcurrencyPerShard: 2, Distribution: Hash})
			// Only one engine is exercised per pass; both constructors own resources.
			defer sp.Close()
			defer sg.Close()
			if closed >= 0 {
				if engine == 0 {
					sp.GetShard(closed).Close()
				} else {
					sg.GetShard(closed).Close()
				}
			}
			visits := make([]atomic.Int32, 73)
			wantVisits := make([]int32, 73)
			expected := make([]map[int]error, shards)
			rejected := make([]map[error]int, shards)
			totals := make([]int, shards)
			for s := range shards {
				expected[s] = make(map[int]error)
				rejected[s] = make(map[error]int)
			}
			rr, id := 0, 0
			pick := func(item int, batch bool) int {
				if engine == 0 {
					switch Distribution(config / 2 % 3) {
					case Hash:
						if batch {
							return (item*7 + 3) % shards
						}
						return 0
					case RoundRobin:
					default:
						return 0
					}
				}
				s := rr % shards
				rr++
				return s
			}
			// FNV-1a implemented as arithmetic, independently of the library hash call.
			keyed := func(key string) int {
				h := uint32(2166136261)
				for _, b := range []byte(key) {
					h = (h ^ uint32(b)) * 16777619
				}
				return int(uint64(h) % uint64(shards))
			}
			fn := func(_ context.Context, item int) (int, error) {
				visits[item].Add(1)
				if item%3 == 0 {
					return item, businessErr
				}
				return item, nil
			}
			checkSubmission := func(item, s int, wasCanceled, try bool, got error) error {
				t.Helper()
				var want error
				if s == closed {
					if engine == 0 {
						want = core.ErrPoolClosed
					} else {
						want = core.ErrGroupWaited
					}
				} else if wasCanceled {
					want = context.Canceled
				} else if engine == 0 && try && errors.Is(got, core.ErrSubmitTimeout) {
					// A finite queue may fill before a worker runs. Either admission
					// or nonblocking refusal is legal; the outcome must agree below.
					want = core.ErrSubmitTimeout
				}
				if !errors.Is(got, want) {
					t.Fatalf("engine=%d item=%d shard=%d submit=%v want=%v", engine, item, s, got, want)
				}
				// TrySubmit rejects closed/waited/waiting pools before recording
				// counters or results; ordinary Submit still records the rejection.
				if engine == 0 && try && s == closed {
					return want
				}
				totals[s]++
				if want == nil {
					wantVisits[item] = 1
				}
				if want != nil {
					rejected[s][want]++
				} else if item%3 == 0 {
					expected[s][item] = businessErr
				} else {
					expected[s][item] = nil
				}
				return want
			}
			for _, b := range data {
				ctx := context.Background()
				wasCanceled := b&16 != 0
				if wasCanceled {
					ctx = canceled
				}
				op := b % 6
				if op >= 4 {
					items := make([]int, int(b/32%3)+1)
					routes := make([]int, len(items))
					for i := range items {
						id++
						items[i] = id
						routes[i] = pick(id, true)
					}
					var gotFirst error
					if engine == 0 {
						var results []SubmitBatchResult
						if op == 4 {
							results, gotFirst = sp.SubmitBatch(ctx, items, fn)
						} else {
							results, gotFirst = sp.TrySubmitBatch(ctx, items, fn)
						}
						if len(results) != len(items) {
							t.Fatal("pool batch length")
						}
						var first error
						for i, r := range results {
							if r.Index != i || r.ShardIdx != routes[i] {
								t.Fatalf("pool route=%+v want index=%d shard=%d", r, i, routes[i])
							}
							err := checkSubmission(items[i], routes[i], wasCanceled, op == 5, r.Err)
							if first == nil {
								first = err
							}
						}
						if !errors.Is(gotFirst, first) {
							t.Fatalf("pool first error=%v want=%v", gotFirst, first)
						}
					} else {
						results, err := sg.GoBatch(ctx, items, fn)
						gotFirst = err
						if len(results) != len(items) {
							t.Fatal("group batch length")
						}
						var first error
						for i, r := range results {
							if r.Index != i || r.ShardIdx != routes[i] {
								t.Fatalf("group route=%+v want index=%d shard=%d", r, i, routes[i])
							}
							err := checkSubmission(items[i], routes[i], wasCanceled, false, r.Err)
							if first == nil {
								first = err
							}
						}
						if !errors.Is(gotFirst, first) {
							t.Fatalf("group first error=%v want=%v", gotFirst, first)
						}
					}
					continue
				}
				id++
				item := id
				callback := func(ctx context.Context) (int, error) { return fn(ctx, item) }
				var got error
				s := 0
				if op == 2 {
					key := fmt.Sprintf("route-%d", int(b/32))
					s = keyed(key)
					if engine == 0 {
						got = sp.SubmitKeyed(key, ctx, callback)
					} else {
						got = sg.GoKeyed(key, ctx, callback)
					}
				} else {
					s = pick(item, false)
					if op == 3 {
						// Invalid shard selection falls back to ordinary routing. A fresh sparse
						// result index avoids the engines' different duplicate-index policies.
						if engine == 0 {
							got = sp.SubmitAt(shards, 100+item*4, ctx, callback)
						} else {
							got = sg.GoAt(-1, 100+item*4, ctx, callback)
						}
					} else if engine == 0 {
						if op == 1 {
							got = sp.TrySubmit(ctx, callback)
						} else {
							got = sp.Submit(ctx, callback)
						}
					} else {
						got = sg.Go(ctx, callback)
					}
				}
				checkSubmission(item, s, wasCanceled, op == 1, got)
			}
			for s := range shards {
				var results []core.Result[int]
				var total, failed int64
				if engine == 0 {
					p := sp.GetShard(s)
					results = p.Wait()
					total, failed = p.TotalCount(), p.FailCount()
				} else {
					g := sg.GetShard(s)
					results = g.Wait()
					total, failed = g.TotalCount(), g.FailCount()
				}
				wantFailed := 0
				for _, err := range expected[s] {
					if err != nil {
						wantFailed++
					}
				}
				for _, n := range rejected[s] {
					wantFailed += n
				}
				if total != int64(totals[s]) || failed != int64(wantFailed) {
					t.Fatalf("engine=%d shard=%d counts=%d/%d want=%d/%d", engine, s, total, failed, totals[s], wantFailed)
				}
				for _, r := range results {
					if !r.Occupied {
						continue
					}
					if r.Value == 0 {
						if rejected[s][r.Err] == 0 {
							t.Fatalf("engine=%d shard=%d unexpected rejection=%+v", engine, s, r)
						}
						rejected[s][r.Err]--
						continue
					}
					want, exists := expected[s][r.Value]
					if !exists || !errors.Is(r.Err, want) || visits[r.Value].Load() != 1 {
						t.Fatalf("engine=%d shard=%d unexpected outcome=%+v", engine, s, r)
					}
					delete(expected[s], r.Value)
				}
				if len(expected[s]) != 0 {
					t.Fatalf("engine=%d shard=%d missing outcomes=%v", engine, s, expected[s])
				}
				for err, n := range rejected[s] {
					if n != 0 {
						t.Fatalf("engine=%d shard=%d missing rejection=%v count=%d", engine, s, err, n)
					}
				}
			}
			sp.Close()
			sg.Close()
			for i := 1; i <= id; i++ {
				if visits[i].Load() != wantVisits[i] {
					t.Fatalf("item %d visits=%d want=%d", i, visits[i].Load(), wantVisits[i])
				}
			}
		}
	})
}
