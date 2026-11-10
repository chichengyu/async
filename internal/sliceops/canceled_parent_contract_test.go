package sliceops

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/pool"
)

func TestCanceledParentDispatchByTerminalPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	items := []int{0, 1, 2, 3}
	type mode struct {
		name                string
		policy              Policy
		mapCalls, eachCalls int
	}
	modes := []mode{
		{name: "serial", policy: Seq(), mapCalls: 4, eachCalls: 4},
		{name: "parallel one worker", policy: Par(1), mapCalls: 4, eachCalls: 4},
		{name: "parallel", policy: Par(2), mapCalls: 4, eachCalls: 0},
		{name: "sharded", policy: Par(2).Shard(2), mapCalls: 0, eachCalls: 0},
		{name: "sharded failfast", policy: Par(2).Shard(2).FF(), mapCalls: 0, eachCalls: 0},
		{name: "chunked", policy: Par(2).Chunk(1), mapCalls: 0, eachCalls: 4},
		{name: "chunked pairs", policy: Par(2).Chunk(2), mapCalls: 0, eachCalls: 0},
		{name: "chunked short tail", policy: Par(2).Chunk(3), mapCalls: 0, eachCalls: 1},
		{name: "timeout", policy: Par(2).TO(time.Hour), mapCalls: 0, eachCalls: 0},
		{name: "serial timeout", policy: Seq().TO(time.Hour), mapCalls: 0, eachCalls: 0},
		{name: "serial failfast", policy: Seq().FF(), mapCalls: 4, eachCalls: 4},
		{name: "parallel one worker failfast", policy: Par(1).FF(), mapCalls: 0, eachCalls: 4},
		{name: "parallel failfast", policy: Par(2).FF(), mapCalls: 0, eachCalls: 0},
		{name: "failfast takes precedence over chunk", policy: Par(2).Chunk(3).FF(), mapCalls: 0, eachCalls: 0},
		{name: "timeout failfast", policy: Par(2).TO(time.Hour).FF(), mapCalls: 0, eachCalls: 0},
	}
	for _, tc := range modes {
		t.Run(tc.name+"/map", func(t *testing.T) {
			var calls atomic.Int32
			results, err := NewRunner[int, int](ctx, items, tc.policy).Map(func(got context.Context, v int) (int, error) {
				calls.Add(1)
				if !errors.Is(got.Err(), context.Canceled) {
					t.Errorf("callback context error=%v", got.Err())
				}
				return v + 10, nil
			})
			if int(calls.Load()) != tc.mapCalls {
				t.Fatalf("callback calls=%d want=%d", calls.Load(), tc.mapCalls)
			}
			if len(results) != 4 {
				t.Fatalf("results=%+v", results)
			}
			for i, r := range results {
				if tc.mapCalls == 4 {
					if r.Value != items[i]+10 || r.Err != nil {
						t.Fatalf("result %d=%+v", i, r)
					}
				} else if !errors.Is(r.Err, context.Canceled) {
					t.Fatalf("rejected result %d=%+v", i, r)
				}
			}
			if tc.policy.IsFailFast() && tc.mapCalls == 0 && !errors.Is(err, context.Canceled) {
				t.Fatalf("fail-fast summary=%v", err)
			}
		})
		t.Run(tc.name+"/foreach", func(t *testing.T) {
			var calls atomic.Int32
			total, failures, err, _ := NewRunner[int, int](ctx, items, tc.policy).Each(func(got context.Context, _ int) error {
				calls.Add(1)
				if !errors.Is(got.Err(), context.Canceled) {
					t.Errorf("callback context error=%v", got.Err())
				}
				return nil
			})
			if int(calls.Load()) != tc.eachCalls {
				t.Fatalf("callback calls=%d want=%d", calls.Load(), tc.eachCalls)
			}
			if total != 4 || failures != int64(4-tc.eachCalls) {
				t.Fatalf("statistics total=%d failures=%d", total, failures)
			}
			if failures > 0 && !errors.Is(err, context.Canceled) {
				t.Fatalf("rejected summary=%v", err)
			}
			if failures == 0 && err != nil {
				t.Fatalf("callback ignored cancellation; error=%v", err)
			}
		})
	}
	t.Run("pool rejects before callback", func(t *testing.T) {
		pl := pool.NewPool[int](2)
		defer pl.Close()
		var mapCalls, eachCalls atomic.Int32
		r := NewRunner[int, int](ctx, items, Par(2).WithPool(pl))
		results, err := r.Map(func(context.Context, int) (int, error) { mapCalls.Add(1); return 1, nil })
		if mapCalls.Load() != 0 || len(results) != 4 || err == nil {
			t.Fatalf("pool map calls=%d results=%+v err=%v", mapCalls.Load(), results, err)
		}
		total, failures, err, _ := r.Each(func(context.Context, int) error { eachCalls.Add(1); return nil })
		if eachCalls.Load() != 0 || total != 4 || failures != 4 || err == nil {
			t.Fatalf("pool each calls=%d stats=%d/%d err=%v", eachCalls.Load(), total, failures, err)
		}
	})
}

func TestCanceledParentBatchDispatchByTerminalPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	items := []int{0, 1, 2, 3}
	cases := []struct {
		name                string
		policy              Policy
		batchLengths        []int
		mapCalls, eachCalls int
	}{
		{"serial default", Seq(), []int{1, 1, 1, 1}, 4, 4},
		{"parallel default", Par(2), []int{1, 1, 1, 1}, 4, 0},
		{"parallel negative chunk", Par(2).Chunk(-1), []int{1, 1, 1, 1}, 4, 0},
		{"parallel chunk one", Par(2).Chunk(1), []int{1, 1, 1, 1}, 0, 4},
		{"parallel chunk two", Par(2).Chunk(2), []int{2, 2}, 0, 0},
		{"parallel chunk three", Par(2).Chunk(3), []int{3, 1}, 0, 0},
		{"parallel single batch", Par(2).Chunk(4), []int{4}, 0, 1},
		{"serial chunk three", Seq().Chunk(3), []int{3, 1}, 0, 2},
		{"parallel failfast default", Par(2).FF(), []int{1, 1, 1, 1}, 0, 0},
		{"parallel failfast single batch", Par(2).Chunk(4).FF(), []int{4}, 0, 1},
		{"timeout single batch", Par(2).Chunk(4).TO(time.Hour), []int{4}, 0, 0},
		{"sharded single batch", Par(2).Chunk(4).Shard(2), []int{4}, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mapCalls, eachCalls atomic.Int32
			results, err := NewRunner[int, int](ctx, items, tc.policy).MapBatch(func(got context.Context, batch []int) (int, error) {
				mapCalls.Add(1)
				if !errors.Is(got.Err(), context.Canceled) {
					t.Errorf("map batch context error=%v", got.Err())
				}
				return len(batch), nil
			})
			if int(mapCalls.Load()) != tc.mapCalls || len(results) != len(tc.batchLengths) {
				t.Fatalf("map batch calls=%d results=%+v", mapCalls.Load(), results)
			}
			for i, result := range results {
				if tc.mapCalls > 0 {
					if result.Value != tc.batchLengths[i] || result.Err != nil {
						t.Fatalf("batch %d=%+v", i, result)
					}
				} else if !errors.Is(result.Err, context.Canceled) {
					t.Fatalf("rejected batch %d=%+v", i, result)
				}
			}
			if tc.mapCalls == 0 && tc.policy.IsFailFast() && !errors.Is(err, context.Canceled) {
				t.Fatalf("map batch summary=%v", err)
			}
			total, failures, err, _ := NewRunner[int, int](ctx, items, tc.policy).EachBatch(func(got context.Context, batch []int) error {
				eachCalls.Add(1)
				if !errors.Is(got.Err(), context.Canceled) {
					t.Errorf("each batch context error=%v", got.Err())
				}
				if len(batch) == 0 {
					t.Error("unexpected empty callback batch")
				}
				return nil
			})
			wantFailures := int64(len(tc.batchLengths) - tc.eachCalls)
			if int(eachCalls.Load()) != tc.eachCalls || total != int64(len(tc.batchLengths)) || failures != wantFailures {
				t.Fatalf("each batch calls=%d total=%d failures=%d want=%d/%d/%d", eachCalls.Load(), total, failures, tc.eachCalls, len(tc.batchLengths), wantFailures)
			}
			if failures > 0 && !errors.Is(err, context.Canceled) || failures == 0 && err != nil {
				t.Fatalf("each batch summary=%v", err)
			}
		})
	}
}
